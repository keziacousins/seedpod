"""The Instance aggregate, from birth to destroyed, through the REAL Dispatcher, EffectExecutor,
TimerService and WorkflowEngine over real tmp SQLite, with FAKE verbs (DR-0055).

No provider, no shipped workflow YAML: the two workflows below (``provision-instance-fake``,
``destroy-instance``) are test fixtures, and the verbs are fakes that block, fail or pause on
command. What is under test is the plumbing this phase adds:

- an instance's RunWorkflow/CancelWorkflow are admitted into runs with ``instance_id`` and no
  ``cluster_id``, and the terminal and ``emit:`` events target the INSTANCE;
- destroy while provisioning cancels the provision run, waits for it, then destroys;
- expiry during provisioning does the same, from the timer;
- a destroy that fails is retried, by request and by the timer it armed;
- a crash at each persistence point resumes to the same end state, with nothing applied twice;
- cancel kills the run's subprocesses;
- a cluster and an instance that share an id and a slug never cross.

No Mock/patch: every double is a plain class (tests/engine/fakes.py's convention).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel
from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.core.events import CreateRequested, InstanceCreateRequested, InstanceDestroyRequested
from seedpod.core.machine import INSTANCE_DESTROY_RETRY_DELAY
from seedpod.data.repositories import (
    ClusterRepository,
    ClusterStateAuditRepository,
    DeploymentRepository,
    DeploymentStateAuditRepository,
    InstanceRepository,
    InstanceStateAuditRepository,
    OutboxRepository,
    Repositories,
    TimerRepository,
    WorkflowRunRepository,
)
from seedpod.engine.dispatch_table import WorkflowDispatch
from seedpod.engine.schedule import NAMED_POLICIES
from seedpod.engine.step import EmptyOutput, Step, StepContext
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.runtime.effect_executor import EffectExecutor
from seedpod.runtime.subprocess_manager import SubprocessManager
from seedpod.runtime.timers import TimerService
from tests.engine.fakes import EngineHarness, PauseGate, build_engine, crash_run
from tests.runtime.conftest import FakeHub, make_cluster_row, make_instance_row, make_owner_key

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)
HOUR = timedelta(hours=1)

PROVISION_YAML = """
workflow: provision-instance-fake
version: 1
inputs:
  instance_id: {type: str}
on_failure: compensate
outcome:
  succeeded:
    event: InstanceProvisioned
    payload: {address: {from: create.address}, resource_ids: {from: create.resource_ids}}
  failed: {event: InstanceProvisionFailed}
  cancelled: {event: InstanceProvisionFailed}
steps:
  - id: create
    uses: fake.instance_create
    with: {instance_id: {from: run.instance_id}}
    emit:
      event: InstanceResourcesAllocated
      payload: {resource_ids: {from: create.resource_ids}}
  - id: boot
    uses: fake.instance_boot
    with: {instance_id: {from: run.instance_id}}
"""

DESTROY_YAML = """
workflow: destroy-instance
version: 1
inputs:
  instance_id: {type: str}
on_failure: report
outcome:
  succeeded: {event: InstanceDestroySucceeded}
  failed: {event: InstanceDestroyFailed}
  cancelled: {event: InstanceDestroyFailed}
steps:
  - id: teardown
    uses: fake.instance_destroy
    with: {instance_id: {from: run.instance_id}}
"""

CLUSTER_PROVISION_YAML = """
workflow: provision-fake
version: 1
inputs:
  cluster_id: {type: str}
on_failure: compensate
outcome:
  succeeded:
    event: ProvisionSucceeded
    payload: {public_ip: {from: make.address}, kubeconfig_ref: {from: make.kubeconfig_ref}}
  failed: {event: ProvisionFailed}
  cancelled: {event: ProvisionFailed}
steps:
  - id: make
    uses: fake.cluster_create
    with: {cluster_id: {from: run.cluster_id}}
"""


# ---------------------------------------------------------------------------
# fake verbs
# ---------------------------------------------------------------------------


class InstanceParams(BaseModel):
    instance_id: str


class CreateOutput(BaseModel):
    address: str
    resource_ids: Mapping[str, str]


class _Behaviour:
    """What a fake verb does when it runs. ``gate`` parks it on a PauseGate (a crash point),
    ``block`` parks it in a cancel-aware sleep (a run that is still going when something cancels
    it), ``subprocess`` runs a real ``sleep 30`` through ``ctx.run_subprocess``, and
    ``fail_times`` makes its first N calls raise a PermanentError."""

    def __init__(
        self,
        *,
        gate: PauseGate | None = None,
        block: bool = False,
        subprocess: bool = False,
        fail_times: int = 0,
        pause_undo: PauseGate | None = None,
    ) -> None:
        self.gate, self.block, self.subprocess = gate, block, subprocess
        self.fail_times, self.pause_undo = fail_times, pause_undo
        self.calls = 0
        self.entered = asyncio.Event()
        self.undo_calls: list[dict[str, str]] = []
        self.refused_cluster_id = False
        self.seen: list[tuple[str, str]] = []

    async def behave(self, ctx: StepContext, what: str) -> None:
        self.calls += 1
        self.seen.append((ctx.instance_id, ctx.workflow))
        try:
            ctx.cluster_id  # noqa: B018 -- an instance run must NOT be able to read a cluster id
        except ValueError:
            self.refused_cluster_id = True
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        if self.block:
            await ctx.sleep(3600)
        if self.subprocess:
            await ctx.run_subprocess(["sleep", "30"])
        if self.calls <= self.fail_times:
            raise PermanentError(f"{what} failed (call {self.calls})", code=ErrorCode.CAPACITY)


class InstanceCreateStep(_Behaviour, Step[InstanceParams, CreateOutput]):
    verb = "fake.instance_create"
    Params = InstanceParams
    Output = CreateOutput
    idempotent = True
    undoable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    async def execute(self, params: InstanceParams, ctx: StepContext) -> CreateOutput:
        await self.behave(ctx, "create")
        await ctx.note(vm=f"spi-{params.instance_id}")
        return CreateOutput(address="192.0.2.7", resource_ids={"vm": f"spi-{params.instance_id}"})

    async def undo(self, params, output, notes, ctx) -> None:
        if self.pause_undo is not None:
            await self.pause_undo.wait()
        self.undo_calls.append(dict(notes))


class InstanceBootStep(_Behaviour, Step[InstanceParams, EmptyOutput]):
    verb = "fake.instance_boot"
    Params = InstanceParams
    Output = EmptyOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    async def execute(self, params: InstanceParams, ctx: StepContext) -> EmptyOutput:
        await self.behave(ctx, "boot")
        return EmptyOutput()


class InstanceDestroyStep(_Behaviour, Step[InstanceParams, EmptyOutput]):
    verb = "fake.instance_destroy"
    Params = InstanceParams
    Output = EmptyOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    async def execute(self, params: InstanceParams, ctx: StepContext) -> EmptyOutput:
        await self.behave(ctx, "teardown")
        return EmptyOutput()


class ClusterParams(BaseModel):
    cluster_id: str


class ClusterCreateOutput(BaseModel):
    address: str
    kubeconfig_ref: str


class ClusterCreateStep(Step[ClusterParams, ClusterCreateOutput]):
    verb = "fake.cluster_create"
    Params = ClusterParams
    Output = ClusterCreateOutput
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self) -> None:
        self.cluster_ids: list[str] = []
        self.refused_instance_id = False

    async def execute(self, params: ClusterParams, ctx: StepContext) -> ClusterCreateOutput:
        self.cluster_ids.append(ctx.cluster_id)
        try:
            ctx.instance_id  # noqa: B018
        except ValueError:
            self.refused_instance_id = True
        return ClusterCreateOutput(address="203.0.113.5", kubeconfig_ref=f"cluster-kubeconfig:{params.cluster_id}")


# ---------------------------------------------------------------------------
# the stack
# ---------------------------------------------------------------------------


class RecordingSubprocessManager(SubprocessManager):
    """The real manager, plus a list of every process it was handed (a plain subclass, not a mock)."""

    def __init__(self) -> None:
        super().__init__()
        self.handed: list[asyncio.subprocess.Process] = []

    def register(self, process, **scope) -> None:
        self.handed.append(process)
        super().register(process, **scope)


@dataclass
class Stack:
    h: EngineHarness
    repos: Repositories
    dispatcher: Dispatcher
    executor: EffectExecutor
    timers: TimerService
    clock: FrozenClock
    hub: FakeHub
    create: InstanceCreateStep
    boot: InstanceBootStep
    destroy: InstanceDestroyStep
    cluster_step: ClusterCreateStep
    manager: Any = None
    run_ids: list[str] = field(default_factory=list)

    @property
    def uow(self):
        return self.h.uow


def _repos() -> Repositories:
    return Repositories(
        clusters=ClusterRepository(),
        deployments=DeploymentRepository(),
        cluster_state_audits=ClusterStateAuditRepository(),
        deployment_state_audits=DeploymentStateAuditRepository(),
        timers=TimerRepository(),
        outbox=OutboxRepository(),
        workflow_runs=WorkflowRunRepository(),
        instances=InstanceRepository(),
        instance_state_audits=InstanceStateAuditRepository(),
    )


def build_stack(
    tmp_path: Path,
    clock: FrozenClock,
    *,
    create: InstanceCreateStep | None = None,
    boot: InstanceBootStep | None = None,
    destroy: InstanceDestroyStep | None = None,
    manager: SubprocessManager | None = None,
) -> Stack:
    """One process's worth of runtime over ``tmp_path/engine.db``. Calling it again with the same
    ``tmp_path`` is a process restart: new objects, same database."""
    create, boot, destroy = create or InstanceCreateStep(), boot or InstanceBootStep(), destroy or InstanceDestroyStep()
    cluster_step = ClusterCreateStep()
    repos = _repos()
    h = build_engine(
        tmp_path,
        {
            "provision-instance-fake": PROVISION_YAML,
            "destroy-instance": DESTROY_YAML,
            "provision-fake": CLUSTER_PROVISION_YAML,
        },
        [create, boot, destroy, cluster_step],
        clock,
        dispatcher_factory=lambda uow: Dispatcher(uow, repos, clock),
        subprocess_manager=manager,
    )
    hub = FakeHub()
    executor = EffectExecutor(
        h.uow, repos, hub, h.engine, WorkflowDispatch(destroy_by_provider={"fake": "destroy-fake"}), clock,
        poll_interval=10.0,
    )
    timers = TimerService(h.uow, repos, h.dispatcher, clock, poll_interval=0.01)
    return Stack(h, repos, h.dispatcher, executor, timers, clock, hub, create, boot, destroy, cluster_step, manager)


async def birth(stack: Stack, instance_id: str = "i1", slug: str = "box", **overrides) -> None:
    async with stack.uow() as tx:
        owner = make_owner_key(tx)
    row = make_instance_row(
        instance_id, slug, owner, provider="fake", created_at=stack.clock.now(), updated_at=stack.clock.now(),
        expires_at=stack.clock.now() + HOUR, hard_expires_at=stack.clock.now() + 24 * HOUR, **overrides,
    )
    await stack.dispatcher.apply(
        "instance", instance_id, InstanceCreateRequested(at=stack.clock.now(), actor="api:alice"), record=row
    )


async def settle(stack: Stack, *, rounds: int = 60) -> None:
    """Drain the outbox and let every live run finish, until nothing is due and nothing is running.
    A row deferred into the future (the destroy waiting on a cancelled provision) is NOT due, so
    this returns with it still pending: the test advances the clock itself."""

    async def _go() -> None:
        for _ in range(rounds):
            await stack.executor.drain_pending()
            tasks = [handle.task for handle in list(stack.h.engine._runs.values())]  # noqa: SLF001
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                continue
            async with stack.uow() as tx:
                if not stack.repos.outbox.due(tx, stack.clock.now()):
                    return
        raise AssertionError("settle: still busy after the round limit")

    await asyncio.wait_for(_go(), timeout=20)


async def fire_due_timers(stack: Stack) -> None:
    await stack.timers.start()
    try:
        for _ in range(500):
            async with stack.uow() as tx:
                if not stack.repos.timers.due(tx, stack.clock.now()):
                    return
            await asyncio.sleep(0.005)
        raise AssertionError("a due timer never fired")
    finally:
        await stack.timers.stop()


async def instance(stack: Stack, instance_id: str = "i1"):
    async with stack.uow() as tx:
        return stack.repos.instances.get(tx, instance_id)


async def audits(stack: Stack, instance_id: str = "i1"):
    """Oldest first, as ``(from, to, event, actor)``."""
    async with stack.uow() as tx:
        rows = stack.repos.instance_state_audits.list_for_instance(tx, instance_id)
    return [(r.from_state, r.to_state, r.event, r.actor) for r in reversed(rows)]


async def runs(stack: Stack):
    async with stack.uow() as tx:
        return sorted(stack.repos.workflow_runs.list_all(tx), key=lambda r: (r.created_at, r.workflow))


async def timer_keys(stack: Stack):
    async with stack.uow() as tx:
        return {(t.aggregate_type, t.aggregate_id, t.timer_key): t for t in stack.repos.timers.list_all(tx)}


def _events(trail) -> list[str]:
    return [e for (_, _, e, _) in trail]


@pytest.fixture
def clock():
    return FrozenClock(NOW)


# ---------------------------------------------------------------------------
# provision: success and failure
# ---------------------------------------------------------------------------


async def test_provision_success_runs_an_instance_run_and_lands_every_event_on_the_instance(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)

    row = await instance(stack)
    assert row.status == "active"
    assert row.address == "192.0.2.7" and dict(row.resource_ids) == {"vm": "spi-i1"}
    assert row.version == 3

    (run,) = await runs(stack)
    assert (run.workflow, run.instance_id, run.cluster_id, run.deployment_id) == (
        "provision-instance-fake", "i1", None, None
    )
    assert run.status == "succeeded" and dict(run.args) == {"instance_id": "i1"}

    trail = await audits(stack)
    assert [(f, t, e) for f, t, e, _ in trail] == [
        ("new", "provisioning", "InstanceCreateRequested"),
        ("provisioning", "provisioning", "InstanceResourcesAllocated"),
        ("provisioning", "active", "InstanceProvisioned"),
    ]
    assert trail[1][3] == trail[2][3] == f"engine:run:{run.id}"  # the run's own actor

    assert stack.create.seen == [("i1", "provision-instance-fake")]
    assert stack.create.refused_cluster_id  # the step could read its instance id, and could not read a cluster's
    timers = await timer_keys(stack)
    assert set(timers) == {("instance", "i1", "expiry")}
    assert timers[("instance", "i1", "expiry")].fire_at == NOW + HOUR  # still armed, untouched
    async with stack.uow() as tx:
        assert tx.execute(text("SELECT COUNT(*) FROM clusters")).scalar() == 0
        assert tx.execute(text("SELECT COUNT(*) FROM cluster_state_audits")).scalar() == 0


async def test_the_runs_own_notifications_name_the_instance_not_a_cluster(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)

    (run,) = await runs(stack)
    job = [(t, d) for (t, d, _env) in stack.hub.calls if t in ("job_started", "job_completed")]
    assert [t for t, _ in job] == ["job_started", "job_completed"]
    for _, payload in job:
        assert payload == {"run_id": run.id, "workflow": "provision-instance-fake", "instance_id": "i1"}
    state = [(t, d) for (t, d, _env) in stack.hub.calls if t == "instance_state_changed"]
    assert [d["new_status"] for _, d in state] == ["provisioning", "provisioning", "active"]


async def test_provision_failure_compensates_marks_failed_and_keeps_the_expiry_armed(tmp_path, clock):
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(fail_times=1))
    await birth(stack)
    await settle(stack)

    row = await instance(stack)
    assert row.status == "failed" and "boot failed" in row.failure_reason
    assert row.failure_reason.startswith("permanent: ")
    (run,) = await runs(stack)
    assert run.status == "failed" and run.error["kind"] == "permanent"
    assert stack.create.undo_calls == [{"vm": "spi-i1"}]  # compensation ran, from the write-ahead notes

    trail = await audits(stack)
    assert trail[-1][:3] == ("provisioning", "failed", "InstanceProvisionFailed")
    # failure did not touch the expiry: no instance may exist without one
    assert ("instance", "i1", "expiry") in await timer_keys(stack)
    assert (await instance(stack)).expires_at == NOW + HOUR


async def test_a_failed_instance_is_destroyed_at_its_expiry(tmp_path, clock):
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(fail_times=1))
    await birth(stack)
    await settle(stack)
    assert (await instance(stack)).status == "failed"

    clock.advance(HOUR + timedelta(seconds=1))
    await fire_due_timers(stack)
    await settle(stack)

    assert (await instance(stack)).status == "destroyed"
    assert (await timer_keys(stack)) == {}


class EmptyAddressCreateStep(InstanceCreateStep):
    async def execute(self, params, ctx):
        await super().execute(params, ctx)
        return CreateOutput(address="", resource_ids={"vm": f"spi-{params.instance_id}"})


async def test_provision_success_with_an_empty_address_fails_the_instance_and_its_expiry_destroys_it(tmp_path, clock):
    stack = build_stack(tmp_path, clock, create=EmptyAddressCreateStep())
    await birth(stack)
    await settle(stack)

    row = await instance(stack)
    assert row.status == "failed" and row.address is None and row.failure_reason
    assert [r.status for r in await runs(stack)] == ["succeeded"]  # the run is done; the machine refused its report
    assert (await timer_keys(stack))[("instance", "i1", "expiry")].fire_at == NOW + HOUR

    clock.advance(HOUR + timedelta(seconds=1))
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"
    assert stack.destroy.calls == 1


# ---------------------------------------------------------------------------
# destroy / expiry while a provision is running
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("trigger", ["request", "expiry"])
async def test_destroy_or_expiry_during_provision_cancels_it_waits_for_it_then_destroys(tmp_path, clock, trigger):
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(block=True))
    await birth(stack)
    await stack.executor.drain_pending()  # admits the provision run and hands it to the engine
    await asyncio.wait_for(stack.boot.entered.wait(), timeout=5)
    assert (await instance(stack)).status == "provisioning"
    (prov,) = await runs(stack)
    assert prov.status == "running"

    if trigger == "request":
        await stack.dispatcher.apply(
            "instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice")
        )
        expected_actor = "api:alice"
    else:
        clock.advance(HOUR + timedelta(seconds=1))
        await fire_due_timers(stack)
        expected_actor = "timer:expiry"
    assert (await instance(stack)).status == "destroying"

    await settle(stack)  # cancels the provision, compensates it, and WAITS: the destroy row is deferred

    row = await instance(stack)
    assert row.status == "destroying"  # the cancelled provision's outcome did not move it
    (prov,) = await runs(stack)
    assert (prov.workflow, prov.status) == ("provision-instance-fake", "cancelled")
    assert prov.cancel_requested is True
    assert stack.create.undo_calls == [{"vm": "spi-i1"}]  # compensated before anything was destroyed
    async with stack.uow() as tx:
        assert stack.repos.workflow_runs.active_for_instance(tx, "i1") is None  # no run beside it, none yet
        waiting = [r for r in stack.repos.outbox.list_for_aggregate(tx, "instance", "i1") if r.kind == "run_workflow"]
    destroy_row = next(r for r in waiting if json.loads(r.payload)["workflow"] == "destroy")
    assert (destroy_row.status, destroy_row.attempts) == ("pending", 0)  # waiting is not failing
    assert stack.destroy.calls == 0

    clock.advance(timedelta(seconds=2))
    await settle(stack)

    row = await instance(stack)
    assert row.status == "destroyed" and row.destroyed_at is not None
    assert [(r.workflow, r.status) for r in await runs(stack)] == [
        ("provision-instance-fake", "cancelled"),
        ("destroy-instance", "succeeded"),
    ]
    trail = await audits(stack)
    # The cancelled provision's InstanceProvisionFailed met an instance already destroying: Ignore,
    # so it left no audit row and no version.
    assert "InstanceProvisionFailed" not in _events(trail)
    assert [(f, t, e) for f, t, e, _ in trail] == [
        ("new", "provisioning", "InstanceCreateRequested"),
        ("provisioning", "provisioning", "InstanceResourcesAllocated"),
        ("provisioning", "destroying", "InstanceDestroyRequested" if trigger == "request" else "InstanceExpired"),
        ("destroying", "destroyed", "InstanceDestroySucceeded"),
    ]
    assert trail[2][3] == expected_actor
    assert row.version == 4
    assert await timer_keys(stack) == {}


async def test_destroy_during_provision_never_runs_beside_the_provision(tmp_path, clock):
    """Sample the one-active-run slot at every step boundary: it never holds two runs, and the
    destroy run is never admitted while the provision run is live."""
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(block=True))
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(stack.boot.entered.wait(), timeout=5)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))

    seen_destroy_while_provision_live = False
    for _ in range(40):
        await stack.executor.drain_pending()
        async with stack.uow() as tx:
            live = tx.execute(
                text("SELECT workflow FROM workflow_runs WHERE instance_id = 'i1' AND status IN "
                     "('pending','running','blocked','compensating')")
            ).scalars().all()
        assert len(live) <= 1
        if "destroy-instance" in live and stack.create.undo_calls == []:
            seen_destroy_while_provision_live = True
        tasks = [handle.task for handle in list(stack.h.engine._runs.values())]  # noqa: SLF001
        if tasks:
            await asyncio.gather(*tasks)
        clock.advance(timedelta(seconds=2))
        if (await instance(stack)).status == "destroyed":
            break
    assert not seen_destroy_while_provision_live
    assert (await instance(stack)).status == "destroyed"


# ---------------------------------------------------------------------------
# destroy failure and retry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("retry", ["timer", "request"])
async def test_a_failed_destroy_is_retried_by_its_timer_or_by_request(tmp_path, clock, retry):
    stack = build_stack(tmp_path, clock, destroy=InstanceDestroyStep(fail_times=1))
    await birth(stack)
    await settle(stack)
    assert (await instance(stack)).status == "active"

    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await settle(stack)

    row = await instance(stack)
    assert row.status == "destroy-failed" and "teardown failed" in row.failure_reason
    retry_at = NOW + INSTANCE_DESTROY_RETRY_DELAY
    timers = await timer_keys(stack)
    assert timers[("instance", "i1", "expiry")].fire_at == retry_at  # a path back exists
    assert row.expires_at == NOW + HOUR  # the lease end does not move: only the timer does
    assert [r.status for r in await runs(stack) if r.workflow == "destroy-instance"] == ["failed"]

    if retry == "timer":
        clock.advance(INSTANCE_DESTROY_RETRY_DELAY + timedelta(seconds=1))
        await fire_due_timers(stack)
        retry_actor = "timer:expiry"
    else:
        await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:bob"))
        retry_actor = "api:bob"
    await settle(stack)

    row = await instance(stack)
    assert row.status == "destroyed" and row.destroyed_at is not None
    destroys = [r for r in await runs(stack) if r.workflow == "destroy-instance"]
    assert [r.status for r in destroys] == ["failed", "succeeded"]
    assert len({r.id for r in destroys}) == len({r.dedupe_key for r in destroys}) == 2  # two admissions
    assert stack.destroy.calls == 2
    trail = await audits(stack)
    assert [(f, t) for f, t, _, _ in trail][-4:] == [
        ("active", "destroying"), ("destroying", "destroy-failed"), ("destroy-failed", "destroying"),
        ("destroying", "destroyed"),
    ]
    assert trail[-2][3] == retry_actor
    assert await timer_keys(stack) == {}  # destroyed: nothing left to fire


async def test_a_destroy_that_keeps_failing_keeps_rearming_its_retry(tmp_path, clock):
    stack = build_stack(tmp_path, clock, destroy=InstanceDestroyStep(fail_times=3))
    await birth(stack)
    await settle(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await settle(stack)

    for attempt in range(2, 4):
        assert (await instance(stack)).status == "destroy-failed"
        assert ("instance", "i1", "expiry") in await timer_keys(stack)  # still a way back
        clock.advance(INSTANCE_DESTROY_RETRY_DELAY + timedelta(seconds=1))
        await fire_due_timers(stack)
        await settle(stack)
        assert stack.destroy.calls == attempt

    clock.advance(INSTANCE_DESTROY_RETRY_DELAY + timedelta(seconds=1))
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed" and stack.destroy.calls == 4


# ---------------------------------------------------------------------------
# crash replay: kill the run's task at a persistence point, restart, resume
# ---------------------------------------------------------------------------


async def test_crash_mid_provision_resumes_without_rerunning_or_reapplying_what_landed(tmp_path, clock):
    gate = PauseGate()
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(gate=gate))
    await birth(stack)
    await stack.executor.drain_pending()
    (run,) = await runs(stack)
    await crash_run(stack.h, run.id, gate)

    # At rest after the crash: create committed and its emit landed; boot is mid-flight; nothing terminal.
    row = await instance(stack)
    assert (row.status, row.version, dict(row.resource_ids)) == ("provisioning", 2, {"vm": "spi-i1"})
    async with stack.uow() as tx:
        assert stack.repos.workflow_runs.get(tx, run.id).status == "running"
        steps = {s.step_path: s.status for s in stack.h.step_repo.list_for_run(tx, run.id)}
    assert steps == {"create": "succeeded", "boot": "running"}

    stack2 = build_stack(tmp_path, clock)  # a restarted process: fresh objects, same database
    await stack2.executor.start()  # the boot-time drain
    await stack2.h.engine.resume_inflight()
    await settle(stack2)
    await stack2.executor.stop()

    row = await instance(stack2)
    assert (row.status, row.address) == ("active", "192.0.2.7")
    assert stack2.create.calls == 0  # succeeded before the crash: not run again
    assert stack2.boot.calls == 1
    trail = await audits(stack2)
    assert _events(trail).count("InstanceResourcesAllocated") == 1  # the emit was not applied twice
    assert _events(trail).count("InstanceProvisioned") == 1
    assert [r.status for r in await runs(stack2)] == ["succeeded"]  # the same run, not a second


async def test_crash_after_birth_before_admission_admits_exactly_one_run_on_restart(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)  # committed with its outbox rows; the process dies before any drain

    stack2 = build_stack(tmp_path, clock)
    await stack2.executor.start()
    await settle(stack2)
    await stack2.executor.stop()
    stack3 = build_stack(tmp_path, clock)  # and another restart, for good measure
    await stack3.executor.start()
    await stack3.h.engine.resume_inflight()
    await settle(stack3)
    await stack3.executor.stop()

    assert len(await runs(stack3)) == 1
    assert (await instance(stack3)).status == "active"
    assert stack3.create.calls == 0 and stack3.boot.calls == 0  # nothing re-ran after stack2 finished it


async def test_crash_after_admission_before_the_engine_started_it_is_picked_up_by_resume_inflight(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await stack.h.engine.stop()  # from here the engine adopts nothing: admission lands, hand-off does not
    await stack.executor.drain_pending()
    (run,) = await runs(stack)
    assert run.status == "pending" and run.instance_id == "i1"
    assert (await instance(stack)).status == "provisioning"

    stack2 = build_stack(tmp_path, clock)
    await stack2.executor.drain_pending()  # nothing to admit again
    await stack2.h.engine.resume_inflight()
    await settle(stack2)

    assert [(r.id, r.status) for r in await runs(stack2)] == [(run.id, "succeeded")]
    assert (await instance(stack2)).status == "active"


async def test_crash_mid_compensation_of_a_cancelled_provision_resumes_then_destroys_once(tmp_path, clock):
    undo_gate = PauseGate()
    stack = build_stack(
        tmp_path, clock, create=InstanceCreateStep(pause_undo=undo_gate), boot=InstanceBootStep(block=True)
    )
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(stack.boot.entered.wait(), timeout=5)
    (prov,) = await runs(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()  # cancel flips the run; the destroy row waits
    await crash_run(stack.h, prov.id, undo_gate)  # the run is inside its undo when the process dies

    async with stack.uow() as tx:
        assert stack.repos.workflow_runs.get(tx, prov.id).status == "compensating"
    assert stack.create.undo_calls == []  # it never finished
    assert (await instance(stack)).status == "destroying"

    stack2 = build_stack(tmp_path, clock)
    await stack2.executor.start()
    await stack2.h.engine.resume_inflight()
    await settle(stack2)
    clock.advance(timedelta(seconds=2))  # the destroy row's wait
    await settle(stack2)
    await stack2.executor.stop()

    assert stack2.create.undo_calls == [{"vm": "spi-i1"}]  # the unfinished undo was run, once
    assert [(r.workflow, r.status) for r in await runs(stack2)] == [
        ("provision-instance-fake", "cancelled"), ("destroy-instance", "succeeded"),
    ]
    assert (await instance(stack2)).status == "destroyed"
    trail = await audits(stack2)
    assert _events(trail).count("InstanceDestroySucceeded") == 1 and "InstanceProvisionFailed" not in _events(trail)
    assert stack2.destroy.calls == 1


async def test_crash_mid_destroy_resumes_and_destroys_exactly_once(tmp_path, clock):
    gate = PauseGate()
    stack = build_stack(tmp_path, clock, destroy=InstanceDestroyStep(gate=gate))
    await birth(stack)
    await settle(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()
    destroy_run = next(r for r in await runs(stack) if r.workflow == "destroy-instance")
    await crash_run(stack.h, destroy_run.id, gate)
    assert (await instance(stack)).status == "destroying"

    stack2 = build_stack(tmp_path, clock)
    await stack2.executor.start()
    await stack2.h.engine.resume_inflight()
    await settle(stack2)
    await stack2.executor.stop()

    assert (await instance(stack2)).status == "destroyed"
    assert _events(await audits(stack2)).count("InstanceDestroySucceeded") == 1
    assert [r.id for r in await runs(stack2) if r.workflow == "destroy-instance"] == [destroy_run.id]


# ---------------------------------------------------------------------------
# cancel terminates the run's subprocesses
# ---------------------------------------------------------------------------


async def test_cancelling_an_instance_run_kills_its_real_subprocess(tmp_path, clock):
    manager = RecordingSubprocessManager()
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(subprocess=True), manager=manager)
    await birth(stack)
    await stack.executor.drain_pending()
    for _ in range(400):
        if manager.handed:
            break
        await asyncio.sleep(0.01)
    (child,) = manager.handed
    assert child.returncode is None

    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await settle(stack)

    assert child.returncode is not None  # the group was signalled: `sleep 30` did not run to completion
    assert manager.active_count() == 0
    (prov, *_rest) = await runs(stack)
    assert prov.status == "cancelled"


# ---------------------------------------------------------------------------
# a cluster and an instance that share an id and a slug
# ---------------------------------------------------------------------------


async def test_a_cluster_and_an_instance_sharing_an_id_and_a_slug_do_not_cross(tmp_path, clock):
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(block=True))
    cluster_row = make_cluster_row(
        "same", "box", status="new", provider="fake", environment="ephemeral", expires_at=None, version=0
    )
    await stack.dispatcher.apply(
        "cluster", "same", CreateRequested(at=clock.now(), actor="api:alice"), record=cluster_row
    )
    await birth(stack, "same", "box")  # the same id string, the same slug

    await stack.executor.drain_pending()
    await asyncio.wait_for(stack.boot.entered.wait(), timeout=5)
    # the cluster's run finishes while the instance's is still blocked
    for _ in range(200):
        async with stack.uow() as tx:
            cluster_state = stack.repos.clusters.get(tx, "same").status
        if cluster_state == "active":
            break
        await asyncio.sleep(0.01)
    assert cluster_state == "active"

    by_workflow = {r.workflow: r for r in await runs(stack)}
    assert (by_workflow["provision-fake"].cluster_id, by_workflow["provision-fake"].instance_id) == ("same", None)
    inst_run = by_workflow["provision-instance-fake"]
    assert (inst_run.cluster_id, inst_run.instance_id) == (None, "same")
    assert stack.cluster_step.cluster_ids == ["same"] and stack.cluster_step.refused_instance_id
    assert stack.create.seen == [("same", "provision-instance-fake")]

    # cancel ONLY the instance's run: the cluster's finished run, and its state, are untouched
    await stack.dispatcher.apply("instance", "same", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    clock.advance(timedelta(seconds=2))
    for _ in range(3):
        await settle(stack)
        clock.advance(timedelta(seconds=2))

    async with stack.uow() as tx:
        cluster = stack.repos.clusters.get(tx, "same")
        cluster_audits = stack.repos.cluster_state_audits.list_for_cluster(tx, "same")
        cluster_run = stack.repos.workflow_runs.get(tx, by_workflow["provision-fake"].id)
    assert (cluster.status, cluster.public_ip, cluster.kubeconfig_ref) == (
        "active", "203.0.113.5", "cluster-kubeconfig:same"
    )
    assert cluster_run.status == "succeeded" and cluster_run.cancel_requested is False
    assert [(a.from_state, a.to_state) for a in sorted(cluster_audits, key=lambda a: a.id)] == [
        ("new", "provisioning"), ("provisioning", "active"),
    ]
    inst = await instance(stack, "same")
    assert inst.status == "destroyed"
    assert all(e.startswith("Instance") for e in _events(await audits(stack, "same")))
    assert not any(e.startswith("Instance") for e in [a.event for a in cluster_audits])
