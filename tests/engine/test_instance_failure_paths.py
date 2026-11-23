"""The ways an instance run can fail to report, and what keeps the instance destroyable (DR-0055).

The invariant: an instance that may hold a resource always has a working path to destruction, and
a destroy can only ever hit that instance's own VM. Each test below is a way that used to break it,
through the REAL Dispatcher, EffectExecutor, TimerService and WorkflowEngine over real tmp SQLite,
with the fake verbs of ``test_instance_lifecycle.py``:

- a provision row held back until after the instance was destroyed must not create a VM;
- a destroy row that goes ``dead`` must not strand the instance in ``destroying``;
- a run resumed with no definition must still report its outcome;
- a provision whose result the event refuses must end ``failed`` (compensated), not wedge;
- a delayed cancel for the provision must not cancel the destroy that took its slot.

No Mock/patch: the one double that is not a fake verb (``FlakyCancelEngine``) is a plain delegating
class, in the repo's convention (tests/engine/fakes.py).
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from datetime import timedelta

import pytest

from seedpod.core.clock import FrozenClock
from seedpod.core.events import (
    InstanceDestroyRequested,
    InstanceDestroySucceeded,
)
from seedpod.engine.dispatch_table import WorkflowDispatch
from seedpod.runtime.effect_executor import EffectExecutor
from tests.engine.fakes import PauseGate, crash_run
from tests.engine.test_instance_lifecycle import (
    HOUR,
    NOW,
    CreateOutput,
    InstanceBootStep,
    InstanceCreateStep,
    InstanceDestroyStep,
    audits,
    birth,
    build_stack,
    fire_due_timers,
    instance,
    runs,
    settle,
    timer_keys,
)


@pytest.fixture
def clock():
    return FrozenClock(NOW)


async def _outbox(stack, kinds=("run_workflow", "cancel_workflow")):
    async with stack.uow() as tx:
        return [r for r in stack.repos.outbox.list_for_aggregate(tx, "instance", "i1") if r.kind in kinds]


async def _drain_until_dead(stack, kind="run_workflow", workflow="destroy", rounds=12):
    """Drive the executor through its whole backoff ladder (its failures, with the clock moved
    past each retry) until the matching outbox row is dead, then let everything settle."""
    for _ in range(rounds):
        await settle(stack)
        rows = [r for r in await _outbox(stack, (kind,)) if json.loads(r.payload)["workflow"] == workflow]
        if rows and all(r.status == "dead" for r in rows):
            return rows
        stack.clock.advance(timedelta(minutes=11))
    raise AssertionError("the outbox row never went dead")


# ---------------------------------------------------------------------------
# 1. a provision row admitted after the instance was destroyed
# ---------------------------------------------------------------------------


async def test_a_delayed_provision_row_creates_no_vm_once_the_instance_is_destroyed(tmp_path, clock, caplog):
    stack = build_stack(tmp_path, clock)
    definition = stack.h.engine._definitions.pop("provision-instance-fake")  # noqa: SLF001
    await birth(stack)
    await settle(stack)  # admission fails (no definition): backed off, still pending
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"

    stack.h.engine._definitions["provision-instance-fake"] = definition  # noqa: SLF001
    with caplog.at_level(logging.WARNING, logger="seedpod.runtime.effect_executor"):
        clock.advance(timedelta(minutes=5))
        await settle(stack)

    assert stack.create.calls == 0  # nothing was created that nothing owns
    assert [(r.workflow, r.status) for r in await runs(stack)] == [("destroy-instance", "succeeded")]
    assert (await instance(stack)).status == "destroyed"
    provision_rows = [
        r for r in await _outbox(stack, ("run_workflow",)) if json.loads(r.payload)["workflow"] == "provision"
    ]
    assert [r.status for r in provision_rows] == ["done"]  # finished with no run, not retried for ever
    assert any("no longer provisioning" in r.getMessage() for r in caplog.records)
    assert await timer_keys(stack) == {}


async def test_a_delayed_destroy_row_runs_no_destroy_once_the_instance_is_no_longer_destroying(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    definition = stack.h.engine._definitions.pop("destroy-instance")  # noqa: SLF001
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await settle(stack)  # the destroy row cannot be admitted: backed off
    # The instance is reported destroyed some other way (another run's report) while the row waits.
    await stack.dispatcher.apply("instance", "i1", InstanceDestroySucceeded(at=clock.now(), actor="engine:run:other"))

    stack.h.engine._definitions["destroy-instance"] = definition  # noqa: SLF001
    clock.advance(timedelta(minutes=5))
    await settle(stack)

    assert stack.destroy.calls == 0
    assert [(r.workflow, r.status) for r in await runs(stack)] == [("provision-instance-fake", "succeeded")]
    assert [
        r.status for r in await _outbox(stack, ("run_workflow",)) if json.loads(r.payload)["workflow"] == "destroy"
    ] == ["done"]


# ---------------------------------------------------------------------------
# 2. a dead run row must not strand the instance
# ---------------------------------------------------------------------------


async def test_a_dead_destroy_row_sends_the_instance_to_destroy_failed_and_the_retry_destroys_it(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    definition = stack.h.engine._definitions.pop("destroy-instance")  # noqa: SLF001
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))

    (dead_row,) = await _drain_until_dead(stack)
    row = await instance(stack)
    assert dead_row.attempts == 8
    assert row.status == "destroy-failed"  # not `destroying` for ever
    assert "went dead" in row.failure_reason
    retry_timer = (await timer_keys(stack))[("instance", "i1", "expiry")]
    assert retry_timer.fire_at >= clock.now()  # a way back is armed
    assert row.expires_at <= row.hard_expires_at
    trail = await audits(stack)
    assert trail[-1][:3] == ("destroying", "destroy-failed", "InstanceDestroyFailed")
    assert trail[-1][3] == "executor:dead-row"

    stack.h.engine._definitions["destroy-instance"] = definition  # noqa: SLF001
    clock.advance(timedelta(hours=1))
    await fire_due_timers(stack)  # the retry timer
    await settle(stack)

    assert (await instance(stack)).status == "destroyed"
    assert stack.destroy.calls == 1  # only the retry ran a destroy
    rows = [r for r in await _outbox(stack, ("run_workflow",)) if json.loads(r.payload)["workflow"] == "destroy"]
    assert len(rows) == 2 and len({r.effect_id for r in rows}) == 2  # a fresh effect, not the dead row's identity
    assert sorted(r.status for r in rows) == ["dead", "done"]
    assert await timer_keys(stack) == {}


async def test_a_dead_destroy_row_can_also_be_retried_by_request(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    definition = stack.h.engine._definitions.pop("destroy-instance")  # noqa: SLF001
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await _drain_until_dead(stack)
    assert (await instance(stack)).status == "destroy-failed"

    stack.h.engine._definitions["destroy-instance"] = definition  # noqa: SLF001
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:bob"))
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"


async def test_a_dead_provision_row_fails_the_instance_and_leaves_its_expiry_armed(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    definition = stack.h.engine._definitions.pop("provision-instance-fake")  # noqa: SLF001
    await birth(stack)

    await _drain_until_dead(stack, workflow="provision")
    row = await instance(stack)
    assert row.status == "failed" and "went dead" in row.failure_reason
    assert row.expires_at == NOW + HOUR  # untouched
    assert (await timer_keys(stack))[("instance", "i1", "expiry")].fire_at == NOW + HOUR  # still armed

    stack.h.engine._definitions["provision-instance-fake"] = definition  # noqa: SLF001
    clock.advance(HOUR)
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"  # the expiry destroys it


async def test_a_dead_cluster_row_is_left_alone(tmp_path, clock):
    """The report is for an INSTANCE row. Nothing about a cluster's dead row changes."""
    from seedpod.core.events import CreateRequested
    from tests.runtime.conftest import make_cluster_row

    stack = build_stack(tmp_path, clock)
    definition = stack.h.engine._definitions.pop("provision-fake")  # noqa: SLF001
    row = make_cluster_row("c1", "demo", status="new", provider="fake", environment="ephemeral", expires_at=None, version=0)
    await stack.dispatcher.apply("cluster", "c1", CreateRequested(at=clock.now(), actor="api:alice"), record=row)
    for _ in range(12):
        await settle(stack)
        clock.advance(timedelta(minutes=11))
    async with stack.uow() as tx:
        dead = [r for r in stack.repos.outbox.list_for_aggregate(tx, "cluster", "c1") if r.status == "dead"]
        cluster = stack.repos.clusters.get(tx, "c1")
        audits_ = stack.repos.cluster_state_audits.list_for_cluster(tx, "c1")
    assert len(dead) == 1
    assert cluster.status == "provisioning"  # unchanged behaviour: reconciliation's surface
    assert [a.event for a in audits_] == ["CreateRequested"]
    assert definition is not None


# ---------------------------------------------------------------------------
# 3. a resumed run with no definition still reports
# ---------------------------------------------------------------------------


async def test_a_destroy_run_resumed_with_no_definition_ends_failed_and_the_instance_can_retry(tmp_path, clock):
    gate = PauseGate()
    stack = build_stack(tmp_path, clock, destroy=InstanceDestroyStep(gate=gate))
    await birth(stack)
    await settle(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()
    await asyncio.wait_for(stack.destroy.entered.wait(), 5)
    destroy_run = next(r for r in await runs(stack) if r.workflow == "destroy-instance")
    await crash_run(stack.h, destroy_run.id, gate)

    stack2 = build_stack(tmp_path, clock)  # a restarted process whose build lacks the definition
    stack2.h.engine._definitions.pop("destroy-instance")  # noqa: SLF001
    await stack2.h.engine.resume_inflight()
    await asyncio.gather(*[h.task for h in list(stack2.h.engine._runs.values())], return_exceptions=True)  # noqa: SLF001

    row = await instance(stack2)
    assert row.status == "destroy-failed"  # not `destroying` with nothing left to report it
    assert "unknown workflow" in row.failure_reason
    assert [r.status for r in await runs(stack2) if r.workflow == "destroy-instance"] == ["failed"]
    trail = await audits(stack2)
    assert trail[-1][:3] == ("destroying", "destroy-failed", "InstanceDestroyFailed")
    assert ("instance", "i1", "expiry") in await timer_keys(stack2)

    stack3 = build_stack(tmp_path, clock)  # the right build is back
    clock.advance(timedelta(hours=2))
    await fire_due_timers(stack3)
    await settle(stack3)
    assert (await instance(stack3)).status == "destroyed"


async def test_a_destroy_run_resumed_with_an_input_it_never_had_ends_failed_and_the_instance_can_retry(tmp_path, clock):
    """The args were frozen at admission, and a newer definition wants one more input. The run cannot
    start, and the instance (waiting in `destroying`) must still be told."""
    gate = PauseGate()
    stack = build_stack(tmp_path, clock, destroy=InstanceDestroyStep(gate=gate))
    await birth(stack)
    await settle(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()
    await asyncio.wait_for(stack.destroy.entered.wait(), 5)
    destroy_run = next(r for r in await runs(stack) if r.workflow == "destroy-instance")
    await crash_run(stack.h, destroy_run.id, gate)

    stack2 = build_stack(tmp_path, clock)
    engine = stack2.h.engine
    wf = engine._definitions["destroy-instance"]  # noqa: SLF001
    engine._definitions["destroy-instance"] = dataclasses.replace(  # noqa: SLF001
        wf, inputs={**wf.inputs, "extra": wf.inputs["instance_id"]}
    )
    await engine.resume_inflight()
    await asyncio.gather(*[h.task for h in list(engine._runs.values())], return_exceptions=True)  # noqa: SLF001

    row = await instance(stack2)
    assert row.status == "destroy-failed" and "missing declared input" in row.failure_reason
    assert ("instance", "i1", "expiry") in await timer_keys(stack2)


async def test_a_provision_run_resumed_with_no_definition_ends_failed_and_the_instance_is_failed(tmp_path, clock):
    gate = PauseGate()
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(gate=gate))
    await birth(stack)
    await stack.executor.drain_pending()
    (run,) = await runs(stack)
    await crash_run(stack.h, run.id, gate)

    stack2 = build_stack(tmp_path, clock)
    stack2.h.engine._definitions.pop("provision-instance-fake")  # noqa: SLF001
    await stack2.h.engine.resume_inflight()
    await asyncio.gather(*[h.task for h in list(stack2.h.engine._runs.values())], return_exceptions=True)  # noqa: SLF001

    row = await instance(stack2)
    assert row.status == "failed" and "unknown workflow" in row.failure_reason
    assert ("instance", "i1", "expiry") in await timer_keys(stack2)


# ---------------------------------------------------------------------------
# 4. a provision result the success event refuses
# ---------------------------------------------------------------------------


class EmptyAddressCreate(InstanceCreateStep):
    """Creates the VM and records it, then hands back an empty address, which ``InstanceProvisioned`` refuses."""

    async def execute(self, params, ctx):
        await self.behave(ctx, "create")
        await ctx.note(vm=f"spi-{params.instance_id}")
        return CreateOutput(address="", resource_ids={"vm": f"spi-{params.instance_id}"})


async def test_an_invalid_provision_result_ends_the_run_failed_compensated_and_the_instance_failed(tmp_path, clock):
    create = EmptyAddressCreate()
    stack = build_stack(tmp_path, clock, create=create)
    await birth(stack)
    await settle(stack)

    (run,) = await runs(stack)
    assert run.status == "failed"  # not `running` with no task
    assert not stack.h.engine.is_running(run.id)
    assert run.error["kind"] == "permanent" and "InstanceProvisioned" in run.error["message"]
    assert create.undo_calls == [{"vm": "spi-i1"}]  # the normal failure route: compensation ran

    row = await instance(stack)
    assert row.status == "failed" and "InstanceProvisioned" in row.failure_reason
    assert dict(row.resource_ids) == {"vm": "spi-i1"}  # the recorded resource is still on the row
    trail = await audits(stack)
    assert trail[-1][:3] == ("provisioning", "failed", "InstanceProvisionFailed")
    assert ("instance", "i1", "expiry") in await timer_keys(stack)

    clock.advance(HOUR + timedelta(seconds=1))
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"  # and it can be destroyed
    assert stack.destroy.calls == 1


async def test_an_invalid_provision_result_resumes_to_the_same_failed_end(tmp_path, clock):
    """A crash while the run compensates the invalid result resumes into the same `failed`."""
    create = EmptyAddressCreate()
    stack = build_stack(tmp_path, clock, create=create)
    await birth(stack)
    await settle(stack)
    stack2 = build_stack(tmp_path, clock, create=EmptyAddressCreate())
    await stack2.executor.start()
    await stack2.h.engine.resume_inflight()
    await settle(stack2)
    await stack2.executor.stop()
    assert [r.status for r in await runs(stack2)] == ["failed"]
    assert (await instance(stack2)).status == "failed"
    assert [e for (_, _, e, _) in await audits(stack2)].count("InstanceProvisionFailed") == 1


# ---------------------------------------------------------------------------
# 5. a delayed cancel for the provision must not cancel the destroy
# ---------------------------------------------------------------------------


class FlakyCancelEngine:
    """The real engine, except that the Nth ``cancel`` calls (1-based) raise once each."""

    def __init__(self, engine, fail_on: set[int]) -> None:
        self._engine, self._fail_on, self.calls = engine, fail_on, 0

    @property
    def definitions(self):
        return self._engine.definitions

    async def start(self, run_id: str) -> None:
        await self._engine.start(run_id)

    async def cancel(self, run_id: str) -> None:
        self.calls += 1
        if self.calls in self._fail_on:
            raise RuntimeError("transient")
        await self._engine.cancel(run_id)


async def test_a_delayed_provision_cancel_does_not_cancel_the_destroy_run_that_took_its_slot(tmp_path, clock):
    """The provision's compensation is slow, so the cancel row keeps failing while the provision run
    is still live and backs off past the moment the destroy run is admitted. When the old cancel is
    finally delivered, the live run is the DESTROY run, and it must not be cancelled."""
    undo_gate, destroy_gate = PauseGate(), PauseGate()
    stack = build_stack(
        tmp_path, clock,
        create=InstanceCreateStep(pause_undo=undo_gate), boot=InstanceBootStep(block=True),
        destroy=InstanceDestroyStep(gate=destroy_gate),
    )
    flaky = FlakyCancelEngine(stack.h.engine, fail_on={1, 3})
    stack.executor = EffectExecutor(
        stack.h.uow, stack.repos, stack.hub, flaky, WorkflowDispatch(destroy_by_provider={"fake": "destroy-fake"}),
        clock, poll_interval=10.0,
    )
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(stack.boot.entered.wait(), 5)

    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()  # cancel row: fails (1), backs off 1s. destroy row: cancels the provision (2), waits 2s
    await asyncio.wait_for(undo_gate.entered.wait(), 5)  # the provision is compensating, slowly

    clock.advance(timedelta(seconds=1))
    await stack.executor.drain_pending()  # cancel row again: the provision is still live, the cancel fails (3), backs off 5s
    clock.advance(timedelta(seconds=1))
    await stack.executor.drain_pending()  # destroy row again: still blocked, cancels again (4), waits 2s
    (cancel_row,) = await _outbox(stack, ("cancel_workflow",))
    assert (cancel_row.status, cancel_row.attempts) == ("pending", 2)

    undo_gate.release()
    await asyncio.gather(*[h.task for h in list(stack.h.engine._runs.values())], return_exceptions=True)  # noqa: SLF001
    clock.advance(timedelta(seconds=2))
    await stack.executor.drain_pending()  # the destroy run is admitted
    await asyncio.wait_for(stack.destroy.entered.wait(), 5)

    clock.advance(timedelta(seconds=10))
    await stack.executor.drain_pending()  # the old provision cancel is delivered, with the destroy run live
    destroy_gate.release()
    await settle(stack)

    destroy_run = next(r for r in await runs(stack) if r.workflow == "destroy-instance")
    assert destroy_run.status == "succeeded" and destroy_run.cancel_requested is False
    assert (await instance(stack)).status == "destroyed"
    assert stack.destroy.calls == 1
    assert [r.status for r in await _outbox(stack, ("cancel_workflow",))] == ["done"]
