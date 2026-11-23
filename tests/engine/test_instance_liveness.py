"""An instance that may hold a resource always has a WORKING path to destruction, without a restart
(DR-0055). The scenarios here are the ways an earlier build lost that path, each ported from the
checker's reproductions, plus the level-triggered ``InstanceLivenessSweep`` that is the guarantee
behind every edge-triggered repair.

Real Dispatcher, EffectExecutor, TimerService, WorkflowEngine and sweep over real tmp SQLite, with
the fake verbs of ``test_instance_lifecycle.py``. The clock is a ``FrozenClock``: nothing sleeps, and
the sweep's pass is driven by calling ``run_once()``. No Mock/patch. The few doubles are plain
classes: a delegating engine that never starts a run, and a repository whose ``mark_done`` fails
once, which is how the real failure ("database is locked") arrives.

Direct fixes (no sweep involved):

- a ``RunWorkflow`` row that already admitted a run is handed to the engine, never stale-dropped;
- an INSTANCE run whose step ignores its cancel is ended by the engine (``cancelled``, with its
  outcome event, its subprocesses killed), and nothing its straggling step does afterwards can write
  to it.

The sweep:

- a run with no task is adopted by the engine, once, and never given a second task;
- ``destroying`` / ``provisioning`` with no path is reported (dead row, undecodable row, failed
  report);
- a timer-waiting state with no armed timer gets it back, never past ``hard_expires_at``;
- one instance's failure does not stop the next, and the next pass retries.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import timedelta

import pytest
from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.core.codec import canonical_json, decode_event
from seedpod.core.events import (
    InstanceDestroyRequested,
    InstanceExpired,
    InstanceRunningObserved,
    InstanceStoppedObserved,
    InstanceTimerLost,
)
from seedpod.core.machine import INSTANCE_DESTROY_RETRY_DELAY
from seedpod.data.repositories import _iso
from seedpod.engine.dispatch_table import WorkflowDispatch
from seedpod.engine.step import EmptyOutput
from seedpod.runtime.effect_executor import EffectExecutor
from seedpod.runtime.instance_liveness import InstanceLivenessSweep, report_if_no_path
from tests.engine.fakes import PauseGate, crash_run
from tests.engine.test_instance_lifecycle import (
    HOUR,
    NOW,
    CreateOutput,
    InstanceBootStep,
    InstanceCreateStep,
    InstanceDestroyStep,
    RecordingSubprocessManager,
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


def sweep_of(stack) -> InstanceLivenessSweep:
    return InstanceLivenessSweep(stack.uow, stack.repos, stack.dispatcher, stack.h.engine, stack.clock, interval=0.01)


async def _outbox(stack, instance_id="i1", kinds=("run_workflow", "cancel_workflow")):
    async with stack.uow() as tx:
        return [r for r in stack.repos.outbox.list_for_aggregate(tx, "instance", instance_id) if r.kind in kinds]


async def _drain_until_dead(stack, workflow="destroy", rounds=12):
    for _ in range(rounds):
        await settle(stack)
        rows = [  # (a corrupted payload has no workflow: it is the row under test)
            r for r in await _outbox(stack, kinds=("run_workflow",)) if json.loads(r.payload).get("workflow", workflow) == workflow
        ]
        if rows and all(r.status == "dead" for r in rows):
            return rows
        stack.clock.advance(timedelta(minutes=11))
    raise AssertionError("the outbox row never went dead")


def _fail_mark_done_once(stack, workflow: str) -> dict:
    """The next ``mark_done`` of a ``run_workflow`` row for ``workflow`` raises once, as a locked
    database does. (``OutboxRepository`` is a frozen-free plain object, so the instance attribute shadows it.)"""
    real = stack.repos.outbox.mark_done
    state = {"failed": 0}

    def wrapper(t, effect_id, *, done_at):
        row = t.execute(text("SELECT kind, payload FROM effects_outbox WHERE effect_id = :e"), {"e": effect_id}).mappings().first()
        if (
            row is not None and row["kind"] == "run_workflow"
            and json.loads(row["payload"]).get("workflow") == workflow and state["failed"] == 0
        ):
            state["failed"] += 1
            raise RuntimeError("database is locked (injected)")
        return real(t, effect_id, done_at=done_at)

    object.__setattr__(stack.repos.outbox, "mark_done", wrapper)
    return state


class NeverStartsEngine:
    """The real engine, except that handing it a run does nothing: the executor's admission commits,
    and the hand-off is lost (the window ``resume_inflight`` exists for)."""

    def __init__(self, engine) -> None:
        self._engine = engine

    @property
    def definitions(self):
        return self._engine.definitions

    async def start(self, run_id: str) -> None:
        return None

    async def cancel(self, run_id: str) -> None:
        await self._engine.cancel(run_id)


async def _active_runs(stack):
    return [r for r in await runs(stack) if r.status in ("pending", "running", "blocked", "compensating")]


# ===========================================================================
# defect 3: an effect that already admitted a run hands it over, before any staleness check
# ===========================================================================


async def test_a_provision_row_that_already_admitted_a_run_hands_it_over_even_after_a_destroy_arrived(tmp_path, clock):
    """The admission committed the run, the done-mark failed, the engine was never handed it, and then
    the instance was destroyed. The replay of the provision row finds the instance `destroying`, which
    the staleness check would call overtaken, marking the row done and abandoning the run. The run is
    owed to the engine first, so the destroy's cancel finishes it and the destroy can proceed."""
    stack = build_stack(tmp_path, clock)
    state = _fail_mark_done_once(stack, "provision")
    await birth(stack)
    await stack.executor.drain_pending()
    assert state["failed"] == 1
    (admitted,) = await runs(stack)
    assert admitted.status == "pending" and not stack.h.engine.is_running(admitted.id)  # admitted, not handed over

    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    for _ in range(8):
        clock.advance(timedelta(seconds=3))
        await settle(stack)

    assert (await instance(stack)).status == "destroyed"  # no restart, no sweep
    by_workflow = {r.workflow: r for r in await runs(stack)}
    assert by_workflow["provision-instance-fake"].id == admitted.id  # the SAME run, not a second admission
    assert by_workflow["provision-instance-fake"].status == "cancelled"
    assert by_workflow["destroy-instance"].status == "succeeded"
    assert stack.create.calls == 0  # it was cancelled before its first step
    assert stack.destroy.calls == 1


async def test_a_destroy_row_replayed_after_its_admission_does_not_cancel_its_own_run(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    state = _fail_mark_done_once(stack, "destroy")
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()
    assert state["failed"] == 1
    clock.advance(timedelta(seconds=2))
    await settle(stack)

    (destroy,) = [r for r in await runs(stack) if r.workflow == "destroy-instance"]
    assert destroy.status == "succeeded" and destroy.cancel_requested is False  # it was not cancelled by its own replay
    assert (await instance(stack)).status == "destroyed"
    assert stack.destroy.calls == 1


# ===========================================================================
# defect 4: a run with no task, and a step that ignores its cancel
# ===========================================================================


class StubbornCreateStep(InstanceCreateStep):
    """A create step that ignores its cancel. It starts a real child process and registers it under
    its instance. Cancelled, it swallows the cancellation and carries on (`swallow`), or just never
    reaches a cancel point (`hold`). Released, it makes late writes (a note) and returns."""

    def __init__(self, *, swallow: bool) -> None:
        super().__init__()
        self.swallow = swallow
        self.hold = asyncio.Event()
        self.entered = asyncio.Event()
        self.child: asyncio.subprocess.Process | None = None
        self.finished = asyncio.Event()

    async def execute(self, params, ctx):
        self.calls += 1
        self.child = await asyncio.create_subprocess_exec("sleep", "30")
        ctx.services.subprocess_manager.register(self.child, instance_id=ctx.instance_id, run_id=str(ctx.run_id))
        self.entered.set()
        if self.swallow:
            while not self.hold.is_set():
                try:
                    await self.hold.wait()
                except asyncio.CancelledError:  # ignores the cancel
                    await asyncio.sleep(0)
        else:
            await self.hold.wait()
        await ctx.note(vm="late")  # a straggler's write to a run that has been finished
        await ctx.progress("late")
        self.finished.set()
        return CreateOutput(address="192.0.2.99", resource_ids={"vm": "late"})


@pytest.mark.parametrize("swallow", [False, True], ids=["task-cancelled", "cancel-swallowed"])
async def test_an_instance_run_whose_step_ignores_its_cancel_is_ended_so_the_destroy_can_proceed(tmp_path, clock, swallow):
    manager = RecordingSubprocessManager()
    create = StubbornCreateStep(swallow=swallow)
    stack = build_stack(tmp_path, clock, create=create, manager=manager)
    stack.h.engine._cancel_grace_seconds = 0.05  # noqa: SLF001
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(create.entered.wait(), 5)
    (prov,) = await runs(stack)
    assert manager.instance_process_count("i1") == 1

    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()  # cancel the provision; the destroy row waits

    for _ in range(100):  # the grace is real time (0.05s); the clock below is the fake one
        await asyncio.sleep(0.02)
        async with stack.uow() as tx:
            if stack.repos.workflow_runs.get(tx, prov.id).status == "cancelled":
                break
    async with stack.uow() as tx:
        ended = stack.repos.workflow_runs.get(tx, prov.id)
    assert ended.status == "cancelled" and ended.finished_at is not None and ended.error["kind"] == "cancelled"
    assert create.child.returncode is not None  # its subprocess was killed

    clock.advance(timedelta(seconds=3))
    await stack.executor.drain_pending()  # the destroy was waiting for the slot (`settle` would wait for the straggler too)
    for _ in range(200):
        if (await instance(stack)).status == "destroyed":
            break
        await asyncio.sleep(0.01)
    assert (await instance(stack)).status == "destroyed"  # without a restart and without a sweep
    assert stack.destroy.calls == 1
    trail = await audits(stack)
    assert [(f, t, e) for f, t, e, _ in trail][-2:] == [
        ("provisioning", "destroying", "InstanceDestroyRequested"),
        ("destroying", "destroyed", "InstanceDestroySucceeded"),
    ]

    # The straggler finally returns. It must not write to the finished run, resurrect it, or move the instance.
    create.hold.set()
    if swallow:  # (the other variant's task is dead: there is no straggler to come back)
        await asyncio.wait_for(create.finished.wait(), 5)
    await asyncio.sleep(0.05)
    async with stack.uow() as tx:
        after = stack.repos.workflow_runs.get(tx, prov.id)
        steps = {s.step_path: s for s in stack.h.step_repo.list_for_run(tx, prov.id)}
    assert (after.status, after.finished_at, after.error) == (ended.status, ended.finished_at, ended.error)
    assert steps["create"].status == "running"  # its success was never recorded
    assert dict(steps["create"].notes).get("vm") != "late"  # the late note was dropped
    assert dict(steps["create"].output or {}) == {}  # and its output never recorded
    assert (await instance(stack)).status == "destroyed"
    assert "InstanceResourcesAllocated" not in [e for _, _, e, _ in (await audits(stack))][-2:]
    assert not stack.h.engine.is_running(prov.id)


async def test_a_cluster_runs_hard_cancel_is_unchanged(tmp_path, clock):
    """The engine ends an INSTANCE run itself. A cluster run keeps HEAD's behaviour: its task is
    cancelled and its status is left alone."""
    from seedpod.core.events import CreateRequested
    from tests.runtime.conftest import make_cluster_row
    gate = PauseGate()
    stack = build_stack(tmp_path, clock)
    stack.cluster_step.execute = lambda params, ctx: gate.wait()  # type: ignore[method-assign]  # never returns
    stack.h.engine._cancel_grace_seconds = 0.05  # noqa: SLF001
    row = make_cluster_row("c1", "demo", status="new", provider="fake", environment="ephemeral", expires_at=None, version=0)
    await stack.dispatcher.apply("cluster", "c1", CreateRequested(at=clock.now(), actor="api:alice"), record=row)
    await stack.executor.drain_pending()
    await asyncio.wait_for(gate.entered.wait(), 5)
    (run,) = [r for r in await runs(stack) if r.cluster_id == "c1"]
    assert stack.h.engine.is_running(run.id)

    await stack.h.engine.cancel(run.id)
    for _ in range(100):
        await asyncio.sleep(0.02)
        if not stack.h.engine.is_running(run.id):
            break
    assert not stack.h.engine.is_running(run.id)  # the task is gone
    async with stack.uow() as tx:
        after = stack.repos.workflow_runs.get(tx, run.id)
    assert after.status == "running" and after.cancel_requested is True  # and the run is as it always was: not ended


async def test_a_run_whose_finalize_write_failed_is_adopted_by_the_sweep_without_a_restart(tmp_path, clock):
    """The run's work is done but its terminal write failed: `running`, no task, holding the slot."""
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    real = stack.h.engine._finalize  # noqa: SLF001
    failures = {"n": 0}

    async def flaky(state, status, **kw):
        if state.row.workflow == "destroy-instance" and failures["n"] == 0:
            failures["n"] += 1
            raise RuntimeError("database is locked (injected)")
        return await real(state, status, **kw)

    stack.h.engine._finalize = flaky  # noqa: SLF001
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await settle(stack)
    (destroy,) = [r for r in await runs(stack) if r.workflow == "destroy-instance"]
    assert destroy.status == "running" and not stack.h.engine.is_running(destroy.id)  # wedged: no task
    assert (await instance(stack)).status == "destroying"

    repairs = await sweep_of(stack).run_once()
    await settle(stack)

    assert [(r.kind, r.instance_id) for r in repairs] == [("adopted-run", "i1")]
    assert (await instance(stack)).status == "destroyed"
    assert [r.status for r in await runs(stack) if r.workflow == "destroy-instance"] == ["succeeded"]
    assert stack.destroy.calls == 1  # the step did not run again: it had already succeeded


async def test_a_run_whose_task_died_is_adopted_without_a_restart_and_never_gets_a_second_task(tmp_path, clock):
    gate = PauseGate()
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(gate=gate))
    await birth(stack)
    await stack.executor.drain_pending()
    (run,) = await runs(stack)
    await crash_run(stack.h, run.id, gate)  # the task is dead; the run is `running`
    sweep = sweep_of(stack)
    gate.release()

    assert [(r.kind) for r in await sweep.run_once()] == ["adopted-run"]
    assert stack.h.engine.is_running(run.id)
    # While it has a live task, passes leave it alone: no second task, no second boot.
    for _ in range(3):
        assert [r for r in await sweep.run_once() if r.kind == "adopted-run"] == [] or not stack.h.engine.is_running(run.id)
    await settle(stack)
    assert (await instance(stack)).status == "active"
    assert stack.boot.calls == 2  # the crashed attempt, and exactly one more
    assert [r.status for r in await runs(stack)] == ["succeeded"]


async def test_a_run_admitted_but_never_handed_over_is_adopted_once_and_the_late_hand_off_adds_nothing(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    stack.executor = EffectExecutor(
        stack.h.uow, stack.repos, stack.hub, NeverStartsEngine(stack.h.engine),
        WorkflowDispatch(destroy_by_provider={"fake": "destroy-fake"}), clock, poll_interval=10.0,
    )
    await birth(stack)
    await stack.executor.drain_pending()
    (run,) = await runs(stack)
    assert run.status == "pending" and not stack.h.engine.is_running(run.id)

    sweep = sweep_of(stack)
    results = await asyncio.gather(sweep.run_once(), sweep.run_once())  # two racing passes
    assert sum(len(r) for r in results) == 1  # one of them adopted it
    await stack.h.engine.start(run.id)  # the executor's own late hand-off
    await settle(stack)

    assert [r.status for r in await runs(stack)] == ["succeeded"]
    assert stack.create.calls == 1 and stack.boot.calls == 1  # one task, one execution
    assert (await instance(stack)).status == "active"


async def test_the_sweep_racing_the_executor_never_starts_a_second_run(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    sweep = sweep_of(stack)
    await birth(stack)
    for _ in range(3):
        await asyncio.gather(stack.executor.drain_pending(), sweep.run_once(), sweep.run_once())
        await settle(stack)
    assert len(await runs(stack)) == 1
    assert stack.create.calls == 1 and (await instance(stack)).status == "active"


# ===========================================================================
# defects 1 and 2: a dead or undecodable run row must not strand `destroying`
# ===========================================================================


async def test_a_failed_dead_row_report_is_repaired_by_the_sweep_not_a_restart(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    definition = stack.h.engine._definitions.pop("destroy-instance")  # noqa: SLF001
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    for _ in range(7):  # attempts 1..7
        await stack.executor.drain_pending()
        clock.advance(timedelta(minutes=11))
    async with stack.uow() as tx:  # the report's write fails: the death commit rolls back, the fallback marks it dead
        tx.execute(text("CREATE TRIGGER inj BEFORE UPDATE ON instances BEGIN SELECT RAISE(ABORT, 'injected'); END"))
    await stack.executor.drain_pending()
    async with stack.uow() as tx:
        tx.execute(text("DROP TRIGGER inj"))
    stack.h.engine._definitions["destroy-instance"] = definition  # noqa: SLF001

    assert [r.status for r in await _outbox(stack, kinds=("run_workflow",)) if json.loads(r.payload)["workflow"] == "destroy"] == ["dead"]
    assert (await instance(stack)).status == "destroying"  # stranded: the edge-triggered report was lost
    assert await _active_runs(stack) == []

    repairs = await sweep_of(stack).run_once()

    assert [(r.kind, r.instance_id) for r in repairs] == [("destroy-failed", "i1")]
    row = await instance(stack)
    assert row.status == "destroy-failed"
    assert (await timer_keys(stack))[("instance", "i1", "expiry")].fire_at == clock.now() + INSTANCE_DESTROY_RETRY_DELAY
    clock.advance(INSTANCE_DESTROY_RETRY_DELAY + timedelta(seconds=1))
    await fire_due_timers(stack)  # the retry the repair armed
    await settle(stack)
    assert (await instance(stack)).status == "destroyed" and stack.destroy.calls == 1


async def _corrupt_the_pending_destroy_row(stack) -> None:
    async with stack.uow() as tx:
        tx.execute(
            text("UPDATE effects_outbox SET payload = :p WHERE kind = 'run_workflow' AND status = 'pending'"),
            {"p": json.dumps({"kind": "run_workflow", "future_field": 1})},
        )


async def test_an_undecodable_destroy_row_is_dead_then_the_instance_is_reported_not_stranded(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await _corrupt_the_pending_destroy_row(stack)

    (dead,) = await _drain_until_dead(stack)
    assert dead.attempts == 8
    assert (await instance(stack)).status == "destroy-failed"  # reported from the instance's state: the payload is not needed
    clock.advance(INSTANCE_DESTROY_RETRY_DELAY + timedelta(seconds=1))
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"


async def test_an_undecodable_pending_destroy_row_is_not_a_path_so_the_sweep_does_not_wait_for_it_to_die(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await _corrupt_the_pending_destroy_row(stack)
    await stack.executor.drain_pending()  # attempt 1 fails: the row is pending, backed off
    assert (await instance(stack)).status == "destroying"

    repairs = await sweep_of(stack).run_once()

    assert [r.kind for r in repairs] == ["destroy-failed"]
    assert (await instance(stack)).status == "destroy-failed"
    clock.advance(INSTANCE_DESTROY_RETRY_DELAY + timedelta(seconds=1))
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed" and stack.destroy.calls == 1
    # The old row, dying later, must not move an instance that has long since moved on.
    for _ in range(10):
        clock.advance(timedelta(minutes=11))
        await settle(stack)
    assert (await instance(stack)).status == "destroyed"


async def test_a_dead_row_does_not_report_an_instance_that_still_has_a_path(tmp_path, clock):
    """`report_if_no_path` is the executor's immediate form of the repair. With a pending destroy row
    for the same instance there is a path, so nothing is applied."""
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    stack.h.engine._definitions.pop("destroy-instance")  # noqa: SLF001
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()  # the destroy row is pending (backed off)
    async with stack.uow() as tx:
        assert await report_if_no_path(
            stack.repos, stack.dispatcher, tx, "i1", clock=clock, reason="x", actor="executor:dead-row"
        ) is None
    assert (await instance(stack)).status == "destroying"


# ===========================================================================
# the sweep itself
# ===========================================================================


async def _mark_run_rows_done(stack) -> None:
    async with stack.uow() as tx:
        tx.execute(text("UPDATE effects_outbox SET status = 'done' WHERE kind IN ('run_workflow', 'cancel_workflow')"))


async def test_destroying_with_no_path_is_reported_and_its_retry_armed(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await _mark_run_rows_done(stack)  # the destroy row is gone: whatever lost it, nothing will start a destroy

    repairs = await sweep_of(stack).run_once()

    assert [(r.instance_id, r.kind) for r in repairs] == [("i1", "destroy-failed")]
    row = await instance(stack)
    assert row.status == "destroy-failed" and "liveness sweep" in row.failure_reason
    assert (await timer_keys(stack))[("instance", "i1", "expiry")].fire_at == NOW + INSTANCE_DESTROY_RETRY_DELAY
    assert (await audits(stack))[-1][2:] == ("InstanceDestroyFailed", "sweep:instance-liveness")


async def test_provisioning_with_no_path_is_failed_and_its_expiry_stays_armed(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await _mark_run_rows_done(stack)  # the provision row is gone, there is no run

    repairs = await sweep_of(stack).run_once()

    assert [r.kind for r in repairs] == ["provision-failed"]
    assert (await instance(stack)).status == "failed"
    assert (await timer_keys(stack))[("instance", "i1", "expiry")].fire_at == NOW + HOUR  # untouched
    clock.advance(HOUR + timedelta(seconds=1))
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"


async def test_an_instance_with_a_pending_row_a_live_task_or_an_armed_timer_is_left_alone(tmp_path, clock):
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(block=True))
    sweep = sweep_of(stack)
    await birth(stack, "pending-row", "a")
    assert await sweep.run_once() == []  # provisioning, a pending provision row
    await stack.executor.drain_pending()
    await asyncio.wait_for(stack.boot.entered.wait(), 5)
    assert await sweep.run_once() == []  # provisioning, a live run
    await stack.dispatcher.apply("instance", "pending-row", InstanceDestroyRequested(at=clock.now(), actor="api:a"))
    assert await sweep.run_once() == []  # destroying, a live (cancelling) run and a pending destroy row
    await settle(stack)
    clock.advance(timedelta(seconds=3))
    await settle(stack)
    assert (await instance(stack, "pending-row")).status == "destroyed"
    assert await sweep.run_once() == []  # destroyed: nothing to do


@pytest.mark.parametrize("state", ["active", "failed", "destroy-failed", "zombie"])
async def test_a_lost_expiry_timer_is_restored_through_the_machine(tmp_path, clock, state):
    stack = await _instance_in_state(tmp_path, clock, state)
    async with stack.uow() as tx:
        tx.execute(text("DELETE FROM timers"))
    assert await timer_keys(stack) == {}
    before = await instance(stack)

    repairs = await sweep_of(stack).run_once()

    assert [(r.kind, r.instance_id) for r in repairs] == [("timer-restored", "i1")]
    row = await instance(stack)
    assert row.status == state and row.version == before.version + 1  # audited, nothing else changed
    assert row.hard_expires_at == before.hard_expires_at and row.expires_at == before.expires_at
    timer = (await timer_keys(stack))[("instance", "i1", "expiry")]
    assert timer.fire_at <= row.hard_expires_at
    expected = clock.now() + INSTANCE_DESTROY_RETRY_DELAY if state == "destroy-failed" else row.expires_at
    assert timer.fire_at == expected
    assert (await audits(stack))[-1][2:] == ("InstanceTimerLost", "sweep:instance-liveness")
    assert await sweep_of(stack).run_once() == []  # idempotent: the second pass finds a path

    clock.advance(INSTANCE_DESTROY_RETRY_DELAY + timedelta(seconds=1) if state == "destroy-failed" else 2 * HOUR)
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"  # and the restored timer does its job


async def test_a_lost_timer_already_past_is_due_at_once_and_never_beyond_the_hard_limit(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    clock.advance(30 * HOUR)  # past the lease AND the hard limit (a day)
    async with stack.uow() as tx:
        tx.execute(text("DELETE FROM timers"))
    await sweep_of(stack).run_once()

    timer = (await timer_keys(stack))[("instance", "i1", "expiry")]
    row = await instance(stack)
    assert timer.fire_at == row.expires_at <= row.hard_expires_at < clock.now()  # past, so due on the next poll
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"


async def _instance_in_state(tmp_path, clock, state):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    if state in ("destroy-failed",):
        stack.destroy.fail_times = 1
        await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
        await settle(stack)
    elif state == "failed":
        stack.destroy.fail_times = 0
        await stack.dispatcher.apply("instance", "i1", InstanceStoppedObserved(at=clock.now(), actor="reconciler"))
    elif state == "zombie":
        await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
        await settle(stack)
        assert (await instance(stack)).status == "destroyed"
        await stack.dispatcher.apply("instance", "i1", InstanceRunningObserved(at=clock.now(), actor="reconciler"))
    assert (await instance(stack)).status == state
    return stack


async def test_one_failing_instance_does_not_stop_the_next_and_is_retried_on_the_next_pass(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    for iid, slug in (("a", "aa"), ("bad", "bb"), ("c", "cc")):
        await birth(stack, iid, slug)
    await settle(stack)
    async with stack.uow() as tx:
        tx.execute(text("DELETE FROM timers"))
        tx.execute(text("CREATE TRIGGER poison BEFORE UPDATE ON instances WHEN OLD.id = 'bad' BEGIN SELECT RAISE(ABORT, 'injected'); END"))
    sweep = sweep_of(stack)

    repairs = await sweep.run_once()  # 'bad' fails and is logged; 'a' and 'c' are still repaired

    assert sorted(r.instance_id for r in repairs) == ["a", "c"]
    keys = await timer_keys(stack)
    assert ("instance", "bad", "expiry") not in keys and ("instance", "a", "expiry") in keys
    async with stack.uow() as tx:
        tx.execute(text("DROP TRIGGER poison"))
    repairs = await sweep.run_once()  # the next pass retries it
    assert [r.instance_id for r in repairs] == ["bad"]
    assert ("instance", "bad", "expiry") in await timer_keys(stack)


async def test_the_sweep_runs_on_its_own_interval_and_stops_cleanly(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    async with stack.uow() as tx:
        tx.execute(text("DELETE FROM timers"))
    sweep = sweep_of(stack)  # interval 0.01s
    assert not sweep.running
    await sweep.start()
    await sweep.start()  # idempotent
    assert sweep.running
    try:
        for _ in range(200):
            if ("instance", "i1", "expiry") in await timer_keys(stack):
                break
            await asyncio.sleep(0.01)
        assert ("instance", "i1", "expiry") in await timer_keys(stack)
    finally:
        await sweep.stop()
    assert not sweep.running
    await sweep.stop()  # idempotent


async def test_a_zombie_slug_and_a_second_instance_are_each_swept_on_their_own(tmp_path, clock):
    """Instances are swept independently: one that has a path is not affected by one that has not."""
    stack = build_stack(tmp_path, clock)
    await birth(stack, "i1", "one")
    await birth(stack, "i2", "two")
    await settle(stack)
    async with stack.uow() as tx:
        tx.execute(text("DELETE FROM timers WHERE aggregate_id = 'i2'"))
    repairs = await sweep_of(stack).run_once()
    assert [(r.instance_id, r.kind) for r in repairs] == [("i2", "timer-restored")]
    assert (await instance(stack, "i1")).version == 3  # untouched


# ===========================================================================
# rework 3: a timer that can never fire, a poison row, a stale backstop, the window while a run is ended
# ===========================================================================

_BAD_TIMER_EVENTS = {
    "garbage-json": "{not json",
    "unknown-type": json.dumps({"type": "EventFromANewerBuild", "at": "2026-07-15T13:00:00.000Z", "actor": "x"}),
    "wrong-type": canonical_json(InstanceTimerLost(at=NOW, actor="x")),  # decodes, but is not an expiry
}


async def _set_expiry_event(stack, event_text: str, instance_id: str = "i1") -> None:
    async with stack.uow() as tx:
        tx.execute(
            text("UPDATE timers SET event = :e WHERE aggregate_id = :i AND timer_key = 'expiry'"),
            {"e": event_text, "i": instance_id},
        )


@pytest.mark.parametrize("bad", list(_BAD_TIMER_EVENTS))
async def test_a_timer_that_can_never_fire_is_not_a_path_and_the_repair_replaces_it(tmp_path, clock, bad):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    await _set_expiry_event(stack, _BAD_TIMER_EVENTS[bad])
    sweep = sweep_of(stack)

    repairs = await sweep.run_once()

    assert [(r.instance_id, r.kind) for r in repairs] == [("i1", "timer-restored")]
    timer = (await timer_keys(stack))[("instance", "i1", "expiry")]
    assert isinstance(decode_event(json.loads(timer.event)), InstanceExpired)  # the bad row was overwritten
    assert await sweep.run_once() == []  # and now it counts
    clock.advance(timedelta(hours=2))  # the repaired timer fires, and the instance is destroyed
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"


async def test_a_bad_timer_retried_by_the_timer_service_blocks_neither_its_repair_nor_a_sibling(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack, "i1", "one")
    await birth(stack, "i2", "two")
    await settle(stack)
    await _set_expiry_event(stack, "{not json", "i1")
    clock.advance(timedelta(hours=2))  # both timers are due, and past the hard-limit's reach for nothing else
    await stack.timers.start()  # the real service, polling every 10ms: it retries i1's bad row on every poll
    try:
        for _ in range(500):
            if (await instance(stack, "i2")).status != "active":
                break
            await asyncio.sleep(0.01)
        assert (await instance(stack, "i2")).status != "active"  # the sibling fired in spite of the bad row
        assert (await instance(stack, "i1")).status == "active"  # the bad one did not

        repairs = await sweep_of(stack).run_once()

        assert [(r.instance_id, r.kind) for r in repairs] == [("i1", "timer-restored")]
        for _ in range(500):
            if (await instance(stack, "i1")).status != "active":
                break
            await asyncio.sleep(0.01)
        assert (await instance(stack, "i1")).status != "active"  # the repaired timer fired
    finally:
        await stack.timers.stop()
    await settle(stack)
    assert [(await instance(stack, i)).status for i in ("i1", "i2")] == ["destroyed", "destroyed"]


async def _counts(stack):
    async with stack.uow() as tx:
        return (
            tx.execute(text("SELECT count(*) FROM instance_state_audits")).scalar(),
            tx.execute(text("SELECT count(*) FROM effects_outbox")).scalar(),
            tx.execute(text("SELECT version FROM instances WHERE id = 'i1'")).scalar(),
        )


async def test_a_stopped_timer_service_leaves_a_valid_overdue_timer_alone_for_any_number_of_passes(tmp_path, clock, caplog):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    sweep = sweep_of(stack)
    before = await _counts(stack)
    clock.advance(HOUR + timedelta(minutes=6))  # overdue past the warning threshold; nothing fires it
    with caplog.at_level(logging.WARNING, logger="seedpod.runtime.instance_liveness"):
        for _ in range(120):  # an hour of 30s passes
            assert await sweep.run_once() == []
            clock.advance(timedelta(seconds=30))
    assert await _counts(stack) == before  # no audit, no outbox row, no version bump
    assert len([r for r in caplog.records if "overdue" in r.getMessage()]) == 1  # once per instance per timer value
    await fire_due_timers(stack)  # the service comes back and fires it
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"
    assert "InstanceTimerLost" not in [e for _, _, e, _ in await audits(stack)]


async def test_a_healthy_timer_service_is_never_second_guessed_by_the_sweep(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    sweep = sweep_of(stack)
    clock.advance(timedelta(days=3))  # the process was down for days: every timer is long overdue...
    await fire_due_timers(stack)  # ...and the service fires it on its first poll
    assert await sweep.run_once() == []
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"
    assert "InstanceTimerLost" not in [e for _, _, e, _ in await audits(stack)]


async def _set_expiry_fire_at(stack, fire_at: str, instance_id: str = "i1") -> None:
    async with stack.uow() as tx:
        tx.execute(
            text("UPDATE timers SET fire_at = :f WHERE aggregate_id = :i AND timer_key = 'expiry'"),
            {"f": fire_at, "i": instance_id},
        )


@pytest.mark.parametrize(
    "bad_fire_at",
    ["2020-bad", "2026-07-15T13:00:00", "2099-01-01T00:00:00+00:00", "20260715T130000+00:00", "2026-W29-3T13:00:00+00:00"],
    ids=["unparseable", "naive", "beyond-the-hard-limit", "compact-sorts-after-now", "week-sorts-after-now"],
)
async def test_a_timer_with_a_bad_fire_at_is_repaired_once_and_then_the_sweep_writes_nothing(tmp_path, clock, bad_fire_at):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    await _set_expiry_fire_at(stack, bad_fire_at)
    sweep = sweep_of(stack)

    assert [(r.instance_id, r.kind) for r in await sweep.run_once()] == [("i1", "timer-restored")]
    after_repair = await _counts(stack)
    for _ in range(5):
        assert await sweep.run_once() == []
    assert await _counts(stack) == after_repair
    clock.advance(timedelta(hours=2))
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"


async def test_a_destroy_failed_retry_armed_after_the_hard_limit_is_a_path(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    async with stack.uow() as tx:
        hard = stack.repos.instances.load(tx, "i1").hard_expires_at
        tx.execute(text("UPDATE instances SET status = 'destroy-failed' WHERE id = 'i1'"))
    clock.advance((hard - clock.now()) + timedelta(hours=1))  # the failure happens after the hard limit
    retry_at = clock.now() + INSTANCE_DESTROY_RETRY_DELAY
    await _set_expiry_fire_at(stack, _iso(retry_at))
    sweep = sweep_of(stack)
    assert await sweep.run_once() == []
    await _set_expiry_fire_at(stack, _iso(retry_at + timedelta(minutes=1)))  # later than any retry
    assert [r.kind for r in await sweep.run_once()] == ["timer-restored"]


@pytest.mark.parametrize("aggregate_type", ["cluster", "deployment"])
async def test_a_corrupt_cluster_or_deployment_timer_still_raises_in_the_scan(tmp_path, clock, aggregate_type):
    stack = build_stack(tmp_path, clock)
    async with stack.uow() as tx:
        tx.execute(
            text(
                "INSERT INTO timers (aggregate_type, aggregate_id, timer_key, fire_at, event, created_by_effect) "
                "VALUES (:t, 'x', 'ttl', '2020-bad', '{}', 'test')"
            ),
            {"t": aggregate_type},
        )
    async with stack.uow() as tx:
        with pytest.raises(ValueError):
            stack.repos.timers.due(tx, clock.now())


async def test_a_corrupt_instance_timer_does_not_stop_a_healthy_one_once_the_sweep_repairs_it(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack, "i1", "one")
    await birth(stack, "i2", "two")
    await settle(stack)
    await _set_expiry_fire_at(stack, "2020-bad", "i1")  # sorts before any real time
    clock.advance(timedelta(hours=2))
    async with stack.uow() as tx:
        assert [r.aggregate_id for r in stack.repos.timers.due(tx, clock.now())] == ["i2"]  # i1 is skipped
    await stack.timers.start()
    try:
        for _ in range(500):  # the healthy instance expires with the corrupt row still there
            if (await instance(stack, "i2")).status != "active":
                break
            await asyncio.sleep(0.01)
        assert stack.timers.running
        assert (await instance(stack, "i2")).status != "active"
        assert (await instance(stack, "i1")).status == "active"
        assert [(r.instance_id, r.kind) for r in await sweep_of(stack).run_once()] == [("i1", "timer-restored")]
        for _ in range(500):
            if [(await instance(stack, i)).status for i in ("i1", "i2")].count("active") == 0:
                break
            await asyncio.sleep(0.01)
    finally:
        await stack.timers.stop()
    await settle(stack)
    assert [(await instance(stack, i)).status for i in ("i1", "i2")] == ["destroyed", "destroyed"]
    assert [r.kind for r in await sweep_of(stack).run_once()] == []  # no restart, nothing left to repair


@pytest.mark.parametrize(
    "poison_sql",
    ["UPDATE instances SET resource_ids = '{bad' WHERE id = 'i1'", "UPDATE instances SET expires_at = 'not a date' WHERE id = 'i1'"],
    ids=["resource_ids", "expires_at"],
)
async def test_one_undecodable_instance_row_does_not_disable_the_sweep_for_the_others(tmp_path, clock, caplog, poison_sql):
    stack = build_stack(tmp_path, clock)
    await birth(stack, "i1", "one")
    await birth(stack, "i2", "two")
    await settle(stack)
    async with stack.uow() as tx:
        tx.execute(text("DELETE FROM timers WHERE aggregate_id = 'i2'"))  # i2 needs a repair
        tx.execute(text(poison_sql))  # i1 cannot be read at all, and sorts first
    sweep = sweep_of(stack)

    with caplog.at_level(logging.ERROR, logger="seedpod.runtime.instance_liveness"):
        first = await sweep.run_once()  # did not raise
        second = await sweep.run_once()

    assert [(r.instance_id, r.kind) for r in first] == [("i2", "timer-restored")]
    assert second == []
    assert ("instance", "i2", "expiry") in await timer_keys(stack)
    complaints = [r for r in caplog.records if r.levelno >= logging.ERROR and "i1" in r.getMessage()]
    assert len(complaints) == 2  # logged at ERROR, with its id, on EVERY pass


class SwallowingCreateStep(InstanceCreateStep):
    """Ignores its cancel token AND swallows ``CancelledError`` until released: a run that only the
    engine's hard cancel can end."""

    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.swallowed = 0

    async def execute(self, params, ctx):
        self.calls += 1
        self.entered.set()
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.swallowed += 1
        await ctx.note(vm="late")
        return CreateOutput(address="192.0.2.9", resource_ids={"vm": "late"})


def _backstops() -> list[asyncio.Task]:
    return [t for t in asyncio.all_tasks() if getattr(t.get_coro(), "__name__", "") == "_hard_cancel_backstop"]


async def _wait_run_status(stack, run_id: str, status: str) -> None:
    for _ in range(500):
        async with stack.uow() as tx:
            if stack.repos.workflow_runs.get(tx, run_id).status == status:
                return
        await asyncio.sleep(0.01)
    raise AssertionError(f"run {run_id} never became {status}")


async def _straggler_ended_and_destroy_running(tmp_path, clock):
    """A provision run whose step swallows its cancel is ended by the engine; the destroy then runs a
    real child process. The straggler's task is still registered. Returns what a test needs."""
    manager = RecordingSubprocessManager()
    create = SwallowingCreateStep()
    destroy = InstanceDestroyStep(subprocess=True)
    stack = build_stack(tmp_path, clock, create=create, destroy=destroy, manager=manager)
    stack.h.engine._cancel_grace_seconds = 0.2  # noqa: SLF001
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(create.entered.wait(), 5)
    (prov,) = await runs(stack)
    try:
        await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
        await stack.executor.drain_pending()  # the cancel; the destroy row waits for the slot
        await _wait_run_status(stack, prov.id, "cancelled")  # the backstop ended it (task.cancel(), swallowed)
        clock.advance(timedelta(seconds=3))
        await stack.executor.drain_pending()  # the destroy is admitted, and spawns `sleep 30`
        for _ in range(500):
            if manager.instance_process_count("i1"):
                break
            await asyncio.sleep(0.01)
        assert manager.instance_process_count("i1") == 1
        assert stack.h.engine.is_running(prov.id)  # the straggler lives on
    except BaseException:
        await _let_go(stack, manager, create)  # (the swallowing step would hang the loop's teardown)
        raise
    return stack, manager, create, prov


async def _let_go(stack, manager, create) -> None:
    create.release.set()
    for p in manager.handed:
        if p.returncode is None:
            p.kill()
    await asyncio.gather(*[h.task for h in list(stack.h.engine._runs.values())], return_exceptions=True)  # noqa: SLF001


async def test_a_cancel_after_the_run_was_ended_adds_no_backstop_to_kill_the_next_runs_subprocess(tmp_path, clock):
    """The destroy row's retry cancels the provision run again, once it is already ended."""
    stack, manager, create, prov = await _straggler_ended_and_destroy_running(tmp_path, clock)
    (child,) = manager.handed
    swallowed = create.swallowed
    try:
        await stack.h.engine.cancel(prov.id)
        await asyncio.sleep(0.5)  # well past the 0.2s grace

        assert child.returncode is None  # the destroy's subprocess is alive
        assert create.swallowed == swallowed  # and the straggler was not cancelled again
    finally:
        await _let_go(stack, manager, create)


async def test_a_backstop_for_a_run_that_is_no_longer_active_does_nothing(tmp_path, clock):
    """Even a backstop that exists (one made before the run was ended) checks the run first."""
    stack, manager, create, prov = await _straggler_ended_and_destroy_running(tmp_path, clock)
    (child,) = manager.handed
    handle = stack.h.engine._runs[prov.id]  # noqa: SLF001
    swallowed = create.swallowed
    try:
        await stack.h.engine._hard_cancel_backstop(handle, prov.id)  # noqa: SLF001 -- waits out the grace, then looks

        assert child.returncode is None and create.swallowed == swallowed
    finally:
        await _let_go(stack, manager, create)


async def test_ending_a_run_kills_only_that_runs_processes_not_the_instances_others(tmp_path, clock):
    manager = RecordingSubprocessManager()
    create = SwallowingCreateStep()
    stack = build_stack(tmp_path, clock, create=create, manager=manager)
    stack.h.engine._cancel_grace_seconds = 0.05  # noqa: SLF001
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(create.entered.wait(), 5)
    (prov,) = await runs(stack)
    mine = await asyncio.create_subprocess_exec("sleep", "30")
    other_runs = await asyncio.create_subprocess_exec("sleep", "30")
    manager.register(mine, instance_id="i1", run_id=prov.id)
    manager.register(other_runs, instance_id="i1", run_id="another-run-of-the-same-instance")
    try:
        await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
        await stack.executor.drain_pending()
        await _wait_run_status(stack, prov.id, "cancelled")

        assert mine.returncode is not None  # this run's process was killed
        assert other_runs.returncode is None  # another run's, on the same instance, was not
    finally:
        await _let_go(stack, manager, create)


async def test_repeated_cancels_of_one_instance_run_make_one_backstop(tmp_path, clock):
    create = SwallowingCreateStep()
    stack = build_stack(tmp_path, clock, create=create)
    stack.h.engine._cancel_grace_seconds = 60  # noqa: SLF001
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(create.entered.wait(), 5)
    (prov,) = await runs(stack)

    try:
        for _ in range(4):
            await stack.h.engine.cancel(prov.id)

        assert len(_backstops()) == 1
    finally:
        for task in _backstops():
            task.cancel()
        create.release.set()
        await asyncio.gather(*[h.task for h in list(stack.h.engine._runs.values())], return_exceptions=True)  # noqa: SLF001


class GatedTerminateManager(RecordingSubprocessManager):
    """The real manager, except that a kill (by run or by instance) parks until the test lets it go:
    the window in which the engine is ending a run."""

    def __init__(self) -> None:
        super().__init__()
        self.terminating = asyncio.Event()
        self.proceed = asyncio.Event()

    async def terminate_for_run(self, run_id, **kw):
        self.terminating.set()
        await self.proceed.wait()
        return await super().terminate_for_run(run_id, **kw)

    async def terminate_for_instance(self, instance_id, **kw):
        self.terminating.set()
        await self.proceed.wait()
        return await super().terminate_for_instance(instance_id, **kw)


class TokenIgnoringBootStep(InstanceBootStep):
    """Ignores its cancel token, honours ``CancelledError``: the engine's hard cancel stops it."""

    async def execute(self, params, ctx):
        self.calls += 1
        self.entered.set()
        await asyncio.sleep(3600)
        return EmptyOutput()


async def _open_the_ending_window(tmp_path, clock):
    """The hard cancel has cancelled the provision run's task and is parked in the kill: the run is
    still `running` in the database and has no task."""
    manager = GatedTerminateManager()
    boot = TokenIgnoringBootStep()
    stack = build_stack(tmp_path, clock, boot=boot, manager=manager)
    stack.h.engine._cancel_grace_seconds = 0.05  # noqa: SLF001
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(boot.entered.wait(), 5)
    (prov,) = await runs(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()
    await asyncio.wait_for(manager.terminating.wait(), 5)
    for _ in range(200):
        if not stack.h.engine.is_running(prov.id):
            break
        await asyncio.sleep(0.01)
    assert not stack.h.engine.is_running(prov.id)
    assert (await runs(stack))[0].status == "running" and stack.h.engine.is_ending(prov.id)
    return stack, manager, prov


async def test_a_run_being_ended_by_the_hard_cancel_is_not_adopted_and_its_compensation_does_not_run(tmp_path, clock):
    stack, manager, prov = await _open_the_ending_window(tmp_path, clock)

    assert await sweep_of(stack).run_once() == []  # a path: the engine is finishing it
    assert await stack.h.engine.resume_run(prov.id) is False
    await stack.h.engine.start(prov.id)  # a replayed hand-off
    assert not stack.h.engine.is_running(prov.id)

    manager.proceed.set()  # the kill finishes, the run is ended
    await _wait_run_status(stack, prov.id, "cancelled")
    assert not stack.h.engine.is_ending(prov.id)
    assert stack.create.undo_calls == []  # no second task ran the compensation
    clock.advance(timedelta(seconds=3))
    await settle(stack)  # the destroy follows, once, with no overlap
    assert (await instance(stack)).status == "destroyed"
    assert stack.destroy.calls == 1


@pytest.mark.parametrize("recover", ["resume_inflight", "sweep"])
async def test_a_crash_while_a_run_is_being_ended_is_recovered_after_restart(tmp_path, clock, recover):
    stack, manager, prov = await _open_the_ending_window(tmp_path, clock)
    for task in _backstops():  # the process dies: the backstop's kill never finishes, nothing is written
        task.cancel()
    await asyncio.sleep(0.05)
    assert not stack.h.engine.is_ending(prov.id)
    assert (await runs(stack))[0].status == "running"

    stack2 = build_stack(tmp_path, clock)  # a restarted process: fresh objects, same database
    if recover == "sweep":
        assert [r.kind for r in await sweep_of(stack2).run_once()] == ["adopted-run"]
    else:
        await stack2.h.engine.resume_inflight()
    for _ in range(8):  # the destroy row retries on its backoff until the slot is free
        await settle(stack2)
        clock.advance(timedelta(seconds=3))
    await settle(stack2)

    by_workflow = {r.workflow: r for r in await runs(stack2)}
    assert by_workflow["provision-instance-fake"].status == "cancelled"
    assert by_workflow["destroy-instance"].status == "succeeded"
    assert (await instance(stack2)).status == "destroyed"
    assert stack2.destroy.calls == 1
