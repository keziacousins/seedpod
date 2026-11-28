"""The instance liveness sweep is the only recovery for an instance (DR-0055): an instance that may
hold a resource always gets a way to destruction, without a restart.

Real Dispatcher, EffectExecutor, TimerService, WorkflowEngine and sweep over real tmp SQLite, with
the fake verbs of ``test_instance_lifecycle.py``. The clock is a ``FrozenClock`` and the sweep is
driven by ``run_once()``. No Mock/patch.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest
from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.core.events import InstanceDestroyRequested, InstanceDestroySucceeded
from seedpod.runtime.instance_liveness import InstanceLivenessSweep
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


def sweep_of(stack, engine=None) -> InstanceLivenessSweep:
    return InstanceLivenessSweep(
        stack.uow, stack.repos, stack.dispatcher, engine or stack.h.engine, stack.clock, interval=0.01
    )


async def _run_rows(stack, workflow):
    async with stack.uow() as tx:
        rows = stack.repos.outbox.list_for_aggregate(tx, "instance", "i1")
    return [r for r in rows if r.kind == "run_workflow" and json.loads(r.payload)["workflow"] == workflow]


async def _drain_until_dead(stack, workflow):
    """Drive the executor through its backoff ladder, the clock moved past each retry, until the row is dead."""
    for _ in range(12):
        await settle(stack)
        rows = await _run_rows(stack, workflow)
        if rows and all(r.status == "dead" for r in rows):
            return
        stack.clock.advance(timedelta(minutes=11))
    raise AssertionError("the outbox row never went dead")


# ---------------------------------------------------------------------------
# admission: a row that was overtaken starts nothing
# ---------------------------------------------------------------------------


async def test_a_delayed_provision_row_creates_no_vm_once_the_instance_is_destroyed(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    definition = stack.h.engine._definitions.pop("provision-instance-fake")  # noqa: SLF001
    await birth(stack)
    await settle(stack)  # admission fails (no definition): backed off, still pending
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"

    stack.h.engine._definitions["provision-instance-fake"] = definition  # noqa: SLF001
    clock.advance(timedelta(minutes=5))
    await settle(stack)

    assert stack.create.calls == 0  # nothing was created that nothing owns
    assert [(r.workflow, r.status) for r in await runs(stack)] == [("destroy-instance", "succeeded")]
    assert [r.status for r in await _run_rows(stack, "provision")] == ["done"]  # finished with no run


async def test_a_delayed_destroy_row_runs_no_destroy_once_the_instance_is_no_longer_destroying(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    definition = stack.h.engine._definitions.pop("destroy-instance")  # noqa: SLF001
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await settle(stack)  # the destroy row cannot be admitted: backed off
    await stack.dispatcher.apply("instance", "i1", InstanceDestroySucceeded(at=clock.now(), actor="engine:run:other"))

    stack.h.engine._definitions["destroy-instance"] = definition  # noqa: SLF001
    clock.advance(timedelta(minutes=5))
    await settle(stack)

    assert stack.destroy.calls == 0
    assert [r.status for r in await _run_rows(stack, "destroy")] == ["done"]


# ---------------------------------------------------------------------------
# a dead row, a run that ended with no outcome
# ---------------------------------------------------------------------------


async def test_a_dead_destroy_row_is_sent_to_destroy_failed_by_the_sweep_and_the_retry_destroys_it(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    definition = stack.h.engine._definitions.pop("destroy-instance")  # noqa: SLF001
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await _drain_until_dead(stack, "destroy")
    assert (await instance(stack)).status == "destroying"  # stranded: nothing reports it yet

    assert await sweep_of(stack).run_once() == ["i1"]
    row = await instance(stack)
    assert row.status == "destroy-failed" and ("instance", "i1", "expiry") in await timer_keys(stack)

    stack.h.engine._definitions["destroy-instance"] = definition  # noqa: SLF001
    clock.advance(timedelta(hours=1))
    await fire_due_timers(stack)  # the retry timer: no restart
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"
    assert stack.destroy.calls == 1
    assert await timer_keys(stack) == {}


async def test_a_dead_provision_row_fails_the_instance_and_its_expiry_destroys_it(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    definition = stack.h.engine._definitions.pop("provision-instance-fake")  # noqa: SLF001
    await birth(stack)
    await _drain_until_dead(stack, "provision")

    assert await sweep_of(stack).run_once() == ["i1"]
    assert (await instance(stack)).status == "failed"
    assert (await timer_keys(stack))[("instance", "i1", "expiry")].fire_at == NOW + HOUR  # still armed

    stack.h.engine._definitions["provision-instance-fake"] = definition  # noqa: SLF001
    clock.advance(HOUR)
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"


async def test_a_run_that_ended_with_no_definition_is_reported_by_the_sweep(tmp_path, clock):
    gate = PauseGate()
    stack = build_stack(tmp_path, clock, destroy=InstanceDestroyStep(gate=gate))
    await birth(stack)
    await settle(stack)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()
    await asyncio.wait_for(stack.destroy.entered.wait(), 5)
    (destroy_run,) = [r for r in await runs(stack) if r.workflow == "destroy-instance"]
    await crash_run(stack.h, destroy_run.id, gate)

    stack2 = build_stack(tmp_path, clock)  # a restarted process whose build lacks the definition
    stack2.h.engine._definitions.pop("destroy-instance")  # noqa: SLF001
    await stack2.h.engine.resume_inflight()
    await asyncio.gather(*[h.task for h in list(stack2.h.engine._runs.values())], return_exceptions=True)  # noqa: SLF001
    assert [r.status for r in await runs(stack2) if r.workflow == "destroy-instance"] == ["failed"]
    assert (await instance(stack2)).status == "destroying"  # the run ended, and nothing said so

    assert await sweep_of(stack2).run_once() == ["i1"]
    assert (await instance(stack2)).status == "destroy-failed"
    assert (await audits(stack2))[-1][:3] == ("destroying", "destroy-failed", "InstanceDestroyFailed")


# ---------------------------------------------------------------------------
# a run with no task
# ---------------------------------------------------------------------------


async def test_a_run_whose_task_died_is_adopted_once_and_never_gets_a_second_task(tmp_path, clock):
    gate = PauseGate()
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(gate=gate))
    await birth(stack)
    await stack.executor.drain_pending()
    (run,) = await runs(stack)
    await crash_run(stack.h, run.id, gate)  # the task is dead; the run is `running`
    sweep = sweep_of(stack)
    gate.release()

    assert await sweep.run_once() == ["i1"]
    assert stack.h.engine.is_running(run.id)
    assert await sweep.run_once() == []  # a live task is left alone
    await settle(stack)
    assert (await instance(stack)).status == "active"
    assert stack.boot.calls == 2  # the crashed attempt, and exactly one more


class StubbornCreateStep(InstanceCreateStep):
    """A create step that never reaches a cancel point: the engine's hard cancel has to cancel its task."""

    def __init__(self) -> None:
        super().__init__()
        self.hold = asyncio.Event()

    async def execute(self, params, ctx):
        self.calls += 1
        self.entered.set()
        await self.hold.wait()
        return CreateOutput(address="192.0.2.99", resource_ids={"vm": "late"})


async def test_a_run_whose_step_ignored_its_cancel_past_the_grace_is_recovered_by_the_sweep(tmp_path, clock):
    create = StubbornCreateStep()
    stack = build_stack(tmp_path, clock, create=create)
    stack.h.engine._cancel_grace_seconds = 0.05  # noqa: SLF001
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(create.entered.wait(), 5)
    (prov,) = await runs(stack)

    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))
    await stack.executor.drain_pending()  # cancels the provision; the destroy row waits for its slot
    for _ in range(200):  # the grace is real time; the clock below is the fake one
        if not stack.h.engine.is_running(prov.id):
            break
        await asyncio.sleep(0.01)
    assert not stack.h.engine.is_running(prov.id)  # the task was cancelled, the run was not ended
    assert [r.status for r in await runs(stack)] == ["running"]

    assert await sweep_of(stack).run_once() == ["i1"]  # adopts it; it sees its cancel and ends
    for _ in range(6):
        clock.advance(timedelta(seconds=3))
        await settle(stack)
    assert (await instance(stack)).status == "destroyed"  # no restart
    assert stack.destroy.calls == 1


async def test_an_undo_longer_than_the_cancel_grace_finishes_and_the_destroy_then_runs(tmp_path, clock):
    class SlowUndo(InstanceCreateStep):
        finished = 0

        async def undo(self, params, output, notes, ctx):
            await asyncio.sleep(0.2)  # longer than the grace below
            type(self).finished += 1

    stack = build_stack(tmp_path, clock, create=SlowUndo(), boot=InstanceBootStep(block=True))
    stack.h.engine._cancel_grace_seconds = 0.05  # noqa: SLF001
    await birth(stack)
    await stack.executor.drain_pending()
    await asyncio.wait_for(stack.boot.entered.wait(), 5)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=clock.now(), actor="api:alice"))

    sweep = sweep_of(stack)
    for _ in range(25):  # the destroy row retries while the provision run unwinds
        await stack.executor.drain_pending()
        await asyncio.sleep(0.08)
        await sweep.run_once()
        clock.advance(timedelta(seconds=3))
        if (await instance(stack)).status == "destroyed":
            break
    assert (await instance(stack)).status == "destroyed"
    assert SlowUndo.finished >= 1


# ---------------------------------------------------------------------------
# the timer
# ---------------------------------------------------------------------------


async def test_a_lost_expiry_timer_is_restored_once_and_later_passes_write_nothing(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    async with stack.uow() as tx:
        tx.execute(text("DELETE FROM timers"))
    sweep = sweep_of(stack)

    assert await sweep.run_once() == ["i1"]
    assert (await timer_keys(stack))[("instance", "i1", "expiry")].fire_at == NOW + HOUR
    version, trail = (await instance(stack)).version, await audits(stack)
    assert trail[-1][2] == "InstanceTimerLost"

    for _ in range(3):
        assert await sweep.run_once() == []
    assert ((await instance(stack)).version, await audits(stack)) == (version, trail)

    clock.advance(HOUR)
    await fire_due_timers(stack)
    await settle(stack)
    assert (await instance(stack)).status == "destroyed"


async def test_a_healthy_instance_is_left_alone_while_provisioning_and_when_active(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    sweep = sweep_of(stack)
    await birth(stack)
    assert await sweep.run_once() == []  # a pending provision row is a path
    await stack.executor.drain_pending()
    assert await sweep.run_once() == []  # a run with a live task is a path
    await settle(stack)
    version = (await instance(stack)).version
    assert await sweep.run_once() == []  # an armed timer is a path
    assert (await instance(stack)).version == version
    assert stack.create.calls == 1 and len(await runs(stack)) == 1


# ---------------------------------------------------------------------------
# isolation and the loop
# ---------------------------------------------------------------------------


class FailingFirstEngine:
    """A plain double for the sweep's engine: ``resume_run`` fails for instance ``bad``."""

    def __init__(self, engine) -> None:
        self._engine, self.resumed = engine, []

    def is_running(self, run_id: str) -> bool:
        return self._engine.is_running(run_id)

    async def resume_run(self, run_id: str) -> None:
        async with self._engine._uow() as tx:  # noqa: SLF001
            instance_id = self._engine._run_repo.get(tx, run_id).instance_id  # noqa: SLF001
        if instance_id == "bad":
            raise RuntimeError("injected")
        self.resumed.append(instance_id)


async def test_one_failing_instance_does_not_stop_the_next(tmp_path, clock):
    stack = build_stack(tmp_path, clock, boot=InstanceBootStep(block=True))
    for iid, slug in (("a", "aa"), ("bad", "bb"), ("c", "cc")):
        await birth(stack, iid, slug)
    await stack.executor.drain_pending()
    for handle in list(stack.h.engine._runs.values()):  # noqa: SLF001
        handle.task.cancel()
    await asyncio.sleep(0.05)  # three admitted runs, none with a task
    engine = FailingFirstEngine(stack.h.engine)

    assert await sweep_of(stack, engine).run_once() == ["a", "c"]
    assert engine.resumed == ["a", "c"]


async def test_the_sweep_runs_on_its_own_interval_and_stops_cleanly(tmp_path, clock):
    stack = build_stack(tmp_path, clock)
    await birth(stack)
    await settle(stack)
    async with stack.uow() as tx:
        tx.execute(text("DELETE FROM timers"))
    sweep = sweep_of(stack)
    await sweep.start()
    await sweep.start()  # idempotent
    try:
        for _ in range(200):
            if ("instance", "i1", "expiry") in await timer_keys(stack):
                break
            await asyncio.sleep(0.01)
        assert ("instance", "i1", "expiry") in await timer_keys(stack)
    finally:
        await sweep.stop()
    await sweep.stop()  # idempotent
