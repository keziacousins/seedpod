"""tests/engine/test_engine_stop.py — DR-0024: ``WorkflowEngine.stop()`` quiesces
by INTERRUPTION, never by cancellation.

The load-bearing test in this module is
``test_stop_does_not_cancel_the_run_and_never_compensates``. Every
``provision-*.yml`` declares ``on_failure: compensate``, so a ``stop()`` built on
the existing ``cancel(run_id)`` path would trip each run's ``CancelToken`` and
**destroy every cluster the process was mid-way through provisioning, on every
restart** — a real droplet deleted because an operator restarted a service. That
is the failure this DR exists to foreclose, and the assertion that
``cancel_requested`` stays false while no undo runs is what keeps it foreclosed.

A run interrupted by ``stop()`` must be indistinguishable from one interrupted by
``kill -9``: the row stays non-terminal and the next boot's ``resume_inflight()``
re-adopts it, bounded by the existing ``interrupted_count``/``resume_replay_limit``
machinery. No Mock/patch anywhere — real engine, real SQLite, hand-written fake Steps.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.data.repositories import WorkflowRunRow
from tests.engine.fakes import SleeperStep, SleepingUndoStep, build_engine

NOW = datetime(2026, 8, 3, 12, 0, 0, tzinfo=UTC)

_WORKFLOW = """
workflow: stop-interruption
version: 1
inputs:
  cluster_id: {type: str}
on_failure: compensate
outcome:
  succeeded: {event: ProvisionSucceeded, payload: {public_ip: "1.2.3.4", kubeconfig_ref: "kc-1"}}
  failed:    {event: ProvisionFailed, payload: {reason: "n/a"}}
  cancelled: {event: ProvisionFailed, payload: {reason: "cancelled"}}
steps:
  - id: alloc
    uses: fake.sleeping_undo
    with: {resource_id: "r-a"}
  - id: sleeper
    uses: fake.sleeper
    with: {message: "hi"}
"""


def _insert_cluster(session, cluster_id: str) -> None:
    session.execute(
        text(
            """
            INSERT INTO clusters (id, name, slug, environment, status, provider, created_at, updated_at)
            VALUES (:id, :id, :id, 'ephemeral', 'provisioning', 'fake', :now, :now)
            """
        ),
        {"id": cluster_id, "now": NOW.isoformat()},
    )


async def _insert_run(harness, run_id: str, cluster_id: str) -> None:
    async with harness.uow() as tx:
        _insert_cluster(tx, cluster_id)
        harness.run_repo.insert(
            tx,
            WorkflowRunRow(
                id=run_id,
                workflow="stop-interruption",
                workflow_version=1,
                cluster_id=cluster_id,
                deployment_id=None,
                dedupe_key=f"dedupe:{run_id}",
                args={"cluster_id": cluster_id},
                status="pending",
                cancel_requested=False,
                failed_step=None,
                error=None,
                undo_incomplete=None,
                initiated_by="test",
                created_at=NOW,
                started_at=None,
                finished_at=None,
            ),
        )


async def _wait_until(check, *, timeout: float = 5.0, interval: float = 0.005) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if check():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


def _build(tmp_path, *, sleeper_seconds: float):
    """A fresh engine over ``tmp_path``'s SQLite file. Called twice by the resume
    test — ``build_engine`` derives a deterministic path, so the second call is a
    faithful 'next boot' against the same durable state."""
    alloc = SleepingUndoStep(undo_seconds=0.05)
    sleeper = SleeperStep(seconds=sleeper_seconds)
    harness = build_engine(tmp_path, {"stop-interruption": _WORKFLOW}, [alloc, sleeper], FrozenClock(NOW))
    return harness, alloc, sleeper


# ---------------------------------------------------------------------------
# The anti-compensation guarantee — the reason DR-0024 exists.
# ---------------------------------------------------------------------------


async def test_stop_does_not_cancel_the_run_and_never_compensates(tmp_path):
    harness, alloc, sleeper = _build(tmp_path, sleeper_seconds=60.0)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id)

    await harness.engine.start(run_id)
    await _wait_until(lambda: sleeper.started >= 1)
    handle = harness.engine._runs.get(run_id)  # noqa: SLF001 -- white-box: the token must NOT trip
    assert handle is not None

    await asyncio.wait_for(harness.engine.stop(grace_seconds=0.1), timeout=5.0)

    # The token was never tripped and cancel_requested was never written: this run
    # was INTERRUPTED, not cancelled.
    assert handle.cancel_token.tripped is False
    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        steps = {r.step_path: r for r in harness.step_repo.list_for_run(tx, run_id)}
    assert run_row.cancel_requested is False
    assert run_row.status == "running", "a shutdown-interrupted run must stay non-terminal"
    assert run_row.finished_at is None
    assert steps["sleeper"].status == "running"  # interrupted mid-step, not 'cancelled'

    # THE assertion: no compensation ran. `alloc` is an undoable step that already
    # succeeded, and the workflow is `on_failure: compensate` -- had stop() reused
    # cancel(), its undo would have fired here (that is what deletes a real droplet).
    assert alloc.undo_calls == []
    assert steps["alloc"].status == "succeeded"
    assert steps["alloc"].undo_status is None
    harness.db.dispose()


# ---------------------------------------------------------------------------
# Quiescing: idle is a no-op, and a stopped engine adopts nothing.
# ---------------------------------------------------------------------------


async def test_stop_on_an_idle_engine_is_an_immediate_no_op(tmp_path):
    harness, _alloc, _sleeper = _build(tmp_path, sleeper_seconds=0.0)

    # No live runs => returns without burning the grace at all.
    await asyncio.wait_for(harness.engine.stop(grace_seconds=30.0), timeout=1.0)
    await asyncio.wait_for(harness.engine.stop(grace_seconds=30.0), timeout=1.0)  # idempotent
    harness.db.dispose()


async def test_start_after_stop_adopts_nothing(tmp_path):
    harness, _alloc, sleeper = _build(tmp_path, sleeper_seconds=0.0)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id)

    await harness.engine.stop()
    await harness.engine.start(run_id)

    assert harness.engine.is_running(run_id) is False
    assert sleeper.started == 0
    async with harness.uow() as tx:
        assert harness.run_repo.get(tx, run_id).status == "pending"  # untouched
    harness.db.dispose()


async def test_resume_inflight_after_stop_adopts_nothing_on_the_same_engine(tmp_path):
    """``_stopping`` is one-way for the instance (DR-0024 decision 5): recovery is
    the NEXT boot's job, not a restart of the engine that is shutting down."""
    harness, _alloc, sleeper = _build(tmp_path, sleeper_seconds=0.0)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id)

    await harness.engine.stop()
    await harness.engine.resume_inflight()

    assert harness.engine.is_running(run_id) is False
    assert sleeper.started == 0
    harness.db.dispose()


# ---------------------------------------------------------------------------
# The other half of the contract: the next boot picks the run back up.
# ---------------------------------------------------------------------------


async def test_a_run_interrupted_by_stop_is_resumed_by_the_next_boot(tmp_path):
    """Simulates a real restart: engine A is stopped mid-step, then a FRESH engine
    over the same SQLite file runs ``resume_inflight()`` — the existing crash-recovery
    path, which is exactly why DR-0024 needs no new persistence or status."""
    harness_a, _alloc_a, sleeper_a = _build(tmp_path, sleeper_seconds=60.0)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness_a, run_id, cluster_id)

    await harness_a.engine.start(run_id)
    await _wait_until(lambda: sleeper_a.started >= 1)
    await asyncio.wait_for(harness_a.engine.stop(grace_seconds=0.1), timeout=5.0)
    harness_a.db.dispose()

    # --- next boot ---
    harness_b, _alloc_b, sleeper_b = _build(tmp_path, sleeper_seconds=0.0)
    await harness_b.engine.resume_inflight()
    assert harness_b.engine.is_running(run_id) is True

    await asyncio.wait_for(harness_b.engine.wait_for(run_id), timeout=10.0)
    async with harness_b.uow() as tx:
        run_row = harness_b.run_repo.get(tx, run_id)
        steps = {r.step_path: r for r in harness_b.step_repo.list_for_run(tx, run_id)}
    assert run_row.status == "succeeded"
    assert run_row.cancel_requested is False
    assert sleeper_b.started == 1  # replayed on resume -- idempotent, so allowed
    assert steps["sleeper"].interrupted_count == 1  # the interruption was RECORDED
    harness_b.db.dispose()
