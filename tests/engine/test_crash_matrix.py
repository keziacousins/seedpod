"""tests/engine/test_crash_matrix.py — the crash/resume matrix: for each persistence
point 2-9 in docs/design/seam-b-engine.md §2.3.2's table, drive a run to exactly that
point against fakes, kill the run's in-process task there, assert the DB state at rest
matches the point's transaction contents (nothing inside an in-flight attempt
persisted), then re-adopt via resume (§2.3.3, amended by coherence-review Conflict 5)
and assert the documented resume behavior.

Point 1 (Admission) is out of scope — coherence-review Conflict 2 assigns run
admission to the runtime spine's run-admitter, not this engine; this module (like
test_engine_smoke.py) always inserts the ``workflow_runs`` row directly and hands the
id to ``WorkflowEngine.start()``/``resume_inflight()``.

**Two crash-injection techniques, used deliberately for different points:**

1. **Live task interruption** (``tests.engine.fakes.crash_run`` + ``PauseGate``/
   ``GatedSleeper``): a fake verb (or the engine's injected ``Sleeper`` seam) parks on
   a bare ``asyncio.Event``; the test waits for the run's task to actually park there,
   then hard-cancels that ``asyncio.Task``. This is a faithful process-crash
   simulation ONLY where the park point has no ``try/finally`` on the call stack back
   up to ``WorkflowEngine._run`` — Python runs ``finally`` blocks even when a
   coroutine is torn down by ``CancelledError``, which a real process kill would
   never do. Grepping seedpod/engine/engine.py confirms exactly ONE ``finally``
   exists in the whole module: ``_park_and_wait``'s blocked-status restore. Every
   live-interruption test in this file parks somewhere else, so this technique is
   used for points 2, 3, 4, 5, 6, 7 (the ``on_failure: continue`` flavor), 8, 9, the
   resume_replay_limit crash-loop, and compensation resume.
2. **Direct DB-state crafting** (insert/update ``workflow_runs``/``workflow_steps``
   rows by hand, exactly the shape a crash would leave behind, then resume): used for
   the three scenarios where live-interrupting the exact moment would either land
   inside ``_park_and_wait``'s guarded finally (the blocked-run-adopted-like-running
   scenario) or where the "point" under test is fundamentally about resume reading a
   given DB state rather than about interrupting a live attempt (the non-idempotent-
   resume-fails scenario; the cancel_requested-pre-crash-G1 scenario). This matches
   coherence-review Conflict 2's own testing convention: "Engine tests insert run rows
   directly."

Every "resume" half of a test builds a BRAND NEW ``EngineHarness`` (fresh
``WorkflowEngine``, fresh ``StepRegistry``, fresh fake-verb instances) pointed at the
SAME sqlite file as the "crash" half's harness — proving resume is driven purely by
what is on disk, never by anything retained in the first harness's Python objects.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.data.repositories import WorkflowRunRow, WorkflowStepRow
from tests.engine.fakes import (
    GatedSleeper,
    InstantStep,
    PausableStep,
    PauseGate,
    UnreachableNTimesStep,
    build_engine,
    crash_run,
)

NOW = datetime(2026, 7, 13, 12, 0, 0, tzinfo=UTC)


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


async def _insert_run(
    harness, run_id: str, cluster_id: str, workflow: str, *, status: str = "pending", cancel_requested: bool = False
) -> None:
    async with harness.uow() as tx:
        _insert_cluster(tx, cluster_id)
        harness.run_repo.insert(
            tx,
            WorkflowRunRow(
                id=run_id,
                workflow=workflow,
                workflow_version=1,
                cluster_id=cluster_id,
                deployment_id=None,
                dedupe_key=f"dedupe:{run_id}",
                args={"cluster_id": cluster_id},
                status=status,
                cancel_requested=cancel_requested,
                failed_step=None,
                error=None,
                undo_incomplete=None,
                initiated_by="test",
                created_at=NOW,
                started_at=NOW if status != "pending" else None,
                finished_at=None,
            ),
        )


async def _seed_ordinal_counter(harness, run_id: str) -> None:
    """WORKAROUND for a discovered crash-recovery gap, not a spec requirement of
    this helper's callers: ``WorkflowEngine._next_ordinal``/``_ordinal_counters``
    (seedpod/engine/engine.py) is a pure in-memory per-instance counter, never
    seeded from persisted ``effects_outbox`` rows on adoption. A fresh engine
    instance -- exactly what a real process restart is -- restarts that counter
    at 0 for a run that already has persisted Notify rows, colliding on the
    ``effect_id`` UNIQUE constraint the moment it writes the next one (e.g.
    ``job_completed`` colliding with the ``job_started`` a pre-crash process
    already wrote). See ``test_notify_ordinal_counter_is_not_seeded_after_resume``
    below, which demonstrates this directly and is LEFT FAILING per this task's
    brief ("if the engine deviates, leave the test failing... record the
    deviation") -- seedpod/engine/engine.py is out of this task's edit scope.
    This helper exists only so the OTHER crash-matrix tests, whose actual
    subject is workflow_runs/workflow_steps persistence-point semantics (not
    outbox ordinal generation), aren't collaterally broken by a bug orthogonal
    to what they're each individually testing."""
    async with harness.uow() as tx:
        rows = harness.outbox_repo.list_for_aggregate(tx, "run", run_id)
    harness.engine._ordinal_counters[run_id] = len(rows)  # noqa: SLF001 -- see docstring


def _outcome_block(workflow: str, on_failure: str = "report") -> str:
    return f"""
workflow: {workflow}
version: 1
inputs:
  cluster_id: {{type: str}}
on_failure: {on_failure}
outcome:
  succeeded: {{event: ProvisionSucceeded, payload: {{public_ip: "1.2.3.4", kubeconfig_ref: "kc-1"}}}}
  failed:    {{event: ProvisionFailed, payload: {{reason: "n/a"}}}}
  cancelled: {{event: ProvisionFailed, payload: {{reason: "cancelled"}}}}
"""


# ============================================================================
# Point 2 — Step start: INSERT workflow_steps(status='running', params=resolved)
# ============================================================================


async def test_point2_step_start_crash_persists_only_the_running_row_then_resume_reenters(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    step = PausableStep(verb="fake.p2", pause_execute=gate)
    wf = _outcome_block("crash-p2") + """steps:
  - id: only
    uses: fake.p2
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"crash-p2": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p2")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, gate)

    # ---- DB at rest: exactly point 2's transaction contents, nothing more ----
    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "running"
    assert run_row.finished_at is None
    assert step_row.status == "running"
    assert step_row.attempt == 1
    assert step_row.interrupted_count == 0
    assert step_row.params == {"resource_id": "r-1"}
    assert step_row.output is None
    assert step_row.notes == {}
    harness.db.dispose()

    # ---- resume: idempotent verb re-enters execute, interrupted_count+1 ----
    resumed_step = PausableStep(verb="fake.p2")  # fresh instance, no pause this time
    harness2 = build_engine(tmp_path, {"crash-p2": wf}, [resumed_step], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
        step_row = harness2.step_repo.get(tx, run_id, "only")
    assert run_row.status == "succeeded"
    assert step_row.status == "succeeded"
    assert step_row.interrupted_count == 1
    assert step_row.output == {"resource_id": "r-1"}
    assert resumed_step.calls == 1
    harness2.db.dispose()


# ============================================================================
# Point 3 — Retry: bump attempt, then cancel-aware backoff sleep
# ============================================================================


async def test_point3_retry_attempt_bump_crash_mid_backoff_then_resume_from_bumped_attempt(tmp_path):
    clock = FrozenClock(NOW)
    sleep_gate = PauseGate()
    sleeper = GatedSleeper(gate=sleep_gate)
    step = PausableStep(verb="fake.p3", fail_times=1)  # fails attempt 1, would succeed on attempt 2
    wf = _outcome_block("crash-p3") + """steps:
  - id: only
    uses: fake.p3
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"crash-p3": wf}, [step], clock, sleeper=sleeper)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p3")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, sleep_gate)

    async with harness.uow() as tx:
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert step_row.status == "running"  # unchanged since before the retry
    assert step_row.attempt == 2  # point 3's bump committed
    assert step_row.interrupted_count == 0
    assert step_row.output is None
    assert step.calls == 1  # only the failed attempt actually ran execute()
    harness.db.dispose()

    resumed_step = PausableStep(verb="fake.p3", fail_times=0)  # fresh instance: succeeds immediately now
    harness2 = build_engine(tmp_path, {"crash-p3": wf}, [resumed_step], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
        step_row = harness2.step_repo.get(tx, run_id, "only")
    assert run_row.status == "succeeded"
    assert step_row.status == "succeeded"
    assert step_row.attempt == 2  # resumed at the persisted attempt, not restarted at 1
    assert step_row.interrupted_count == 1
    assert resumed_step.calls == 1
    harness2.db.dispose()


# ============================================================================
# Point 4 — Each ctx.note(): merged into notes, committed before note() returns
# ============================================================================


async def test_point4_note_commit_crash_then_resume_preserves_note_and_reenters(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    step = PausableStep(verb="fake.p4", note={"resource_id": "r-1"}, pause_execute=gate)
    wf = _outcome_block("crash-p4") + """steps:
  - id: only
    uses: fake.p4
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"crash-p4": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p4")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, gate)

    async with harness.uow() as tx:
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert step_row.status == "running"
    assert step_row.notes == {"resource_id": "r-1"}  # point 4's write survived
    assert step_row.output is None
    harness.db.dispose()

    resumed_step = PausableStep(verb="fake.p4", note={"resource_id": "r-1"})
    harness2 = build_engine(tmp_path, {"crash-p4": wf}, [resumed_step], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
        step_row = harness2.step_repo.get(tx, run_id, "only")
    assert run_row.status == "succeeded"
    assert step_row.status == "succeeded"
    assert step_row.notes == {"resource_id": "r-1"}
    assert step_row.interrupted_count == 1
    harness2.db.dispose()


# ============================================================================
# Point 5 — Execute done: output + status ('succeeded', no gate)
# ============================================================================


async def test_point5_execute_done_crash_before_next_step_then_resume_does_not_rerun_completed_step(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    first = InstantStep()
    second = PausableStep(verb="fake.p5.second", pause_execute=gate)
    wf = _outcome_block("crash-p5") + """steps:
  - id: first
    uses: fake.instant
    with: {message: "hello"}
  - id: second
    uses: fake.p5.second
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"crash-p5": wf}, [first, second], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p5")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, gate)

    async with harness.uow() as tx:
        first_row = harness.step_repo.get(tx, run_id, "first")
        second_row = harness.step_repo.get(tx, run_id, "second")
    assert first_row.status == "succeeded"  # point 5 committed for "first"
    assert first_row.output == {"echoed": "hello"}
    assert first_row.finished_at is not None
    assert second_row.status == "running"  # point 2 for "second" committed, nothing more
    harness.db.dispose()

    resumed_first = InstantStep()
    resumed_second = PausableStep(verb="fake.p5.second")
    harness2 = build_engine(tmp_path, {"crash-p5": wf}, [resumed_first, resumed_second], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
        second_row = harness2.step_repo.get(tx, run_id, "second")
    assert run_row.status == "succeeded"
    assert second_row.status == "succeeded"
    assert second_row.interrupted_count == 1
    assert resumed_first.calls == []  # "first" was NEVER re-executed -- read from the DB row
    harness2.db.dispose()


# ============================================================================
# Point 6 — Gate passes: final output (Ready enrichment) + 'succeeded'
# ============================================================================


async def test_point6_gate_pass_crash_before_next_step_then_resume_does_not_repoll_completed_gate(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    gate_step = PausableStep(verb="fake.p6.gate", gateable=True, ready_after=1)
    second = PausableStep(verb="fake.p6.second", pause_execute=gate)
    wf = _outcome_block("crash-p6") + """steps:
  - id: gate_step
    uses: fake.p6.gate
    with: {resource_id: "r-1"}
    gate: {timeout_seconds: 30, interval_seconds: 1}
  - id: second
    uses: fake.p6.second
    with: {resource_id: "r-2"}
"""
    harness = build_engine(tmp_path, {"crash-p6": wf}, [gate_step, second], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p6")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, gate)

    async with harness.uow() as tx:
        gate_row = harness.step_repo.get(tx, run_id, "gate_step")
        second_row = harness.step_repo.get(tx, run_id, "second")
    assert gate_row.status == "succeeded"  # NOT stuck at "gating"
    assert gate_row.output == {"resource_id": "r-1"}
    assert second_row.status == "running"
    assert gate_step.polls == 1
    harness.db.dispose()

    resumed_gate = PausableStep(verb="fake.p6.gate", gateable=True, ready_after=1)
    resumed_second = PausableStep(verb="fake.p6.second")
    harness2 = build_engine(tmp_path, {"crash-p6": wf}, [resumed_gate, resumed_second], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
    assert run_row.status == "succeeded"
    assert resumed_gate.polls == 0  # the already-succeeded gate was never re-polled
    harness2.db.dispose()


# ============================================================================
# Point 7 — Step failure/cancel ('on_failure: continue' flavor): failed_continued
# recorded, run proceeds to the next step
# ============================================================================


async def test_point7_failed_continued_crash_before_next_step_then_resume_advances_past_it(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    bad = PausableStep(verb="fake.p7.bad", fail_times=1, fail_kind="permanent")
    second = PausableStep(verb="fake.p7.second", pause_execute=gate)
    wf = _outcome_block("crash-p7") + """steps:
  - id: bad
    uses: fake.p7.bad
    with: {resource_id: "r-1"}
    on_failure: continue
  - id: second
    uses: fake.p7.second
    with: {resource_id: "r-2"}
"""
    harness = build_engine(tmp_path, {"crash-p7": wf}, [bad, second], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p7")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, gate)

    async with harness.uow() as tx:
        bad_row = harness.step_repo.get(tx, run_id, "bad")
        second_row = harness.step_repo.get(tx, run_id, "second")
        run_row = harness.run_repo.get(tx, run_id)
    assert bad_row.status == "failed_continued"
    assert bad_row.error == {"kind": "permanent", "message": "permanent failure #1"}
    assert second_row.status == "running"
    assert run_row.status == "running"  # continue never flips the run
    harness.db.dispose()

    resumed_bad = PausableStep(verb="fake.p7.bad", fail_times=1, fail_kind="permanent")
    resumed_second = PausableStep(verb="fake.p7.second")
    harness2 = build_engine(tmp_path, {"crash-p7": wf}, [resumed_bad, resumed_second], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
    assert run_row.status == "succeeded"
    assert resumed_bad.calls == 0  # "bad" was already terminal (failed_continued): never re-run
    harness2.db.dispose()


# ============================================================================
# Point 7 (continue flavor), downstream Ref regression -- a step bound via
# `{from: <continued_step>.<field>}` to a step that recorded 'failed_continued'
# must NOT crash the run. Seam B 2.2's grammar comment on destroy-cloud.yml's
# `tailscale` step ("Optional kubeconfig=None => no-op") + V4's "Optional[T]
# sources bind only Optional[T] params" together establish the only coherent
# runtime meaning: a field the failed step never got to record falls back to
# its Output model's own declared default (never a synthesized value for a
# genuinely-required field with no default). Before the fix, BOTH halves of
# this were missing: the live path never even put the continued step's id in
# `state.scope` at all (bare KeyError on the head name), and the fallback
# value for a field the row didn't record didn't exist either (KeyError on the
# field name once the head-name gap was closed) -- either one permanently
# wedges the run non-terminal with no outcome event (§2.3.3's totality
# promise broken).
# ============================================================================


async def test_point7_continue_downstream_ref_resolves_to_output_default_live(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    # PausableStep's Output (NoteOutput) declares `resource_id: str = "r-1"` --
    # a field with a default, exactly the "Optional[T]-shaped" case this fix
    # targets, even though the concrete type here isn't literally Optional.
    bad = PausableStep(verb="fake.p7ref.bad", fail_times=99, fail_kind="permanent")
    second = PausableStep(verb="fake.p7ref.second", pause_execute=gate)
    wf = _outcome_block("crash-p7ref") + """steps:
  - id: bad
    uses: fake.p7ref.bad
    with: {resource_id: "ignored"}
    on_failure: continue
  - id: second
    uses: fake.p7ref.second
    with: {resource_id: {from: bad.resource_id}}
"""
    harness = build_engine(tmp_path, {"crash-p7ref": wf}, [bad, second], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p7ref")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, gate)  # parked mid "second".execute()

    async with harness.uow() as tx:
        bad_row = harness.step_repo.get(tx, run_id, "bad")
        second_row = harness.step_repo.get(tx, run_id, "second")
    assert bad_row.status == "failed_continued"
    assert bad_row.output is None  # execute() raised: no Output was ever produced
    # `second`'s params were resolved (and persisted at point 2) from the LIVE
    # `_advance_top_level_step` path -- this is the assertion the fix targets:
    # the Ref resolved to the Output model's declared default, not a crash.
    assert second_row.params == {"resource_id": "r-1"}
    harness.db.dispose()

    resumed_bad = PausableStep(verb="fake.p7ref.bad", fail_times=99, fail_kind="permanent")
    resumed_second = PausableStep(verb="fake.p7ref.second")
    harness2 = build_engine(tmp_path, {"crash-p7ref": wf}, [resumed_bad, resumed_second], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
    assert run_row.status == "succeeded"
    harness2.db.dispose()


async def test_point7_continue_downstream_ref_resolves_to_output_default_on_resume(tmp_path):
    """Same fallback, but reached purely via resume's `_rebuild_scope` +
    `_advance_top_level_step`'s existing-row branch: "bad" is crafted directly
    in the DB (Conflict 2's own testing convention) as an already-terminal
    'failed_continued' row with no output, and "second" (bound to
    `{from: bad.resource_id}`) does not exist yet -- forcing resume to build
    its params from scratch, exercising the exact same fallback the live path
    uses, proving §2.3.3's "byte-identical inputs" promise holds for this case
    too."""
    clock = FrozenClock(NOW)
    second = PausableStep(verb="fake.p7ref2.second")
    wf = _outcome_block("crash-p7ref2") + """steps:
  - id: bad
    uses: fake.p7ref2.bad
    with: {resource_id: "ignored"}
    on_failure: continue
  - id: second
    uses: fake.p7ref2.second
    with: {resource_id: {from: bad.resource_id}}
"""
    harness = build_engine(tmp_path, {"crash-p7ref2": wf}, [PausableStep(verb="fake.p7ref2.bad"), second], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p7ref2", status="running")
    async with harness.uow() as tx:
        harness.step_repo.insert(
            tx,
            WorkflowStepRow(
                run_id=run_id, step_path="bad", verb="fake.p7ref2.bad", status="failed_continued",
                attempt=1, interrupted_count=0, params={"resource_id": "ignored"}, notes={},
                output=None, undo_status=None, error={"kind": "permanent", "message": "permanent failure #1"},
                started_at=NOW, finished_at=NOW,
            ),
        )

    await harness.engine.resume_inflight()
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        second_row = harness.step_repo.get(tx, run_id, "second")
    assert run_row.status == "succeeded"
    assert second_row.params == {"resource_id": "r-1"}  # same fallback value as the live path
    harness.db.dispose()


# ============================================================================
# Point 7 (abort + compensate flavor) -> compensating, crash before ANY undo runs
# ============================================================================


async def test_point7_abort_compensate_crash_before_first_undo_then_resume_compensates_lifo(tmp_path):
    clock = FrozenClock(NOW)
    undo_gate = PauseGate()
    a = PausableStep(verb="fake.p7c.a", undoable=True, pause_undo=undo_gate)
    b = PausableStep(verb="fake.p7c.b", fail_times=1, fail_kind="permanent")
    wf = _outcome_block("crash-p7c", on_failure="compensate") + """steps:
  - id: a
    uses: fake.p7c.a
    with: {resource_id: "r-a"}
  - id: b
    uses: fake.p7c.b
    with: {resource_id: "r-b"}
"""
    harness = build_engine(tmp_path, {"crash-p7c": wf}, [a, b], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p7c")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, undo_gate)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        a_row = harness.step_repo.get(tx, run_id, "a")
        b_row = harness.step_repo.get(tx, run_id, "b")
    assert run_row.status == "compensating"
    assert run_row.failed_step == "b"
    assert b_row.status == "failed"
    assert b_row.undo_status == "skipped"  # "b" is not undoable -- skipped immediately
    assert a_row.status == "succeeded"
    assert a_row.undo_status is None  # mid-undo, not yet committed
    harness.db.dispose()

    resumed_a = PausableStep(verb="fake.p7c.a", undoable=True)
    resumed_b = PausableStep(verb="fake.p7c.b", fail_times=1, fail_kind="permanent")
    harness2 = build_engine(tmp_path, {"crash-p7c": wf}, [resumed_a, resumed_b], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
        a_row = harness2.step_repo.get(tx, run_id, "a")
    assert run_row.status == "failed"
    assert run_row.undo_incomplete is None
    assert a_row.undo_status == "done"
    assert len(resumed_a.undo_calls) == 1
    harness2.db.dispose()


# ============================================================================
# Point 8 — Each undo result: crash after ONE undo commits, before the next
# starts; resume skips the already-done undo and continues LIFO
# ============================================================================


async def test_point8_undo_result_crash_mid_lifo_then_resume_skips_done_and_continues(tmp_path):
    clock = FrozenClock(NOW)
    undo_gate = PauseGate()
    a = PausableStep(verb="fake.p8.a", undoable=True)
    b = PausableStep(verb="fake.p8.b", undoable=True, pause_undo=undo_gate)
    c = PausableStep(verb="fake.p8.c", undoable=True)
    d = PausableStep(verb="fake.p8.d", fail_times=1, fail_kind="permanent")
    wf = _outcome_block("crash-p8", on_failure="compensate") + """steps:
  - id: a
    uses: fake.p8.a
    with: {resource_id: "r-a"}
  - id: b
    uses: fake.p8.b
    with: {resource_id: "r-b"}
  - id: c
    uses: fake.p8.c
    with: {resource_id: "r-c"}
  - id: d
    uses: fake.p8.d
    with: {resource_id: "r-d"}
"""
    harness = build_engine(tmp_path, {"crash-p8": wf}, [a, b, c, d], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p8")

    await harness.engine.start(run_id)
    # LIFO order: d (fail, not undoable -> skipped), c (undo, completes), b (undo, PARKS)
    await crash_run(harness, run_id, undo_gate)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        rows = {r.step_path: r for r in harness.step_repo.list_for_run(tx, run_id)}
    assert run_row.status == "compensating"
    assert rows["d"].undo_status == "skipped"
    assert rows["c"].undo_status == "done"  # point 8's write for "c" survived the crash
    assert rows["b"].undo_status is None  # "b"'s undo is in flight, uncommitted
    assert rows["a"].undo_status is None  # never reached
    assert len(c.undo_calls) == 1
    harness.db.dispose()

    resumed_a = PausableStep(verb="fake.p8.a", undoable=True)
    resumed_b = PausableStep(verb="fake.p8.b", undoable=True)
    resumed_c = PausableStep(verb="fake.p8.c", undoable=True)
    resumed_d = PausableStep(verb="fake.p8.d", fail_times=1, fail_kind="permanent")
    harness2 = build_engine(tmp_path, {"crash-p8": wf}, [resumed_a, resumed_b, resumed_c, resumed_d], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
        rows = {r.step_path: r for r in harness2.step_repo.list_for_run(tx, run_id)}
    assert run_row.status == "failed"
    assert run_row.undo_incomplete is None
    assert rows["a"].undo_status == "done"
    assert rows["b"].undo_status == "done"
    assert rows["c"].undo_status == "done"
    assert rows["d"].undo_status == "skipped"
    assert len(resumed_c.undo_calls) == 0  # already-done undo was NOT re-run
    assert len(resumed_b.undo_calls) == 1
    assert len(resumed_a.undo_calls) == 1
    harness2.db.dispose()


# ============================================================================
# Point 9 — Run terminal: run status + finished_at + outcome event, one
# transaction. (No reachable park point exists strictly between "last step
# succeeded" and "run finalized" -- see module docstring. This test instead
# parks the LAST step at its own entry, proving nothing terminal is persisted
# while it's outstanding, then resumes and proves the terminal transition fires
# EXACTLY ONCE -- the guarantee point 9 actually protects.)
# ============================================================================


async def test_point9_run_terminal_not_persisted_before_last_step_then_fires_exactly_once_on_resume(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    first = InstantStep()
    last = PausableStep(verb="fake.p9.last", pause_execute=gate)
    wf = _outcome_block("crash-p9") + """steps:
  - id: first
    uses: fake.instant
    with: {message: "hello"}
  - id: last
    uses: fake.p9.last
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"crash-p9": wf}, [first, last], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-p9")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, gate)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
    assert run_row.status == "running"  # nothing terminal committed yet
    assert run_row.finished_at is None
    assert len(harness.dispatcher.calls) == 0  # no outcome event applied pre-crash
    harness.db.dispose()

    resumed_last = PausableStep(verb="fake.p9.last")
    harness2 = build_engine(tmp_path, {"crash-p9": wf}, [InstantStep(), resumed_last], clock)
    await _seed_ordinal_counter(harness2, run_id)  # workaround for a flagged bug -- see helper docstring
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
        rows = harness2.outbox_repo.list_for_aggregate(tx, "run", run_id)
    assert run_row.status == "succeeded"
    assert run_row.finished_at == NOW
    # exactly one outcome event, applied exactly once, only after resume
    assert len(harness2.dispatcher.calls) == 1
    assert type(harness2.dispatcher.calls[0].event).__name__ == "ProvisionSucceeded"
    job_topics = [json.loads(r.payload)["topic"] for r in rows if r.kind == "notify"]
    assert job_topics.count("job_started") == 1  # written by the FIRST harness, before crash
    assert job_topics.count("job_completed") == 1  # written by the SECOND harness, on resume
    harness2.db.dispose()


# ============================================================================
# Resume of a non-idempotent step interrupted mid-execute: fails WITHOUT
# re-entering execute() (§2.3.3 bullet: "if not idempotent -> mark failed").
# Direct DB-state crafting (see module docstring).
# ============================================================================


async def test_resume_non_idempotent_running_step_fails_without_reentering_execute(tmp_path):
    clock = FrozenClock(NOW)
    step = PausableStep(verb="fake.nonidempotent", idempotent=False)
    wf = _outcome_block("crash-nonidem") + """steps:
  - id: only
    uses: fake.nonidempotent
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"crash-nonidem": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-nonidem", status="running")
    async with harness.uow() as tx:
        harness.step_repo.insert(
            tx,
            WorkflowStepRow(
                run_id=run_id, step_path="only", verb="fake.nonidempotent", status="running",
                attempt=1, interrupted_count=0, params={"resource_id": "r-1"}, notes={},
                output=None, undo_status=None, error=None, started_at=NOW, finished_at=None,
            ),
        )

    await harness.engine.resume_inflight()
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert step.calls == 0  # execute() was NEVER re-entered
    assert step_row.status == "failed"
    assert step_row.error == {"kind": "permanent", "message": "interrupted; non-idempotent"}
    assert run_row.status == "failed"
    assert run_row.failed_step == "only"
    harness.db.dispose()


# ============================================================================
# resume_replay_limit crash-loop convergence (default = 5): a step that always
# parks at execute() entry, crashed and resumed repeatedly, converges to
# 'failed' once interrupted_count would exceed the budget -- WITHOUT ever
# succeeding, proving the loop can't run forever.
# ============================================================================


async def test_resume_replay_limit_crash_loop_converges_to_failed(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    step = PausableStep(verb="fake.loop", pause_execute=gate)
    wf = _outcome_block("crash-loop") + """steps:
  - id: only
    uses: fake.loop
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"crash-loop": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-loop")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, gate)  # initial run: interrupted_count stays 0

    for _ in range(5):  # resumes 1..5: interrupted_count climbs 0->1->2->3->4->5, still <= limit
        step.pause_execute = PauseGate()
        await harness.engine.resume_inflight()
        await crash_run(harness, run_id, step.pause_execute)

    # resume 6: existing.interrupted_count=5 -> new=6 > resume_replay_limit=5 -> fails outright
    await harness.engine.resume_inflight()
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "failed"
    assert step_row.status == "failed"
    assert step_row.interrupted_count == 5  # the failing resume never persisted a 6th bump
    assert "resume_replay_limit" in step_row.error["message"]
    assert step.calls == 6  # 1 initial + 5 resumes that proceeded to execute(); the 6th did not
    harness.db.dispose()


# ============================================================================
# Blocked runs adopted like running (Conflict 5 rule 5). Direct DB-state
# crafting (see module docstring: live-interrupting mid-park would run
# _park_and_wait's guarded finally, which a real crash never would).
# ============================================================================


async def test_resume_adopts_blocked_run_like_running(tmp_path):
    clock = FrozenClock(NOW)
    step = UnreachableNTimesStep(unreachable_times=1)  # parks once, then succeeds on reprobe
    wf = _outcome_block("crash-blocked") + """steps:
  - id: only
    uses: fake.unreachable_n_times
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"crash-blocked": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-blocked", status="blocked")
    async with harness.uow() as tx:
        harness.step_repo.insert(
            tx,
            WorkflowStepRow(
                run_id=run_id, step_path="only", verb="fake.unreachable_n_times", status="running",
                attempt=1, interrupted_count=2, params={"message": "hi"}, notes={},
                output=None, undo_status=None, error=None, started_at=NOW, finished_at=None,
            ),
        )

    async with harness.uow() as tx:
        resumable_ids = {r.id for r in harness.run_repo.resumable(tx)}
    assert run_id in resumable_ids  # 'blocked' holds the resumable slot (ACTIVE_RUN_STATUSES)

    await harness.engine.resume_inflight()
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "succeeded"  # restored from 'blocked', through the reprobe, to terminal
    assert step_row.status == "succeeded"
    assert step_row.interrupted_count == 3  # bumped exactly like a plain 'running' resume would be
    assert step.calls == 2  # 1 unreachable probe (parked) + 1 successful reprobe
    harness.db.dispose()


# ============================================================================
# cancel_requested set pre-crash -> straight to cancel path on adoption (G1).
# Direct DB-state crafting: the durable flag (as if cancel()'s commit landed
# just before the process died) is what resume must react to, not anything
# retained in memory.
# ============================================================================


async def test_resume_with_cancel_requested_precrash_goes_straight_to_cancel_path(tmp_path):
    clock = FrozenClock(NOW)
    step = InstantStep()
    wf = _outcome_block("crash-cancel") + """steps:
  - id: only
    uses: fake.instant
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"crash-cancel": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-cancel", status="running", cancel_requested=True)
    async with harness.uow() as tx:
        harness.step_repo.insert(
            tx,
            WorkflowStepRow(
                run_id=run_id, step_path="only", verb="fake.instant", status="running",
                attempt=1, interrupted_count=0, params={"message": "hi"}, notes={},
                output=None, undo_status=None, error=None, started_at=NOW, finished_at=None,
            ),
        )

    await harness.engine.resume_inflight()
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
    assert step.calls == []  # forward execution was skipped entirely -- G1/G2
    assert run_row.status == "cancelled"
    assert run_row.error["kind"] == "cancelled"
    assert len(harness.dispatcher.calls) == 1
    assert type(harness.dispatcher.calls[0].event).__name__ == "ProvisionFailed"
    assert harness.dispatcher.calls[0].event.reason == "cancelled"
    harness.db.dispose()


# ============================================================================
# FLAGGED DEVIATION, left failing on purpose (see _seed_ordinal_counter's
# docstring above): WorkflowEngine's Notify-ordinal counter is pure in-memory
# per-instance state, never seeded from persisted effects_outbox rows on
# adoption. A crash between two Notify writes for the same run (here:
# job_started before the crash, job_completed after resuming on a fresh engine
# instance) makes the resumed engine reuse ordinal 0, producing a colliding
# effect_id and a hard IntegrityError -- not a graceful failure, an unhandled
# exception out of the run's task. This is squarely a crash-matrix scenario
# (job_started always fires before any step even starts), not an edge case.
# ============================================================================


async def test_notify_ordinal_counter_is_not_seeded_after_resume(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    step = PausableStep(verb="fake.ordinalbug", pause_execute=gate)
    wf = _outcome_block("crash-ordinalbug") + """steps:
  - id: only
    uses: fake.ordinalbug
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"crash-ordinalbug": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "crash-ordinalbug")

    await harness.engine.start(run_id)
    await crash_run(harness, run_id, gate)
    harness.db.dispose()

    resumed_step = PausableStep(verb="fake.ordinalbug")
    harness2 = build_engine(tmp_path, {"crash-ordinalbug": wf}, [resumed_step], clock)
    await harness2.engine.resume_inflight()
    await harness2.engine.wait_for(run_id)  # raises sqlalchemy.exc.IntegrityError today

    async with harness2.uow() as tx:
        run_row = harness2.run_repo.get(tx, run_id)
    assert run_row.status == "succeeded"
    harness2.db.dispose()
