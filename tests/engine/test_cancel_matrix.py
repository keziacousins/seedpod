"""tests/engine/test_cancel_matrix.py — the cancellation matrix: G1-G5 (docs/design/
seam-b-engine.md §2.3.5, amended by coherence-review Conflict 8's outcome-event
mapping), tripped at each interesting point named in this task's brief:

  before run start · during step execute (ctx.sleep) · during backoff ·
  between gate polls · during a gate poll · during foreach between iterations ·
  during subprocess (real child, process-group-killed) · after cancel_requested
  is committed but before the in-process token trips (DB-serialized point-2
  re-read wins).

Every test asserts, per §2.3.5:

- **G2**: no new step/retry/poll starts once ``cancel_requested`` is committed --
  proven either by a next-step/next-iteration row never existing, or by a fake
  verb's call/poll counter not advancing past the point cancel landed.
- The interrupted step (where one exists) is recorded ``'cancelled'``.
- **G4**: per ``on_failure`` -- ``compensate`` runs full LIFO compensation on a
  FRESH, non-tripped ``CancelToken`` that cannot itself be cancelled (§2.3.4);
  ``report`` stops and marks, with zero undo calls.
- Exactly ONE ``outcome.cancelled`` event is applied via ``FakeDispatcher``,
  *regardless of where the cancel landed* -- and per coherence-review Conflict 8's
  outcome-event mapping table, a provision-shaped run's cancelled outcome is
  ``ProvisionFailed(reason="cancelled")`` (the actual event instance the engine
  built is asserted, not just its type name).

Run admission (Conflict 2) is out of this engine's job; every test inserts the
``workflow_runs`` row directly and hands the id to ``WorkflowEngine.start()``,
exactly like test_engine_smoke.py / test_crash_matrix.py.

**Two FLAGGED DEVIATIONS** (left failing on purpose, per this task's brief: "if
the engine deviates, leave the test failing... record the deviation"; see each
test's docstring and this module's report for detail): a cancel landing during
backoff (`_execute_with_retries`'s retry branch) or during the gate's
between-polls interval wait (`_run_gate`'s loop) raises a bare ``StepCancelled``
out of ``WorkflowEngine._run`` that nothing converts to the clean cancel path --
`seedpod/engine/engine.py`'s two `_cancel_aware_wait` call sites at those two
spots are not wrapped by a `try/except StepCancelled` the way the mid-execute and
mid-gate-poll call sites (both wrapped by `_probe_with_park`'s enclosing `try`)
are. `seedpod/engine/engine.py` is out of this task's edit scope, so these two
tests assert the SPEC behavior and are expected to fail against today's
implementation.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import UTC, datetime

from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.data.repositories import WorkflowRunRow
from tests.engine.fakes import (
    GatedSleeper,
    GateSleeperStep,
    PausableStep,
    PauseAfterExecuteStep,
    PauseGate,
    SleeperStep,
    SleepingUndoStep,
    SubprocessSleeperStep,
    build_engine,
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


async def _insert_run(harness, run_id: str, cluster_id: str, workflow: str, *, args: dict | None = None) -> None:
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
                args=args if args is not None else {"cluster_id": cluster_id},
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


def _outcome_block(workflow: str, on_failure: str = "report", extra_inputs: str = "") -> str:
    return f"""
workflow: {workflow}
version: 1
inputs:
  cluster_id: {{type: str}}
{extra_inputs}on_failure: {on_failure}
outcome:
  succeeded: {{event: ProvisionSucceeded, payload: {{public_ip: "1.2.3.4", kubeconfig_ref: "kc-1"}}}}
  failed:    {{event: ProvisionFailed, payload: {{reason: "n/a"}}}}
  cancelled: {{event: ProvisionFailed, payload: {{reason: "cancelled"}}}}
"""


async def _wait_until(predicate, *, timeout: float = 5.0) -> None:
    """Busy-polls (yielding via ``asyncio.sleep(0)``) until ``predicate()`` is
    true. Deterministic in single-threaded asyncio for the fake verbs this module
    uses: each one signals its "I have started" state synchronously (before its
    own first real await), so the very next loop-tick after the run's task is
    first scheduled observes it."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(0)


def _assert_single_cancelled_outcome(harness, cluster_id: str) -> None:
    """Conflict 8: a provision-shaped run's cancelled outcome is exactly
    ``ProvisionFailed(reason="cancelled")``, applied via the Dispatcher-shaped
    dependency exactly once, targeted ('cluster', run.cluster_id) -- regardless
    of where in the run the cancel landed (G4's last sentence)."""
    assert len(harness.dispatcher.calls) == 1
    call = harness.dispatcher.calls[0]
    assert call.aggregate == "cluster"
    assert call.aggregate_id == cluster_id
    assert type(call.event).__name__ == "ProvisionFailed"
    assert call.event.reason == "cancelled"


# ============================================================================
# Point: before run start
# ============================================================================


async def test_cancel_before_run_start_report_mode_never_starts(tmp_path):
    """``cancel(run_id)`` commits before the run is ever adopted; ``start()``'s
    very first act (``_run``) re-reads the row, sees ``cancel_requested`` on a
    still-'pending' row, and takes the cancel path before ``_mark_started`` even
    runs -- zero steps, zero job_started."""
    clock = FrozenClock(NOW)
    step = SleeperStep(seconds=60.0)
    wf = _outcome_block("cancel-before-start", on_failure="report") + """steps:
  - id: only
    uses: fake.sleeper
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"cancel-before-start": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "cancel-before-start")

    await harness.engine.cancel(run_id)  # no live task yet: commits the DB flag only
    await harness.engine.start(run_id)
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        rows = harness.step_repo.list_for_run(tx, run_id)
    assert run_row.status == "cancelled"
    assert run_row.started_at is None  # _mark_started never ran -- G2
    assert run_row.failed_step is None
    assert run_row.error == {"kind": "cancelled", "step": None, "message": "cancelled"}
    assert rows == []  # not one step row was ever inserted
    assert step.started == 0
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()


async def test_cancel_before_run_start_compensate_mode_trivially_compensates(tmp_path):
    """Same as above with ``on_failure: compensate``: LIFO compensation over zero
    steps is a no-op, but the run still reaches 'cancelled' (not 'failed') and
    fires exactly the one outcome event -- proving the compensate branch is
    actually exercised, not skipped."""
    clock = FrozenClock(NOW)
    step = SleeperStep(seconds=60.0)
    wf = _outcome_block("cancel-before-start-c", on_failure="compensate") + """steps:
  - id: only
    uses: fake.sleeper
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"cancel-before-start-c": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "cancel-before-start-c")

    await harness.engine.cancel(run_id)
    await harness.engine.start(run_id)
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
    assert run_row.status == "cancelled"  # not 'failed' -- _handle_cancel finalizes 'cancelled' always
    assert run_row.undo_incomplete is None
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()


# ============================================================================
# Point: during step execute (ctx.sleep) -- G3, report vs compensate (G4)
# ============================================================================


async def test_cancel_during_step_execute_sleeper_report_mode_stops_and_marks(tmp_path):
    """G3: cancelling while a step is parked in ``ctx.sleep`` raises
    ``StepCancelled`` immediately -- the sleep does not run to completion. The
    interrupted step is recorded 'cancelled'; ``on_failure: report`` stops
    without attempting any compensation."""
    clock = FrozenClock(NOW)
    step = SleeperStep(seconds=60.0)
    wf = _outcome_block("cancel-execute-report", on_failure="report") + """steps:
  - id: only
    uses: fake.sleeper
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"cancel-execute-report": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "cancel-execute-report")

    await harness.engine.start(run_id)
    await _wait_until(lambda: step.started >= 1)
    await harness.engine.cancel(run_id)
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "cancelled"
    assert run_row.failed_step == "only"
    assert step_row.status == "cancelled"  # the interrupted step, recorded per G4
    assert step_row.undo_status is None  # report mode: no compensation attempted at all
    assert step.cancelled == 1  # ctx.sleep actually raised StepCancelled, not just timed out
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()


async def test_cancel_during_step_execute_sleeper_compensate_mode_runs_full_lifo_on_fresh_uncancellable_token(
    tmp_path,
):
    """G4 + §2.3.4: the cancelled step ('sleeper', not undoable -> skipped) then
    every earlier succeeded undoable step in reverse order ('alloc') is undone.
    'alloc's undo runs on a FRESH ``CancelToken`` (never tripped by
    ``engine.cancel``) and sleeps for the duration of its own undo -- proving
    compensation completes even though the run's own token has been tripped the
    whole time it is running (compensation "cannot be cancelled", §2.3.4)."""
    clock = FrozenClock(NOW)
    alloc = SleepingUndoStep(undo_seconds=0.05)
    sleeper = SleeperStep(seconds=60.0)
    wf = _outcome_block("cancel-execute-compensate", on_failure="compensate") + """steps:
  - id: alloc
    uses: fake.sleeping_undo
    with: {resource_id: "r-a"}
  - id: sleeper
    uses: fake.sleeper
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"cancel-execute-compensate": wf}, [alloc, sleeper], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "cancel-execute-compensate")

    await harness.engine.start(run_id)
    await _wait_until(lambda: sleeper.started >= 1)

    handle = harness.engine._runs.get(run_id)  # noqa: SLF001 -- white-box: prove the token trips
    assert handle is not None
    assert handle.cancel_token.tripped is False

    await harness.engine.cancel(run_id)
    assert handle.cancel_token.tripped is True  # tripped immediately by cancel()
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        rows = {r.step_path: r for r in harness.step_repo.list_for_run(tx, run_id)}
    assert run_row.status == "cancelled"
    assert run_row.failed_step == "sleeper"
    assert run_row.undo_incomplete is None  # nothing left incomplete
    assert rows["sleeper"].status == "cancelled"
    assert rows["sleeper"].undo_status == "skipped"  # SleeperStep is not undoable
    assert rows["alloc"].status == "succeeded"
    assert rows["alloc"].undo_status == "done"  # completed DESPITE the run's token being tripped
    assert len(alloc.undo_calls) == 1
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()


# ============================================================================
# Point: during backoff -- FLAGGED DEVIATION, left failing on purpose
# ============================================================================


async def test_cancel_during_backoff_FLAGGED_DEVIATION_uncaught_stepcancelled_crashes_the_run(tmp_path):
    """FLAGGED DEVIATION (left failing on purpose; see this module's docstring and
    this task's report for the full analysis) -- Seam B §2.3.5 G2 promises "the
    engine checks the token before every retry, backoff sleep", i.e. a cancel
    landing while a step is mid-backoff must reach the same clean cancel path as
    a cancel landing mid-execute or mid-gate-poll.

    ``WorkflowEngine._execute_with_retries``'s retry branch calls
    ``self._cancel_aware_wait(...)`` (engine/engine.py) directly inside its
    ``except Exception as exc:`` handler -- NOT inside a nested
    try/except-StepCancelled the way the mid-execute probe is (that probe's
    ``except StepCancelled as exc: raise _RunCancelledSignal(...)`` sits on the
    SAME ``try`` the probe runs in, several lines above). When
    ``_cancel_aware_wait`` raises ``StepCancelled`` because the token tripped
    during the backoff sleep, nothing catches it: it propagates out of
    ``_execute_with_retries``, out of ``WorkflowEngine._run`` entirely (none of
    ``_run``'s except clauses name bare ``StepCancelled``), and the run's
    in-process task dies with an unhandled exception instead of reaching
    ``_handle_cancel``. Confirmed by direct repro: ``wait_for(run_id)`` re-raises
    ``StepCancelled("cancelled during engine-owned wait")``;
    ``workflow_runs.status`` stays 'running' forever; ``dispatcher.calls == []``.
    """
    clock = FrozenClock(NOW)
    sleep_gate = PauseGate()
    sleeper = GatedSleeper(gate=sleep_gate)
    step = PausableStep(verb="fake.backoff_cancel", fail_times=5)  # never succeeds within the test's lifetime
    wf = _outcome_block("cancel-backoff", on_failure="report") + """steps:
  - id: only
    uses: fake.backoff_cancel
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"cancel-backoff": wf}, [step], clock, sleeper=sleeper)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "cancel-backoff")

    await harness.engine.start(run_id)
    await asyncio.wait_for(sleep_gate.entered.wait(), timeout=5.0)  # parked in backoff after attempt 1's failure
    await harness.engine.cancel(run_id)
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)  # raises StepCancelled today

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "cancelled"  # spec: reaches the clean cancel path
    assert step_row.status == "cancelled"
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()


# ============================================================================
# Point: between gate polls -- FLAGGED DEVIATION, left failing on purpose
# ============================================================================


async def test_cancel_between_gate_polls_FLAGGED_DEVIATION_uncaught_stepcancelled_crashes_the_run(tmp_path):
    """FLAGGED DEVIATION (left failing on purpose; see this module's docstring and
    this task's report) -- the SAME underlying gap as the backoff test above,
    hit from ``WorkflowEngine._run_gate``'s loop instead: the interval wait
    between two polls (``await self._cancel_aware_wait(interval, state.token)``)
    sits AFTER the loop's own ``try/except StepCancelled`` block, at the same
    indentation as the ``try:`` itself -- not nested inside it. A cancel landing
    during that interval wait raises a bare ``StepCancelled`` that propagates all
    the way out of ``WorkflowEngine._run`` unhandled, exactly like the backoff
    case. Confirmed by direct repro: identical symptoms (``wait_for`` re-raises;
    status stuck 'running'; zero dispatcher calls).
    """
    clock = FrozenClock(NOW)
    interval_gate = PauseGate()
    sleeper = GatedSleeper(gate=interval_gate)
    step = PausableStep(verb="fake.gate_interval_cancel", gateable=True, ready_after=1000)  # never becomes Ready
    wf = _outcome_block("cancel-gate-interval", on_failure="report") + """steps:
  - id: only
    uses: fake.gate_interval_cancel
    with: {resource_id: "r-1"}
    gate: {timeout_seconds: 3600, interval_seconds: 5}
"""
    harness = build_engine(tmp_path, {"cancel-gate-interval": wf}, [step], clock, sleeper=sleeper)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "cancel-gate-interval")

    await harness.engine.start(run_id)
    await asyncio.wait_for(interval_gate.entered.wait(), timeout=5.0)  # parked between polls, after poll 1's NotReady
    assert step.polls == 1
    await harness.engine.cancel(run_id)
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)  # raises StepCancelled today

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "cancelled"  # spec: reaches the clean cancel path
    assert step_row.status == "cancelled"
    assert step.polls == 1  # spec: no poll #2 starts once cancel_requested is committed (G2)
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()


# ============================================================================
# Point: during a gate poll (mid poll_ready, distinct from between polls)
# ============================================================================


async def test_cancel_during_a_gate_poll_interrupts_promptly(tmp_path):
    """G3: a poll_ready that itself calls ``ctx.sleep`` (a realistic shape for a
    provider probe with its own internal wait) is interrupted immediately when
    the token trips WHILE the poll is in flight -- distinct from, and unaffected
    by, the between-polls deviation above (this call site IS wrapped by
    ``_run_gate``'s own ``except StepCancelled`` clause, on the same ``try`` the
    ``poll_ready`` probe runs in)."""
    clock = FrozenClock(NOW)
    step = GateSleeperStep(seconds=60.0)
    wf = _outcome_block("cancel-gate-poll", on_failure="report") + """steps:
  - id: only
    uses: fake.gate_sleeper
    with: {message: "hi"}
    gate: {timeout_seconds: 300, interval_seconds: 1}
"""
    harness = build_engine(tmp_path, {"cancel-gate-poll": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "cancel-gate-poll")

    await harness.engine.start(run_id)
    await _wait_until(lambda: step.polls >= 1)
    await harness.engine.cancel(run_id)
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "cancelled"
    assert step_row.status == "cancelled"
    assert step.polls == 1  # no poll #2 -- interrupted during poll #1 itself
    assert step.cancelled == 1  # the in-flight poll's ctx.sleep actually raised StepCancelled
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()


# ============================================================================
# Point: during foreach, between iterations
# ============================================================================


async def test_cancel_during_foreach_between_iterations_no_new_iteration_starts(tmp_path):
    """G2: iteration 0 finishes and is persisted 'succeeded' (its own step body
    never checks cancellation and completes on its own -- see
    ``PauseAfterExecuteStep``'s docstring); cancel is committed while the run is
    genuinely in the GAP between iterations; iteration 1's step row is never
    even inserted -- the DB-serialized re-read in ``_insert_step_row`` wins."""
    clock = FrozenClock(NOW)
    step = PauseAfterExecuteStep()
    wf = _outcome_block(
        "cancel-foreach", on_failure="report", extra_inputs='  items: {type: "list[str]"}\n'
    ) + """steps:
  - id: loop
    foreach: {items: {from: run.items}, as: item}
    body:
      - id: work
        uses: fake.pause_after_execute
        with: {message: {from: item}}
"""
    harness = build_engine(tmp_path, {"cancel-foreach": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(
        harness, run_id, cluster_id, "cancel-foreach", args={"cluster_id": cluster_id, "items": ["a", "b"]}
    )

    await harness.engine.start(run_id)
    await asyncio.wait_for(step.entered.wait(), timeout=5.0)  # iteration 0's body step is mid-execute
    await harness.engine.cancel(run_id)
    step.release.set()  # let iteration 0 finish on its own -- it never observed the cancel
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        rows = {r.step_path: r for r in harness.step_repo.list_for_run(tx, run_id)}
    assert run_row.status == "cancelled"
    assert rows.keys() == {"loop[0].work"}  # iteration 1's row was never inserted -- G2
    assert rows["loop[0].work"].status == "succeeded"  # iteration 0 completed cleanly, untouched
    assert step.calls == ["a"]  # only iteration 0's body ever ran
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()


# ============================================================================
# Point: during subprocess (real child, process-group-killed within grace)
# ============================================================================


async def test_cancel_during_subprocess_kills_real_child_process_group_within_grace(tmp_path):
    """G3: a step blocked in ``ctx.run_subprocess`` on a real, slow child process
    (``sleep 30``) is interrupted well within the grace window (process-group
    SIGTERM within ~1s per §2.3.5) -- proving cancellation genuinely reaches and
    kills live subprocess IO through the full engine, not just ``ctx`` in
    isolation (test_cancel.py already covers ``StepContext.run_subprocess`` on its
    own; this is the same guarantee end-to-end through ``WorkflowEngine``)."""
    clock = FrozenClock(NOW)
    step = SubprocessSleeperStep()
    wf = _outcome_block("cancel-subprocess", on_failure="report") + """steps:
  - id: only
    uses: fake.subprocess_sleeper
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"cancel-subprocess": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "cancel-subprocess")

    await harness.engine.start(run_id)
    await _wait_until(lambda: step.started >= 1)
    await asyncio.sleep(0.2)  # let the real child process actually spawn before killing it
    start = time.monotonic()
    await harness.engine.cancel(run_id)
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=15.0)
    elapsed = time.monotonic() - start

    assert elapsed < 5.0  # nowhere near the 30s sleep or the 10s SIGKILL grace
    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "cancelled"
    assert step_row.status == "cancelled"
    assert step.cancelled == 1  # ctx.run_subprocess actually raised StepCancelled after killing the group
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()


# ============================================================================
# Point: cancel_requested committed but the in-process token never trips
# (DB-serialized point-2 re-read wins)
# ============================================================================


async def test_cancel_db_serialized_re_read_wins_even_when_token_never_trips(tmp_path):
    """The G1/G2 mechanism does NOT depend on the in-process token: this test
    commits ``cancel_requested`` directly via ``WorkflowRunRepository.request_cancel``
    (bypassing ``WorkflowEngine.cancel`` entirely, so the token is provably NEVER
    tripped) and proves the run still lands on the clean cancel path, because
    ``_insert_step_row`` re-reads the DB flag before inserting the next step's row
    -- exactly the "DB-serialized, zero TOCTOU at boundaries" guarantee G2 names."""
    clock = FrozenClock(NOW)
    first = PauseAfterExecuteStep()
    second = SleeperStep(seconds=60.0)
    wf = _outcome_block("cancel-db-race", on_failure="report") + """steps:
  - id: first
    uses: fake.pause_after_execute
    with: {message: "hi"}
  - id: second
    uses: fake.sleeper
    with: {message: "world"}
"""
    harness = build_engine(tmp_path, {"cancel-db-race": wf}, [first, second], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "cancel-db-race")

    await harness.engine.start(run_id)
    await asyncio.wait_for(first.entered.wait(), timeout=5.0)

    async with harness.uow() as tx:
        harness.run_repo.request_cancel(tx, run_id)  # commits the flag -- never calls engine.cancel()

    handle = harness.engine._runs.get(run_id)  # noqa: SLF001 -- white-box: prove the token is untouched
    assert handle is not None
    assert handle.cancel_token.tripped is False  # the commit alone does not trip it

    first.release.set()  # "first" finishes on its own -- it never observed anything
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        rows = {r.step_path: r for r in harness.step_repo.list_for_run(tx, run_id)}
    assert run_row.status == "cancelled"
    assert rows.keys() == {"first"}  # "second" was never inserted -- the DB re-read alone blocked it
    assert rows["first"].status == "succeeded"
    assert second.started == 0
    _assert_single_cancelled_outcome(harness, cluster_id)
    harness.db.dispose()
