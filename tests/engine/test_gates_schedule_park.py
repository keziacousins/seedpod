"""tests/engine/test_gates_schedule_park.py — the behavioral matrix for the THREE
distinct wait mechanisms docs/design/seam-b-engine.md §2.3.1/§2.3.2 and
docs/design/coherence-review.md Conflict 5 keep separate, never conflated:

1. **Schedule retry** (engine/schedule.py + ``_execute_with_retries``): bounded by
   ``max_attempts``, classification is fixed (``TransientError``/per-attempt-timeout ->
   retry, ``PermanentError``/anything else -> fail immediately, ``retry_after``
   overrides the computed backoff).
2. **Gate loop** (``_run_gate``): engine-owned poll interval + a SEPARATE
   consecutive-poll-failure hysteresis counter (never Schedule-retried) + an overall
   gate timeout; ``Ready(outputs=...)`` enrichment REPLACES the persisted output
   (persistence point 6).
3. **Conflict 5 blocked-park**: ``InfrastructureUnreachableError`` from execute or a
   gate poll NEVER consumes Schedule budget and NEVER touches the gate's
   consecutive-failure counter; it parks the run (status='blocked'), re-probes on a
   5s/15s/30s/60s-cap cadence up to ``unreachable_budget_seconds``, and restores the
   prior status on success with the attempt UNCHANGED. Exhaustion during a forward
   step fails the run with error kind 'unreachable' and skips compensation ENTIRELY
   (every step's ``undo_status`` -> 'skipped'), even on an ``on_failure: compensate``
   workflow. Exhaustion during an undo instead marks just THAT undo 'failed' (appended
   to ``run.undo_incomplete``) while the remaining LIFO undos still run.

No Mock/patch anywhere — every scenario drives a real ``WorkflowEngine`` (via
``tests/engine/fakes.py``'s ``build_engine``) against real SQLite, with purpose-built
fake verbs (some added to ``fakes.py`` for this file: ``RetryAfterNTimesStep``,
``TimeoutNTimesStep``, ``ScriptedGateStep``, ``UnreachableUndoStep``) and the
controllable ``InstantSleeper``/``GatedSleeper`` seams recording every engine-owned
wait duration.

Per this task's brief: two tests below are LEFT FAILING ON PURPOSE, documenting a
genuine engine.py gap rather than working around it (CLAUDE.md: "wanting an escape
hatch is the stop signal, not a judgment call" — the analogous rule for a discovered
bug is "a failing test that says so, not a rewritten assertion that hides it"). See
each test's docstring; the identical root cause is also documented in
tests/engine/test_cancel_matrix.py's "FLAGGED DEVIATION" tests.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.data.repositories import WorkflowRunRow
from tests.engine.fakes import (
    DetailBearingPermanentStep,
    GatedSleeper,
    GateReadyAfterKStep,
    PausableStep,
    PauseGate,
    PermanentStep,
    RetryAfterNTimesStep,
    ScriptedGateStep,
    TimeoutNTimesStep,
    TransientNTimesStep,
    UndoableEchoStep,
    UnreachableNTimesStep,
    UnreachableUndoStep,
    build_engine,
)

NOW = datetime(2026, 7, 13, 12, 0, 0, tzinfo=UTC)


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


async def _insert_run(harness, run_id: str, cluster_id: str, workflow: str) -> None:
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


# ============================================================================
# 1. Schedule retry — engine/schedule.py's fixed classification, driven end to
# end through WorkflowEngine._execute_with_retries.
# ============================================================================


async def test_transient_n_times_succeeds_within_budget(tmp_path):
    clock = FrozenClock(NOW)
    step = TransientNTimesStep(fail_times=2)  # default_retry=FAST_RETRY (max_attempts=10)
    wf = _outcome_block("gsp-transient-ok") + """steps:
  - id: only
    uses: fake.transient_n_times
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-transient-ok": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-transient-ok")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "succeeded"
    assert step_row.status == "succeeded"
    assert step_row.attempt == 3  # 2 failures + the succeeding 3rd attempt
    assert step.calls == 3
    harness.db.dispose()


async def test_transient_exhaustion_fails_with_the_last_error(tmp_path):
    clock = FrozenClock(NOW)
    step = TransientNTimesStep(fail_times=10)  # never succeeds within max_attempts=3
    wf = _outcome_block("gsp-transient-exhaust") + """steps:
  - id: only
    uses: fake.transient_n_times
    with: {message: "hi"}
    retry: {max_attempts: 3, base_delay_seconds: 0.01, factor: 1.0, max_delay_seconds: 0.01}
"""
    harness = build_engine(tmp_path, {"gsp-transient-exhaust": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-transient-exhaust")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "failed"
    assert run_row.failed_step == "only"
    assert step_row.status == "failed"
    assert step_row.attempt == 3
    assert step_row.error == {"kind": "transient", "message": "transient failure #3"}  # the LAST error
    assert step.calls == 3  # exactly max_attempts, no more
    harness.db.dispose()


async def test_permanent_error_fails_immediately_with_no_retry(tmp_path):
    clock = FrozenClock(NOW)
    step = PermanentStep()
    wf = _outcome_block("gsp-permanent") + """steps:
  - id: only
    uses: fake.permanent
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-permanent": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-permanent")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "failed"
    assert step_row.status == "failed"
    assert step_row.attempt == 1  # never bumped -- no retry was ever attempted
    assert step_row.error == {"kind": "permanent", "message": "permanent failure"}
    assert step.calls == 1
    assert harness.sleeper.requested == []  # no backoff wait was ever entered
    harness.db.dispose()


async def test_step_failure_message_carries_the_providers_stderr(tmp_path):
    """Backlog #18: ``ProviderError.detail``'s stderr reaches ``workflow_steps.error``.

    Smoke 8 (2026-08-09) persisted ``"kubectl.apply_manifest: invalid input"`` and
    nothing else. The stderr naming the offending document was computed by the
    classifier, attached to the exception, and dropped by ``_failure_message`` --
    diagnosing it meant decrypting the audit blob and re-running the apply against
    the live cluster to recover a string the process already had.
    """
    clock = FrozenClock(NOW)
    stderr = (
        'The Ingress "mailhog" is invalid: spec.rules[0].host: '
        'Invalid value: "203.0.113.40": must be a DNS name, not an IP address'
    )
    step = DetailBearingPermanentStep(stderr=stderr)
    wf = _outcome_block("gsp-detail") + """steps:
  - id: only
    uses: fake.permanent_with_detail
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-detail": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-detail")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    # The classifier's own message is preserved verbatim and the stderr APPENDED --
    # never substituted, so an already-diagnostic message never loses its prefix.
    assert step_row.error == {
        "kind": "permanent",
        "message": f"kubectl.apply_manifest: invalid input; stderr: {stderr}",
    }
    # The run-level row mirrors the step's text (``_fail_and_signal`` writes both in
    # one transaction), so the reason is on the row an operator reads first.
    assert run_row.error["message"] == step_row.error["message"]
    harness.db.dispose()


async def test_outcome_reason_carries_the_failure_message_not_just_the_kind(tmp_path):
    """DR-0039, and #18's sibling one layer up: ``clusters.failure_reason`` said
    ``"permanent"``.

    #18 got the stderr as far as ``workflow_steps.error``/``workflow_runs.error``. But
    the TERMINAL event -- the one the Dispatcher writes onto the cluster row, and the
    only thing ``GET /api/clusters/{id}`` and the SPA ever show -- carried
    ``reason = <error kind>``, so an operator saw the taxonomy bucket and nothing else.
    The 2026-08-12 tart run is the worked example: ``failure_reason: "permanent"`` on a
    dead cluster while ``install_k3s: exited 1 ... Download failed`` sat one table over,
    reachable only by opening sqlite.

    Note the outcome block deliberately omits ``reason`` from its payload, exactly as
    every shipped ``provision-*.yml``/``destroy-*.yml`` does -- a workflow that DOES
    supply one still wins (see the ``"n/a"`` assertions elsewhere in this file)."""
    clock = FrozenClock(NOW)
    stderr = "[ERROR]  Download failed"
    step = DetailBearingPermanentStep(stderr=stderr)
    wf = """
workflow: gsp-reason
version: 1
inputs:
  cluster_id: {type: str}
on_failure: report
outcome:
  succeeded: {event: ProvisionSucceeded, payload: {public_ip: "1.2.3.4", kubeconfig_ref: "kc-1"}}
  failed:    {event: ProvisionFailed}
  cancelled: {event: ProvisionFailed, payload: {reason: "cancelled"}}
steps:
  - id: only
    uses: fake.permanent_with_detail
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-reason": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-reason")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    outcome = harness.dispatcher.calls[-1]
    assert type(outcome.event).__name__ == "ProvisionFailed"
    # The KIND is still the prefix -- nothing that read it as a bucket is broken --
    # and the reason an operator actually needs now follows it.
    assert outcome.event.reason.startswith("permanent: ")
    assert stderr in outcome.event.reason
    harness.db.dispose()


async def test_step_failure_message_is_unchanged_when_no_stderr_is_attached(tmp_path):
    """#18's other half, matching DR-0033's: an error carrying no ``detail`` reads
    exactly as it did before. The engine reports what it was told; it never invents
    a reason, and never leaves a dangling ``"; stderr: "``."""
    clock = FrozenClock(NOW)
    step = PermanentStep()  # PermanentError with no detail at all
    wf = _outcome_block("gsp-no-detail") + """steps:
  - id: only
    uses: fake.permanent
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-no-detail": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-no-detail")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert step_row.error == {"kind": "permanent", "message": "permanent failure"}
    assert "stderr" not in step_row.error["message"]
    harness.db.dispose()


async def test_step_failure_message_truncates_a_pathological_stderr(tmp_path):
    """The cap is a bound on a runaway script, not a working limit -- but when it does
    bite it must say so, because nothing else persists the dropped remainder."""
    clock = FrozenClock(NOW)
    step = DetailBearingPermanentStep(stderr="x" * 2500)
    wf = _outcome_block("gsp-detail-long") + """steps:
  - id: only
    uses: fake.permanent_with_detail
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-detail-long": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-detail-long")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        step_row = harness.step_repo.get(tx, run_id, "only")
    message = step_row.error["message"]
    assert message.startswith("kubectl.apply_manifest: invalid input; stderr: " + "x" * 2000)
    assert message.endswith("… (+500 chars truncated)")
    harness.db.dispose()


async def test_retry_after_override_honored_over_computed_backoff(tmp_path):
    clock = FrozenClock(NOW)
    # RetryAfterNTimesStep's default_retry is FAST_RETRY (computed backoff ~0.01s):
    # if retry_after weren't honored, the sleeper would record ~0.01s, not 7.5s.
    step = RetryAfterNTimesStep(fail_times=2, retry_after=7.5)
    wf = _outcome_block("gsp-retry-after") + """steps:
  - id: only
    uses: fake.retry_after_n_times
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-retry-after": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-retry-after")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
    assert run_row.status == "succeeded"
    assert harness.sleeper.requested == [7.5, 7.5]  # retry_after verbatim, twice (2 failures)
    harness.db.dispose()


async def test_per_attempt_timeout_expiry_is_classified_as_transient_and_retried(tmp_path):
    clock = FrozenClock(NOW)
    step = TimeoutNTimesStep(slow_times=1)  # 1st call exceeds default_timeout_seconds=0.05
    wf = _outcome_block("gsp-timeout-retry") + """steps:
  - id: only
    uses: fake.timeout_n_times
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-timeout-retry": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-timeout-retry")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "succeeded"
    assert step_row.status == "succeeded"
    assert step_row.attempt == 2  # 1 timed-out attempt (retried) + 1 succeeding attempt
    assert step.calls == 2
    harness.db.dispose()


async def test_a_step_that_times_out_every_attempt_records_WHY_not_an_empty_message(tmp_path):
    """A per-attempt timeout that exhausts the Schedule must persist a message an
    operator can act on.

    Names smoke 4 (2026-08-08). During a real DigitalOcean outage in which "customers
    are unable to create Droplets" (status.digitalocean.com incident 2wql4f4sb13r),
    ``infra.create_instance`` failed twice -- once on a 504, once on a create that hung
    past the step's own 60s budget -- and BOTH rows persisted as
    ``{"kind": "transient", "message": ""}``. The engine recorded ``str(exc)`` verbatim
    and a bare ``TimeoutError`` (what ``asyncio.timeout`` raises) stringifies to ``""``,
    so the run history could not distinguish an upstream outage from a v2 defect --
    exactly the question a smoke run exists to answer.

    The ``kind`` is unchanged (``classify()`` still maps a timeout to RETRY -> transient,
    per the test directly above); only the previously-empty text is now diagnostic."""
    clock = FrozenClock(NOW)
    step = TimeoutNTimesStep(slow_times=99)  # never fast enough: every attempt expires
    wf = _outcome_block("gsp-timeout-exhausted") + """steps:
  - id: only
    uses: fake.timeout_n_times
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-timeout-exhausted": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-timeout-exhausted")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")

    assert run_row.status == "failed"
    assert step_row.status == "failed"
    assert step_row.error["kind"] == "transient"  # unchanged classification
    message = step_row.error["message"]
    assert message, "a failed step must never persist an empty message (smoke 4)"
    assert "TimeoutError" in message
    assert "0.05" in message  # the step budget that produced it, not a generic string
    assert run_row.error["message"] == message  # run-level text matches the step's
    harness.db.dispose()


async def test_backoff_wait_is_cancel_aware_FLAGGED_DEVIATION_uncaught_stepcancelled(tmp_path):
    """Schedule retry's backoff sleep must be cancel-aware (docs/design/
    seam-b-engine.md §2.3.2 persistence point 3: "cancel-aware backoff sleep") -- a
    cancel() landing while a step is mid-backoff should reach the SAME clean cancel
    path (run -> 'cancelled') as a cancel landing anywhere else.

    FLAGGED DEVIATION, left failing on purpose. Root cause (identical to the one
    tests/engine/test_cancel_matrix.py's
    ``test_cancel_during_backoff_FLAGGED_DEVIATION_uncaught_stepcancelled_crashes_the_run``
    already documents in detail; included here too because this task's brief
    explicitly names "backoff cancel-aware" as one of Schedule retry's required
    properties for THIS file's matrix): ``WorkflowEngine._execute_with_retries``'s
    ``await self._cancel_aware_wait(...)`` backoff call (engine/engine.py) sits
    OUTSIDE that method's own ``try/except StepCancelled`` block -- that block only
    wraps the ``execute()`` probe -- so a token trip during the backoff sleep raises a
    bare ``StepCancelled`` that propagates straight out of ``WorkflowEngine._run``
    unhandled (none of ``_run``'s ``except`` clauses name it). ``wait_for()`` below
    re-raises that ``StepCancelled`` to this test instead of the run ever reaching
    'cancelled'. seedpod/engine/engine.py is out of this task's edit scope.
    """
    clock = FrozenClock(NOW)
    sleep_gate = PauseGate()
    sleeper = GatedSleeper(gate=sleep_gate)
    step = TransientNTimesStep(fail_times=100)  # never succeeds within this test's lifetime
    wf = _outcome_block("gsp-backoff-cancel") + """steps:
  - id: only
    uses: fake.transient_n_times
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-backoff-cancel": wf}, [step], clock, sleeper=sleeper)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-backoff-cancel")

    await harness.engine.start(run_id)
    await asyncio.wait_for(sleep_gate.entered.wait(), timeout=5.0)  # parked mid-backoff after attempt 1 failed
    await harness.engine.cancel(run_id)
    await asyncio.wait_for(harness.engine.wait_for(run_id), timeout=5.0)  # raises StepCancelled today

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
    assert run_row.status == "cancelled"
    harness.db.dispose()


# ============================================================================
# 2. Gate loop — engine-owned poll interval, SEPARATE hysteresis counter (never
# Schedule), overall timeout, Ready(outputs=...) enrichment.
# ============================================================================


async def test_gate_ready_after_k_passes(tmp_path):
    clock = FrozenClock(NOW)
    gate_step = GateReadyAfterKStep(ready_after=3)
    wf = _outcome_block("gsp-gate-k") + """steps:
  - id: only
    uses: fake.gate_ready_after_k
    with: {message: "hi"}
    gate: {timeout_seconds: 30, interval_seconds: 1}
"""
    harness = build_engine(tmp_path, {"gsp-gate-k": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-k")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "succeeded"
    assert step_row.status == "succeeded"
    assert gate_step.polls == 3  # NotReady x2, Ready on poll 3
    assert step_row.output == {"echoed": "hi-ready"}
    harness.db.dispose()


async def test_gate_settle_seconds_is_a_post_ready_grace_distinct_from_interval(tmp_path):
    """DR-0022 Erratum E2: `settle_seconds` is a ONE-TIME grace after the gate
    reaches Ready, not folded into the poll interval -- v1's
    destruction_job.py:164-181 "give Tailscale a few extra seconds to send
    disconnect" semantic, preserved as gate data. Two NotReady polls (each
    followed by the 1s interval wait) then Ready on the 3rd poll must record
    exactly [1, 1, 3] on the sleeper -- the settle appears ONCE, at the end,
    never repeated per-poll and never conflated with interval_seconds."""
    clock = FrozenClock(NOW)
    gate_step = GateReadyAfterKStep(ready_after=3)
    wf = _outcome_block("gsp-gate-settle") + """steps:
  - id: only
    uses: fake.gate_ready_after_k
    with: {message: "hi"}
    gate: {timeout_seconds: 30, interval_seconds: 1, settle_seconds: 3}
"""
    harness = build_engine(tmp_path, {"gsp-gate-settle": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-settle")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "succeeded"
    assert step_row.status == "succeeded"
    assert gate_step.polls == 3
    assert harness.sleeper.requested == [1, 1, 3]  # two interval waits, then the settle -- once
    harness.db.dispose()


async def test_gate_settle_seconds_never_runs_when_the_gate_times_out(tmp_path):
    """The settle grace is Ready-only -- a gate that never reaches Ready (here:
    it exhausts its overall timeout first) must never request the settle
    wait at all."""
    clock = FrozenClock(NOW)
    gate_step = GateReadyAfterKStep(ready_after=100)  # never reaches Ready in time
    wf = _outcome_block("gsp-gate-settle-timeout") + """steps:
  - id: only
    uses: fake.gate_ready_after_k
    with: {message: "hi"}
    gate: {timeout_seconds: 2, interval_seconds: 1, settle_seconds: 3}
"""
    harness = build_engine(tmp_path, {"gsp-gate-settle-timeout": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-settle-timeout")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
    assert run_row.status == "failed"
    assert 3 not in harness.sleeper.requested  # settle never requested -- Ready was never reached
    harness.db.dispose()


async def test_gate_transient_polls_increment_and_reset_on_a_successful_poll(tmp_path):
    gate_step_script = ["transient", "transient", "notready", "transient", "transient", "ready"]
    clock = FrozenClock(NOW)
    gate_step = ScriptedGateStep(gate_step_script)
    wf = _outcome_block("gsp-gate-reset") + """steps:
  - id: only
    uses: fake.gate_scripted
    with: {message: "hi"}
    gate: {timeout_seconds: 60, interval_seconds: 1, max_consecutive_poll_failures: 3}
"""
    harness = build_engine(tmp_path, {"gsp-gate-reset": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-reset")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    # never 3 CONSECUTIVE transient failures -- the "notready" in the middle reset the
    # counter, so both transient pairs (2 each, < max=3) are individually survivable
    assert run_row.status == "succeeded"
    assert gate_step.polls == len(gate_step_script)
    assert step_row.output == {"echoed": "hi-ready"}
    harness.db.dispose()


async def test_gate_fails_at_max_consecutive_poll_failures(tmp_path):
    clock = FrozenClock(NOW)
    gate_step = ScriptedGateStep(["transient", "transient", "transient", "ready"])
    wf = _outcome_block("gsp-gate-maxfail") + """steps:
  - id: only
    uses: fake.gate_scripted
    with: {message: "hi"}
    gate: {timeout_seconds: 60, interval_seconds: 1, max_consecutive_poll_failures: 3}
"""
    harness = build_engine(tmp_path, {"gsp-gate-maxfail": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-maxfail")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "failed"
    assert step_row.status == "failed"
    assert step_row.error["kind"] == "transient"
    assert "max_consecutive_poll_failures=3" in step_row.error["message"]
    assert gate_step.polls == 3  # fails on the 3rd consecutive transient poll -- "ready" never reached
    harness.db.dispose()


async def test_gate_permanent_poll_fails_immediately_no_hysteresis(tmp_path):
    clock = FrozenClock(NOW)
    gate_step = ScriptedGateStep(["permanent"])
    wf = _outcome_block("gsp-gate-permanent") + """steps:
  - id: only
    uses: fake.gate_scripted
    with: {message: "hi"}
    gate: {timeout_seconds: 60, interval_seconds: 1, max_consecutive_poll_failures: 3}
"""
    harness = build_engine(tmp_path, {"gsp-gate-permanent": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-permanent")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "failed"
    assert step_row.error["kind"] == "permanent"
    assert gate_step.polls == 1  # fails on the FIRST poll -- no retry, no interval wait
    assert harness.sleeper.requested == []
    harness.db.dispose()


async def test_gate_overall_timeout_enforced(tmp_path):
    clock = FrozenClock(NOW)
    gate_step = ScriptedGateStep(["notready"])  # never becomes Ready
    wf = _outcome_block("gsp-gate-timeout") + """steps:
  - id: only
    uses: fake.gate_scripted
    with: {message: "hi"}
    gate: {timeout_seconds: 2, interval_seconds: 1, max_consecutive_poll_failures: 100}
"""
    harness = build_engine(tmp_path, {"gsp-gate-timeout": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-timeout")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "failed"
    assert step_row.error["kind"] == "permanent"
    assert "gate timed out" in step_row.error["message"]
    # DR-0033 point 1: the message names the LAST NotReady detail. Before this, every gate
    # timeout in v2 reported only the elapsed budget -- the reason was computed by the step,
    # thrown away by the gate, and a `deploy.await_wave` timeout could not say which service
    # hung. ScriptedGateStep's "notready" detail is f"poll {n}", so the last poll wins.
    assert "; last poll: poll 2" in step_row.error["message"]
    harness.db.dispose()


async def test_gate_timeout_message_omits_detail_when_the_step_never_supplies_one(tmp_path):
    """DR-0033 point 1's other half: a step returning a bare ``NotReady()`` must read
    exactly as it did before the change -- the gate reports what it was told and never
    invents a reason."""
    clock = FrozenClock(NOW)
    gate_step = ScriptedGateStep(["notready_bare"])
    wf = _outcome_block("gsp-gate-timeout-bare") + """steps:
  - id: only
    uses: fake.gate_scripted
    with: {message: "hi"}
    gate: {timeout_seconds: 2, interval_seconds: 1, max_consecutive_poll_failures: 100}
"""
    harness = build_engine(tmp_path, {"gsp-gate-timeout-bare": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-timeout-bare")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert step_row.error["message"] == "gate timed out after 2.0s"
    assert "last poll" not in step_row.error["message"]
    harness.db.dispose()


async def test_gate_timeout_keeps_the_last_INFORMATIVE_detail(tmp_path):
    """A later bare ``NotReady()`` must not erase an earlier informative detail. This is the
    realistic shape: a step that can only sometimes explain itself (``k3s.await_ssh`` gets a
    detail from the provider on a refused dial but not on every code path) would otherwise
    lose the one poll that actually knew something."""
    clock = FrozenClock(NOW)
    gate_step = ScriptedGateStep(["notready", "notready_bare"])  # informative, then silent forever
    wf = _outcome_block("gsp-gate-timeout-sticky") + """steps:
  - id: only
    uses: fake.gate_scripted
    with: {message: "hi"}
    gate: {timeout_seconds: 3, interval_seconds: 1, max_consecutive_poll_failures: 100}
"""
    harness = build_engine(tmp_path, {"gsp-gate-timeout-sticky": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-timeout-sticky")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert gate_step.calls == ["notready", "notready_bare", "notready_bare"]
    assert "; last poll: poll 1" in step_row.error["message"]
    harness.db.dispose()


async def test_gate_ready_outputs_enrichment_replaces_the_persisted_output(tmp_path):
    clock = FrozenClock(NOW)
    gate_step = GateReadyAfterKStep(ready_after=1)
    wf = _outcome_block("gsp-gate-enrich") + """steps:
  - id: only
    uses: fake.gate_ready_after_k
    with: {message: "provisional-value"}
    gate: {timeout_seconds: 30, interval_seconds: 1}
"""
    harness = build_engine(tmp_path, {"gsp-gate-enrich": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-enrich")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        step_row = harness.step_repo.get(tx, run_id, "only")
    # the provisional output from execute() alone would have been {"echoed": "provisional-value"};
    # Ready(outputs=...) REPLACES it entirely with the enriched value (persistence point 6)
    assert step_row.output == {"echoed": "provisional-value-ready"}
    assert step_row.output != {"echoed": "provisional-value"}
    harness.db.dispose()


# ============================================================================
# 3. Conflict 5 blocked-park — InfrastructureUnreachableError from execute or a
# gate poll: never Schedule-classified, never touches the gate hysteresis
# counter, parks status='blocked', re-probes 5/15/30/60s-cap, restores on
# success with attempt unchanged; exhaustion diverges by forward-vs-undo.
# ============================================================================


async def test_unreachable_from_execute_parks_blocked_attempt_unchanged_then_restores_to_running(tmp_path):
    clock = FrozenClock(NOW)
    park_gate = PauseGate()
    sleeper = GatedSleeper(gate=park_gate)
    unreachable_step = UnreachableNTimesStep(unreachable_times=1)  # 1 park cycle, then succeeds
    second_gate = PauseGate()
    second = PausableStep(verb="fake.gsp.second", pause_execute=second_gate)
    wf = _outcome_block("gsp-park-basic") + """steps:
  - id: only
    uses: fake.unreachable_n_times
    with: {message: "hi"}
  - id: second
    uses: fake.gsp.second
    with: {resource_id: "r-1"}
"""
    harness = build_engine(tmp_path, {"gsp-park-basic": wf}, [unreachable_step, second], clock, sleeper=sleeper)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-park-basic")

    await harness.engine.start(run_id)
    await asyncio.wait_for(park_gate.entered.wait(), timeout=5.0)  # parked in the 5s reprobe wait

    # ---- while parked: run is 'blocked'; step attempt UNCHANGED (no Schedule budget consumed) ----
    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "blocked"
    assert step_row.status == "running"
    # default_retry=NAMED_POLICIES["none"] (max_attempts=1) -- if Unreachable had consumed
    # Schedule budget, this step would already be exhausted/failed, not blocked-and-reprobing.
    assert step_row.attempt == 1

    park_gate.release()
    await asyncio.wait_for(second_gate.entered.wait(), timeout=5.0)  # "only" reprobed+succeeded, "second" started

    # ---- reachability restored: run back to 'running' (not still 'blocked'), same attempt ----
    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "running"
    assert step_row.status == "succeeded"
    assert step_row.attempt == 1  # resumed the SAME attempt -- Unreachable never bumped it
    assert unreachable_step.calls == 2  # 1 parked probe + 1 successful reprobe

    second_gate.release()
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
    assert run_row.status == "succeeded"
    harness.db.dispose()


async def test_unreachable_reprobe_cadence_is_5_15_30_60_capped(tmp_path):
    clock = FrozenClock(NOW)
    step = UnreachableNTimesStep(unreachable_times=5)  # 5 unreachable probes -> 5 reprobe waits, then succeeds
    wf = _outcome_block("gsp-cadence") + """steps:
  - id: only
    uses: fake.unreachable_n_times
    with: {message: "hi"}
"""
    harness = build_engine(tmp_path, {"gsp-cadence": wf}, [step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-cadence")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    assert run_row.status == "succeeded"
    assert step_row.attempt == 1  # still never bumped
    assert step.calls == 6  # 5 unreachable probes + 1 successful reprobe
    assert harness.sleeper.requested == [5.0, 15.0, 30.0, 60.0, 60.0]  # 5/15/30/60-cap, default schedule
    harness.db.dispose()


async def test_unreachable_budget_exhausted_forward_fails_and_skips_compensation_entirely(tmp_path):
    clock = FrozenClock(NOW)
    a = UndoableEchoStep()
    stuck = UnreachableNTimesStep(unreachable_times=1000)  # never succeeds within the tiny test budget
    wf = _outcome_block("gsp-exhaust-fwd", on_failure="compensate") + """steps:
  - id: a
    uses: test.undoable_echo
    with: {message: "hi"}
  - id: stuck
    uses: fake.unreachable_n_times
    with: {message: "hi"}
"""
    harness = build_engine(
        tmp_path, {"gsp-exhaust-fwd": wf}, [a, stuck], clock,
        unreachable_budget_seconds=2.0, unreachable_reprobe_schedule=(1.0,),
    )
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-exhaust-fwd")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        a_row = harness.step_repo.get(tx, run_id, "a")
        stuck_row = harness.step_repo.get(tx, run_id, "stuck")
    assert run_row.status == "failed"
    assert run_row.failed_step == "stuck"
    assert run_row.error == {"kind": "unreachable", "step": "stuck", "message": "unreachable_budget exhausted"}
    assert run_row.undo_incomplete is None
    # compensation SKIPPED ENTIRELY -- even for the undoable, already-succeeded "a" step,
    # even though this workflow declares on_failure: compensate
    assert a_row.undo_status == "skipped"
    assert stuck_row.undo_status == "skipped"
    assert a.undo_calls == []  # undo() was never actually invoked
    # Backlog #18, one layer up from `_failure_message`: the engine caught, classified
    # and discarded every one of those InfrastructureUnreachableErrors, so a run that
    # parked for the full budget failed saying only "unreachable_budget exhausted" --
    # unreachable HOW was the one question the record could not answer. The step row
    # now carries the last probe's reason.
    assert stuck_row.error == {
        "kind": "unreachable",
        "message": "unreachable_budget exhausted during execute; last probe: still unreachable",
    }
    harness.db.dispose()


async def test_unreachable_during_undo_marks_that_undo_failed_but_remaining_undos_still_run(tmp_path):
    clock = FrozenClock(NOW)
    a = UndoableEchoStep()
    b = UnreachableUndoStep()  # always raises Unreachable from undo() -- exhausts the tiny budget below
    c = PermanentStep()  # triggers compensation; not undoable -- skipped immediately
    wf = _outcome_block("gsp-exhaust-undo", on_failure="compensate") + """steps:
  - id: a
    uses: test.undoable_echo
    with: {message: "hi"}
  - id: b
    uses: fake.unreachable_undo
    with: {message: "hi"}
  - id: c
    uses: fake.permanent
    with: {message: "hi"}
"""
    harness = build_engine(
        tmp_path, {"gsp-exhaust-undo": wf}, [a, b, c], clock,
        unreachable_budget_seconds=2.0, unreachable_reprobe_schedule=(1.0,),
    )
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-exhaust-undo")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        rows = {r.step_path: r for r in harness.step_repo.list_for_run(tx, run_id)}
    assert run_row.status == "failed"
    assert run_row.failed_step == "c"
    assert run_row.undo_incomplete == ["b"]
    assert rows["c"].undo_status == "skipped"  # "c" is not undoable
    assert rows["b"].undo_status == "failed"  # b's undo exhausted its unreachable budget
    assert rows["a"].undo_status == "done"  # the REMAINING undo ("a", earlier in LIFO order) still ran
    assert len(a.undo_calls) == 1
    assert b.undo_calls >= 1
    harness.db.dispose()


async def test_unreachable_gate_poll_does_not_feed_the_transient_failure_counter(tmp_path):
    clock = FrozenClock(NOW)
    gate_step = ScriptedGateStep(["transient", "transient", "unreachable", "transient", "ready"])
    wf = _outcome_block("gsp-gate-unreach-counter") + """steps:
  - id: only
    uses: fake.gate_scripted
    with: {message: "hi"}
    gate: {timeout_seconds: 600, interval_seconds: 1, max_consecutive_poll_failures: 3}
"""
    harness = build_engine(tmp_path, {"gsp-gate-unreach-counter": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-unreach-counter")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    # 2 direct "transient" polls + 1 "unreachable" (parks/reprobes; itself uncounted) + 1 more
    # "transient" (the reprobe's own outcome) == exactly 3 COUNTED transient failures -> the gate
    # fails HERE, never reaching the script's final "ready" entry. If the "unreachable" poll had
    # itself incremented the counter, the gate would have failed one poll EARLIER (immediately at
    # the "unreachable" call), without ever making this 4th call at all.
    assert run_row.status == "failed"
    assert step_row.error["kind"] == "transient"
    assert "max_consecutive_poll_failures=3" in step_row.error["message"]
    assert gate_step.polls == 4  # "ready" (the script's 5th entry) never reached
    harness.db.dispose()


async def test_gate_timeout_clock_is_suspended_while_parked(tmp_path):
    clock = FrozenClock(NOW)
    gate_step = ScriptedGateStep(["notready", "unreachable", "unreachable", "ready"])
    wf = _outcome_block("gsp-gate-suspend") + """steps:
  - id: only
    uses: fake.gate_scripted
    with: {message: "hi"}
    gate: {timeout_seconds: 6, interval_seconds: 5, max_consecutive_poll_failures: 100}
"""
    harness = build_engine(tmp_path, {"gsp-gate-suspend": wf}, [gate_step], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "gsp-gate-suspend")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        step_row = harness.step_repo.get(tx, run_id, "only")
    # the two park reprobes alone record 5s + 15s == 20s of "requested" sleep -- already past
    # gate_timeout=6s -- yet the gate SUCCEEDS: none of that parked time counts against the
    # gate's own elapsed/timeout clock, which only ever advanced by ONE interval (5s, from the
    # single "notready" poll) before the run succeeded via the park's eventual "ready" reprobe.
    assert run_row.status == "succeeded"
    assert step_row.output == {"echoed": "hi-ready"}
    assert sum(harness.sleeper.requested) > 6  # more "requested" seconds than gate_timeout allows
    harness.db.dispose()
