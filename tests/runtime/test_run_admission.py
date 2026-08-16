"""``seedpod/runtime/effect_executor.py``'s run-admitter -- docs/design/
coherence-review.md Conflict 2 ("the run-admission drain-rules comment block IS
the spec"), AS AMENDED BY docs/decisions/DR-0011-admitter-wait-and-run-conflict.md
(RATIFIED): ``run_workflow``/``cancel_workflow`` drain handling, H7 crash-replay
idempotency (``dedupe_key``), the generalized supersede-wait loop (DR-0011
Clause 1 -- destroy-flip+wait, and the cancel-then-rollback "victim already
unwinding" wait), the H14 run-conflict path as a durable, environment-scoped
outbox ``Notify`` row (DR-0011 Clause 2), and Conflict 12's
CancelWorkflow-before-RunWorkflow seq ordering (a cancelled deploy's rollback
waits for the victim run to reach terminal rather than being dropped as a
spurious conflict -- see ``effect_executor.py``'s ``_handle_active_run_conflict``
docstring for the full citation trail).

Real tmp SQLite (``tests/runtime/conftest.py``'s fixtures), ``FrozenClock``, and
hand-built fakes (``FakeEngine``) -- no Mock/patch anywhere (CLAUDE.md).
"""

from __future__ import annotations

from datetime import timedelta

from seedpod.core.effects import CancelWorkflow, RunWorkflow
from seedpod.core.events import CancelRequested
from seedpod.data.repositories import WorkflowRunRow
from seedpod.runtime.dispatcher import outbox_row
from seedpod.runtime.effect_executor import EffectExecutor
from tests.runtime.conftest import (
    NOW,
    FakeEngine,
    FakeWorkflowDefinition,
    make_cluster_row,
    make_deployment_row,
    make_run_row,
)


def _active_runs(repos_run_repo, t):
    return repos_run_repo.list_by_status(t, ["pending", "running", "blocked", "compensating"])


# ---------------------------------------------------------------------------
# Successful admission: version pinned at admission, hand-off via engine.start().
# ---------------------------------------------------------------------------


async def test_successful_admission_pins_workflow_version_and_hands_off_to_engine(uow, repos, executor, engine):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="provisioning"))
    engine.definitions["provision-fake"] = FakeWorkflowDefinition(version=3)
    eff = RunWorkflow(workflow="provision", cluster_id="c1")
    row = outbox_row(eff, "cluster", "c1", 1, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    await executor.drain_pending()

    async with uow() as t:
        runs = _active_runs(repos.workflow_runs, t)
        outbox_after = repos.outbox.get(t, row.effect_id)
    assert len(runs) == 1
    run = runs[0]
    assert run.workflow == "provision-fake"
    assert run.workflow_version == 3
    assert run.cluster_id == "c1"
    assert run.dedupe_key == row.effect_id
    assert run.status == "pending"
    assert engine.started == [run.id]
    assert outbox_after.status == "done"


# ---------------------------------------------------------------------------
# H7 crash-replay: a fresh executor instance re-drains a row whose admission
# already landed pre-crash -- exactly once, dedupe_key proving idempotency.
# ---------------------------------------------------------------------------


async def test_h7_crash_replay_drains_exactly_once(uow, repos, dispatch, clock, hub):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="provisioning"))
    eff = RunWorkflow(workflow="provision", cluster_id="c1")
    row = outbox_row(eff, "cluster", "c1", 1, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    # Simulate a PRIOR process's partial admission: the run row landed (and
    # committed) but the process crashed before marking the outbox row done or
    # handing off to the engine -- exactly what a crash between the two
    # `async with uow()` blocks in `_drain_run_workflow` leaves behind.
    pre_crash_run = WorkflowRunRow(
        id="run-1", workflow="provision-fake", workflow_version=1, cluster_id="c1", deployment_id=None,
        dedupe_key=row.effect_id, args={"cluster_id": "c1"}, status="pending", cancel_requested=False,
        failed_step=None, error=None, undo_incomplete=None, initiated_by=None,
        created_at=NOW, started_at=None, finished_at=None,
    )
    async with uow() as t:
        assert repos.workflow_runs.insert_admitted(t, pre_crash_run) is True

    # A fresh executor AND a fresh engine (empty in-process registry) -- exactly
    # the process-boundary this test proves.
    engine2 = FakeEngine(uow, repos.workflow_runs, {"provision-fake": FakeWorkflowDefinition(version=1)})
    executor2 = EffectExecutor(uow, repos, hub, engine2, dispatch, clock, poll_interval=10.0)

    await executor2.drain_pending()

    async with uow() as t:
        runs = _active_runs(repos.workflow_runs, t)
        outbox_after = repos.outbox.get(t, row.effect_id)
    assert len(runs) == 1  # no duplicate admitted
    assert runs[0].id == "run-1"
    assert outbox_after.status == "done"
    assert engine2.started == ["run-1"]  # hand-off happened despite the pre-existing row

    # Draining again (nothing left pending) genuinely no-ops -- still exactly one run.
    await executor2.drain_pending()
    async with uow() as t:
        runs_again = _active_runs(repos.workflow_runs, t)
    assert len(runs_again) == 1


# ---------------------------------------------------------------------------
# Destroy-supersede wait loop: blocked admission flips the victim + retries
# (attempts untouched) until the victim reaches terminal.
# ---------------------------------------------------------------------------


async def test_destroy_supersede_wait_loop_retries_without_incrementing_attempts(uow, repos, executor, engine, clock):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="destroy-scheduled"))
        repos.workflow_runs.insert(t, make_run_row("victim", "c1", workflow="provision-fake", status="running"))
    engine.definitions["destroy-cloud"] = FakeWorkflowDefinition(version=2)
    eff = RunWorkflow(workflow="destroy", cluster_id="c1")
    row = outbox_row(eff, "cluster", "c1", 2, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    await executor.drain_pending()

    # Blocked: victim cancelled (flip + "trip"); THIS row stays pending, deferred
    # +2s, attempts untouched -- waiting is not failure.
    assert engine.cancelled == ["victim"]
    async with uow() as t:
        after = repos.outbox.get(t, row.effect_id)
        victim = repos.workflow_runs.get(t, "victim")
        runs = _active_runs(repos.workflow_runs, t)
    assert after.status == "pending"
    assert after.attempts == 0
    assert after.available_at == NOW + timedelta(seconds=2)
    assert victim.cancel_requested is True
    assert {r.id for r in runs} == {"victim"}  # no destroy run admitted yet

    # Draining again before the deferred deadline is a genuine no-op (not due yet).
    await executor.drain_pending()
    async with uow() as t:
        still_waiting = repos.outbox.get(t, row.effect_id)
    assert still_waiting.attempts == 0
    assert still_waiting.status == "pending"

    # Victim reaches terminal (what the real engine's own run task would do
    # asynchronously) -- advance the clock past the defer and retry.
    async with uow() as t:
        repos.workflow_runs.update(t, "victim", status="cancelled", finished_at=clock.now())
    clock.advance(timedelta(seconds=2))
    await executor.drain_pending()

    async with uow() as t:
        after2 = repos.outbox.get(t, row.effect_id)
        runs2 = _active_runs(repos.workflow_runs, t)
    assert after2.status == "done"
    assert len(runs2) == 1
    new_run = runs2[0]
    assert new_run.workflow == "destroy-cloud"
    assert new_run.cluster_id == "c1"
    assert engine.started == [new_run.id]


# ---------------------------------------------------------------------------
# run_conflict (H14): a genuinely unrelated, still-live blocking run drops the
# request and notifies -- it is NOT retried. docs/decisions/DR-0011 Clause 2:
# the notification is a durable, environment-scoped outbox Notify row (NOT a
# direct broadcast) -- proved here by draining it through the SAME executor on
# a later pass, exactly like any other notify row.
# ---------------------------------------------------------------------------


async def test_run_conflict_for_non_destroy_marks_done_and_emits_durable_scoped_notify(
    uow, repos, executor, engine, hub
):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="active", environment="production"))
        repos.workflow_runs.insert(
            t, make_run_row("victim", "c1", workflow="deploy-waves", status="running", cancel_requested=False)
        )
    engine.definitions["deploy-waves"] = FakeWorkflowDefinition(version=1)
    eff = RunWorkflow(workflow="deploy", cluster_id="c1", deployment_id="d1")
    row = outbox_row(eff, "cluster", "c1", 3, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    await executor.drain_pending()

    assert engine.cancelled == []  # not a supersede -- nothing gets cancelled
    async with uow() as t:
        after = repos.outbox.get(t, row.effect_id)
        runs = _active_runs(repos.workflow_runs, t)
        notify_row = repos.outbox.get(t, f"{row.effect_id}#run_conflict")
    assert after.status == "done"
    assert {r.id for r in runs} == {"victim"}  # the conflicting request was dropped

    # The Notify row landed durably, scoped to the cluster's own environment
    # (DR-0011 Clause 2/DR-0010 extension), in the SAME pass that marked the
    # blocked RunWorkflow row done -- and it drained (delivered) too, since
    # drain_pending() loops until nothing is due.
    assert notify_row is not None
    assert notify_row.kind == "notify"
    assert notify_row.aggregate_type == "cluster"
    assert notify_row.aggregate_id == "c1"
    assert notify_row.status == "done"
    assert hub.calls == [
        (
            "run_conflict",
            {"workflow": "deploy", "cluster_id": "c1", "deployment_id": "d1", "blocked_by_run_id": "victim"},
            "production",
        )
    ]


async def test_run_conflict_notify_insert_is_replay_idempotent(uow, repos, executor, hub):
    """DR-0011 Clause 2's pinned crash test: ``effect_id =
    "{blocked_row.effect_id}#run_conflict"`` is deterministic and INSERTed
    ``ON CONFLICT DO NOTHING``, so a crash between the done-mark+insert
    transaction committing and the Notify's own delivery -- followed by
    whatever replay path re-derives the SAME blocked row and re-runs the
    conflict handling -- must not create a second Notify row or deliver twice.
    Drives ``_handle_active_run_conflict`` directly twice (the narrowest
    reproduction of "the same blocked row gets handled again"), then drains."""
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="active", environment="staging"))
        repos.workflow_runs.insert(
            t, make_run_row("victim", "c1", workflow="deploy-waves", status="running", cancel_requested=False)
        )
        cluster = repos.clusters.load(t, "c1")
    eff = RunWorkflow(workflow="deploy", cluster_id="c1", deployment_id="d1")
    row = outbox_row(eff, "cluster", "c1", 3, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    await executor._handle_active_run_conflict(row, eff, cluster)
    await executor._handle_active_run_conflict(row, eff, cluster)  # the "replay"

    async with uow() as t:
        all_rows = repos.outbox.list_for_aggregate(t, "cluster", "c1")
    run_conflict_rows = [r for r in all_rows if r.effect_id == f"{row.effect_id}#run_conflict"]
    assert len(run_conflict_rows) == 1  # ON CONFLICT DO NOTHING -- no duplicate row

    await executor.drain_pending()  # delivers the (single) Notify row

    assert hub.calls == [
        (
            "run_conflict",
            {"workflow": "deploy", "cluster_id": "c1", "deployment_id": "d1", "blocked_by_run_id": "victim"},
            "staging",
        )
    ]  # exactly one delivery, not two


# ---------------------------------------------------------------------------
# cancel_workflow: flips the active run + trips the token, marks its own row done.
# ---------------------------------------------------------------------------


async def test_cancel_workflow_flips_active_run_and_marks_done(uow, repos, executor, engine):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="active"))
        repos.workflow_runs.insert(t, make_run_row("victim", "c1", workflow="deploy-waves", status="running"))
    eff = CancelWorkflow(workflow="deploy", cluster_id="c1", deployment_id="d1")
    row = outbox_row(eff, "cluster", "c1", 4, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    await executor.drain_pending()

    assert engine.cancelled == ["victim"]
    async with uow() as t:
        after = repos.outbox.get(t, row.effect_id)
        victim = repos.workflow_runs.get(t, "victim")
    assert after.status == "done"
    assert victim.cancel_requested is True


async def test_cancel_workflow_with_no_active_run_is_a_harmless_no_op(uow, repos, executor, engine):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="active"))
    eff = CancelWorkflow(workflow="deploy", cluster_id="c1", deployment_id="d1")
    row = outbox_row(eff, "cluster", "c1", 1, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    await executor.drain_pending()

    assert engine.cancelled == []
    async with uow() as t:
        after = repos.outbox.get(t, row.effect_id)
    assert after.status == "done"


# ---------------------------------------------------------------------------
# Conflict 12: CancelWorkflow(deploy) THEN RunWorkflow(rollback) in the SAME
# transition's effect tuple -- seq ordering means rollback waits for the
# cancelled deploy run to reach terminal rather than being dropped (H14).
# ---------------------------------------------------------------------------


async def test_cw_before_rw_seq_ordering_rollback_waits_for_cancelled_deploy(uow, repos, dispatcher, engine, hub, dispatch, clock):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="active"))
        repos.deployments.insert(t, make_deployment_row("d1", "c1", status="deploying"))
        repos.workflow_runs.insert(
            t, make_run_row("deploy-run", "c1", workflow="deploy-waves", deployment_id="d1", status="running")
        )
    engine.definitions["deploy-rollback"] = FakeWorkflowDefinition(version=1)
    executor = EffectExecutor(uow, repos, hub, engine, dispatch, clock, poll_interval=10.0)

    await dispatcher.apply("deployment", "d1", CancelRequested(at=NOW, actor="api:test"))

    # The transition emitted (persist, notify, CancelWorkflow(deploy), RunWorkflow(rollback))
    # in that exact ordinal order -- confirm seq preserves it before draining.
    async with uow() as t:
        outbox_rows = repos.outbox.list_for_aggregate(t, "deployment", "d1")
    drain_lane = [r for r in outbox_rows if r.lane == "drain"]
    assert [r.kind for r in drain_lane] == ["notify", "cancel_workflow", "run_workflow"]
    assert drain_lane[1].seq < drain_lane[2].seq

    await executor.drain_pending()

    # CancelWorkflow processed: the blocking deploy run gets cancel_requested
    # flipped + "tripped".
    assert engine.cancelled == ["deploy-run"]
    async with uow() as t:
        deploy_run = repos.workflow_runs.get(t, "deploy-run")
        runs = _active_runs(repos.workflow_runs, t)
        rw_row = next(r for r in repos.outbox.list_for_aggregate(t, "deployment", "d1") if r.kind == "run_workflow")
        cw_row = next(r for r in repos.outbox.list_for_aggregate(t, "deployment", "d1") if r.kind == "cancel_workflow")
    assert deploy_run.cancel_requested is True
    assert {r.id for r in runs} == {"deploy-run"}  # rollback NOT admitted yet -- waiting, not conflicted
    assert rw_row.status == "pending"
    assert rw_row.attempts == 0  # waiting is not failure
    assert cw_row.status == "done"

    # The deploy run reaches terminal (what the real engine's own run task would
    # do asynchronously once its cancel token is tripped) -- rollback admits on
    # the next pass.
    async with uow() as t:
        repos.workflow_runs.update(t, "deploy-run", status="cancelled", finished_at=clock.now())
    clock.advance(timedelta(seconds=2))
    await executor.drain_pending()

    async with uow() as t:
        runs2 = _active_runs(repos.workflow_runs, t)
        rw_row_after = repos.outbox.get(t, rw_row.effect_id)
    rollback_runs = [r for r in runs2 if r.workflow == "deploy-rollback"]
    assert len(rollback_runs) == 1
    assert rollback_runs[0].deployment_id == "d1"
    assert rollback_runs[0].cluster_id == "c1"
    assert rw_row_after.status == "done"
    assert engine.started == [rollback_runs[0].id]
