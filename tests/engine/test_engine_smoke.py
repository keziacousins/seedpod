"""tests/engine/test_engine_smoke.py — end-to-end smoke test for WorkflowEngine: a
3-step workflow (one gate, one emit) runs to 'succeeded' against fakes; both tables'
rows are correct; the outcome event is applied via FakeDispatcher with Conflict 8
targeting; job_started/job_completed Notify rows land in effects_outbox.

Run admission (Conflict 2) is NOT this engine's job -- this test inserts the
workflow_runs row directly, exactly as the real run-admitter would, then hands the
id to WorkflowEngine.start().
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.data.repositories import WorkflowRunRow
from tests.engine.fakes import GateReadyAfterKStep, InstantStep, build_engine

NOW = datetime(2026, 7, 13, 12, 0, 0, tzinfo=UTC)

WORKFLOW_YAML = """
workflow: provision-fake
version: 1
inputs:
  cluster_id: {type: str}
on_failure: report
outcome:
  succeeded: {event: ProvisionSucceeded, payload: {public_ip: {from: gate_step.echoed}, kubeconfig_ref: "kc-ref-1"}}
  failed:    {event: ProvisionFailed, payload: {reason: "n/a"}}
  cancelled: {event: ProvisionFailed, payload: {reason: "cancelled"}}
steps:
  - id: first
    uses: fake.instant
    with: {message: "hello"}
  - id: gate_step
    uses: fake.gate_ready_after_k
    with: {message: {from: first.echoed}}
    gate: {timeout_seconds: 30, interval_seconds: 1}
    emit: {event: EndpointReady, payload: {public_ip: {from: gate_step.echoed}}}
  - id: last
    uses: fake.instant
    with: {message: {from: gate_step.echoed}}
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


async def test_three_step_workflow_with_gate_and_emit_runs_to_succeeded(tmp_path):
    clock = FrozenClock(NOW)
    instant = InstantStep()
    gate = GateReadyAfterKStep(ready_after=2)
    harness = build_engine(tmp_path, {"provision-fake": WORKFLOW_YAML}, [instant, gate], clock)

    run_id = str(uuid.uuid4())
    cluster_id = str(uuid.uuid4())
    async with harness.uow() as tx:
        _insert_cluster(tx, cluster_id)
        harness.run_repo.insert(
            tx,
            WorkflowRunRow(
                id=run_id,
                workflow="provision-fake",
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

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    # ---- workflow_runs: terminal, succeeded -------------------------------------
    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
    assert run_row.status == "succeeded"
    assert run_row.started_at == NOW
    assert run_row.finished_at == NOW
    assert run_row.failed_step is None
    assert run_row.error is None
    assert run_row.undo_incomplete is None

    # ---- workflow_steps: three rows, all succeeded, gate really gated ----------
    async with harness.uow() as tx:
        steps = {s.step_path: s for s in harness.step_repo.list_for_run(tx, run_id)}
    assert set(steps) == {"first", "gate_step", "last"}
    for row in steps.values():
        assert row.status == "succeeded"
        assert row.finished_at is not None
    assert steps["first"].output == {"echoed": "hello"}
    assert steps["gate_step"].output == {"echoed": "hello-ready"}
    assert steps["last"].output == {"echoed": "hello-ready"}
    assert gate.polls == 2  # NotReady once, Ready on the 2nd poll (ready_after=2)
    assert instant.calls[0].message == "hello"
    assert instant.calls[1].message == "hello-ready"

    # ---- FakeDispatcher: emit (mid-run) + outcome (terminal), Conflict 8 targeting
    assert len(harness.dispatcher.calls) == 2
    emit_call, outcome_call = harness.dispatcher.calls
    assert emit_call.aggregate == "cluster"
    assert emit_call.aggregate_id == cluster_id
    assert type(emit_call.event).__name__ == "EndpointReady"
    assert emit_call.event.public_ip == "hello-ready"
    assert emit_call.event.actor == f"engine:run:{run_id}"

    assert outcome_call.aggregate == "cluster"
    assert outcome_call.aggregate_id == cluster_id
    assert type(outcome_call.event).__name__ == "ProvisionSucceeded"
    assert outcome_call.event.public_ip == "hello-ready"
    assert outcome_call.event.kubeconfig_ref == "kc-ref-1"

    # ---- job_started / job_completed Notify rows in effects_outbox -------------
    async with harness.uow() as tx:
        rows = harness.outbox_repo.list_for_aggregate(tx, "run", run_id)
    job_topics = [json.loads(r.payload)["topic"] for r in rows if r.kind == "notify"]
    assert "job_started" in job_topics
    assert "job_completed" in job_topics
    assert job_topics.index("job_started") < job_topics.index("job_completed")
    for r in rows:
        assert r.aggregate_type == "run"
        assert r.to_version == 0
        assert r.lane == "drain"
        assert r.status == "pending"

    # bonus: at least one workflow_progress row was written from the gate polls
    progress_topics = [json.loads(r.payload)["topic"] for r in rows if r.kind == "notify"]
    assert "workflow_progress" in progress_topics
