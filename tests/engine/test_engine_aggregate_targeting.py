"""DR-0055 decision 4: the engine targets each run's event at the aggregate the run belongs
to. ``_target_for`` used to treat every non-deploy run as a cluster run; a run now has
``cluster_id`` OR ``instance_id``.

The workflow definitions are the SHIPPED ones, so the concrete names the engine sees in
production (``provision-tart``, ``destroy-cloud``, ``deploy-waves``, ``deploy-rollback``)
are the ones under test. Runs are built in memory for `_target_for`; the adoption test goes
through ``engine.start`` against real tmp SQLite.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.data.repositories import WorkflowRunRow
from seedpod.engine.cancel import CancelToken
from seedpod.engine.config import load_workflow
from seedpod.engine.engine import _RunState
from tests.engine.declared_verbs import ShippedWorkflowRegistry
from tests.engine.fakes import build_engine

NOW = datetime(2026, 7, 13, 12, 0, 0, tzinfo=UTC)
WORKFLOWS_DIR = Path(__file__).resolve().parents[2] / "config" / "workflows"


def _definition(name: str):
    return load_workflow((WORKFLOWS_DIR / f"{name}.yml").read_text(), ShippedWorkflowRegistry())


def _run(workflow: str, **ids) -> WorkflowRunRow:
    fields = {"cluster_id": None, "deployment_id": None, **ids}
    return WorkflowRunRow(
        id="run-1", workflow=workflow, workflow_version=1, dedupe_key=None, args={}, status="running",
        cancel_requested=False, failed_step=None, error=None, undo_incomplete=None, initiated_by=None,
        created_at=NOW, started_at=NOW, finished_at=None, **fields,
    )


@pytest.fixture
def engine(tmp_path):
    return build_engine(tmp_path, {}, [], FrozenClock(NOW)).engine


def _target(engine, workflow: str, **ids):
    state = _RunState(run_id="run-1", wf=_definition(workflow), token=CancelToken(), row=_run(workflow, **ids))
    return engine._target_for(state)


@pytest.mark.parametrize("workflow", ["provision-tart", "provision-digitalocean", "provision-kind", "destroy-cloud", "destroy-shared"])
def test_cluster_runs_target_their_cluster(engine, workflow):
    assert _target(engine, workflow, cluster_id="c1") == ("cluster", "c1")


@pytest.mark.parametrize("workflow", ["deploy-waves", "deploy-rollback"])
def test_deploy_and_rollback_runs_still_target_their_deployment(engine, workflow):
    # A deploy run carries BOTH ids: its cluster holds the one-active-run slot, its deployment is the target.
    assert _target(engine, workflow, cluster_id="c1", deployment_id="d1") == ("deployment", "d1")


def test_an_instance_run_targets_its_instance(engine):
    # No instance workflow ships yet; the engine's rule is on the run's row, not on a workflow name.
    assert _target(engine, "provision-tart", instance_id="i1") == ("instance", "i1")
    assert _target(engine, "destroy-cloud", instance_id="i1") == ("instance", "i1")


def test_a_deploy_run_with_no_deployment_is_an_error_not_a_cluster_target(engine):
    with pytest.raises(ValueError, match="no deployment_id"):
        _target(engine, "deploy-waves", cluster_id="c1")


async def _insert_instance_and_run(h, *, status: str, workflow: str = "provision-tart", finished_at=None):
    run_id, instance_id = str(uuid.uuid4()), str(uuid.uuid4())
    async with h.uow() as tx:
        tx.execute(
            text("INSERT INTO api_keys (key_hash, username, created_at) VALUES ('h', 'u', :now)"),
            {"now": NOW.isoformat()},
        )
        tx.execute(
            text(
                "INSERT INTO instances (id, slug, provider, environment, template, size, status,"
                " owner_key_id, lease_seconds, expires_at, hard_expires_at, hourly_price, vcpu,"
                " memory_mb, created_at, updated_at) VALUES (:id, 'box', 'tart', 'e', 't', 's',"
                " 'provisioning', 1, 60, :now, :now, 0, 1, 1, :now, :now)"
            ),
            {"id": instance_id, "now": NOW.isoformat()},
        )
        h.run_repo.insert(tx, WorkflowRunRow(
            id=run_id, workflow=workflow, workflow_version=1, cluster_id=None,
            instance_id=instance_id, deployment_id=None, dedupe_key=None, args={}, status=status,
            cancel_requested=False, failed_step=None, error=None, undo_incomplete=None,
            initiated_by=None, created_at=NOW, started_at=None, finished_at=finished_at,
        ))
    return run_id, instance_id


async def test_an_instance_run_with_no_definition_fails_for_that_reason_and_reports_to_its_instance(tmp_path):
    """The temporary guard that failed every instance run is gone. An instance run now takes the
    same path as a cluster run, so one whose workflow is not defined fails for THAT reason. Unlike a
    cluster run, it also reports the failure to its instance (from the workflow's name, since there
    is no definition to read an outcome from): the instance is waiting on it."""
    h = build_engine(tmp_path, {}, [], FrozenClock(NOW))
    run_id, _ = await _insert_instance_and_run(h, status="pending")

    await h.engine.start(run_id)
    await h.engine.wait_for(run_id)

    async with h.uow() as tx:
        run = h.run_repo.get(tx, run_id)
        steps = h.step_repo.list_for_run(tx, run_id)
    assert run.status == "failed" and run.finished_at == NOW
    assert run.error["kind"] == "permanent" and "unknown workflow 'provision-tart'" in run.error["message"]
    assert steps == []
    (call,) = h.dispatcher.calls  # exactly one event, to the instance, and nothing for a cluster
    assert (call.aggregate, type(call.event).__name__) == ("instance", "InstanceProvisionFailed")
    assert "unknown workflow 'provision-tart'" in call.event.reason
    assert call.tx is not None  # in the transaction that marked the run failed


@pytest.mark.parametrize("status", ["succeeded", "failed", "cancelled"])
async def test_a_terminal_instance_run_is_never_rewritten_when_it_is_adopted_again(tmp_path, status):
    """The old guard wrote `failed` over a `succeeded` instance run. Nothing that replaces it may touch
    a run that is already terminal -- not the unknown-workflow failure, not the outcome event."""
    h = build_engine(tmp_path, {}, [], FrozenClock(NOW))
    finished = NOW.replace(hour=1)
    run_id, _ = await _insert_instance_and_run(h, status=status, finished_at=finished)
    async with h.uow() as tx:
        h.run_repo.update(tx, run_id, error={"kind": "permanent", "step": None, "message": "original"})
        before = h.run_repo.get(tx, run_id)

    await h.engine.start(run_id)  # a replayed admission
    await h.engine.wait_for(run_id)
    await h.engine.resume_inflight()  # and the startup sweep, which only picks up live runs

    async with h.uow() as tx:
        after = h.run_repo.get(tx, run_id)
    assert after == before
    assert after.status == status and after.finished_at == finished
    assert h.dispatcher.calls == []


@pytest.mark.parametrize("status", ["succeeded", "failed", "cancelled"])
async def test_a_terminal_cluster_run_with_no_definition_is_handled_exactly_as_before_dr_0055(tmp_path, status):
    """The terminal-run early return is for INSTANCE runs. A cluster run takes the path it took at
    HEAD, which includes a KNOWN quirk: adopting a finished run whose workflow has no definition
    rewrites it to `failed`. This test pins "unchanged", not "right" -- the quirk is the owner's to
    raise as its own issue, and the day it is fixed this assertion flips with it."""
    h = build_engine(tmp_path, {}, [], FrozenClock(NOW))
    async with h.uow() as tx:
        tx.execute(
            text(
                "INSERT INTO clusters (id, name, slug, environment, status, provider, created_at, updated_at)"
                " VALUES ('c1', 'c1', 'c1', 'e', 'active', 'tart', :now, :now)"
            ),
            {"now": NOW.isoformat()},
        )
        h.run_repo.insert(tx, WorkflowRunRow(
            id="r1", workflow="no-such-workflow", workflow_version=1, cluster_id="c1", instance_id=None,
            deployment_id=None, dedupe_key=None, args={}, status=status, cancel_requested=False,
            failed_step=None, error=None, undo_incomplete=None, initiated_by=None, created_at=NOW,
            started_at=NOW, finished_at=NOW,
        ))
    await h.engine.start("r1")
    await h.engine.wait_for("r1")
    async with h.uow() as tx:
        run = h.run_repo.get(tx, "r1")
    assert run.status == "failed"  # HEAD: rewritten, whatever it was
    assert run.error["message"] == "unknown workflow 'no-such-workflow'"
