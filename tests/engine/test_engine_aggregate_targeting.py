"""DR-0055 decision 4: the engine targets each run's event at the aggregate the run belongs
to. ``_target_for`` used to treat every non-deploy run as a cluster run; a run now has
``cluster_id`` OR ``instance_id``.

The workflow definitions are the SHIPPED ones, so the concrete names the engine sees in
production (``provision-tart``, ``destroy-cloud``, ``deploy-waves``, ``deploy-rollback``)
are the ones under test. Runs are built in memory for `_target_for`; the adoption test goes
through ``engine.start`` against real tmp SQLite.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

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
