"""Shared fixtures for ``seedpod/runtime/dispatcher.py``, ``effect_executor.py``,
and ``timers.py`` tests: real tmp SQLite (``migrate()`` onto ``tmp_path``,
mirroring ``tests/data/test_workflow_repos.py``'s pattern), a real
``UnitOfWork``/``Repositories`` bundle, and a ``FrozenClock``. No Mock/patch
anywhere -- fakes below are plain, hand-built classes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from seedpod.core.clock import FrozenClock
from seedpod.core.records import ClusterState, DeploymentState, Origin
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import (
    ClusterRepository,
    ClusterRow,
    ClusterStateAuditRepository,
    DeploymentRepository,
    DeploymentRow,
    DeploymentStateAuditRepository,
    OutboxRepository,
    Repositories,
    TimerRepository,
    WorkflowRunRepository,
    WorkflowRunRow,
)
from seedpod.data.uow import UnitOfWork
from seedpod.engine.dispatch_table import WorkflowDispatch
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.runtime.effect_executor import EffectExecutor

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)


class FakePokeable:
    """Records ``poke()`` calls; asserts nothing on its own (CLAUDE.md: no Mock)."""

    def __init__(self) -> None:
        self.count = 0

    def poke(self) -> None:
        self.count += 1


@pytest.fixture
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'dispatcher.db'}")
    migrate(database.engine)
    return database


@pytest.fixture
def uow(db):
    return UnitOfWork(db)


@pytest.fixture
def repos():
    return Repositories(
        clusters=ClusterRepository(),
        deployments=DeploymentRepository(),
        cluster_state_audits=ClusterStateAuditRepository(),
        deployment_state_audits=DeploymentStateAuditRepository(),
        timers=TimerRepository(),
        outbox=OutboxRepository(),
        workflow_runs=WorkflowRunRepository(),
    )


@pytest.fixture
def clock():
    return FrozenClock(NOW)


@pytest.fixture
def dispatcher(uow, repos, clock):
    return Dispatcher(uow, repos, clock)


# ---------------------------------------------------------------------------
# effect_executor.py fixtures/fakes -- FakeHub records broadcasts (optionally
# raising, to exercise the notify one-attempt/best-effort discipline);
# FakeEngine records start()/cancel() calls and exposes a plain, test-mutable
# `definitions` dict (no live WorkflowEngine task registry needed for these
# tests -- `cancel()` mirrors the real engine's documented contract by writing
# `cancel_requested` through the SAME UnitOfWork/WorkflowRunRepository the
# test's own fixtures use, then recording the call).
# ---------------------------------------------------------------------------


@dataclass
class FakeWorkflowDefinition:
    version: int


class FakeHub:
    def __init__(self, raise_on: frozenset[str] = frozenset()) -> None:
        self.calls: list[tuple[str, dict, str | None]] = []
        self._raise_on = raise_on

    def broadcast(self, type: str, data, environment: str | None = None) -> None:  # noqa: A002
        if type in self._raise_on:
            raise RuntimeError(f"FakeHub configured to raise on broadcast({type!r})")
        self.calls.append((type, dict(data), environment))


@dataclass
class FakeEngine:
    uow: UnitOfWork
    run_repo: WorkflowRunRepository
    definitions: dict = field(default_factory=dict)
    started: list = field(default_factory=list)
    cancelled: list = field(default_factory=list)

    async def start(self, run_id: str) -> None:
        self.started.append(run_id)

    async def cancel(self, run_id: str) -> None:
        async with self.uow() as t:
            self.run_repo.request_cancel(t, run_id)
        self.cancelled.append(run_id)


@pytest.fixture
def dispatch():
    return WorkflowDispatch(
        destroy_by_provider={"fake": "destroy-cloud", "digitalocean": "destroy-cloud"},
    )


@pytest.fixture
def hub():
    return FakeHub()


@pytest.fixture
def engine(uow, repos):
    return FakeEngine(uow, repos.workflow_runs)


@pytest.fixture
def executor(uow, repos, hub, engine, dispatch, clock):
    return EffectExecutor(uow, repos, hub, engine, dispatch, clock, poll_interval=0.02)


# ---------------------------------------------------------------------------
# Row builders (mirrors tests/data/test_machine_repos.py's make_cluster_row /
# make_deployment_row -- fixture setup only, never goes through Dispatcher).
# ---------------------------------------------------------------------------


def make_cluster_row(cluster_id: str, slug: str, *, status: str = "active", **overrides) -> ClusterRow:
    fields = {
        "id": cluster_id,
        "name": cluster_id,
        "slug": slug,
        "origin": Origin.MANAGED,
        "environment": "ephemeral",
        "repository": None,
        "branch": None,
        "status": status,
        "pre_destroy_state": None,
        "version": 0,
        "provider": "fake",
        "provider_config": {},
        "provider_resources": {},
        "dns_hostname": None,
        "dns_zone": None,
        "dns_record_id": None,
        "public_ip": None,
        "node_count": 1,
        "encrypted_kubeconfig": None,
        "kubeconfig_key_class": None,
        "kubeconfig_ref": None,
        "cost_per_hour": 0.0,
        "total_cost": 0.0,
        "consecutive_health_failures": 0,
        "failure_reason": None,
        "last_reconciled_at": None,
        "created_at": NOW,
        "updated_at": NOW,
        "expires_at": None,
    }
    fields.update(overrides)
    return ClusterRow(**fields)


def make_deployment_row(deployment_id: str, cluster_id: str, *, status: str = "active", **overrides) -> DeploymentRow:
    fields = {
        "id": deployment_id,
        "cluster_id": cluster_id,
        "environment": "ephemeral",
        "status": status,
        "version": 0,
        "manifest_version": "v1",
        "spec_ref": None,
        "resolved_images": {},
        "superseded_by": None,
        "deployed_by": "api:test",
        "failure_reason": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return DeploymentRow(**fields)


def make_run_row(run_id: str, cluster_id: str, *, workflow: str = "deploy-waves", status: str = "running", **overrides) -> WorkflowRunRow:
    fields = {
        "id": run_id,
        "workflow": workflow,
        "workflow_version": 1,
        "cluster_id": cluster_id,
        "deployment_id": None,
        "dedupe_key": f"dedupe-{run_id}",
        "args": {},
        "status": status,
        "cancel_requested": False,
        "failed_step": None,
        "error": None,
        "undo_incomplete": None,
        "initiated_by": None,
        "created_at": NOW,
        "started_at": NOW,
        "finished_at": None,
    }
    fields.update(overrides)
    return WorkflowRunRow(**fields)


__all__ = [
    "NOW",
    "FakePokeable",
    "FakeHub",
    "FakeEngine",
    "FakeWorkflowDefinition",
    "make_cluster_row",
    "make_deployment_row",
    "make_run_row",
    "ClusterState",
    "DeploymentState",
]
