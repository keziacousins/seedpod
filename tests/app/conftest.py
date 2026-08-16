"""Shared fixtures for ``tests/app/test_services_*.py`` -- real tmp SQLite
(``migrate()``), a real ``UnitOfWork``/``Repositories``/``Dispatcher`` stack, a
``FrozenClock``, and a hand-built ``RuleEngine`` (no YAML file needed -- its
constructor is a plain ``RuleConfig`` value, so tests build the exact rule set
their own matrix needs without touching disk). No Mock/patch anywhere.

These are SERVICE-level tests (CLAUDE.md/this round's brief): they construct
``ClusterService``/``DeploymentService``/``SecretService``/``ApiKeyService``
directly, not through ``build_app()`` -- the HTTP-level parity gate is a later
Round-6 component's job.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from seedpod.core.clock import FrozenClock
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import (
    ApiKeyRepository,
    ClusterRepository,
    ClusterStateAuditRepository,
    DeploymentAuditRepository,
    DeploymentRepository,
    DeploymentStateAuditRepository,
    OutboxRepository,
    Repositories,
    SecretAuditRepository,
    SecretRepository,
    TimerRepository,
    WorkflowRunRepository,
)
from seedpod.data.uow import UnitOfWork
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.services.crypto import CryptoService
from seedpod.services.manifests import ManifestResolver
from seedpod.services.rules import Rule, RuleConfig, RuleEngine
from tests.fakes import sequential_ids

NOW = datetime(2026, 7, 16, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(NOW)


@pytest.fixture
def id_gen():
    return sequential_ids()


@pytest.fixture
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'services.db'}")
    migrate(database.engine)
    return database


@pytest.fixture
def uow(db):
    return UnitOfWork(db)


@pytest.fixture
def repos() -> Repositories:
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
def dispatcher(uow, repos, clock) -> Dispatcher:
    return Dispatcher(uow, repos, clock)


@pytest.fixture
def crypto() -> CryptoService:
    return CryptoService(dev_key=Fernet.generate_key(), prod_key=Fernet.generate_key())


@pytest.fixture
def deployment_audits_repo(crypto) -> DeploymentAuditRepository:
    return DeploymentAuditRepository(crypto)


@pytest.fixture
def secrets_repo(crypto) -> SecretRepository:
    return SecretRepository(crypto)


@pytest.fixture
def secret_audits_repo() -> SecretAuditRepository:
    return SecretAuditRepository()


@pytest.fixture
def api_keys_repo() -> ApiKeyRepository:
    return ApiKeyRepository()


@pytest.fixture
def manifest_resolver() -> ManifestResolver:
    return ManifestResolver(ghcr_service=None)


@pytest.fixture
def rules() -> RuleEngine:
    """The exact matrix ``tests/fixtures/deployment-rules.yml`` encodes (feature
    branches -> ephemeral, main -> staging), PLUS one disabled rule this fixture
    file doesn't carry -- built as a plain ``RuleConfig`` value rather than a
    second on-disk fixture, since ``RuleEngine.__init__`` takes one directly."""
    config = RuleConfig(
        version="1.0",
        global_ephemeral_enabled=True,
        default_ttl_hours=2,
        defaults={"ephemeral": {"ttl_hours": 2}, "persistent": {}},
        rules=(
            Rule(
                name="feature_branches", description="", enabled=True,
                branch_patterns=("feature/*",), action="create_ephemeral",
                config={"ttl_hours": 2, "deployment_profile": "test-profile"},
            ),
            Rule(
                name="main_staging", description="", enabled=True,
                branch_patterns=("main",), action="update_environment",
                config={"environment": "staging", "deployment_profile": "test-profile"},
            ),
            Rule(
                name="disabled_rule", description="", enabled=False,
                branch_patterns=("disabled/*",), action="create_ephemeral",
                config={"deployment_profile": "test-profile"},
            ),
        ),
        valid_actions=("create_ephemeral", "update_environment", "no_action"),
        valid_environments=("dev", "staging", "production"),
    )
    return RuleEngine(config)


@pytest.fixture
def config_dir(tmp_path) -> Path:
    """A minimal ``config/`` tree: one deployment profile (``test-profile``) whose
    single service IS the triggering repo used throughout these tests
    (``exampleco-core``) -- the "triggering repo shortcut"
    (``ManifestResolver._resolve_service_images``) resolves it without any GHCR
    call, so ``ghcr_service=None`` never blocks these tests on image resolution."""
    root = tmp_path / "config"
    profiles_dir = root / "deployment-profiles"
    profiles_dir.mkdir(parents=True)
    (profiles_dir / "test-profile.yml").write_text(
        """
version: "1.0"
description: "test profile"
manifests_dir: "config/manifest-templates/test-profile"
resolution_strategy: "branch_discovery_with_fallback"
provider: "fake"
cluster_spec:
  cluster_config:
    node_count: 1
services:
  exampleco-core:
    repository: "exampleco-core"
    required: true
"""
    )
    templates_dir = root / "manifest-templates" / "test-profile"
    templates_dir.mkdir(parents=True)
    (templates_dir / "exampleco-core.yaml").write_text(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: test\ndata:\n  image: \"{{ images.exampleco_core }}\"\n"
    )
    return root
