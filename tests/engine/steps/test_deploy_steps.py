"""tests/engine/steps/test_deploy_steps.py — Round 10's "load-and-plan"
component: ``deploy.load_audit``, ``deploy.plan_waves`` (the crown jewel),
``deploy.prepare_wave`` (``seedpod/engine/steps/deploy.py``).

``deploy.load_audit`` runs against a real tmp SQLite DB (``migrate()``), a
real ``UnitOfWork``, a real ``CryptoService`` (Fernet) -- matching
``test_domain_steps.py``'s own idiom for domain steps. ``deploy.plan_waves``
is pure, no IO, tested directly against literal ``ManifestDoc``/
``DeploymentProfile`` values. ``deploy.prepare_wave`` runs against the REAL
``KubectlProvider`` over the conformance fake transport
(``tests/conformance/kubectl_harness.py``) -- matching
``test_kube_destroy_steps.py``'s own idiom for ``kube.*`` composites.

DR-0025 Erratum E2's DEFERRED case gets its own section below
("deploy.load_audit -- DR-0025 Erratum E2"): a pending audit must be
recognised via the explicit ``resolved_config[DEFERRED_MANIFEST_RENDERING_KEY]``
marker, never inferred from empty manifests, and must raise rather than ever
return ``manifests=[]`` -- and an audit that is empty for any OTHER
(non-deferred) reason must raise too, distinguishably.

No Mock/patch anywhere (CLAUDE.md testing posture).
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from pathlib import Path

import pytest
from cryptography.fernet import Fernet
from pydantic import SecretStr

from seedpod.core.deploy_wave import (
    DEFERRED_MANIFEST_RENDERING_KEY,
    DeploymentProfile,
    ManifestDoc,
    RestoreFromLatest,
    SnapshotRestoreSpec,
)
from seedpod.core.errors import ErrorCode, InfrastructureUnreachableError, PermanentError
from seedpod.core.records import Origin
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import (
    ClusterRepository,
    ClusterRow,
    DeploymentAuditRepository,
    DeploymentAuditRow,
    DeploymentRepository,
    DeploymentRow,
)
from seedpod.data.uow import UnitOfWork
from seedpod.engine.step import EmptyOutput, StepServices
from seedpod.engine.steps.deploy import (
    DeleteJobsParams,
    DeployLoadAudit,
    DeployLoadAuditParams,
    DeployPrepareWave,
    PlanWaves,
    PlanWavesParams,
    _is_stuck_pod,
)
from seedpod.providers.contract import KubeGetPods
from seedpod.services.crypto import CryptoService
from seedpod.services.manifests import ManifestResolver
from tests.conformance.harness import Fault
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG, KubectlHarness
from tests.engine.fakes import FakeSubprocessManager, make_step_context

NOW = datetime(2026, 8, 7, 12, 0, 0, tzinfo=UTC)
_KUBECONFIG = SecretStr(FAKE_KUBECONFIG)
_REPO_ROOT = Path(__file__).resolve().parents[3]
_REAL_CONFIG_DIR = _REPO_ROOT / "config"


def _load_audit_step(
    uow, deployments, deployment_audits, *, clusters=None, manifest_resolver=None, config_dir=None
) -> DeployLoadAudit:
    """Every ``DeployLoadAudit`` construction in this module goes through here
    -- the restore-and-rehydrate component's three NEW required dependencies
    (``clusters``/``manifest_resolver``/``config_dir``) are only ever
    EXERCISED by the DR-0025 Erratum E2 rehydration tests below; every other
    test in this file never reaches ``_rehydrate`` at all (no deferred
    marker in its fixture data), so a fresh, real, stateless
    ``ClusterRepository()``/``ManifestResolver(ghcr_service=None)`` (neither
    holds any state; both are IO-free to construct) plus the real
    ``config/`` tree are safe, inert defaults for every OTHER test."""
    return DeployLoadAudit(
        uow=uow,
        deployments=deployments,
        deployment_audits=deployment_audits,
        clusters=clusters if clusters is not None else ClusterRepository(),
        manifest_resolver=manifest_resolver if manifest_resolver is not None else ManifestResolver(ghcr_service=None),
        config_dir=config_dir if config_dir is not None else _REAL_CONFIG_DIR,
    )


# ---------------------------------------------------------------------------
# deploy.load_audit -- real sqlite fixtures + birth helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'deploy_steps.db'}")
    migrate(database.engine)
    return database


@pytest.fixture
def uow(db) -> UnitOfWork:
    return UnitOfWork(db)


@pytest.fixture
def clusters() -> ClusterRepository:
    return ClusterRepository()


@pytest.fixture
def deployments() -> DeploymentRepository:
    return DeploymentRepository()


@pytest.fixture
def crypto() -> CryptoService:
    return CryptoService(dev_key=Fernet.generate_key(), prod_key=Fernet.generate_key())


@pytest.fixture
def deployment_audits(crypto) -> DeploymentAuditRepository:
    return DeploymentAuditRepository(crypto)


def _birth_cluster_row(cluster_id: str, *, environment: str = "ephemeral") -> ClusterRow:
    return ClusterRow(
        id=cluster_id, name=cluster_id, slug=cluster_id, origin=Origin.MANAGED, environment=environment,
        repository="exampleco-core", branch="feature/x", status="active", pre_destroy_state=None, version=0,
        provider="digitalocean", provider_config={}, provider_resources={}, dns_hostname=None, dns_zone=None, dns_record_id=None,
        public_ip=None, node_count=1, encrypted_kubeconfig=None, kubeconfig_key_class=None, kubeconfig_ref=None,
        cost_per_hour=0.0, total_cost=0.0, consecutive_health_failures=0, failure_reason=None,
        last_reconciled_at=None, created_at=NOW, updated_at=NOW, expires_at=None,
    )


def _audit_row(
    audit_id: str,
    *,
    cluster_id: str,
    resolved_manifests: str,
    resolved_config: dict | None = None,
    resolved_images: dict | None = None,
) -> DeploymentAuditRow:
    return DeploymentAuditRow(
        id=audit_id, deployment_id=None, cluster_id=cluster_id, environment="ephemeral",
        triggering_repo="exampleco-core", triggering_branch="feature/x", triggering_image="ghcr.io/exampleco/core:x",
        commit_sha="abc123", deployment_profile_name="exampleco-dev-stack-nodns",
        resolution_strategy="branch_discovery_with_fallback", registry_queries=[],
        resolved_images=resolved_images or {}, resolved_config=resolved_config or {},
        resolved_manifests=resolved_manifests, resolved_secrets={}, key_class="DEV",
        template_files_used=["postgres.yaml"], created_at=NOW,
    )


def _deployment_row(deployment_id: str, *, cluster_id: str, spec_ref: str | None) -> DeploymentRow:
    return DeploymentRow(
        id=deployment_id, cluster_id=cluster_id, environment="ephemeral", status="deploying", version=0,
        manifest_version="exampleco-dev-stack-nodns", spec_ref=spec_ref, resolved_images={}, superseded_by=None,
        deployed_by=None, failure_reason=None, created_at=NOW, updated_at=NOW,
    )


_MANIFEST_YAML = """\
kind: ServiceAccount
metadata:
  name: exampleco-api-sa
  namespace: default
---
kind: Deployment
metadata:
  name: postgres
  namespace: default
  labels:
    app: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:16
---
kind: Job
metadata:
  name: exampleco-atlas-migrations
  namespace: default
spec:
  template:
    spec:
      containers:
        - name: migrate
          image: exampleco-atlas:latest
---
kind: Deployment
metadata:
  name: exampleco-api
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: exampleco-api
  template:
    metadata:
      labels:
        app: exampleco-api
    spec:
      containers:
        - name: exampleco-api
          image: exampleco-api:latest
"""


async def _insert_cluster_audit_and_deployment(
    uow, clusters, deployment_audits, deployments, *, cluster_id, deployment_id, audit_id, **audit_kwargs
) -> None:
    async with uow() as tx:
        clusters.insert(tx, _birth_cluster_row(cluster_id))
        deployment_audits.insert(tx, _audit_row(audit_id, cluster_id=cluster_id, **audit_kwargs))
        deployments.insert(tx, _deployment_row(deployment_id, cluster_id=cluster_id, spec_ref=audit_id))


def test_load_audit_declares_the_dr_0022_contract():
    step = DeployLoadAudit(
        uow=object(), deployments=object(), deployment_audits=object(),
        clusters=object(), manifest_resolver=object(), config_dir=object(),
    )  # construction only
    assert step.verb == "deploy.load_audit"
    assert step.plane == "domain"
    assert step.thin is False
    assert step.gateable is False
    assert step.undoable is False
    assert step.idempotent is True


async def test_load_audit_round_trips_a_real_stored_audit_row(uow, clusters, deployment_audits, deployments):
    """The round's own test requirement: a REAL stored audit row, through
    `normalize_resolved_manifests` (never reimplemented -- deploy.py's own
    docstring), parsed into typed ManifestDocs, with persistence_services/
    deploy_wave/rollout_timeout_seconds/data_initialization all read back off
    resolved_config "like every other resolved fact" (DR-0028 decision 2;
    DR-0029 §2/§8 for deploy_wave)."""
    await _insert_cluster_audit_and_deployment(
        uow, clusters, deployment_audits, deployments,
        cluster_id="c1", deployment_id="d1", audit_id="a1",
        resolved_manifests=_MANIFEST_YAML,
        resolved_config={
            "persistence_services": ["postgres"],
            "deploy_wave": {"postgres": 1, "exampleco-atlas-migrations": 2, "exampleco-api": 3},
            "rollout_timeout_seconds": 240,
            "data_initialization": {"restore_from_snapshot": "snap-1"},
        },
        resolved_images={"exampleco-api": "ghcr.io/exampleco/core:x"},
    )
    step = _load_audit_step(uow, deployments, deployment_audits)

    output = await step.execute(
        DeployLoadAuditParams(deployment_id="d1"), make_step_context(cluster_id="c1")
    )

    assert [d.kind for d in output.manifests] == ["ServiceAccount", "Deployment", "Job", "Deployment"]
    assert [d.name for d in output.manifests] == ["exampleco-api-sa", "postgres", "exampleco-atlas-migrations", "exampleco-api"]
    assert output.profile.persistence_services == ["postgres"]
    assert output.profile.deploy_wave == {"postgres": 1, "exampleco-atlas-migrations": 2, "exampleco-api": 3}
    assert output.rollout_timeout_seconds == 240
    assert output.resolved_images == {"exampleco-api": "ghcr.io/exampleco/core:x"}
    assert output.data_initialization == SnapshotRestoreSpec(restore_from_snapshot="snap-1")


async def test_load_audit_defaults_rollout_timeout_and_persistence_services_and_data_initialization(
    uow, clusters, deployment_audits, deployments
):
    """No resolved_config keys at all -- the common case for a profile with no
    persistence block and no restore request. rollout_timeout_seconds falls
    back to 300 (matching v1's own `resolved_config.get("rollout_timeout_seconds",
    300)`, deployment_job.py:415); persistence_services defaults empty;
    data_initialization stays None, never an empty SnapshotRestoreSpec()."""
    await _insert_cluster_audit_and_deployment(
        uow, clusters, deployment_audits, deployments,
        cluster_id="c2", deployment_id="d2", audit_id="a2",
        resolved_manifests=_MANIFEST_YAML, resolved_config={},
    )
    step = _load_audit_step(uow, deployments, deployment_audits)

    output = await step.execute(DeployLoadAuditParams(deployment_id="d2"), make_step_context(cluster_id="c2"))

    assert output.profile.persistence_services == []
    assert output.profile.deploy_wave == {}
    assert output.rollout_timeout_seconds == 300
    assert output.data_initialization is None
    assert output.resolved_images == {}


async def test_load_audit_empty_manifests_without_deferred_marker_raises_permanent_error(
    uow, clusters, deployment_audits, deployments
):
    """DR-0025 Erratum E2's own requirement: "an audit empty for any OTHER
    reason must stay distinguishable from a deferred one, and must still be
    an error." An earlier revision of this step tolerated an empty
    ``resolved_manifests`` string and returned ``manifests=[]`` -- exactly the
    silent-deploys-nothing failure mode this DR exists to close. With no
    ``DEFERRED_MANIFEST_RENDERING_KEY`` marker set, this is now a hard error,
    never a quiet empty success."""
    await _insert_cluster_audit_and_deployment(
        uow, clusters, deployment_audits, deployments,
        cluster_id="c3", deployment_id="d3", audit_id="a3",
        resolved_manifests="", resolved_config={},
    )
    step = _load_audit_step(uow, deployments, deployment_audits)

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(DeployLoadAuditParams(deployment_id="d3"), make_step_context(cluster_id="c3"))

    assert exc_info.value.code == ErrorCode.INVALID_INPUT
    assert exc_info.value.detail["reason"] == "empty_resolved_manifests"


async def test_load_audit_deferred_marker_raises_a_distinct_error_not_empty_manifests(
    uow, clusters, deployment_audits, deployments
):
    """The round's own required test shape: a DEFERRED audit (DR-0025 Erratum
    E2) must be recognised as a DISTINCT, queryable fact -- never inferred
    from manifests being empty, and never silently returned as
    ``manifests=[]`` (this DR's own words: "the worst failure in this whole
    class"). Distinguished from the generic-empty case above by BOTH
    ``code`` and ``detail["reason"]``."""
    await _insert_cluster_audit_and_deployment(
        uow, clusters, deployment_audits, deployments,
        cluster_id="c-deferred", deployment_id="d-deferred", audit_id="a-deferred",
        resolved_manifests="", resolved_config={DEFERRED_MANIFEST_RENDERING_KEY: True},
    )
    step = _load_audit_step(uow, deployments, deployment_audits)

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(
            DeployLoadAuditParams(deployment_id="d-deferred"), make_step_context(cluster_id="c-deferred")
        )

    assert exc_info.value.code == ErrorCode.NOT_FOUND
    assert exc_info.value.detail["reason"] == "manifest_rendering_deferred"
    assert exc_info.value.detail["deployment_id"] == "d-deferred"
    assert exc_info.value.detail["spec_ref"] == "a-deferred"


async def test_load_audit_deferred_marker_raises_even_when_resolved_manifests_is_non_empty(
    uow, clusters, deployment_audits, deployments
):
    """The "NOT inferred from manifests being empty" half, pinned directly: a
    row carrying the DEFERRED marker raises UNCONDITIONALLY, even when
    ``resolved_manifests`` happens to hold real, well-formed content (a stale
    or partial value alongside the marker) -- proving the recognition reads
    the explicit ``resolved_config`` fact, never derives "is this deferred?"
    from whether manifests happen to be empty."""
    await _insert_cluster_audit_and_deployment(
        uow, clusters, deployment_audits, deployments,
        cluster_id="c-deferred-2", deployment_id="d-deferred-2", audit_id="a-deferred-2",
        resolved_manifests=_MANIFEST_YAML,
        resolved_config={DEFERRED_MANIFEST_RENDERING_KEY: True, "persistence_services": ["postgres"]},
    )
    step = _load_audit_step(uow, deployments, deployment_audits)

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(
            DeployLoadAuditParams(deployment_id="d-deferred-2"), make_step_context(cluster_id="c-deferred-2")
        )

    assert exc_info.value.detail["reason"] == "manifest_rendering_deferred"


async def test_load_audit_deferred_marker_false_is_not_treated_as_deferred(
    uow, clusters, deployment_audits, deployments
):
    """The converse of the previous test: an explicit falsy marker
    (``DEFERRED_MANIFEST_RENDERING_KEY: False``) is NOT the deferred case --
    only a truthy value is (``DeployLoadAudit.execute``'s own
    ``resolved_config.get(DEFERRED_MANIFEST_RENDERING_KEY)`` check). With
    empty manifests and a falsy marker, this falls through to the OTHER
    (generic-empty) error, not the deferred one -- confirming the marker is
    read as a real value, not merely "key present"."""
    await _insert_cluster_audit_and_deployment(
        uow, clusters, deployment_audits, deployments,
        cluster_id="c-not-deferred", deployment_id="d-not-deferred", audit_id="a-not-deferred",
        resolved_manifests="", resolved_config={DEFERRED_MANIFEST_RENDERING_KEY: False},
    )
    step = _load_audit_step(uow, deployments, deployment_audits)

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(
            DeployLoadAuditParams(deployment_id="d-not-deferred"), make_step_context(cluster_id="c-not-deferred")
        )

    assert exc_info.value.detail["reason"] == "empty_resolved_manifests"


def test_load_audit_deferred_and_empty_errors_are_never_confusable():
    """A direct, non-DB pin that the two error-construction helpers this step
    raises for its two "no usable manifests" cases can never be mistaken for
    one another by a caller inspecting the raised error -- different
    ``ErrorCode`` AND different ``detail["reason"]``, checked directly against
    the real helpers rather than merely re-asserted per scenario above."""
    from seedpod.engine.steps.deploy import (
        _empty_manifests_not_deferred,
        _manifest_rendering_deferred,
    )

    deferred = _manifest_rendering_deferred("dep-1", "audit-1")
    empty = _empty_manifests_not_deferred("dep-1", "audit-1")

    assert deferred.code != empty.code
    assert deferred.detail["reason"] != empty.detail["reason"]


async def test_load_audit_restore_from_latest_criteria_round_trip(uow, clusters, deployment_audits, deployments):
    """The OTHER v1 restore mode -- criteria, not a bare snapshot id."""
    await _insert_cluster_audit_and_deployment(
        uow, clusters, deployment_audits, deployments,
        cluster_id="c4", deployment_id="d4", audit_id="a4",
        resolved_manifests=_MANIFEST_YAML,
        resolved_config={
            "data_initialization": {
                "restore_from_latest": {"branch": "main", "profile": "exampleco-dev-stack-nodns", "max_age_days": 7}
            }
        },
    )
    step = _load_audit_step(uow, deployments, deployment_audits)

    output = await step.execute(DeployLoadAuditParams(deployment_id="d4"), make_step_context(cluster_id="c4"))

    assert output.data_initialization == SnapshotRestoreSpec(
        restore_from_latest=RestoreFromLatest(branch="main", profile="exampleco-dev-stack-nodns", max_age_days=7)
    )


async def test_load_audit_unknown_deployment_raises_permanent_error(uow, deployments, deployment_audits):
    step = _load_audit_step(uow, deployments, deployment_audits)
    with pytest.raises(PermanentError):
        await step.execute(DeployLoadAuditParams(deployment_id="does-not-exist"), make_step_context())


async def test_load_audit_deployment_with_no_spec_ref_raises_permanent_error(
    uow, clusters, deployments, deployment_audits
):
    """A deployment row that exists but has never been audited (spec_ref unset)
    -- a deploy workflow should never actually be dispatched for one, but this
    step must not silently invent an empty audit if it somehow is."""
    async with uow() as tx:
        clusters.insert(tx, _birth_cluster_row("c5"))
        deployments.insert(tx, _deployment_row("d5", cluster_id="c5", spec_ref=None))
    step = _load_audit_step(uow, deployments, deployment_audits)

    with pytest.raises(PermanentError):
        await step.execute(DeployLoadAuditParams(deployment_id="d5"), make_step_context(cluster_id="c5"))


class _FakeDeploymentAuditRepositoryReturningNone:
    """The real ``deployment_audits.id`` FK on ``deployments.spec_ref`` makes a
    genuinely dangling pointer unreachable through the repository/DB path at
    all -- ``deployment_audits`` is append-only (no delete method exists
    anywhere in ``DeploymentAuditRepository``), so SQLite's own
    ``PRAGMA foreign_keys=ON`` refuses to let a ``deployments`` row reference
    an audit id that was never inserted. This hand-written fake (never Mock/
    patch) is the only way to exercise this step's own defensive branch:
    prove it raises loudly rather than silently degrading, in case a future
    migration or manual intervention ever does produce this state."""

    def get(self, session, audit_id: str) -> DeploymentAuditRow | None:
        return None


async def test_load_audit_dangling_spec_ref_raises_permanent_error(uow, clusters, deployments, deployment_audits):
    """A spec_ref the injected repository cannot resolve to any row -- a
    data-integrity defect, not a normal empty case; must raise loudly, never
    silently degrade to "no manifests". See
    ``_FakeDeploymentAuditRepositoryReturningNone``'s own docstring for why a
    fake, not real data, is the only way to construct this scenario."""
    async with uow() as tx:
        clusters.insert(tx, _birth_cluster_row("c6"))
        deployment_audits.insert(tx, _audit_row("real-audit-id", cluster_id="c6", resolved_manifests=""))
        deployments.insert(tx, _deployment_row("d6", cluster_id="c6", spec_ref="real-audit-id"))
    step = _load_audit_step(uow, deployments, _FakeDeploymentAuditRepositoryReturningNone())

    with pytest.raises(PermanentError):
        await step.execute(DeployLoadAuditParams(deployment_id="d6"), make_step_context(cluster_id="c6"))


class _FakeDeploymentAuditRepositoryReturningADict:
    """A tiny, hand-written (never Mock/patch) stand-in proving the "gotcha 12"
    str/dict tolerance is genuinely wired through this step -- not just called
    on a value that can never actually be anything but a str. The REAL
    ``DeploymentAuditRepository.get`` structurally can never hand back a dict
    for ``resolved_manifests`` (``_decrypt_row`` always calls
    ``CryptoService.decrypt``, which always returns ``str`` -- there is no
    live write path today that could smuggle a dict through the real
    repository), so this fake is the only way to exercise
    ``normalize_resolved_manifests``'s Mapping branch at all, for a value
    old/foreign data (or a future write path) could plausibly still produce.

    ``dataclasses.replace`` on the frozen ``DeploymentAuditRow`` deliberately
    overrides ``resolved_manifests`` with a dict -- dataclasses never validate
    field types at runtime (frozen only blocks reassignment, not construction
    with a "wrong" type), so this is a legitimate, minimal way to construct
    the exact row shape gotcha 12 exists to tolerate."""

    def get(self, session, audit_id: str) -> DeploymentAuditRow:
        row = _audit_row(audit_id, cluster_id="c-dict", resolved_manifests="unused")
        return dataclasses.replace(
            row, resolved_manifests={"content": "kind: ConfigMap\nmetadata:\n  name: from-dict\n"}
        )


async def test_load_audit_tolerates_a_dict_shaped_resolved_manifests(uow, clusters, deployments, deployment_audits):
    """Gotcha 12: v1 had two independent inline call sites defensively
    handling a dict-shaped resolved_manifests (a `{"content": ...}` or
    `{"yaml": ...}` wrapper); `normalize_resolved_manifests` is the one
    shared home for that tolerance now, and this step must call it rather
    than assume `resolved_manifests` is always already a plain string. The
    real (encrypted, str-typed) audit row inserted here only exists to
    satisfy `deployments.spec_ref`'s FK -- the step reads through the FAKE
    repository below, which is what actually hands back the dict shape."""
    async with uow() as tx:
        clusters.insert(tx, _birth_cluster_row("c-dict"))
        deployment_audits.insert(tx, _audit_row("a-dict", cluster_id="c-dict", resolved_manifests=""))
        deployments.insert(tx, _deployment_row("d-dict", cluster_id="c-dict", spec_ref="a-dict"))
    step = _load_audit_step(uow, deployments, _FakeDeploymentAuditRepositoryReturningADict())

    output = await step.execute(DeployLoadAuditParams(deployment_id="d-dict"), make_step_context(cluster_id="c-dict"))

    assert [d.name for d in output.manifests] == ["from-dict"]


# ---------------------------------------------------------------------------
# DR-0025 Erratum E2 -- deploy.load_audit's rehydration seam. Real
# ``exampleco-dev-stack-nodns``/full-``_deploy()`` end-to-end coverage lives in
# ``tests/app/test_services_deployment.py``'s own
# ``test_provider_host_profile_defers_at_decision_time_then_rehydrates_at_deploy_time``
# (that test needs the app-layer ``DeploymentService`` this module deliberately
# never imports); these are the narrower, isolated pins the two mechanisms this
# module OWNS -- ``DeployLoadAudit._rehydrate`` and
# ``DeploymentAuditRepository.update_rendered_manifests`` -- get on their own.
# ---------------------------------------------------------------------------


async def test_update_rendered_manifests_rewrites_the_same_row_and_round_trips_encrypted(
    uow, clusters, deployments, deployment_audits
):
    await _insert_cluster_audit_and_deployment(
        uow, clusters, deployment_audits, deployments,
        cluster_id="c-rewrite", deployment_id="d-rewrite", audit_id="a-rewrite",
        resolved_manifests="", resolved_config={DEFERRED_MANIFEST_RENDERING_KEY: True},
    )

    async with uow() as tx:
        ok = deployment_audits.update_rendered_manifests(
            tx, "a-rewrite", resolved_manifests=_MANIFEST_YAML,
            resolved_config={DEFERRED_MANIFEST_RENDERING_KEY: False, "cluster_hostname": "203.0.113.42"},
            template_files_used=["exampleco-api.yaml"], key_class="DEV",
        )
    assert ok is True

    async with uow() as tx:
        row = deployment_audits.get(tx, "a-rewrite")
    assert row.resolved_manifests == _MANIFEST_YAML  # decrypts back to the exact plaintext
    assert row.resolved_config == {DEFERRED_MANIFEST_RENDERING_KEY: False, "cluster_hostname": "203.0.113.42"}
    assert row.template_files_used == ["exampleco-api.yaml"]
    # untouched columns -- rehydration re-renders ONLY the hostname-dependent half:
    assert row.triggering_repo == "exampleco-core"
    assert row.resolved_images == {}


async def test_update_rendered_manifests_returns_false_for_a_vanished_row(uow, deployment_audits):
    async with uow() as tx:
        ok = deployment_audits.update_rendered_manifests(
            tx, "does-not-exist", resolved_manifests="x", resolved_config={}, template_files_used=[], key_class="DEV",
        )
    assert ok is False


async def test_rehydrate_raises_the_deferred_error_when_cluster_still_has_no_public_ip(
    uow, clusters, deployments, deployment_audits
):
    """Defensive branch: DR-0025's own ordering guarantee (core/machine.py's
    ``_deployment_pending_cluster_ready``) means a live run should never
    observe this, but the step must not silently proceed on an assumption it
    cannot verify -- same error, same detail shape a caller already relies on
    (test_load_audit_deferred_marker_raises_a_distinct_error_not_empty_manifests,
    above -- this is that SAME scenario, now reached via the real rehydration
    attempt rather than an unconditional raise)."""
    await _insert_cluster_audit_and_deployment(
        uow, clusters, deployment_audits, deployments,
        cluster_id="c-no-ip", deployment_id="d-no-ip", audit_id="a-no-ip",
        resolved_manifests="", resolved_config={DEFERRED_MANIFEST_RENDERING_KEY: True},
    )
    step = _load_audit_step(uow, deployments, deployment_audits)

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(DeployLoadAuditParams(deployment_id="d-no-ip"), make_step_context(cluster_id="c-no-ip"))

    assert exc_info.value.detail["reason"] == "manifest_rendering_deferred"


# ---------------------------------------------------------------------------
# deploy.plan_waves -- the crown jewel. Pure; no fixtures beyond literals.
# ---------------------------------------------------------------------------


def _doc(kind: str, name: str, *, namespace: str = "default", body: dict | None = None) -> ManifestDoc:
    """``body`` overrides/extends the minimal default -- a top-level
    ``metadata`` key in ``body`` is MERGED (never replaces ``name``/
    ``namespace``), matching real multi-key manifest documents where
    ``metadata.labels`` sits alongside ``metadata.name``."""
    full_body: dict = {"kind": kind, "metadata": {"name": name, "namespace": namespace}}
    for key, value in (body or {}).items():
        if key == "metadata" and isinstance(value, dict):
            full_body["metadata"] = {**full_body["metadata"], **value}
        else:
            full_body[key] = value
    return ManifestDoc(kind=kind, name=name, namespace=namespace, body=full_body)


def test_plan_waves_declares_the_dr_0022_contract():
    step = PlanWaves()
    assert step.verb == "deploy.plan_waves"
    assert step.plane == "domain"
    assert step.thin is False
    assert step.gateable is False
    assert step.undoable is False
    assert step.idempotent is True


async def test_plan_waves_a_profile_declaring_no_deploy_wave_anywhere_produces_exactly_one_wave():
    """DR-0029 Consequences, the mandatory back-compat pin: a profile whose
    ``deploy_wave`` mapping has an entry for every declared service, but every
    single value is the default 3 (no service's YAML ever set ``deploy_wave``
    explicitly), behaves EXACTLY like today's single unsplit `kubectl apply`
    -- one wave, everything in it -- because every document matches SOME
    declared service and every declared service resolves to the same rank."""
    docs = [_doc("Job", "migrate"), _doc("Deployment", "exampleco-api")]
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=docs,
            profile=DeploymentProfile(deploy_wave={"migrate": 3, "exampleco-api": 3}),
            rollout_timeout_seconds=300,
        ),
        make_step_context(),
    )

    assert len(output.waves) == 1
    wave = output.waves[0]
    assert wave.index == 3
    assert wave.docs == docs
    assert wave.jobs == ["migrate"]
    assert wave.deployments == ["exampleco-api"]
    assert wave.gate_timeout_seconds == 300
    assert wave.restore is None


async def test_plan_waves_a_secret_belonging_to_no_service_lands_in_wave_0():
    """DR-0029's own mandatory pin, verbatim: "a Secret belonging to no
    service lands in wave 0" -- an UNMATCHED document (no declared service's
    name/label/selector matches it) is wave 0, regardless of ``deploy_wave``
    being declared for OTHER services."""
    secret = _doc("Secret", "ghcr-secret")
    app_doc = _doc("Deployment", "exampleco-api")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[app_doc, secret],  # deliberately not infra-first in the input
            profile=DeploymentProfile(deploy_wave={"exampleco-api": 3}), rollout_timeout_seconds=300,
        ),
        make_step_context(),
    )

    assert [w.index for w in output.waves] == [0, 3]
    infra_wave, app_wave = output.waves
    assert infra_wave.docs == [secret]
    assert infra_wave.restore is None
    assert app_wave.docs == [app_doc]
    assert app_wave.deployments == ["exampleco-api"]


async def test_plan_waves_a_statefulset_belonging_to_a_declared_service_lands_in_that_services_wave_not_wave_0():
    """DR-0029's OTHER mandatory pin, verbatim: "a StatefulSet belonging to a
    declared service lands in THAT SERVICE'S wave, not wave 0" -- the whole
    point of the service-name rule over a kind test (DR-0029 §3, "Not a kind
    test"). A naive kind-based classifier (StatefulSet is not Job/Deployment)
    would wrongly strand this in wave 0 alongside truly-unmatched infra."""
    statefulset = _doc("StatefulSet", "postgres")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[statefulset],
            profile=DeploymentProfile(deploy_wave={"postgres": 1}), rollout_timeout_seconds=300,
        ),
        make_step_context(),
    )

    assert len(output.waves) == 1
    assert output.waves[0].index == 1  # postgres's own declared rank, NOT wave 0
    assert output.waves[0].docs == [statefulset]
    # Neither Job nor Deployment: a StatefulSet contributes to `docs` but
    # neither name list (DR-0029 §4 -- kind still answers a DIFFERENT
    # question, "what Wave.jobs/deployments gate on", never "which wave").
    assert output.waves[0].jobs == []
    assert output.waves[0].deployments == []


async def test_plan_waves_real_shipped_profile_sorts_datastores_before_migrations_before_apps(test_config_dir):
    """The seam between the mapping WRITER (``_build_resolved_config``,
    pinned in isolation by ``tests/app/test_deployment_service_resolved_
    config.py``) and the PLANNER (``PlanWaves``, pinned above only against
    hand-written ``DeploymentProfile(deploy_wave={...})`` literals) was,
    until this test, never exercised against real data. A typo in
    ``config/deployment-profiles/exampleco-dev-stack-nodns.yml`` (a `deploy_wave`
    indented under the wrong key, a value written as a string, a datastore
    left at the default 3) would pass every other test while silently
    collapsing the datastore tier into the application wave -- the
    deploy-app-before-its-database failure DR-0029 exists to prevent.

    Loads the REAL shipped profile, runs it through the REAL
    ``_build_resolved_config``, builds the REAL ``DeploymentProfile`` from
    that output exactly as ``deploy.load_audit`` does, and only then hands
    hand-built ``ManifestDoc``s (named/kinded to match the REAL manifest
    templates -- ``config/manifest-templates/exampleco-stack/{postgres,
    cache,minio,exampleco-migrations,exampleco-api,rbac,ghcr-secret}.yaml``)
    to ``PlanWaves``."""
    from seedpod.app.services.deployment_service import _build_resolved_config
    from seedpod.app.services.profiles import load_deployment_profile

    _profile, raw_profile = load_deployment_profile(test_config_dir, "exampleco-dev-stack-nodns")
    resolved_config = _build_resolved_config(
        "c-real", "ephemeral", raw_profile, {}, "real-slug", "exampleco-dev-stack-nodns"
    )
    profile = DeploymentProfile(
        persistence_services=list(resolved_config.get("persistence_services") or []),
        deploy_wave=dict(resolved_config.get("deploy_wave") or {}),
    )

    # Datastores (wave 1 in the profile's own YAML).
    postgres = _doc("Deployment", "postgres")
    cache = _doc("Deployment", "cache")
    minio = _doc("Deployment", "minio")
    # Migration Job (wave 2).
    migrations = _doc("Job", "exampleco-migrations")
    # An application service (wave 3, the default -- exampleco-api sets no
    # explicit deploy_wave in the shipped YAML).
    exampleco_api = _doc("Deployment", "exampleco-api")
    # Infrastructure matching NO declared service -- wave 0
    # (config/manifest-templates/exampleco-stack/{ghcr-secret,rbac}.yaml's
    # own real kinds/names).
    ghcr_secret = _doc("Secret", "ghcr-secret")
    job_reader = _doc("Role", "job-reader")

    step = PlanWaves()
    output = await step.execute(
        PlanWavesParams(
            manifests=[exampleco_api, migrations, postgres, cache, minio, ghcr_secret, job_reader],
            profile=profile,
            rollout_timeout_seconds=resolved_config["rollout_timeout_seconds"],
        ),
        make_step_context(),
    )

    def _names(docs: list[ManifestDoc]) -> set[str]:
        return {doc.name for doc in docs}

    waves_by_index = {w.index: w for w in output.waves}
    assert sorted(waves_by_index) == [0, 1, 2, 3]
    assert _names(waves_by_index[0].docs) == {"ghcr-secret", "job-reader"}
    assert _names(waves_by_index[1].docs) == {"postgres", "cache", "minio"}
    assert waves_by_index[2].docs == [migrations]
    assert waves_by_index[2].jobs == ["exampleco-migrations"]
    assert waves_by_index[3].docs == [exampleco_api]
    assert waves_by_index[3].deployments == ["exampleco-api"]
    # And the wave ORDER itself: datastores before migrations before apps,
    # infra before all three -- the whole point of the exercise.
    assert list(waves_by_index) == [0, 1, 2, 3]


async def test_plan_waves_restore_requested_without_persistence_services_raises_permanent_error():
    """DR-0028 Erratum E2, surviving DR-0029 unchanged, pinned by the RAISE
    itself, not merely "the deployment proceeds": v1's own compound gate
    (deployment_job.py:530) silently drops a requested restore when there is
    no persistence service to protect it -- a genuine v1 bug, deliberately NOT
    ported. This step raises instead of reproducing that silent drop."""
    docs = [_doc("ConfigMap", "cm-1")]
    step = PlanWaves()

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(
            PlanWavesParams(
                manifests=docs, profile=DeploymentProfile(persistence_services=[]), rollout_timeout_seconds=300,
                data_initialization=SnapshotRestoreSpec(restore_from_snapshot="snap-1"),
            ),
            make_step_context(),
        )

    assert exc_info.value.code == ErrorCode.INVALID_INPUT


@pytest.mark.parametrize(
    "heuristic_doc",
    [
        pytest.param(_doc("Deployment", "postgres-deployment"), id="a-name-startswith-service"),
        pytest.param(
            _doc("Deployment", "pg-primary", body={"metadata": {"labels": {"app": "postgres"}}}),
            id="b-metadata-labels-app",
        ),
        pytest.param(
            _doc(
                "Deployment", "pg-primary",
                body={"spec": {"template": {"metadata": {"labels": {"app": "postgres"}}}}},
            ),
            id="b-template-labels-app",
        ),
        pytest.param(
            _doc("Deployment", "pg-primary", body={"spec": {"selector": {"matchLabels": {"app": "postgres"}}}}),
            id="c-selector-matchlabels-app",
        ),
    ],
)
async def test_plan_waves_each_heuristic_independently_classifies_a_service_doc(heuristic_doc):
    """The round's own required test shape: each of the three heuristics
    (name-prefix/equals is (a); the label check is (b), exercised via BOTH
    its two label sites; matchLabels is (c)) proven in ISOLATION, not one
    combined fixture that could pass even if two of the three were silently
    dropped. `other` matches no declared service, so this test's wave
    count/order stays exactly two (postgres's own wave, then wave 0) without
    a third tier complicating it."""
    other = _doc("Deployment", "unrelated-app")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[heuristic_doc, other],
            profile=DeploymentProfile(persistence_services=["postgres"], deploy_wave={"postgres": 1}),
            rollout_timeout_seconds=300,
        ),
        make_step_context(),
    )

    assert [w.index for w in output.waves] == [0, 1]
    infra_wave, persistence_wave = output.waves
    assert infra_wave.docs == [other]  # matches no declared service -> wave 0
    assert persistence_wave.docs == [heuristic_doc]


async def test_plan_waves_a_doc_matching_none_of_the_three_heuristics_is_not_classified_as_the_service():
    """The converse: a Deployment named "worker" with no matching label/
    selector must NOT be swept into postgres's wave just because SOME
    service happens to be declared -- it is UNMATCHED (matches no declared
    service at all), so it lands in wave 0, not postgres's wave 1."""
    unrelated = _doc("Deployment", "worker", body={"metadata": {"labels": {"app": "worker"}}})
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[unrelated],
            profile=DeploymentProfile(persistence_services=["postgres"], deploy_wave={"postgres": 1}),
            rollout_timeout_seconds=300,
        ),
        make_step_context(),
    )

    assert len(output.waves) == 1
    assert output.waves[0].index == 0
    assert output.waves[0].docs == [unrelated]


async def test_plan_waves_persistence_wave_is_first_deliberately_generalized_split():
    """persistence_services non-empty but NO data_initialization requested
    still SPLITS into a persistence wave (rank 1) then an app wave (rank 3,
    default) -- v1 itself would NOT split here (its compound gate needs
    data_initialization too), a deliberate divergence
    ``PlanWaves.execute``'s own docstring defends. No restore anywhere when
    nothing was requested."""
    db_doc = _doc("Deployment", "postgres")
    app_doc = _doc("Deployment", "exampleco-api")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[app_doc, db_doc],  # deliberately NOT database-first in the input
            profile=DeploymentProfile(
                persistence_services=["postgres"], deploy_wave={"postgres": 1, "exampleco-api": 3}
            ),
            rollout_timeout_seconds=180,
        ),
        make_step_context(),
    )

    assert [w.index for w in output.waves] == [1, 3]
    assert output.waves[0].docs == [db_doc]  # the persistence wave, regardless of input order
    assert output.waves[1].docs == [app_doc]
    assert all(w.restore is None for w in output.waves)
    assert all(w.gate_timeout_seconds == 180 for w in output.waves)


async def test_plan_waves_restore_attaches_directly_to_the_persistence_wave():
    """DR-0028 Erratum E1 point 2, surviving DR-0029 unchanged, the round's
    own required test: `restore` is attached DIRECTLY to the SAME wave
    carrying the matched persistence documents -- there is NO separate
    empty-docs wave for it. Exactly two waves, not three."""
    db_doc = _doc("Deployment", "postgres")
    app_doc = _doc("Deployment", "exampleco-api")
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-42")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[db_doc, app_doc],
            profile=DeploymentProfile(
                persistence_services=["postgres"], deploy_wave={"postgres": 1, "exampleco-api": 3}
            ),
            rollout_timeout_seconds=180, data_initialization=spec,
        ),
        make_step_context(),
    )

    assert [w.index for w in output.waves] == [1, 3]
    persistence_wave, app_wave = output.waves
    assert persistence_wave.docs == [db_doc]
    assert persistence_wave.restore == spec  # directly on the persistence wave itself
    assert app_wave.docs == [app_doc]
    assert app_wave.restore is None


async def test_plan_waves_restore_wave_exists_even_with_no_matching_persistence_docs():
    """The persistence wave is the ONLY place a resolved restore can attach
    (Wave's own docstring) -- so it must exist, carrying `restore`, even when
    nothing in this particular deploy's manifests happened to match a
    persistence service by name/label."""
    app_doc = _doc("Deployment", "exampleco-api")
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-1")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[app_doc],
            profile=DeploymentProfile(
                persistence_services=["postgres"], deploy_wave={"postgres": 1, "exampleco-api": 3}
            ),
            rollout_timeout_seconds=300, data_initialization=spec,
        ),
        make_step_context(),
    )

    assert [w.index for w in output.waves] == [1, 3]
    persistence_wave, app_wave = output.waves
    assert persistence_wave.docs == []
    assert persistence_wave.restore == spec
    assert app_wave.docs == [app_doc]


async def test_plan_waves_a_secret_a_persistence_workload_depends_on_lands_in_an_earlier_wave():
    """The round's own required regression pin for WHY wave 0 is load-bearing,
    not cosmetic: a Secret that does not itself match any declared-service
    heuristic (a generic credential, not name-prefixed/labelled "postgres")
    must still be applied in a wave BEFORE the persistence workload that
    might depend on it -- otherwise that workload's own readiness gate could
    deadlock waiting for a pod that can never start without a Secret still
    stuck in a later wave."""
    secret = _doc("Secret", "db-credentials")
    db_doc = _doc("Deployment", "postgres")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[db_doc, secret],  # deliberately not infra-first in the input
            profile=DeploymentProfile(persistence_services=["postgres"], deploy_wave={"postgres": 1}),
            rollout_timeout_seconds=300,
        ),
        make_step_context(),
    )

    secret_wave = next(w for w in output.waves if secret in w.docs)
    persistence_wave = next(w for w in output.waves if db_doc in w.docs)
    assert secret_wave.index < persistence_wave.index


async def test_plan_waves_three_tier_ordering_infra_then_persistence_then_app():
    """The full DR-0029 model in one deploy: an unmatched infra doc
    (ghcr-secret, wave 0), a matched persistence doc (rank 1, with its
    restore attached directly), and an app doc (rank 3, the default) -- in
    that order, regardless of input order."""
    ghcr_secret = _doc("Secret", "ghcr-secret")
    db_doc = _doc("Deployment", "postgres")
    app_doc = _doc("Deployment", "exampleco-api")
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-7")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[app_doc, db_doc, ghcr_secret],  # deliberately scrambled
            profile=DeploymentProfile(
                persistence_services=["postgres"], deploy_wave={"postgres": 1, "exampleco-api": 3}
            ),
            rollout_timeout_seconds=300,
            data_initialization=spec,
        ),
        make_step_context(),
    )

    assert [w.index for w in output.waves] == [0, 1, 3]
    infra_wave, persistence_wave, app_wave = output.waves
    assert infra_wave.docs == [ghcr_secret]
    assert infra_wave.restore is None
    assert persistence_wave.docs == [db_doc]
    assert persistence_wave.restore == spec
    assert app_wave.docs == [app_doc]
    assert app_wave.restore is None


async def test_plan_waves_jobs_and_deployments_are_derived_from_doc_kind_not_all_docs():
    """Wave.jobs/Wave.deployments are the NAMES of only this wave's Job/
    Deployment documents -- a ConfigMap/Secret/ServiceAccount in the same
    tier contributes to `docs` but neither name list. ServiceAccount/ConfigMap
    match no declared service, landing in wave 0; Job/Deployment match the
    declared `exampleco-api`/`migrate-job`... wait, only `exampleco-api` is declared
    here, so `migrate-job` also lands in wave 0 alongside the ServiceAccount/
    ConfigMap -- kind still determines jobs/deployments membership within
    whichever wave a doc lands in, independent of that wave's index."""
    docs = [
        _doc("ServiceAccount", "sa-1"),
        _doc("Job", "migrate-job"),
        _doc("Deployment", "exampleco-api"),
        _doc("ConfigMap", "cm-1"),
    ]
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=docs, profile=DeploymentProfile(deploy_wave={"exampleco-api": 3}), rollout_timeout_seconds=300
        ),
        make_step_context(),
    )

    assert [w.index for w in output.waves] == [0, 3]
    wave_0, app_wave = output.waves
    assert {d.name for d in wave_0.docs} == {"sa-1", "migrate-job", "cm-1"}
    assert wave_0.jobs == ["migrate-job"]
    assert wave_0.deployments == []
    assert {d.name for d in app_wave.docs} == {"exampleco-api"}
    assert app_wave.jobs == []
    assert app_wave.deployments == ["exampleco-api"]


async def test_plan_waves_gate_timeout_seconds_is_sourced_from_rollout_timeout_seconds_not_hardcoded():
    """The brief's own explicit ask: v1's hardcoded `timeout=180`
    (deployment_job.py:555) must NOT be reproduced literally -- every wave's
    gate_timeout_seconds is `params.rollout_timeout_seconds`, whatever value
    that happens to be."""
    docs = [_doc("Deployment", "postgres")]
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=docs,
            profile=DeploymentProfile(persistence_services=["postgres"], deploy_wave={"postgres": 1}),
            rollout_timeout_seconds=555,
        ),
        make_step_context(),
    )

    assert all(w.gate_timeout_seconds == 555 for w in output.waves)
    assert 180 not in {w.gate_timeout_seconds for w in output.waves}


async def test_plan_waves_multiple_persistence_services():
    postgres = _doc("Deployment", "postgres")
    keycloak_postgres = _doc("Deployment", "keycloak-postgres-primary")
    app = _doc("Deployment", "exampleco-api")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[postgres, keycloak_postgres, app],
            profile=DeploymentProfile(
                persistence_services=["postgres", "keycloak-postgres"],
                deploy_wave={"postgres": 1, "keycloak-postgres": 1, "exampleco-api": 3},
            ),
            rollout_timeout_seconds=300,
        ),
        make_step_context(),
    )

    # ManifestDoc (a plain pydantic BaseModel) is not hashable -- compare by
    # name instead of putting instances in a set.
    assert [w.index for w in output.waves] == [1, 3]
    assert {d.name for d in output.waves[0].docs} == {"postgres", "keycloak-postgres-primary"}
    assert output.waves[1].docs == [app]


async def test_plan_waves_the_prefix_match_case_independently():
    """DR-0029's own mandatory pin: the prefix-match case, independently of
    the label cases -- v1's own real, named example ("postgres-deployment"
    matches service "postgres", deployment_job.py:99-101). A LONGER,
    more-specific service name is not present here, so `_service_for`'s own
    "longest prefix wins" tie-break is not exercised by this test, only the
    base prefix match -- proven correct by itself, not merely alongside the
    label heuristics (the parametrized heuristic test above already covers
    that combination)."""
    doc = _doc("Deployment", "postgres-deployment")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[doc],
            profile=DeploymentProfile(persistence_services=["postgres"], deploy_wave={"postgres": 1}),
            rollout_timeout_seconds=300,
        ),
        make_step_context(),
    )

    assert len(output.waves) == 1
    assert output.waves[0].index == 1
    assert output.waves[0].docs == [doc]


async def test_plan_waves_prefix_match_prefers_the_longest_most_specific_service_name():
    """`_service_for`'s own documented tie-break: when a document's name has
    more than one candidate service prefix, the LONGEST (most specific) wins
    -- proven directly, not merely asserted in a docstring nobody runs."""
    doc = _doc("Deployment", "postgres-replica-0")
    step = PlanWaves()

    output = await step.execute(
        PlanWavesParams(
            manifests=[doc],
            profile=DeploymentProfile(deploy_wave={"postgres": 1, "postgres-replica": 2}),
            rollout_timeout_seconds=300,
        ),
        make_step_context(),
    )

    assert len(output.waves) == 1
    assert output.waves[0].index == 2  # "postgres-replica", not "postgres"


# ---------------------------------------------------------------------------
# deploy.prepare_wave -- against the REAL KubectlProvider + conformance fake.
# ---------------------------------------------------------------------------


def _ctx(providers):
    return make_step_context(services=StepServices(subprocess_manager=FakeSubprocessManager(), providers=providers))


def test_prepare_wave_declares_the_dr_0022_contract():
    step = DeployPrepareWave()
    assert step.verb == "deploy.prepare_wave"
    assert step.plane == "provider"
    assert step.provider_name == "kubectl"
    assert step.thin is False
    assert step.gateable is False
    assert step.undoable is False
    assert step.idempotent is True


def test_prepare_wave_command_lists_pods_cluster_wide():
    step = DeployPrepareWave()
    command = step.command(DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=[]))
    assert command == KubeGetPods(kubeconfig=FAKE_KUBECONFIG, namespace=None)


def test_prepare_wave_output_from_is_always_empty_output():
    assert isinstance(DeployPrepareWave().output_from(object()), EmptyOutput)


async def test_prepare_wave_deletes_every_named_job():
    harness = KubectlHarness()
    harness.backend.jobs = {
        ("default", "migrate-a"): {"metadata": {"name": "migrate-a", "namespace": "default"}},
        ("default", "migrate-b"): {"metadata": {"name": "migrate-b", "namespace": "default"}},
    }
    step = DeployPrepareWave()

    output = await step.execute(
        DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=["migrate-a", "migrate-b"]), _ctx({"kubectl": harness.provider()})
    )

    assert isinstance(output, EmptyOutput)
    assert harness.backend.jobs == {}


async def test_prepare_wave_deleting_an_absent_job_is_tolerated():
    """--ignore-not-found: a Job that was never applied (first deploy) or was
    already cleaned up must not be an error."""
    harness = KubectlHarness()
    step = DeployPrepareWave()

    output = await step.execute(
        DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=["never-existed"]), _ctx({"kubectl": harness.provider()})
    )

    assert isinstance(output, EmptyOutput)


async def test_prepare_wave_with_no_jobs_at_all_still_runs_the_stuck_pod_sweep():
    harness = KubectlHarness()
    harness.backend.pods = {("default", "stuck-1"): _pending_pod("stuck-1")}
    step = DeployPrepareWave()

    await step.execute(DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=[]), _ctx({"kubectl": harness.provider()}))

    assert ("default", "stuck-1") not in harness.backend.pods


async def test_prepare_wave_is_fully_best_effort_never_raises_even_on_a_hard_provider_failure():
    """v1's own posture (deployment_job.py:926-1017): every delete is wrapped
    individually, and the WHOLE cleanup section is best-effort -- a genuine
    connectivity/auth failure here must never abort the wave (the workflow's
    own `on_failure: continue` is a belt-and-suspenders backstop, not the
    only mechanism -- this step tolerates internally too, matching v1's
    finer per-resource grain)."""
    harness = KubectlHarness()
    step = DeployPrepareWave()

    output = await step.execute(
        DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=["a", "b"]), _ctx({"kubectl": harness.provider(Fault.AUTH)})
    )

    assert isinstance(output, EmptyOutput)


async def test_prepare_wave_continues_to_the_next_job_after_one_fails():
    """Per-resource tolerance, not just per-step: BOTH job deletes must be
    ATTEMPTED even though every one of them fails against this harness --
    proven via the call log, since Fault.AUTH makes every kubectl invocation
    fail identically (there is no state to inspect on the backend side)."""
    harness = KubectlHarness()
    step = DeployPrepareWave()

    await step.execute(
        DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=["job-a", "job-b"]),
        _ctx({"kubectl": harness.provider(Fault.AUTH)}),
    )

    delete_calls = [call for call in harness.backend.call_log if len(call) > 1 and call[1] == "delete"]
    assert any("job-a" in call for call in delete_calls)
    assert any("job-b" in call for call in delete_calls)
    # And the stuck-pod sweep's own list call was STILL attempted afterward.
    assert any(len(call) > 1 and call[1] == "get" for call in harness.backend.call_log)


def _pod(name: str, phase: str, *, namespace: str = "default") -> dict:
    return {
        "metadata": {"name": name, "namespace": namespace, "creationTimestamp": "2026-01-01T00:00:00Z", "labels": {}},
        "spec": {"containers": []},
        "status": {"phase": phase},
    }


def _pending_pod(name: str, *, namespace: str = "default") -> dict:
    return _pod(name, "Pending", namespace=namespace)


async def test_prepare_wave_force_deletes_a_pending_pod():
    harness = KubectlHarness()
    harness.backend.pods = {("default", "stuck-1"): _pending_pod("stuck-1")}
    step = DeployPrepareWave()

    await step.execute(DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=[]), _ctx({"kubectl": harness.provider()}))

    assert ("default", "stuck-1") not in harness.backend.pods
    force_delete = next(
        call for call in harness.backend.call_log if len(call) > 2 and call[1:3] == ("delete", "pod")
    )
    assert "--force" in force_delete
    assert "--grace-period=0" in force_delete
    assert "-n" in force_delete and "default" in force_delete


async def test_prepare_wave_force_deletes_a_failed_pod():
    harness = KubectlHarness()
    harness.backend.pods = {("default", "stuck-2"): _pod("stuck-2", "Failed")}
    step = DeployPrepareWave()

    await step.execute(DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=[]), _ctx({"kubectl": harness.provider()}))

    assert ("default", "stuck-2") not in harness.backend.pods


async def test_prepare_wave_leaves_a_running_pod_alone():
    harness = KubectlHarness()
    harness.backend.pods = {("default", "healthy-1"): _pod("healthy-1", "Running")}
    step = DeployPrepareWave()

    await step.execute(DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=[]), _ctx({"kubectl": harness.provider()}))

    assert ("default", "healthy-1") in harness.backend.pods


async def test_prepare_wave_only_deletes_stuck_pods_never_healthy_ones_in_a_mixed_set():
    harness = KubectlHarness()
    harness.backend.pods = {
        ("default", "healthy"): _pod("healthy", "Running"),
        ("default", "pending"): _pending_pod("pending"),
        ("default", "failed"): _pod("failed", "Failed"),
    }
    step = DeployPrepareWave()

    await step.execute(DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=[]), _ctx({"kubectl": harness.provider()}))

    assert set(harness.backend.pods) == {("default", "healthy")}


async def test_prepare_wave_sweeps_stuck_pods_across_every_namespace():
    """v1's own `k8s_provider.get_pods(cluster_id)` call carries NO namespace
    (deployment_job.py:963) -- a cluster-wide sweep, matching this step's own
    `KubeGetPods(kubeconfig, namespace=None)`. A stuck pod in a namespace
    other than "default" must still be found and force-deleted."""
    harness = KubectlHarness()
    harness.backend.pods = {("kube-system", "stuck-elsewhere"): _pending_pod("stuck-elsewhere", namespace="kube-system")}
    step = DeployPrepareWave()

    await step.execute(DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=[]), _ctx({"kubectl": harness.provider()}))

    assert ("kube-system", "stuck-elsewhere") not in harness.backend.pods


async def test_prepare_wave_a_hard_failure_listing_pods_is_tolerated_as_no_stuck_pods():
    """v1's own outer try/except around the whole stuck-pod section
    (deployment_job.py:961-1016, and the `if not success_pods:` branch at
    965-967): a listing failure degrades to "nothing to clean", not a raise --
    but ONLY for a bare ``ProviderError`` (here, ``Fault.AUTH``: a genuinely
    bad kubeconfig for the pod-listing call), never for
    ``InfrastructureUnreachableError`` (see the next test): CLAUDE.md's hard
    rule forbids conflating "cannot determine state" with "there is nothing
    to clean up"."""
    harness = KubectlHarness()
    harness.backend.pods = {("default", "would-be-stuck"): _pending_pod("would-be-stuck")}
    step = DeployPrepareWave()

    output = await step.execute(
        DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=[]), _ctx({"kubectl": harness.provider(Fault.AUTH)})
    )

    assert isinstance(output, EmptyOutput)  # never raises
    assert ("default", "would-be-stuck") in harness.backend.pods  # untouched: listing never succeeded


async def test_prepare_wave_an_unreachable_cluster_listing_pods_is_not_tolerated():
    """The converse of the test above, and a genuine correctness fix over an
    earlier revision of this step: ``InfrastructureUnreachableError`` means
    "cannot determine what pods exist", never "there are no stuck pods"
    (CLAUDE.md's hard rule -- "it never triggers compensation and is never
    conflated with absence"). Swallowing it here would let this step record
    SUCCEEDED having swept nothing, when it actually never learned whether
    anything needed sweeping. The workflow's own `prep` step still runs
    `on_failure: continue` (deploy-waves.yml), so this raise does not block
    the wave's `apply` -- it only makes THIS step's own outcome honest."""
    harness = KubectlHarness()
    harness.backend.pods = {("default", "would-be-stuck"): _pending_pod("would-be-stuck")}
    step = DeployPrepareWave()

    with pytest.raises(InfrastructureUnreachableError):
        await step.execute(
            DeleteJobsParams(kubeconfig=_KUBECONFIG, jobs=[]),
            _ctx({"kubectl": harness.provider(Fault.UNREACHABLE)}),
        )

    assert ("default", "would-be-stuck") in harness.backend.pods  # untouched: listing never succeeded


# ---------------------------------------------------------------------------
# _is_stuck_pod -- pins the v1 substring list, INCLUDING the documented dead
# conditions (a known v1 weakness this module's own docstring names loudly).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("phase", ["Pending", "Failed"])
def test_is_stuck_pod_true_for_the_two_reachable_v1_phases(phase):
    """The ONLY two of v1's seven substrings that can ever match a bare
    Kubernetes pod PHASE (Pending/Running/Succeeded/Failed/Unknown) --
    everything PodInfo.status is ever populated with, in both v1 and v2."""
    assert _is_stuck_pod(phase) is True


@pytest.mark.parametrize("phase", ["Running", "Succeeded", "Unknown"])
def test_is_stuck_pod_false_for_every_other_real_phase(phase):
    assert _is_stuck_pod(phase) is False


@pytest.mark.parametrize("dead_condition", ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "Init:2/3"])
def test_is_stuck_pod_the_documented_dead_conditions_never_reach_this_function_as_a_real_phase(dead_condition):
    """These four substrings are real entries in v1's own list
    (deployment_job.py:978-986) and this step's own `_STUCK_POD_STATUS_
    SUBSTRINGS`. The FUNCTION correctly recognizes them as substrings (proven
    here) -- what neither v1 nor v2 can ever do is hand it a `PodInfo.status`
    value that actually CONTAINS one, since that field is always the bare
    `.status.phase` (DeployPrepareWave's own class docstring has the full
    citation trail). This test pins the function's own correctness in
    isolation, precisely so the gap is "the input this function is fed",
    never "the substring check itself"."""
    assert _is_stuck_pod(dead_condition) is True  # the function itself works correctly
    # ...but no REAL PodInfo.status value the fake (or the real provider) ever
    # produces is anything other than one of the five plain phases below, so
    # this condition is unreachable via _pod()/_pending_pod()'s own shape --
    # not asserted further here (that would just restate _default_pods()'s own
    # fixed vocabulary); see the four phase-only tests above for what actually
    # reaches this function in practice.


def test_is_stuck_pod_substring_not_exact_match_pending_prefix():
    """v1's own check is a substring test (`'Pending' in pod_status`), not
    equality -- a hypothetical richer status string that merely CONTAINS
    'Pending' still counts, matching v1's own (over-)permissive behaviour."""
    assert _is_stuck_pod("PendingSomethingElse") is True
