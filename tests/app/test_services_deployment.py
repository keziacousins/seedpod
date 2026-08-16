"""``seedpod/app/services/deployment_service.py`` -- the version-update decision
matrix at the SERVICE layer (real sqlite, ``FrozenClock``, no Mock/patch), plus
DR-0008 (manifest resolution never runs inside an open ``uow()``).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from seedpod.app.services.deployment_service import DeploymentService
from seedpod.app.services.preset_service import PresetService
from seedpod.core.deploy_wave import (
    DEFERRED_MANIFEST_RENDERING_KEY,
    MANIFEST_RENDERING_REHYDRATED_KEY,
    SnapshotRestoreSpec,
)
from seedpod.core.errors import PermanentError
from seedpod.core.events import ProvisionSucceeded
from seedpod.core.records import ClusterState, DeploymentState, Origin
from seedpod.data.repositories import PresetRepository
from seedpod.engine.steps.cluster import LoadSpec, LoadSpecParams
from seedpod.engine.steps.deploy import DeployLoadAudit, DeployLoadAuditParams
from tests.engine.fakes import make_step_context

# The REAL, shipped config/ tree (not the tmp-path `config_dir` fixture) --
# needed for the DR-0026 tests below, which exercise `exampleco-web-2` (a real
# secret-bearing profile) rather than the synthetic `test-profile` fixture.
_REPO_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


@pytest.fixture
def deployment_service(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, config_dir,
    deployment_audits_repo, secrets_repo,
):
    return DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=config_dir,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )


async def test_feature_branch_creates_ephemeral_queued_deployment(deployment_service):
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:abc",
        commit="abc123", actor="api:test-user",
    )
    assert result.environment == "ephemeral"
    assert result.status == "queued"
    assert result.cluster_id is not None
    assert len(result.cluster_id) == 36
    assert result.deployment_id is not None


async def test_main_branch_creates_staging_queued_deployment(deployment_service):
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="main", image="ghcr.io/x/exampleco-core:main-def",
        commit="def456", actor="api:test-user",
    )
    assert result.environment == "staging"
    assert result.status == "queued"
    assert result.cluster_id is not None


async def test_unmatched_branch_is_no_action(deployment_service):
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="experimental/unknown", image="ghcr.io/x/exampleco-core:x",
        commit="x", actor="api:test-user",
    )
    assert result.status == "no_action"
    assert result.environment == "none"
    assert result.cluster_id is None
    assert "no deployment rule matches" in result.message.lower()


async def test_disabled_rule_is_no_action(deployment_service):
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="disabled/whatever", image="ghcr.io/x/exampleco-core:x",
        commit="x", actor="api:test-user",
    )
    assert result.status == "no_action"
    assert result.environment == "none"
    assert result.cluster_id is None


async def test_births_land_full_rows_via_dispatcher(deployment_service, repos, uow):
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:abc",
        commit="abc123", actor="api:test-user",
    )
    async with uow() as tx:
        cluster = repos.clusters.get(tx, result.cluster_id)
        deployment = repos.deployments.get(tx, result.deployment_id)

    assert cluster is not None
    assert cluster.environment == "ephemeral"
    assert cluster.provider == "fake"
    assert cluster.repository == "exampleco-core"
    assert cluster.branch == "feature/payment-system"
    assert cluster.origin == Origin.MANAGED
    assert cluster.status == ClusterState.PROVISIONING.value  # NEW x CreateRequested -> PROVISIONING
    assert cluster.slug  # non-empty, derived
    assert cluster.expires_at is not None  # ttl_hours=2 in the fixture rule

    assert deployment is not None
    assert deployment.cluster_id == cluster.id
    assert deployment.environment == "ephemeral"
    assert deployment.status == DeploymentState.PENDING.value  # NEW x DeployRequested -> PENDING
    assert deployment.spec_ref is not None
    assert deployment.resolved_images == {"exampleco-core": "ghcr.io/x/exampleco-core:abc"}
    assert deployment.deployed_by == "api:test-user"  # DR-0032


async def test_deployed_by_is_the_same_actor_the_state_audit_records(deployment_service, repos, uow):
    """DR-0032's decision, stated as an equality rather than left incidental.

    The column records the actor string, and it is the SAME string the cluster's
    state audit records for the identical request -- one notion of "who did this"
    spanning the audit trail and the column. v1 instead stored the triggering REPO
    here for webhook deploys (``cluster_manager.py:1200,1465``) and the username for
    preset deploys (``presets.py:807,827``), so one column held two different kinds
    of thing; this test is what stops that drifting back in.
    """
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:abc",
        commit="abc123", actor="api:someone-else",
    )
    async with uow() as tx:
        deployment = repos.deployments.get(tx, result.deployment_id)
        audits = repos.cluster_state_audits.list_for_cluster(tx, result.cluster_id)

    birth_audit = min(audits, key=lambda a: a.created_at)
    assert birth_audit.actor == "api:someone-else"
    assert deployment.deployed_by == birth_audit.actor


async def test_deployed_by_survives_every_later_transition(deployment_service, repos, uow, dispatcher, clock):
    """``deployed_by`` is a row-only column: ``DeploymentRepository.persist``
    CAS-updates only the columns ``DeploymentRecord`` carries, so the birth value is
    never overwritten by a subsequent state change. Written once, true forever --
    which is why DR-0032 needs no ``core/`` or machine-table change at all."""
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:abc",
        commit="abc123", actor="api:test-user",
    )
    await dispatcher.apply(
        "cluster", result.cluster_id,
        ProvisionSucceeded(at=clock.now(), actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref"),
    )

    async with uow() as tx:
        row = repos.deployments.get(tx, result.deployment_id)

    assert row.status != DeploymentState.PENDING.value, "the deployment must have moved on"
    assert row.deployed_by == "api:test-user", "a later transition must not clear the birth actor"


async def test_birthed_provider_config_loads_via_cluster_load_spec(deployment_service, repos, uow, tmp_path):
    """Round-8a review finding: a birthed cluster's ``provider_config`` must
    not be the dead-on-arrival ``{}`` -- ``cluster.load_spec`` (the real
    provision-head domain step) must be able to build a valid
    ``ClusterSpecification`` straight from what ``DeploymentService`` births,
    including the sibling-shaped ``ingress_strategy`` 3 of the 5 shipped
    ``config/deployment-profiles/*.yml`` actually use (coherence-review.md:
    row synthesis is this service's job; cluster_spec.py's own ingress-
    sibling trap)."""
    # `deployment_service`'s own `config_dir` fixture already created
    # `tmp_path/config/deployment-profiles/test-profile.yml` (lacking
    # `node_specification`) -- overwrite it in place with a full block rather
    # than re-``mkdir``ing (same `tmp_path`, shared across fixtures in this test).
    profiles_dir = tmp_path / "config" / "deployment-profiles"
    (profiles_dir / "test-profile.yml").write_text(
        """
version: "1.0"
description: "test profile with a full cluster_spec"
manifests_dir: "config/manifest-templates/test-profile"
resolution_strategy: "branch_discovery_with_fallback"
provider: "fake"
cluster_spec:
  node_specification:
    cpu_cores: 2
    memory_gb: 4
    region_hint: "europe-west"
  cluster_config:
    node_count: 1
    ttl_hours: 2
  ingress_strategy:
    type: "traefik"
services:
  exampleco-core:
    repository: "exampleco-core"
    required: true
"""
    )
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:abc",
        commit="abc123", actor="api:test-user",
    )
    assert result.cluster_id is not None

    load_spec = LoadSpec(uow=uow, clusters=repos.clusters, ssh_identities={})
    output = await load_spec.execute(
        LoadSpecParams(cluster_id=result.cluster_id), make_step_context(cluster_id=result.cluster_id)
    )

    assert output.provider == "fake"
    assert output.spec.node_specification.cpu_cores == 2
    assert output.spec.node_specification.region_hint == "europe-west"
    assert output.spec.cluster_config.node_count == 1
    # The sibling-shaped ingress_strategy made it onto the row and through
    # cluster.load_spec's overlay -- never dropped on the floor.
    assert output.spec.cluster_config.ingress_strategy == {"type": "traefik"}


async def test_reuses_existing_active_cluster_for_same_repo_branch_environment(deployment_service, repos, uow, dispatcher, clock):
    first = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:abc",
        commit="abc123", actor="api:test-user",
    )
    # Bring the cluster to ACTIVE so find_active_cluster_by_branch can see it.
    await dispatcher.apply(
        "cluster", first.cluster_id,
        ProvisionSucceeded(at=clock.now(), actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref"),
    )

    second = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:def",
        commit="def456", actor="api:test-user",
    )
    assert second.cluster_id == first.cluster_id
    assert second.deployment_id != first.deployment_id

    # DR-0031: and it must actually DEPLOY. Everything above this line passed for the
    # whole of Round 10 while a redeploy silently never started -- smoke 4 (2026-08-09)
    # found deployment `64db05b5` sitting in `pending` with zero workflow runs, because
    # PENDING -> DEPLOYING is driven only by ClusterReady and an already-ACTIVE cluster
    # never re-emits it. The assertions above pin the routing DECISION (which cluster,
    # which id); these pin its CONSEQUENCE, which is the part that was broken.
    async with uow() as tx:
        row = repos.deployments.get(tx, second.deployment_id)
        assert row.status == "deploying", (
            "a deployment born onto an ALREADY-ACTIVE cluster must start, not sit in pending"
        )


async def test_redeploy_onto_the_original_active_cluster_actually_starts(
    deployment_service, repos, uow, dispatcher, clock
):
    """DR-0031, the SECOND dispatch site -- and the one that was broken 100% of the time.

    ``DeploymentService.redeploy`` (`:800`) births onto ``original.cluster_id``, so it
    ALWAYS targets an existing cluster; when that cluster is ACTIVE (the entire point of
    redeploying) the new deployment stranded in `pending` forever. Smoke 4 found the
    fault via ``version_update``; this site had **no test at all** -- ``redeploy`` was
    referenced only by ``api/routers/deployments.py:371`` -- which is precisely how it
    stayed broken. DR-0031 chose the dispatcher-level escalation over per-caller
    chaining so that one fix covers both sites; this test is the half that proves the
    "both" part.
    """
    first = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:abc",
        commit="abc123", actor="api:test-user",
    )
    await dispatcher.apply(
        "cluster", first.cluster_id,
        ProvisionSucceeded(at=clock.now(), actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref"),
    )

    again = await deployment_service.redeploy(first.deployment_id, actor="api:redeployer")

    assert again.cluster_id == first.cluster_id
    assert again.deployment_id != first.deployment_id
    async with uow() as tx:
        row = repos.deployments.get(tx, again.deployment_id)
        assert row.status == "deploying", "redeploy onto an ACTIVE cluster must start, not sit in pending"
        # DR-0032: the THIRD birth site records its own actor -- a redeploy is
        # attributed to whoever redeployed, not to whoever deployed originally.
        assert row.deployed_by == "api:redeployer"


async def test_missing_profile_aborts_before_any_cluster_is_created(deployment_service, repos, uow, tmp_path):
    """v1 (orchestrator/cluster_manager.py:1501-1507): the deployment profile is
    fetched BEFORE cluster creation, so a missing/unparseable profile aborts
    before any infrastructure is provisioned. No existing cluster to attach to
    -> nothing is written (deployments.cluster_id is NOT NULL): status is
    'manifest_resolution_failed' with cluster_id=None, never a crash."""
    empty_config_dir = tmp_path / "empty-config"
    empty_config_dir.mkdir()
    deployment_service._config_dir = empty_config_dir  # test-profile.yml unreachable here
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/missing-profile", image="ghcr.io/x/exampleco-core:x",
        commit="x", actor="api:test-user",
    )
    assert result.status == "manifest_resolution_failed"
    assert result.deployment_id is not None
    assert result.cluster_id is None

    async with uow() as tx:
        assert repos.clusters.list_all(tx) == []


async def test_missing_profile_still_rejects_deployment_when_cluster_already_exists(
    deployment_service, repos, uow, dispatcher, clock, tmp_path
):
    """Companion to the above: when an ACTIVE cluster for (repo, branch,
    environment) already exists, a subsequently-broken profile config still
    records a rejected deployment against that KNOWN cluster_id -- no new
    infrastructure is provisioned, matching v1's redeploy-target case (the
    cluster_id was already resolved before the profile fetch)."""
    first = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:abc",
        commit="abc123", actor="api:test-user",
    )
    await dispatcher.apply(
        "cluster", first.cluster_id,
        ProvisionSucceeded(at=clock.now(), actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref"),
    )

    empty_config_dir = tmp_path / "empty-config"
    empty_config_dir.mkdir()
    deployment_service._config_dir = empty_config_dir  # test-profile.yml unreachable here
    result = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/payment-system", image="ghcr.io/x/exampleco-core:def",
        commit="def456", actor="api:test-user",
    )
    assert result.status == "manifest_resolution_failed"
    assert result.cluster_id == first.cluster_id

    async with uow() as tx:
        deployment = repos.deployments.get(tx, result.deployment_id)
        clusters = repos.clusters.list_all(tx)
    assert deployment is not None
    assert deployment.cluster_id == first.cluster_id
    assert {c.id for c in clusters} == {first.cluster_id}  # no new cluster born


async def test_image_resolution_failure_with_valid_profile_still_creates_deployment_record(
    dispatcher, repos, uow, crypto, clock, id_gen, deployment_audits_repo, secrets_repo, tmp_path
):
    """Distinct sub-case from the profile-missing one above: the PROFILE loads
    fine, but a required service's image cannot be resolved (GHCR/registry
    failure). v1 had already created the cluster by this point
    (orchestrator/cluster_manager.py:1507 runs before :1510) -- this sub-case IS
    faithfully ported as birth-then-reject."""
    from seedpod.services.manifests import ManifestResolver
    from seedpod.services.rules import Rule, RuleConfig, RuleEngine

    rules = RuleEngine(RuleConfig(
        version="1.0",
        global_ephemeral_enabled=True,
        default_ttl_hours=2,
        defaults={"ephemeral": {"ttl_hours": 2}, "persistent": {}},
        rules=(
            Rule(
                name="needs_registry", description="", enabled=True,
                branch_patterns=("feature/*",), action="create_ephemeral",
                config={"ttl_hours": 2, "deployment_profile": "needs-registry"},
            ),
        ),
        valid_actions=("create_ephemeral", "no_action"),
        valid_environments=("dev", "staging", "production"),
    ))

    root = tmp_path / "config"
    profiles_dir = root / "deployment-profiles"
    profiles_dir.mkdir(parents=True)
    (profiles_dir / "needs-registry.yml").write_text(
        """
version: "1.0"
description: "test profile with an unresolvable required service"
manifests_dir: "config/manifest-templates/needs-registry"
resolution_strategy: "branch_discovery_with_fallback"
provider: "fake"
services:
  exampleco-core:
    repository: "exampleco-core"
    required: true
  exampleco-other:
    repository: "exampleco-other"
    required: true
"""
    )
    (root / "manifest-templates" / "needs-registry").mkdir(parents=True)

    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=ManifestResolver(ghcr_service=None), dns=None, id_gen=id_gen, config_dir=root,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo,
        default_provider="fake", default_profile="needs-registry",
    )
    result = await service.version_update(
        repo="exampleco-core", branch="feature/needs-registry", image="ghcr.io/x/exampleco-core:x",
        commit="x", actor="api:test-user",
    )
    assert result.status == "manifest_resolution_failed"
    assert result.deployment_id is not None
    assert result.cluster_id is not None

    async with uow() as tx:
        cluster = repos.clusters.get(tx, result.cluster_id)
        deployment = repos.deployments.get(tx, result.deployment_id)
    assert cluster is not None  # cluster WAS born (v1's faithful sub-case)
    assert deployment is not None
    assert deployment.status == DeploymentState.REJECTED.value
    # DR-0032: the rejected branch is a birth site too. "Who triggered the deploy
    # that failed to resolve?" is exactly when this column earns its keep.
    assert deployment.deployed_by == "api:test-user"


class _LockCheckingResolver:
    """Hand-built fake (CLAUDE.md: no Mock/patch) wrapping the real
    ``ManifestResolver``: asserts DR-0008 -- ``.resolve()`` (IO) must never run
    while ``UnitOfWork``'s single-writer lock is held."""

    def __init__(self, inner, uow) -> None:
        self._inner = inner
        self._uow = uow
        self.calls = 0

    async def resolve(self, *args, **kwargs):
        self.calls += 1
        assert not self._uow._lock.locked(), "ManifestResolver.resolve() ran inside an open uow() (DR-0008)"
        return await self._inner.resolve(*args, **kwargs)


async def test_manifest_resolution_never_runs_inside_an_open_uow(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, config_dir,
    deployment_audits_repo, secrets_repo,
):
    checking_resolver = _LockCheckingResolver(manifest_resolver, uow)
    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=checking_resolver, dns=None, id_gen=id_gen, config_dir=config_dir,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )
    result = await service.version_update(
        repo="exampleco-core", branch="feature/dr0008", image="ghcr.io/x/exampleco-core:x",
        commit="x", actor="api:test-user",
    )
    assert checking_resolver.calls == 1
    assert result.status == "queued"


async def test_deployment_preview_mirrors_without_persisting(deployment_service, repos, uow):
    preview = await deployment_service.deployment_preview(
        deployment_profile_name="test-profile",
        triggering_repo="exampleco-core",
        triggering_branch="feature/preview-test",
        triggering_image="ghcr.io/x/exampleco-core:preview123",
    )
    assert preview.status == "success"
    assert preview.deployment_profile == "test-profile"
    assert preview.resolved_images == {"exampleco-core": "ghcr.io/x/exampleco-core:preview123"}

    async with uow() as tx:
        rows = repos.clusters.list_all(tx)
    assert rows == []  # preview persists nothing


async def test_real_secrets_are_loaded_and_threaded_into_resolve(
    deployment_service, repos, uow, secrets_repo, deployment_audits_repo, clock
):
    """Round 9: DeploymentService now loads REAL decrypted secrets and threads
    them into ``ManifestResolver.resolve(secrets=...)`` -- previously silently
    omitted (``secrets=None`` at both call sites, DIAGNOSIS fact 3). Doesn't need
    a template that actually references the secret: ``resolved_secrets`` round-
    trips onto the persisted ``DeploymentAudit`` regardless of whether any
    template consumes it, and THAT round trip (store encrypted -> decrypt -> pass
    to resolve() -> persist again) is what this test pins."""
    async with uow() as tx:
        secrets_repo.upsert(
            tx, environment="ephemeral", key_name="TEST_SECRET", value="super-secret-value",
            key_class="DEV", clock=clock,
        )

    result = await deployment_service.version_update(
        repo="exampleco-core", branch="feature/secrets-test", image="ghcr.io/x/exampleco-core:secrets123",
        commit="secrets123", actor="api:test-user",
    )
    assert result.status == "queued"

    async with uow() as tx:
        deployment = repos.deployments.get(tx, result.deployment_id)
        audit = deployment_audits_repo.get(tx, deployment.spec_ref)

    assert audit.resolved_secrets["TEST_SECRET"] == "super-secret-value"


async def test_unknown_environment_fails_secret_loading_loudly_not_as_zero_secrets(
    dispatcher, repos, uow, crypto, clock, manifest_resolver, id_gen, config_dir,
    deployment_audits_repo, secrets_repo,
):
    """gotcha 8: an environment outside CryptoService's known DEV/PROD mapping must
    raise, not silently resolve to "zero secrets found" (indistinguishable from a
    real, valid, empty environment) -- the crown-jewel-#1 absence-vs-invalid
    conflation CLAUDE.md exists to rule out. Constructed with a rule matrix whose
    ``valid_environments`` accepts "not-a-real-environment" (RuleEngine's own
    validation) so the failure under test is UNAMBIGUOUSLY ``_load_decrypted_
    secrets``'s ``key_class_for_environment`` guard, not RuleEngine rejecting the
    environment name first."""
    from seedpod.app.services.deployment_service import DeploymentService
    from seedpod.services.rules import Rule, RuleConfig, RuleEngine

    rules = RuleEngine(RuleConfig(
        version="1.0",
        global_ephemeral_enabled=True,
        default_ttl_hours=2,
        defaults={},
        rules=(
            Rule(
                name="weird_env", description="", enabled=True,
                branch_patterns=("feature/*",), action="update_environment",
                config={"environment": "not-a-real-environment", "deployment_profile": "test-profile"},
            ),
        ),
        valid_actions=("update_environment", "no_action"),
        valid_environments=("not-a-real-environment",),
    ))
    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=config_dir,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )
    result = await service.version_update(
        repo="exampleco-core", branch="feature/weird-env", image="ghcr.io/x/exampleco-core:x",
        commit="x", actor="api:test-user",
    )
    assert result.status == "manifest_resolution_failed"
    async with uow() as tx:
        deployment = repos.deployments.get(tx, result.deployment_id)
    assert "not-a-real-environment" in deployment.failure_reason


# ---------------------------------------------------------------------------
# DR-0026 (docs/decisions/DR-0026-preview-render-context-and-error-mapping.md)
# -- deployment_preview's redacted-secrets render context. Real exampleco-web-2
# (config/manifest-templates/exampleco-misc/tailscale.yaml's `{{ secrets.
# tailscale_auth_key }}`) is the one shipped profile that references a secret
# with zero GHCR calls (triggering-repo shortcut + tailscale's image_override).
# ---------------------------------------------------------------------------


async def test_redacted_secrets_for_preview_never_returns_plaintext(deployment_service, secrets_repo, uow, clock):
    """DR-0026 part 1's REQUIRED permission-boundary test: the mapping preview
    threads into ``ManifestResolver.resolve(secrets=...)`` carries the
    redaction sentinel for every key that exists in the environment -- NEVER
    the real plaintext value. This is the exact artifact that would leak into
    a rendered manifest (and from there, potentially, an API response) if this
    boundary were ever crossed."""
    async with uow() as tx:
        secrets_repo.upsert(
            tx, environment="ephemeral", key_name="tailscale_auth_key",
            value="tskey-REAL-do-not-leak", key_class="DEV", clock=clock,  # pragma: allowlist secret
        )

    redacted = await deployment_service._redacted_secrets_for_preview("ephemeral")

    assert redacted == {"tailscale_auth_key": "<redacted-for-preview>"}
    assert "tskey-REAL-do-not-leak" not in redacted.values()


async def test_deployment_preview_secret_bearing_profile_succeeds_via_redaction(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, deployment_audits_repo, secrets_repo,
):
    """DR-0026 part 1: previewing ``exampleco-web-2`` (a REAL, shipped, secret-
    bearing profile) succeeds once ``tailscale_auth_key`` is known to the
    environment -- ``StrictUndefined`` is satisfied by the redaction sentinel
    alone, with no decrypt call anywhere on this path. Before DR-0026,
    ``secrets={}`` always meant this same call would raise
    ``'tailscale_auth_key' is undefined`` for every secret-bearing profile."""
    async with uow() as tx:
        secrets_repo.upsert(
            tx, environment="ephemeral", key_name="tailscale_auth_key",
            value="tskey-REAL-do-not-leak", key_class="DEV", clock=clock,  # pragma: allowlist secret
        )
    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=_REPO_CONFIG_DIR,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )

    preview = await service.deployment_preview(
        deployment_profile_name="exampleco-web-2",
        triggering_repo="exampleco-web-2",
        triggering_branch="feature/dr-0026-preview",
        triggering_image="ghcr.io/exampleco/exampleco-web-2:feature-dr-0026-preview-p1",
    )

    assert preview.status == "success"
    assert "tailscale.yaml" in preview.template_files


async def test_deployment_preview_missing_secret_raises_not_silently_lenient(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, deployment_audits_repo, secrets_repo,
):
    """DR-0026 part 1's other required half: a key a template references but
    that is genuinely ABSENT from the environment must still raise -- the
    redaction mapping only covers keys that EXIST; it must not quietly make
    everything pass, which would reopen the exact silent-empty hole Round 9
    closed for the real deploy path."""
    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=_REPO_CONFIG_DIR,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )

    with pytest.raises(PermanentError, match="tailscale_auth_key"):
        await service.deployment_preview(
            deployment_profile_name="exampleco-web-2",
            triggering_repo="exampleco-web-2",
            triggering_branch="feature/dr-0026-preview-missing",
            triggering_image="ghcr.io/exampleco/exampleco-web-2:feature-dr-0026-preview-missing-p1",
        )


async def test_deployment_preview_renders_a_provider_host_profile_via_synthetic_hostname(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, deployment_audits_repo,
    secrets_repo, tmp_path,
):
    """DR-0025 E2 + this method's "identical template-rendering path" contract: a
    `provider_host` profile OMITS `cluster_hostname` until its cluster exists, so preview
    -- which has no cluster and never will -- used to fail it against StrictUndefined.
    Every such profile (both shipped `-nodns` ones) was therefore un-pre-flightable while
    the same profile deployed fine, which is exactly backwards for a tool whose job is to
    catch problems before spending infrastructure.

    Preview substitutes a SYNTHETIC hostname rather than adopting `_deploy`'s
    `render=False` deferral, so the render actually HAPPENS -- asserted via
    `template_files`, since `resolve(render=False)` returns an empty tuple and would let
    this "succeed" having checked nothing.
    """
    root = tmp_path / "config"
    (root / "deployment-profiles").mkdir(parents=True)
    (root / "manifest-templates" / "hostful").mkdir(parents=True)
    (root / "deployment-profiles" / "hostful.yml").write_text(
        'version: "1.0"\n'
        'manifests_dir: "config/manifest-templates/hostful"\n'
        'environment_type: "ephemeral"\n'
        'resolution_strategy: "branch_discovery_with_fallback"\n'
        'hostname:\n'
        '  strategy: "provider_host"\n'
        "services:\n"
        "  app:\n"
        '    repository: "app"\n'
        "    image_override: \"nginx:latest\"\n"
        "    required: true\n"
    )
    # The template READS the hostname -- without the substitution this render raises.
    (root / "manifest-templates" / "hostful" / "app.yaml").write_text(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app\n"
        "data:\n  url: \"https://{{ cluster_hostname }}/x\"\n"
    )
    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=root,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )

    preview = await service.deployment_preview(
        deployment_profile_name="hostful",
        triggering_repo="app",
        triggering_branch="staging",
        triggering_image="nginx:latest",
    )

    assert preview.status == "success"
    assert "app.yaml" in preview.template_files  # the render really ran


# ---------------------------------------------------------------------------
# DR-0027 (docs/decisions/DR-0027-secret-scope-is-the-rule-derived-environment.md)
# -- secrets are scoped by the environment a deployment is actually RECORDED
# under (the rule-derived/passed `environment`), never the profile's own
# `environment_type`. No shipped profile distinguishes the two (all five
# declare `environment_type: "ephemeral"`), so the mandatory pin below needs a
# purpose-built fixture profile; the `deployment_preview` half instead reuses
# the real `exampleco-web-2` profile, varying WHERE the secret is stored.
# ---------------------------------------------------------------------------


async def test_deploy_scopes_secrets_by_passed_environment_not_profile_environment_type(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen,
    deployment_audits_repo, secrets_repo, tmp_path,
):
    """DR-0027's MANDATORY regression pin. A profile whose own
    ``environment_type: "ephemeral"`` differs from the environment a
    deployment is actually triggered/recorded under (``staging``, via
    ``deploy_direct``'s direct ``environment=`` argument -- no rule
    evaluation needed) must render against the PASSED environment's secrets
    -- never the profile's. v1's opposite behaviour
    (``reference-code/seedpod/seedpod/orchestrator/cluster_manager.py:
    1651-1655``) is the deliberately-not-ported bug this pins against: a
    v1-faithful port would have rendered against ``ephemeral-value``, not
    ``staging-value``, below."""
    root = tmp_path / "config"
    profiles_dir = root / "deployment-profiles"
    profiles_dir.mkdir(parents=True)
    (profiles_dir / "divergent-env-profile.yml").write_text(
        """
version: "1.0"
description: "environment_type deliberately differs from the deploy-time environment"
manifests_dir: "config/manifest-templates/divergent-env-profile"
resolution_strategy: "branch_discovery_with_fallback"
provider: "fake"
environment_type: "ephemeral"
services:
  exampleco-core:
    repository: "exampleco-core"
    required: true
"""
    )
    (root / "manifest-templates" / "divergent-env-profile").mkdir(parents=True)

    async with uow() as tx:
        secrets_repo.upsert(
            tx, environment="ephemeral", key_name="TEST_SECRET", value="ephemeral-value",
            key_class="DEV", clock=clock,
        )
        secrets_repo.upsert(
            tx, environment="staging", key_name="TEST_SECRET", value="staging-value",
            key_class="DEV", clock=clock,
        )

    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=root,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )

    result = await service.deploy_direct(
        profile_name="divergent-env-profile", environment="staging",
        repo="exampleco-core", branch="feature/dr-0027", image="ghcr.io/x/exampleco-core:dr0027",
        commit="dr0027", actor="api:test-user",
    )
    assert result.status == "queued"
    assert result.environment == "staging"

    async with uow() as tx:
        deployment = repos.deployments.get(tx, result.deployment_id)
        audit = deployment_audits_repo.get(tx, deployment.spec_ref)

    # The PASSED environment's secret, not the profile's own environment_type's.
    assert audit.resolved_secrets["TEST_SECRET"] == "staging-value"


async def test_deployment_preview_environment_override_is_exact(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, deployment_audits_repo, secrets_repo,
):
    """DR-0027's preview half: an explicit ``environment=`` overrides the
    profile's OWN ``environment_type`` ("ephemeral" for exampleco-web-2) exactly.
    ``tailscale_auth_key`` is stored ONLY under "staging" -- preview succeeds
    only when the override actually names that environment."""
    async with uow() as tx:
        secrets_repo.upsert(
            tx, environment="staging", key_name="tailscale_auth_key",
            value="tskey-staging-only", key_class="DEV", clock=clock,  # pragma: allowlist secret
        )
    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=_REPO_CONFIG_DIR,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )

    preview = await service.deployment_preview(
        deployment_profile_name="exampleco-web-2",
        triggering_repo="exampleco-web-2",
        triggering_branch="feature/dr-0027-override",
        triggering_image="ghcr.io/exampleco/exampleco-web-2:feature-dr-0027-override-p1",
        environment="staging",
    )
    assert preview.status == "success"


async def test_deployment_preview_without_override_falls_back_to_profile_environment_type(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, deployment_audits_repo, secrets_repo,
):
    """The complementary half: omitting ``environment=`` falls back to
    exampleco-web-2's own ``environment_type`` ("ephemeral") -- the explicitly
    APPROXIMATE case DR-0027 names. The SAME ``tailscale_auth_key``, stored
    only under "staging" (as in the test above), is therefore genuinely
    unknown under "ephemeral", so preview must still raise -- not silently
    succeed against the wrong environment's secret set."""
    async with uow() as tx:
        secrets_repo.upsert(
            tx, environment="staging", key_name="tailscale_auth_key",
            value="tskey-staging-only", key_class="DEV", clock=clock,  # pragma: allowlist secret
        )
    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=_REPO_CONFIG_DIR,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )

    with pytest.raises(PermanentError, match="tailscale_auth_key"):
        await service.deployment_preview(
            deployment_profile_name="exampleco-web-2",
            triggering_repo="exampleco-web-2",
            triggering_branch="feature/dr-0027-no-override",
            triggering_image="ghcr.io/exampleco/exampleco-web-2:feature-dr-0027-no-override-p1",
        )


# ---------------------------------------------------------------------------
# DR-0028 Erratum E2: `data_initialization` reaches the stored audit
# ---------------------------------------------------------------------------


async def test_preset_deploy_data_initialization_reaches_the_stored_audit_end_to_end(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, config_dir,
    deployment_audits_repo, secrets_repo,
):
    """DR-0028 Erratum E2, closed. Before this fix, NOTHING wrote
    ``resolved_config["data_initialization"]``: ``PresetService.deploy`` only
    echoed the request into the response ``message`` (this module's own
    docstring, pre-fix), so ``deploy.load_audit``'s own already-pinned reader
    (``tests/engine/steps/test_deploy_steps.py``) always saw ``None`` in
    production -- an operator who explicitly requested a snapshot restore got
    no error and no restore, "strictly worse than failing... because the
    operator believes their data was restored" (DR-0028 Erratum E2's own
    words).

    This is the WRITER half of that same contract: a real
    ``PresetService.deploy`` call carrying ``data_initialization`` all the way
    through ``DeploymentService.deploy_direct`` -> ``_build_resolved_config``
    into the stored ``deployment_audits`` row, then back OUT through the real,
    registered ``deploy.load_audit`` step -- the exact verb
    ``deploy-waves.yml``'s ``audit`` step runs -- to a non-``None``
    ``DeployLoadAuditOutput.data_initialization``. ``service_overrides``
    supplies an explicit image tag for ``test-profile``'s one service
    (``exampleco-core``) so ``ManifestResolver.resolve`` short-circuits GHCR
    entirely (``manifest_resolver`` here is built with ``ghcr_service=None``,
    and ``PresetService.deploy``'s synthetic ``repo=f"preset:{name}"`` does
    NOT match the triggering-repo shortcut the way a direct ``version_update``
    call would)."""
    deployment_service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=config_dir,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )
    preset_service = PresetService(PresetRepository(), deployment_service, uow, clock, id_gen, config_dir)

    preset = await preset_service.create(
        name="restore-preset", description=None, profile_name="test-profile",
        service_overrides={"exampleco-core": {"tag": "v1"}}, default_branch="main", default_ttl_hours=None,
        naming_strategy=None, created_by="api:test-user",
    )

    result = await preset_service.deploy(
        preset.id, data_initialization={"restore_from_snapshot": "snap-abc123"}, actor="api:test-user",
    )
    assert result.status == "queued"

    step = DeployLoadAudit(
        uow=uow, deployments=repos.deployments, deployment_audits=deployment_audits_repo,
        clusters=repos.clusters, manifest_resolver=manifest_resolver, config_dir=config_dir,
    )
    output = await step.execute(
        DeployLoadAuditParams(deployment_id=result.deployment_id), make_step_context(cluster_id=result.cluster_id)
    )

    assert output.data_initialization == SnapshotRestoreSpec(restore_from_snapshot="snap-abc123")


# ---------------------------------------------------------------------------
# DR-0025 Erratum E2: `provider_host` DEFERS at decision time (point (i)),
# then REHYDRATES at deploy time (point (ii)) -- closing the part-1/part-2
# contradiction Erratum E2 itself diagnosed. The restore-and-rehydrate
# component's own gate: "no 'https:///' string can reach an applied
# manifest."
# ---------------------------------------------------------------------------

# `exampleco-dev-stack-nodns.yml`'s own required, non-EXTERNAL, non-triggering-
# repo services -- none carries a profile-level `image_override` (only its
# EXTERNAL required services do: postgres/cache/minio/rbac, all declared
# straight in the profile's own YAML), so a deploy against it with
# `ghcr_service=None` (this module's own `manifest_resolver` fixture) needs an
# explicit REQUEST-level override for each one, or image resolution itself
# fails loud -- independent of anything this test is actually about (DR-0025
# Erratum E2 is scoped to hostname resolution alone, never image resolution).
_NODNS_REQUIRED_IMAGE_OVERRIDES = {
    "exampleco-migrations": "ghcr.io/exampleco/exampleco-migrations:e2test",
}

# Every `secrets.<key>` reference `config/manifest-templates/exampleco-stack/*.yaml`
# carries (grep-verified across the whole tree; `ghcr_dockerconfig_json` is
# NOT in this list -- `_add_ghcr_auth_if_needed` synthesizes it automatically
# for the ghcr.io triggering image this test uses). None of these values are
# under test here -- DR-0025 Erratum E2 is scoped to hostname resolution
# alone -- this list exists purely so StrictUndefined doesn't fail this real
# profile's render for an unrelated reason.
_NODNS_REQUIRED_SECRETS = (
    "cache_password", "database_password", "jwt_secret", "mail_password",
    "minio_root_password", "s3_access_key", "s3_secret_key",
)


async def test_provider_host_profile_defers_at_decision_time_then_rehydrates_at_deploy_time(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, deployment_audits_repo, secrets_repo,
):
    """THE test that closes DR-0025: ``exampleco-dev-stack-nodns.yml`` (the REAL
    shipped ``provider_host`` profile -- the one DR-0025's own Consequences
    named as unable to complete a first deployment "between Round 9 and Round
    10") now completes one, end to end, in two verified phases.

    1. **Decision time (Erratum E2 point (i))** -- deploying it no longer
       REJECTS. ``deploy_direct`` reaches ``queued`` (not
       ``manifest_resolution_failed``); a ``deployment_audits`` row IS
       written -- image resolution ran to completion (``resolved_images``
       non-empty) -- marked DEFERRED, with NO rendered manifests
       (``resolved_manifests == ""``) and no ``cluster_hostname`` key at all
       (Erratum E1's own OMITTED branch, unchanged).
    2. **Deploy time (Erratum E2 point (ii))** -- once the cluster is
       provisioned for real (``ProvisionSucceeded``, the EXACT event
       ``core/machine.py``'s ``_deployment_pending_cluster_ready`` requires
       before ``RunWorkflow(workflow="deploy", ...)`` is ever dispatched --
       so this is not a contrived ordering, it is the only ordering a live
       run could ever reach ``deploy.load_audit`` in), the REAL, registered
       ``DeployLoadAudit`` step renders successfully against the real host --
       and the concrete assertion that actually closes this DR: no
       ``https:///`` string reaches the rewritten, applied-manifest audit
       row, anywhere."""
    service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=_REPO_CONFIG_DIR,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo, default_provider="fake",
    )
    async with uow() as tx:
        for key in _NODNS_REQUIRED_SECRETS:
            secrets_repo.upsert(tx, environment="ephemeral", key_name=key, value=f"{key}-value", key_class="DEV", clock=clock)

    result = await service.deploy_direct(
        profile_name="exampleco-dev-stack-nodns", environment="ephemeral", repo="exampleco-api",
        branch="feature/dr-0025-e2", image="ghcr.io/exampleco/exampleco-core:e2test", commit="e2test",
        image_overrides=_NODNS_REQUIRED_IMAGE_OVERRIDES, actor="api:test-user",
    )

    # -- (1) decision time: DEFERRED, never rejected --
    assert result.status == "queued"
    async with uow() as tx:
        deployment = repos.deployments.get(tx, result.deployment_id)
        audit = deployment_audits_repo.get(tx, deployment.spec_ref)
    assert audit.resolved_config[DEFERRED_MANIFEST_RENDERING_KEY] is True
    assert "cluster_hostname" not in audit.resolved_config
    assert audit.resolved_manifests == ""
    assert audit.resolved_images  # image resolution proceeded in full (point (i))

    # -- (2) deploy time: the cluster provisions for real --
    await dispatcher.apply(
        "cluster", result.cluster_id,
        ProvisionSucceeded(at=clock.now(), actor="engine:run:r1", public_ip="203.0.113.42", kubeconfig_ref="ref"),
    )

    step = DeployLoadAudit(
        uow=uow, deployments=repos.deployments, deployment_audits=deployment_audits_repo,
        clusters=repos.clusters, manifest_resolver=manifest_resolver, config_dir=_REPO_CONFIG_DIR,
    )
    output = await step.execute(
        DeployLoadAuditParams(deployment_id=result.deployment_id),
        make_step_context(cluster_id=result.cluster_id),
    )
    assert output.manifests  # real manifests now -- not the deferred-empty case

    async with uow() as tx:
        rehydrated_audit = deployment_audits_repo.get(tx, deployment.spec_ref)
    assert rehydrated_audit.resolved_config[DEFERRED_MANIFEST_RENDERING_KEY] is False
    assert rehydrated_audit.resolved_config[MANIFEST_RENDERING_REHYDRATED_KEY] is True
    assert rehydrated_audit.resolved_config["cluster_hostname"] == "203.0.113.42"
    assert rehydrated_audit.resolved_manifests != ""
    assert "203.0.113.42" in rehydrated_audit.resolved_manifests
    assert "https:///" not in rehydrated_audit.resolved_manifests
