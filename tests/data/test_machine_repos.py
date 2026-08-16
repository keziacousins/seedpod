"""Round-4 "repos-machine": ClusterRepository, DeploymentRepository,
ClusterStateAuditRepository, DeploymentStateAuditRepository, DeploymentAuditRepository
-- against real tmp SQLite (0001_initial.sql). No mocks.

Covers: row<->record round-trip fidelity, CAS persist + StaleVersion, the
ux_clusters_slug_live live/terminal slug behavior (reusable after destroyed/failed,
still reserved while destroy-failed), the Dispatcher's deployments_in Cascade
primitive, state-audit shape (actor, no trigger/initiated_by), and the
DeploymentAuditRepository crypto round-trip via a real CryptoService (Fernet).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from seedpod.core.clock import FrozenClock
from seedpod.core.codec import encode
from seedpod.core.events import (
    CreateRequested,
    DeployRejected,
    DeployRequested,
    HealthCheckFailed,
)
from seedpod.core.machine import StaleVersion
from seedpod.core.records import (
    ClusterRecord,
    ClusterState,
    DeploymentRecord,
    DeploymentState,
    Origin,
)
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import (
    ClusterRepository,
    ClusterRow,
    ClusterStateAuditRepository,
    DeploymentAuditRepository,
    DeploymentAuditRow,
    DeploymentRepository,
    DeploymentRow,
    DeploymentStateAuditRepository,
)
from seedpod.data.uow import UnitOfWork
from seedpod.services.crypto import CryptoService

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)
LATER = NOW + timedelta(minutes=5)

clusters = ClusterRepository()
deployments = DeploymentRepository()
cluster_audits = ClusterStateAuditRepository()
deployment_audits_state = DeploymentStateAuditRepository()


@pytest.fixture
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(database.engine)
    return database


@pytest.fixture
def uow(db):
    return UnitOfWork(db)


def make_cluster_row(cluster_id: str, slug: str, *, status: str = "active", **overrides) -> ClusterRow:
    fields = {
        "id": cluster_id,
        "name": cluster_id,
        "slug": slug,
        "origin": Origin.MANAGED,
        "environment": "ephemeral",
        "repository": "org/repo",
        "branch": "main",
        "status": status,
        "pre_destroy_state": None,
        "version": 0,
        "provider": "fake",
        "provider_config": {"size": "small"},
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
        "resolved_images": {"web": "ghcr.io/org/web:sha"},
        "superseded_by": None,
        "deployed_by": "api:test",
        "failure_reason": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    fields.update(overrides)
    return DeploymentRow(**fields)


# ---------------------------------------------------------------------------
# ClusterRepository
# ---------------------------------------------------------------------------


async def test_insert_and_get_cluster_round_trips(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))

    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched == make_cluster_row("c1", "demo")


async def test_get_missing_cluster_returns_none(uow):
    async with uow() as tx:
        assert clusters.get(tx, "does-not-exist") is None


async def test_load_narrows_row_to_pure_record(uow):
    async with uow() as tx:
        clusters.insert(
            tx,
            make_cluster_row(
                "c1", "demo", status="provisioning", provider_resources={"droplet_id": "d-1"}
            ),
        )

    async with uow() as tx:
        record = clusters.load(tx, "c1")
    assert record == ClusterRecord(
        id="c1",
        name="c1",
        state=ClusterState.PROVISIONING,
        version=0,
        provider="fake",
        environment="ephemeral",
        origin=Origin.MANAGED,
        expires_at=None,
        public_ip=None,
        kubeconfig_ref=None,
        provider_resources={"droplet_id": "d-1"},
        pre_destroy_state=None,
        failure_reason=None,
    )


async def test_load_missing_cluster_returns_none(uow):
    async with uow() as tx:
        assert clusters.load(tx, "does-not-exist") is None


async def test_persist_cas_update_succeeds_and_bumps_version(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="new", version=0))

    async with uow() as tx:
        record = clusters.load(tx, "c1")
        new = ClusterRecord(
            id=record.id,
            name=record.name,
            state=ClusterState.PROVISIONING,
            version=record.version,
            provider=record.provider,
            environment=record.environment,
            origin=record.origin,
            expires_at=record.expires_at,
            public_ip=record.public_ip,
            kubeconfig_ref=record.kubeconfig_ref,
            provider_resources=record.provider_resources,
            pre_destroy_state=record.pre_destroy_state,
            failure_reason=record.failure_reason,
        )
        clusters.persist(tx, new, record.version, clock=FrozenClock(LATER))

    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched.status == "provisioning"
    assert fetched.version == 1
    assert fetched.updated_at == LATER
    assert fetched.slug == "demo"  # row-only column untouched by persist


async def test_persist_stale_version_raises(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="active", version=0))

    async with uow() as tx:
        record = clusters.load(tx, "c1")

    with pytest.raises(StaleVersion):
        async with uow() as tx:
            clusters.persist(
                tx,
                ClusterRecord(
                    id=record.id,
                    name=record.name,
                    state=ClusterState.FAILED,
                    version=record.version,
                    provider=record.provider,
                    environment=record.environment,
                    origin=record.origin,
                    failure_reason="boom",
                ),
                expected_version=99,  # wrong -- another writer would have moved it
                clock=FrozenClock(LATER),
            )

    # untouched
    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched.status == "active"
    assert fetched.version == 0


async def test_persist_leaves_row_only_columns_alone(uow):
    """persist() is a CAS UPDATE over exactly the ClusterRecord-mapped columns --
    slug/provider_config/node_count/cost/crypto columns are NEVER touched by it."""
    async with uow() as tx:
        clusters.insert(
            tx,
            make_cluster_row(
                "c1", "demo", status="active", version=0,
                provider_config={"region": "nyc1"}, node_count=3, cost_per_hour=0.5,
                encrypted_kubeconfig="ciphertext", kubeconfig_key_class="DEV",
            ),
        )

    async with uow() as tx:
        record = clusters.load(tx, "c1")
        clusters.persist(
            tx,
            ClusterRecord(
                id=record.id, name=record.name, state=ClusterState.ACTIVE,
                version=record.version, provider=record.provider,
                environment=record.environment, origin=record.origin, public_ip="1.2.3.4",
            ),
            record.version,
            clock=FrozenClock(LATER),
        )

    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched.public_ip == "1.2.3.4"
    assert fetched.provider_config == {"region": "nyc1"}
    assert fetched.node_count == 3
    assert fetched.cost_per_hour == 0.5
    assert fetched.encrypted_kubeconfig == "ciphertext"
    assert fetched.kubeconfig_key_class == "DEV"


async def test_set_health_failures_dedicated_write_path(uow):
    """docs/design/seam-d-foundation.md Decision 6: consecutive_health_failures is
    mutated ONLY via a dedicated repo method (v1 job_manager.py:634,647's
    reset-to-0 / increment-to-N bypass is gone, the column stays)."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", consecutive_health_failures=0))

    async with uow() as tx:
        clusters.set_health_failures(tx, "c1", 3, clock=FrozenClock(LATER))

    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched.consecutive_health_failures == 3
    assert fetched.updated_at == LATER

    async with uow() as tx:
        clusters.set_health_failures(tx, "c1", 0, clock=FrozenClock(LATER))  # reset

    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched.consecutive_health_failures == 0


async def test_set_health_failures_does_not_bump_version(uow):
    """A plain UPDATE, not a CAS -- this counter runs parallel to, not as part of,
    the ClusterRecord the pure machine transitions, so it must not perturb the
    optimistic-concurrency version an in-flight Persist is racing against."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", version=5))

    async with uow() as tx:
        clusters.set_health_failures(tx, "c1", 1, clock=FrozenClock(LATER))

    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched.version == 5


async def test_set_last_reconciled_at_does_not_touch_updated_at(uow):
    """A sweep that observed no drift changed nothing about the cluster, so it must not
    move ``updated_at`` -- the column the API serialises to the SPA as "when this
    cluster last changed". Deliberately UNLIKE its set_health_failures/update_cost
    siblings, which record real changes.

    Found on 2026-08-13: every cluster in the minimax DB carried a millisecond-identical
    ``updated_at``, including one destroyed four days earlier, because the reconciler
    stamped them all on every pass. It cost real diagnosis time -- the TTL destroy could
    not be correlated with its own cluster row."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))

    async with uow() as tx:
        before = clusters.get(tx, "c1").updated_at

    async with uow() as tx:
        clusters.set_last_reconciled_at(tx, ["c1"], clock=FrozenClock(LATER))

    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched.last_reconciled_at == LATER  # the column that DOES move
    assert fetched.updated_at == before  # and the one that must not


async def test_update_cost_dedicated_write_path(uow):
    """v1's update_cluster_cost (reference-code/seedpod/seedpod/data/repositories.py
    lines 318-326) salvaged as a dedicated repo method, same shape as
    set_health_failures: plain UPDATE, no CAS, no version bump."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", total_cost=0.0, version=2))

    async with uow() as tx:
        clusters.update_cost(tx, "c1", 12.5, clock=FrozenClock(LATER))

    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched.total_cost == 12.5
    assert fetched.version == 2
    assert fetched.updated_at == LATER


async def test_set_kubeconfig_dedicated_write_path(uow):
    """cluster.store_kubeconfig's (DR-0022, replaces kubeconfig.store) dedicated
    write path: plain UPDATE, no CAS, no version bump -- same discipline as
    set_health_failures/update_cost/set_expires_at."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", version=1))

    async with uow() as tx:
        wrote = clusters.set_kubeconfig(tx, "c1", encrypted_kubeconfig="ciphertext", key_class="DEV", clock=FrozenClock(LATER))
    assert wrote is True

    async with uow() as tx:
        fetched = clusters.get(tx, "c1")
    assert fetched.encrypted_kubeconfig == "ciphertext"
    assert fetched.kubeconfig_key_class == "DEV"
    assert fetched.updated_at == LATER
    assert fetched.version == 1  # not bumped


async def test_set_kubeconfig_returns_false_for_a_row_that_no_longer_exists(uow):
    """Round-8a review finding: a lost write must be detectable by the caller
    (cluster.store_kubeconfig raises rather than minting a kubeconfig_ref for a
    write that silently affected zero rows) -- rowcount-tells-the-story, same
    idiom as WorkflowRunRepository.insert_admitted. "does-not-exist" here stands
    in for "vanished between read and write" as far as this UPDATE statement
    can tell -- it has no way to distinguish "never existed" from "existed and
    was deleted a moment ago"."""
    async with uow() as tx:
        wrote = clusters.set_kubeconfig(
            tx, "does-not-exist", encrypted_kubeconfig="ciphertext", key_class="DEV", clock=FrozenClock(LATER)
        )
    assert wrote is False


async def test_get_by_slug_active_only_default(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="active"))

    async with uow() as tx:
        found = clusters.get_by_slug(tx, "demo")
    assert found is not None
    assert found.id == "c1"


async def test_slug_reusable_after_destroyed_or_failed(uow):
    """ux_clusters_slug_live: TERMINAL_STATES-driven -- a destroyed cluster's slug is
    free for reuse by a brand-new row (the unique index only covers LIVE rows)."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="destroyed"))
        clusters.insert(tx, make_cluster_row("c2", "demo", status="new"))  # same slug, allowed

    async with uow() as tx:
        active_only = clusters.get_by_slug(tx, "demo", active_only=True)
    assert active_only.id == "c2"  # the live one

    async with uow() as tx:
        not_filtered = clusters.get_by_slug(tx, "demo", active_only=False)
    assert not_filtered is not None  # either row is a legitimate match


async def test_slug_still_reserved_while_destroy_failed(uow):
    """'destroy-failed' is deliberately LIVE (the cluster still owns real infra) --
    the unique index (and thus a second insert of the same slug) must reject it."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="destroy-failed"))

    with pytest.raises(IntegrityError):
        async with uow() as tx:
            clusters.insert(tx, make_cluster_row("c2", "demo", status="new"))


async def test_slug_still_reserved_while_zombie(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="zombie"))

    with pytest.raises(IntegrityError):
        async with uow() as tx:
            clusters.insert(tx, make_cluster_row("c2", "demo", status="new"))


async def test_get_by_id_or_slug_prefers_id_regardless_of_active_only(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="destroyed"))

    async with uow() as tx:
        found = clusters.get_by_id_or_slug(tx, "c1")  # active_only=True default, but this is an ID match
    assert found is not None
    assert found.id == "c1"


async def test_get_by_id_or_slug_falls_back_to_slug(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="active"))

    async with uow() as tx:
        found = clusters.get_by_id_or_slug(tx, "demo")
    assert found is not None
    assert found.id == "c1"


async def test_find_active_cluster_by_branch(uow):
    async with uow() as tx:
        clusters.insert(
            tx, make_cluster_row("c1", "demo", status="active", repository="org/repo", branch="main")
        )
        clusters.insert(
            tx,
            make_cluster_row(
                "c2", "demo-old", status="destroyed", repository="org/repo", branch="main"
            ),
        )

    async with uow() as tx:
        found = clusters.find_active_cluster_by_branch(tx, "org/repo", "main", "ephemeral")
    assert found is not None
    assert found.id == "c1"

    async with uow() as tx:
        assert clusters.find_active_cluster_by_branch(tx, "org/repo", "other-branch", "ephemeral") is None


async def test_find_clusters_by_branch_any_state_newest_first(uow):
    async with uow() as tx:
        clusters.insert(
            tx,
            make_cluster_row(
                "c1", "demo1", status="destroyed", repository="org/repo", branch="main",
                created_at=NOW,
            ),
        )
        clusters.insert(
            tx,
            make_cluster_row(
                "c2", "demo2", status="active", repository="org/repo", branch="main",
                created_at=LATER,
            ),
        )

    async with uow() as tx:
        found = clusters.find_clusters_by_branch(tx, "org/repo", "main", "ephemeral")
    assert [c.id for c in found] == ["c2", "c1"]


async def test_list_by_status(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "s1", status="active"))
        clusters.insert(tx, make_cluster_row("c2", "s2", status="failed"))
        clusters.insert(tx, make_cluster_row("c3", "s3", status="active"))

    async with uow() as tx:
        found = {c.id for c in clusters.list_by_status(tx, ["active"])}
    assert found == {"c1", "c3"}

    async with uow() as tx:
        assert clusters.list_by_status(tx, []) == []


async def test_list_expired_excludes_terminal_and_future(uow):
    async with uow() as tx:
        clusters.insert(
            tx, make_cluster_row("c1", "s1", status="active", expires_at=NOW - timedelta(hours=1))
        )
        clusters.insert(
            tx, make_cluster_row("c2", "s2", status="active", expires_at=NOW + timedelta(hours=1))
        )
        clusters.insert(
            tx,
            make_cluster_row(
                "c3", "s3", status="destroyed", expires_at=NOW - timedelta(hours=1)
            ),
        )
        clusters.insert(tx, make_cluster_row("c4", "s4", status="active", expires_at=None))

    async with uow() as tx:
        found = {c.id for c in clusters.list_expired(tx, NOW)}
    assert found == {"c1"}


# ---------------------------------------------------------------------------
# DeploymentRepository
# ---------------------------------------------------------------------------


async def test_insert_and_get_deployment_round_trips(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1"))

    async with uow() as tx:
        fetched = deployments.get(tx, "d1")
    assert fetched == make_deployment_row("d1", "c1")


async def test_deployment_load_narrows_to_record(uow, crypto):
    repo = DeploymentAuditRepository(crypto)
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        repo.insert(tx, make_deployment_audit_row("audit-1", None, "c1"))
        deployments.insert(tx, make_deployment_row("d1", "c1", status="deploying", spec_ref="audit-1"))

    async with uow() as tx:
        record = deployments.load(tx, "d1")
    assert record == DeploymentRecord(
        id="d1",
        cluster_id="c1",
        state=DeploymentState.DEPLOYING,
        version=0,
        environment="ephemeral",
        manifest_version="v1",
        spec_ref="audit-1",
        resolved_images={"web": "ghcr.io/org/web:sha"},
        superseded_by=None,
        failure_reason=None,
    )


async def test_deployment_persist_cas_and_stale_version(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1", status="deploying", version=0))

    async with uow() as tx:
        record = deployments.load(tx, "d1")
        deployments.persist(
            tx,
            DeploymentRecord(
                id=record.id, cluster_id=record.cluster_id, state=DeploymentState.ACTIVE,
                version=record.version, environment=record.environment,
                manifest_version=record.manifest_version, resolved_images={"web": "sha256:abc"},
            ),
            record.version,
            clock=FrozenClock(LATER),
        )

    async with uow() as tx:
        fetched = deployments.get(tx, "d1")
    assert fetched.status == "active"
    assert fetched.version == 1
    assert fetched.resolved_images == {"web": "sha256:abc"}
    assert fetched.updated_at == LATER

    with pytest.raises(StaleVersion):
        async with uow() as tx:
            deployments.persist(
                tx,
                DeploymentRecord(
                    id="d1", cluster_id="c1", state=DeploymentState.FAILED, version=1,
                    environment="ephemeral", manifest_version="v1", failure_reason="boom",
                ),
                expected_version=0,  # stale on purpose
                clock=FrozenClock(LATER),
            )


async def test_active_for_cluster(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1", status="superseded", created_at=NOW))
        deployments.insert(tx, make_deployment_row("d2", "c1", status="active", created_at=LATER))

    async with uow() as tx:
        found = deployments.active_for_cluster(tx, "c1")
    assert found is not None
    assert found.id == "d2"


async def test_list_for_cluster_newest_first(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1", created_at=NOW))
        deployments.insert(tx, make_deployment_row("d2", "c1", created_at=LATER))

    async with uow() as tx:
        found = [d.id for d in deployments.list_for_cluster(tx, "c1")]
    assert found == ["d2", "d1"]


async def test_deployments_in_cascade_primitive(uow):
    """Dispatcher's Cascade effect: every deployment of a cluster in `where_state`,
    excluding `except_id` (docs/design/coherence-review.md Conflict 3)."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1", status="active"))
        deployments.insert(tx, make_deployment_row("d2", "c1", status="pending"))
        deployments.insert(tx, make_deployment_row("d3", "c1", status="destroyed"))

    async with uow() as tx:
        found = deployments.deployments_in(
            tx, "c1", frozenset({DeploymentState.ACTIVE, DeploymentState.PENDING}), except_id=None
        )
    assert {d.id for d in found} == {"d1", "d2"}
    assert all(isinstance(d, DeploymentRecord) for d in found)


async def test_deployments_in_excludes_except_id(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1", status="active"))
        deployments.insert(tx, make_deployment_row("d2", "c1", status="active"))

    async with uow() as tx:
        found = deployments.deployments_in(
            tx, "c1", frozenset({DeploymentState.ACTIVE}), except_id="d1"
        )
    assert {d.id for d in found} == {"d2"}


async def test_deployments_in_empty_where_state_returns_empty(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1", status="active"))

    async with uow() as tx:
        assert deployments.deployments_in(tx, "c1", frozenset(), None) == []


# ---------------------------------------------------------------------------
# ClusterStateAuditRepository / DeploymentStateAuditRepository
# ---------------------------------------------------------------------------


async def test_cluster_state_audit_add_and_list(uow):
    """DR-0007: a reasonless event class (``CreateRequested`` declares no ``reason``
    field) lands ``reason IS NULL`` -- never invented -- and ``context`` is the full
    tagged event verbatim, not a caller-supplied grab-bag."""
    event = CreateRequested(at=NOW, actor="api:alice")
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="new"))
        cluster_audits.add(
            tx,
            cluster_id="c1",
            from_state="new",
            to_state="provisioning",
            event=event,
            actor="api:alice",
            at=NOW,
        )

    async with uow() as tx:
        trail = cluster_audits.list_for_cluster(tx, "c1")
    assert len(trail) == 1
    row = trail[0]
    assert row.from_state == "new"
    assert row.to_state == "provisioning"
    assert row.event == "CreateRequested"
    assert row.actor == "api:alice"  # NOT trigger/initiated_by -- Conflict 11
    assert row.reason is None  # CreateRequested declares no `reason` field
    assert row.context == encode(event)  # the full tagged event, verbatim
    assert row.created_at == NOW


async def test_cluster_state_audit_reason_derives_from_event_field(uow):
    """DR-0007: when the event class DOES declare a ``reason`` field
    (``HealthCheckFailed``, one of the six enumerated classes), ``add()`` derives
    ``reason`` from it mechanically -- the Dispatcher never invents or overrides it."""
    event = HealthCheckFailed(at=NOW, actor="health", reason="oom-killed")
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="active"))
        cluster_audits.add(
            tx,
            cluster_id="c1",
            from_state="active",
            to_state="active",
            event=event,
            actor="health",
            at=NOW,
        )

    async with uow() as tx:
        trail = cluster_audits.list_for_cluster(tx, "c1")
    assert trail[0].reason == "oom-killed"
    assert trail[0].context == encode(event)


async def test_cluster_state_audit_created_at_is_event_at_not_wall_clock(uow):
    """Conflict 11 / 0001_initial.sql lines 106-112: ``created_at = event.at``, NOT
    whenever the write physically lands -- a late-firing timer or a post-crash
    outbox replay must still stamp the audit row with the event's own time."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="new"))
        cluster_audits.add(
            tx,
            cluster_id="c1",
            from_state="new",
            to_state="provisioning",
            event=CreateRequested(at=NOW, actor="api:alice"),
            actor="api:alice",
            at=NOW,  # the event's own timestamp -- deliberately earlier than "now"
        )

    async with uow() as tx:
        trail = cluster_audits.list_for_cluster(tx, "c1")
    assert trail[0].created_at == NOW
    assert trail[0].created_at != LATER


async def test_cluster_state_audit_list_orders_newest_first_and_respects_limit(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", status="new"))
        for i in range(3):
            cluster_audits.add(
                tx, cluster_id="c1", from_state=f"s{i}", to_state=f"s{i + 1}",
                event=CreateRequested(at=NOW, actor="reconciler"), actor="reconciler",
                at=NOW + timedelta(seconds=i),
            )

    async with uow() as tx:
        trail = cluster_audits.list_for_cluster(tx, "c1", limit=2)
    assert len(trail) == 2
    assert trail[0].from_state == "s2"  # newest first


async def test_deployment_state_audit_add_and_list(uow):
    """DR-0007 twin: ``DeployRequested`` declares no ``reason`` -- NULL, full event
    in ``context``."""
    event = DeployRequested(at=NOW, actor="api:bob", spec_ref="audit-1")
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1", status="new"))
        deployment_audits_state.add(
            tx,
            deployment_id="d1",
            cluster_id="c1",
            from_state="new",
            to_state="pending",
            event=event,
            actor="api:bob",
            at=NOW,
        )

    async with uow() as tx:
        trail = deployment_audits_state.list_for_deployment(tx, "d1")
    assert len(trail) == 1
    assert trail[0].actor == "api:bob"
    assert trail[0].cluster_id == "c1"
    assert trail[0].created_at == NOW
    assert trail[0].reason is None
    assert trail[0].context == encode(event)


async def test_deployment_state_audit_reason_derives_from_event_field(uow):
    """DR-0007: ``DeployRejected`` declares ``reason`` -- derived mechanically."""
    event = DeployRejected(at=NOW, actor="api:bob", reason="quota exceeded")
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1", status="new"))
        deployment_audits_state.add(
            tx,
            deployment_id="d1",
            cluster_id="c1",
            from_state="new",
            to_state="new",
            event=event,
            actor="api:bob",
            at=NOW,
        )

    async with uow() as tx:
        trail = deployment_audits_state.list_for_deployment(tx, "d1")
    assert trail[0].reason == "quota exceeded"
    assert trail[0].context == encode(event)


async def test_deployment_state_audit_created_at_is_event_at_not_wall_clock(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1", status="new"))
        deployment_audits_state.add(
            tx,
            deployment_id="d1",
            cluster_id="c1",
            from_state="new",
            to_state="pending",
            event=DeployRequested(at=NOW, actor="timer:destroy", spec_ref="audit-1"),
            actor="timer:destroy",
            at=NOW,
        )

    async with uow() as tx:
        trail = deployment_audits_state.list_for_deployment(tx, "d1")
    assert trail[0].created_at == NOW
    assert trail[0].created_at != LATER


# ---------------------------------------------------------------------------
# DeploymentAuditRepository (encrypted)
# ---------------------------------------------------------------------------


@pytest.fixture
def crypto():
    return CryptoService(dev_key=Fernet.generate_key(), prod_key=Fernet.generate_key())


def make_deployment_audit_row(audit_id: str, deployment_id: str | None, cluster_id: str, **overrides) -> DeploymentAuditRow:
    fields = {
        "id": audit_id,
        "deployment_id": deployment_id,
        "cluster_id": cluster_id,
        "environment": "ephemeral",
        "triggering_repo": "org/repo",
        "triggering_branch": "main",
        "triggering_image": "ghcr.io/org/web:sha",
        "commit_sha": "abc123",
        "deployment_profile_name": "default",
        "resolution_strategy": "latest",
        "registry_queries": [{"repo": "org/web", "tag": "sha"}],
        "resolved_images": {"web": "ghcr.io/org/web:sha"},
        "resolved_config": {"replicas": 2},
        "resolved_manifests": "apiVersion: v1\nkind: Deployment\n",
        "resolved_secrets": {"DB_PASSWORD": "hunter2"},
        "key_class": "DEV",
        "template_files_used": ["deployment.yaml.j2"],
        "created_at": NOW,
    }
    fields.update(overrides)
    return DeploymentAuditRow(**fields)


async def test_deployment_audit_insert_and_get_round_trips_decrypted(uow, crypto):
    repo = DeploymentAuditRepository(crypto)
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1"))
        repo.insert(tx, make_deployment_audit_row("a1", "d1", "c1"))

    async with uow() as tx:
        fetched = repo.get(tx, "a1")
    assert fetched == make_deployment_audit_row("a1", "d1", "c1")


async def test_deployment_audit_ciphertext_is_not_plaintext_on_disk(uow, crypto):
    repo = DeploymentAuditRepository(crypto)
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1"))
        repo.insert(tx, make_deployment_audit_row("a1", "d1", "c1"))

    async with uow() as tx:
        raw = tx.execute(
            text("SELECT encrypted_resolved_manifests, encrypted_resolved_secrets FROM deployment_audits WHERE id = 'a1'")
        ).mappings().first()
    assert "hunter2" not in raw["encrypted_resolved_secrets"]
    assert "apiVersion" not in raw["encrypted_resolved_manifests"]


async def test_deployment_audit_get_by_deployment_id(uow, crypto):
    repo = DeploymentAuditRepository(crypto)
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1"))
        repo.insert(tx, make_deployment_audit_row("a1", "d1", "c1"))

    async with uow() as tx:
        fetched = repo.get_by_deployment_id(tx, "d1")
    assert fetched is not None
    assert fetched.id == "a1"


async def test_deployment_audit_list_for_cluster(uow, crypto):
    repo = DeploymentAuditRepository(crypto)
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        deployments.insert(tx, make_deployment_row("d1", "c1"))
        repo.insert(tx, make_deployment_audit_row("a1", "d1", "c1", created_at=NOW))
        repo.insert(tx, make_deployment_audit_row("a2", None, "c1", created_at=LATER))

    async with uow() as tx:
        found = [a.id for a in repo.list_for_cluster(tx, "c1")]
    assert found == ["a2", "a1"]  # newest first


async def test_deployment_audit_prod_key_class_round_trips(uow, crypto):
    repo = DeploymentAuditRepository(crypto)
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo", environment="production"))
        deployments.insert(tx, make_deployment_row("d1", "c1", environment="production"))
        repo.insert(
            tx,
            make_deployment_audit_row(
                "a1", "d1", "c1", environment="production", key_class="PROD"
            ),
        )

    async with uow() as tx:
        fetched = repo.get(tx, "a1")
    assert fetched.key_class == "PROD"
    assert fetched.resolved_secrets == {"DB_PASSWORD": "hunter2"}
