"""``seedpod/app/services/cluster_service.py`` -- real sqlite, ``FrozenClock``, no
Mock/patch. Covers the read/query surface and the three Dispatcher-mediated
state changes (extend is the one DR-0009 dedicated-write-path exception -- see
its own docstring). Provider-plane reads (pods/logs/events) need a real kubectl
binary and are exercised at the conformance/api layer, not here.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from seedpod.app.services.cluster_service import ClusterNotFound, ClusterService
from seedpod.core.errors import PermanentError
from seedpod.core.events import CreateRequested, Discovered, DiscoveredInfo, ProvisionSucceeded
from seedpod.core.records import ClusterRecord, ClusterState, Origin
from seedpod.data.repositories import ClusterRow


@pytest.fixture
def cluster_service(dispatcher, repos, uow, id_gen, clock, crypto):
    return ClusterService(dispatcher, repos, uow, id_gen, clock, crypto=crypto, kubectl_provider=None)


def _birth_row(cluster_id: str, *, slug: str, environment: str, now, expires_at=None) -> ClusterRow:
    return ClusterRow(
        id=cluster_id, name=cluster_id, slug=slug, origin=Origin.MANAGED, environment=environment,
        repository="exampleco-core", branch="feature/x", status="new", pre_destroy_state=None, version=0,
        provider="fake", provider_config={}, provider_resources={}, dns_hostname=None, dns_zone=None, dns_record_id=None,
        public_ip=None, node_count=1, encrypted_kubeconfig=None, kubeconfig_key_class=None,
        kubeconfig_ref=None, cost_per_hour=0.0, total_cost=0.0, consecutive_health_failures=0,
        failure_reason=None, last_reconciled_at=None, created_at=now, updated_at=now, expires_at=expires_at,
    )


async def _birth_cluster(dispatcher, clock, cluster_id, *, slug, environment="ephemeral", ttl_hours=None):
    now = clock.now()
    expires_at = now + timedelta(hours=ttl_hours) if ttl_hours else None
    row = _birth_row(cluster_id, slug=slug, environment=environment, now=now, expires_at=expires_at)
    await dispatcher.apply("cluster", cluster_id, CreateRequested(at=now, actor="api:test"), record=row)


async def test_get_returns_the_birthed_row(cluster_service, dispatcher, clock):
    await _birth_cluster(dispatcher, clock, "c1", slug="c1-slug")
    row = await cluster_service.get("c1")
    assert row.id == "c1"
    assert row.status == ClusterState.PROVISIONING.value


async def test_get_by_slug_also_works(cluster_service, dispatcher, clock):
    await _birth_cluster(dispatcher, clock, "c1", slug="c1-slug")
    row = await cluster_service.get("c1-slug")
    assert row.id == "c1"


async def test_get_missing_cluster_raises_not_found(cluster_service):
    with pytest.raises(ClusterNotFound):
        await cluster_service.get("does-not-exist")


async def test_list_hides_terminal_states_by_default(cluster_service, dispatcher, repos, uow, clock):
    await _birth_cluster(dispatcher, clock, "c1", slug="c1-slug")
    await _birth_cluster(dispatcher, clock, "c2", slug="c2-slug")
    async with uow() as tx:
        repos.clusters.persist(
            tx,
            ClusterRecord(
                id="c2", name="c2", state=ClusterState.DESTROYED, version=1, provider="fake",
                environment="ephemeral", origin=Origin.MANAGED,
            ),
            expected_version=1,
            clock=clock,
        )

    visible = await cluster_service.list()
    assert {c.id for c in visible} == {"c1"}

    all_rows = await cluster_service.list(show_destroyed=True)
    assert {c.id for c in all_rows} == {"c1", "c2"}


async def test_extend_bumps_expires_at_and_rearms_timer(cluster_service, dispatcher, clock, repos, uow):
    await _birth_cluster(dispatcher, clock, "c1", slug="c1-slug", ttl_hours=2)
    before = (await cluster_service.get("c1")).expires_at

    row = await cluster_service.extend("c1", ttl_hours=3, actor="api:test")

    assert row.expires_at == before + timedelta(hours=3)
    async with uow() as tx:
        timer = repos.timers.get(tx, "cluster", "c1", "ttl")
    assert timer is not None
    assert timer.fire_at == row.expires_at


async def test_extend_without_ttl_raises(cluster_service, dispatcher, clock):
    await _birth_cluster(dispatcher, clock, "c1", slug="c1-slug", ttl_hours=None)
    with pytest.raises(PermanentError):
        await cluster_service.extend("c1", ttl_hours=1, actor="api:test")


async def test_extend_of_already_lapsed_cluster_computes_from_now(cluster_service, dispatcher, clock):
    """v1 (reference-code/seedpod/seedpod/api/clusters.py:353-356, :391):
    `base = max(cluster.expires_at, utc_now())` -- a cluster whose TTL already
    lapsed (but is still ACTIVE, pending the reconciler/timer destroying it) must
    get at least ttl_hours MORE runtime from NOW, not from its already-past
    expiry, or the re-armed timer fires immediately and the just-extended cluster
    is destroyed anyway."""
    await _birth_cluster(dispatcher, clock, "c1", slug="c1-slug", ttl_hours=1)
    clock.advance(timedelta(hours=5))  # expires_at is now well in the past

    row = await cluster_service.extend("c1", ttl_hours=3, actor="api:test")

    assert row.expires_at == clock.now() + timedelta(hours=3)
    assert row.expires_at > clock.now()


async def test_extend_rejects_production_cluster(cluster_service, dispatcher, clock):
    now = clock.now()
    row = _birth_row("c1", slug="c1-slug", environment="production", now=now, expires_at=now + timedelta(hours=2))
    await dispatcher.apply("cluster", "c1", CreateRequested(at=now, actor="api:test"), record=row)
    with pytest.raises(PermanentError):
        await cluster_service.extend("c1", ttl_hours=1, actor="api:test")


async def test_extend_rejects_non_active_non_provisioning_status(cluster_service, dispatcher, clock):
    await _birth_cluster(dispatcher, clock, "c1", slug="c1-slug", ttl_hours=2)
    await dispatcher.apply(
        "cluster", "c1",
        ProvisionSucceeded(at=clock.now(), actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref"),
    )
    row = await cluster_service.destroy("c1", actor="api:test")
    assert row.status == ClusterState.DESTROY_SCHEDULED.value
    with pytest.raises(PermanentError):
        await cluster_service.extend("c1", ttl_hours=1, actor="api:test")


async def test_destroy_transitions_to_destroy_scheduled(cluster_service, dispatcher, clock):
    await _birth_cluster(dispatcher, clock, "c1", slug="c1-slug")
    await dispatcher.apply(
        "cluster", "c1",
        ProvisionSucceeded(at=clock.now(), actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref"),
    )
    row = await cluster_service.destroy("c1", actor="api:test")
    assert row.status == ClusterState.DESTROY_SCHEDULED.value


async def test_rehabilitate_discovered_cluster(cluster_service, dispatcher, clock):
    now = clock.now()
    row = _birth_row("c1", slug="c1-slug", environment="ephemeral", now=now)  # status="new" (birth)

    await dispatcher.apply(
        "cluster", "c1", Discovered(at=now, actor="reconciler", observed=DiscoveredInfo(provider="fake")),
        record=row,
    )
    result = await cluster_service.rehabilitate("c1", actor="api:test")
    assert result.status == ClusterState.ACTIVE.value
