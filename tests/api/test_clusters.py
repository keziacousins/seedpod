"""``seedpod/api/routers/clusters.py`` -- router-level tests. Real ``build_app()``
over ``httpx.ASGITransport`` (``tests/conftest.py``'s ``app``/``client``/
``auth_headers`` fixtures), no Mock/patch anywhere (CLAUDE.md).

Provider-plane fixtures (pods/pod-details/logs/events) swap ``ClusterService``'s
real ``kubectl_provider`` for one built from ``tests/conformance/fake_kubectl.py``'s
``FakeKubectlTransport`` -- a real, fully-typed ``SubprocessRunner`` implementation
(the exact seam ``KubectlProvider`` is constructed against in production, per that
module's own docstring), never ``Mock``/``patch``. This mirrors
``tests/app/test_services_cluster.py``'s own module docstring: "Provider-plane
reads (pods/logs/events) need a real kubectl binary and are exercised at the
conformance/api layer, not here" -- THIS is that api layer. Swapping the private
``ClusterService._kubectl`` attribute post-construction is plain object
substitution (a real, typed provider instance), not a mock framework; there is no
dedicated ``build_app`` test seam for the kubectl transport (only providers/clock/
id_gen/http_transport, per ``tests/conftest.py``'s own docstring), so this is the
only way to exercise these reads against a controlled backend instead of a real
cluster.
"""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta

from sqlalchemy import text

from seedpod.core.events import CreateRequested, Discovered, DiscoveredInfo, ProvisionSucceeded
from seedpod.core.records import ClusterRecord, ClusterState, Origin
from seedpod.data.repositories import ClusterRow
from seedpod.providers.kubectl import KubectlConfig, KubectlProvider
from tests.conformance.fake_kubectl import FakeKubectlBackend, FakeKubectlTransport
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG

# tests/conformance/fake_kubectl.py's _default_pods() key -- the one pod every
# fresh FakeKubectlBackend is seeded with.
_SEEDED_POD = "web-abc123"
_NAMESPACE = "default"


# ---------------------------------------------------------------------------
# Birth helpers -- direct dispatcher.apply(), mirroring tests/app/
# test_services_cluster.py's own `_birth_row`/`_birth_cluster` pattern, so tests
# here can control cluster state/origin precisely without going through the full
# version-update flow every time.
# ---------------------------------------------------------------------------


def _birth_row(cluster_id: str, *, slug: str, environment: str, now, expires_at=None, dns_hostname=None) -> ClusterRow:
    return ClusterRow(
        id=cluster_id, name=cluster_id, slug=slug, origin=Origin.MANAGED, environment=environment,
        repository="exampleco-core", branch="feature/x", status="new", pre_destroy_state=None, version=0,
        provider="fake", provider_config={"deployment_profile": "infrastructure-only"}, provider_resources={},
        dns_hostname=dns_hostname, dns_zone=None, dns_record_id=None, public_ip=None, node_count=1, encrypted_kubeconfig=None,
        kubeconfig_key_class=None, kubeconfig_ref=None, cost_per_hour=0.0, total_cost=0.0,
        consecutive_health_failures=0, failure_reason=None, last_reconciled_at=None,
        created_at=now, updated_at=now, expires_at=expires_at,
    )


async def _birth_cluster(
    app, cluster_id: str, *, slug: str, environment: str = "ephemeral", ttl_hours: float | None = None,
    dns_hostname: str | None = None, actor: str = "api:test-user",
) -> None:
    now = app.clock.now()
    expires_at = now + timedelta(hours=ttl_hours) if ttl_hours else None
    row = _birth_row(cluster_id, slug=slug, environment=environment, now=now, expires_at=expires_at, dns_hostname=dns_hostname)
    await app.dispatcher.apply("cluster", cluster_id, CreateRequested(at=now, actor=actor), record=row)


async def _birth_discovered_cluster(app, cluster_id: str, *, slug: str, environment: str = "ephemeral") -> None:
    now = app.clock.now()
    row = _birth_row(cluster_id, slug=slug, environment=environment, now=now)
    await app.dispatcher.apply(
        "cluster", cluster_id,
        Discovered(at=now, actor="reconciler", observed=DiscoveredInfo(provider="fake")),
        record=row,
    )


async def _promote_to_active(app, cluster_id: str) -> None:
    await app.dispatcher.apply(
        "cluster", cluster_id,
        ProvisionSucceeded(at=app.clock.now(), actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref"),
    )


async def _set_kubeconfig(app, cluster_id: str, *, environment: str = "ephemeral") -> None:
    """Stamp a decryptable ``encrypted_kubeconfig``/``kubeconfig_key_class`` --
    there is no Dispatcher-mediated write path for this yet (module docstring of
    ``seedpod/app/services/cluster_service.py`` names ``kubeconfig_ref`` as the
    only kubeconfig-adjacent field the machine persists), so this is a direct,
    real-crypto SQL write, matching v1's own ``Cluster.set_kubeconfig`` shape."""
    key_class = app.crypto.key_class_for_environment(environment)
    encrypted = app.crypto.encrypt(FAKE_KUBECONFIG, key_class)
    async with app.uow() as tx:
        tx.execute(
            text("UPDATE clusters SET encrypted_kubeconfig = :enc, kubeconfig_key_class = :kc WHERE id = :id"),
            {"enc": encrypted, "kc": key_class, "id": cluster_id},
        )


def _install_fake_kubectl(app, *, backend: FakeKubectlBackend | None = None, transport=None) -> FakeKubectlBackend:
    backend = backend or FakeKubectlBackend()
    app.services.clusters._kubectl = KubectlProvider(
        KubectlConfig(), transport or FakeKubectlTransport(backend, frozenset())
    )
    return backend


# ---------------------------------------------------------------------------
# GET /api/clusters -- envelope + filters
# ---------------------------------------------------------------------------


async def test_list_envelope_and_default_filter_hides_terminal(app, client, auth_headers):
    await _birth_cluster(app, "c-active", slug="c-active-slug")
    await _promote_to_active(app, "c-active")

    await _birth_cluster(app, "c-destroyed", slug="c-destroyed-slug")
    async with app.uow() as tx:
        app.repos.clusters.persist(
            tx,
            ClusterRecord(
                id="c-destroyed", name="c-destroyed", state=ClusterState.DESTROYED, version=1,
                provider="fake", environment="ephemeral", origin=Origin.MANAGED,
            ),
            expected_version=1, clock=app.clock,
        )

    response = await client.get("/api/clusters", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert "clusters" in body
    ids = {c["id"] for c in body["clusters"]}
    assert ids == {"c-active"}


async def test_list_show_destroyed_and_status_filters(app, client, auth_headers):
    await _birth_cluster(app, "c-active", slug="c-active-slug")
    await _promote_to_active(app, "c-active")
    await _birth_cluster(app, "c-provisioning", slug="c-provisioning-slug")

    await _birth_cluster(app, "c-destroyed", slug="c-destroyed-slug")
    async with app.uow() as tx:
        app.repos.clusters.persist(
            tx,
            ClusterRecord(
                id="c-destroyed", name="c-destroyed", state=ClusterState.DESTROYED, version=1,
                provider="fake", environment="ephemeral", origin=Origin.MANAGED,
            ),
            expected_version=1, clock=app.clock,
        )

    response = await client.get("/api/clusters?show_destroyed=true", headers=auth_headers)
    ids = {c["id"] for c in response.json()["clusters"]}
    assert ids == {"c-active", "c-provisioning", "c-destroyed"}

    response = await client.get("/api/clusters?status=active", headers=auth_headers)
    ids = {c["id"] for c in response.json()["clusters"]}
    assert ids == {"c-active"}


async def test_list_default_hide_set_matches_dr_0019_exactly(app, client, auth_headers):
    """DR-0019: default hide-set is EXACTLY {destroyed, zombie, unmanaged} -- not
    core.records.TERMINAL_STATES ({destroyed, failed}). FAILED/DESTROY_FAILED must
    stay visible by default (operators need failures visible for attention/
    rehabilitation); ZOMBIE/UNMANAGED must stay hidden by default."""

    async def _birth_with_state(cluster_id: str, state: ClusterState) -> None:
        await _birth_cluster(app, cluster_id, slug=f"{cluster_id}-slug")
        async with app.uow() as tx:
            app.repos.clusters.persist(
                tx,
                ClusterRecord(
                    id=cluster_id, name=cluster_id, state=state, version=1,
                    provider="fake", environment="ephemeral", origin=Origin.MANAGED,
                ),
                expected_version=1, clock=app.clock,
            )

    await _birth_cluster(app, "c-active", slug="c-active-slug")
    await _promote_to_active(app, "c-active")
    await _birth_with_state("c-failed", ClusterState.FAILED)
    await _birth_with_state("c-destroy-failed", ClusterState.DESTROY_FAILED)
    await _birth_with_state("c-destroy-scheduled", ClusterState.DESTROY_SCHEDULED)
    await _birth_with_state("c-destroying", ClusterState.DESTROYING)
    await _birth_with_state("c-destroyed", ClusterState.DESTROYED)
    await _birth_with_state("c-zombie", ClusterState.ZOMBIE)
    await _birth_with_state("c-unmanaged", ClusterState.UNMANAGED)

    response = await client.get("/api/clusters", headers=auth_headers)
    ids = {c["id"] for c in response.json()["clusters"]}
    assert ids == {
        "c-active", "c-failed", "c-destroy-failed", "c-destroy-scheduled", "c-destroying",
    }

    response = await client.get("/api/clusters?show_destroyed=true", headers=auth_headers)
    ids = {c["id"] for c in response.json()["clusters"]}
    assert ids == {
        "c-active", "c-failed", "c-destroy-failed", "c-destroy-scheduled", "c-destroying",
        "c-destroyed", "c-zombie", "c-unmanaged",
    }


# ---------------------------------------------------------------------------
# GET /api/clusters/{id} -- origin + derived fields
# ---------------------------------------------------------------------------


async def test_get_cluster_detail_shape_and_id_or_slug_lookup(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug", dns_hostname="c1.example.com")

    response = await client.get("/api/clusters/c1", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "c1"
    assert body["origin"] == "managed"
    assert body["environment"] == "ephemeral"
    assert body["cluster_url"] == "https://c1.example.com"
    assert body["reconciliation_stale"] is False
    assert body["last_reconciled_at"] is None
    assert body["provider_config"]["deployment_profile"] == "infrastructure-only"
    assert "status" in body and body["status"] not in ("creating", "deploying")

    by_slug = await client.get("/api/clusters/c1-slug", headers=auth_headers)
    assert by_slug.status_code == 200
    assert by_slug.json()["id"] == "c1"


async def test_reconciliation_stale_flips_after_threshold(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug")
    async with app.uow() as tx:
        app.repos.clusters.set_last_reconciled_at(tx, ["c1"], clock=app.clock)

    fresh = await client.get("/api/clusters/c1", headers=auth_headers)
    assert fresh.json()["reconciliation_stale"] is False

    app.clock.advance(timedelta(minutes=31))
    stale = await client.get("/api/clusters/c1", headers=auth_headers)
    assert stale.json()["reconciliation_stale"] is True
    assert stale.json()["last_reconciled_at"] is not None


async def test_get_unknown_cluster_is_404(client, auth_headers):
    response = await client.get("/api/clusters/does-not-exist", headers=auth_headers)
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /api/clusters/{id} -- the discovered-origin force gate
# ---------------------------------------------------------------------------


async def test_discovered_cluster_requires_force_to_destroy(app, client, auth_headers):
    await _birth_discovered_cluster(app, "c-disc", slug="c-disc-slug")

    rehab = await client.post("/api/clusters/c-disc/rehabilitate", headers=auth_headers)
    assert rehab.status_code == 200
    assert rehab.json()["status"] == "active"
    assert (await client.get("/api/clusters/c-disc", headers=auth_headers)).json()["origin"] == "discovered"

    denied = await client.delete("/api/clusters/c-disc", headers=auth_headers)
    assert denied.status_code == 409

    allowed = await client.delete("/api/clusters/c-disc?force=true", headers=auth_headers)
    assert allowed.status_code == 200
    assert allowed.json()["status"] == "destroy-scheduled"


async def test_managed_cluster_destroys_without_force(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug")
    await _promote_to_active(app, "c1")

    response = await client.delete("/api/clusters/c1", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["status"] == "destroy-scheduled"


# ---------------------------------------------------------------------------
# DELETE /api/clusters/{id} -- DR-0018's production-destroy force gate: a
# SEPARATE, additional guard from the discovered-origin gate above -- a managed
# cluster with environment == "production" also requires force=true, else 400
# (PermanentError(INVALID_INPUT) from ClusterService.destroy).
# ---------------------------------------------------------------------------


async def test_production_managed_cluster_requires_force_to_destroy(app, client, auth_headers):
    await _birth_cluster(app, "c-prod", slug="c-prod-slug", environment="production")
    await _promote_to_active(app, "c-prod")

    denied = await client.delete("/api/clusters/c-prod", headers=auth_headers)
    assert denied.status_code == 400

    # cluster is untouched -- still active, no destroy scheduled.
    still_active = await client.get("/api/clusters/c-prod", headers=auth_headers)
    assert still_active.json()["status"] == "active"

    allowed = await client.delete("/api/clusters/c-prod?force=true", headers=auth_headers)
    assert allowed.status_code == 200
    assert allowed.json()["status"] == "destroy-scheduled"


async def test_destroy_snapshot_before_destroy_is_fail_open_not_501(app, client, auth_headers):
    """DR-0020, FINAL state (api-features REPLACES api-clusters' interim 501 --
    ``tests/api/test_features.py`` covers the real snapshot-taken-then-destroy
    and fail-open-on-failure cases in detail; this pins the router-level
    contract that survived the swap): ``snapshot_before_destroy=true`` no
    longer 501s -- the real ``SnapshotService`` collaborator is wired by
    ``seedpod/app/factory.py``, and its own pre-destroy attempt is fail-open
    (this cluster has no deployment row to derive a profile from, so the
    snapshot attempt itself fails internally -- but destroy proceeds anyway,
    exactly the fail-open contract DR-0020 requires)."""
    await _birth_cluster(app, "c1", slug="c1-slug")
    await _promote_to_active(app, "c1")

    response = await client.delete("/api/clusters/c1?snapshot_before_destroy=true", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["status"] == "destroy-scheduled"


async def test_destroy_without_snapshot_before_destroy_takes_no_snapshot(app, client, auth_headers):
    await _birth_cluster(app, "c2", slug="c2-slug")
    await _promote_to_active(app, "c2")

    response = await client.delete("/api/clusters/c2", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["status"] == "destroy-scheduled"

    snapshots = await client.get("/api/snapshots", headers=auth_headers)
    assert snapshots.json()["snapshots"] == []


async def test_destroy_unknown_cluster_is_404(client, auth_headers):
    response = await client.delete("/api/clusters/does-not-exist", headers=auth_headers)
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# POST /extend, POST /rehabilitate
# ---------------------------------------------------------------------------


async def test_extend_bumps_expires_at(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug", ttl_hours=2)
    before = (await client.get("/api/clusters/c1", headers=auth_headers)).json()["expires_at"]

    response = await client.post("/api/clusters/c1/extend", headers=auth_headers, json={"ttl_hours": 3})
    assert response.status_code == 200
    after = response.json()["expires_at"]
    assert after > before


async def test_extend_unknown_cluster_is_404(client, auth_headers):
    response = await client.post("/api/clusters/does-not-exist/extend", headers=auth_headers, json={"ttl_hours": 1})
    assert response.status_code == 404


async def test_extend_without_ttl_is_400(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug", ttl_hours=None)
    response = await client.post("/api/clusters/c1/extend", headers=auth_headers, json={"ttl_hours": 1})
    assert response.status_code == 400


async def test_rehabilitate_unknown_cluster_is_404(client, auth_headers):
    response = await client.post("/api/clusters/does-not-exist/rehabilitate", headers=auth_headers)
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# GET /audit -- actor, not trigger/initiated_by (ui-contract worklist 12)
# ---------------------------------------------------------------------------


async def test_audit_rows_expose_actor(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug", actor="api:test-user")

    response = await client.get("/api/clusters/c1/audit", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert "audit" in body
    assert len(body["audit"]) == 1
    entry = body["audit"][0]
    assert entry["actor"] == "api:test-user"
    assert entry["from_state"] == "new"
    assert entry["to_state"] == "provisioning"
    assert "trigger" not in entry
    assert "initiated_by" not in entry


async def test_audit_unknown_cluster_is_404(client, auth_headers):
    response = await client.get("/api/clusters/does-not-exist/audit", headers=auth_headers)
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# GET /deployments -- cluster-scoped, DB-only
# ---------------------------------------------------------------------------


async def test_cluster_deployments_list_shape(client, auth_headers):
    create = await client.post(
        "/api/version-update", headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/cluster-deployments-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-cluster-deployments-test-cd1", "commit": "cd1",
        },
    )
    cluster_id = create.json()["cluster_id"]
    deployment_id = create.json()["deployment_id"]

    response = await client.get(f"/api/clusters/{cluster_id}/deployments", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert "deployments" in body
    entry = next(d for d in body["deployments"] if d["deployment_id"] == deployment_id)
    assert set(entry) == {
        "deployment_id", "cluster_id", "environment", "status", "manifest_version",
        "resolved_images", "superseded_by", "deployed_by", "failure_reason", "deployed_at",
    }
    assert "error_message" not in entry
    assert "services" not in entry


async def test_cluster_deployments_unknown_cluster_is_404(client, auth_headers):
    response = await client.get("/api/clusters/does-not-exist/deployments", headers=auth_headers)
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Provider-plane reads -- pods/pod details/logs/events, OUTSIDE any uow (DR-0008)
# ---------------------------------------------------------------------------


async def test_pods_without_kubeconfig_is_404(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug")  # still PROVISIONING -- no kubeconfig yet
    response = await client.get("/api/clusters/c1/pods", headers=auth_headers)
    assert response.status_code == 404


async def test_pods_pod_details_logs_events_shapes(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug")
    await _set_kubeconfig(app, "c1")
    _install_fake_kubectl(app)

    pods = await client.get("/api/clusters/c1/pods", headers=auth_headers)
    assert pods.status_code == 200
    pods_body = pods.json()
    assert "pods" in pods_body
    assert pods_body["pods"][0]["name"] == _SEEDED_POD
    assert pods_body["pods"][0]["namespace"] == _NAMESPACE

    detail = await client.get(f"/api/clusters/c1/pods/{_NAMESPACE}/{_SEEDED_POD}", headers=auth_headers)
    assert detail.status_code == 200
    pod = detail.json()["pod"]
    assert pod["name"] == _SEEDED_POD
    assert "hostIP" in pod
    assert "initContainers" in pod

    missing = await client.get(f"/api/clusters/c1/pods/{_NAMESPACE}/does-not-exist", headers=auth_headers)
    assert missing.status_code == 404

    logs = await client.get(f"/api/clusters/c1/pods/{_NAMESPACE}/{_SEEDED_POD}/logs", headers=auth_headers)
    assert logs.status_code == 200
    assert "logs" in logs.json()
    assert _SEEDED_POD in logs.json()["logs"]

    events = await client.get("/api/clusters/c1/events", headers=auth_headers)
    assert events.status_code == 200
    assert "events" in events.json()
    assert len(events.json()["events"]) == 2


async def test_pod_logs_accepts_tail_and_container_params(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug")
    await _set_kubeconfig(app, "c1")
    _install_fake_kubectl(app)

    response = await client.get(
        f"/api/clusters/c1/pods/{_NAMESPACE}/{_SEEDED_POD}/logs?tail_lines=50&container=web&previous=false",
        headers=auth_headers,
    )
    assert response.status_code == 200


async def test_provider_plane_read_holds_no_uow_lock(app, client, auth_headers):
    """DR-0008: a provider-plane read must never straddle an open ``uow()``
    transaction. Proven here by making the fake kubectl transport artificially
    slow and confirming an UNRELATED write (extending a second cluster's TTL)
    completes promptly rather than queuing behind the still-open provider call --
    if the router (or ``ClusterService``) held the DR-0008 lock across the
    kubectl call, this second request would stall for the same duration."""
    await _birth_cluster(app, "c1", slug="c1-slug")
    await _set_kubeconfig(app, "c1")
    await _birth_cluster(app, "c2", slug="c2-slug", ttl_hours=2)

    backend = FakeKubectlBackend()
    inner = FakeKubectlTransport(backend, frozenset())

    class _SlowTransport:
        async def run(self, argv, **kwargs):
            await asyncio.sleep(0.3)
            return await inner.run(argv, **kwargs)

        def stream(self, *args, **kwargs):
            return inner.stream(*args, **kwargs)

    _install_fake_kubectl(app, backend=backend, transport=_SlowTransport())

    pods_task = asyncio.ensure_future(client.get("/api/clusters/c1/pods", headers=auth_headers))
    await asyncio.sleep(0.05)  # let the pods request actually reach the slow kubectl call

    start = time.monotonic()
    extend = await client.post("/api/clusters/c2/extend", headers=auth_headers, json={"ttl_hours": 1})
    elapsed = time.monotonic() - start

    assert extend.status_code == 200
    assert elapsed < 0.2, "extend was blocked behind the in-flight provider read -- DR-0008 violation"

    pods = await pods_task
    assert pods.status_code == 200


async def test_pod_logs_unknown_cluster_is_404(client, auth_headers):
    response = await client.get(
        f"/api/clusters/does-not-exist/pods/{_NAMESPACE}/{_SEEDED_POD}/logs", headers=auth_headers
    )
    assert response.status_code == 404


async def test_events_without_kubeconfig_is_404(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug")
    response = await client.get("/api/clusters/c1/events", headers=auth_headers)
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


async def test_list_clusters_requires_auth(client):
    response = await client.get("/api/clusters")
    assert response.status_code == 401
