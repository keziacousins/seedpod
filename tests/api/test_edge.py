"""``seedpod/api/`` cross-cutting edge -- Bearer auth (``seedpod/api/auth.py``),
``require_permission`` scope enforcement (``seedpod/api/permissions.py``), and the
DR-0003 read-only surfaces (``GET /api/workflows``, ``GET /api/timers``, ``GET
/api/permissions``).

Real ``build_app()`` over ``httpx.ASGITransport`` (``tests/conftest.py``'s
``app``/``client``/``auth_headers`` fixtures) -- no Mock/patch anywhere
(CLAUDE.md)."""

from __future__ import annotations

from datetime import timedelta

from seedpod.core.effects import ScheduleTimer
from seedpod.core.events import CreateRequested, TtlExpired
from seedpod.core.records import Origin
from seedpod.data.repositories import ClusterRow, WorkflowRunRow


def _birth_cluster_row(cluster_id: str, *, slug: str, now) -> ClusterRow:
    return ClusterRow(
        id=cluster_id, name=cluster_id, slug=slug, origin=Origin.MANAGED, environment="ephemeral",
        repository="exampleco-core", branch="feature/x", status="new", pre_destroy_state=None, version=0,
        provider="fake", provider_config={}, provider_resources={}, dns_hostname=None, dns_zone=None, dns_record_id=None,
        public_ip=None, node_count=1, encrypted_kubeconfig=None, kubeconfig_key_class=None,
        kubeconfig_ref=None, cost_per_hour=0.0, total_cost=0.0, consecutive_health_failures=0,
        failure_reason=None, last_reconciled_at=None, created_at=now, updated_at=now, expires_at=None,
    )


async def _birth_cluster(app, cluster_id: str, *, slug: str) -> None:
    now = app.clock.now()
    row = _birth_cluster_row(cluster_id, slug=slug, now=now)
    await app.dispatcher.apply("cluster", cluster_id, CreateRequested(at=now, actor="api:test"), record=row)


# ---------------------------------------------------------------------------
# Bearer auth
# ---------------------------------------------------------------------------


async def test_missing_bearer_token_is_401(client):
    response = await client.get("/api/workflows")
    assert response.status_code == 401


async def test_bad_bearer_token_is_401(client):
    response = await client.get("/api/workflows", headers={"Authorization": "Bearer not-a-real-key"})
    assert response.status_code == 401


async def test_valid_full_permission_key_is_200(client, auth_headers):
    response = await client.get("/api/workflows", headers=auth_headers)
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# require_permission scope enforcement
# ---------------------------------------------------------------------------


async def test_scoped_key_without_required_permission_is_403(app, client):
    _, plaintext = await app.services.api_keys.create_api_key(
        username="scoped-user", environment="all", permissions=["clusters:read"]
    )
    response = await client.get("/api/workflows", headers={"Authorization": f"Bearer {plaintext}"})
    assert response.status_code == 403


async def test_category_wildcard_permission_admits_the_scope(app, client):
    _, plaintext = await app.services.api_keys.create_api_key(
        username="scoped-user", environment="all", permissions=["workflows:*"]
    )
    response = await client.get("/api/workflows", headers={"Authorization": f"Bearer {plaintext}"})
    assert response.status_code == 200


async def test_super_wildcard_admits_everything(app, client):
    _, plaintext = await app.services.api_keys.create_api_key(
        username="scoped-user", environment="all", permissions=["*"]
    )
    response = await client.get("/api/permissions", headers={"Authorization": f"Bearer {plaintext}"})
    assert response.status_code == 200


async def test_non_admin_key_cannot_read_the_permissions_registry(app, client):
    """``GET /api/permissions`` is gated behind the literal ``"*"`` scope --
    ``workflows:*`` (a category wildcard, not the super-wildcard) must not admit
    it (``seedpod/api/routers/permissions.py``'s module docstring)."""
    _, plaintext = await app.services.api_keys.create_api_key(
        username="scoped-user", environment="all", permissions=["workflows:*"]
    )
    response = await client.get("/api/permissions", headers={"Authorization": f"Bearer {plaintext}"})
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/permissions
# ---------------------------------------------------------------------------


async def test_permissions_endpoint_shape(client, auth_headers):
    response = await client.get("/api/permissions", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"permissions", "categories"}
    assert "*" in body["permissions"]
    assert "workflows:read" in body["permissions"]
    assert "Workflows" in body["categories"]
    assert "workflows:read" in body["categories"]["Workflows"]


# ---------------------------------------------------------------------------
# GET /api/workflows
# ---------------------------------------------------------------------------


async def test_workflows_endpoint_empty_shape(client, auth_headers):
    response = await client.get("/api/workflows", headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == {"workflows": []}


async def test_workflows_endpoint_serializes_a_real_row(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug")
    now = app.clock.now()
    row = WorkflowRunRow(
        id="run-1", workflow="provision-kind", workflow_version=1, cluster_id="c1", deployment_id=None,
        dedupe_key="dedupe-1", args={"a": 1}, status="running", cancel_requested=False,
        failed_step=None, error=None, undo_incomplete=None, initiated_by="api:test",
        created_at=now, started_at=now, finished_at=None,
    )
    async with app.uow() as t:
        app.repos.workflow_runs.insert(t, row)

    response = await client.get("/api/workflows", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()["workflows"]
    assert len(body) == 1
    entry = body[0]
    assert entry == {
        "id": "run-1",
        "workflow": "provision-kind",
        "cluster_id": "c1",
        "deployment_id": None,
        "status": "running",
        "failed_step": None,
        "error": None,
        "undo_incomplete": None,
        "created_at": now.isoformat(),
        "started_at": now.isoformat(),
        "finished_at": None,
    }


# ---------------------------------------------------------------------------
# GET /api/timers
# ---------------------------------------------------------------------------


async def test_timers_endpoint_empty_shape(client, auth_headers):
    response = await client.get("/api/timers", headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == {"timers": []}


async def test_timers_endpoint_serializes_a_real_row_ordered_by_fire_at(app, client, auth_headers):
    await _birth_cluster(app, "c1", slug="c1-slug")
    now = app.clock.now()
    later = now + timedelta(hours=2)
    sooner = now + timedelta(hours=1)
    async with app.uow() as t:
        app.repos.timers.upsert(
            t,
            ScheduleTimer(
                aggregate_type="cluster", aggregate_id="c1", timer_key="destroy",
                fire_at=later, event=TtlExpired(at=now, actor="timer:ttl"),
            ),
            created_by_effect="effect-later",
        )
        app.repos.timers.upsert(
            t,
            ScheduleTimer(
                aggregate_type="cluster", aggregate_id="c1", timer_key="ttl",
                fire_at=sooner, event=TtlExpired(at=now, actor="timer:ttl"),
            ),
            created_by_effect="effect-sooner",
        )

    response = await client.get("/api/timers", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()["timers"]
    assert len(body) == 2
    # ordered by fire_at -- the sooner ("ttl") timer comes first.
    assert body[0] == {
        "aggregate_type": "cluster",
        "aggregate_id": "c1",
        "timer_key": "ttl",
        "fire_at": sooner.isoformat(),
    }
    assert body[1]["timer_key"] == "destroy"
