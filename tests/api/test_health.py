"""``GET /health`` (the acceptance parity gate's own probe) and ``GET
/health/detailed`` (docs/decisions/DR-0003's exact shape, ui-contract obligation
7) -- public, no auth required (neither route declares a
``require_permission(...)`` dependency; see ``seedpod/api/routers/health.py``'s
own docstring for why no middleware allowlist is involved).

Real ``build_app()`` over ``httpx.ASGITransport`` -- no Mock/patch anywhere
(CLAUDE.md)."""

from __future__ import annotations

from seedpod.core.events import CreateRequested
from seedpod.core.records import Origin
from seedpod.data.repositories import ClusterRow, WorkflowRunRow


async def test_health_basic_shape(client):
    response = await client.get("/api/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["service"] == "seedpod"
    assert isinstance(body["version"], str)
    assert isinstance(body["timestamp"], str)


async def test_health_basic_requires_no_auth(client):
    """The parity gate hits ``/health`` bare -- no ``Authorization`` header."""
    response = await client.get("/api/health")
    assert response.status_code == 200


async def test_health_detailed_requires_no_auth(client):
    response = await client.get("/api/health/detailed")
    assert response.status_code == 200


async def test_health_detailed_has_the_dr0003_blocks(client):
    response = await client.get("/api/health/detailed")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert set(body["database"]) == {"connected", "cluster_count", "deployment_count", "api_key_count"}
    assert body["database"]["connected"] is True
    assert set(body["executor"]) == {"running", "pending_outbox", "dead_outbox"}
    assert set(body["timers"]) == {"running", "next_fire_at"}
    assert set(body["engine"]) == {"active_runs"}
    assert set(body["reconciler"]) == {"running", "last_sync"}
    # No v1-shaped `scheduler` block regression (DR-0003).
    assert "scheduler" not in body


async def test_health_detailed_executor_and_timers_are_live_and_running(client):
    """``App.start()`` always starts the executor and the timer poller,
    regardless of ``background_tasks`` (coherence-review Conflict 15) -- both
    fixtures the acceptance/edge suites use leave ``background_tasks=False``."""
    response = await client.get("/api/health/detailed")
    body = response.json()
    assert body["executor"]["running"] is True
    assert body["timers"]["running"] is True
    assert body["executor"]["pending_outbox"] == 0
    assert body["executor"]["dead_outbox"] == 0


async def test_health_detailed_reconciler_reflects_background_tasks_off(client):
    """``tests/conftest.py``'s ``make_app`` fixture sets ``background_tasks=False``
    -- the reconciler must report itself as not running, matching
    ``app.services.reconciliation.running``."""
    response = await client.get("/api/health/detailed")
    body = response.json()
    assert body["reconciler"]["running"] is False
    assert body["reconciler"]["last_sync"] is None


async def test_health_detailed_database_counts_reflect_live_rows(app, client, auth_headers):
    """``auth_headers`` already minted one API key through the service layer --
    the count must be live, not a stub zero."""
    now = app.clock.now()
    cluster = ClusterRow(
        id="c1", name="c1", slug="c1-slug", origin=Origin.MANAGED, environment="ephemeral",
        repository="exampleco-core", branch="feature/x", status="new", pre_destroy_state=None, version=0,
        provider="fake", provider_config={}, provider_resources={}, dns_hostname=None, dns_zone=None, dns_record_id=None,
        public_ip=None, node_count=1, encrypted_kubeconfig=None, kubeconfig_key_class=None,
        kubeconfig_ref=None, cost_per_hour=0.0, total_cost=0.0, consecutive_health_failures=0,
        failure_reason=None, last_reconciled_at=None, created_at=now, updated_at=now, expires_at=None,
    )
    await app.dispatcher.apply("cluster", "c1", CreateRequested(at=now, actor="api:test"), record=cluster)

    response = await client.get("/api/health/detailed", headers=auth_headers)
    body = response.json()
    assert body["database"]["cluster_count"] == 1
    assert body["database"]["deployment_count"] == 0
    assert body["database"]["api_key_count"] >= 1


async def test_health_detailed_engine_active_runs_counts_non_terminal_workflow_runs(app, client):
    now = app.clock.now()
    cluster = ClusterRow(
        id="c1", name="c1", slug="c1-slug", origin=Origin.MANAGED, environment="ephemeral",
        repository="exampleco-core", branch="feature/x", status="new", pre_destroy_state=None, version=0,
        provider="fake", provider_config={}, provider_resources={}, dns_hostname=None, dns_zone=None, dns_record_id=None,
        public_ip=None, node_count=1, encrypted_kubeconfig=None, kubeconfig_key_class=None,
        kubeconfig_ref=None, cost_per_hour=0.0, total_cost=0.0, consecutive_health_failures=0,
        failure_reason=None, last_reconciled_at=None, created_at=now, updated_at=now, expires_at=None,
    )
    await app.dispatcher.apply("cluster", "c1", CreateRequested(at=now, actor="api:test"), record=cluster)

    running_run = WorkflowRunRow(
        id="run-running", workflow="provision-kind", workflow_version=1, cluster_id="c1", deployment_id=None,
        dedupe_key="dedupe-running", args={}, status="running", cancel_requested=False,
        failed_step=None, error=None, undo_incomplete=None, initiated_by="api:test",
        created_at=now, started_at=now, finished_at=None,
    )
    finished_run = WorkflowRunRow(
        id="run-done", workflow="provision-kind", workflow_version=1, cluster_id="c1", deployment_id=None,
        dedupe_key="dedupe-done", args={}, status="succeeded", cancel_requested=False,
        failed_step=None, error=None, undo_incomplete=None, initiated_by="api:test",
        created_at=now, started_at=now, finished_at=now,
    )
    async with app.uow() as t:
        app.repos.workflow_runs.insert(t, finished_run)
    # `ux_wr_one_active` only allows one non-terminal run per cluster -- the
    # running one must be inserted on its own to avoid an IntegrityError, but
    # a `succeeded` row doesn't hold that slot, so both can coexist here.
    async with app.uow() as t:
        app.repos.workflow_runs.insert(t, running_run)

    response = await client.get("/api/health/detailed")
    body = response.json()
    assert body["engine"]["active_runs"] == 1
