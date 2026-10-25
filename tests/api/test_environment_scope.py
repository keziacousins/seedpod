"""DR-0054: a key scoped to one environment acts only on that environment.

Every test here uses a key scoped to ``staging`` against something in
``ephemeral`` (the environment ``feature/*`` branches and the shipped profiles
get), and an ``'all'`` key for setup. The key holds every permission the route
needs, so a refusal can only come from the environment check. Real ``build_app()``
over ``httpx.ASGITransport``, no Mock/patch (CLAUDE.md).
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from seedpod.core.effects import ScheduleTimer
from seedpod.core.events import TtlExpired
from seedpod.data.repositories import WorkflowRunRow
from tests.api.test_clusters import _birth_cluster, _promote_to_active, _set_kubeconfig
from tests.api.test_features import _birth_active_snapshot_cluster, _install_fake_kubectl


@pytest.fixture
async def app(make_app, tmp_path):
    # Same override as tests/api/test_features.py: snapshot dumps go under tmp_path.
    return await make_app(snapshot_storage_path=tmp_path / "snapshots")


async def _key(app, *, environment: str, permissions: list[str] | None = None) -> dict[str, str]:
    _, plaintext = await app.services.api_keys.create_api_key(
        username=f"{environment}-key", environment=environment, permissions=permissions or ["*"]
    )
    return {"Authorization": f"Bearer {plaintext}", "Content-Type": "application/json"}


def _refused(response) -> None:
    assert response.status_code == 403, response.text
    assert "'staging'" in response.json()["detail"]


async def _count(app, table: str) -> int:
    async with app.uow() as tx:
        return tx.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()  # noqa: S608 -- fixed names


# ---------------------------------------------------------------------------
# The rule itself
# ---------------------------------------------------------------------------


async def test_a_wildcard_key_scoped_to_one_environment_does_not_reach_another(app, client):
    """``*`` grants every permission, not every environment."""
    await _birth_cluster(app, "c1", slug="c1-slug", environment="ephemeral")
    _refused(await client.get("/api/clusters/c1", headers=await _key(app, environment="staging")))
    ok = await client.get("/api/clusters/c1", headers=await _key(app, environment="ephemeral"))
    assert ok.status_code == 200
    ok = await client.get("/api/clusters/c1", headers=await _key(app, environment="all"))
    assert ok.status_code == 200


# ---------------------------------------------------------------------------
# Clusters
# ---------------------------------------------------------------------------


async def test_cluster_list_shows_only_the_key_environment(app, client):
    await _birth_cluster(app, "c1", slug="c1-slug", environment="ephemeral")
    await _birth_cluster(app, "s1", slug="s1-slug", environment="staging")
    staging = await client.get("/api/clusters", headers=await _key(app, environment="staging"))
    assert {c["id"] for c in staging.json()["clusters"]} == {"s1"}
    every = await client.get("/api/clusters", headers=await _key(app, environment="all"))
    assert {c["id"] for c in every.json()["clusters"]} == {"c1", "s1"}


async def test_every_per_cluster_route_refuses_a_cluster_outside_the_key_environment(app, client):
    await _birth_cluster(app, "c1", slug="c1-slug", environment="ephemeral", ttl_hours=4)
    await _promote_to_active(app, "c1")
    await _set_kubeconfig(app, "c1")
    _install_fake_kubectl_for_clusters(app)
    headers = await _key(app, environment="staging")

    for method, path, body in [
        ("GET", "/api/clusters/c1", None),
        ("GET", "/api/clusters/c1-slug", None),
        ("GET", "/api/clusters/c1/kubeconfig", None),
        ("GET", "/api/clusters/c1/pods", None),
        ("GET", "/api/clusters/c1/pods/default/web-abc123", None),
        ("GET", "/api/clusters/c1/pods/default/web-abc123/logs", None),
        ("GET", "/api/clusters/c1/events", None),
        ("GET", "/api/clusters/c1/deployments", None),
        ("GET", "/api/clusters/c1/audit", None),
        ("POST", "/api/clusters/c1/extend", {"ttl_hours": 1}),
        ("POST", "/api/clusters/c1/rehabilitate", None),
        ("DELETE", "/api/clusters/c1", None),
    ]:
        response = await client.request(method, path, headers=headers, json=body)
        assert response.status_code == 403, (method, path, response.text)

    row = await app.services.clusters.get("c1")
    assert row.status == "active"  # the refused DELETE changed nothing


def _install_fake_kubectl_for_clusters(app) -> None:
    from seedpod.providers.kubectl import KubectlConfig, KubectlProvider
    from tests.conformance.fake_kubectl import FakeKubectlBackend, FakeKubectlTransport

    app.services.clusters._kubectl = KubectlProvider(
        KubectlConfig(), FakeKubectlTransport(FakeKubectlBackend(), frozenset())
    )


# ---------------------------------------------------------------------------
# Deployments
# ---------------------------------------------------------------------------


_FEATURE_BRANCH = {"repo": "exampleco-core", "branch": "feature/x", "image": "ghcr.io/x/core:abc", "commit": "abc"}
_MAIN_BRANCH = {"repo": "exampleco-core", "branch": "main", "image": "ghcr.io/x/core:main", "commit": "def"}


async def test_version_update_outside_the_key_environment_is_refused_and_writes_nothing(app, client):
    """The rules choose the environment. The check is after the rules and before
    the first write, so a refused deploy leaves no cluster and no deployment."""
    headers = await _key(app, environment="staging", permissions=["deployments:trigger"])
    _refused(await client.post("/api/version-update", headers=headers, json=_FEATURE_BRANCH))
    assert await _count(app, "clusters") == 0
    assert await _count(app, "deployments") == 0

    allowed = await client.post("/api/version-update", headers=headers, json=_MAIN_BRANCH)
    assert allowed.status_code == 200, allowed.text
    assert allowed.json()["environment"] == "staging"


async def test_deployment_routes_refuse_a_deployment_outside_the_key_environment(app, client):
    created = await client.post("/api/version-update", headers=await _key(app, environment="all"), json=_FEATURE_BRANCH)
    deployment_id = created.json()["deployment_id"]
    status_before = (await app.services.deployments.get(deployment_id)).status
    headers = await _key(app, environment="staging")

    listed = await client.get("/api/deployments", headers=headers)
    assert listed.json()["deployments"] == []
    _refused(await client.get(f"/api/deployments/{deployment_id}", headers=headers))
    _refused(await client.post(f"/api/deployments/{deployment_id}/cancel", headers=headers))
    _refused(await client.post(f"/api/deployments/{deployment_id}/redeploy", headers=headers))
    _refused(await client.post(f"/api/deployments/{deployment_id}/retrigger", headers=headers))

    assert (await app.services.deployments.get(deployment_id)).status == status_before  # cancel changed nothing
    assert await _count(app, "deployments") == 1


async def test_retrigger_checks_the_new_environment_too(app, client):
    """The original is ``staging``, but its branch now maps to ``ephemeral``: the
    rules run again, so the key must admit the new environment as well."""
    original = await app.services.deployments.deploy_direct(
        key_environment="all",
        profile_name="infrastructure-only", environment="staging", **_FEATURE_BRANCH,
        ttl_hours=None, actor="api:test",
    )
    headers = await _key(app, environment="staging")
    assert (await client.get(f"/api/deployments/{original.deployment_id}", headers=headers)).status_code == 200
    _refused(await client.post(f"/api/deployments/{original.deployment_id}/retrigger", headers=headers))
    assert await _count(app, "deployments") == 1


async def test_preview_is_checked_against_the_environment_it_would_use(app, client):
    """A preview lists the secret names of its environment."""
    headers = await _key(app, environment="staging", permissions=["deployments:read"])
    body = {
        "deployment_profile_name": "infrastructure-only", "triggering_repo": "exampleco-core",
        "triggering_branch": "feature/x", "triggering_image": "ghcr.io/x/core:abc",
    }
    _refused(await client.post("/api/deployment-preview", headers=headers, json=body))  # profile's own: ephemeral
    _refused(await client.post("/api/version-update/preview", headers=headers, json=body))
    allowed = await client.post("/api/deployment-preview", headers=headers, json={**body, "environment": "staging"})
    assert allowed.status_code == 200, allowed.text


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------


async def test_preset_routes_refuse_a_preset_outside_the_key_environment(app, client):
    preset = {"name": "p", "profile_name": "infrastructure-only", "default_branch": "main"}  # ephemeral
    headers = await _key(app, environment="staging", permissions=["deployments:*"])
    _refused(await client.post("/api/presets", headers=headers, json=preset))

    created = await client.post("/api/presets", headers=await _key(app, environment="all"), json=preset)
    preset_id = created.json()["id"]

    assert (await client.get("/api/presets", headers=headers)).json()["presets"] == []
    _refused(await client.get(f"/api/presets/{preset_id}", headers=headers))
    _refused(await client.put(f"/api/presets/{preset_id}", headers=headers, json={"description": "x"}))
    _refused(await client.post(f"/api/presets/{preset_id}/deploy", headers=headers))
    _refused(await client.delete(f"/api/presets/{preset_id}", headers=headers))

    row = await app.services.presets.get(preset_id)
    assert row.use_count == 0  # the refused deploy did not count as a use
    assert row.description is None
    assert await _count(app, "deployments") == 0


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


async def test_snapshot_routes_use_the_source_cluster_environment(app, client):
    cluster_id, _ = await _birth_active_snapshot_cluster(app)  # ephemeral
    _install_fake_kubectl(app)
    admin = await _key(app, environment="all")
    created = await client.post("/api/snapshots", headers=admin, json={"cluster_id": cluster_id, "name": "snap"})
    assert created.status_code == 201, created.text
    snapshot_id = created.json()["id"]
    headers = await _key(app, environment="staging")

    assert (await client.get("/api/snapshots", headers=headers)).json()["snapshots"] == []
    assert len((await client.get("/api/snapshots", headers=admin)).json()["snapshots"]) == 1
    _refused(await client.get(f"/api/snapshots/{snapshot_id}", headers=headers))
    _refused(await client.get(f"/api/snapshots/clusters/{cluster_id}/restore-history", headers=headers))
    _refused(await client.post("/api/snapshots", headers=headers, json={"cluster_id": cluster_id, "name": "again"}))
    _refused(await client.delete(f"/api/snapshots/{snapshot_id}", headers=headers))
    assert (await client.get(f"/api/snapshots/{snapshot_id}", headers=admin)).status_code == 200


async def test_restore_needs_both_the_target_and_the_source_environment(app, client):
    """A staging key must not pour ephemeral data into a staging cluster."""
    cluster_id, _ = await _birth_active_snapshot_cluster(app)  # ephemeral
    _install_fake_kubectl(app)
    created = await client.post(
        "/api/snapshots", headers=await _key(app, environment="all"), json={"cluster_id": cluster_id, "name": "snap"}
    )
    await _birth_cluster(app, "s1", slug="s1-slug", environment="staging")
    await _promote_to_active(app, "s1")

    response = await client.post(
        f"/api/snapshots/{created.json()['id']}/restore",
        headers=await _key(app, environment="staging"), json={"cluster_id": "s1"},
    )
    _refused(response)
    assert "snapshot" in response.json()["detail"]


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------


async def test_secret_routes_refuse_another_environment(app, client):
    admin = await _key(app, environment="all")
    created = await client.post(
        "/api/secrets", headers=admin, json={"environment": "ephemeral", "key_name": "DB_PASSWORD", "value": "hunter2"}
    )
    assert created.status_code == 201
    headers = await _key(app, environment="staging", permissions=["secrets:*"])

    _refused(await client.get("/api/secrets/ephemeral/DB_PASSWORD/reveal", headers=headers))
    _refused(await client.get("/api/secrets?environment=ephemeral", headers=headers))
    _refused(await client.post(
        "/api/secrets", headers=headers, json={"environment": "ephemeral", "key_name": "X", "value": "y"}
    ))
    _refused(await client.delete("/api/secrets/ephemeral/DB_PASSWORD", headers=headers))

    own = await client.get("/api/secrets", headers=headers)  # defaults to the key's own environment
    assert own.status_code == 200
    revealed = await client.get("/api/secrets/ephemeral/DB_PASSWORD/reveal", headers=admin)
    assert revealed.json()["value"] == "hunter2"  # the refused DELETE removed nothing


# ---------------------------------------------------------------------------
# Workflows and timers
# ---------------------------------------------------------------------------


async def test_workflow_and_timer_lists_show_only_the_key_environment(app, client):
    await _birth_cluster(app, "c1", slug="c1-slug", environment="ephemeral")
    await _birth_cluster(app, "s1", slug="s1-slug", environment="staging")
    now = app.clock.now()
    async with app.uow() as t:
        for cluster_id in ("c1", "s1"):
            app.repos.timers.upsert(t, ScheduleTimer(
                aggregate_type="cluster", aggregate_id=cluster_id, timer_key="destroy",
                fire_at=now + timedelta(hours=1), event=TtlExpired(at=now, actor="timer:ttl"),
            ), created_by_effect=f"effect-{cluster_id}")
            app.repos.workflow_runs.insert(t, WorkflowRunRow(
                id=f"run-{cluster_id}", workflow="provision-kind", workflow_version=1, cluster_id=cluster_id,
                deployment_id=None, dedupe_key=f"dedupe-{cluster_id}", args={}, status="running",
                cancel_requested=False, failed_step=None, error=None, undo_incomplete=None,
                initiated_by="api:test", created_at=now, started_at=now, finished_at=None,
            ))
    admin = await _key(app, environment="all")
    headers = await _key(app, environment="staging")

    assert {r["cluster_id"] for r in (await client.get("/api/workflows", headers=admin)).json()["workflows"]} == {"c1", "s1"}
    assert [r["cluster_id"] for r in (await client.get("/api/workflows", headers=headers)).json()["workflows"]] == ["s1"]

    all_timers = (await client.get("/api/timers", headers=admin)).json()["timers"]
    assert {t["aggregate_id"] for t in all_timers} == {"c1", "s1"}
    staging_timers = (await client.get("/api/timers", headers=headers)).json()["timers"]
    assert {t["aggregate_id"] for t in staging_timers} == {"s1"}


# ---------------------------------------------------------------------------
# Changes that reach every environment
# ---------------------------------------------------------------------------


async def test_key_management_needs_an_all_key(app, client):
    """Otherwise a scoped ``*`` key could make an ``'all'`` key."""
    headers = await _key(app, environment="staging")
    for method, path, body in [
        ("GET", "/api/keys", None),
        ("POST", "/api/keys", {"username": "escalate", "environment": None, "permissions": ["*"]}),
        ("GET", "/api/keys/1", None),
        ("PATCH", "/api/keys/1", {"description": "x"}),
        ("DELETE", "/api/keys/1", None),
        ("GET", "/api/permissions", None),
    ]:
        response = await client.request(method, path, headers=headers, json=body)
        assert response.status_code == 403, (method, path, response.text)
        assert "every environment" in response.json()["detail"]
    assert (await client.get("/api/keys", headers=await _key(app, environment="all"))).status_code == 200


async def test_rule_and_profile_changes_need_an_all_key(app, client):
    headers = await _key(app, environment="staging", permissions=["config:*"])
    for path in ["/api/rules/reload", "/api/deployment-profiles/reload", "/api/rules/feature-branches/disable"]:
        response = await client.post(path, headers=headers)
        assert response.status_code == 403, (path, response.text)
    # Reading the configuration changes nothing, so a scoped key may.
    assert (await client.get("/api/config/overview", headers=headers)).status_code == 200


@pytest.mark.parametrize(
    ("environment", "expected"),
    [("staging", 201), ("production", 201), ("all", 201), (None, 201), ("prod", 400), ("Staging", 400)],
)
async def test_key_creation_refuses_an_unknown_environment(app, client, environment, expected):
    response = await client.post(
        "/api/keys", headers=await _key(app, environment="all"),
        json={"username": "new", "environment": environment, "permissions": ["clusters:read"]},
    )
    assert response.status_code == expected, response.text
