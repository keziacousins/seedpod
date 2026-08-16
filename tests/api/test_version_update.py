"""``seedpod/api/routers/deployments.py`` -- the parity-gate spine, at the HTTP
layer. Real ``build_app()`` over ``httpx.ASGITransport`` (``tests/conftest.py``'s
``app``/``client``/``auth_headers``/``make_app`` fixtures), no Mock/patch anywhere
(CLAUDE.md). ``tests/acceptance/test_deployment_flow.py`` is the parity gate proper;
this file exercises the same branches at router granularity plus obligation 1 and
the rule-admin surface this component adds.
"""

from __future__ import annotations

import asyncio

from seedpod.core.events import ProvisionSucceeded
from tests.conftest import make_auth_headers, make_client

# ---------------------------------------------------------------------------
# POST /api/version-update -- one test per parity branch
# ---------------------------------------------------------------------------


async def test_feature_branch_is_ephemeral_and_queued(client, auth_headers):
    response = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/payments",
            "image": "ghcr.io/exampleco/exampleco-core:feature-payments-abc123",
            "commit": "abc123",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["environment"] == "ephemeral"
    assert body["status"] in ("queued", "manifest_resolution_failed", "ready")
    assert body["cluster_id"] is not None
    assert len(body["cluster_id"]) == 36
    assert body["deployment_id"] is not None


async def test_main_branch_is_staging(client, auth_headers):
    response = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "main",
            "image": "ghcr.io/exampleco/exampleco-core:main-def456", "commit": "def456",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["environment"] == "staging"
    assert body["status"] in ("queued", "updating", "ready")


async def test_unmatched_branch_is_no_action_with_message(client, auth_headers):
    response = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "experimental/unknown",
            "image": "ghcr.io/exampleco/exampleco-core:x", "commit": "x",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] in ("no_action", "skipped")
    assert body["environment"] == "none"
    assert body["cluster_id"] is None
    assert "no deployment rule matches" in body["message"].lower()


async def test_hotfix_branch_is_no_action(client, auth_headers):
    response = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "hotfix/urgent",
            "image": "ghcr.io/exampleco/exampleco-core:hotfix-urgent-x", "commit": "x",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["environment"] == "none"
    assert body["status"] == "no_action"
    assert body["cluster_id"] is None
    assert body["deployment_id"] is not None


async def test_version_update_requires_auth(client):
    response = await client.post(
        "/api/version-update",
        json={"repo": "exampleco-core", "branch": "main", "image": "x", "commit": "x"},
    )
    assert response.status_code == 401


async def test_concurrent_version_updates_get_unique_deployment_ids(client, auth_headers):
    payloads = [
        {"repo": "exampleco-core", "branch": f"feature/concurrent-{i}",
         "image": f"ghcr.io/exampleco/exampleco-core:feature-concurrent-{i}-c{i}", "commit": f"c{i}"}
        for i in range(5)
    ]
    responses = await asyncio.gather(
        *(client.post("/api/version-update", headers=auth_headers, json=p) for p in payloads)
    )
    for r in responses:
        assert r.status_code == 200
    ids = {r.json()["deployment_id"] for r in responses}
    assert len(ids) == len(responses)


async def test_manifest_resolution_failed_is_200_with_deployment_id(client, auth_headers):
    """The obligation the v1-carried skip (``test_manifest_resolution_error_flow``
    in ``tests/acceptance/test_deployment_flow.py``, left skipped verbatim per this
    round's brief) leaves unguarded: a required-service resolution failure must
    still be a normal 200 response, never a 500. Constructed entirely through the
    injected seams (no Mock) -- ``registry-test/*`` (``tests/fixtures/
    deployment-rules.yml``) routes to the ``needs-registry`` profile (``tests/
    fixtures/deployment-profiles/needs-registry.yml``), whose one required,
    non-external, non-overridden service can never resolve because the default
    ``auth_headers``/``client`` fixtures build an app with no ``github_token``
    (``ghcr_service`` stays ``None`` -- ``seedpod/app/factory.py`` step 5.5), so
    resolution fails via the real ``ManifestResolver``/GHCR-absence path, not a
    patched exception. The profile itself loads fine, so this is the
    birth-then-reject sub-case (mirrors ``tests/app/test_services_deployment.py``'s
    ``test_image_resolution_failure_with_valid_profile_still_creates_deployment_
    record``): the cluster IS born, only the deployment is rejected.
    """
    response = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "registry-test/whatever",
            "image": "ghcr.io/exampleco/exampleco-core:registry-test-whatever-rt1", "commit": "rt1",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "manifest_resolution_failed"
    assert body["deployment_id"] is not None
    assert body["environment"] == "ephemeral"
    assert body["cluster_id"] is not None  # birth-then-reject: cluster WAS born


# ---------------------------------------------------------------------------
# obligation 1 -- deployment_status_changed carries deployment_id/cluster_id/
# old_status/new_status
# ---------------------------------------------------------------------------


async def test_deployment_status_changed_notify_carries_full_payload(app, client, auth_headers):
    subscriber_id, queue = app.hub.subscribe(environment="all")
    try:
        response = await client.post(
            "/api/version-update",
            headers=auth_headers,
            json={
                "repo": "exampleco-core", "branch": "feature/obligation-1",
                "image": "ghcr.io/exampleco/exampleco-core:feature-obligation-1-o1", "commit": "o1",
            },
        )
        assert response.status_code == 200
        deployment_id = response.json()["deployment_id"]
        cluster_id = response.json()["cluster_id"]

        seen = None
        for _ in range(20):
            envelope = await asyncio.wait_for(queue.get(), timeout=1.0)
            if envelope["type"] == "deployment_status_changed" and envelope["data"]["deployment_id"] == deployment_id:
                seen = envelope
                break
        assert seen is not None, "deployment_status_changed was never broadcast for this deployment"
        data = seen["data"]
        assert data["deployment_id"] == deployment_id
        assert data["cluster_id"] == cluster_id
        assert "old_status" in data
        assert "new_status" in data
    finally:
        app.hub.unsubscribe(subscriber_id)


# ---------------------------------------------------------------------------
# POST /api/deployment-preview + POST /api/version-update/preview
# ---------------------------------------------------------------------------


async def test_deployment_preview_success(client, auth_headers):
    response = await client.post(
        "/api/deployment-preview",
        headers=auth_headers,
        json={
            "deployment_profile_name": "infrastructure-only",
            "triggering_repo": "exampleco-core",
            "triggering_branch": "feature/preview-test",
            "triggering_image": "ghcr.io/exampleco/exampleco-core:feature-preview-test-p1",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["deployment_profile"] == "infrastructure-only"
    assert body["resolved_images"]["postgres"] == "postgres:15.4-alpine"
    assert body["resolved_images"]["keycloak"] == "quay.io/keycloak/keycloak:22.0"


async def test_deployment_preview_unknown_profile_is_404(client, auth_headers):
    response = await client.post(
        "/api/deployment-preview",
        headers=auth_headers,
        json={
            "deployment_profile_name": "does-not-exist",
            "triggering_repo": "exampleco-core",
            "triggering_branch": "feature/x",
            "triggering_image": "ghcr.io/exampleco/exampleco-core:x",
        },
    )
    assert response.status_code == 404


async def test_deployment_preview_unresolvable_profile_is_4xx_not_500(client, auth_headers):
    """DR-0026 part 2 (docs/decisions/DR-0026-preview-render-context-and-error-
    mapping.md): a manifest-resolution ``PermanentError(INVALID_INPUT)`` at the
    API edge is a 4xx, never an unhandled 500. ``exampleco-web-2`` (real, shipped)
    references ``secrets.tailscale_auth_key``; this app's fresh test DB has no
    such secret stored for "ephemeral", so it is genuinely ABSENT -- DR-0026's
    own "a key referenced by a template but absent from the environment still
    raises" -- and the router must map that to a client error, not a crash."""
    response = await client.post(
        "/api/deployment-preview",
        headers=auth_headers,
        json={
            "deployment_profile_name": "exampleco-web-2",
            "triggering_repo": "exampleco-web-2",
            "triggering_branch": "feature/dr-0026-preview-4xx",
            "triggering_image": "ghcr.io/exampleco/exampleco-web-2:feature-dr-0026-preview-4xx-p1",
        },
    )
    assert 400 <= response.status_code < 500
    assert "tailscale_auth_key" in response.text


async def test_deployment_preview_with_stored_secret_succeeds_and_never_leaks_plaintext(client, auth_headers):
    """DR-0026 part 1, at the wire level: once ``tailscale_auth_key`` is stored
    (through the real ``POST /api/secrets`` surface) preview of the SAME
    secret-bearing profile succeeds via the redaction sentinel -- and the real
    plaintext value never appears anywhere in the HTTP response body. That
    assertion is the permission boundary DR-0026 exists to hold: a caller with
    only ``deployments:read`` (what mints ``auth_headers`` here also carries
    ``secrets:create`` in this fixture, but the PREVIEW response itself must
    never depend on or reveal it)."""
    store = await client.post(
        "/api/secrets",
        headers=auth_headers,
        json={
            "environment": "ephemeral", "key_name": "tailscale_auth_key",
            "value": "tskey-REAL-do-not-leak",  # pragma: allowlist secret
        },
    )
    assert store.status_code == 201

    response = await client.post(
        "/api/deployment-preview",
        headers=auth_headers,
        json={
            "deployment_profile_name": "exampleco-web-2",
            "triggering_repo": "exampleco-web-2",
            "triggering_branch": "feature/dr-0026-preview-ok",
            "triggering_image": "ghcr.io/exampleco/exampleco-web-2:feature-dr-0026-preview-ok-p1",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert "tskey-REAL-do-not-leak" not in response.text


async def test_deployment_preview_environment_override_scopes_secrets_at_the_wire_level(client, auth_headers):
    """DR-0027 (docs/decisions/DR-0027-secret-scope-is-the-rule-derived-
    environment.md), at the wire level: the new OPTIONAL ``environment`` field
    on ``POST /api/deployment-preview`` is used EXACTLY when supplied,
    overriding exampleco-web-2's own ``environment_type`` ("ephemeral").
    ``tailscale_auth_key`` is stored ONLY under "staging": preview WITHOUT the
    override falls back to "ephemeral" and still 4xxs (DR-0026 part 2, the
    secret genuinely isn't known there); WITH ``"environment": "staging"`` it
    succeeds."""
    store = await client.post(
        "/api/secrets",
        headers=auth_headers,
        json={
            "environment": "staging", "key_name": "tailscale_auth_key",
            "value": "tskey-staging-only-do-not-leak",  # pragma: allowlist secret
        },
    )
    assert store.status_code == 201

    without_override = await client.post(
        "/api/deployment-preview",
        headers=auth_headers,
        json={
            "deployment_profile_name": "exampleco-web-2",
            "triggering_repo": "exampleco-web-2",
            "triggering_branch": "feature/dr-0027-wire-no-override",
            "triggering_image": "ghcr.io/exampleco/exampleco-web-2:feature-dr-0027-wire-no-override-p1",
        },
    )
    assert 400 <= without_override.status_code < 500
    assert "tailscale_auth_key" in without_override.text

    with_override = await client.post(
        "/api/deployment-preview",
        headers=auth_headers,
        json={
            "deployment_profile_name": "exampleco-web-2",
            "triggering_repo": "exampleco-web-2",
            "triggering_branch": "feature/dr-0027-wire-override",
            "triggering_image": "ghcr.io/exampleco/exampleco-web-2:feature-dr-0027-wire-override-p1",
            "environment": "staging",
        },
    )
    assert with_override.status_code == 200
    assert with_override.json()["status"] == "success"
    assert "tskey-staging-only-do-not-leak" not in with_override.text


async def test_version_update_preview_is_an_alias(client, auth_headers):
    response = await client.post(
        "/api/version-update/preview",
        headers=auth_headers,
        json={
            "deployment_profile_name": "infrastructure-only",
            "triggering_repo": "exampleco-core",
            "triggering_branch": "feature/preview-test",
            "triggering_image": "ghcr.io/exampleco/exampleco-core:feature-preview-test-p2",
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "success"


async def test_preview_then_actual_deployment(client, auth_headers):
    preview = await client.post(
        "/api/deployment-preview",
        headers=auth_headers,
        json={
            "deployment_profile_name": "infrastructure-only",
            "triggering_repo": "exampleco-core",
            "triggering_branch": "feature/preview-then-actual",
            "triggering_image": "ghcr.io/exampleco/exampleco-core:feature-preview-then-actual-p3",
        },
    )
    assert preview.status_code == 200
    assert preview.json()["status"] == "success"

    actual = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/preview-then-actual",
            "image": "ghcr.io/exampleco/exampleco-core:feature-preview-then-actual-p3", "commit": "p3",
        },
    )
    assert actual.status_code == 200
    body = actual.json()
    assert "deployment_id" in body
    assert body["environment"] == "ephemeral"


# ---------------------------------------------------------------------------
# Rule admin: disable/enable/reload
# ---------------------------------------------------------------------------


async def test_disabled_rule_makes_version_update_no_action(client, auth_headers):
    disable = await client.post("/api/rules/feature_branches/disable", headers=auth_headers)
    assert disable.status_code == 200
    assert disable.json() == {"status": "disabled", "rule": "feature_branches"}

    try:
        response = await client.post(
            "/api/version-update",
            headers=auth_headers,
            json={
                "repo": "exampleco-core", "branch": "feature/disabled-test",
                "image": "ghcr.io/exampleco/exampleco-core:feature-disabled-test-d1", "commit": "d1",
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["status"] in ("no_action", "skipped")
        assert "message" in body
    finally:
        enable = await client.post("/api/rules/feature_branches/enable", headers=auth_headers)
        assert enable.status_code == 200
        assert enable.json() == {"status": "enabled", "rule": "feature_branches"}

    # re-enabled: the SAME branch now matches again.
    response = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/disabled-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-disabled-test-d2", "commit": "d2",
        },
    )
    assert response.json()["environment"] == "ephemeral"


async def test_disable_unknown_rule_is_404(client, auth_headers):
    response = await client.post("/api/rules/does-not-exist/disable", headers=auth_headers)
    assert response.status_code == 404


async def test_reload_rules_returns_summary(client, auth_headers):
    response = await client.post("/api/rules/reload", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "reloaded"
    assert "feature_branches" in body["summary"]["enabled_rules"]
    assert "main_staging" in body["summary"]["enabled_rules"]


async def test_reload_rules_after_disable_restores_from_disk(client, auth_headers):
    await client.post("/api/rules/feature_branches/disable", headers=auth_headers)
    reload_response = await client.post("/api/rules/reload", headers=auth_headers)
    assert reload_response.status_code == 200
    assert "feature_branches" in reload_response.json()["summary"]["enabled_rules"]


async def test_reload_deployment_profiles(client, auth_headers):
    response = await client.post("/api/deployment-profiles/reload", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "reloaded"
    assert body["deployment_profiles_count"] >= 1


# ---------------------------------------------------------------------------
# GET /api/deployments, GET /api/deployments/{id}
# ---------------------------------------------------------------------------


async def test_list_deployments_shape(client, auth_headers):
    create = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/list-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-list-test-l1", "commit": "l1",
        },
    )
    deployment_id = create.json()["deployment_id"]

    response = await client.get("/api/deployments", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert "deployments" in body
    entry = next(d for d in body["deployments"] if d["deployment_id"] == deployment_id)
    assert set(entry) == {
        "deployment_id", "cluster_id", "environment", "manifest_version",
        "status", "deployed_by", "deployed_at",
    }


async def test_get_deployment_detail_not_found_is_404(client, auth_headers):
    response = await client.get("/api/deployments/does-not-exist", headers=auth_headers)
    assert response.status_code == 404


async def test_get_deployment_detail_shape_and_audit_history(client, auth_headers):
    create = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/detail-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-detail-test-dt1", "commit": "dt1",
        },
    )
    deployment_id = create.json()["deployment_id"]

    response = await client.get(f"/api/deployments/{deployment_id}", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["deployment_id"] == deployment_id
    assert "failure_reason" in body
    assert "resolved_images" in body
    assert "spec_ref" in body
    assert "superseded_by" in body
    assert "error_message" not in body
    assert "services" not in body
    # DR-0016: the response KEY is `deployed_at`, sourced from the row's created_at.
    assert "deployed_at" in body
    assert "created_at" not in body
    assert len(body["audit_history"]) == 1
    audit = body["audit_history"][0]
    assert audit["triggering_repo"] == "exampleco-core"
    assert audit["triggering_branch"] == "feature/detail-test"


# ---------------------------------------------------------------------------
# redeploy / retrigger / cancel
# ---------------------------------------------------------------------------


async def test_redeploy(client, auth_headers):
    create = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/redeploy-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-redeploy-test-r1", "commit": "r1",
        },
    )
    deployment_id = create.json()["deployment_id"]

    response = await client.post(f"/api/deployments/{deployment_id}/redeploy", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["deployment_id"] != deployment_id
    assert body["cluster_id"] == create.json()["cluster_id"]


async def test_redeploy_unknown_deployment_is_404(client, auth_headers):
    response = await client.post("/api/deployments/does-not-exist/redeploy", headers=auth_headers)
    assert response.status_code == 404


async def test_retrigger(app, client, auth_headers):
    create = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/retrigger-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-retrigger-test-rt1", "commit": "rt1",
        },
    )
    deployment_id = create.json()["deployment_id"]
    cluster_id = create.json()["cluster_id"]
    # Promote to ACTIVE first so retrigger's fresh version_update reuses the SAME
    # cluster (find_active_cluster_by_branch) rather than birthing a second one --
    # the realistic "retrigger an already-running deployment" scenario, and avoids
    # a same-tick slug collision the deterministic `sequential_ids()` test id_gen
    # would otherwise hit (two 000...00-prefixed ids truncate to the same 8-char
    # slug suffix).
    now = app.clock.now()
    await app.dispatcher.apply(
        "cluster", cluster_id,
        ProvisionSucceeded(at=now, actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref"),
    )

    response = await client.post(f"/api/deployments/{deployment_id}/retrigger", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["new_deployment_id"] != deployment_id
    assert body["original_deployment_id"] == deployment_id


async def test_cancel_without_body(client, auth_headers):
    create = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/cancel-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-cancel-test-c1", "commit": "c1",
        },
    )
    deployment_id = create.json()["deployment_id"]

    response = await client.post(f"/api/deployments/{deployment_id}/cancel", headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == {"status": "cancelled", "deployment_id": deployment_id}

    detail = await client.get(f"/api/deployments/{deployment_id}", headers=auth_headers)
    assert detail.json()["status"] == "cancelled"


async def test_cancel_unknown_deployment_is_404(client, auth_headers):
    response = await client.post("/api/deployments/does-not-exist/cancel", headers=auth_headers)
    assert response.status_code == 404


async def test_cancel_twice_is_409(client, auth_headers):
    create = await client.post(
        "/api/version-update",
        headers=auth_headers,
        json={
            "repo": "exampleco-core", "branch": "feature/double-cancel-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-double-cancel-test-dc1", "commit": "dc1",
        },
    )
    deployment_id = create.json()["deployment_id"]

    first = await client.post(f"/api/deployments/{deployment_id}/cancel", headers=auth_headers)
    assert first.status_code == 200
    second = await client.post(f"/api/deployments/{deployment_id}/cancel", headers=auth_headers)
    assert second.status_code == 409


# ---------------------------------------------------------------------------
# GHCR fakeability (DR-0015) sanity: make_app/make_client/make_auth_headers work
# for this router the same way the acceptance test uses them.
# ---------------------------------------------------------------------------


async def test_version_update_via_make_app(make_app):
    app = await make_app()
    auth = await make_auth_headers(app)
    async with make_client(app) as client:
        response = await client.post(
            "/api/version-update",
            headers=auth,
            json={
                "repo": "exampleco-core", "branch": "feature/make-app-test",
                "image": "ghcr.io/exampleco/exampleco-core:feature-make-app-test-ma1", "commit": "ma1",
            },
        )
        assert response.status_code == 200
        assert response.json()["environment"] == "ephemeral"
