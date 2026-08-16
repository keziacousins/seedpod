"""The parity gate — green here = cutover-ready.

Ported from reference-code/seedpod/tests/e2e/test_deployment_flow.py, adapted to the
build_app conftest shape (Seam D "Test construction"). Assertions are v1's, verbatim;
only the fixture plumbing changed:

  * test_client -> client (httpx.ASGITransport over app.api)
  * rule_engine fixture -> gone; rules load fail-fast from the overlaid test config dir
  * mock_github_token/no_github_token (monkeypatch.setenv + patch of GHCRClient.list_tags)
    -> AppConfig(github_token=...) via the make_app factory. v2 reads the environment
    only in AppConfig.from_env, and Mock/patch is banned suite-wide.

This file stays RED until the pillars + runtime spine land. That's the point.
"""

import asyncio

import pytest

from tests.conftest import make_auth_headers, make_client


class TestDeploymentFlow:
    """Test cases for complete deployment workflows."""

    async def test_feature_branch_deployment_flow(self, client, auth_headers):
        """Test complete feature branch deployment flow."""
        webhook_payload = {
            "repo": "exampleco-core",
            "branch": "feature/payment-system",
            "image": "ghcr.io/exampleco/exampleco-core:feature-payment-system-abc123",
            "commit": "abc123456",
        }

        # Step 1: Webhook triggers deployment
        response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        assert response.status_code == 200
        data = response.json()

        # Verify deployment decision
        assert data["environment"] == "ephemeral"
        assert "cluster_id" in data
        assert "deployment_id" in data
        cluster_id = data["cluster_id"]

        # Step 2: Check that cluster_id is a valid UUID and deployment is queued
        assert cluster_id is not None
        assert len(cluster_id) == 36  # UUID format
        assert "-" in cluster_id

        # Step 3: Verify deployment tracking
        assert data["status"] in ["queued", "manifest_resolution_failed", "ready"]

    async def test_main_branch_staging_deployment_flow(self, client, auth_headers):
        """Test main branch triggers staging deployment."""
        webhook_payload = {
            "repo": "exampleco-core",
            "branch": "main",
            "image": "ghcr.io/exampleco/exampleco-core:main-def456",
            "commit": "def456789",
        }

        response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        assert response.status_code == 200
        data = response.json()

        # Main branch should trigger staging update
        assert data["environment"] == "staging"
        assert data["status"] in ["queued", "updating", "ready"]

    async def test_production_tag_deployment_flow(self, client, auth_headers):
        """Test production tag deployment flow."""
        webhook_payload = {
            "repo": "exampleco-core",
            "branch": "main",
            "image": "ghcr.io/exampleco/exampleco-core:v1.2.3",
            "commit": "prod123456",
            "tag": "v1.2.3",
        }

        response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        assert response.status_code == 200
        data = response.json()

        # Test config only has main->staging rule, no production rule for tags
        assert data["environment"] == "staging"
        assert data["status"] in ["queued", "awaiting_approval", "ready", "updating"]

    async def test_deployment_preview_to_actual_deployment(self, client, auth_headers):
        """Test deployment preview followed by actual deployment."""
        # Step 1: Preview deployment
        preview_request = {
            "deployment_profile_name": "infrastructure-only",
            "triggering_repo": "exampleco-core",
            "triggering_branch": "feature/preview-test",
            "triggering_image": "ghcr.io/exampleco/exampleco-core:feature-preview-test-preview123",
        }

        preview_response = await client.post(
            "/api/deployment-preview", headers=auth_headers, json=preview_request
        )

        assert preview_response.status_code == 200
        preview_data = preview_response.json()
        assert preview_data["status"] == "success"

        # Step 2: Actual deployment
        webhook_payload = {
            "repo": "exampleco-core",
            "branch": "feature/preview-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-preview-test-preview123",
            "commit": "preview123",
        }

        deployment_response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        assert deployment_response.status_code == 200
        deployment_data = deployment_response.json()

        # Compare preview vs actual deployment
        assert "deployment_id" in deployment_data
        assert deployment_data["environment"] == "ephemeral"

    @pytest.mark.skip(
        reason="Manifest resolution error handling not fully implemented in test environment"
        " (skip carried over from v1 verbatim — revisit at parity)"
    )
    async def test_manifest_resolution_error_flow(self, client, auth_headers):
        """Test deployment flow when manifest resolution fails."""
        # Use a branch that will trigger ephemeral-stack (which has missing services)
        webhook_payload = {
            "repo": "exampleco-financial-backend",
            "branch": "feature/test-missing-services",
            "image": "ghcr.io/exampleco/exampleco-financial-backend:feature-test-missing-services-miss123",
            "commit": "miss123",
        }

        response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        assert response.status_code == 200
        data = response.json()

        # Should indicate manifest resolution failed
        assert data["status"] in ["manifest_resolution_failed", "failed"]
        assert "message" in data
        assert "deployment_id" in data  # Should still create deployment record

    async def test_rule_disabled_flow(self, client, auth_headers):
        """Test deployment flow when matching rule is disabled."""
        # First disable feature branch rules
        rules_response = await client.post(
            "/api/rules/feature_branches/disable", headers=auth_headers
        )

        if rules_response.status_code == 200:
            # Now try feature branch deployment
            webhook_payload = {
                "repo": "exampleco-core",
                "branch": "feature/disabled-test",
                "image": "ghcr.io/exampleco/exampleco-core:feature-disabled-test-disabled123",
                "commit": "disabled123",
            }

            response = await client.post(
                "/api/version-update", headers=auth_headers, json=webhook_payload
            )

            assert response.status_code == 200
            data = response.json()

            # Should result in no action
            assert data["status"] in ["no_action", "skipped"]
            assert "message" in data

            # Re-enable the rule for other tests
            await client.post("/api/rules/feature_branches/enable", headers=auth_headers)

    async def test_concurrent_deployment_flow(self, client, auth_headers):
        """Test concurrent deployments don't interfere."""
        webhook_payloads = [
            {
                "repo": "exampleco-core",
                "branch": "feature/concurrent-1",
                "image": "ghcr.io/exampleco/exampleco-core:feature-concurrent-1-con1",
                "commit": "con1",
            },
            {
                "repo": "exampleco-api",
                "branch": "feature/concurrent-2",
                "image": "ghcr.io/exampleco/exampleco-api:feature-concurrent-2-con2",
                "commit": "con2",
            },
            {
                "repo": "exampleco-web",
                "branch": "feature/concurrent-3",
                "image": "ghcr.io/exampleco/exampleco-web:feature-concurrent-3-con3",
                "commit": "con3",
            },
        ]

        # Submit all deployments concurrently
        tasks = [
            client.post("/api/version-update", headers=auth_headers, json=payload)
            for payload in webhook_payloads
        ]

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        # All deployments should succeed
        successful_responses = []
        for response in responses:
            if not isinstance(response, Exception):
                assert response.status_code == 200
                successful_responses.append(response.json())

        # All should have unique deployment IDs
        deployment_ids = {r["deployment_id"] for r in successful_responses}
        assert len(deployment_ids) == len(successful_responses)

        # All should be ephemeral environments
        for response_data in successful_responses:
            assert response_data["environment"] == "ephemeral"

    async def test_deployment_flow_with_github_token(self, make_app):
        """Test deployment flow when GitHub token is configured.

        v1 patched seedpod.providers.ghcr.GHCRClient.list_tags to avoid real GHCR
        traffic. patch() is banned here: when the spine lands, GhcrService must be
        fakeable at its transport seam (conformance-style fault injection) or
        injectable through build_app — return the problem to a DR if neither fits,
        do NOT reach for Mock.
        """
        app = await make_app(github_token="ghp_test_token_12345")
        auth_headers = await make_auth_headers(app)
        async with make_client(app) as client:
            webhook_payload = {
                "repo": "exampleco-core",
                "branch": "feature/github-test",
                "image": "ghcr.io/exampleco/exampleco-core:feature-github-test-git123",
                "commit": "git123",
            }

            response = await client.post(
                "/api/version-update", headers=auth_headers, json=webhook_payload
            )

            assert response.status_code == 200
            data = response.json()
            assert data["environment"] == "ephemeral"

    async def test_deployment_flow_without_github_token(self, client, auth_headers):
        """Test deployment flow when GitHub token is not configured.

        The default test AppConfig carries github_token=None (nothing reads the
        environment outside AppConfig.from_env), so the plain client IS the
        no-token case.
        """
        webhook_payload = {
            "repo": "exampleco-core",
            "branch": "feature/no-token-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-no-token-test-notoken123",
            "commit": "notoken123",
        }

        response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        assert response.status_code == 200
        data = response.json()

        # Should still work but might have limited manifest resolution
        assert "deployment_id" in data
        assert data["environment"] == "ephemeral"

    async def test_hotfix_deployment_flow(self, client, auth_headers):
        """Test hotfix branch deployment flow."""
        webhook_payload = {
            "repo": "exampleco-core",
            "branch": "hotfix/security-patch-urgent",
            "image": "ghcr.io/exampleco/exampleco-core:hotfix-security-patch-urgent-hotfix123",
            "commit": "hotfix123",
        }

        response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        # Verify hotfix deployment
        assert response.status_code == 200
        data = response.json()

        # Test config has no hotfix rule, should result in no_action
        assert data["environment"] == "none"
        assert data["status"] == "no_action"

        # No cluster_id for no_action deployments
        assert data["cluster_id"] is None or data["cluster_id"] == "none"

    async def test_dev_branch_deployment_flow(self, client, auth_headers):
        """Test dev branch deployment flow."""
        webhook_payload = {
            "repo": "exampleco-core",
            "branch": "dev",
            "image": "ghcr.io/exampleco/exampleco-core:dev-latest-dev123",
            "commit": "dev123",
        }

        response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        # Verify dev deployment
        assert response.status_code == 200
        data = response.json()

        # Test config has no dev rule, should result in no_action
        assert data["environment"] == "none"
        assert data["status"] == "no_action"

    async def test_unknown_branch_deployment_flow(self, client, auth_headers):
        """Test deployment flow with branch that matches no rules."""
        webhook_payload = {
            "repo": "exampleco-core",
            "branch": "experimental/unknown-pattern",
            "image": "ghcr.io/exampleco/exampleco-core:experimental-unknown-pattern-unknown123",
            "commit": "unknown123",
        }

        response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        assert response.status_code == 200
        data = response.json()

        # Should result in no action
        assert data["status"] in ["no_action", "skipped"]
        assert "message" in data
        assert (
            "no matching rule" in data["message"].lower()
            or "no action" in data["message"].lower()
            or "no deployment rule matches" in data["message"].lower()
        )

    async def test_health_check_during_deployment(self, client, auth_headers):
        """Test that health checks work during deployment processing."""
        # Start a deployment
        webhook_payload = {
            "repo": "exampleco-core",
            "branch": "feature/health-test",
            "image": "ghcr.io/exampleco/exampleco-core:feature-health-test-health123",
            "commit": "health123",
        }

        deployment_response = await client.post(
            "/api/version-update", headers=auth_headers, json=webhook_payload
        )

        assert deployment_response.status_code == 200

        # Health check should still work.
        # DR-0042 moved this to /api/health (the SPA owns /health now that seedpod
        # serves it same-origin). The ASSERTIONS below remain v1's verbatim; only the
        # URL they are made against changed -- the one line of this gate that DR
        # knowingly edits.
        health_response = await client.get("/api/health")
        assert health_response.status_code == 200

        health_data = health_response.json()
        assert health_data["status"] == "healthy"
