"""``seedpod/api/routers/{presets,snapshots,secrets,keys,registry,config}.py`` --
Round 6, api-features component. Real ``build_app()`` over
``httpx.ASGITransport`` (``tests/conftest.py``'s ``app``/``client``/
``auth_headers`` fixtures), no Mock/patch anywhere (CLAUDE.md). GHCR fault
injection uses ``tests/services/fake_ghcr.py``'s real ``httpx.AsyncBaseTransport``
implementation (sanctioned per this round's brief); kubectl fault injection
reuses ``tests/conformance/fake_kubectl.py``'s ``FakeKubectlTransport``, swapped
into ``SnapshotService``/``ClusterService``'s private ``_kubectl`` attribute
post-construction, mirroring ``tests/api/test_clusters.py``'s own established
pattern (that module's own docstring explains why: "the only way to exercise
these reads against a controlled backend instead of a real cluster").
"""

from __future__ import annotations

import json

import httpx
import pytest
from sqlalchemy import text

from seedpod.app.services.snapshot_service import SnapshotIncompatible
from seedpod.core.codec import decode_event
from seedpod.core.errors import InfrastructureUnreachableError
from seedpod.core.events import DeploySucceeded, ProvisionSucceeded
from seedpod.providers.kubectl import KubectlConfig, KubectlProvider
from tests.conformance.fake_kubectl import FakeKubectlBackend, FakeKubectlTransport
from tests.conformance.harness import Fault
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG
from tests.services.fake_ghcr import FakeGhcrBackend, FakeGhcrTransport

# Local override of conftest.py's `app` fixture (by name -- `client`/`auth_headers`
# both depend on `app` by parameter name, so overriding it here redirects every
# fixture in this module without touching tests/conftest.py's pinned contract):
# every real ``SnapshotService.create`` call in this file writes actual bytes to
# disk under ``AppConfig.snapshot_storage_path`` -- the production default
# (``Path("data/snapshots")``, relative to cwd) would otherwise leak test dump
# files into the real repo working tree. Scoped under pytest's own per-test
# ``tmp_path`` instead, same isolation discipline every other on-disk artifact
# in this suite already gets (the sqlite db file, the config_dir overlay).


@pytest.fixture
async def app(make_app, tmp_path):
    return await make_app(snapshot_storage_path=tmp_path / "snapshots")

# ---------------------------------------------------------------------------
# Shared helpers -- birth a fully ACTIVE cluster+deployment with a persistable
# service and a real, decryptable kubeconfig (SnapshotService's exercise
# surface), mirroring tests/api/test_clusters.py's own birth/promote/kubeconfig
# helpers.
# ---------------------------------------------------------------------------


async def _birth_active_snapshot_cluster(
    app, *, actor: str = "api:test-user", profile_name: str = "snapshot-stack", branch: str = "main"
) -> tuple[str, str]:
    result = await app.services.deployments.deploy_direct(
        profile_name=profile_name, environment="ephemeral", repo="snap-test", branch=branch,
        image="ghcr.io/x/web:main", commit="abc123", ttl_hours=None, actor=actor,
    )
    assert result.status == "queued"
    now = app.clock.now()
    await app.dispatcher.apply(
        "cluster", result.cluster_id,
        ProvisionSucceeded(at=now, actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref"),
    )
    await app.dispatcher.apply(
        "deployment", result.deployment_id, DeploySucceeded(at=now, actor="engine:run:r1", resolved_images={}),
    )
    key_class = app.crypto.key_class_for_environment("ephemeral")
    encrypted = app.crypto.encrypt(FAKE_KUBECONFIG, key_class)
    async with app.uow() as tx:
        tx.execute(
            text("UPDATE clusters SET encrypted_kubeconfig = :enc, kubeconfig_key_class = :kc WHERE id = :id"),
            {"enc": encrypted, "kc": key_class, "id": result.cluster_id},
        )
    return result.cluster_id, result.deployment_id


def _install_fake_kubectl(app, *, backend: FakeKubectlBackend | None = None) -> FakeKubectlBackend:
    backend = backend or FakeKubectlBackend()
    provider = KubectlProvider(KubectlConfig(), FakeKubectlTransport(backend, frozenset()))
    app.services.clusters._kubectl = provider
    app.services.snapshots._kubectl = provider
    return backend


# ---------------------------------------------------------------------------
# Presets: CRUD + deploy
# ---------------------------------------------------------------------------


async def test_preset_crud_and_deploy_returns_deployment_id(client, auth_headers):
    create = await client.post(
        "/api/presets", headers=auth_headers,
        json={
            "name": "my-preset", "description": "test preset", "profile_name": "infrastructure-only",
            "default_branch": "main", "default_ttl_hours": 4,
        },
    )
    assert create.status_code == 201, create.text
    body = create.json()
    preset_id = body["id"]
    assert body["profile_name"] == "infrastructure-only"
    assert body["environment"] == "ephemeral"  # infrastructure-only.yml sets no environment_type -> fallback
    assert body["use_count"] == 0

    listed = await client.get("/api/presets", headers=auth_headers)
    assert listed.status_code == 200
    assert {p["id"] for p in listed.json()["presets"]} == {preset_id}

    got = await client.get(f"/api/presets/{preset_id}", headers=auth_headers)
    assert got.status_code == 200
    assert got.json()["name"] == "my-preset"

    updated = await client.put(
        f"/api/presets/{preset_id}", headers=auth_headers, json={"description": "updated"}
    )
    assert updated.status_code == 200
    assert updated.json()["description"] == "updated"

    deployed = await client.post(f"/api/presets/{preset_id}/deploy", headers=auth_headers, json={})
    assert deployed.status_code == 200, deployed.text
    deploy_body = deployed.json()
    assert deploy_body["deployment_id"]
    assert deploy_body["cluster_id"]
    assert deploy_body["status"] == "queued"

    after = await client.get(f"/api/presets/{preset_id}", headers=auth_headers)
    assert after.json()["use_count"] == 1

    deleted = await client.delete(f"/api/presets/{preset_id}", headers=auth_headers)
    assert deleted.status_code == 204
    missing = await client.get(f"/api/presets/{preset_id}", headers=auth_headers)
    assert missing.status_code == 404


async def test_preset_deploy_unknown_preset_is_404(client, auth_headers):
    response = await client.post("/api/presets/does-not-exist/deploy", headers=auth_headers, json={})
    assert response.status_code == 404


async def test_preset_create_duplicate_name_is_400(client, auth_headers):
    body = {"name": "dup", "profile_name": "infrastructure-only"}
    first = await client.post("/api/presets", headers=auth_headers, json=body)
    assert first.status_code == 201
    second = await client.post("/api/presets", headers=auth_headers, json=body)
    assert second.status_code == 400


async def test_preset_create_unknown_profile_is_400(client, auth_headers):
    response = await client.post(
        "/api/presets", headers=auth_headers, json={"name": "p1", "profile_name": "does-not-exist"}
    )
    assert response.status_code == 400


# ---------------------------------------------------------------------------
# Snapshots: create, restore, restore-history reads workflow_runs
# ---------------------------------------------------------------------------


async def test_snapshot_create_restore_and_history_reads_workflow_runs(app, client, auth_headers):
    cluster_id, _ = await _birth_active_snapshot_cluster(app)
    _install_fake_kubectl(app)

    created = await client.post(
        "/api/snapshots", headers=auth_headers,
        json={"cluster_id": cluster_id, "name": "snap-1", "description": "manual snapshot"},
    )
    assert created.status_code == 201, created.text
    snap = created.json()
    assert snap["source_cluster_id"] == cluster_id
    assert snap["deployment_profile"] == "snapshot-stack"
    assert [s["service_name"] for s in snap["services"]] == ["web"]
    assert snap["total_size_bytes"] > 0
    assert "storage_path" in snap  # detail-only field

    listed = await client.get("/api/snapshots", headers=auth_headers)
    assert listed.status_code == 200
    assert snap["id"] in {s["id"] for s in listed.json()["snapshots"]}
    assert "storage_path" not in listed.json()["snapshots"][0]  # list is the summary shape

    # restore-history is empty before any restore.
    before = await client.get(f"/api/snapshots/clusters/{cluster_id}/restore-history", headers=auth_headers)
    assert before.status_code == 200
    assert before.json()["restore_history"] == []

    restored = await client.post(
        f"/api/snapshots/{snap['id']}/restore", headers=auth_headers,
        json={"cluster_id": cluster_id, "run_migrations": True},
    )
    assert restored.status_code == 200, restored.text
    assert restored.json()["success"] is True
    assert restored.json()["services_restored"] == ["web"]

    history = await client.get(f"/api/snapshots/clusters/{cluster_id}/restore-history", headers=auth_headers)
    assert history.status_code == 200
    rows = history.json()["restore_history"]
    assert len(rows) == 1
    assert rows[0]["snapshot_id"] == snap["id"]
    assert rows[0]["snapshot_name"] == "snap-1"
    assert rows[0]["status"] == "succeeded"
    assert rows[0]["services_completed"] == 1
    assert rows[0]["services_total"] == 1

    deleted = await client.delete(f"/api/snapshots/{snap['id']}", headers=auth_headers)
    assert deleted.status_code == 204
    missing = await client.get(f"/api/snapshots/{snap['id']}", headers=auth_headers)
    assert missing.status_code == 404


async def test_snapshot_create_requires_active_cluster(app, client, auth_headers):
    cluster_id, _ = await _birth_active_snapshot_cluster(app)
    # force the cluster back out of `active` without touching the machine.
    async with app.uow() as tx:
        tx.execute(text("UPDATE clusters SET status = 'provisioning' WHERE id = :id"), {"id": cluster_id})

    response = await client.post(
        "/api/snapshots", headers=auth_headers, json={"cluster_id": cluster_id, "name": "nope"}
    )
    assert response.status_code == 400


# ---------------------------------------------------------------------------
# DR-0030: SnapshotService.restore stops conflating "unreachable" with
# "failed", and regains v1's pre-flight compatibility check. The two tests
# below call `app.services.snapshots.restore` directly -- these are
# SnapshotService's own tests, per DR-0030's own "neither is optional"
# requirement -- so each pins the exact defect DR-0030 fixes: a blanket
# `except Exception` collapsing the error taxonomy, and a silently dropped
# pre-flight check. Both must fail if either fix is reverted.
#
# A third test, `test_snapshot_restore_router_maps_dr0030_exceptions`, pins
# the HTTP-layer knock-on a fix-pass review found: once `SnapshotService.
# restore` RAISES these instead of swallowing them, the router (the OTHER
# caller of `restore`, besides `deploy.restore_snapshot`) must map them to
# real status codes or they surface as unstructured 500s.
# ---------------------------------------------------------------------------


async def test_snapshot_restore_unreachable_target_raises_not_recorded_as_failed(app, client, auth_headers):
    """DR-0030 fix 1. A target that could not be REACHED (kubectl's own
    "connection refused" symptom, `Fault.UNREACHABLE`) must surface as
    `InfrastructureUnreachableError` -- never flattened into
    `RestoreResult(success=False, ...)`, which is indistinguishable from a
    definitive failure and would let CLAUDE.md's "never conflated with
    absence" rule regress silently. Reverting the fix (widening the narrowed
    `except` back to a single blanket `except Exception`) makes this test
    fail by turning the raise into a swallowed `RestoreResult`."""
    cluster_id, _ = await _birth_active_snapshot_cluster(app)
    _install_fake_kubectl(app)
    created = await client.post(
        "/api/snapshots", headers=auth_headers,
        json={"cluster_id": cluster_id, "name": "snap-unreachable", "description": None},
    )
    assert created.status_code == 201, created.text
    snapshot_id = created.json()["id"]

    # Swap in a transport that fails every subsequent kubectl call with a
    # genuine connectivity symptom -- the restore's own pod-discovery call
    # (`KubeGetPods`, inside `_find_pod`) is what raises.
    unreachable_provider = KubectlProvider(
        KubectlConfig(), FakeKubectlTransport(FakeKubectlBackend(), frozenset({Fault.UNREACHABLE}))
    )
    app.services.snapshots._kubectl = unreachable_provider

    with pytest.raises(InfrastructureUnreachableError):
        await app.services.snapshots.restore(snapshot_id, cluster_id=cluster_id, actor="api:test")

    # Not recorded as a terminal (succeeded/failed) run -- an indeterminate
    # attempt is not a fact worth asserting into restore-history.
    history = await app.services.snapshots.restore_history(cluster_id)
    assert history == []


async def test_snapshot_restore_incompatible_target_fails_preflight_naming_services(app, client, auth_headers):
    """DR-0030 fix 2, salvaged from v1's `_perform_snapshot_restore`
    (`reference-code/seedpod/seedpod/jobs/state/deployment_job.py:300-313`): a
    snapshot taken from a profile with a persisted service ("web",
    `snapshot-stack`) restored against a cluster running a DIFFERENT profile
    that declares NO persistence at all (`infrastructure-only`) must fail
    PRE-FLIGHT, naming the missing service and both profile names -- never
    late and generically via "pod_name is None -> failed.append(...)", which
    reads identically to "the pod isn't up yet". No kubectl call is even
    attempted (no fake kubectl transport is installed on the target cluster's
    service at all -- if the pre-flight check were dropped, this test would
    fail differently, on a live-provider connectivity error, not on the
    named-mismatch message this test actually asserts).

    Raises `SnapshotIncompatible` (a `PermanentError`), not
    `RestoreResult(success=False, ...)` -- a fix-pass finding on this exact
    test caught it pinning the SWALLOW (a bare `except Exception` folding
    `SnapshotIncompatible` back into `RestoreResult.error`, indistinguishable
    from "not up yet" and silently RETRIED by any caller, e.g.
    `deploy.restore_snapshot`, that treats every `success=False` as
    transient) rather than the distinction DR-0030 fix 2 exists to create."""
    source_cluster_id, _ = await _birth_active_snapshot_cluster(app, profile_name="snapshot-stack")
    _install_fake_kubectl(app)
    created = await client.post(
        "/api/snapshots", headers=auth_headers,
        json={"cluster_id": source_cluster_id, "name": "snap-src", "description": None},
    )
    assert created.status_code == 201, created.text
    snapshot_id = created.json()["id"]

    target_cluster_id, _ = await _birth_active_snapshot_cluster(
        app, actor="api:test-user-2", profile_name="infrastructure-only", branch="other"
    )
    assert target_cluster_id != source_cluster_id

    with pytest.raises(SnapshotIncompatible) as exc_info:
        await app.services.snapshots.restore(snapshot_id, cluster_id=target_cluster_id, actor="api:test")

    message = str(exc_info.value)
    assert "web" in message
    assert "infrastructure-only" in message
    assert "snapshot-stack" in message

    # Not recorded as a terminal (succeeded/failed) run either -- matching
    # the unreachable case above: a pre-flight rejection never got far enough
    # to be a real restore ATTEMPT worth a restore-history row.
    history = await app.services.snapshots.restore_history(target_cluster_id)
    assert history == []


async def test_snapshot_restore_router_maps_dr0030_exceptions(app, client, auth_headers):
    """Fix-pass finding: DR-0030 scoped itself to `SnapshotService.restore`
    ("The change is confined to `SnapshotService.restore` and its tests" --
    DR-0030 Consequences), so once that method RAISES `SnapshotIncompatible`/
    `InfrastructureUnreachableError` instead of swallowing them, the HTTP
    router (`POST /api/snapshots/{id}/restore`) -- the OTHER caller of
    `restore`, besides `deploy.restore_snapshot` -- needed its own explicit
    handling or these would surface as unstructured 500s with no
    `snapshot_restore_completed` broadcast. Both must fail (as a 500) if
    the router's own `except` clauses are removed."""
    source_cluster_id, _ = await _birth_active_snapshot_cluster(app, profile_name="snapshot-stack")
    _install_fake_kubectl(app)
    created = await client.post(
        "/api/snapshots", headers=auth_headers,
        json={"cluster_id": source_cluster_id, "name": "snap-src", "description": None},
    )
    assert created.status_code == 201, created.text
    snapshot_id = created.json()["id"]

    # (a) SnapshotIncompatible -> 400, naming the mismatch, not a bare 500.
    incompatible_target_id, _ = await _birth_active_snapshot_cluster(
        app, actor="api:test-user-2", profile_name="infrastructure-only", branch="other"
    )
    incompatible_response = await client.post(
        f"/api/snapshots/{snapshot_id}/restore", headers=auth_headers,
        json={"cluster_id": incompatible_target_id},
    )
    assert incompatible_response.status_code == 400, incompatible_response.text
    assert "web" in incompatible_response.text
    assert "infrastructure-only" in incompatible_response.text

    # (b) InfrastructureUnreachableError -> 503, not a bare 500.
    unreachable_target_id, _ = await _birth_active_snapshot_cluster(
        app, actor="api:test-user-3", profile_name="snapshot-stack", branch="another"
    )
    _install_fake_kubectl(app)
    unreachable_provider = KubectlProvider(
        KubectlConfig(), FakeKubectlTransport(FakeKubectlBackend(), frozenset({Fault.UNREACHABLE}))
    )
    app.services.snapshots._kubectl = unreachable_provider
    unreachable_response = await client.post(
        f"/api/snapshots/{snapshot_id}/restore", headers=auth_headers,
        json={"cluster_id": unreachable_target_id},
    )
    assert unreachable_response.status_code == 503, unreachable_response.text
    assert unreachable_target_id in unreachable_response.text


async def test_snapshot_restore_falls_open_when_target_profile_unresolvable(app, client, auth_headers):
    """A narrower fix-pass finding, mirroring v1's own guard: v1's pre-flight
    check only ever runs `if profile:` (`deployment_job.py:298-300`) --
    ``manifest_resolver.manifests.get(deployment_profile_name)`` returning
    ``None`` (an unresolvable profile) SKIPS the compatibility comparison
    rather than blocking the restore. A target cluster whose deployment
    profile YAML has since been renamed/removed must restore exactly as it
    did before this pre-flight check existed, not fail generically on a
    profile-lookup error the operator would have to reverse-engineer."""
    source_cluster_id, _ = await _birth_active_snapshot_cluster(app, profile_name="snapshot-stack")
    _install_fake_kubectl(app)
    created = await client.post(
        "/api/snapshots", headers=auth_headers,
        json={"cluster_id": source_cluster_id, "name": "snap-src", "description": None},
    )
    assert created.status_code == 201, created.text
    snapshot_id = created.json()["id"]

    target_cluster_id, target_deployment_id = await _birth_active_snapshot_cluster(
        app, actor="api:test-user-4", profile_name="snapshot-stack", branch="renamed-profile"
    )
    async with app.uow() as tx:
        tx.execute(
            text("UPDATE deployments SET manifest_version = :name WHERE id = :id"),
            {"name": "this-profile-no-longer-exists", "id": target_deployment_id},
        )

    # Falls open past the pre-flight check and proceeds all the way to a
    # real, successful restore attempt against the fake kubectl transport --
    # never a generic "profile not found" failure.
    result = await app.services.snapshots.restore(snapshot_id, cluster_id=target_cluster_id, actor="api:test")
    assert result.success is True
    assert result.services_restored == ["web"]


# ---------------------------------------------------------------------------
# DR-0020: snapshot_before_destroy is a real, fail-open pre-destroy snapshot
# ---------------------------------------------------------------------------


async def test_destroy_snapshot_before_destroy_declares_it_without_taking_it(app, client, auth_headers):
    """DR-0043: the request DECLARES the snapshot and returns; the destroy workflow's
    `cluster.auto_snapshot` step takes it.

    This used to assert a snapshot row existed by the time DELETE returned, because
    `ClusterService.destroy` awaited the whole thing inline -- which is exactly why the
    request hung for up to 300s per service. What the API is now responsible for is
    carrying the operator's intent onto the armed destroy timer, intact; that the step
    then honours it is pinned in `tests/engine/steps/test_domain_steps.py`."""
    cluster_id, _ = await _birth_active_snapshot_cluster(app)
    _install_fake_kubectl(app)

    response = await client.delete(
        f"/api/clusters/{cluster_id}?snapshot_before_destroy=true", headers=auth_headers
    )
    assert response.status_code == 200
    assert response.json()["status"] == "destroy-scheduled"

    # The intent survives onto the timer's injected DestroyDue, which is the only
    # channel that reaches the workflow.
    async with app.uow() as tx:
        timer = app.repos.timers.get(tx, "cluster", cluster_id, "destroy")
    assert timer is not None
    assert decode_event(json.loads(timer.event)).snapshot is True

    # ...and nothing was taken during the request itself.
    snapshots = await client.get("/api/snapshots", headers=auth_headers)
    assert [s for s in snapshots.json()["snapshots"] if s["source_cluster_id"] == cluster_id] == []


async def test_destroy_without_snapshot_flag_leaves_the_timer_unmarked(app, client, auth_headers):
    """The converse of the above -- an unticked box must not arrive as a request."""
    cluster_id, _ = await _birth_active_snapshot_cluster(app)
    _install_fake_kubectl(app)

    response = await client.delete(f"/api/clusters/{cluster_id}", headers=auth_headers)
    assert response.status_code == 200

    async with app.uow() as tx:
        timer = app.repos.timers.get(tx, "cluster", cluster_id, "destroy")
    assert decode_event(json.loads(timer.event)).snapshot is False


# DR-0043 Erratum E1's refusal (a re-request CARRYING a snapshot, on an
# already-scheduled destroy) is pinned at the machine instead --
# `tests/core/test_cluster_table.py::
# test_destroy_scheduled_destroy_requested_carrying_a_snapshot_is_refused`.
# It cannot be driven deterministically from here: both destroy routes arm
# `fire_at` at "now" and `TimerService` runs unconditionally, so the cluster has
# already left DESTROY_SCHEDULED for DESTROYING by the time a second request
# lands. That one-poll-wide window is itself why the refusal was chosen over
# carrying the trigger on the record (which would have needed a migration).
# The router's `InvalidTransition -> 409` mapping is covered by
# `tests/api/test_clusters.py`.


async def test_destroy_without_snapshot_flag_takes_no_snapshot(app, client, auth_headers):
    cluster_id, _ = await _birth_active_snapshot_cluster(app)
    _install_fake_kubectl(app)

    response = await client.delete(f"/api/clusters/{cluster_id}", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["status"] == "destroy-scheduled"

    snapshots = await client.get("/api/snapshots", headers=auth_headers)
    assert [s for s in snapshots.json()["snapshots"] if s["source_cluster_id"] == cluster_id] == []


# ---------------------------------------------------------------------------
# Secrets: reveal audits + unknown-env raises
# ---------------------------------------------------------------------------


async def test_secret_create_reveal_audits_and_delete(client, auth_headers):
    created = await client.post(
        "/api/secrets", headers=auth_headers,
        json={"environment": "staging", "key_name": "db-password", "value": "hunter2"},
    )
    assert created.status_code == 201

    listed = await client.get("/api/secrets?environment=staging", headers=auth_headers)
    assert listed.status_code == 200
    rows = listed.json()["secrets"]
    assert len(rows) == 1
    assert rows[0]["key_name"] == "db-password"
    assert rows[0]["environment"] == "staging"
    assert rows[0]["key_class"] in ("DEV", "PROD")
    assert "value" not in rows[0]  # metadata only, never decrypted on the list view

    revealed = await client.get("/api/secrets/staging/db-password/reveal", headers=auth_headers)
    assert revealed.status_code == 200
    assert revealed.json()["value"] == "hunter2"

    deleted = await client.delete("/api/secrets/staging/db-password", headers=auth_headers)
    assert deleted.status_code == 200
    missing_reveal = await client.get("/api/secrets/staging/db-password/reveal", headers=auth_headers)
    assert missing_reveal.status_code == 404


async def test_secret_unknown_environment_raises_400(client, auth_headers):
    response = await client.post(
        "/api/secrets", headers=auth_headers,
        json={"environment": "not-a-real-env", "key_name": "x", "value": "y"},
    )
    assert response.status_code == 400

    # GET (list) never touches the crypto layer -- it's a plain SELECT, so an
    # unknown environment is legitimately "zero secrets", not an error (only
    # the encrypt/decrypt-touching operations -- create/reveal/delete -- raise;
    # SecretService.list_for_environment's own body never calls
    # `key_class_for_environment`).
    listed = await client.get("/api/secrets?environment=not-a-real-env", headers=auth_headers)
    assert listed.status_code == 200
    assert listed.json()["secrets"] == []

    revealed = await client.get("/api/secrets/not-a-real-env/x/reveal", headers=auth_headers)
    assert revealed.status_code == 400

    deleted = await client.delete("/api/secrets/not-a-real-env/x", headers=auth_headers)
    assert deleted.status_code == 400


# ---------------------------------------------------------------------------
# API keys: plaintext-once + permissions round-trip as a list
# ---------------------------------------------------------------------------


async def test_key_create_returns_plaintext_once_and_permissions_round_trip_as_list(client, auth_headers):
    created = await client.post(
        "/api/keys", headers=auth_headers,
        json={"username": "ci-bot", "environment": "staging", "permissions": ["clusters:read", "deployments:read"]},
    )
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["api_key"].startswith("seedpod_staging_")
    assert isinstance(body["permissions"], list)
    assert set(body["permissions"]) == {"clusters:read", "deployments:read"}
    key_id = body["id"]

    got = await client.get(f"/api/keys/{key_id}", headers=auth_headers)
    assert got.status_code == 200
    assert "api_key" not in got.json()  # plaintext shown exactly once, at creation
    assert isinstance(got.json()["permissions"], list)
    assert got.json()["is_valid"] is True

    listed = await client.get("/api/keys?active_only=true", headers=auth_headers)
    assert key_id in {k["id"] for k in listed.json()["keys"]}

    patched = await client.patch(f"/api/keys/{key_id}", headers=auth_headers, json={"description": "ci key"})
    assert patched.status_code == 200
    assert patched.json()["description"] == "ci key"
    assert set(patched.json()["permissions"]) == {"clusters:read", "deployments:read"}  # unchanged, immutable

    revoked = await client.delete(f"/api/keys/{key_id}", headers=auth_headers)
    assert revoked.status_code == 200
    after = await client.get(f"/api/keys/{key_id}", headers=auth_headers)
    assert after.json()["is_active"] is False
    assert after.json()["is_valid"] is False


async def test_permissions_catalog_shape(client, auth_headers):
    response = await client.get("/api/permissions", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert "snapshots:read" in body["permissions"]
    assert "Snapshots" in body["categories"]


# ---------------------------------------------------------------------------
# Registry: tags via a fake GHCR transport
# ---------------------------------------------------------------------------


async def test_registry_profiles_and_providers(client, auth_headers):
    profiles = await client.get("/api/registry/profiles", headers=auth_headers)
    assert profiles.status_code == 200
    names = {p["name"] for p in profiles.json()["profiles"]}
    assert "infrastructure-only" in names

    one = await client.get("/api/registry/profiles/infrastructure-only", headers=auth_headers)
    assert one.status_code == 200
    assert {s["name"] for s in one.json()["services"]} == {"postgres", "keycloak"}

    missing = await client.get("/api/registry/profiles/does-not-exist", headers=auth_headers)
    assert missing.status_code == 404

    providers = await client.get("/api/registry/providers", headers=auth_headers)
    assert providers.status_code == 200
    assert {p["name"] for p in providers.json()["providers"]} == {"fake"}

    repos = await client.get("/api/registry/repositories", headers=auth_headers)
    assert repos.status_code == 200
    assert {r["name"] for r in repos.json()["repositories"]} >= {"postgres", "keycloak"}


async def test_registry_tags_no_ghcr_configured_is_503(client, auth_headers):
    response = await client.get("/api/registry/tags/exampleco-core", headers=auth_headers)
    assert response.status_code == 503


async def test_registry_tags_via_fake_ghcr_transport(make_app):
    from tests.conftest import make_auth_headers, make_client

    backend = FakeGhcrBackend()
    backend.add_version(
        "exampleco-core", digest="sha256:aaa", tags=["main-abc1234"],
        created_at="2026-01-01T00:00:00Z", updated_at="2026-01-02T00:00:00Z", size=2048,
    )
    backend.add_version(
        "exampleco-core", digest="sha256:bbb", tags=["main-def5678"],
        created_at="2026-01-03T00:00:00Z", updated_at="2026-01-04T00:00:00Z", size=4096,
    )
    transport = httpx.AsyncClient(transport=FakeGhcrTransport(backend))
    ghcr_app = await make_app(github_token="fake-token", github_organization="exampleco", http_transport=transport)
    auth_headers = await make_auth_headers(ghcr_app)

    async with make_client(ghcr_app) as client:
        response = await client.get("/api/registry/tags/exampleco-core?limit=1", headers=auth_headers)
        assert response.status_code == 200
        body = response.json()
        assert body["repository"] == "exampleco-core"
        assert len(body["tags"]) == 1
        assert body["tags"][0]["tag"] == "main-def5678"  # newest (updated_at desc) wins under limit=1
        assert body["tags"][0]["size_bytes"] == 4096


# ---------------------------------------------------------------------------
# Config: overview shape
# ---------------------------------------------------------------------------


async def test_config_overview_shape(client, auth_headers):
    response = await client.get("/api/config/overview", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == {"rules", "deployment_profiles", "resolution_strategies"}
    assert set(body["rules"].keys()) == {
        "version", "total", "enabled", "disabled", "global_ephemeral_enabled", "enabled_rules", "disabled_rules",
    }
    assert body["deployment_profiles"]["total"] == len(body["deployment_profiles"]["profiles"])
    assert body["resolution_strategies"]["default"] == "branch_discovery_with_fallback"
    assert body["resolution_strategies"]["total"] > 0


async def test_config_rules_full_detail(client, auth_headers):
    response = await client.get("/api/config/rules", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "loaded"  # DeploymentRulesList.jsx/RuleDetail.jsx gate on this
    assert isinstance(body["rules"], list)
    assert body["rules"], "fixture rules file should carry at least one rule"
    first = body["rules"][0]
    assert {"name", "description", "enabled", "branch_patterns", "repo_patterns", "action", "config"} <= first.keys()


async def test_config_resolution_strategies(client, auth_headers):
    listed = await client.get("/api/config/resolution-strategies", headers=auth_headers)
    assert listed.status_code == 200
    assert listed.json()["status"] == "success"  # ResolutionStrategiesList.jsx gate
    assert "branch_discovery_with_fallback" in listed.json()["strategies"]

    one = await client.get(
        "/api/config/resolution-strategies/branch_discovery_with_fallback", headers=auth_headers
    )
    assert one.status_code == 200
    assert one.json()["status"] == "success"  # StrategyDetail.jsx gate
    assert one.json()["strategy"]["name"] == "branch_discovery_with_fallback"

    missing = await client.get("/api/config/resolution-strategies/does-not-exist", headers=auth_headers)
    assert missing.status_code == 404


async def test_config_deployment_profiles_shape(client, auth_headers):
    listed = await client.get("/api/config/deployment-profiles", headers=auth_headers)
    assert listed.status_code == 200
    body = listed.json()
    assert body["status"] == "success"  # DeploymentProfilesList.jsx gate
    assert body["count"] == len(body["deployment_profiles"])
    profiles = body["deployment_profiles"]
    assert "infrastructure-only" in profiles
    summary = profiles["infrastructure-only"]
    assert set(summary.keys()) >= {"version", "environment_type", "services", "resolution_strategy"}
    assert set(summary["services"]) == {"postgres", "keycloak"}

    one = await client.get("/api/config/deployment-profiles/infrastructure-only", headers=auth_headers)
    assert one.status_code == 200
    one_body = one.json()
    assert one_body["status"] == "success"  # ProfileDetail.jsx gate
    assert one_body["config"]["resolution_strategy"] == "branch_discovery_with_fallback"

    missing = await client.get("/api/config/deployment-profiles/does-not-exist", headers=auth_headers)
    assert missing.status_code == 404


async def test_config_providers(client, auth_headers):
    response = await client.get("/api/config/providers", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["providers"]["fake"] == {"enabled": True}  # test fixtures enable only "fake"
    assert body["enabled_providers"] == ["fake"]


# ---------------------------------------------------------------------------
# DR-0038 — naming_strategy is withdrawn, not silently stored.
# ---------------------------------------------------------------------------


async def test_creating_a_preset_with_a_naming_strategy_is_refused(client, auth_headers):
    """DR-0038 decision 2. It used to be accepted, stored and echoed back while
    doing nothing -- and the slug is now the DNS record name (DR-0034), so a
    `fixed` strategy would give two clusters from one preset the same hostname."""
    resp = await client.post(
        "/api/presets",
        headers=auth_headers,
        json={
            "name": "named-preset",
            "profile_name": "exampleco-web-2",
            "naming_strategy": {"type": "fixed", "name": "my-cluster"},
        },
    )
    assert resp.status_code == 422
    assert "naming_strategy is not supported" in resp.text


async def test_an_explicit_null_naming_strategy_is_still_accepted(client, auth_headers):
    """Clients that send the key as null (the shape the field has always had)
    must keep working -- only a real value is refused."""
    resp = await client.post(
        "/api/presets",
        headers=auth_headers,
        json={"name": "null-named-preset", "profile_name": "exampleco-web-2", "naming_strategy": None},
    )
    assert resp.status_code in (200, 201), resp.text
