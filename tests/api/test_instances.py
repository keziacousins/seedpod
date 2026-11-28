"""``/api/instances`` (DR-0055 decisions 5 to 9): admission, ownership, DR-0054 scope, renew and
destroy. Real ``build_app()`` over ``httpx.ASGITransport``; no Mock/patch (CLAUDE.md). Templates and
limits are set on the app, as the factory would load them. The ``fake`` provider has no provision
workflow, so no run moves an instance while a test reads it.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from seedpod.core.instance_limits import InstanceLimits
from seedpod.core.instance_template import InstanceSize, InstanceTemplate
from tests.runtime.conftest import NOW, make_instance_row, make_owner_key

KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA user@host"
_SIZES = {"small": InstanceSize(vcpu=2, memory_mb=2048, hourly_price=0.5), "unpriced": InstanceSize(vcpu=1, memory_mb=512)}
_BASE = {"image": "img", "ssh_user": "admin", "entrypoint": "/opt/boot", "sizes": _SIZES}
TEMPLATES = {
    "ubuntu": InstanceTemplate(provider="fake", lease_seconds=3600, max_lifetime_seconds=7200, **_BASE),
    "remote": InstanceTemplate(
        provider="digitalocean", lease_seconds=3600, max_lifetime_seconds=7200,
        **{**_BASE, "sizes": {n: z.model_copy(update={"provider_size": "s-1vcpu-1gb"}) for n, z in _SIZES.items()}},  # DR-0056
    ),
}


@pytest.fixture
async def app(make_app):
    a = await make_app()
    a.instance_templates = TEMPLATES
    return a


async def _key(app, name="a", environment="all", permissions=("instances:*",)):
    record, plaintext = await app.services.api_keys.create_api_key(
        username=name, environment=environment, permissions=list(permissions)
    )
    return {"Authorization": f"Bearer {plaintext}"}, record.id


def _body(**over):
    return {"slug": "box", "template": "ubuntu", "size": "small", "environment": "ephemeral", "ssh_public_key": KEY, **over}


async def _insert(app, instance_id="i1", slug="box", owner=None, **over) -> int:
    """A row born by hand, in any status. Returns its owner key id."""
    async with app.uow() as tx:
        owner = owner or make_owner_key(tx)
        app.repos.instances.insert(tx, make_instance_row(instance_id, slug, owner, **over))
    return owner


async def _all(app, sql: str):
    async with app.uow() as tx:
        return tx.execute(text(sql)).all()


async def test_create_births_the_instance_with_its_timer_and_run(app, client):
    headers, key_id = await _key(app)
    response = await client.post("/api/instances", headers=headers, json=_body(lease_seconds=1800))
    assert response.status_code == 201, response.text
    body = response.json()
    assert (body["status"], body["owner_key_id"], body["provider"], body["vcpu"], body["hourly_price"]) == (
        "provisioning", key_id, "fake", 2, 0.5,
    )
    assert body["expires_at"] == (NOW + timedelta(seconds=1800)).isoformat()
    assert body["hard_expires_at"] == (NOW + timedelta(seconds=7200)).isoformat()
    assert not {"known_hosts", "ssh_public_key", "resource_ids"} & body.keys()

    async with app.uow() as tx:
        row = app.repos.instances.get(tx, body["id"])
    assert (row.ssh_public_key, row.lease_seconds) == (KEY, 1800)
    assert await _all(app, "SELECT aggregate_type, timer_key FROM timers") == [("instance", "expiry")]
    assert await _all(app, "SELECT kind FROM effects_outbox WHERE aggregate_type = 'instance' AND kind = 'run_workflow'")


@pytest.mark.parametrize(
    "over",
    [
        {"template": "nope"}, {"size": "huge"}, {"size": "unpriced"}, {"template": "remote"},  # not configured here
        {"ssh_public_key": f"{KEY}\nssh-rsa AAAA"}, {"boot_secret": "x"}, {"slug": "Bad Slug"}, {"slug": "box\n"}, {"slug": "-x"}, {"slug": "0b7c1d2e-3f40-4a5b-8c6d-7e8f9a0b1c2d"},
        {"environment": "all"}, {"environment": "prod"}, {"lease_seconds": 7201}, {"lease_seconds": 0},
    ],
)
async def test_a_bad_request_is_422_and_leaves_no_row(app, client, over):
    headers, _ = await _key(app)
    response = await client.post("/api/instances", headers=headers, json=_body(**over))
    assert response.status_code == 422, response.text
    assert await _all(app, "SELECT id FROM instances") == []


@pytest.mark.parametrize("status", ["provisioning", "destroyed"])
async def test_a_slug_is_never_reused_even_after_destroyed(app, client, status):
    await _insert(app, status=status)
    headers, _ = await _key(app)
    response = await client.post("/api/instances", headers=headers, json=_body())
    assert response.status_code == 409, response.text


@pytest.mark.parametrize(
    ("existing", "limits", "expected"),
    [
        ("active", {"per_key": {"count": 1}}, "per_key.count"),
        ("active", {"per_key": {"vcpu": 3}}, "per_key.vcpu"),
        ("active", {"per_environment": {"memory_mb": 4000}}, "per_environment.memory_mb"),
        ("active", {"per_provider": {"fake": {"hourly_spend": 0.9}}}, "per_provider.hourly_spend"),
        ("destroyed", {"per_key": {"count": 1}}, None),  # a destroyed instance holds nothing
    ],
)
async def test_a_limit_is_429_naming_it_and_leaves_no_row(app, client, existing, limits, expected):
    headers, key_id = await _key(app)
    await _insert(app, "i0", "other", owner=key_id, status=existing, vcpu=2, memory_mb=2048, hourly_price=0.5)
    app.instance_limits = InstanceLimits.model_validate(limits)
    response = await client.post("/api/instances", headers=headers, json=_body())
    if expected is None:
        assert response.status_code == 201, response.text
        return
    assert response.status_code == 429 and expected in response.json()["detail"]
    assert len(await _all(app, "SELECT id FROM instances")) == 1


async def test_a_key_sees_only_its_own_instances_and_a_wildcard_key_sees_all(app, client):
    alice, _ = await _key(app, "alice")
    bob, _ = await _key(app, "bob")
    admin, _ = await _key(app, "root", permissions=("*",))
    created = (await client.post("/api/instances", headers=alice, json=_body())).json()

    for headers, visible in ((alice, True), (admin, True), (bob, False)):
        listed = [i["id"] for i in (await client.get("/api/instances", headers=headers)).json()["instances"]]
        assert listed == ([created["id"]] if visible else [])
        for by in (created["id"], created["slug"]):
            assert (await client.get(f"/api/instances/{by}", headers=headers)).status_code == (200 if visible else 404)
    assert (await client.post(f"/api/instances/{created['id']}/renew", headers=bob)).status_code == 404
    assert (await client.delete(f"/api/instances/{created['id']}", headers=bob)).status_code == 404
    assert (await client.get("/api/instances/absent", headers=alice)).status_code == 404


async def test_the_environment_rule_applies_to_a_wildcard_key(app, client):
    admin, _ = await _key(app, "root", permissions=("*",))
    created = (await client.post("/api/instances", headers=admin, json=_body())).json()
    staging, _ = await _key(app, "stg", environment="staging", permissions=("*",))

    assert (await client.post("/api/instances", headers=staging, json=_body(slug="other"))).status_code == 403
    assert (await client.get("/api/instances", headers=staging)).json()["instances"] == []
    for method, path in (("GET", ""), ("POST", "/renew"), ("DELETE", "")):
        response = await client.request(method, f"/api/instances/{created['id']}{path}", headers=staging)
        assert response.status_code == 403 and "'staging'" in response.json()["detail"], (method, path)
    assert len(await _all(app, "SELECT id FROM instances WHERE status = 'provisioning'")) == 1


async def test_renew_extends_the_lease_and_destroy_begins_destruction(app, client):
    headers, key_id = await _key(app)
    await _insert(app, owner=key_id, status="active", expires_at=NOW + timedelta(seconds=600), lease_seconds=3600)

    renewed = await client.post("/api/instances/box/renew", headers=headers)
    assert renewed.status_code == 200, renewed.text
    assert renewed.json()["expires_at"] == (NOW + timedelta(seconds=3600)).isoformat()
    assert (await client.post("/api/instances/box/renew", headers=headers)).status_code == 409  # no further

    destroyed = await client.delete("/api/instances/box", headers=headers)
    assert (destroyed.status_code, destroyed.json()["status"]) == (202, "destroying")
    assert (await client.post("/api/instances/box/renew", headers=headers)).status_code == 409
    assert (await client.delete("/api/instances/box", headers=headers)).status_code == 409


@pytest.mark.parametrize("over", [{"status": "failed"}, {"status": "active", "expires_at": NOW}])
async def test_renew_is_409_when_the_instance_cannot_be_renewed(app, client, over):
    headers, key_id = await _key(app)
    await _insert(app, owner=key_id, **over)
    assert (await client.post("/api/instances/box/renew", headers=headers)).status_code == 409


async def test_a_key_without_the_permission_is_403(app, client):
    headers, _ = await _key(app, permissions=("instances:read",))
    assert (await client.post("/api/instances", headers=headers, json=_body())).status_code == 403
    assert (await client.get("/api/instances", headers=headers)).status_code == 200
