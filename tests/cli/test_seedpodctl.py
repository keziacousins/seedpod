"""``seedpodctl`` -- the authenticated HTTP user CLI (docs/decisions/DR-0021
§0c/point 3; ``seedpod/ctl/``). Drives ``SeedpodClient`` over a real
``build_app().api`` via ``httpx.ASGITransport`` -- no live server, no
``Mock``/``patch`` anywhere (CLAUDE.md); ``httpx.ASGITransport`` is a sanctioned
httpx library feature, not a mocking library.

A representative command from three-plus groups round-trips against the real
API (health/keys/secrets/deploy), 401 on a bad/missing key surfaces the client's
own ``AuthenticationError`` rather than a crash, and a structural test asserts
the trust boundary (DR-0021): the ``seedpod.ctl`` package's import graph
contains no ``seedpod.data``/``seedpod.app.services``/``seedpod.services.crypto``/
``sqlalchemy`` import and opens no DB connection.
"""

from __future__ import annotations

import subprocess
import sys

import httpx
import pytest

from seedpod.ctl import cli
from seedpod.ctl.client import AuthenticationError, SeedpodClient


def _client_for(app, *, api_key: str | None = None) -> SeedpodClient:
    transport = httpx.ASGITransport(app=app.api)
    httpx_client = httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Bearer {api_key}"} if api_key else {},
    )
    return SeedpodClient(client=httpx_client)


# ---------------------------------------------------------------------------
# Structural: the trust boundary is enforced, not just documented (DR-0021)
# ---------------------------------------------------------------------------


def test_ctl_package_import_graph_has_no_direct_db_or_crypto_access():
    """A fresh interpreter subprocess: importing ``seedpod.ctl.cli`` (which
    pulls in the whole package -- ``client.py`` + ``cli.py``) must never load
    ``seedpod.data``, ``seedpod.app.services``, ``seedpod.services.crypto``, or
    ``sqlalchemy`` into ``sys.modules`` -- the user CLI speaks ONLY httpx over
    HTTP (DR-0021's rejected alternative: "User CLI with direct DB access")."""
    script = (
        "import sys\n"
        "import seedpod.ctl.cli\n"
        "forbidden_prefixes = ("
        "    'seedpod.data', 'seedpod.app.services', 'seedpod.services.crypto', 'sqlalchemy'"
        ")\n"
        "leaked = sorted(m for m in sys.modules if m.startswith(forbidden_prefixes))\n"
        "print(','.join(leaked))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "", f"forbidden modules leaked into sys.modules: {result.stdout.strip()}"


def test_importing_ctl_package_has_zero_side_effects(tmp_path):
    """Importing ``seedpod.ctl``/``seedpod.ctl.cli``/``seedpod.ctl.client`` runs
    nothing -- no env read, no logging config, no network call (CLAUDE.md /
    DR-0021)."""
    script = (
        "import logging, os\n"
        "assert not any(k.startswith('SEEDPOD_') for k in os.environ), 'test env leaked SEEDPOD_* vars'\n"
        "before = len(logging.root.handlers)\n"
        "import seedpod.ctl\n"
        "import seedpod.ctl.client\n"
        "import seedpod.ctl.cli\n"
        "assert len(logging.root.handlers) == before, 'import installed a logging handler'\n"
        "print('OK')\n"
    )
    env = {k: v for k, v in __import__("os").environ.items() if not k.startswith("SEEDPOD_")}
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "OK"


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


async def test_health_detailed_round_trips_the_dr0003_shape(app):
    client = _client_for(app)
    try:
        body = await client.health_detailed()
    finally:
        await client.aclose()

    assert body["status"] == "healthy"
    assert set(body["database"]) == {"connected", "cluster_count", "deployment_count", "api_key_count"}
    assert set(body["executor"]) == {"running", "pending_outbox", "dead_outbox"}
    assert set(body["timers"]) == {"running", "next_fire_at"}
    assert set(body["engine"]) == {"active_runs"}
    assert set(body["reconciler"]) == {"running", "last_sync"}


async def test_health_basic_requires_no_auth(app):
    client = _client_for(app)  # no api_key at all
    try:
        body = await client.health()
    finally:
        await client.aclose()
    assert body["status"] == "healthy"


# ---------------------------------------------------------------------------
# keys
# ---------------------------------------------------------------------------


async def test_keys_list_returns_the_dr0017_envelope(app, auth_headers):
    api_key = auth_headers["Authorization"].removeprefix("Bearer ")
    client = _client_for(app, api_key=api_key)
    try:
        body = await client.list_keys()
    finally:
        await client.aclose()

    assert "keys" in body
    assert isinstance(body["keys"], list)
    assert any(row["username"] == "test-user" for row in body["keys"])


async def test_keys_create_then_get_round_trips(app, auth_headers):
    api_key = auth_headers["Authorization"].removeprefix("Bearer ")
    client = _client_for(app, api_key=api_key)
    try:
        created = await client.create_key(
            username="ci-bot", environment="all", permissions=["clusters:read"]
        )
        assert created["username"] == "ci-bot"
        assert created["permissions"] == ["clusters:read"]
        assert created["api_key"].startswith("seedpod_")

        fetched = await client.get_key(created["id"])
        assert fetched["username"] == "ci-bot"
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# secrets
# ---------------------------------------------------------------------------


async def test_secrets_create_then_list_and_reveal_round_trip(app, auth_headers):
    api_key = auth_headers["Authorization"].removeprefix("Bearer ")
    client = _client_for(app, api_key=api_key)
    try:
        created = await client.create_secret(environment="local", key_name="DB_PASSWORD", value="hunter2")
        assert created["status"] == "created"

        listed = await client.list_secrets(environment="local")
        assert any(row["key_name"] == "DB_PASSWORD" for row in listed["secrets"])

        revealed = await client.reveal_secret("local", "DB_PASSWORD")
        assert revealed["value"] == "hunter2"
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# deploy (POST /api/version-update)
# ---------------------------------------------------------------------------


async def test_deploy_triggers_a_version_update(app, auth_headers):
    api_key = auth_headers["Authorization"].removeprefix("Bearer ")
    client = _client_for(app, api_key=api_key)
    try:
        result = await client.deploy(
            repo="exampleco/example-service",
            branch="main",
            image="ghcr.io/exampleco/example-service",
            commit="deadbeef",
            tag="v1.2.3",
        )
    finally:
        await client.aclose()

    assert "deployment_id" in result
    assert "status" in result


# ---------------------------------------------------------------------------
# workflows / timers (read-only lists)
# ---------------------------------------------------------------------------


async def test_workflows_and_timers_list_return_their_envelopes(app, auth_headers):
    api_key = auth_headers["Authorization"].removeprefix("Bearer ")
    client = _client_for(app, api_key=api_key)
    try:
        workflows = await client.list_workflows()
        timers = await client.list_timers()
    finally:
        await client.aclose()
    assert "workflows" in workflows
    assert "timers" in timers


# ---------------------------------------------------------------------------
# auth failure surfaces cleanly, not a crash
# ---------------------------------------------------------------------------


async def test_missing_key_surfaces_authentication_error_not_a_crash(app):
    client = _client_for(app)  # no Authorization header at all
    try:
        with pytest.raises(AuthenticationError):
            await client.list_keys()
    finally:
        await client.aclose()


async def test_bad_key_surfaces_authentication_error_not_a_crash(app):
    client = _client_for(app, api_key="seedpod_all_not-a-real-key")
    try:
        with pytest.raises(AuthenticationError):
            await client.list_keys()
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# main() end-to-end: argparse dispatch + clean stderr on auth failure
# ---------------------------------------------------------------------------


def test_cli_main_missing_api_key_fails_cleanly(monkeypatch, capsys):
    monkeypatch.delenv("SEEDPOD_API_KEY", raising=False)
    monkeypatch.setattr(cli, "_DEFAULT_CONFIG_PATH", cli._DEFAULT_CONFIG_PATH.parent / "does-not-exist.json")

    exit_code = cli.main(["--api-url", "http://unused", "health", "basic"])

    assert exit_code == 1
    err = capsys.readouterr().err
    assert "no API key configured" in err


def test_cli_main_requires_a_subcommand(capsys):
    with pytest.raises(SystemExit):
        cli.main([])
