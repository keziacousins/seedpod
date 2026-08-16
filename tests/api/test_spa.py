"""DR-0041 decision 3: seedpod serves the built SPA from its own origin.

Driven over a real ``httpx.ASGITransport`` against a real ``build_app().api`` with
a real (tiny) ``ui/dist`` on disk. No Mock/patch (CLAUDE.md).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from cryptography.fernet import Fernet

from seedpod.api.spa import SpaNotBuilt, mount_spa
from seedpod.app.config import AppConfig
from seedpod.app.factory import build_app
from seedpod.core.clock import FrozenClock
from seedpod.data.migrate import MIGRATIONS_DIR, migrate
from tests.fakes import FakeProvider, sequential_ids

_NOW = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)

_INDEX = "<!doctype html><title>Seedpod</title><div id=app></div>"
_ASSET = "console.log('bundle')"


@pytest.fixture
def ui_dist(tmp_path: Path) -> Path:
    """What `npm run build` actually emits: index.html plus hashed assets/."""
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text(_INDEX)
    (dist / "assets" / "index-abc123.js").write_text(_ASSET)
    (dist / "vite.svg").write_text("<svg/>")
    return dist


def _api(tmp_path: Path, test_config_dir: Path, ui_dir: Path | None):
    config = AppConfig(
        database_url=f"sqlite:///{tmp_path}/spa.db",
        secret_key_dev=Fernet.generate_key().decode(),
        config_dir=test_config_dir,
        background_tasks=False,
        ui_dir=ui_dir,
    )
    app = build_app(
        config,
        providers={"fake": FakeProvider()},
        clock=FrozenClock(_NOW),
        id_gen=sequential_ids(),
    )
    # /health/detailed queries real tables; App.start() is never called here (it
    # would start the whole runtime spine), so apply the schema directly.
    migrate(app.db.engine, MIGRATIONS_DIR)
    if config.ui_dir is not None:
        mount_spa(app.api, config.ui_dir)  # exactly what __main__.main() does
    return app


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app.api), base_url="http://testserver"
    )


# ---------------------------------------------------------------------------
# Unset: nothing mounts, and the vite workflow is untouched
# ---------------------------------------------------------------------------


async def test_with_no_ui_dir_the_root_is_not_served(tmp_path, test_config_dir):
    """The default. Keeps the SPA-on-its-own-origin dev workflow working, and keeps
    ~2400 existing tests from needing a built bundle on disk."""
    app = _api(tmp_path, test_config_dir, ui_dir=None)
    async with _client(app) as client:
        assert (await client.get("/")).status_code == 404
        assert (await client.get("/clusters")).status_code == 404
        assert (await client.get("/api/health")).status_code == 200  # API unaffected


# ---------------------------------------------------------------------------
# Set: the shell, the assets, and deep links
# ---------------------------------------------------------------------------


async def test_root_serves_index_html(tmp_path, test_config_dir, ui_dist):
    app = _api(tmp_path, test_config_dir, ui_dir=ui_dist)
    async with _client(app) as client:
        response = await client.get("/")
    assert response.status_code == 200
    assert response.text == _INDEX


async def test_hashed_assets_are_served_as_themselves(tmp_path, test_config_dir, ui_dist):
    app = _api(tmp_path, test_config_dir, ui_dir=ui_dist)
    async with _client(app) as client:
        response = await client.get("/assets/index-abc123.js")
    assert response.status_code == 200
    assert response.text == _ASSET
    assert "javascript" in response.headers["content-type"]


@pytest.mark.parametrize("path", ["/clusters", "/deployments", "/snapshots/abc-123", "/keys"])
async def test_a_deep_link_returns_the_shell_not_a_404(tmp_path, test_config_dir, ui_dist, path):
    """The SPA uses preact-router with real paths, so a refresh or a pasted link
    arrives as a GET for a file that does not exist. Without the fallback every one
    of these is a 404 and the app appears broken on reload -- the easy thing to
    miss, and the reason DR-0041 names it explicitly."""
    app = _api(tmp_path, test_config_dir, ui_dir=ui_dist)
    async with _client(app) as client:
        response = await client.get(path)
    assert response.status_code == 200
    assert response.text == _INDEX


# ---------------------------------------------------------------------------
# The fallback must not eat the API
# ---------------------------------------------------------------------------


async def test_an_unknown_api_path_stays_a_404_and_does_not_get_the_shell(
    tmp_path, test_config_dir, ui_dist
):
    """The failure mode a naive catch-all route introduces: /api/clusterz answers
    200 with an HTML shell, so a client sees "success" and renders nothing. A JSON
    404 is the honest answer and has to survive the mount."""
    app = _api(tmp_path, test_config_dir, ui_dir=ui_dist)
    async with _client(app) as client:
        response = await client.get("/api/clusterz")
    assert response.status_code == 404
    assert _INDEX not in response.text


async def test_real_api_routes_still_win_over_the_mount(tmp_path, test_config_dir, ui_dist):
    """Ordering is the whole safety argument: routers are registered by create_api
    first, so the mount at "/" only ever sees what nothing else claimed."""
    app = _api(tmp_path, test_config_dir, ui_dir=ui_dist)
    async with _client(app) as client:
        health = await client.get("/api/health")
        clusters = await client.get("/api/clusters")  # no auth -> 401/403, never the shell
    assert health.status_code == 200
    assert health.json()["status"]
    assert clusters.status_code in (401, 403)


async def test_health_detailed_is_not_shadowed(tmp_path, test_config_dir, ui_dist):
    app = _api(tmp_path, test_config_dir, ui_dir=ui_dist)
    async with _client(app) as client:
        response = await client.get("/api/health/detailed")
    assert response.status_code == 200
    assert _INDEX not in response.text


async def test_the_spa_owns_slash_health_now(tmp_path, test_config_dir, ui_dist):
    """DR-0042, and the defect that prompted it. The SPA has a Health page at
    ``/health`` (ui/src/app.jsx:42,130). While the API served ``/health`` at the root
    it won the match, so clicking Health in the nav worked (preact-router never asks
    the server) but REFRESHING that page returned raw JSON. Now the API lives under
    ``/api`` and this path falls through to the shell, like every other SPA route."""
    app = _api(tmp_path, test_config_dir, ui_dir=ui_dist)
    async with _client(app) as client:
        page = await client.get("/health")
        api = await client.get("/api/health")

    assert page.status_code == 200
    assert page.text == _INDEX  # the app, not JSON
    assert api.status_code == 200
    assert api.json()["status"]  # and the endpoint still works, one namespace over


# ---------------------------------------------------------------------------
# Refusing to serve a bundle that was never built
# ---------------------------------------------------------------------------


def test_mount_refuses_a_directory_with_no_index_html(tmp_path):
    """`ui/dist` is a build output and gitignored, so "the artifact was assembled
    without it" is a real failure mode. Refusing at startup beats mounting an empty
    directory that 404s every asset at runtime."""
    from fastapi import FastAPI

    empty = tmp_path / "not-built"
    empty.mkdir()
    with pytest.raises(SpaNotBuilt) as exc_info:
        mount_spa(FastAPI(), empty)
    assert "npm run build" in str(exc_info.value)
