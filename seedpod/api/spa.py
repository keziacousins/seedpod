"""``mount_spa`` — serve the built SPA from the same origin as the API
(DR-0041 decision 3).

**Why this is a function called by the entry point, not part of ``create_api``.**
``seedpod/api/factory.py``'s docstring declined a static mount because no built
bundle existed in the tree, and that boundary is still right for a second reason:
every test fixture drives ``app.api`` over ``httpx.ASGITransport`` and needs no
bundle, so a mount wired into ``create_api`` would be dead weight in ~2400 tests
and would fail whenever ``ui/dist`` was absent. ``seedpod/__main__.py`` is the
production path and already owns the other production-only wiring (the ASGI
lifespan, and since DR-0041 Amendment B the ``.env`` load, the singleton and the
startup log rotation). This joins them.

With ``SEEDPOD_UI_DIR`` unset nothing mounts at all, so the vite dev-server
workflow — the SPA on its own origin, reaching the API cross-origin via
``VITE_API_URL`` — keeps working unchanged.

**Why a subclass rather than a catch-all route.** The SPA uses ``preact-router``
with real paths (``/clusters``, ``/deployments``, …), so a deep link or a refresh
arrives as a GET for a file that does not exist and must return ``index.html``.
Doing that as an ``@api.get("/{path:path}")`` catch-all works, but it silently
swallows unknown ``/api/*`` paths too, turning a JSON 404 into an HTML page — the
kind of thing that reads as "the API broke" from the client. Overriding
``StaticFiles.get_response`` keeps the fallback where the 404 actually happens and
leaves every real route untouched.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response
from starlette.staticfiles import StaticFiles
from starlette.types import Scope

__all__ = ["SpaNotBuilt", "mount_spa"]

# Prefixes the SPA fallback must never answer for. An unknown path under these is
# a genuine 404 and has to stay one -- a client asking for /api/clusterz wants
# JSON telling it so, not 200 and an HTML shell that renders an empty page.
_API_PREFIXES = ("/api",)


class SpaNotBuilt(RuntimeError):
    """``SEEDPOD_UI_DIR`` points somewhere without an ``index.html``."""


class _SpaStaticFiles(StaticFiles):
    """``StaticFiles`` that answers a missing path with ``index.html`` — unless the
    request was for the API, in which case the 404 stands."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code != 404:
                raise
            request_path = scope.get("path", "")
            if request_path.startswith(_API_PREFIXES):
                raise
            return await super().get_response("index.html", scope)


def mount_spa(api: FastAPI, ui_dir: Path) -> None:
    """Mount ``ui_dir`` at ``/``. Must be called AFTER every router is included.

    Ordering is the whole safety argument: Starlette matches ``app.routes`` in
    order, so the API routes registered by ``create_api`` are found first and this
    mount only ever sees what nothing else claimed. Mounting at ``/`` is therefore
    safe here and would not be inside ``create_api``.

    Raises ``SpaNotBuilt`` rather than mounting an empty directory: a UI that
    silently 404s every asset is harder to diagnose than one that refuses to start,
    and ``ui/dist`` is a build output that can legitimately be missing (it is
    gitignored) if the artifact was assembled wrongly.
    """
    if not (ui_dir / "index.html").is_file():
        raise SpaNotBuilt(
            f"SEEDPOD_UI_DIR={ui_dir} has no index.html -- build it with "
            "`npm run build` in ui/, or unset SEEDPOD_UI_DIR to serve the SPA "
            "separately (vite dev server)"
        )
    api.mount("/", _SpaStaticFiles(directory=ui_dir, html=True), name="spa")
