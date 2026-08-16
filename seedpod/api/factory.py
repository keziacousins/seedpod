"""``create_api()`` -- the FastAPI edge (docs/design/seam-d-foundation.md Decision
8's ``api/factory.py`` excerpt, followed almost verbatim): middleware + routers,
constructed LAST by ``seedpod/app/factory.py``'s ``build_app()`` (step 10), owning
nothing of its own. Every request handler reaches the composition root fresh, per
request, through ``api.state.app`` (``seedpod/api/deps.py``'s ``get_app``) -- never
through ``services``/``hub``/``config`` captured here at construction time, even
though this function accepts all three (Decision 8's pinned signature): they are
used only for THIS function's own construction-time needs (``config.cors_origins``
for the CORS middleware) and are otherwise already reachable, identically, via
``api.state.app.services``/``api.state.app.hub`` once ``seedpod/app/factory.py``
stamps it immediately after this call returns -- "consumes services + hub, owns
nothing" (Decision 8's own comment on this construction step).

**No ``lifespan=`` wired here (a deliberate divergence from Decision 8's own
illustrative sketch, which async-with's ``api.state.app.running()`` inside a
FastAPI lifespan).** The ACTUAL, already-committed ``App.start()``/``App.stop()``
(``seedpod/app/app.py``, coherence-review Conflict 15's amended lifecycle) are
called directly by every real caller: ``tests/conftest.py``'s ``make_app`` fixture
awaits ``a.start()``/``a.stop()`` itself, never through ASGI lifespan events, and
neither does any committed test in ``tests/app/``. Wiring ``App.start``/``stop``
into a FastAPI ``lifespan=`` here on top of that would double-start/double-stop
under every existing test fixture (``httpx.ASGITransport`` does not send lifespan
events unless explicitly configured to, and nothing configures it to). The
production entry point (``seedpod/__main__.py``, not built by this component) is
therefore responsible for calling ``app.start()``/``app.stop()`` around its own
``uvicorn.run(app.api, ...)`` -- flagged here, not silently invented, per this
round's brief.

**No ``install_sse_shutdown_signal_handlers``/static-UI mount** (also named in
Decision 8's illustrative sketch): no signal-handling module and no built SPA
bundle exist anywhere in this tree today (``reference-code/.../seedpod-ui/`` is
read-only archaeology, never committed) -- inventing either here would be
authoring product surface this round's brief does not ask for, not wiring
something already built.

**No ``PermissionEnforcementMiddleware`` default-deny backstop** (also named in
Decision 8's illustrative sketch, salvaged from v1's ``api/middleware.py``) --
tried and DELIBERATELY dropped: a ``starlette.middleware.base.BaseHTTPMiddleware``
wraps ``call_next()`` by consuming the inner app's response through an internal
buffering layer, and for a long-lived ``StreamingResponse`` (this round's ``GET
/api/events/stream``) that layer blocks until the inner generator's FIRST body
chunk -- for an idle SSE connection, that is however long until the next real
event or the 30s keepalive tick, not the moment headers are ready. A
default-deny backstop wrapping the whole app therefore hangs every SSE
connection for up to a keepalive interval before its headers are even
observable to the client, a regression obligation 2 exists specifically to
prevent. A correct backstop for a mixed streaming/non-streaming app needs a raw
ASGI middleware that intercepts only the ``http.response.start`` message (never
buffering body chunks) -- substantially more surface than this round's brief
asks for. The REAL enforcement mechanism -- ``seedpod/api/auth.py``'s
``require_permission(scope)``, applied explicitly to every non-public route
below -- is what's actually required and tested; this backstop was always a
belt-and-braces nicety on top of it, not a substitute for it."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from seedpod.api.routers import (
    clusters,
    deployments,
    events,
    health,
    keys,
    permissions,
    presets,
    registry,
    secrets,
    snapshots,
    timers,
    workflows,
)
from seedpod.api.routers import (
    config as config_router,  # aliased -- avoids shadowing this module's own `config: AppConfig` param below
)
from seedpod.app.app import Services
from seedpod.app.config import AppConfig
from seedpod.runtime.sse import SSEHub

__all__ = ["create_api"]


def create_api(*, services: Services, hub: SSEHub, config: AppConfig) -> FastAPI:
    del services, hub  # construction-time signature parity only -- module docstring
    api = FastAPI(title="Seedpod")

    api.add_middleware(
        CORSMiddleware,
        allow_origins=list(config.cors_origins),
        allow_credentials=False,  # Bearer/query-param auth, not cookies -- avoids the
        #                            allow_origins=["*"] + allow_credentials=True combination
        #                            browsers reject outright.
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # DR-0042: under /api like every other router. It was root-level for v1 parity until
    # DR-0041 gave the SPA the same origin -- and the SPA owns /health (app.jsx:42,130), so
    # a refresh on that page was returning raw JSON instead of the app.
    api.include_router(health.router, prefix="/api")  # GET /api/health, /api/health/detailed
    api.include_router(events.router, prefix="/api")
    api.include_router(workflows.router, prefix="/api")
    api.include_router(timers.router, prefix="/api")
    api.include_router(permissions.router, prefix="/api")
    api.include_router(deployments.router, prefix="/api")
    api.include_router(clusters.router, prefix="/api")
    api.include_router(presets.router, prefix="/api")  # Round 6, api-features
    api.include_router(snapshots.router, prefix="/api")
    api.include_router(secrets.router, prefix="/api")
    api.include_router(keys.router, prefix="/api")
    api.include_router(registry.router, prefix="/api")
    api.include_router(config_router.router, prefix="/api")

    return api
