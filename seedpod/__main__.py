"""The server runner -- ``python -m seedpod`` / the ``seedpod`` console script
(docs/decisions/DR-0021 §0a/point 1; docs/design/seam-d-foundation.md Decision
8's server-runner block). One of v2's THREE trust-model entry points -- the
process that owns the DB, the runtime spine, and the ASGI HTTP edge.

``main()``: ``config = AppConfig.from_env()``; ``setup_logging(config)``;
``app = build_app(config)``; attach the production ASGI lifespan to
``app.api``; ``uvicorn.run(app.api, ..., timeout_graceful_shutdown=30)`` --
the 30s bound is salvaged verbatim from ``reference-code/seedpod/start.py:129``
(``# Wait 30s for graceful shutdown (SSE + scheduler)``); without it uvicorn's
default is an unbounded graceful-shutdown wait, and a long-lived SSE stream
(``GET /api/events/stream``) can hang process shutdown indefinitely.

**The lifespan is attached HERE, not in ``seedpod/api/factory.py``.**
``seedpod/api/factory.py``'s own module docstring explains why: ``create_api``
deliberately does not wire a FastAPI ``lifespan=`` because every existing test
fixture drives ``app.api`` over ``httpx.ASGITransport``, which never emits ASGI
lifespan events -- wiring ``App.start()``/``stop()`` into a lifespan there would
double-start/double-stop the moment any such fixture also called
``a.start()``/``a.stop()`` directly (as ``tests/conftest.py``'s ``make_app``
does). This module IS the production path (``uvicorn`` DOES emit lifespan
events), so it is the correct, and only, place to make that connection.

The lifespan is wired onto the already-built ``FastAPI`` instance via
Starlette's supported post-construction mechanism --
``app.api.router.lifespan_context`` (a plain attribute Starlette's own
``Router.__init__`` assigns and every ASGI lifespan dispatch reads from,
``starlette/routing.py``) -- rather than reconstructing ``app.api`` or
threading a ``lifespan=`` callable back through ``create_api``/``build_app``
(both of which are pure-construction composition-root code this round must
not edit, CLAUDE.md).

Zero import-time side effects: importing this module runs nothing (no env
read, no DB, no logging configuration, no network) -- every effect happens
inside ``main()``/``build_server()``.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import uvicorn
from dotenv import load_dotenv

from seedpod.api.spa import mount_spa
from seedpod.app.config import AppConfig
from seedpod.app.factory import build_app
from seedpod.app.logging import rotate_logs_on_startup, setup_logging
from seedpod.app.singleton import (
    AlreadyRunning,
    PortUnavailable,
    assert_port_available,
    single_instance,
)

if TYPE_CHECKING:
    from fastapi import FastAPI

    from seedpod.app.app import App
    from seedpod.core.clock import Clock
    from seedpod.providers.contract import Provider

__all__ = ["EXIT_REFUSED_TO_START", "build_server", "main"]

# A distinct code, so a script can tell "another seedpod already owns this host"
# from "seedpod crashed". The shell launchers use 78 (EX_CONFIG) for their own
# refusals -- a missing runtime, a missing state directory -- and this is the
# other kind: nothing is misconfigured, something is already running.
EXIT_REFUSED_TO_START = 75  # EX_TEMPFAIL: stop the incumbent and try again.


def _attach_lifespan(app: App) -> None:
    """Wire ``App.start()``/``App.stop()`` into ``app.api``'s ASGI lifespan --
    the runtime spine (executor, timers, reconciler, health) now starts/stops
    exactly when the ASGI server does. See this module's docstring for why
    this wiring lives here rather than in ``seedpod/api/factory.py``.
    """

    @asynccontextmanager
    async def lifespan(_asgi_app: FastAPI):
        async with app.running():
            yield

    app.api.router.lifespan_context = lifespan


def build_server(
    config: AppConfig,
    *,
    providers: Mapping[str, Provider] | None = None,
    clock: Clock | None = None,
    id_gen: Callable[[], str] | None = None,
) -> App:
    """Construct the fully-wired ``App`` (``build_app(config)``) and attach
    its production ASGI lifespan to ``app.api``. Performs no IO of its own
    (``build_app`` is pure construction, ``_attach_lifespan`` is a plain
    attribute assignment) -- factored out of ``main()`` so tests can exercise
    the app-build + lifespan-attach wiring without ever calling
    ``uvicorn.run`` (which blocks and binds a real port).

    ``providers``/``clock``/``id_gen`` are ``build_app``'s own test seams,
    forwarded straight through unchanged (default ``None`` each, exactly
    ``build_app``'s own defaults) -- ``main()`` below calls this with none of
    them, so production behavior is unaffected; tests use them the same way
    every other ``build_app`` caller in this tree does (e.g.
    ``tests/conftest.py``'s ``make_app``), to inject a ``FakeProvider``/
    ``FrozenClock``/deterministic id generator instead of real providers and
    wall-clock time.
    """
    app = build_app(config, providers=providers, clock=clock, id_gen=id_gen)
    _attach_lifespan(app)
    return app


def main() -> int:
    """DR-0041 Amendment B: this entry point now carries the three operational
    behaviours that used to exist ONLY in ``start.py`` -- ``.env`` loading, the
    single-instance guard, and startup log rotation. ``start.py`` is a dev
    convenience script and is not shipped in a packaged artifact, so the console
    script (this) was the less capable path: an appliance got no ``.env``, no
    singleton, and no per-boot log separation.

    Order is load-bearing. ``.env`` first, because ``AppConfig.from_env()`` is a
    pure read of the already-loaded environment. Rotation before
    ``setup_logging``, so the outgoing file is renamed before a handler opens it.
    The guards before ``build_server``, so a second start costs nothing and says
    why -- rather than constructing the whole graph, applying migrations through
    the lifespan, and only then failing to bind.

    **On ``load_dotenv()`` moving here.** ``start.py``'s docstring argued the
    opposite -- that this path "assumes the environment is already exported by its
    caller (a process supervisor, a shell with a sourced ``.env``)" -- and under
    DR-0021 that was right, because the only production shape imagined was a
    supervised one. DR-0041 decided there is no supervisor: the operator runs
    ``bin/seedpod`` from a shell and owns the lifecycle. The two positions
    reconcile without either giving way, because ``load_dotenv`` defaults to
    ``override=False``: anything the caller genuinely exported still wins, and the
    file is only consulted for what the caller did not set. ``AppConfig.from_env()``
    also remains the sole ``os.environ`` reader -- nothing here reads it, this only
    populates it before that one reader runs.

    The appliance's own ``var/.env`` is exported by ``bin/seedpod`` rather than
    discovered from here: ``load_dotenv()`` searches upward from the cwd, which a
    release root is not, and inventing a ``SEEDPOD_ENV_FILE`` would add a second
    ``os.environ`` reader to dodge a problem the launcher already solves.

    **Why the two guards are caught here rather than allowed to propagate.**
    Both already say exactly the right thing -- which pid holds the lock, which
    pid holds the port, and the command that stops it. Letting them out of
    ``main()`` wraps that sentence in a traceback whose top frame is
    ``contextlib`` and whose subject is an operator who wanted one line. It is
    this repo's oldest recurring defect in its milder form -- errors know why,
    then throw it away -- with the reason buried rather than dropped: an
    operator who has to read a stack trace to find "pid 16111 -- stop it first"
    has been handed the answer in the least usable shape it has. Found by
    running a second ``bin/seedpod`` against a real installed release, which is
    also the only way this path is ever reached.
    """
    load_dotenv()
    config = AppConfig.from_env()
    rotate_logs_on_startup(log_dir=config.log_dir, retention=10)
    setup_logging(config)

    try:
        with single_instance(config.pid_file):
            assert_port_available(config.api_host, config.api_port)
            app = build_server(config)
            if config.ui_dir is not None:
                # DR-0041 decision 3. AFTER build_server, so every router is already
                # registered and the mount at "/" only sees unclaimed paths.
                mount_spa(app.api, config.ui_dir)
            uvicorn.run(
                app.api,
                host=config.api_host,
                port=config.api_port,
                log_config=None,
                timeout_graceful_shutdown=30,
            )
    except (AlreadyRunning, PortUnavailable) as refusal:
        print(f"seedpod: {refusal}", file=sys.stderr)
        return EXIT_REFUSED_TO_START
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
