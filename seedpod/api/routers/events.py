"""``GET /api/events/stream`` -- the SSE endpoint (ui-contract §2/§5, obligations
2-4; docs/decisions/DR-0010).

**Auth is query-param, not Bearer** (obligation 3: "``GET /api/events/stream?
token=<key>`` -- EventSource cannot set headers"). Salvaged from
``reference-code/seedpod/seedpod/api/events.py``'s ``stream_events`` (query-param
token validation + an ``events:stream`` permission check performed manually --
v1's own comment, kept verbatim: "can't use ``Depends`` with query param auth").

**Environment scoping at connect time (DR-0010, binding).** This router resolves
the authenticated key's ``environment`` and threads it straight into
``hub.subscribe(environment=...)`` -- DR-0010's own text: "The Round-6 events
router must resolve the key's environment at connect time and pass it to
``subscribe()``". ``ApiKeyRow.environment`` is never ``None`` (``ApiKeyService``
normalizes an unset environment to the ``'all'`` sentinel at key-creation time) --
passed through as-is; ``SSEHub``'s own filter already treats ``'all'`` as unscoped
(``seedpod/runtime/sse.py``'s docstring).

**Envelope + keepalive (obligations 2/4).** Every dequeued item is already the
``{type, data, timestamp}`` envelope (``SSEHub.broadcast``'s job, not this
module's) -- this generator only serializes and frames it as an SSE ``data:`` line.
A queue wait longer than ``_KEEPALIVE_SECONDS`` (comfortably under obligation 2's
120s ceiling) yields a ``keepalive`` envelope instead, so an idle connection is
never silent long enough to trip the client's 120s heartbeat monitor (ui-contract
§2: "forces close+reconnect after 120s of silence").

**The keepalive is a real ``data:`` frame, not an SSE comment -- and that is the
whole point.** Both v1 (``reference-code/seedpod/seedpod/api/events.py:78``) and
v2 originally yielded the comment line ``": keepalive\\n\\n"``, which satisfies
obligation 2 on the wire and still fails at its purpose: a browser's
``EventSource`` discards comment frames *before* ``onmessage``, so
``sse-client.js``'s ``updateHeartbeat()`` (only called from ``onmessage``) never
ran, ``lastHeartbeatTime`` never advanced, and the client force-closed and
reconnected every idle ~2 minutes. Observed live during smoke 6, 2026-08-09:
``[SSE] Heartbeat timeout (120002ms), forcing reconnect``. Framing it as
``data:`` makes the keepalive *observable to the client*, which is what obligation
2 was always trying to buy. The envelope comes from ``SSEHub.envelope`` so its
shape stays identical to every broadcast frame (obligation 4); it is the one
envelope the hub builds but never publishes, because a keepalive is
per-connection, not a broadcast. The client needs no change: ``onmessage``
already refreshes the heartbeat before parsing, no listener is registered for
``keepalive``, and ``event-store.js``'s default topic list excludes it -- so it
updates liveness and goes nowhere near the event buffer or the HUD. Dequeuing the
hub's own ``server_shutdown`` message (``SSEHub.close()``'s forced, un-droppable
send) ends the generator after yielding it -- the client's own ``sse-client.js``
switches to its 15s reconnect delay on this message (obligation 2), so there is
nothing further for this connection to do.

**Cleanup.** ``hub.unsubscribe(subscriber_id)`` always runs in a ``finally`` --
covers a normal end of the loop (``server_shutdown``) and a client disconnect
(Starlette's ``StreamingResponse.__call__`` races its own internal
``listen_for_disconnect`` watcher against the body iterator and cancels this
generator when the client goes away -- ``_event_stream`` deliberately does NOT
also poll ``request.is_disconnected()`` itself, which would call the same
single-consumer ASGI ``receive()`` callable that watcher already owns), and any
unexpected exception. ``SSEHub.unsubscribe`` is idempotent (its own docstring),
so double-unsubscribing after ``close()`` already cleared every subscriber is
harmless."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from fastapi import APIRouter, HTTPException, Query, Request, status
from starlette.responses import StreamingResponse

from seedpod.api.deps import get_app
from seedpod.api.permissions import has_permission

__all__ = ["router"]

router = APIRouter(tags=["events"])

# Obligation 2: SSE keepalives <= 120s apart. Comfortably under that ceiling --
# see module docstring.
_KEEPALIVE_SECONDS = 30.0

_STREAM_SCOPE = "events:stream"

_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",  # disable nginx response buffering
}


async def _event_stream(
    hub, subscriber_id: str, queue: asyncio.Queue, *, keepalive_seconds: float = _KEEPALIVE_SECONDS
) -> AsyncIterator[str]:
    # No `request.is_disconnected()` poll here (module docstring's "Cleanup"
    # section): Starlette's `StreamingResponse.__call__` already races its own
    # internal `listen_for_disconnect` watcher against this body iterator and
    # cancels this generator when the client goes away -- a second, independent
    # poll here would call the SAME single-consumer ASGI `receive()` callable
    # that watcher already owns. Cancellation (caught by the `finally` below) is
    # this generator's only disconnect signal.
    #
    # `keepalive_seconds` defaults to the module's real `_KEEPALIVE_SECONDS`
    # (production callers below never override it) and exists as an explicit
    # parameter -- not a monkeypatched module global, not a new build_app/
    # AppConfig seam -- solely so `tests/api/test_events_sse.py` can drive this
    # generator directly with a tiny interval and observe the idle->keepalive
    # branch deterministically (tests/runtime/test_subprocess.py already drives
    # async generators directly via `__anext__()`, the same idiom).
    try:
        while True:
            try:
                envelope = await asyncio.wait_for(queue.get(), timeout=keepalive_seconds)
            except TimeoutError:
                # A `data:` frame, NOT an SSE comment -- EventSource never delivers
                # comment lines to `onmessage`, so a comment keepalive is invisible to
                # the client's heartbeat monitor (module docstring).
                yield f"data: {json.dumps(hub.envelope('keepalive', {}))}\n\n"
                continue
            yield f"data: {json.dumps(envelope)}\n\n"
            if envelope.get("type") == "server_shutdown":
                return
    finally:
        hub.unsubscribe(subscriber_id)


@router.get("/events/stream")
async def stream_events(
    request: Request,
    token: str | None = Query(None, description="API key for authentication"),
) -> StreamingResponse:
    # `token` is optional at the FastAPI-validation layer (rather than
    # `Query(...)`, required) so a MISSING token surfaces as this handler's own
    # 401 below, matching every other endpoint's "no credentials -> 401"
    # discipline -- not FastAPI's generic 422 request-validation error, which a
    # required query param would raise before this body ever runs.
    app = get_app(request)
    api_key = await app.services.api_keys.validate(token) if token else None
    if api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not has_permission(api_key.permissions, _STREAM_SCOPE):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Insufficient permissions for operation: {_STREAM_SCOPE}",
        )
    subscriber_id, queue = app.hub.subscribe(environment=api_key.environment)
    return StreamingResponse(
        _event_stream(app.hub, subscriber_id, queue),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )
