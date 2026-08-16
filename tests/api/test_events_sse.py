"""``GET /api/events/stream`` -- query-param auth (ui-contract obligation 3), the
``{type, data, timestamp}`` envelope (obligation 4), and DR-0010's per-connection
environment scoping.

Real ``build_app()``, real ``SSEHub``, ``httpx.ASGITransport`` streaming.

**Why every test ends the stream via ``hub.close()``.** This ``httpx`` version's
``ASGITransport.handle_async_request`` (``httpx/_transports/asgi.py``) runs the
whole ASGI app call to completion -- collecting every body chunk into memory --
BEFORE it constructs and returns anything a ``client.stream()`` caller can
inspect; it does not deliver chunks to the caller as they're produced. A
genuinely never-ending generator (this endpoint's normal shape) therefore never
lets ``client.stream()`` resolve at all. Each test spawns the request as a
background ``asyncio.Task``, polls ``app.hub.subscriber_count()`` until the
handler's own ``hub.subscribe()`` call has registered (proving the connection is
live and past its own auth/permission checks) so a broadcast made from the test
lands in that subscriber's queue, THEN broadcasts, THEN closes the hub
(``SSEHub.close()`` -- forces a ``server_shutdown`` envelope into every queue and
is this generator's only way to return) so the ASGI call actually completes and
the buffered lines become inspectable. No Mock/patch anywhere (CLAUDE.md).

**The idle->keepalive branch (obligation 2's other half) is exercised by driving
``_event_stream`` directly**, not through the full HTTP round trip: the module's
30s ``_KEEPALIVE_SECONDS`` is a real, non-injectable production constant (no
``httpx.MockTransport``/clock fake can shrink an ``asyncio.wait_for`` timeout
short of actually waiting it out). ``_event_stream`` takes an explicit
``keepalive_seconds`` keyword (defaulting to the real constant for every
production caller) for exactly this: a test can call it with a real ``SSEHub``,
a real ``asyncio.Queue``, and a millisecond-scale interval, and drive it with
``__anext__()`` the same way ``tests/runtime/test_subprocess.py`` already drives
async generators directly -- real components throughout, zero Mock/patch.

**The keepalive is a ``data:`` frame, and that framing is itself under test.** It
used to be an SSE comment, which ``_stream``'s ``aiter_lines`` filter below
discards (``": keepalive\\n\\n"`` never starts with ``"data: "``) -- and a browser's
``EventSource`` discards it before ``onmessage`` for the same reason. So the old
assertion (``first == ": keepalive\\n\\n"``) passed while the client's heartbeat
never saw a single keepalive and force-reconnected every idle ~2 minutes, which is
exactly how the defect survived to smoke 6 (2026-08-09) and had to be caught in a
browser instead. ``test_event_stream_yields_keepalive_data_frame_when_idle`` now
asserts the ``data:`` prefix, which is precisely the property that makes a
keepalive observable to an HTTP client and to ``EventSource``. A full HTTP
round-trip version would have to either wait out the real 30s
``_KEEPALIVE_SECONDS`` or introduce the ``build_app``/``AppConfig`` interval seam
this module's production counterpart deliberately refuses -- neither is worth it
for a property the generator-level test already pins."""

from __future__ import annotations

import asyncio
import json

from seedpod.api.routers.events import _event_stream


async def _wait_for_subscribers(app, count: int, *, timeout: float = 2.0) -> None:
    async def _poll():
        while app.hub.subscriber_count() < count:
            await asyncio.sleep(0.005)

    await asyncio.wait_for(_poll(), timeout=timeout)


def _stream(client, token: str) -> asyncio.Task[tuple[int, list[dict]]]:
    """Drive one SSE connection to completion, returning ``(status_code,
    envelopes)`` -- every ``data:`` line parsed as JSON, in arrival order.
    Completes once the connection ends (module docstring: ``hub.close()``)."""

    async def _run() -> tuple[int, list[dict]]:
        async with client.stream("GET", "/api/events/stream", params={"token": token}) as response:
            envelopes = [
                json.loads(line[len("data: ") :])
                async for line in response.aiter_lines()
                if line.startswith("data: ")
            ]
            return response.status_code, envelopes

    return asyncio.ensure_future(_run())


async def test_missing_token_is_401(client):
    response = await client.get("/api/events/stream")
    assert response.status_code == 401


async def test_bad_token_is_401(client):
    response = await client.get("/api/events/stream", params={"token": "not-a-real-key"})
    assert response.status_code == 401


async def test_key_without_events_stream_permission_is_403(app, client):
    _, plaintext = await app.services.api_keys.create_api_key(
        username="scoped-user", environment="all", permissions=["clusters:read"]
    )
    response = await client.get("/api/events/stream", params={"token": plaintext})
    assert response.status_code == 403


async def test_stream_emits_the_envelope_on_broadcast(app, client):
    _, plaintext = await app.services.api_keys.create_api_key(
        username="watcher", environment="all", permissions=["events:stream"]
    )
    task = _stream(client, plaintext)
    await _wait_for_subscribers(app, 1)
    app.hub.broadcast(
        "cluster_state_changed", {"cluster_id": "c1", "old_status": "new", "new_status": "active"}
    )
    await app.hub.close(grace_period=0)
    status, envelopes = await asyncio.wait_for(task, timeout=2.0)

    assert status == 200
    assert envelopes[0]["type"] == "cluster_state_changed"
    assert envelopes[0]["data"] == {"cluster_id": "c1", "old_status": "new", "new_status": "active"}
    assert isinstance(envelopes[0]["timestamp"], str)
    assert envelopes[-1]["type"] == "server_shutdown"


async def test_environment_scoped_key_does_not_receive_another_environments_broadcast(app, client):
    """DR-0010: a key scoped to one concrete environment must NOT receive
    another environment's broadcast (both still get the unscoped
    ``server_shutdown`` that ends the test)."""
    _, staging_key = await app.services.api_keys.create_api_key(
        username="staging-watcher", environment="staging", permissions=["events:stream"]
    )
    _, production_key = await app.services.api_keys.create_api_key(
        username="production-watcher", environment="production", permissions=["events:stream"]
    )

    staging_task = _stream(client, staging_key)
    production_task = _stream(client, production_key)
    await _wait_for_subscribers(app, 2)

    app.hub.broadcast(
        "deployment_status_changed",
        {"deployment_id": "d1", "cluster_id": "c1", "old_status": "new", "new_status": "active"},
        environment="staging",
    )
    await app.hub.close(grace_period=0)

    _, staging_envelopes = await asyncio.wait_for(staging_task, timeout=2.0)
    _, production_envelopes = await asyncio.wait_for(production_task, timeout=2.0)

    staging_types = [e["type"] for e in staging_envelopes]
    production_types = [e["type"] for e in production_envelopes]
    assert "deployment_status_changed" in staging_types
    assert "deployment_status_changed" not in production_types
    assert production_types == ["server_shutdown"]


async def test_all_scoped_key_receives_every_environments_broadcast(app, client):
    """DR-0010: ``'all'`` behaves as unscoped."""
    _, all_key = await app.services.api_keys.create_api_key(
        username="all-watcher", environment="all", permissions=["events:stream"]
    )
    task = _stream(client, all_key)
    await _wait_for_subscribers(app, 1)
    app.hub.broadcast("cluster_state_changed", {"cluster_id": "c1"}, environment="production")
    await app.hub.close(grace_period=0)
    _, envelopes = await asyncio.wait_for(task, timeout=2.0)

    assert envelopes[0]["type"] == "cluster_state_changed"


async def test_server_shutdown_message_ends_the_stream(app, client):
    _, plaintext = await app.services.api_keys.create_api_key(
        username="watcher", environment="all", permissions=["events:stream"]
    )
    task = _stream(client, plaintext)
    await _wait_for_subscribers(app, 1)
    await app.hub.close(grace_period=0)
    _, envelopes = await asyncio.wait_for(task, timeout=2.0)

    assert len(envelopes) == 1
    assert envelopes[0]["type"] == "server_shutdown"


async def test_event_stream_yields_keepalive_data_frame_when_idle(app):
    """Obligation 2's other half: a queue wait longer than the keepalive interval
    yields a ``keepalive`` envelope framed as a ``data:`` line -- NOT an SSE comment.
    The distinction is the whole fix: ``EventSource`` discards comment frames before
    ``onmessage``, so a comment keepalive can never refresh the client's heartbeat.
    Module docstring's "idle->keepalive branch" section covers why this drives
    ``_event_stream`` directly rather than through the full HTTP stream."""
    subscriber_id, queue = app.hub.subscribe(environment="all")
    gen = _event_stream(app.hub, subscriber_id, queue, keepalive_seconds=0.02)
    try:
        first = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
        assert first.startswith("data: "), "a comment frame is invisible to EventSource"
        keepalive = json.loads(first[len("data: ") :])
        assert keepalive["type"] == "keepalive"
        # Obligation 4: a keepalive is the same envelope shape as every broadcast.
        assert set(keepalive) == {"type", "data", "timestamp"}
        assert keepalive["data"] == {}

        # A real event arriving after the keepalive still flows through the
        # same generator, framed as a normal `data:` line.
        app.hub.broadcast("cluster_state_changed", {"cluster_id": "c1"})
        second = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
        assert second.startswith("data: ")
        envelope = json.loads(second[len("data: ") :])
        assert envelope["type"] == "cluster_state_changed"

        # Idle again -- another keepalive, proving this isn't a one-shot.
        third = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
        assert json.loads(third[len("data: ") :])["type"] == "keepalive"
    finally:
        await gen.aclose()

    # The generator's own `finally` (module docstring's "Cleanup" section) ran
    # on `aclose()` too -- `hub.unsubscribe` already fired.
    assert app.hub.subscriber_count() == 0
