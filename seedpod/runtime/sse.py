"""``SSEHub`` -- in-memory pub/sub for Server-Sent Events, factory-constructed (no
module global; docs/design/coherence-review.md §2 glossary: ``SSEHub`` lives at
``runtime/sse.py``, topics ``cluster_state_changed``, ``deployment_status_changed``,
``workflow_progress``, ``job_*``).

Salvaged from ``reference-code/seedpod/seedpod/core/sse_manager.py``'s
``SSEConnection``/``SSEManager`` (v1 has no ``SSEHub`` type -- this module is that
pair collapsed behind the v2 shape, per this component's build task). Three things
are salvaged at different fidelities:

**The pub/sub shape (loosely salvaged, deliberately changed).** v1's
``SSEConnection.queue`` is a bare, unbounded ``asyncio.Queue()`` (sse_manager.py:24)
fed by ``await queue.put(...)`` (sse_manager.py:29) -- a slow/dead consumer lets the
queue, and therefore process memory, grow without bound. This module bounds every
per-subscriber queue and switches to ``put_nowait`` with drop-on-full: a stalled
consumer loses the newest events (never blocks the broadcaster, never raises, never
grows unbounded).

**Per-connection environment scoping (salvaged VERBATIM, per DR-0010).** A prior
build pass of this component dropped v1's environment filtering as out of scope; the
judge halted on that gap (``docs/decisions/DR-0010-sse-environment-scoping.md``),
which ratified that the filter survives with v1 semantics unchanged.
``subscribe(environment=...)`` records the authenticated key's scope
(``sse_manager.py:21,23`` -- ``SSEConnection.environment``, ``None`` = unscoped) and
``broadcast(type, data, environment=...)`` accepts the broadcast's origin
environment (``Notify.environment``, resolved at decision time by
``seedpod/core/effects.py``, threaded through by the drain-side effect executor).
The filter itself is ``SSEManager.broadcast``'s guard clause, salvaged VERBATIM
(``sse_manager.py:85``):

    if environment_filter and conn.environment and conn.environment != "all" and conn.environment != environment_filter:
        continue

i.e. a connection is skipped iff the broadcast carries an environment AND the
connection is scoped AND the connection's scope is not ``"all"`` AND the scope does
not match the broadcast's environment. Consequences kept knowingly (DR-0010): an
unscoped broadcast (``environment=None``) reaches every connection regardless of its
scope; an unscoped connection (``environment=None``) receives every broadcast
regardless of its environment; a connection scoped ``"all"`` behaves as unscoped.

**The shutdown dance (salvaged VERBATIM, with one deliberate divergence).** ``close()``
below is ``SSEManager.shutdown()`` (sse_manager.py:165-215), same order of operations,
same reasoning: queue the ``server_shutdown`` message to every subscriber *before*
flipping the closed flag (sse_manager.py:188-205, comment preserved below), because
flipping the flag first would let a generator that polls it between iterations exit
before ever dequeuing the shutdown message it was supposed to deliver. That ordering is
what breaks the uvicorn/SSE deadlock this method exists for: uvicorn's graceful shutdown
waits for in-flight response generators to finish, but an ``EventSource`` generator with
nothing else queued blocks forever on ``queue.get()`` unless something wakes it; queuing
``server_shutdown`` first guarantees every generator wakes with a real message to yield
and a reason to stop, and the ``grace_period`` sleep (sse_manager.py:207-209) gives it
time to actually flush that message to the client before this hub drops the subscriber.
UI-contract §2/obligation 2 depends on the client actually receiving this message (it
switches the client's reconnect backoff to a fixed 15s delay) -- so unlike a regular
``broadcast()`` (which drops on a full queue, this module's deliberate bounding of v1's
unbounded ``await queue.put(...)``), the shutdown enqueue is FORCED: if a stalled
subscriber's queue is already full, ``close()`` evicts that subscriber's oldest queued
event and retries, so ``server_shutdown`` itself is never the thing dropped. v1 never
faced this case (its queue was unbounded, so ``await queue.put(...)`` at
sse_manager.py:29 could not fail) -- forced eviction is this module's bounded-queue
analogue of that guarantee, not a v1 behavior being copied.

**Envelope (obligation 4).** ``broadcast()`` wraps every event in
``{type, data: {...}, timestamp}``, timestamp via the injected ``Clock`` (never
``datetime.utcnow()`` -- v1's ``SSEConnection.send_event``, sse_manager.py:32, is the
naive-UTC-timestamp gotcha ``seedpod/core/clock.py`` exists to retire).

**Payload law (obligation 1) -- NOTE FOR THE EFFECT-EXECUTOR COMPONENT.** UI-contract
obligation 1 pins that ``deployment_status_changed`` payloads MUST carry
``deployment_id``, ``cluster_id``, ``old_status``, ``new_status`` (DeploymentDetail.jsx
filters on ``deployment_id``; its absence silently stops live-updating). This hub is
payload-agnostic by design -- it broadcasts whatever ``data`` mapping it is given,
unchanged, into the envelope's ``data`` key. Payload CONSTRUCTION (assembling that
shape from a ``Notify`` effect drained off ``effects_outbox``) is the effect
executor's responsibility, not this module's; obligation 1 binds that component, not
this one.

**Topics are plain strings, no allowlist.** v1 never validated ``event_type`` against
a fixed set (``SSEManager.broadcast``, sse_manager.py:72-97, takes any string) --
this hub follows suit. The glossary/obligation-5 topic set (``cluster_state_changed``,
``deployment_status_changed``, ``workflow_progress``, ``job_started``,
``job_completed``, ``job_failed``, ``pod_status_changed``,
``snapshot_restore_completed``, ``reconciliation_skipped``, ``server_shutdown``) is
documentation of what callers are expected to send, not something this module
enforces.

**Keepalives are out of scope here.** UI-contract obligation 2 (SSE keepalives
<= 120s apart) is Round 6's events-router job (the component that actually holds the
``EventSource`` response open and reads this hub's per-subscriber queue) -- this hub
only publishes and queues; it has no request/response lifecycle to keep alive.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from seedpod.core.clock import Clock

__all__ = ["SSEHub"]

_DEFAULT_QUEUE_SIZE = 100  # per-subscriber bound -- see module docstring


@dataclass(frozen=True, slots=True)
class _Subscriber:
    """A registered connection: its queue plus the authenticated key's
    environment scope (``None`` = unscoped, ``sse_manager.py:23``)."""

    queue: asyncio.Queue[dict[str, Any]]
    environment: str | None


class SSEHub:
    """``SSEHub(clock, queue_size=100)`` -- in-memory pub/sub, factory-constructed
    (no module global; the composition root owns the one instance and threads it to
    every collaborator that needs to publish or subscribe)."""

    def __init__(self, clock: Clock, queue_size: int = _DEFAULT_QUEUE_SIZE) -> None:
        self._clock = clock
        self._queue_size = queue_size
        self._subscribers: dict[str, _Subscriber] = {}
        self._counter = 0
        self._closed = False

    @property
    def closed(self) -> bool:
        """``True`` once ``close()`` has run. Never flips back -- a closed hub is
        gone for the rest of the process's life, matching v1's ``_is_shutdown``
        (sse_manager.py:48, checked by ``is_shutdown()``)."""
        return self._closed

    def subscriber_count(self) -> int:
        """Number of currently-registered subscribers (v1's
        ``get_connection_count``, sse_manager.py:99-101)."""
        return len(self._subscribers)

    def subscribe(self, environment: str | None = None) -> tuple[str, asyncio.Queue[dict[str, Any]]]:
        """Register a new subscriber and return ``(subscriber_id, queue)``.
        ``environment`` is the authenticated key's scope (``None`` = unscoped,
        ``"all"`` behaves as unscoped -- module docstring), threaded in by the
        Round 6 events router at connect time per DR-0010. The caller owns
        draining ``queue`` for the life of its connection and MUST call
        ``unsubscribe()`` when it's done. Mirrors v1's ``add_connection``
        (sse_manager.py:50-64)."""
        self._counter += 1
        subscriber_id = f"sub_{self._counter}"
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers[subscriber_id] = _Subscriber(queue=queue, environment=environment)
        return subscriber_id, queue

    def unsubscribe(self, subscriber_id: str) -> None:
        """Remove a subscriber. Idempotent -- unsubscribing an unknown or
        already-removed id is a no-op (v1's ``remove_connection``,
        sse_manager.py:66-70, has the same idempotence via ``dict.pop`` with a
        membership check)."""
        self._subscribers.pop(subscriber_id, None)

    def broadcast(self, type: str, data: Mapping[str, Any], environment: str | None = None) -> None:  # noqa: A002 -- "type" is the wire field name
        """Publish ``{type, data: {...}, timestamp}`` (obligation 4) to every
        subscriber whose scope admits ``environment`` (module docstring's
        filter rule, salvaged verbatim from ``sse_manager.py:85``).
        ``environment`` is the broadcast's origin environment (``Notify.environment``,
        ``None`` = unscoped -- reaches everyone). Non-blocking and never raises: a
        subscriber whose queue is full (a stalled consumer) simply drops this event
        (module docstring) -- exactly the property v1's unbounded
        ``await queue.put(...)`` (sse_manager.py:29) did not have."""
        self._publish(self.envelope(type, data), environment=environment)

    def envelope(self, type: str, data: Mapping[str, Any]) -> dict[str, Any]:  # noqa: A002 -- "type" is the wire field name
        """Build an obligation-4 ``{type, data, timestamp}`` envelope on the injected
        ``Clock``. Public because the events router builds ONE envelope this hub does
        not publish -- the idle ``keepalive`` frame, which is per-connection and never
        broadcast (``api/routers/events.py``). Sharing this builder keeps a single
        source of truth for the envelope shape that
        ``test_envelope_shape_matches_obligation_4`` pins."""
        return {"type": type, "data": dict(data), "timestamp": self._clock.now().isoformat()}

    def _publish(self, envelope: dict[str, Any], environment: str | None = None, *, force: bool = False) -> None:
        for subscriber in list(self._subscribers.values()):
            # sse_manager.py:85 verbatim: skip iff the broadcast carries an
            # environment AND the connection is scoped AND the connection's
            # scope isn't "all" AND the scope doesn't match the broadcast.
            if (
                environment
                and subscriber.environment
                and subscriber.environment != "all"
                and subscriber.environment != environment
            ):
                continue
            self._enqueue(subscriber.queue, envelope, force=force)

    @staticmethod
    def _enqueue(queue: asyncio.Queue[dict[str, Any]], envelope: dict[str, Any], *, force: bool) -> None:
        try:
            queue.put_nowait(envelope)
        except asyncio.QueueFull:
            if not force:
                return  # drop-on-full -- see module docstring
            # Forced delivery (close()'s server_shutdown only, module docstring's
            # "shutdown dance" section): evict the oldest queued event to make room,
            # then retry. No other coroutine can run between get_nowait() and
            # put_nowait() here (no `await` in between), so this can't race a
            # concurrent producer/consumer of the same queue.
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            queue.put_nowait(envelope)

    async def close(self, grace_period: float = 2.0) -> None:
        """Gracefully shut the hub down: queue a ``server_shutdown`` event to every
        subscriber, THEN flip ``closed``, THEN wait ``grace_period`` seconds before
        dropping every subscriber. Idempotent (a second ``close()`` is a no-op).

        This ordering is salvaged VERBATIM from ``SSEManager.shutdown()``
        (``reference-code/seedpod/seedpod/core/sse_manager.py:165-215``) -- see the
        module docstring's "shutdown dance" section for why the order (queue first,
        flag second) is load-bearing, not incidental.
        """
        if self._closed:
            return  # sse_manager.py:175-177 -- already shutdown, no-op

        if not self._subscribers:
            # sse_manager.py:179-183 -- nothing to drain, just flip the flag.
            self._closed = True
            return

        # Queue the shutdown event to every subscriber BEFORE flipping the closed
        # flag (sse_manager.py:188-201, comment preserved: "Queue shutdown event to
        # all connections BEFORE setting shutdown flag -- this ensures events are in
        # the queue when streams start draining"). Forced: a stalled subscriber's
        # full queue must not cost it the shutdown message (module docstring's
        # "shutdown dance" section -- this is this module's bounded-queue divergence
        # from v1's unbounded, always-succeeds `await queue.put(...)`).
        self._publish(
            self.envelope(
                "server_shutdown",
                {"message": "Server is shutting down, please reconnect shortly", "grace_period": grace_period},
            ),
            force=True,
        )

        # NOW set the closed flag (sse_manager.py:203-205: "NOW set shutdown flag so
        # streams will start draining").
        self._closed = True

        # Wait for the grace period so clients can actually receive the message
        # before their subscriber is dropped (sse_manager.py:207-209).
        await asyncio.sleep(grace_period)

        # Close all subscribers (sse_manager.py:211-215).
        self._subscribers.clear()
