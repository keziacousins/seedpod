"""``seedpod/runtime/sse.py`` -- ``FrozenClock`` only, no DB (the hub is pure
in-memory pub/sub), no Mock/patch anywhere. Every scenario drives ``SSEHub``'s
public surface (``subscribe``/``broadcast``/``unsubscribe``/``close``) directly.

Covers: subscribe/broadcast/unsubscribe basics; the obligation-4 envelope shape with
a clock-driven timestamp (advancing the ``FrozenClock`` between broadcasts changes
the emitted timestamp, proving it is read at broadcast time, not construction time);
a slow consumer's queue drops the newest event at the bound instead of growing
unbounded or raising, while a healthy sibling subscriber still receives everything;
``close()`` queues ``server_shutdown`` to every subscriber (waking a consumer
blocked on ``queue.get()``) before the grace period elapses, then drains every
subscriber by the time it returns; and the DR-0010 per-connection environment
filter's four pinned cases (scope match delivered, scope mismatch skipped, ``"all"``
scope receives scoped broadcasts, unscoped broadcast reaches scoped connections).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from seedpod.core.clock import FrozenClock
from seedpod.runtime.sse import SSEHub

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# subscribe / broadcast / unsubscribe basics.
# ---------------------------------------------------------------------------


def test_subscribe_broadcast_delivers_to_subscriber():
    hub = SSEHub(FrozenClock(NOW))
    sub_id, queue = hub.subscribe()

    hub.broadcast("cluster_state_changed", {"cluster_id": "c1", "old_status": "active", "new_status": "destroying"})

    assert queue.qsize() == 1
    envelope = queue.get_nowait()
    assert envelope["type"] == "cluster_state_changed"
    assert envelope["data"] == {"cluster_id": "c1", "old_status": "active", "new_status": "destroying"}
    assert sub_id  # non-empty id was handed back


def test_broadcast_fans_out_to_every_current_subscriber():
    hub = SSEHub(FrozenClock(NOW))
    _, queue_a = hub.subscribe()
    _, queue_b = hub.subscribe()

    hub.broadcast("workflow_progress", {"message": "provisioning"})

    assert queue_a.get_nowait()["type"] == "workflow_progress"
    assert queue_b.get_nowait()["type"] == "workflow_progress"


def test_broadcast_with_no_subscribers_is_a_no_op():
    hub = SSEHub(FrozenClock(NOW))
    hub.broadcast("cluster_state_changed", {"cluster_id": "c1"})  # must not raise


def test_unsubscribe_stops_further_delivery():
    hub = SSEHub(FrozenClock(NOW))
    sub_id, queue = hub.subscribe()
    assert hub.subscriber_count() == 1

    hub.unsubscribe(sub_id)
    assert hub.subscriber_count() == 0

    hub.broadcast("cluster_state_changed", {"cluster_id": "c1"})
    assert queue.qsize() == 0  # nothing delivered -- subscriber is gone


def test_unsubscribe_is_idempotent_and_tolerates_unknown_id():
    hub = SSEHub(FrozenClock(NOW))
    sub_id, _ = hub.subscribe()

    hub.unsubscribe(sub_id)
    hub.unsubscribe(sub_id)  # second call: no-op, no error
    hub.unsubscribe("never-registered")  # unknown id: no-op, no error

    assert hub.subscriber_count() == 0


def test_multiple_subscribers_get_distinct_ids_and_queues():
    hub = SSEHub(FrozenClock(NOW))
    id_a, queue_a = hub.subscribe()
    id_b, queue_b = hub.subscribe()

    assert id_a != id_b
    assert queue_a is not queue_b


# ---------------------------------------------------------------------------
# Envelope shape (obligation 4) + clock-driven timestamp.
# ---------------------------------------------------------------------------


def test_envelope_shape_matches_obligation_4():
    hub = SSEHub(FrozenClock(NOW))
    _, queue = hub.subscribe()

    hub.broadcast("deployment_status_changed", {"deployment_id": "d1", "cluster_id": "c1"})

    envelope = queue.get_nowait()
    assert set(envelope.keys()) == {"type", "data", "timestamp"}
    assert envelope["timestamp"] == NOW.isoformat()


def test_timestamp_is_read_from_the_clock_at_broadcast_time_not_construction_time():
    clock = FrozenClock(NOW)
    hub = SSEHub(clock)
    _, queue = hub.subscribe()

    hub.broadcast("cluster_state_changed", {"cluster_id": "c1"})
    first = queue.get_nowait()
    assert first["timestamp"] == NOW.isoformat()

    later = NOW + timedelta(seconds=30)
    clock.set(later)
    hub.broadcast("cluster_state_changed", {"cluster_id": "c1"})
    second = queue.get_nowait()
    assert second["timestamp"] == later.isoformat()
    assert second["timestamp"] != first["timestamp"]


def test_broadcast_data_mapping_is_copied_not_aliased():
    hub = SSEHub(FrozenClock(NOW))
    _, queue = hub.subscribe()

    payload = {"cluster_id": "c1"}
    hub.broadcast("cluster_state_changed", payload)
    payload["cluster_id"] = "mutated-after-broadcast"

    envelope = queue.get_nowait()
    assert envelope["data"] == {"cluster_id": "c1"}  # unaffected by the caller's later mutation


# ---------------------------------------------------------------------------
# Slow-consumer drop at the bound: no unbounded growth, no exception.
# ---------------------------------------------------------------------------


def test_slow_consumer_drops_at_the_bound_without_raising_or_growing_unbounded():
    hub = SSEHub(FrozenClock(NOW), queue_size=3)
    _, slow_queue = hub.subscribe()

    for i in range(10):
        hub.broadcast("workflow_progress", {"seq": i})  # never raises even once full

    assert slow_queue.qsize() == 3  # bounded -- did not grow past queue_size
    # The oldest 3 events (drop-newest-on-full via put_nowait) survive, in order.
    delivered = [slow_queue.get_nowait()["data"]["seq"] for _ in range(3)]
    assert delivered == [0, 1, 2]


def test_slow_consumer_drop_does_not_affect_a_healthy_sibling_subscriber():
    hub = SSEHub(FrozenClock(NOW), queue_size=2)
    _, slow_queue = hub.subscribe()
    _, healthy_queue = hub.subscribe()

    received = []
    for i in range(5):
        hub.broadcast("workflow_progress", {"seq": i})
        # Healthy subscriber drains promptly after every broadcast -- never fills up,
        # so it must receive EVERY event despite the slow sibling's queue being full.
        received.append(healthy_queue.get_nowait()["data"]["seq"])

    assert slow_queue.qsize() == 2  # bounded, dropped the rest
    assert received == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# close(grace_period): wakes subscribers with server_shutdown, drains within grace.
# ---------------------------------------------------------------------------


async def test_close_queues_server_shutdown_before_grace_period_elapses():
    hub = SSEHub(FrozenClock(NOW))
    _, queue = hub.subscribe()

    close_task = asyncio.ensure_future(hub.close(grace_period=0.05))

    # "Wakes" a subscriber blocked on queue.get(): the shutdown message must already
    # be queued well before the grace period elapses, per the module's ordering
    # (queue THEN flag THEN sleep) -- not only after close() fully returns.
    envelope = await asyncio.wait_for(queue.get(), timeout=0.03)
    assert envelope["type"] == "server_shutdown"
    assert envelope["data"]["message"]
    assert envelope["data"]["grace_period"] == 0.05
    assert envelope["timestamp"] == NOW.isoformat()

    await close_task


async def test_close_drains_every_subscriber_by_the_time_it_returns():
    hub = SSEHub(FrozenClock(NOW))
    hub.subscribe()
    hub.subscribe()
    assert hub.subscriber_count() == 2

    await hub.close(grace_period=0.01)

    assert hub.subscriber_count() == 0
    assert hub.closed is True


async def test_close_with_no_subscribers_flips_closed_immediately_without_waiting():
    hub = SSEHub(FrozenClock(NOW))

    loop = asyncio.get_running_loop()
    start = loop.time()
    await hub.close(grace_period=5.0)  # would take 5s if it fell through to the sleep
    elapsed = loop.time() - start

    assert hub.closed is True
    assert elapsed < 1.0  # sse_manager.py:179-183's early-return path, no grace sleep


async def test_close_is_idempotent():
    hub = SSEHub(FrozenClock(NOW))
    hub.subscribe()

    await hub.close(grace_period=0.01)
    assert hub.closed is True

    await hub.close(grace_period=0.01)  # second close: no-op, no error, no hang
    assert hub.closed is True


async def test_broadcast_after_close_does_not_raise():
    hub = SSEHub(FrozenClock(NOW))
    hub.subscribe()

    await hub.close(grace_period=0.01)

    hub.broadcast("cluster_state_changed", {"cluster_id": "c1"})  # no subscribers left, must not raise


async def test_close_force_delivers_server_shutdown_even_to_a_full_queue():
    hub = SSEHub(FrozenClock(NOW), queue_size=2)
    _, queue = hub.subscribe()

    # Fill the stalled subscriber's queue to its bound -- a regular broadcast() would
    # now drop silently (drop-on-full), which close()'s server_shutdown must not do.
    hub.broadcast("workflow_progress", {"seq": 0})
    hub.broadcast("workflow_progress", {"seq": 1})
    assert queue.full()

    await hub.close(grace_period=0.01)

    # server_shutdown must be present despite the full queue -- close() force-enqueues
    # it by evicting the oldest queued event (seq 0) rather than dropping the
    # shutdown message. Bound stays 2: [seq 1, server_shutdown].
    types = []
    while not queue.empty():
        types.append(queue.get_nowait()["type"])
    assert "server_shutdown" in types
    assert types == ["workflow_progress", "server_shutdown"]


async def test_close_delivers_server_shutdown_to_environment_scoped_subscribers():
    # DR-0010 "Alternatives considered" pins server_shutdown delivery to SCOPED
    # connections as load-bearing (strict scoping was rejected specifically because
    # it would break server_shutdown for scoped connections -- v1 delivered it to
    # everyone and the SPA's reconnect logic depends on it). close() must keep
    # publishing shutdown as an unscoped broadcast so scoped subscribers still get it.
    hub = SSEHub(FrozenClock(NOW))
    _, staging_queue = hub.subscribe(environment="staging")

    await hub.close(grace_period=0.01)

    assert staging_queue.qsize() == 1
    assert staging_queue.get_nowait()["type"] == "server_shutdown"


# ---------------------------------------------------------------------------
# DR-0010: per-connection environment scoping, v1 filter rule (sse_manager.py:85)
# salvaged verbatim. Four pinned cases.
# ---------------------------------------------------------------------------


def test_scope_match_is_delivered():
    hub = SSEHub(FrozenClock(NOW))
    _, queue = hub.subscribe(environment="staging")

    hub.broadcast("cluster_state_changed", {"cluster_id": "c1"}, environment="staging")

    assert queue.qsize() == 1
    assert queue.get_nowait()["type"] == "cluster_state_changed"


def test_scope_mismatch_is_skipped():
    hub = SSEHub(FrozenClock(NOW))
    _, queue = hub.subscribe(environment="staging")

    hub.broadcast("cluster_state_changed", {"cluster_id": "c1"}, environment="production")

    assert queue.qsize() == 0  # scoped to "staging", broadcast was "production" -- skipped


def test_all_scope_receives_scoped_broadcasts():
    hub = SSEHub(FrozenClock(NOW))
    _, queue = hub.subscribe(environment="all")

    hub.broadcast("cluster_state_changed", {"cluster_id": "c1"}, environment="production")

    assert queue.qsize() == 1  # "all" behaves as unscoped -- receives everything
    assert queue.get_nowait()["data"] == {"cluster_id": "c1"}


def test_unscoped_broadcast_reaches_scoped_connections():
    hub = SSEHub(FrozenClock(NOW))
    _, staging_queue = hub.subscribe(environment="staging")
    _, unscoped_queue = hub.subscribe(environment=None)

    hub.broadcast("workflow_progress", {"message": "provisioning"})  # environment=None (unscoped)

    assert staging_queue.qsize() == 1  # unscoped broadcasts reach every connection
    assert unscoped_queue.qsize() == 1


def test_unscoped_connection_receives_scoped_broadcasts():
    hub = SSEHub(FrozenClock(NOW))
    _, queue = hub.subscribe(environment=None)

    hub.broadcast("cluster_state_changed", {"cluster_id": "c1"}, environment="production")

    assert queue.qsize() == 1  # unscoped connections receive everything regardless of scope
