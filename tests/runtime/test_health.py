"""``seedpod/runtime/health.py`` -- ``HealthMonitor``. Real tmp SQLite
(``tests/runtime/conftest.py``'s ``db``/``uow``/``repos``/``dispatcher``/``clock``
fixtures), a real ``CryptoService`` (never Mock/patch, CLAUDE.md), and hand-built fake
probes: a fake ``execute()`` returns a healthy ``Result`` or RAISES a typed error.

Covers: healthy resets the counter (and is a no-op when already 0); transient
increments below threshold with no state change; transient threshold breach emits
``HealthCheckFailed`` -> FAILED and resets the counter; permanent fails immediately
regardless of counter; the health analog of crown jewel #1 (an
``InfrastructureUnreachableError``-raising probe neither increments nor fails --
fault-injected); a cluster that left ACTIVE during the probe's no-tx window loses the
CAS and is dropped, not failed (StaleVersion re-decide, real race via a legitimate
concurrent write during the fake probe's ``execute()``); the re-decide loop's 3-attempt
bound (a hand-built always-stale fake dispatcher, CLAUDE.md-permitted -- forcing >1
real consecutive CAS losses needs genuine concurrent writers a single-threaded test
can't interleave); the counter persists across ticks; DR-0008 proven structurally (the
fake probe asserts no open transaction); the immediate first tick; and a clean
stop().
"""

from __future__ import annotations

import dataclasses
import logging

import pytest
from cryptography.fernet import Fernet

from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    TransientError,
)
from seedpod.core.machine import StaleVersion
from seedpod.core.records import ClusterState
from seedpod.providers.contract import KubeGetClusterInfo, Result
from seedpod.runtime.health import HealthMonitor
from seedpod.services.crypto import CryptoService
from tests.runtime.conftest import NOW, make_cluster_row

pytestmark = pytest.mark.asyncio

_INTERVAL = 10_000.0  # never let the periodic loop fire during a test's own await


@pytest.fixture
def crypto() -> CryptoService:
    return CryptoService(dev_key=Fernet.generate_key(), prod_key=Fernet.generate_key())


def _kubeconfig_row(crypto: CryptoService, cluster_id: str, slug: str, **overrides) -> tuple:
    """A cluster row with a real, decryptable ``encrypted_kubeconfig`` under the DEV
    key -- ``environment`` defaults to something DEV-mapped so a caller who also
    wants to encrypt via ``crypto.key_class_for_environment`` gets the same class."""
    plaintext = f"kubeconfig-for-{cluster_id}"
    overrides.setdefault("environment", "ephemeral")
    row = make_cluster_row(
        cluster_id,
        slug,
        status="active",
        encrypted_kubeconfig=crypto.encrypt(plaintext, "DEV"),
        kubeconfig_key_class="DEV",
        **overrides,
    )
    return row, plaintext


class FakeProbe:
    """Hand-built ``Provider``-shaped double (CLAUDE.md: no Mock/patch anywhere) for
    ``KubeGetClusterInfo``. Returns a healthy ``Result`` or RAISES a typed error;
    optionally asserts the DR-0008 "no transaction open" invariant and/or runs a
    caller-supplied side effect (used to simulate a genuinely concurrent writer
    racing the probe's real IO gap) before deciding the outcome."""

    def __init__(
        self,
        *,
        raise_exc: Exception | None = None,
        assert_no_tx=None,
        side_effect=None,
    ) -> None:
        self.name = "kubectl"
        self.supported = frozenset({KubeGetClusterInfo})
        self._raise_exc = raise_exc
        self._assert_no_tx = assert_no_tx
        self._side_effect = side_effect
        self.calls: list[KubeGetClusterInfo] = []

    async def check_ready(self) -> None:
        return None

    async def execute(self, cmd):
        self.calls.append(cmd)
        if self._assert_no_tx is not None:
            self._assert_no_tx()
        if self._side_effect is not None:
            await self._side_effect()
        if self._raise_exc is not None:
            raise self._raise_exc
        yield Result("Kubernetes control plane is running")


class AlwaysStaleDispatcher:
    """Hand-built fake matching ``Dispatcher.apply()``'s call shape -- raises
    ``StaleVersion`` on every call. Proves the retry loop is bounded to 3 attempts
    without needing genuine concurrent writers to interleave more than once inside a
    single-threaded test (CLAUDE.md: hand-built fake, never Mock/patch)."""

    def __init__(self) -> None:
        self.calls = 0

    async def apply(self, aggregate, aggregate_id, event, *, tx=None, record=None, **kw):
        self.calls += 1
        raise StaleVersion("fake: always loses the CAS")


def _monitor(kubectl, crypto, repos, dispatcher, uow, clock, **kw) -> HealthMonitor:
    return HealthMonitor(kubectl, crypto, repos, dispatcher, uow, clock, interval=_INTERVAL, **kw)


# ---------------------------------------------------------------------------
# Healthy
# ---------------------------------------------------------------------------


async def test_healthy_resets_nonzero_counter(uow, repos, dispatcher, clock, crypto):
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=2)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    probe = FakeProbe()
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock)
    await monitor.tick()

    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
    assert after.consecutive_health_failures == 0
    assert after.status == "active"


async def test_healthy_noop_when_counter_already_zero(uow, repos, dispatcher, clock, crypto):
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=0)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    probe = FakeProbe()
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock)
    await monitor.tick()

    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
    assert after.consecutive_health_failures == 0
    assert after.updated_at == NOW  # untouched -- no write happened at all


# ---------------------------------------------------------------------------
# Transient
# ---------------------------------------------------------------------------


async def test_transient_increments_below_threshold(uow, repos, dispatcher, clock, crypto):
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=0)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    exc = TransientError("api timeout", code=ErrorCode.API_TIMEOUT, provider="kubectl")
    probe = FakeProbe(raise_exc=exc)
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock, max_transient_failures=3)
    await monitor.tick()

    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
    assert after.consecutive_health_failures == 1
    assert after.status == "active"  # no state change


async def test_transient_threshold_breach_fails_and_resets_counter(uow, repos, dispatcher, clock, crypto):
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=2)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    exc = TransientError("endpoint unreachable", code=ErrorCode.ENDPOINT_UNREACHABLE, provider="kubectl")
    probe = FakeProbe(raise_exc=exc)
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock, max_transient_failures=3)
    await monitor.tick()

    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
        audits = repos.cluster_state_audits.list_for_cluster(tx, "c1")
    assert after.status == "failed"
    assert after.consecutive_health_failures == 0
    assert "3 consecutive transient failures" in after.failure_reason
    assert len(audits) == 1
    assert audits[0].event == "HealthCheckFailed"


# ---------------------------------------------------------------------------
# Permanent
# ---------------------------------------------------------------------------


async def test_permanent_fails_immediately_regardless_of_counter(uow, repos, dispatcher, clock, crypto):
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=0)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    exc = PermanentError("auth failed", code=ErrorCode.AUTH, provider="kubectl")
    probe = FakeProbe(raise_exc=exc)
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock)
    await monitor.tick()

    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
    assert after.status == "failed"
    assert after.consecutive_health_failures == 0
    assert "permanent" in after.failure_reason


# ---------------------------------------------------------------------------
# Crown jewel #1's health analog: unreachable != unhealthy
# ---------------------------------------------------------------------------


async def test_unreachable_neither_increments_nor_fails(uow, repos, dispatcher, clock, crypto):
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=1)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    exc = InfrastructureUnreachableError(
        "control plane outage", code=ErrorCode.API_TIMEOUT, provider="kubectl", host="1.2.3.4"
    )
    probe = FakeProbe(raise_exc=exc)
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock)
    await monitor.tick()

    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
    assert after.status == "active"  # never failed
    assert after.consecutive_health_failures == 1  # untouched -- not incremented


# ---------------------------------------------------------------------------
# No captured kubeconfig -- treated the same as "cannot determine health"
# ---------------------------------------------------------------------------


async def test_no_kubeconfig_skips_without_touching_cluster(uow, repos, dispatcher, clock, crypto):
    row = make_cluster_row("c1", "demo", status="active", consecutive_health_failures=1)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    probe = FakeProbe()  # never called -- resolving kubeconfig fails first
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock)
    await monitor.tick()

    assert probe.calls == []
    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
    assert after.status == "active"
    assert after.consecutive_health_failures == 1


# ---------------------------------------------------------------------------
# StaleVersion / mid-redeploy safety
# ---------------------------------------------------------------------------


async def test_cluster_left_active_during_probe_is_dropped_not_failed(
    uow, repos, dispatcher, clock, crypto, caplog
):
    """A genuine race: another writer moves the cluster off ACTIVE during the probe's
    real no-tx IO window (DR-0008) -- simulated here as a legitimate concurrent CAS
    write inside the fake probe's ``execute()``, exactly where such a race would land
    in production. The health monitor's own ``dispatcher.apply(record=<the
    tick-start row>)`` then genuinely loses the CAS (a real, unmocked
    ``StaleVersion``), re-reads, sees the fresh row is no longer ACTIVE, and drops
    the ``HealthCheckFailed`` without failing the cluster."""
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=0)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    async def _concurrent_redeploy_started():
        async with uow() as tx:
            record = repos.clusters.load(tx, "c1")
            moved = dataclasses.replace(record, state=ClusterState.DESTROYING, version=record.version)
            repos.clusters.persist(tx, moved, record.version, clock=clock)

    exc = PermanentError("auth failed", code=ErrorCode.AUTH, provider="kubectl")
    probe = FakeProbe(raise_exc=exc, side_effect=_concurrent_redeploy_started)
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock)

    with caplog.at_level(logging.INFO, logger="seedpod.runtime.health"):
        await monitor.tick()

    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
        audits = repos.cluster_state_audits.list_for_cluster(tx, "c1")
    assert after.status == "destroying"  # the concurrent writer's transition stands
    assert after.failure_reason is None  # HealthCheckFailed never landed
    assert not any(a.event == "HealthCheckFailed" for a in audits)
    assert any("dropped, not failed" in r.message or "dropped" in r.message for r in caplog.records)


async def test_stale_version_retry_bounded_to_three_attempts(uow, repos, dispatcher, clock, crypto, caplog):
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=0)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    exc = PermanentError("auth failed", code=ErrorCode.AUTH, provider="kubectl")
    probe = FakeProbe(raise_exc=exc)
    fake_dispatcher = AlwaysStaleDispatcher()
    monitor = _monitor(probe, crypto, repos, fake_dispatcher, uow, clock)

    with caplog.at_level(logging.ERROR, logger="seedpod.runtime.health"):
        await monitor.tick()  # must not raise, must not loop forever

    assert fake_dispatcher.calls == 3
    assert any("lost the CAS 3 times" in r.message for r in caplog.records)
    # The row is still ACTIVE (a fake dispatcher touches nothing real) -- next
    # tick's probe picks the same cluster right back up, same discipline as
    # TimerService/ReconciliationService's own per-row isolation.
    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
    assert after.status == "active"


# ---------------------------------------------------------------------------
# Counter persistence across ticks
# ---------------------------------------------------------------------------


async def test_counter_persists_across_ticks(uow, repos, dispatcher, clock, crypto):
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=0)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    exc = TransientError("api timeout", code=ErrorCode.API_TIMEOUT, provider="kubectl")
    probe = FakeProbe(raise_exc=exc)
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock, max_transient_failures=5)

    await monitor.tick()
    await monitor.tick()

    async with uow() as tx:
        after = repos.clusters.get(tx, "c1")
    assert after.consecutive_health_failures == 2


# ---------------------------------------------------------------------------
# DR-0008 structural proof
# ---------------------------------------------------------------------------


async def test_probe_runs_with_no_transaction_open(uow, repos, dispatcher, clock, crypto):
    row, _ = _kubeconfig_row(crypto, "c1", "demo")
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    def _assert_unlocked():
        assert not uow._lock.locked(), "DR-0008: probe ran with a transaction open"

    probe = FakeProbe(assert_no_tx=_assert_unlocked)
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock)
    await monitor.tick()

    assert len(probe.calls) == 1


# ---------------------------------------------------------------------------
# Liveness: immediate first tick, clean stop()
# ---------------------------------------------------------------------------


async def test_start_runs_immediate_first_tick_before_returning(uow, repos, dispatcher, clock, crypto):
    row, _ = _kubeconfig_row(crypto, "c1", "demo", consecutive_health_failures=1)
    async with uow() as tx:
        repos.clusters.insert(tx, row)

    probe = FakeProbe()
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock)
    assert monitor.last_sync() is None

    await monitor.start()
    try:
        assert monitor.running
        assert monitor.last_sync() is not None
        async with uow() as tx:
            after = repos.clusters.get(tx, "c1")
        assert after.consecutive_health_failures == 0  # the immediate tick already ran
    finally:
        await monitor.stop()


async def test_stop_is_clean_and_idempotent(uow, repos, dispatcher, clock, crypto):
    probe = FakeProbe()
    monitor = _monitor(probe, crypto, repos, dispatcher, uow, clock)

    await monitor.start()
    await monitor.stop()
    assert not monitor.running

    await monitor.stop()  # idempotent, no error
    assert not monitor.running
