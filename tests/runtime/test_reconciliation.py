"""``seedpod/runtime/reconciliation.py`` -- ``ReconciliationService``. Real tmp
SQLite (``tests/runtime/conftest.py``'s ``db``/``uow``/``repos``/``dispatcher``/
``clock`` fixtures), hand-built fake providers (never Mock/patch, CLAUDE.md): a fake
provider returns intents or RAISES a typed error.

Covers: crown jewel #1 (an unreachable provider yields ZERO intents + a
``reconciliation_skipped`` broadcast per covered cluster, environment-scoped, while a
healthy second provider still reconciles the SAME tick); DR-0012's zombie two-step
(detect -> ZOMBIE, sweep -> DESTROY_SCHEDULED, two ``reconciler``-actored audits;
crash-after-Phase-1 leaves a stranded ZOMBIE the next tick's sweep cleans up; a
ZOMBIE adopted before the sweep is not re-destroyed; a discovered-origin zombie is
destroyed only because the reconciler sets ``force``); OrphanIntent ->
``InfraMissingObserved`` -> DESTROYED + Casc-gone; CreateUnmanagedIntent -> a
``Discovered`` birth to UNMANAGED with the observed fields landing (DR-0006), even
for a provider with zero pre-existing DB clusters, and carrying
``environment="production"`` (DR-0013) -- proven both on the stored row and,
end-to-end through a real ``SSEHub``/``EffectExecutor`` drain of the birth's
``effects_outbox`` ``Notify``, that DR-0010's per-connection filter actually applies
to it (a non-matching-env connection does not receive the discovered cluster's
``cluster_state_changed``; the matching-env and ``'all'``-scoped connections do);
StatusSyncIntent is a periodic no-op; priority ordering; destructive-intent
suppression for a cluster with a blocked run (OrphanIntent's single apply, and
DR-0014's placement at the Phase-2 ZOMBIE sweep rather than Phase-1 detection --
detection still promotes a run-bearing cluster to ZOMBIE for visibility, and a
ZOMBIE that acquires a blocked run is swept only once the run reaches terminal);
``last_reconciled_at`` set on success but not on skip; the immediate first tick; and
DR-0008 structurally (a fake provider asserts no transaction is open when its
``Reconcile`` is invoked).
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import ErrorCode, InfrastructureUnreachableError
from seedpod.core.events import AdoptRequested
from seedpod.core.reconciliation_intents import (
    CreateUnmanagedIntent,
    OrphanIntent,
    StatusSyncIntent,
    ZombieIntent,
)
from seedpod.core.records import Origin
from seedpod.engine.dispatch_table import WorkflowDispatch
from seedpod.providers.contract import Reconcile, Result
from seedpod.runtime.effect_executor import EffectExecutor
from seedpod.runtime.reconciliation import ReconciliationService
from seedpod.runtime.sse import SSEHub
from tests.runtime.conftest import (
    NOW,
    FakeEngine,
    make_cluster_row,
    make_deployment_row,
    make_run_row,
)

pytestmark = pytest.mark.asyncio

_INTERVAL = 10_000.0  # never let the periodic loop fire during a test's own await


class FakeProvider:
    """Hand-built ``Provider`` double (CLAUDE.md: no Mock/patch anywhere) --
    ``execute()`` yields the configured intents as a ``Reconcile`` ``Result``, or
    RAISES a typed error. Records every ``Reconcile`` command it was handed and can
    assert the DR-0008 "no transaction open" invariant at the instant it runs."""

    def __init__(
        self,
        name: str,
        intents: tuple = (),
        *,
        raise_unreachable: bool = False,
        raise_other: bool = False,
        assert_no_tx=None,
    ) -> None:
        self.name = name
        self.supported = frozenset({Reconcile})
        self._intents = intents
        self._raise_unreachable = raise_unreachable
        self._raise_other = raise_other
        self._assert_no_tx = assert_no_tx
        self.calls: list[Reconcile] = []

    async def check_ready(self) -> None:
        return None

    async def execute(self, cmd):
        self.calls.append(cmd)
        if self._assert_no_tx is not None:
            self._assert_no_tx()
        if self._raise_unreachable:
            raise InfrastructureUnreachableError(
                "fake outage", code=ErrorCode.API_TIMEOUT, host="fake.example"
            )
        if self._raise_other:
            raise RuntimeError("fake provider blew up")
        yield Result(tuple(self._intents))


def _service(providers, repos, dispatcher, uow, hub, clock, **kw) -> ReconciliationService:
    return ReconciliationService(
        providers, repos, dispatcher, engine=None, uow=uow, hub=hub, clock=clock, interval=_INTERVAL, **kw
    )


# ---------------------------------------------------------------------------
# Crown jewel #1
# ---------------------------------------------------------------------------


async def test_unreachable_provider_skipped_zero_intents_healthy_provider_still_reconciles(
    uow, repos, dispatcher, hub, clock
):
    async with uow() as tx:
        repos.clusters.insert(
            tx, make_cluster_row("cA", "flaky-slug", status="active", provider="flaky", environment="staging")
        )
        repos.clusters.insert(
            tx, make_cluster_row("cB", "steady-slug", status="active", provider="steady", environment="ephemeral")
        )

    flaky = FakeProvider("flaky", raise_unreachable=True)
    steady = FakeProvider("steady", intents=(OrphanIntent(cluster_id="cB"),))
    service = _service({"flaky": flaky, "steady": steady}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        row_a = repos.clusters.get(tx, "cA")
        row_b = repos.clusters.get(tx, "cB")
    assert row_a.status == "active"  # untouched -- zero intents from the skipped provider
    assert row_a.last_reconciled_at is None  # not stamped -- this provider's pass was skipped
    assert row_b.status == "destroyed"  # the healthy provider's Orphan still applied, same tick
    assert row_b.last_reconciled_at is not None

    skipped = [c for c in hub.calls if c[0] == "reconciliation_skipped"]
    assert len(skipped) == 1
    _, payload, environment = skipped[0]
    assert payload["cluster_id"] == "cA"
    assert payload["provider"] == "flaky"
    assert environment == "staging"  # DR-0010: scoped from the cluster's own environment


async def test_other_exception_from_provider_also_skips_its_intents(uow, repos, dispatcher, hub, clock):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("cX", "x-slug", status="active", provider="broken"))
    broken = FakeProvider("broken", raise_other=True)
    service = _service({"broken": broken}, repos, dispatcher, uow, hub, clock)

    await service.tick()  # must not raise -- logged and recorded, not fatal to the pass

    async with uow() as tx:
        row = repos.clusters.get(tx, "cX")
    assert row.status == "active"
    assert row.last_reconciled_at is None
    assert not any(c[0] == "reconciliation_skipped" for c in hub.calls)  # not the Unreachable path


# ---------------------------------------------------------------------------
# DR-0008 structural proof
# ---------------------------------------------------------------------------


async def test_provider_io_runs_with_no_transaction_open(uow, repos, dispatcher, hub, clock):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("cL", "lock-slug", status="active", provider="lockcheck"))

    def _assert_unlocked() -> None:
        assert not uow._lock.locked(), "DR-0008: provider Reconcile ran with a transaction open"

    provider = FakeProvider("lockcheck", assert_no_tx=_assert_unlocked)
    service = _service({"lockcheck": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    assert len(provider.calls) == 1  # the assertion inside execute() actually ran


# ---------------------------------------------------------------------------
# DR-0012 zombie two-step
# ---------------------------------------------------------------------------


async def test_zombie_two_step_two_reconciler_audits_in_one_tick(uow, repos, dispatcher, hub, clock):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("cz", "zslug", status="destroyed", provider="fake"))
    provider = FakeProvider(
        "fake", intents=(ZombieIntent(cluster_id="cz", droplet_id="d1", droplet_ip="1.2.3.4"),)
    )
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "cz")
        audits = repos.cluster_state_audits.list_for_cluster(tx, "cz")
    assert row.status == "destroy-scheduled"
    assert row.pre_destroy_state == "zombie"
    ordered = sorted(audits, key=lambda a: a.id)
    assert [(a.from_state, a.to_state, a.actor) for a in ordered] == [
        ("destroyed", "zombie", "reconciler"),
        ("zombie", "destroy-scheduled", "reconciler"),
    ]


async def test_zombie_discovered_origin_destroyed_only_because_reconciler_sets_force(
    uow, repos, dispatcher, hub, clock
):
    async with uow() as tx:
        repos.clusters.insert(
            tx, make_cluster_row("cz2", "zslug2", status="destroyed", provider="fake", origin=Origin.DISCOVERED)
        )
    provider = FakeProvider("fake", intents=(ZombieIntent(cluster_id="cz2", droplet_id="d2"),))
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "cz2")
    # Would raise InvalidTransition (and be swallowed/logged, leaving status="zombie")
    # if the reconciler failed to set force=True for a discovered-origin record.
    assert row.status == "destroy-scheduled"


async def test_crash_after_phase1_stranded_zombie_cleaned_by_next_ticks_sweep(uow, repos, dispatcher, hub, clock):
    # Simulates a prior pass that crashed between Phase 1 and Phase 2: the row is
    # already sitting in ZOMBIE, as InfraRunningObserved would have left it.
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("cz3", "zslug3", status="zombie", provider="fake"))
    provider = FakeProvider("fake")  # this pass's Reconcile sees status="zombie", emits nothing
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "cz3")
    assert row.status == "destroy-scheduled"  # the sweep alone found and cleaned it


async def test_zombie_adopted_before_sweep_not_redestroyed(uow, repos, dispatcher, hub, clock):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("cz4", "zslug4", status="zombie", provider="fake"))
    await dispatcher.apply("cluster", "cz4", AdoptRequested(at=NOW, actor="api:op"))  # operator adopts first

    provider = FakeProvider("fake")
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)
    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "cz4")
    assert row.status == "active"  # sweep re-read fresh state and found it already ACTIVE


# ---------------------------------------------------------------------------
# OrphanIntent
# ---------------------------------------------------------------------------


async def test_orphan_intent_destroys_and_cascades_gone(uow, repos, dispatcher, hub, clock):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("co", "oslug", status="active", provider="fake"))
        repos.deployments.insert(tx, make_deployment_row("do1", "co", status="pending"))
    provider = FakeProvider("fake", intents=(OrphanIntent(cluster_id="co"),))
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        crow = repos.clusters.get(tx, "co")
        drow = repos.deployments.get(tx, "do1")
    assert crow.status == "destroyed"
    assert drow.status == "destroyed"  # Casc-gone: ClusterGone fanned out from the machine


# ---------------------------------------------------------------------------
# CreateUnmanagedIntent
# ---------------------------------------------------------------------------


async def test_create_unmanaged_birth_to_unmanaged_with_observed_fields(uow, repos, dispatcher, hub, clock):
    """Proves the reconciler calls a REGISTERED provider even with zero
    pre-existing DB clusters -- otherwise a provider's very first untracked
    droplet could never be discovered (module docstring)."""
    droplet = {"id": 999, "region": {"slug": "nyc3"}, "size_slug": "s-2vcpu-4gb"}
    provider = FakeProvider(
        "digitalocean", intents=(CreateUnmanagedIntent(cluster_id="cnew", droplet=droplet, slug="wild-one"),)
    )
    service = _service({"digitalocean": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    assert provider.calls[0].clusters == ()  # zero DB clusters for this provider going in
    async with uow() as tx:
        row = repos.clusters.get(tx, "cnew")
    assert row.status == "unmanaged"
    assert row.origin == Origin.DISCOVERED
    assert row.provider == "digitalocean"
    assert row.slug == "wild-one"
    assert row.provider_resources == {"droplet_id": "999"}
    assert row.provider_config == {"droplet_id": "999", "region": "nyc3", "size": "s-2vcpu-4gb"}
    assert row.environment == "production"  # DR-0013: the sole ratified default, no unscoped option


async def test_create_unmanaged_environment_scopes_sse_delivery_dr0010(uow, repos, dispatcher, clock):
    """DR-0013's own consequence, restated: the discovered birth row's
    ``environment="production"`` is not just a stored value -- it is what DR-0010's
    filter reads to decide who gets the birth's ``cluster_state_changed``. Proven
    end-to-end: a real ``SSEHub`` (not the ``FakeHub`` the other tests use) fed by a
    real ``EffectExecutor`` drain of the ``Dispatcher``-written ``effects_outbox``, so
    the scoping actually observed is the same code path production runs, not an
    assertion on the stored row alone."""
    droplet = {"id": 5, "region": {"slug": "nyc3"}, "size_slug": "s-1vcpu-1gb"}
    provider = FakeProvider(
        "digitalocean", intents=(CreateUnmanagedIntent(cluster_id="c-scoped", droplet=droplet, slug="wild-scoped"),)
    )
    sse_hub = SSEHub(clock)
    service = _service({"digitalocean": provider}, repos, dispatcher, uow, sse_hub, clock)

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "c-scoped")
    assert row.environment == "production"

    # Drain the birth's Notify(topic="cluster_state_changed", environment="production")
    # off effects_outbox through a real EffectExecutor into the real SSEHub, then prove
    # DR-0010's per-connection filter: only the matching-env and 'all'-scoped
    # connections receive it; a differently-scoped connection does not.
    _, staging_queue = sse_hub.subscribe(environment="staging")
    _, production_queue = sse_hub.subscribe(environment="production")
    _, all_queue = sse_hub.subscribe(environment="all")
    engine = FakeEngine(uow, repos.workflow_runs)
    dispatch = WorkflowDispatch(destroy_by_provider={})
    executor = EffectExecutor(uow, repos, sse_hub, engine, dispatch, clock, poll_interval=0.02)

    await executor.drain_pending()

    assert staging_queue.qsize() == 0  # non-matching env scope: does NOT receive the discovered birth
    assert production_queue.qsize() == 1
    assert production_queue.get_nowait()["type"] == "cluster_state_changed"
    assert all_queue.qsize() == 1  # 'all' behaves as unscoped -- receives it regardless


async def test_create_unmanaged_falls_back_to_generated_slug(uow, repos, dispatcher, hub, clock):
    droplet = {"id": 42, "region": {"slug": "sfo3"}, "size_slug": "s-1vcpu-1gb"}
    provider = FakeProvider(
        "digitalocean", intents=(CreateUnmanagedIntent(cluster_id="c-noslug-1234", droplet=droplet, slug=None),)
    )
    service = _service({"digitalocean": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "c-noslug-1234")
    assert row.slug == "discovered-c-noslug"


# ---------------------------------------------------------------------------
# StatusSyncIntent -- periodic no-op
# ---------------------------------------------------------------------------


async def test_status_sync_intent_is_a_periodic_noop(uow, repos, dispatcher, hub, clock):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("cs", "sslug", status="destroy-failed", provider="fake"))
    provider = FakeProvider(
        "fake", intents=(StatusSyncIntent(cluster_id="cs", from_status="destroy-failed", to_status="destroyed"),)
    )
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "cs")
        audits = repos.cluster_state_audits.list_for_cluster(tx, "cs")
    assert row.status == "destroy-failed"  # untouched
    assert audits == []


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------


async def test_intents_applied_in_priority_order(uow, repos, dispatcher, hub, clock):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c-zombie", "z", status="destroyed", provider="fake"))
        repos.clusters.insert(tx, make_cluster_row("c-orphan", "o", status="active", provider="fake"))
    droplet = {"id": 7, "region": {"slug": "nyc1"}, "size_slug": "s-1vcpu-1gb"}
    provider = FakeProvider(
        "fake",
        intents=(
            # Deliberately built out of priority order -- Orphan(4) before
            # CreateUnmanaged(2) before Zombie(1) -- to prove application re-sorts.
            OrphanIntent(cluster_id="c-orphan"),
            CreateUnmanagedIntent(cluster_id="c-create", droplet=droplet, slug="ordered"),
            ZombieIntent(cluster_id="c-zombie", droplet_id="d"),
        ),
    )
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        zombie_row = repos.clusters.get(tx, "c-zombie")
        orphan_row = repos.clusters.get(tx, "c-orphan")
        created_row = repos.clusters.get(tx, "c-create")
    # Zombie's Phase 1 + the end-of-pass sweep both landed -> destroy-scheduled.
    assert zombie_row.status == "destroy-scheduled"
    assert orphan_row.status == "destroyed"
    assert created_row.status == "unmanaged"


# ---------------------------------------------------------------------------
# Phase-C destructive-intent suppression for a blocked/in-flight run
# ---------------------------------------------------------------------------


async def test_zombie_detection_not_suppressed_but_sweep_is_dr0014(uow, repos, dispatcher, hub, clock):
    """DR-0014: Phase-1 detection is NEVER suppressed (visibility is desirable even
    with a live run) -- the cluster still promotes DESTROYED -> ZOMBIE. Only the
    Phase-2 sweep's destructive ``DestroyRequested`` is suppressed, so the cluster
    stays ZOMBIE (not destroy-scheduled) while the run is in flight."""
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("cb", "bslug", status="destroyed", provider="fake"))
        repos.workflow_runs.insert(tx, make_run_row("r1", "cb", status="blocked"))
    provider = FakeProvider("fake", intents=(ZombieIntent(cluster_id="cb", droplet_id="d"),))
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "cb")
        audits = repos.cluster_state_audits.list_for_cluster(tx, "cb")
    assert row.status == "zombie"  # detection landed -- visibility preserved
    assert [(a.from_state, a.to_state, a.actor) for a in audits] == [("destroyed", "zombie", "reconciler")]


async def test_zombie_sweep_suppressed_then_swept_once_run_terminal_dr0014(uow, repos, dispatcher, hub, clock):
    """DR-0014's pinned cross-pass case: a ZOMBIE cluster (already promoted, e.g. by
    a prior pass) that ACQUIRES a blocked run is not re-destroyed by this pass's
    sweep -- it stays ZOMBIE, re-swept once the run reaches a terminal status."""
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("cb2", "bslug2", status="zombie", provider="fake"))
        repos.workflow_runs.insert(tx, make_run_row("r2", "cb2", status="blocked"))
    provider = FakeProvider("fake")  # no intents -- the sweep alone drives this
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "cb2")
    assert row.status == "zombie"  # suppressed -- stays ZOMBIE, not re-destroyed

    async with uow() as tx:
        repos.workflow_runs.update(tx, "r2", status="failed", finished_at=clock.now())

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "cb2")
    assert row.status == "destroy-scheduled"  # run cleared -- next sweep destroys it


async def test_orphan_suppressed_for_cluster_with_blocked_run(uow, repos, dispatcher, hub, clock):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("co2", "oslug2", status="active", provider="fake"))
        repos.workflow_runs.insert(tx, make_run_row("r2", "co2", status="blocked"))
    provider = FakeProvider("fake", intents=(OrphanIntent(cluster_id="co2"),))
    service = _service({"fake": provider}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        row = repos.clusters.get(tx, "co2")
    assert row.status == "active"  # suppressed -- untouched while the run is live


# ---------------------------------------------------------------------------
# last_reconciled_at
# ---------------------------------------------------------------------------


async def test_last_reconciled_at_set_on_success_not_on_skip(uow, repos, dispatcher, hub, clock):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c-ok", "ok-slug", status="active", provider="steady"))
        repos.clusters.insert(tx, make_cluster_row("c-skip", "skip-slug", status="active", provider="flaky"))
    steady = FakeProvider("steady")
    flaky = FakeProvider("flaky", raise_unreachable=True)
    service = _service({"steady": steady, "flaky": flaky}, repos, dispatcher, uow, hub, clock)

    await service.tick()

    async with uow() as tx:
        ok_row = repos.clusters.get(tx, "c-ok")
        skip_row = repos.clusters.get(tx, "c-skip")
    assert ok_row.last_reconciled_at == clock.now()
    assert skip_row.last_reconciled_at is None


# ---------------------------------------------------------------------------
# start()/stop()
# ---------------------------------------------------------------------------


async def test_start_fires_immediate_first_tick_before_returning(uow, repos, dispatcher, hub, clock):
    service = _service({}, repos, dispatcher, uow, hub, clock)
    assert service.last_sync() is None

    await service.start()
    try:
        assert service.last_sync() == clock.now()  # the immediate tick already ran
        assert service.running
    finally:
        await service.stop()
    assert not service.running


async def test_start_stop_idempotent(uow, repos, dispatcher, hub, clock):
    service = _service({}, repos, dispatcher, uow, hub, clock)
    await service.start()
    await service.start()  # no-op, does not fire a second immediate tick's worth of double work
    await service.stop()
    await service.stop()  # no-op
    assert not service.running
