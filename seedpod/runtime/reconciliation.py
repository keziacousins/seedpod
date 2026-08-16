"""``ReconciliationService`` -- the thin per-tick orchestrator wired around each
provider's committed ``Reconcile`` command handler. All the heavy Phase A/B
intent-BUILDING (droplet/kind/tart/orbstack <-> DB cross-referencing) already lives
inside each provider (``seedpod/providers/{digitalocean,kind,tart,orbstack}.py``,
docs/design/seam-c-provider.md Sec 5.3); this module only gathers the cluster set each
provider needs, groups by provider, calls ``Reconcile`` through the committed
transport, collects the returned intents, and maps them onto events through
``Dispatcher.apply`` -- the ONLY write path for cluster state (docs/decisions/DR-0006,
DR-0008, DR-0012).

**Salvaged shell only**, from
``reference-code/seedpod/seedpod/core/reconciliation.py``: the gather ->
group-by-provider -> per-provider reconcile -> skip-on-unreachable ->
collect-intents -> execute-in-priority-order shape of ``reconcile_three_phase``
(v1:90-236), ``_update_last_reconciled_at`` (v1:487), and the
``_create_unmanaged_cluster`` row-construction field set (v1:522).

**NOT ported:**

- Phase A/B (v1:238-395) -- the actual droplet/kind/tart/orbstack <-> DB
  intent-mapping logic. That now lives inside each provider's ``Reconcile``
  handler (docs/design/coherence-review.md Sec 2's type glossary); this module never
  inspects backend truth itself.
- ``_execute_startup_recovery`` (v1:571) -- crash recovery of stuck workflow runs is
  ``WorkflowEngine.resume_inflight``'s job (docs/design/coherence-review.md Conflict
  15's amended ``App.start``: ``resume_inflight()`` runs BEFORE
  ``services.reconciliation.start()``), not this module's. There is no
  ``startup_mode`` here -- ``StatusSyncIntent`` is therefore ALWAYS the periodic-mode
  no-op (v1:452-454: "workflows own their state transitions").
- v1's ``ReconciliationResult``/``ConflictType`` API-surface types -- they exist only
  to feed v1 REST responses this round doesn't build. Add them in Round 6 only if an
  endpoint provably needs them; no speculative surface here.

**DR-0012 (RATIFIED) pins the ``ZombieIntent`` -> event mapping, two-step through the
ZOMBIE state** -- read it in full; restated briefly:

- Phase 1 (per intent, inline this pass): ``InfraRunningObserved`` (actor
  ``reconciler``) against the DESTROYED record -> DESTROYED -> ZOMBIE. Detection
  only -- an audited ``Observation``; re-detecting an already-ZOMBIE record Ignores
  under the totality law.
- Phase 2 (once, at the END of the pass, after every provider's intents are
  applied): a FRESH read of every cluster CURRENTLY in ZOMBIE -- covering both this
  pass's Phase-1 promotions AND any left stranded in ZOMBIE by a crashed prior pass
  -- each gets ``DestroyRequested`` (actor ``reconciler``, ``due_at=None`` so it
  fires immediately; ``force=True`` iff ``origin == DISCOVERED``, the discovered
  guard, since the reconciler is a privileged actor) -> ZOMBIE -> DESTROY_SCHEDULED.
  Phase 2 deliberately re-derives its worklist from durable state every pass rather
  than driving off Phase 1's in-memory list -- this is what makes the two-step
  crash-safe (DR-0012's own "crash after Phase 1" test).

**DR-0008 (BINDING) -- a transaction encloses ONLY database statements; NEVER a
provider probe / Reconcile await / hub.broadcast / engine hand-off.** Every DB read
below (the cluster-set scan, each blocked-run check, the ZOMBIE sweep, the final
``last_reconciled_at`` stamp) is its own short, close-before-the-next-await
transaction. Every provider ``Reconcile`` call runs with NO transaction open. Every
applied intent is its OWN transaction -- each ``Dispatcher.apply()`` call below is
given no ``tx=``, so it opens and closes its own, exactly as the pattern read: DB
(own tx, close it) -> IO (no tx open) -> apply results (each own tx).
``tests/runtime/test_reconciliation.py`` proves this structurally: a fake provider's
``Reconcile`` handler asserts ``not uow._lock.locked()`` the instant it is invoked.

**Which clusters get passed to a provider's ``Reconcile``.** Providers are stateless
(no DB access, seam-c Sec 5.4) and ``ClusterSnapshot`` carries no ``provider`` field
of its own -- grouping by provider, and deciding which ``ClusterSnapshot`` rows even
exist to group, is entirely this module's job. Zombie detection needs DESTROYED
clusters cross-referenced against live infra (a ``ZombieIntent`` fires only for a
snapshot whose ``status == "destroyed"`` -- see ``seedpod/providers/kind.py``'s
``_reconcile``, which checks this explicitly), so, UNLIKE v1's exclude-DESTROYED read
(v1:768, safe there only because v1's providers re-queried the DB themselves), this
module's scan MUST include DESTROYED clusters. It must also include every OTHER live
status: ``CreateUnmanagedIntent`` fires whenever a backend-tagged resource's uuid is
simply ABSENT from the snapshot set passed in (``db_by_uuid.get(uuid) is None`` in
``digitalocean.py``'s ``_reconcile``) -- omitting any status here would make an
already-tracked cluster of that status look untracked and misfire a duplicate birth.
The scan is therefore every persisted status except ``NEW`` (which is never actually
persisted -- a birth's ``Persist`` always INSERTs the POST-transition state, per
``seedpod/runtime/dispatcher.py``'s overlay; ``ClusterState.NEW`` never appears in a
``status`` column read back from the DB).

**Every REGISTERED provider is called every tick, even with zero DB clusters.**
v1's grouping (``clusters_by_provider = defaultdict(list); ... for cluster in
db_clusters: clusters_by_provider[cluster.provider].append(cluster)``, then
iterating only THAT dict) only ever calls a provider that already has at least one
DB-tracked cluster -- which would make a provider's very first untracked droplet
permanently undiscoverable via ``CreateUnmanagedIntent`` (zero tracked clusters =>
the provider key never appears in the grouping => ``Reconcile`` never runs for it
at all). That is a genuine v1 gap, not a behavior to pin (CLAUDE.md): this module
iterates ``self._providers`` (the registered set) directly, passing each provider
whatever snapshot list its clusters produced -- ``()`` when none exist yet -- so a
provider with zero tracked clusters still gets probed and can still discover its
first one.

**Phase-C destructive-intent suppression** (docs/design/coherence-review.md Conflict
5: "Phase C already suppresses destructive intents for clusters with a live run --
'blocked' counts as live"; docs/decisions/DR-0014-zombie-sweep-suppression.md
RATIFIED, amending DR-0012). The suppression check
(``repos.workflow_runs.active_for_cluster``, a plain short DB read, no provider IO
nearby) applies to the DESTRUCTIVE steps ONLY, each checked FRESH at its own
apply/sweep time:

- ``OrphanIntent``'s single apply (``InfraMissingObserved`` -> DESTROYED) --
  destructive, suppressed.
- The Phase-2 ZOMBIE sweep's ``DestroyRequested`` (ZOMBIE -> DESTROY_SCHEDULED) --
  destructive, suppressed, re-checked fresh for EVERY cluster the sweep finds
  (covering both this pass's Phase-1 promotions and any left in ZOMBIE by a prior
  pass that has since acquired a run) -- a suppressed cluster simply stays ZOMBIE
  and is re-swept once the run clears.

``ZombieIntent``'s Phase-1 detection (``InfraRunningObserved`` -> ZOMBIE) is
DELIBERATELY NOT suppressed: it is non-destructive (it only makes an already-live
zombie visible), and ZOMBIE visibility is desirable even for a cluster with a live
run (DR-0014). An earlier build pass had this inverted -- suppressing detection and
leaving the sweep unchecked, which (a) guards the harmless step while leaving the
harmful one open, and (b) misses exactly the cross-pass case DR-0014 exists to
close: a cluster promoted to ZOMBIE when no run existed, that acquires a blocking
run before the next sweep, would be destroyed unsuppressed. DR-0014 is the
normative fix; this module implements it.

**``interval`` default.** No spec pins one. v1's ``reconciliation_full_interval``
config default was 600 seconds (``reference-code/seedpod/seedpod/core/config.py:215``)
-- this module keeps that number as its own default, an implementation choice (like
``TimerService.poll_interval``'s), not a spec gap.

**``environment`` for a ``CreateUnmanagedIntent`` birth**
(docs/decisions/DR-0013-discovered-cluster-environment.md, RATIFIED -- the
normative source; this is NOT a build-agent implementation choice). A genuinely
foreign, untracked droplet carries no environment signal at all (v1 rode a
since-retired ``'discovered'`` environment SENTINEL for this -- seam-d Decision 6
explicitly un-smuggles that pattern; v2's ``clusters.environment`` is ``NOT NULL``,
"real env only"). DR-0013 pins the birth row's ``environment`` to ``"production"``:
the conservative-authorization reading -- unknown infra is sensitive-until-triaged,
so it surfaces (DR-0010 SSE scoping, REST-GET) only to production-scoped and
``'all'``-scoped operators, never assumed disposable/ephemeral. (DR-0013 flags this
default for revisit once discovery is exercised against a real fleet -- not this
round's job.)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime

from seedpod.core.clock import Clock
from seedpod.core.errors import InfrastructureUnreachableError
from seedpod.core.events import (
    DestroyRequested,
    Discovered,
    DiscoveredInfo,
    InfraMissingObserved,
    InfraRunningObserved,
)
from seedpod.core.reconciliation_intents import (
    CreateUnmanagedIntent,
    OrphanIntent,
    ReconciliationIntent,
    StatusSyncIntent,
    ZombieIntent,
)
from seedpod.core.records import ClusterState, Origin
from seedpod.data.repositories import ClusterRow, Repositories
from seedpod.data.uow import UnitOfWork
from seedpod.providers.contract import ClusterSnapshot, Provider, Reconcile, Result
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.runtime.effect_executor import HubLike

__all__ = ["ReconciliationService"]

_log = logging.getLogger(__name__)

_DEFAULT_INTERVAL = 600.0  # seconds -- see module docstring's "interval default" note

# Every persisted ClusterState except NEW (module docstring: NEW is never actually
# persisted -- a birth's Persist always INSERTs the post-transition state).
_RECONCILABLE_STATUSES: tuple[str, ...] = tuple(s.value for s in ClusterState if s != ClusterState.NEW)


class ReconciliationService:
    """``ReconciliationService(providers, repos, dispatcher, engine, uow, hub, clock,
    interval=600.0)`` (module docstring). ``engine`` is carried for signature/wiring
    symmetry with the composition root's ``Services`` bundle (docs/design/
    coherence-review.md Conflict 15's amended ``App.start``, where reconciliation
    starts alongside the engine) -- nothing in this Round-5 build calls anything on
    it; the correctness-critical blocked-run check reads the durable
    ``workflow_runs`` row directly (module docstring)."""

    def __init__(
        self,
        providers: Mapping[str, Provider],
        repos: Repositories,
        dispatcher: Dispatcher,
        engine: object,
        uow: UnitOfWork,
        hub: HubLike,
        clock: Clock,
        interval: float = _DEFAULT_INTERVAL,
    ) -> None:
        self._providers = providers
        self.repos = repos
        self._dispatcher = dispatcher
        self._engine = engine
        self._uow = uow
        self._hub = hub
        self._clock = clock
        self._interval = interval
        self._task: asyncio.Task[None] | None = None
        self._last_sync: datetime | None = None

    def last_sync(self) -> datetime | None:
        """The clock-stamped time of the last COMPLETED tick (successful or not --
        a tick every one of whose providers was skipped still completes). Backs
        Round 6's ``/health/detailed``."""
        return self._last_sync

    @property
    def running(self) -> bool:
        """Truthful liveness, same discipline as ``TimerService.running``/
        ``EffectExecutor.running``."""
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Idempotent. Fires one immediate tick BEFORE returning (coherence-review:
        "periodic + a real immediate first tick" -- same discipline as
        ``EffectExecutor.start()``'s "drain everything pending before returning,
        THEN spawn the loop"), then spawns the periodic loop."""
        if self._task is not None:
            return
        await self.tick()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Cancel the periodic loop and wait for it to actually finish. Idempotent."""
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self.tick()
            except Exception:
                # Scan-level isolation, same discipline as TimerService/EffectExecutor:
                # a transient failure in a whole pass must not kill this task -- no
                # supervisor restarts it, and start()/stop() cannot revive a dead one.
                _log.exception("reconciliation pass failed, will retry next tick")

    async def tick(self) -> None:
        """One full pass (module docstring's DR-0008 read/IO/apply ordering)."""
        rows_by_id, snapshots_by_provider = await self._snapshot_state()

        collected: list[tuple[str, ReconciliationIntent]] = []
        reconciled_ids: set[str] = set()

        # Every registered provider, not just ones already covering a DB cluster
        # (module docstring: "every registered provider is called every tick").
        for provider_name, provider in self._providers.items():
            snapshots = snapshots_by_provider.get(provider_name, [])
            covered_ids = tuple(s.cluster_uuid for s in snapshots)
            try:
                provider_intents = await self._reconcile_provider(provider, snapshots)
            except InfrastructureUnreachableError as exc:
                # Crown jewel #1: skip this provider entirely -- zero intents, touch
                # nothing -- and broadcast reconciliation_skipped per covered cluster,
                # environment-scoped (DR-0010).
                _log.warning(
                    "reconciliation: provider %r unreachable, skipping %d cluster(s): %s",
                    provider_name,
                    len(covered_ids),
                    exc,
                )
                self._broadcast_skipped(covered_ids, rows_by_id, provider_name, exc)
                continue
            except Exception:
                # The reconciler's own belt-and-braces guard (the provider now
                # raises rather than swallowing -- seam-c Sec 5.3's deliberate
                # change #2): log + record, emit no intents from this provider,
                # continue to the next one.
                _log.exception(
                    "reconciliation: provider %r reconcile failed, skipping its intents this pass",
                    provider_name,
                )
                continue

            collected.extend((provider_name, intent) for intent in provider_intents)
            reconciled_ids.update(covered_ids)

        for unregistered in snapshots_by_provider.keys() - self._providers.keys():
            _log.warning(
                "reconciliation: no provider registered for %r, skipping %d cluster(s)",
                unregistered,
                len(snapshots_by_provider[unregistered]),
            )

        for provider_name, intent in sorted(collected, key=lambda pair: pair[1].priority):
            try:
                await self._apply_intent(provider_name, intent)
            except Exception:
                _log.exception(
                    "reconciliation: failed to apply %s for cluster %s, will retry next tick",
                    type(intent).__name__,
                    intent.cluster_id,
                )

        await self._sweep_zombies()

        if reconciled_ids:
            async with self._uow() as t:
                self.repos.clusters.set_last_reconciled_at(t, tuple(reconciled_ids), clock=self._clock)

        self._last_sync = self._clock.now()

    # -------------------------------------------------------------------------
    # Reads -- own short, DB-only transactions (DR-0008)
    # -------------------------------------------------------------------------

    async def _snapshot_state(self) -> tuple[dict[str, ClusterRow], dict[str, list[ClusterSnapshot]]]:
        async with self._uow() as t:
            rows = self.repos.clusters.list_by_status(t, _RECONCILABLE_STATUSES)
        rows_by_id: dict[str, ClusterRow] = {}
        by_provider: dict[str, list[ClusterSnapshot]] = defaultdict(list)
        for row in rows:
            rows_by_id[row.id] = row
            by_provider[row.provider].append(
                ClusterSnapshot(
                    cluster_uuid=row.id,
                    slug=row.slug,
                    status=row.status,
                    resource_ids=row.provider_resources,
                )
            )
        return rows_by_id, by_provider

    async def _has_blocking_run(self, cluster_id: str) -> bool:
        """Coherence-review's Phase-C suppression (module docstring)."""
        async with self._uow() as t:
            return self.repos.workflow_runs.active_for_cluster(t, cluster_id) is not None

    # -------------------------------------------------------------------------
    # Provider IO -- NO transaction open (DR-0008)
    # -------------------------------------------------------------------------

    async def _reconcile_provider(
        self, provider: Provider, snapshots: Sequence[ClusterSnapshot]
    ) -> tuple[ReconciliationIntent, ...]:
        cmd = Reconcile(clusters=tuple(snapshots))
        intents: tuple[ReconciliationIntent, ...] = ()
        async for ev in provider.execute(cmd):
            if isinstance(ev, Result):
                intents = ev.value  # type: ignore[assignment]
        return intents

    # -------------------------------------------------------------------------
    # Applying intents -- each its OWN transaction via Dispatcher.apply (DR-0008)
    # -------------------------------------------------------------------------

    async def _apply_intent(self, provider_name: str, intent: ReconciliationIntent) -> None:
        if isinstance(intent, ZombieIntent):
            await self._apply_zombie(intent)
        elif isinstance(intent, CreateUnmanagedIntent):
            await self._apply_create_unmanaged(provider_name, intent)
        elif isinstance(intent, StatusSyncIntent):
            return  # periodic mode: skip -- v1:452-454, workflows own their transitions
        elif isinstance(intent, OrphanIntent):
            await self._apply_orphan(intent)

    async def _apply_zombie(self, intent: ZombieIntent) -> None:
        """DR-0012 Phase 1: detection only, on the DESTROYED record. NOT suppressed
        (DR-0014, module docstring): detection is non-destructive and ZOMBIE
        visibility is desirable even for a cluster with a live/blocked run. The
        destructive cleanup -- and its suppression check -- happens at the Phase-2
        sweep (``_sweep_zombies``), not here."""
        await self._dispatcher.apply(
            "cluster", intent.cluster_id, InfraRunningObserved(at=self._clock.now(), actor="reconciler")
        )

    async def _apply_orphan(self, intent: OrphanIntent) -> None:
        if await self._has_blocking_run(intent.cluster_id):
            _log.info("reconciliation: suppressing OrphanIntent for %s -- run in flight", intent.cluster_id)
            return
        await self._dispatcher.apply(
            "cluster", intent.cluster_id, InfraMissingObserved(at=self._clock.now(), actor="reconciler")
        )

    async def _apply_create_unmanaged(self, provider_name: str, intent: CreateUnmanagedIntent) -> None:
        """NEW -> UNMANAGED birth via ``Dispatcher.apply(record=)`` (DR-0006).
        Field set salvaged from v1's ``_create_unmanaged_cluster``
        (reference-code/seedpod/seedpod/core/reconciliation.py:522):
        ``droplet_id``/``region``/``size`` -> the row-only ``provider_config``
        (provisioning INPUTS, v1's field set verbatim); ``droplet_id`` ALSO ->
        the machine-owned ``provider_resources`` (v2's provisioning-OUTPUTS
        column -- future Probe/Destroy commands address this droplet through it).
        See the module docstring for the ``environment`` default."""
        droplet = intent.droplet
        slug = intent.slug or f"discovered-{intent.cluster_id[:8]}"
        if isinstance(droplet, Mapping):
            droplet_id = str(droplet.get("id", ""))
            region = (droplet.get("region") or {}).get("slug", "unknown")
            size = droplet.get("size_slug", "unknown")
        else:
            droplet_id = str(getattr(droplet, "id", ""))
            region = "unknown"
            size = "unknown"

        now = self._clock.now()
        birth_row = ClusterRow(
            id=intent.cluster_id,
            name=slug,
            slug=slug,
            origin=Origin.DISCOVERED,
            environment="production",
            repository=None,
            branch=None,
            status=ClusterState.NEW.value,
            pre_destroy_state=None,
            version=0,
            provider=provider_name,
            provider_config={"droplet_id": droplet_id, "region": region, "size": size},
            provider_resources={},
            dns_hostname=None,
            dns_zone=None,
            # A discovered cluster has no deployment profile, so seedpod never made
            # it a DNS record and must not claim one (v1 was the same). DR-0034.
            dns_record_id=None,
            public_ip=None,
            node_count=1,
            encrypted_kubeconfig=None,
            kubeconfig_key_class=None,
            kubeconfig_ref=None,
            cost_per_hour=0.0,
            total_cost=0.0,
            consecutive_health_failures=0,
            failure_reason=None,
            last_reconciled_at=None,
            created_at=now,
            updated_at=now,
            expires_at=None,
        )
        event = Discovered(
            at=now,
            actor="reconciler",
            observed=DiscoveredInfo(
                provider=provider_name, public_ip=None, provider_resources={"droplet_id": droplet_id}
            ),
        )
        await self._dispatcher.apply("cluster", intent.cluster_id, event, record=birth_row)

    async def _sweep_zombies(self) -> None:
        """DR-0012 Phase 2: re-read every cluster CURRENTLY in ZOMBIE -- a fresh
        read, covering both this pass's Phase-1 promotions and any left stranded
        by a crashed prior pass (module docstring) -- and destroy each. NEVER
        driven off Phase 1's in-memory intent list. DR-0014: the destructive
        ``DestroyRequested`` is Phase-C suppressed, checked FRESH per cluster here
        -- a cluster with a live/blocked run stays ZOMBIE and is re-swept once the
        run clears."""
        async with self._uow() as t:
            rows = self.repos.clusters.list_by_status(t, (ClusterState.ZOMBIE.value,))
        now = self._clock.now()
        for row in rows:
            if await self._has_blocking_run(row.id):
                _log.info("reconciliation: suppressing ZOMBIE sweep for %s -- run in flight", row.id)
                continue
            try:
                await self._dispatcher.apply(
                    "cluster",
                    row.id,
                    DestroyRequested(
                        at=now, actor="reconciler", due_at=None, force=row.origin == Origin.DISCOVERED
                    ),
                )
            except Exception:
                _log.exception(
                    "reconciliation: failed to schedule destroy for zombie %s, will retry next tick", row.id
                )

    # -------------------------------------------------------------------------
    # SSE (DR-0010 environment scoping; ui-contract obligation 5)
    # -------------------------------------------------------------------------

    def _broadcast_skipped(
        self,
        cluster_ids: Sequence[str],
        rows_by_id: Mapping[str, ClusterRow],
        provider_name: str,
        exc: Exception,
    ) -> None:
        for cluster_id in cluster_ids:
            row = rows_by_id.get(cluster_id)
            environment = row.environment if row is not None else None
            try:
                self._hub.broadcast(
                    "reconciliation_skipped",
                    {"cluster_id": cluster_id, "provider": provider_name, "reason": str(exc)},
                    environment=environment,
                )
            except Exception:
                _log.warning("reconciliation: failed to broadcast reconciliation_skipped for %s", cluster_id)
