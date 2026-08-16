"""``ClusterService`` -- the thin read/query + Dispatcher-fronting surface
``api/clusters.py`` (a later Round-6 component) calls.

Constructor shape follows docs/design/seam-d-foundation.md Decision 8 step 9's
``ClusterService(dispatcher, repos, uow, id_gen=id_gen, clock=clock)`` verbatim
(the exact call ``seedpod/app/factory.py``'s own TODO comment names), plus two
kwargs Decision 8's illustrative snippet predates: ``crypto``/``kubectl_provider``,
needed for the provider-plane reads (pods/logs/events -- DR-0008: OUTSIDE any
``uow()``) this class's own brief requires and which no version of the
Decision-8 sketch's signature carries a collaborator for.

Salvage: the read/query surface (``get``/``list``/pods/logs/events) is salvaged
from ``reference-code/seedpod/seedpod/orchestrator/cluster_manager.py``
(``get_cluster_status``, ``list_clusters``) and
``reference-code/seedpod/seedpod/api/clusters.py`` (the pods/logs/events routes,
which call straight through to ``KubernetesProvider`` with a decrypted
kubeconfig); state changes (extend/rehabilitate/destroy) do NOT reuse v1's
``ClusterManager``/``StateManager`` bodies -- they go through
``Dispatcher.apply()`` only (CLAUDE.md hard rule), which v1 had no equivalent
single choke point for.

TTL-extend (``extend``) is deliberately NOT a ``Dispatcher.apply()`` call: DR-0009
pins ``expires_at``-bump + timer re-arm as ``ClusterRepository.set_expires_at`` +
``TimerRepository.upsert`` in one plain transaction ("becomes race-proof end to
end without the API layer knowing anything about it") -- there is no Pillar-1
event for "same-state TTL change", so this is the one state-touching method here
that is NOT a machine transition, matching the existing dedicated-write-path
precedent ``ClusterRepository.set_health_failures``/``update_cost`` already set.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

from seedpod.app.services.snapshot_service import SnapshotService
from seedpod.core.clock import Clock
from seedpod.core.effects import ScheduleTimer
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.core.events import AdoptRequested, DestroyRequested, Event
from seedpod.core.events import TtlExpired as TtlExpiredEvent
from seedpod.core.machine import InvalidTransition
from seedpod.core.records import ClusterState, Origin
from seedpod.data.repositories import ClusterRow, Repositories
from seedpod.data.uow import UnitOfWork
from seedpod.providers.contract import (
    KubeGetEvents,
    KubeGetPodDetails,
    KubeGetPodLogs,
    KubeGetPods,
    Result,
)
from seedpod.providers.kube_types import EventInfo, PodDetails, PodInfo
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.services.crypto import CryptoService

__all__ = ["ClusterService", "ClusterNotFound", "ClusterHasNoKubeconfig"]


class ClusterNotFound(LookupError):
    """No cluster with the given id/slug -- the API layer's 404."""


# DR-0019: GET /api/clusters' default hide-set, ported verbatim from v1
# (reference-code/seedpod/seedpod/api/clusters.py:124,135,167 -- "excludes
# destroyed/zombie/unmanaged clusters unless show_destroyed=true"). This is a
# UI-list concern, deliberately its OWN constant -- NOT core.records.TERMINAL_STATES
# ({destroyed, failed}, the slug-release set, Conflict 11): FAILED/DESTROY_FAILED
# must stay visible by default (operators need failures visible for attention/
# rehabilitation), while ZOMBIE/UNMANAGED must stay hidden by default. Do not
# conflate the two constants or define one in terms of the other.
_LIST_HIDDEN_STATES = frozenset(
    {ClusterState.DESTROYED.value, ClusterState.ZOMBIE.value, ClusterState.UNMANAGED.value}
)


class ClusterHasNoKubeconfig(PermanentError):
    """Provider-plane read requested (pods/logs/events) before the cluster has a
    kubeconfig (still PROVISIONING, or FAILED before ``ProvisionSucceeded``)."""

    def __init__(self, cluster_id: str) -> None:
        super().__init__(
            f"cluster {cluster_id} has no kubeconfig yet",
            code=ErrorCode.NOT_FOUND,
            provider="cluster-service",
            command="kubeconfig",
            detail={"cluster_id": cluster_id},
        )


class ClusterService:
    def __init__(
        self,
        dispatcher: Dispatcher,
        repos: Repositories,
        uow: UnitOfWork,
        id_gen: Callable[[], str],
        clock: Clock,
        *,
        crypto: CryptoService,
        kubectl_provider,
        snapshots: SnapshotService | None = None,
    ) -> None:
        self._dispatcher = dispatcher
        self._repos = repos
        self._uow = uow
        self._id_gen = id_gen  # unused by this round's methods (clusters are only ever
        #   born as a side effect of DeploymentService.version_update -- ui-contract's
        #   REST inventory has no `POST /api/clusters`); kept because Decision 8 pins it
        #   on this constructor and a future direct-create surface would need it.
        self._clock = clock
        self._crypto = crypto
        self._kubectl = kubectl_provider
        # DR-0020 (Round 6, api-features): the real fail-open pre-destroy snapshot
        # capability, injected by `seedpod/app/factory.py`. Defaults to `None` so
        # every pre-existing construction site (`tests/app/test_services_cluster.py`'s
        # own fixture never passes it) keeps working unchanged -- `destroy()` below
        # falls back to the DR-0020-interim `PermanentError(UNSUPPORTED)` (-> 501,
        # never a silent skip) only when no snapshot collaborator was wired in.
        self._snapshots = snapshots

    # -------------------------------------------------------------------
    # Reads
    # -------------------------------------------------------------------

    async def get(self, id_or_slug: str) -> ClusterRow:
        async with self._uow() as tx:
            row = self._repos.clusters.get_by_id_or_slug(tx, id_or_slug, active_only=False)
        if row is None:
            raise ClusterNotFound(id_or_slug)
        return row

    async def list(self, *, show_destroyed: bool = False, status: str | None = None) -> list[ClusterRow]:
        """ui-contract obligation 6: ``show_destroyed``/``status=active`` query
        params. Filtering happens here, in Python, over ``list_all``'s one
        unfiltered SELECT -- see ``ClusterRepository.list_all``'s docstring.
        DR-0019: the default (``show_destroyed=False``) hide-set is
        ``_LIST_HIDDEN_STATES`` -- NOT ``core.records.TERMINAL_STATES``."""
        async with self._uow() as tx:
            rows = self._repos.clusters.list_all(tx)
        if status == "active":
            return [r for r in rows if r.status == ClusterState.ACTIVE.value]
        if show_destroyed:
            return rows
        return [r for r in rows if r.status not in _LIST_HIDDEN_STATES]

    # -------------------------------------------------------------------
    # State changes -- Dispatcher.apply() only
    # -------------------------------------------------------------------

    async def extend(self, cluster_id: str, *, ttl_hours: float, actor: str) -> ClusterRow:
        """DR-0009: not a machine transition -- see module docstring."""
        async with self._uow() as tx:
            row = self._repos.clusters.get(tx, cluster_id)
            if row is None:
                raise ClusterNotFound(cluster_id)
            if row.expires_at is None:
                raise PermanentError(
                    f"cluster {cluster_id} has no TTL to extend",
                    code=ErrorCode.INVALID_INPUT,
                    provider="cluster-service",
                    command="extend",
                    detail={"cluster_id": cluster_id},
                )
            # v1 (reference-code/seedpod/seedpod/api/clusters.py:379-389): production
            # clusters and clusters outside active/creating (v2: active/provisioning
            # -- PROVISIONING absorbs v1 CREATING, core/records.py:46) never accept
            # extension. Guarded here, in the extend path itself, matching where v1
            # enforced it -- there is no other choke point for this method yet.
            if row.environment == "production":
                raise PermanentError(
                    f"cluster {cluster_id} is a production cluster and cannot be extended",
                    code=ErrorCode.INVALID_INPUT,
                    provider="cluster-service",
                    command="extend",
                    detail={"cluster_id": cluster_id},
                )
            if row.status not in {ClusterState.ACTIVE.value, ClusterState.PROVISIONING.value}:
                raise PermanentError(
                    f"cluster {cluster_id} is in status {row.status!r} and cannot be extended",
                    code=ErrorCode.INVALID_INPUT,
                    provider="cluster-service",
                    command="extend",
                    detail={"cluster_id": cluster_id, "status": row.status},
                )
            # v1 (reference-code/seedpod/seedpod/api/clusters.py:353-356, :391):
            # `base = max(cluster.expires_at, utc_now())` -- a cluster whose TTL has
            # already lapsed but is still ACTIVE (pending the reconciler/timer
            # destroying it) must get at least ttl_hours MORE runtime from NOW, not
            # from its already-past expiry, or the re-armed timer fires immediately.
            base = max(row.expires_at, self._clock.now())
            new_expires_at = base + timedelta(hours=ttl_hours)
            self._repos.clusters.set_expires_at(tx, cluster_id, new_expires_at, clock=self._clock)
            timer = ScheduleTimer(
                aggregate_type="cluster",
                aggregate_id=cluster_id,
                timer_key="ttl",
                fire_at=new_expires_at,
                event=TtlExpiredEvent(at=new_expires_at, actor="timer:ttl"),
            )
            self._repos.timers.upsert(tx, timer, f"api:extend/{cluster_id}@{new_expires_at.isoformat()}")
            row = self._repos.clusters.get(tx, cluster_id)
        assert row is not None
        return row

    async def rehabilitate(self, cluster_id: str, *, actor: str) -> ClusterRow:
        event: Event = AdoptRequested(at=self._clock.now(), actor=actor)
        await self._dispatcher.apply("cluster", cluster_id, event)
        return await self.get(cluster_id)

    async def destroy(
        self,
        cluster_id: str,
        *,
        actor: str,
        force: bool = False,
        due_at=None,
        snapshot_before_destroy: bool = False,
    ) -> ClusterRow:
        """The discovered-origin force guard (core/machine.py's dagger-marked
        rules) is enforced authoritatively by ``transition()`` itself inside
        ``Dispatcher.apply()`` -- a discovered cluster destroyed without
        ``force=True`` raises ``InvalidTransition``, which the API layer maps to
        its 409. This method ALSO pre-checks the identical public-field
        condition (``origin == DISCOVERED and not force``) before taking any
        snapshot, purely so a request ``apply()`` would reject never triggers the
        DR-0020 side effect first; ``apply()`` still re-validates and remains the
        sole source of truth (a core rule change here would just make this
        pre-check a no-op guard, never a hole -- ``apply()`` always has the last
        word).

        DR-0018: a SEPARATE, additional guard restored here at the service edge --
        a managed cluster (``origin == MANAGED``) with ``environment ==
        'production'`` also requires ``force=True``, else ``PermanentError``
        (``INVALID_INPUT`` -> 400 at the API layer), mirroring v1
        (``reference-code/seedpod/seedpod/api/clusters.py:472-477``) and the
        existing asymmetric precedent this method's sibling ``extend`` already
        set (:145 above). This composes with, but does not replace, the
        machine's discovered-origin guard: force overrides both independently.

        Both guards run BEFORE anything is dispatched: a destroy that is ultimately
        going to be rejected must never leave a stray ``is_auto`` snapshot behind as
        a side effect of a call that never actually destroys anything. That ordering
        is what still protects the snapshot after DR-0043 moved it downstream -- the
        guards reject here, so the event carrying ``snapshot=True`` is never emitted.

        DR-0043: ``snapshot_before_destroy=true`` is now DECLARED here and PERFORMED
        by the destroy workflow's ``cluster.auto_snapshot`` step, not awaited inline.
        This method used to block on the whole snapshot -- ``kubectl exec`` per
        persistable service at a 300s timeout each -- before dispatching, which made
        ``DELETE /api/clusters/{id}?snapshot_before_destroy=true`` hang for minutes on
        a response body ``api/routers/clusters.py`` records the SPA never reads. The
        snapshot itself is unchanged in substance (DR-0020: real, fail-open, v1 parity
        with ``_attempt_auto_snapshot``, ``reference-code/.../orchestrator/
        cluster_manager.py:681``); only where it runs changed, so it now reports
        through workflow progress like every other step.

        If no ``SnapshotService`` collaborator was injected at all (the DR-0020-interim
        state some construction sites still exercise -- ``tests/app/
        test_services_cluster.py``'s own fixture), this still raises
        ``PermanentError(UNSUPPORTED)`` (-> 501) rather than silently skip. DR-0043 kept
        that guard deliberately: the work moved, but DR-0020's "the flag is never a lie
        even in a degraded configuration" property did not, and it should not be lost as
        a side effect of relocating a call."""
        row = await self.get(cluster_id)
        if row.origin == Origin.MANAGED and row.environment == "production" and not force:
            raise PermanentError(
                f"cluster {cluster_id} is a production cluster and destruction requires force=true",
                code=ErrorCode.INVALID_INPUT,
                provider="cluster-service",
                command="destroy",
                detail={"cluster_id": cluster_id},
            )
        if row.origin == Origin.DISCOVERED and not force:
            raise InvalidTransition(f"DestroyRequested on discovered cluster {cluster_id} requires force=True")
        if snapshot_before_destroy and self._snapshots is None:
            raise PermanentError(
                f"cluster {cluster_id}: snapshot_before_destroy has no snapshot capability wired",
                code=ErrorCode.UNSUPPORTED,
                provider="cluster-service",
                command="destroy",
                detail={"cluster_id": cluster_id},
            )
        event: Event = DestroyRequested(
            at=self._clock.now(), actor=actor, due_at=due_at, force=force,
            snapshot=snapshot_before_destroy,  # DR-0043 -- declared here, taken by the workflow
        )
        await self._dispatcher.apply("cluster", cluster_id, event)
        return await self.get(cluster_id)

    # -------------------------------------------------------------------
    # Provider-plane reads -- OUTSIDE any uow (DR-0008)
    # -------------------------------------------------------------------

    async def _kubeconfig_for(self, cluster_id: str) -> str:
        async with self._uow() as tx:
            row = self._repos.clusters.get(tx, cluster_id)
        if row is None:
            raise ClusterNotFound(cluster_id)
        if row.encrypted_kubeconfig is None or row.kubeconfig_key_class is None:
            raise ClusterHasNoKubeconfig(cluster_id)
        return self._crypto.decrypt(row.encrypted_kubeconfig, row.kubeconfig_key_class)

    async def _execute_kubectl(self, cmd) -> object:
        result = None
        async for event in self._kubectl.execute(cmd):
            if isinstance(event, Result):
                result = event.value
        return result

    async def pods(self, cluster_id: str, *, namespace: str | None = None) -> tuple[PodInfo, ...]:
        kubeconfig = await self._kubeconfig_for(cluster_id)
        return await self._execute_kubectl(KubeGetPods(kubeconfig=kubeconfig, namespace=namespace))

    async def pod_details(self, cluster_id: str, namespace: str, pod_name: str) -> PodDetails:
        kubeconfig = await self._kubeconfig_for(cluster_id)
        return await self._execute_kubectl(
            KubeGetPodDetails(kubeconfig=kubeconfig, pod_name=pod_name, namespace=namespace)
        )

    async def pod_logs(
        self,
        cluster_id: str,
        namespace: str,
        pod_name: str,
        *,
        container: str | None = None,
        tail_lines: int = 100,
        previous: bool = False,
    ) -> str:
        kubeconfig = await self._kubeconfig_for(cluster_id)
        return await self._execute_kubectl(
            KubeGetPodLogs(
                kubeconfig=kubeconfig,
                pod_name=pod_name,
                namespace=namespace,
                container=container,
                tail_lines=tail_lines,
                previous=previous,
            )
        )

    async def events(self, cluster_id: str, *, namespace: str | None = None, limit: int = 200) -> tuple[EventInfo, ...]:
        kubeconfig = await self._kubeconfig_for(cluster_id)
        return await self._execute_kubectl(KubeGetEvents(kubeconfig=kubeconfig, namespace=namespace, limit=limit))
