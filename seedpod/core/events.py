"""The event taxonomy: one base class, five totality-law categories, the full union.

Salvaged from docs/design/seam-a-core.md §F (``seedpod2/core/events.py``; the
``seedpod2`` name is dead per coherence review Conflict 16.1), amended by
docs/design/coherence-review.md Conflict 8:

- NEW cluster Reports ``InfraAllocated(resource_ids)``, ``EndpointReady(public_ip)``.
- ``ProvisionSucceeded`` AMENDED to exactly ``(public_ip: str, kubeconfig_ref: str)``
  — ``provider_resource_id`` is DELETED from it (owned by ``InfraAllocated`` now).
- NEW deployment Report ``RollbackFinished(ok: bool)``, deliberately total-Ignore.
- DELETED everywhere: ``DestroyCompleted``, ``DestroyStalled``, ``ProvisionCancelled``,
  ``DeployCancelled``, ``DropletReady`` (none of these appear below).

Every event is a frozen, slotted, kind-registered dataclass with two mandatory base
fields: ``at`` (aware UTC, caller-supplied — ``transition()`` never calls ``now()``)
and ``actor`` (``api:<user>`` | ``reconciler`` | ``health`` | ``engine:run:<run_id>``
| ``timer:<key>`` | ``cluster-machine``). Events serialize as tagged JSON via the
codec (needed for ``timers.event``); the tag is the class name, registered in
``EVENT_REGISTRY`` by the ``@_event`` decorator below.

**The five event classes drive the totality law** (docs/design/seam-a-core.md §F,
enforced by ``core/machine.py``'s ``_fill_defaults``): ``Command`` may raise
``InvalidTransition``; ``Report``/``TimerFired``/``Observation``/``Cascaded`` are
NEVER invalid — an unlisted cell for one of them is a structural ``Ignore``.

``force=True`` is retired (docs/design/seam-a-core.md §F): ``Observation``'s actor
privilege check (``reconciler``/``health`` only) and the discovered-cluster
``DestroyRequested`` force guard are the two principled replacements, both enforced
as pure event-field checks in ``core/machine.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

__all__ = [
    "EVENT_REGISTRY",
    "Event",
    "Command",
    "Report",
    "TimerFired",
    "Observation",
    "Cascaded",
    "DiscoveredInfo",
    # cluster events
    "CreateRequested",
    "Discovered",
    "RetryRequested",
    "AdoptRequested",
    "DestroyRequested",
    "DestroyCancelled",
    "TtlExpired",
    "DestroyDue",
    "ProvisionSucceeded",
    "ProvisionFailed",
    "DestroySucceeded",
    "DestroyFailed",
    "InfraRunningObserved",
    "InfraMissingObserved",
    "HealthCheckFailed",
    "InfraAllocated",
    "EndpointReady",
    # deployment events
    "DeployRequested",
    "DeployRejected",
    "CancelRequested",
    "ClusterReady",
    "DeploySucceeded",
    "DeployFailed",
    "SupersededBy",
    "ClusterGone",
    "DeploymentPending",
    "RollbackFinished",
    "ClusterEvent",
    "DeploymentEvent",
]

EVENT_REGISTRY: dict[str, type[Event]] = {}


def _event(cls: type[Event]) -> type[Event]:
    """Register a leaf event class under its class name (the codec's `kind` tag)."""
    EVENT_REGISTRY[cls.__name__] = cls
    return cls


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    at: datetime  # aware UTC, caller-supplied -- transition() never calls now()
    actor: str  # 'api:<user>' | 'reconciler' | 'health' | 'engine:run:<id>' | 'timer:<key>' | 'cluster-machine'


@dataclass(frozen=True, slots=True, kw_only=True)
class Command(Event):
    """user/API/reconciler intent -- MAY raise InvalidTransition (caller's 409)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Report(Event):
    """workflow-run outcome -- NEVER invalid; stale/duplicate => Ignore."""


@dataclass(frozen=True, slots=True, kw_only=True)
class TimerFired(Event):
    """durable timer delivery -- NEVER invalid; raced => Ignore."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Observation(Event):
    """reconciler/health facts -- NEVER invalid; THE replacement for force=True."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Cascaded(Event):
    """delivered by the Cascade effect (or chained by the service layer)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscoveredInfo:
    """Provider-observed facts a reconciler folds into a `Discovered` command.

    Not itself a registered event `kind` -- a plain nested dataclass decoded via
    field type hints (docs/design/seam-a-core.md §B), matching the fields a
    NEW->UNMANAGED birth needs on `ClusterRecord` beyond the caller-supplied
    id/name/environment (mirrors v1's CreateUnmanagedIntent discovery payload,
    reference-code/seedpod/seedpod/core/reconciliation_intents.py).
    """

    provider: str
    public_ip: str | None = None
    provider_resources: Mapping[str, str] = ()


# ---- Cluster events (17: Seam A's 15 + Conflict 8's InfraAllocated/EndpointReady) ----


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class CreateRequested(Command):
    pass


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class Discovered(Command):
    observed: DiscoveredInfo  # reconciler found foreign infra


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class RetryRequested(Command):
    pass


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class AdoptRequested(Command):
    pass  # takeover / rehabilitation


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DestroyRequested(Command):
    due_at: datetime | None = None
    force: bool = False
    # DR-0043: the operator's `snapshot_before_destroy=true`, carried on the event
    # instead of performed inline by `ClusterService.destroy` before dispatch. It
    # rides ALONGSIDE `DestroyDue.trigger`, never folded into it: `trigger` is
    # provenance (which route reached DestroyDue), and "did the operator ask for a
    # snapshot" is not provenance -- one field answering two unrelated questions is
    # how the next reader gets it wrong. Defaulted, so every existing construction
    # still reads as "no snapshot asked for".
    snapshot: bool = False


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DestroyCancelled(Command):
    pass


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class TtlExpired(TimerFired):
    pass  # timer_key "ttl"


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DestroyDue(TimerFired):
    # timer_key "destroy". DR-0040: the destroy timer carries the event it will inject
    # (ScheduleTimer.event), so the transition that SCHEDULES the destroy can stamp why
    # -- which is the only place that still knows. Both routes converge on this event
    # (ACTIVE x TtlExpired and ACTIVE x DestroyRequested both reach DESTROY_SCHEDULED),
    # so without this the destroy workflow cannot tell an unattended deletion from one
    # an operator asked for, and `cluster.auto_snapshot` would fire on both -- doubling
    # up with DR-0020's own pre-destroy snapshot on the operator path.
    # Defaulted, so every existing construction (and any timer row already armed in a
    # live DB before migration) still reads as the operator case.
    trigger: str = "operator"
    # DR-0043: whether the operator asked for a pre-destroy snapshot, threaded from
    # `DestroyRequested.snapshot` by the transitions that arm the destroy timer. The
    # TTL route leaves it False and expresses its own intent through `trigger`; both
    # can be true at once (a TTL-scheduled destroy an operator then asks to snapshot
    # -- see `_cluster_destroy_scheduled_destroy_requested_dup`), which is exactly
    # why they are two fields.
    snapshot: bool = False


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class ProvisionSucceeded(Report):
    # AMENDED by coherence review Conflict 8: provider_resource_id DELETED
    # (InfraAllocated owns resource_ids now); kubeconfig never rides the event
    # (Conflict 9) -- only the opaque ref does.
    public_ip: str
    kubeconfig_ref: str


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class ProvisionFailed(Report):
    reason: str  # fired after compensation ran


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DestroySucceeded(Report):
    pass


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DestroyFailed(Report):
    reason: str


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class InfraRunningObserved(Observation):
    pass  # provider confirms PRESENT


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class InfraMissingObserved(Observation):
    # provider confirms ABSENT -- only after the salvaged InfrastructureUnreachableError
    # distinction says "absent", never "unreachable"
    pass


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class HealthCheckFailed(Observation):
    reason: str


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class InfraAllocated(Report):
    # NEW (Conflict 8): mid-provision fact; Ignore everywhere but PROVISIONING per the totality law
    resource_ids: Mapping[str, str]


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class EndpointReady(Report):
    # NEW (Conflict 8): provider-neutral replacement for Seam B's 'DropletReady'
    public_ip: str


# ---- Deployment events (9: Seam A's 8 + Conflict 8's RollbackFinished) ----


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DeployRequested(Command):
    spec_ref: str  # applied to service-built NEW record


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DeployRejected(Command):
    reason: str  # rule engine said no; audited record


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class CancelRequested(Command):
    reason: str = ""  # api | (via Cascade) cluster machine


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class ClusterReady(Cascaded):
    pass  # cluster-machine cascade | api redeploy chain


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DeploySucceeded(Report):
    resolved_images: Mapping[str, str] = ()


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DeployFailed(Report):
    reason: str


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class SupersededBy(Cascaded):
    new_deployment_id: str


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class DeploymentPending(Cascaded):
    """DR-0031 (+ Erratum E1): "a deployment is waiting on you" -- the CLUSTER-side
    counterpart of a deployment's ``DeployRequested``.

    Raised by ``Dispatcher.apply``'s escalation the moment a deployment is born, and
    answered by the cluster table: ACTIVE cascades ``ClusterReady`` straight back
    (unblocking a redeploy onto a live cluster, DR-0031's whole purpose); every other
    state ignores it, because some other transition -- ``ProvisionSucceeded``,
    ``ClusterGone`` -- is already the right one to resolve that deployment's fate.

    **Why this exists rather than routing ``DeployRequested`` to the cluster**, which
    is what DR-0031's ratified text proposed: ``tests/core/test_totality.py``'s
    ``test_event_type_unions_partition_an_event_exactly`` pins that ``ClusterEvent``
    and ``DeploymentEvent`` are DISJOINT -- every event kind belongs to exactly one
    aggregate. Reusing ``DeployRequested`` would have made that partition conditional
    for every future reader in order to save one class. A distinct name is also more
    honest: the cluster is not being asked to deploy anything, it is being told a
    dependant is waiting.

    ``deployment_id`` is carried for the audit trail (the cluster's own state-audit row
    names which deployment prompted the escalation); the ``Cascade`` it triggers still
    fans out by ``where_state``, never by this id.
    """

    deployment_id: str


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class ClusterGone(Cascaded):
    pass


@_event
@dataclass(frozen=True, slots=True, kw_only=True)
class RollbackFinished(Report):
    # NEW (Conflict 8): deliberately total-Ignore everywhere -- satisfies "exactly one
    # terminal event per run" for the rollback workflow (Conflict 12) without moving
    # any machine.
    ok: bool


ClusterEvent = (
    CreateRequested
    | Discovered
    | RetryRequested
    | AdoptRequested
    | DestroyRequested
    | DestroyCancelled
    | TtlExpired
    | DestroyDue
    | ProvisionSucceeded
    | ProvisionFailed
    | DestroySucceeded
    | DestroyFailed
    | InfraRunningObserved
    | InfraMissingObserved
    | HealthCheckFailed
    | InfraAllocated
    | EndpointReady
    | DeploymentPending  # DR-0031 E1: cluster-side "a deployment is waiting on you"
)

DeploymentEvent = (
    DeployRequested
    | DeployRejected
    | CancelRequested
    | ClusterReady
    | DeploySucceeded
    | DeployFailed
    | SupersededBy
    | ClusterGone
    | RollbackFinished
)
