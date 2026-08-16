"""The pure total transition function -- the whole of Pillar 1.

Salvaged from docs/design/seam-a-core.md §§F-H (``seedpod2/core/machine.py``,
``seedpod2/core/apply.py``; the ``seedpod2`` name is dead per coherence review
Conflict 16.1), amended by docs/design/coherence-review.md:

- **Conflict 3**: there is no ``core/apply.py`` in v2. The transition-applier
  (``Dispatcher``, which owns IO, the outbox, and the drain lane) is runtime
  work for a later phase; this module ships only the pure ``transition()``,
  ``TransitionResult``, ``InvalidTransition``, and ``StaleVersion``.
- **Conflict 8**: two new PROVISIONING rows (``InfraAllocated``, ``EndpointReady``),
  ``ProvisionSucceeded`` narrowed to ``(public_ip, kubeconfig_ref)``, deployment's
  ``RollbackFinished`` deliberately total-Ignore, and the P/N law reworded:
  "every non-Ignore row emits exactly one Persist and one Notify" (covering
  same-state field-update rows, not just state changes).
- **Conflict 12**: ``DEPLOYING x CancelRequested -> CANCELLED`` now emits
  ``CancelWorkflow(deploy)`` THEN ``RunWorkflow(rollback, deployment_id=self.id)``
  in that order; ``DEPLOYING x ClusterGone -> DESTROYED`` emits
  ``CancelWorkflow(deploy)`` only (no rollback on a dying cluster).
- **Conflict 16**: ``StaleVersion`` (not ``StaleVersionError``).

**Totality law** (binding, docs/design/seam-a-core.md §F, restated verbatim in the
task brief): the transition tables are ``dict[(state, event_type) -> rule]``, made
total by ``_fill_defaults``:

- On ``NEW``, only the birth Commands (explicitly listed) are valid; *everything
  else* -- including Reports/Timers/Observations/Cascaded that would otherwise
  default to Ignore on non-NEW states -- is ``InvalidTransition``. The one
  documented exception is deployment's ``RollbackFinished``, which Conflict 8
  pins as "deliberately total-Ignore" (including at NEW) to satisfy "exactly one
  terminal event per run" for the rollback workflow without adding a machine;
  it is therefore given an explicit ``(NEW, RollbackFinished) -> Ignore`` row
  that overrides the general NEW default. This is the one place the coherence
  review's per-event pin outranks the general totality default, per the binding
  precedence rule.
- On every other state, an unlisted ``Command`` -> ``InvalidTransition``; an
  unlisted ``Report``/``TimerFired``/``Observation``/``Cascaded`` -> ``Ignore``
  (unchanged record, empty effects, no version bump, no SSE, no audit).

Two cross-cutting pure checks replace v1's ``force=True`` (docs/design/seam-a-core.md
§F): (1) an ``Observation``'s ``actor`` must be ``reconciler`` or ``health``, checked
once in ``transition()`` before table dispatch, for every ``Observation`` regardless
of aggregate or state; (2) the discovered-cluster ``DestroyRequested`` guard
(``origin == DISCOVERED and not event.force -> InvalidTransition``), checked inline
in each dagger-marked (`†`) rule of cluster table §G -- explicitly NOT applied to
``UNMANAGED x DestroyRequested`` (unguarded, v1 parity: the intersection check was
only ever wired at v1's ACTIVE destroy site).

Every rule builds the new record via
``dataclasses.replace(record, state=..., version=record.version + 1, <fields>)``
and emits ``Persist(record=new, expected_version=old.version)`` (``None`` only for
the two birth rows per aggregate, which INSERT rather than CAS UPDATE). Every
non-Ignore row emits exactly one ``Persist`` and exactly one ``Notify``.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from dataclasses import dataclass

from seedpod.core.effects import (
    CancelTimer,
    CancelWorkflow,
    Cascade,
    Effect,
    Notify,
    Persist,
    RunWorkflow,
    ScheduleTimer,
)
from seedpod.core.events import (
    AdoptRequested,
    CancelRequested,
    ClusterEvent,
    ClusterGone,
    ClusterReady,
    Command,
    CreateRequested,
    DeployFailed,
    DeploymentEvent,
    DeploymentPending,
    DeployRejected,
    DeployRequested,
    DeploySucceeded,
    DestroyCancelled,
    DestroyDue,
    DestroyFailed,
    DestroyRequested,
    DestroySucceeded,
    Discovered,
    EndpointReady,
    Event,
    HealthCheckFailed,
    InfraAllocated,
    InfraMissingObserved,
    InfraRunningObserved,
    Observation,
    ProvisionFailed,
    ProvisionSucceeded,
    RetryRequested,
    RollbackFinished,
    SupersededBy,
    TtlExpired,
)
from seedpod.core.records import (
    ClusterRecord,
    ClusterState,
    DeploymentRecord,
    DeploymentState,
    Origin,
)

__all__ = ["TransitionResult", "InvalidTransition", "StaleVersion", "transition"]


@dataclass(frozen=True, slots=True)
class TransitionResult:
    record: ClusterRecord | DeploymentRecord  # version bumped iff a Persist is present
    effects: tuple[Effect, ...]  # () => Ignore


class InvalidTransition(Exception):
    """A Command is illegal for the record's current state, or a privileged-actor
    check failed. From ``api:*`` actors this is the caller's 409; from
    ``reconciler``/``engine:*``/``timer:*`` actors it is logged and dropped."""


class StaleVersion(Exception):
    """Raised by the Dispatcher (runtime, not this module) when a Persist's CAS
    UPDATE matches zero rows. The caller re-reads and re-decides, bounded to 3
    attempts (docs/design/seam-a-core.md §D). Named ``StaleVersion`` per
    coherence review Conflict 16.2 -- ``StaleVersionError`` is dead."""


_PRIVILEGED_OBSERVATION_ACTORS = frozenset({"reconciler", "health"})

# Casc-gone (§G legend): every deployment state except DESTROYED.
_ALL_DEPLOYMENT_STATES_EXCEPT_DESTROYED = frozenset(
    s for s in DeploymentState if s is not DeploymentState.DESTROYED
)


# ---------------------------------------------------------------------------
# Generic defaults + result builders
# ---------------------------------------------------------------------------


def _ignore(record: ClusterRecord | DeploymentRecord, event: Event) -> TransitionResult:
    return TransitionResult(record=record, effects=())


def _invalid(record: ClusterRecord | DeploymentRecord, event: Event) -> TransitionResult:
    raise InvalidTransition(
        f"{type(event).__name__} is not valid for {type(record).__name__} "
        f"in state {record.state.value!r} (actor={event.actor!r})"
    )


def _fill_defaults(
    table: dict,
    states: tuple,
    event_types: tuple[type, ...],
    *,
    birth_state,
) -> dict:
    """Total-ize a partial (state, event_type) -> rule table per the totality law."""
    filled = dict(table)
    for state in states:
        for etype in event_types:
            key = (state, etype)
            if key in filled:
                continue
            if state == birth_state:
                filled[key] = _invalid
            elif issubclass(etype, Command):
                filled[key] = _invalid
            else:
                filled[key] = _ignore
    return filled


def _cluster_result(
    old: ClusterRecord,
    event: Event,
    *,
    state: ClusterState,
    birth: bool = False,
    notify_old_status: str | None = None,
    extra_effects: tuple[Effect, ...] = (),
    **fields,
) -> TransitionResult:
    new = dataclasses.replace(old, state=state, version=old.version + 1, **fields)
    persist = Persist(record=new, expected_version=None if birth else old.version)
    old_status = old.state.value if notify_old_status is None else notify_old_status
    notify = Notify(
        topic="cluster_state_changed",
        payload={"cluster_id": new.id, "old_status": old_status, "new_status": new.state.value},
        environment=new.environment,
    )
    return TransitionResult(record=new, effects=(persist, notify, *extra_effects))


def _deployment_result(
    old: DeploymentRecord,
    event: Event,
    *,
    state: DeploymentState,
    birth: bool = False,
    notify_old_status: str | None = None,
    extra_effects: tuple[Effect, ...] = (),
    **fields,
) -> TransitionResult:
    new = dataclasses.replace(old, state=state, version=old.version + 1, **fields)
    persist = Persist(record=new, expected_version=None if birth else old.version)
    old_status = old.state.value if notify_old_status is None else notify_old_status
    notify = Notify(
        topic="deployment_status_changed",
        payload={
            "deployment_id": new.id,
            "cluster_id": new.cluster_id,
            "old_status": old_status,
            "new_status": new.state.value,
        },
        environment=new.environment,
    )
    return TransitionResult(record=new, effects=(persist, notify, *extra_effects))


def _check_discovered_guard(record: ClusterRecord, event: DestroyRequested) -> None:
    """dagger (†) guard: origin == DISCOVERED and not event.force => InvalidTransition."""
    if record.origin == Origin.DISCOVERED and not event.force:
        raise InvalidTransition(
            f"DestroyRequested on discovered cluster {record.id} requires force=True"
        )


def _ttl_timer(cluster_id: str, expires_at) -> ScheduleTimer:
    return ScheduleTimer(
        aggregate_type="cluster",
        aggregate_id=cluster_id,
        timer_key="ttl",
        fire_at=expires_at,
        event=TtlExpired(at=expires_at, actor="timer:ttl"),
    )


def _destroy_timer(
    cluster_id: str, fire_at, *, trigger: str = "operator", snapshot: bool = False
) -> ScheduleTimer:
    """DR-0040: ``trigger`` rides the injected ``DestroyDue`` so the destroy workflow can
    tell an unattended deletion from one an operator asked for. Defaults to "operator" --
    the TTL transitions are the only two callers that pass anything else, which keeps the
    unattended case explicit at both of its sites rather than implied by omission.

    DR-0043: ``snapshot`` rides the same channel, carrying the operator's
    ``snapshot_before_destroy=true`` to ``cluster.auto_snapshot`` instead of being
    performed inline before dispatch. Independent of ``trigger`` on purpose -- the two
    answer different questions and can both be set (a TTL-scheduled destroy an operator
    then asks to snapshot)."""
    return ScheduleTimer(
        aggregate_type="cluster",
        aggregate_id=cluster_id,
        timer_key="destroy",
        fire_at=fire_at,
        event=DestroyDue(at=fire_at, actor="timer:destroy", trigger=trigger, snapshot=snapshot),
    )


def _cancel_timer(cluster_id: str, timer_key: str) -> CancelTimer:
    return CancelTimer(aggregate_type="cluster", aggregate_id=cluster_id, timer_key=timer_key)


def _cancel_all_timers(cluster_id: str) -> CancelTimer:
    return CancelTimer(aggregate_type="cluster", aggregate_id=cluster_id, timer_key=None)


def _cluster_gone_cascade(record: ClusterRecord, event: Event, *, except_id: str | None = None) -> Cascade:
    return Cascade(
        cluster_id=record.id,
        where_state=_ALL_DEPLOYMENT_STATES_EXCEPT_DESTROYED,
        event=ClusterGone(at=event.at, actor="cluster-machine"),
        except_id=except_id,
    )


# ---------------------------------------------------------------------------
# Cluster table (docs/design/seam-a-core.md §G, complete 10 x 15,
# amended by coherence review Conflict 8's two new PROVISIONING rows -> 10 x 17)
# ---------------------------------------------------------------------------


def _cluster_new_create_requested(record: ClusterRecord, event: CreateRequested) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.PROVISIONING,
        birth=True,
        notify_old_status="",  # preserves v1's UI-visible birth broadcast shape
        extra_effects=(RunWorkflow(workflow="provision", cluster_id=record.id),),
    )


def _cluster_new_discovered(record: ClusterRecord, event: Discovered) -> TransitionResult:
    observed = event.observed
    return _cluster_result(
        record,
        event,
        state=ClusterState.UNMANAGED,
        birth=True,
        notify_old_status="",
        origin=Origin.DISCOVERED,
        provider=observed.provider,
        public_ip=observed.public_ip,
        provider_resources=observed.provider_resources,
    )


def _cluster_provisioning_provision_succeeded(record: ClusterRecord, event: ProvisionSucceeded) -> TransitionResult:
    extra: list[Effect] = []
    if record.expires_at is not None:
        extra.append(_ttl_timer(record.id, record.expires_at))
    extra.append(
        Cascade(
            cluster_id=record.id,
            where_state=frozenset({DeploymentState.PENDING}),
            event=ClusterReady(at=event.at, actor="cluster-machine"),
        )
    )
    return _cluster_result(
        record,
        event,
        state=ClusterState.ACTIVE,
        public_ip=event.public_ip,
        kubeconfig_ref=event.kubeconfig_ref,
        extra_effects=tuple(extra),
    )


def _cluster_provisioning_provision_failed(record: ClusterRecord, event: ProvisionFailed) -> TransitionResult:
    extra: tuple[Effect, ...] = ()
    if record.expires_at is not None:  # failed clusters get TTL auto-cleanup too
        extra = (_ttl_timer(record.id, record.expires_at),)
    return _cluster_result(
        record, event, state=ClusterState.FAILED, failure_reason=event.reason, extra_effects=extra
    )


def _cluster_provisioning_infra_allocated(record: ClusterRecord, event: InfraAllocated) -> TransitionResult:
    merged = {**dict(record.provider_resources), **dict(event.resource_ids)}
    return _cluster_result(record, event, state=ClusterState.PROVISIONING, provider_resources=merged)


def _cluster_provisioning_endpoint_ready(record: ClusterRecord, event: EndpointReady) -> TransitionResult:
    return _cluster_result(record, event, state=ClusterState.PROVISIONING, public_ip=event.public_ip)


def _cluster_active_deployment_pending(record: ClusterRecord, event: DeploymentPending) -> TransitionResult:
    """DR-0031: a deployment born onto an ALREADY-ACTIVE cluster must start.

    ``PENDING -> DEPLOYING`` is driven only by ``ClusterReady``, and its other emitter
    (``_cluster_provisioning_provision_succeeded``) fires on ``provisioning ->
    ACTIVE`` -- a transition an already-ACTIVE cluster will never re-enter. Without
    this row the deployment sits in ``pending`` with zero workflow runs, forever
    (smoke 4, 2026-08-09, deployment ``64db05b5``).

    **This is not a state change** -- the cluster was ACTIVE and stays ACTIVE -- but it
    still carries a ``Persist``, and that is deliberate rather than incidental.
    ``effect_id`` is ``"{aggregate}/{id}@{to_version}#{ordinal}"`` and is UNIQUE, so a
    transition that emits effects without advancing ``version`` produces a colliding
    id the second time it runs. A first draft of this rule returned the ``Cascade``
    alone; ``test_retrigger`` immediately failed on
    ``UNIQUE constraint failed: effects_outbox.effect_id`` -- two deploy requests
    against one ACTIVE cluster both minting ``cluster/<id>@2#0``. Every other
    effect-producing transition in this table bumps the version; the outbox identity
    scheme requires it, so this one does too.

    It therefore goes through ``_cluster_result`` like every other rule, which also
    means it emits ``Notify`` (``cluster_state_changed`` with
    ``old_status == new_status == "active"``). A second draft tried to suppress that
    as a "notification about a non-change" and hand-built the result with
    ``Persist`` + ``Cascade`` only;
    ``test_cluster_transitions_never_double_persist_or_mismatch_expected_version``
    rejected it, because this table has a pinned law -- **exactly one ``Notify`` per
    ``Persist``**. The law wins: a redundant refresh signal on a cluster that just
    accepted a deploy request is harmless (the SPA re-reads), whereas a
    ``Persist``-without-``Notify`` special case would make the invariant conditional
    for every future reader. Both drafts are recorded here because each was killed by
    a different existing test, which is those tests working exactly as intended.

    The cascade targets every PENDING deployment of the cluster rather than only the
    triggering one, and does so deliberately: the pure machine is not told which
    deployment prompted the escalation, and "a deployment is pending on a cluster that
    is ready" is precisely the condition ``ClusterReady`` exists to resolve. In the
    ordinary case there is exactly one such row -- the deployment just born in this
    same transaction. Where an older pending row exists too, advancing it is the
    correct repair, not a side effect.

    v1 never had this hole to fill: ``cluster_manager._schedule_deployment_work``
    (reference-code/.../orchestrator/cluster_manager.py:1443) scheduled the deployment
    workflow UNCONDITIONALLY and decided about provisioning inside it
    (``_ensure_target_cluster``). v2's cascade is the better design; DR-0031 restores
    the branch the translation dropped.
    """
    return _cluster_result(
        record,
        event,
        state=ClusterState.ACTIVE,  # unchanged -- the Cascade is the payload of this rule
        extra_effects=(
            Cascade(
                cluster_id=record.id,
                where_state=frozenset({DeploymentState.PENDING}),
                event=ClusterReady(at=event.at, actor="cluster-machine"),
            ),
        ),
    )


def _cluster_active_destroy_requested(record: ClusterRecord, event: DestroyRequested) -> TransitionResult:
    _check_discovered_guard(record, event)
    fire_at = event.due_at or event.at
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=ClusterState.ACTIVE,
        extra_effects=(
            _cancel_timer(record.id, "ttl"),
            _destroy_timer(record.id, fire_at, snapshot=event.snapshot),  # DR-0043
        ),
    )


def _cluster_active_ttl_expired(record: ClusterRecord, event: TtlExpired) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=ClusterState.ACTIVE,
        extra_effects=(_destroy_timer(record.id, event.at, trigger="ttl_expiry"),),
    )


def _cluster_active_health_check_failed(record: ClusterRecord, event: HealthCheckFailed) -> TransitionResult:
    return _cluster_result(record, event, state=ClusterState.FAILED, failure_reason=event.reason)


def _cluster_active_infra_missing_observed(record: ClusterRecord, event: InfraMissingObserved) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROYED,
        extra_effects=(_cancel_all_timers(record.id), _cluster_gone_cascade(record, event)),
    )


def _cluster_destroy_scheduled_destroy_due(record: ClusterRecord, event: DestroyDue) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROYING,
        extra_effects=(
            RunWorkflow(
                workflow="destroy", cluster_id=record.id,
                args={"trigger": event.trigger, "snapshot": event.snapshot},  # DR-0040, DR-0043
            ),
        ),
    )


def _cluster_destroy_scheduled_destroy_cancelled(record: ClusterRecord, event: DestroyCancelled) -> TransitionResult:
    if record.pre_destroy_state is None:
        raise InvalidTransition(f"cluster {record.id} has no pre_destroy_state to cancel back to")
    target = record.pre_destroy_state
    extra: list[Effect] = [_cancel_timer(record.id, "destroy")]
    if target == ClusterState.ACTIVE and record.expires_at is not None:
        extra.append(_ttl_timer(record.id, record.expires_at))
    return _cluster_result(
        record, event, state=target, pre_destroy_state=None, extra_effects=tuple(extra)
    )


def _cluster_destroy_scheduled_infra_missing_observed(
    record: ClusterRecord, event: InfraMissingObserved
) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROYED,
        extra_effects=(_cancel_all_timers(record.id), _cluster_gone_cascade(record, event)),
    )


def _cluster_destroy_scheduled_destroy_requested_dup(
    record: ClusterRecord, event: DestroyRequested
) -> TransitionResult:
    """A re-request for an already-scheduled destroy is idempotent -- EXCEPT when it
    carries a snapshot request, which this state cannot honour and must not swallow
    (DR-0043 Erratum E1).

    Plain re-request: ignored, as it always has been. But once DR-0043 moved the
    operator's snapshot onto the event, "ignored" stopped being safe for the snapshot
    half: a TTL that scheduled the destroy, followed by an operator's
    `DELETE ?snapshot_before_destroy=true` before the timer fires, would drop the
    snapshot silently while the API still answered 200 -- exactly the silent data loss
    DR-0020 exists to prevent (`api/routers/clusters.py`: "silently accepting the flag
    and skipping the snapshot is indistinguishable from silent data loss").

    DR-0043 originally specified MERGING the request into the armed timer -- re-arming
    with `snapshot=True` while keeping the existing `trigger`. That is not
    implementable here and the DR was wrong to ask for it: this machine is pure and
    sees only `record` and `event`, `ClusterRecord` carries no record of WHY the
    destroy was scheduled, and `TimerRepository.upsert` replaces the armed `event`
    wholesale rather than merging it. Re-arming blind would overwrite a TTL's
    `trigger="ttl_expiry"` with `"operator"`, silently disabling the profile-gated
    auto-snapshot DR-0040 delivers -- trading one silent loss for another. Carrying
    the trigger on `ClusterRecord` would need a new column and a migration, for a
    window that is about one timer poll wide (both routes arm `fire_at` at "now").

    So it raises instead. The operator learns their snapshot did not happen, which is
    the whole point, and `InvalidTransition` already maps to 409 at the router. The
    recourse is `POST /api/snapshots`, which takes one directly and does not need the
    destroy to be pending."""
    _check_discovered_guard(record, event)  # idempotent re-request, guard still applies
    if event.snapshot:
        raise InvalidTransition(
            f"cluster {record.id} already has a destroy scheduled, so "
            f"snapshot_before_destroy cannot be added to it (actor={event.actor!r}) -- "
            f"take one directly with POST /api/snapshots instead"
        )
    return _ignore(record, event)


def _cluster_destroying_destroy_succeeded(record: ClusterRecord, event: DestroySucceeded) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROYED,
        extra_effects=(_cancel_all_timers(record.id), _cluster_gone_cascade(record, event)),
    )


def _cluster_destroying_destroy_failed(record: ClusterRecord, event: DestroyFailed) -> TransitionResult:
    return _cluster_result(record, event, state=ClusterState.DESTROY_FAILED, failure_reason=event.reason)


def _cluster_destroying_infra_missing_observed(record: ClusterRecord, event: InfraMissingObserved) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROYED,
        extra_effects=(_cancel_all_timers(record.id), _cluster_gone_cascade(record, event)),
    )


def _cluster_failed_retry_requested(record: ClusterRecord, event: RetryRequested) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.PROVISIONING,
        failure_reason=None,
        extra_effects=(RunWorkflow(workflow="provision", cluster_id=record.id),),
    )


def _cluster_failed_destroy_requested(record: ClusterRecord, event: DestroyRequested) -> TransitionResult:
    _check_discovered_guard(record, event)
    fire_at = event.due_at or event.at
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=ClusterState.FAILED,
        extra_effects=(
            _cancel_timer(record.id, "ttl"),
            _destroy_timer(record.id, fire_at, snapshot=event.snapshot),  # DR-0043
        ),
    )


def _cluster_failed_ttl_expired(record: ClusterRecord, event: TtlExpired) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=ClusterState.FAILED,
        extra_effects=(_destroy_timer(record.id, event.at, trigger="ttl_expiry"),),
    )


def _cluster_failed_infra_missing_observed(record: ClusterRecord, event: InfraMissingObserved) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROYED,
        extra_effects=(_cancel_all_timers(record.id), _cluster_gone_cascade(record, event)),
    )


def _cluster_destroy_failed_destroy_requested(record: ClusterRecord, event: DestroyRequested) -> TransitionResult:
    _check_discovered_guard(record, event)
    fire_at = event.due_at or event.at
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=ClusterState.DESTROY_FAILED,
        extra_effects=(_destroy_timer(record.id, fire_at, snapshot=event.snapshot),),  # DR-0043
    )


def _cluster_destroy_failed_adopt_requested(record: ClusterRecord, event: AdoptRequested) -> TransitionResult:
    extra: tuple[Effect, ...] = ()
    if record.expires_at is not None:
        extra = (_ttl_timer(record.id, record.expires_at),)
    return _cluster_result(
        record,
        event,
        state=ClusterState.ACTIVE,
        failure_reason=None,
        pre_destroy_state=None,
        extra_effects=extra,
    )


def _cluster_destroy_failed_infra_missing_observed(
    record: ClusterRecord, event: InfraMissingObserved
) -> TransitionResult:
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROYED,
        extra_effects=(_cancel_all_timers(record.id), _cluster_gone_cascade(record, event)),
    )


def _cluster_destroyed_destroy_requested(record: ClusterRecord, event: DestroyRequested) -> TransitionResult:
    _check_discovered_guard(record, event)
    fire_at = event.due_at or event.at
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=ClusterState.DESTROYED,
        extra_effects=(_destroy_timer(record.id, fire_at, snapshot=event.snapshot),),  # DR-0043
    )


def _cluster_destroyed_adopt_requested(record: ClusterRecord, event: AdoptRequested) -> TransitionResult:
    extra: tuple[Effect, ...] = ()
    if record.expires_at is not None:
        extra = (_ttl_timer(record.id, record.expires_at),)
    return _cluster_result(record, event, state=ClusterState.ACTIVE, extra_effects=extra)


def _cluster_destroyed_infra_running_observed(record: ClusterRecord, event: InfraRunningObserved) -> TransitionResult:
    return _cluster_result(record, event, state=ClusterState.ZOMBIE)


def _cluster_zombie_destroy_requested(record: ClusterRecord, event: DestroyRequested) -> TransitionResult:
    _check_discovered_guard(record, event)
    fire_at = event.due_at or event.at
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=ClusterState.ZOMBIE,
        extra_effects=(_destroy_timer(record.id, fire_at, snapshot=event.snapshot),),  # DR-0043
    )


def _cluster_zombie_adopt_requested(record: ClusterRecord, event: AdoptRequested) -> TransitionResult:
    extra: tuple[Effect, ...] = ()
    if record.expires_at is not None:
        extra = (_ttl_timer(record.id, record.expires_at),)
    return _cluster_result(record, event, state=ClusterState.ACTIVE, extra_effects=extra)


def _cluster_zombie_infra_missing_observed(record: ClusterRecord, event: InfraMissingObserved) -> TransitionResult:
    # zombie died on its own; no Casc -- deployments were cascaded on first DESTROYED
    return _cluster_result(record, event, state=ClusterState.DESTROYED)


def _cluster_unmanaged_adopt_requested(record: ClusterRecord, event: AdoptRequested) -> TransitionResult:
    # origin stays DISCOVERED, so the destroy guard keeps protecting it
    return _cluster_result(record, event, state=ClusterState.ACTIVE)


def _cluster_unmanaged_destroy_requested(record: ClusterRecord, event: DestroyRequested) -> TransitionResult:
    # NOT dagger-marked in §G: manual cleanup, unguarded (matches v1's ACTIVE-only
    # restriction -- the discovered-cluster guard was only ever wired at v1's ACTIVE
    # destroy site).
    fire_at = event.due_at or event.at
    return _cluster_result(
        record,
        event,
        state=ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=ClusterState.UNMANAGED,
        extra_effects=(_destroy_timer(record.id, fire_at, snapshot=event.snapshot),),  # DR-0043
    )


def _cluster_unmanaged_infra_missing_observed(record: ClusterRecord, event: InfraMissingObserved) -> TransitionResult:
    # record hygiene; no Casc -- discovered clusters have no managed deployments
    return _cluster_result(record, event, state=ClusterState.DESTROYED)


_CLUSTER_TABLE_RAW: dict[tuple[ClusterState, type], Callable] = {
    (ClusterState.NEW, CreateRequested): _cluster_new_create_requested,
    (ClusterState.NEW, Discovered): _cluster_new_discovered,
    (ClusterState.PROVISIONING, ProvisionSucceeded): _cluster_provisioning_provision_succeeded,
    (ClusterState.PROVISIONING, ProvisionFailed): _cluster_provisioning_provision_failed,
    (ClusterState.PROVISIONING, InfraAllocated): _cluster_provisioning_infra_allocated,
    (ClusterState.PROVISIONING, EndpointReady): _cluster_provisioning_endpoint_ready,
    (ClusterState.ACTIVE, DestroyRequested): _cluster_active_destroy_requested,
    (ClusterState.ACTIVE, TtlExpired): _cluster_active_ttl_expired,
    (ClusterState.ACTIVE, HealthCheckFailed): _cluster_active_health_check_failed,
    (ClusterState.ACTIVE, InfraMissingObserved): _cluster_active_infra_missing_observed,
    (ClusterState.ACTIVE, ProvisionSucceeded): _ignore,  # duplicate report -- gotcha 2's law, generalized
    (ClusterState.ACTIVE, DestroyCancelled): _ignore,  # idempotent double-cancel
    (ClusterState.DESTROY_SCHEDULED, DestroyDue): _cluster_destroy_scheduled_destroy_due,
    (ClusterState.DESTROY_SCHEDULED, DestroyCancelled): _cluster_destroy_scheduled_destroy_cancelled,
    (ClusterState.DESTROY_SCHEDULED, InfraMissingObserved): _cluster_destroy_scheduled_infra_missing_observed,
    (ClusterState.DESTROY_SCHEDULED, DestroyRequested): _cluster_destroy_scheduled_destroy_requested_dup,
    (ClusterState.DESTROYING, DestroySucceeded): _cluster_destroying_destroy_succeeded,
    (ClusterState.DESTROYING, DestroyFailed): _cluster_destroying_destroy_failed,
    (ClusterState.DESTROYING, InfraMissingObserved): _cluster_destroying_infra_missing_observed,
    (ClusterState.FAILED, RetryRequested): _cluster_failed_retry_requested,
    (ClusterState.FAILED, DestroyRequested): _cluster_failed_destroy_requested,
    (ClusterState.FAILED, TtlExpired): _cluster_failed_ttl_expired,
    (ClusterState.FAILED, InfraMissingObserved): _cluster_failed_infra_missing_observed,
    (ClusterState.DESTROY_FAILED, DestroyRequested): _cluster_destroy_failed_destroy_requested,
    (ClusterState.DESTROY_FAILED, AdoptRequested): _cluster_destroy_failed_adopt_requested,
    (ClusterState.DESTROY_FAILED, InfraMissingObserved): _cluster_destroy_failed_infra_missing_observed,
    (ClusterState.DESTROYED, DestroyRequested): _cluster_destroyed_destroy_requested,
    (ClusterState.DESTROYED, AdoptRequested): _cluster_destroyed_adopt_requested,
    (ClusterState.DESTROYED, InfraRunningObserved): _cluster_destroyed_infra_running_observed,
    (ClusterState.ZOMBIE, DestroyRequested): _cluster_zombie_destroy_requested,
    (ClusterState.ZOMBIE, AdoptRequested): _cluster_zombie_adopt_requested,
    (ClusterState.ZOMBIE, InfraMissingObserved): _cluster_zombie_infra_missing_observed,
    (ClusterState.UNMANAGED, AdoptRequested): _cluster_unmanaged_adopt_requested,
    (ClusterState.UNMANAGED, DestroyRequested): _cluster_unmanaged_destroy_requested,
    (ClusterState.UNMANAGED, InfraMissingObserved): _cluster_unmanaged_infra_missing_observed,
    # DR-0031 -- DeployRequested reaches the CLUSTER aggregate as well as the deployment
    # (the escalation lives in Dispatcher.apply, which is the only component that can see
    # both). Every state is spelled out rather than left to _fill_defaults, because
    # DeployRequested is a `Command` and the default for an undefined (state, Command)
    # pair is _invalid -- i.e. silently omitting a row here would make a perfectly
    # ordinary deploy request RAISE. ACTIVE is the only row that does anything; every
    # other state _ignore's, which reproduces today's behavior exactly (today the event
    # does not reach this table at all), making this change strictly additive.
    (ClusterState.NEW, DeploymentPending): _invalid,  # pre-persistence; no row exists to carry deployments
    (ClusterState.PROVISIONING, DeploymentPending): _ignore,  # the NORMAL new-cluster path:
    #   ProvisionSucceeded's own cascade is what will advance this deployment shortly.
    (ClusterState.ACTIVE, DeploymentPending): _cluster_active_deployment_pending,  # <- the DR-0031 fix
    (ClusterState.DESTROY_SCHEDULED, DeploymentPending): _ignore,  # deliberately NOT advanced: the
    #   cluster is on its way out, and ClusterGone will reap the pending deployment.
    (ClusterState.DESTROYING, DeploymentPending): _ignore,
    (ClusterState.DESTROYED, DeploymentPending): _ignore,
    (ClusterState.DESTROY_FAILED, DeploymentPending): _ignore,
    (ClusterState.FAILED, DeploymentPending): _ignore,  # smoke 4 confirmed the existing behavior is
    #   right: the deployment waits, and reconciliation's reap + ClusterGone destroys it.
    (ClusterState.ZOMBIE, DeploymentPending): _ignore,
    (ClusterState.UNMANAGED, DeploymentPending): _ignore,
}

_CLUSTER_EVENT_TYPES: tuple[type, ...] = (
    CreateRequested,
    Discovered,
    RetryRequested,
    AdoptRequested,
    DestroyRequested,
    DestroyCancelled,
    TtlExpired,
    DestroyDue,
    ProvisionSucceeded,
    ProvisionFailed,
    DestroySucceeded,
    DestroyFailed,
    InfraRunningObserved,
    InfraMissingObserved,
    HealthCheckFailed,
    InfraAllocated,
    EndpointReady,
    DeploymentPending,  # DR-0031 E1: see _cluster_active_deployment_pending
)

CLUSTER_TABLE = _fill_defaults(
    _CLUSTER_TABLE_RAW, tuple(ClusterState), _CLUSTER_EVENT_TYPES, birth_state=ClusterState.NEW
)


# ---------------------------------------------------------------------------
# Deployment table (docs/design/seam-a-core.md §H, complete 9 x 8,
# amended by coherence review Conflict 8 (RollbackFinished, total-Ignore) and
# Conflict 12 (DEPLOYING x CancelRequested / ClusterGone) -> 9 x 9)
# ---------------------------------------------------------------------------


def _deployment_new_deploy_requested(record: DeploymentRecord, event: DeployRequested) -> TransitionResult:
    return _deployment_result(
        record,
        event,
        state=DeploymentState.PENDING,
        birth=True,
        notify_old_status="",
        spec_ref=event.spec_ref,
    )


def _deployment_new_deploy_rejected(record: DeploymentRecord, event: DeployRejected) -> TransitionResult:
    return _deployment_result(
        record,
        event,
        state=DeploymentState.REJECTED,
        birth=True,
        notify_old_status="",
        failure_reason=event.reason,
    )


def _deployment_pending_cluster_ready(record: DeploymentRecord, event: ClusterReady) -> TransitionResult:
    return _deployment_result(
        record,
        event,
        state=DeploymentState.DEPLOYING,
        extra_effects=(
            RunWorkflow(workflow="deploy", cluster_id=record.cluster_id, deployment_id=record.id),
        ),
    )


def _deployment_pending_cancel_requested(record: DeploymentRecord, event: CancelRequested) -> TransitionResult:
    return _deployment_result(record, event, state=DeploymentState.CANCELLED)


def _deployment_pending_cluster_gone(record: DeploymentRecord, event: ClusterGone) -> TransitionResult:
    return _deployment_result(record, event, state=DeploymentState.DESTROYED)


def _deployment_deploying_deploy_succeeded(record: DeploymentRecord, event: DeploySucceeded) -> TransitionResult:
    cascade = Cascade(
        cluster_id=record.cluster_id,
        where_state=frozenset({DeploymentState.ACTIVE}),
        event=SupersededBy(new_deployment_id=record.id, at=event.at, actor="cluster-machine"),
        except_id=record.id,
    )
    return _deployment_result(
        record,
        event,
        state=DeploymentState.ACTIVE,
        resolved_images=event.resolved_images,
        extra_effects=(cascade,),
    )


def _deployment_deploying_deploy_failed(record: DeploymentRecord, event: DeployFailed) -> TransitionResult:
    # cluster record untouched: gotcha 1's UX (infra fine, redeployable), minus the state smear
    return _deployment_result(record, event, state=DeploymentState.FAILED, failure_reason=event.reason)


def _deployment_deploying_cancel_requested(record: DeploymentRecord, event: CancelRequested) -> TransitionResult:
    # Conflict 12: CancelWorkflow(deploy) THEN RunWorkflow(rollback, deployment_id=self.id), in that
    # order -- the drain-lane admitter processes them in seq order, so rollback waits for the
    # cancelled deploy run to reach terminal.
    cancel_deploy = CancelWorkflow(workflow="deploy", cluster_id=record.cluster_id, deployment_id=record.id)
    run_rollback = RunWorkflow(workflow="rollback", cluster_id=record.cluster_id, deployment_id=record.id)
    return _deployment_result(
        record, event, state=DeploymentState.CANCELLED, extra_effects=(cancel_deploy, run_rollback)
    )


def _deployment_deploying_cluster_gone(record: DeploymentRecord, event: ClusterGone) -> TransitionResult:
    # Conflict 12: CancelWorkflow(deploy) only -- no rollback on a dying cluster (v1 parity)
    cancel_deploy = CancelWorkflow(workflow="deploy", cluster_id=record.cluster_id, deployment_id=record.id)
    return _deployment_result(record, event, state=DeploymentState.DESTROYED, extra_effects=(cancel_deploy,))


def _deployment_active_superseded_by(record: DeploymentRecord, event: SupersededBy) -> TransitionResult:
    return _deployment_result(
        record, event, state=DeploymentState.SUPERSEDED, superseded_by=event.new_deployment_id
    )


def _deployment_cluster_gone(record: DeploymentRecord, event: ClusterGone) -> TransitionResult:
    # v1 bulk _mark_deployments_destroyed, per-record and audited, incl. per-deployment Notify
    return _deployment_result(record, event, state=DeploymentState.DESTROYED)


_DEPLOYMENT_TABLE_RAW: dict[tuple[DeploymentState, type], Callable] = {
    (DeploymentState.NEW, DeployRequested): _deployment_new_deploy_requested,
    (DeploymentState.NEW, DeployRejected): _deployment_new_deploy_rejected,
    # Conflict 8: RollbackFinished is deliberately total-Ignore -- overrides the NEW-state default
    # (which would otherwise mark every unlisted, non-birth event Invalid).
    (DeploymentState.NEW, RollbackFinished): _ignore,
    (DeploymentState.PENDING, ClusterReady): _deployment_pending_cluster_ready,
    (DeploymentState.PENDING, CancelRequested): _deployment_pending_cancel_requested,
    (DeploymentState.PENDING, ClusterGone): _deployment_pending_cluster_gone,
    (DeploymentState.DEPLOYING, DeploySucceeded): _deployment_deploying_deploy_succeeded,
    (DeploymentState.DEPLOYING, DeployFailed): _deployment_deploying_deploy_failed,
    (DeploymentState.DEPLOYING, CancelRequested): _deployment_deploying_cancel_requested,
    (DeploymentState.DEPLOYING, ClusterGone): _deployment_deploying_cluster_gone,
    (DeploymentState.ACTIVE, SupersededBy): _deployment_active_superseded_by,
    (DeploymentState.ACTIVE, DeploySucceeded): _ignore,  # v1's "monitor job won race", gotcha 2
    (DeploymentState.ACTIVE, DeployFailed): _ignore,  # stale failure after success
    (DeploymentState.ACTIVE, ClusterGone): _deployment_cluster_gone,
    (DeploymentState.FAILED, ClusterGone): _deployment_cluster_gone,
    (DeploymentState.SUPERSEDED, ClusterGone): _deployment_cluster_gone,
    (DeploymentState.CANCELLED, ClusterGone): _deployment_cluster_gone,
    (DeploymentState.REJECTED, ClusterGone): _deployment_cluster_gone,
    # DESTROYED: terminal; every event, Command or not, is left to the totality defaults --
    # non-Command => Ignore ("duplicate cascades harmless"), Command => InvalidTransition.
}

_DEPLOYMENT_EVENT_TYPES: tuple[type, ...] = (
    DeployRequested,
    DeployRejected,
    CancelRequested,
    ClusterReady,
    DeploySucceeded,
    DeployFailed,
    SupersededBy,
    ClusterGone,
    RollbackFinished,
)

DEPLOYMENT_TABLE = _fill_defaults(
    _DEPLOYMENT_TABLE_RAW, tuple(DeploymentState), _DEPLOYMENT_EVENT_TYPES, birth_state=DeploymentState.NEW
)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def transition(record: ClusterRecord | DeploymentRecord, event: ClusterEvent | DeploymentEvent) -> TransitionResult:
    """Pure. No IO, no clock, no locks. Total over (state x event-type).

    Raises ``InvalidTransition`` for illegal Commands, for privileged-actor
    violations on ``Observation`` events, and for the discovered-cluster destroy
    guard. Never raises for a Report/TimerFired/Cascaded outside NEW.
    """
    if isinstance(event, Observation) and event.actor not in _PRIVILEGED_OBSERVATION_ACTORS:
        raise InvalidTransition(
            f"Observation {type(event).__name__} requires actor in "
            f"{sorted(_PRIVILEGED_OBSERVATION_ACTORS)}, got {event.actor!r}"
        )
    if isinstance(record, ClusterRecord):
        table = CLUSTER_TABLE
    elif isinstance(record, DeploymentRecord):
        table = DEPLOYMENT_TABLE
    else:
        raise TypeError(f"transition() requires a ClusterRecord or DeploymentRecord, got {type(record)!r}")
    rule = table[(record.state, type(event))]
    return rule(record, event)
