"""Exact-equality effect tests for every row of docs/design/seam-a-core.md §G
(cluster transition table), amended by docs/design/coherence-review.md Conflict 8
(the two new PROVISIONING rows: InfraAllocated, EndpointReady).

Binding test contract: docs/design/seam-a-core.md §K. No unittest.mock. Every
assertion is `result.effects == (...)` against frozen dataclasses, and
`result.record == <expected>` built via `dataclasses.replace` on the input record
(never by re-deriving from the code under test).

Every row checked here was cross-referenced against §G row-by-row while writing
this file; no spec/implementation deviation was found, so every test asserts
against the implementation's actual behavior (which is also the spec's).
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta

import pytest

from seedpod.core import effects as ef
from seedpod.core import events as ev
from seedpod.core import records as rec
from seedpod.core.machine import InvalidTransition, TransitionResult, transition
from tests.core.builders import AN_EVENT, AT, a_cluster, assert_an_event_covers_registry

_ALL_DEPLOYMENT_STATES_EXCEPT_DESTROYED = frozenset(
    s for s in rec.DeploymentState if s is not rec.DeploymentState.DESTROYED
)


def _cn(new: rec.ClusterRecord, old_status: str) -> ef.Notify:
    """The v1-shaped cluster_state_changed Notify payload."""
    return ef.Notify(
        topic="cluster_state_changed",
        payload={"cluster_id": new.id, "old_status": old_status, "new_status": new.state.value},
        environment=new.environment,
    )


def _persist(new: rec.ClusterRecord, expected_version: int | None) -> ef.Persist:
    return ef.Persist(record=new, expected_version=expected_version)


def _gone_cascade(cluster_id: str, at: datetime, *, except_id: str | None = None) -> ef.Cascade:
    return ef.Cascade(
        cluster_id=cluster_id,
        where_state=_ALL_DEPLOYMENT_STATES_EXCEPT_DESTROYED,
        event=ev.ClusterGone(at=at, actor="cluster-machine"),
        except_id=except_id,
    )


def test_an_event_covers_every_registered_kind():
    """Meta-test required by Seam A §K: AN_EVENT covers every registered kind."""
    assert_an_event_covers_registry()


# ---------------------------------------------------------------------------
# NEW
# ---------------------------------------------------------------------------


def test_new_create_requested_births_provisioning():
    old = a_cluster(state=rec.ClusterState.NEW, version=0)
    event = AN_EVENT[ev.CreateRequested]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.PROVISIONING, version=1)
    assert result.record == new
    assert result.effects == (
        _persist(new, None),
        _cn(new, ""),
        ef.RunWorkflow(workflow="provision", cluster_id=new.id),
    )


def test_new_discovered_births_unmanaged():
    old = a_cluster(state=rec.ClusterState.NEW, version=0)
    event = AN_EVENT[ev.Discovered]
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.UNMANAGED,
        version=1,
        origin=rec.Origin.DISCOVERED,
        provider=event.observed.provider,
        public_ip=event.observed.public_ip,
        provider_resources=event.observed.provider_resources,
    )
    assert result.record == new
    assert result.effects == (_persist(new, None), _cn(new, ""))


# ---------------------------------------------------------------------------
# PROVISIONING
# ---------------------------------------------------------------------------


def test_provisioning_provision_succeeded_no_ttl():
    old = a_cluster(state=rec.ClusterState.PROVISIONING, expires_at=None)
    event = AN_EVENT[ev.ProvisionSucceeded]
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.ACTIVE,
        version=old.version + 1,
        public_ip=event.public_ip,
        kubeconfig_ref=event.kubeconfig_ref,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "provisioning"),
        ef.Cascade(
            cluster_id=new.id,
            where_state=frozenset({rec.DeploymentState.PENDING}),
            event=ev.ClusterReady(at=event.at, actor="cluster-machine"),
        ),
    )


def test_provisioning_provision_succeeded_with_ttl_arms_timer():
    expires_at = AT + timedelta(days=1)
    old = a_cluster(state=rec.ClusterState.PROVISIONING, expires_at=expires_at)
    event = AN_EVENT[ev.ProvisionSucceeded]
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.ACTIVE,
        version=old.version + 1,
        public_ip=event.public_ip,
        kubeconfig_ref=event.kubeconfig_ref,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "provisioning"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="ttl",
            fire_at=expires_at,
            event=ev.TtlExpired(at=expires_at, actor="timer:ttl"),
        ),
        ef.Cascade(
            cluster_id=new.id,
            where_state=frozenset({rec.DeploymentState.PENDING}),
            event=ev.ClusterReady(at=event.at, actor="cluster-machine"),
        ),
    )


def test_provisioning_provision_failed_no_ttl():
    old = a_cluster(state=rec.ClusterState.PROVISIONING, expires_at=None)
    event = AN_EVENT[ev.ProvisionFailed]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.FAILED, version=old.version + 1, failure_reason=event.reason
    )
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "provisioning"))


def test_provisioning_provision_failed_with_ttl_still_arms_timer():
    """Failed clusters get TTL auto-cleanup too (closes a v1 leak)."""
    expires_at = AT + timedelta(days=1)
    old = a_cluster(state=rec.ClusterState.PROVISIONING, expires_at=expires_at)
    event = AN_EVENT[ev.ProvisionFailed]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.FAILED, version=old.version + 1, failure_reason=event.reason
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "provisioning"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="ttl",
            fire_at=expires_at,
            event=ev.TtlExpired(at=expires_at, actor="timer:ttl"),
        ),
    )


def test_provisioning_infra_allocated_merges_resources_same_state():
    """Conflict 8 new row: P (merge provider_resources), N; state stays PROVISIONING."""
    old = a_cluster(state=rec.ClusterState.PROVISIONING, provider_resources={"droplet_id": "1"})
    event = ev.InfraAllocated(at=AT, actor="engine:run:1", resource_ids={"volume_id": "2"})
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.PROVISIONING,
        version=old.version + 1,
        provider_resources={"droplet_id": "1", "volume_id": "2"},
    )
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "provisioning"))


def test_provisioning_infra_allocated_overwrites_same_key():
    old = a_cluster(state=rec.ClusterState.PROVISIONING, provider_resources={"droplet_id": "1"})
    event = ev.InfraAllocated(at=AT, actor="engine:run:1", resource_ids={"droplet_id": "2"})
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.PROVISIONING, version=old.version + 1, provider_resources={"droplet_id": "2"}
    )
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "provisioning"))


def test_provisioning_endpoint_ready_sets_public_ip_same_state():
    """Conflict 8 new row: P (sets public_ip), N; state stays PROVISIONING."""
    old = a_cluster(state=rec.ClusterState.PROVISIONING, public_ip=None)
    event = AN_EVENT[ev.EndpointReady]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.PROVISIONING, version=old.version + 1, public_ip=event.public_ip
    )
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "provisioning"))


# ---------------------------------------------------------------------------
# ACTIVE
# ---------------------------------------------------------------------------


def test_active_destroy_requested_due_at_set():
    old = a_cluster(state=rec.ClusterState.ACTIVE, origin=rec.Origin.MANAGED)
    due_at = AT + timedelta(hours=2)
    event = ev.DestroyRequested(at=AT, actor="api:alice", due_at=due_at)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.ACTIVE,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "active"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key="ttl"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=due_at,
            event=ev.DestroyDue(at=due_at, actor="timer:destroy"),
        ),
    )


def test_active_destroy_requested_due_at_none_falls_back_to_event_at():
    old = a_cluster(state=rec.ClusterState.ACTIVE, origin=rec.Origin.MANAGED)
    event = ev.DestroyRequested(at=AT, actor="api:alice", due_at=None)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.ACTIVE,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "active"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key="ttl"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=AT,
            event=ev.DestroyDue(at=AT, actor="timer:destroy"),
        ),
    )


def test_active_destroy_requested_discovered_without_force_raises():
    """dagger guard: origin == DISCOVERED and not event.force => InvalidTransition."""
    old = a_cluster(state=rec.ClusterState.ACTIVE, origin=rec.Origin.DISCOVERED)
    event = ev.DestroyRequested(at=AT, actor="api:alice", force=False)
    with pytest.raises(InvalidTransition):
        transition(old, event)


def test_active_destroy_requested_discovered_with_force_succeeds():
    old = a_cluster(state=rec.ClusterState.ACTIVE, origin=rec.Origin.DISCOVERED)
    event = ev.DestroyRequested(at=AT, actor="api:alice", force=True)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.ACTIVE,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "active"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key="ttl"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=AT,
            event=ev.DestroyDue(at=AT, actor="timer:destroy"),
        ),
    )


def test_active_ttl_expired_schedules_destroy():
    old = a_cluster(state=rec.ClusterState.ACTIVE)
    event = AN_EVENT[ev.TtlExpired]
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.ACTIVE,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "active"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=event.at,
            event=ev.DestroyDue(at=event.at, actor="timer:destroy", trigger="ttl_expiry"),
        ),
    )


def test_active_health_check_failed_fails_cluster_ttl_stays_armed():
    old = a_cluster(state=rec.ClusterState.ACTIVE)
    event = AN_EVENT[ev.HealthCheckFailed]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.FAILED, version=old.version + 1, failure_reason=event.reason
    )
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "active"))


def test_active_health_check_failed_requires_privileged_actor():
    old = a_cluster(state=rec.ClusterState.ACTIVE)
    event = ev.HealthCheckFailed(at=AT, actor="api:alice", reason="down")
    with pytest.raises(InvalidTransition):
        transition(old, event)


def test_active_infra_missing_observed_destroys_and_cascades():
    old = a_cluster(state=rec.ClusterState.ACTIVE)
    event = AN_EVENT[ev.InfraMissingObserved]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "active"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key=None),
        _gone_cascade(new.id, event.at),
    )


def test_active_provision_succeeded_is_ignored_as_duplicate_report():
    old = a_cluster(state=rec.ClusterState.ACTIVE)
    event = AN_EVENT[ev.ProvisionSucceeded]
    result = transition(old, event)
    assert result == TransitionResult(record=old, effects=())


def test_active_destroy_cancelled_is_ignored_idempotent():
    old = a_cluster(state=rec.ClusterState.ACTIVE)
    event = AN_EVENT[ev.DestroyCancelled]
    result = transition(old, event)
    assert result == TransitionResult(record=old, effects=())


# ---------------------------------------------------------------------------
# DESTROY_SCHEDULED
# ---------------------------------------------------------------------------


def test_destroy_scheduled_destroy_due_starts_destroying():
    old = a_cluster(state=rec.ClusterState.DESTROY_SCHEDULED, pre_destroy_state=rec.ClusterState.ACTIVE)
    event = AN_EVENT[ev.DestroyDue]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.DESTROYING, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-scheduled"),
        ef.RunWorkflow(
            workflow="destroy",
            cluster_id=new.id,
            # DR-0040 `trigger` + DR-0043 `snapshot` -- two independent questions
            # the destroy workflow needs answered, both stamped onto DestroyDue.
            args={"trigger": "operator", "snapshot": False},
        ),
    )


def test_destroy_scheduled_destroy_cancelled_returns_to_active_with_ttl():
    expires_at = AT + timedelta(days=1)
    old = a_cluster(
        state=rec.ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=rec.ClusterState.ACTIVE,
        expires_at=expires_at,
    )
    event = AN_EVENT[ev.DestroyCancelled]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.ACTIVE, version=old.version + 1, pre_destroy_state=None
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-scheduled"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key="destroy"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="ttl",
            fire_at=expires_at,
            event=ev.TtlExpired(at=expires_at, actor="timer:ttl"),
        ),
    )


def test_destroy_scheduled_destroy_cancelled_returns_to_active_without_ttl():
    old = a_cluster(
        state=rec.ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=rec.ClusterState.ACTIVE,
        expires_at=None,
    )
    event = AN_EVENT[ev.DestroyCancelled]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.ACTIVE, version=old.version + 1, pre_destroy_state=None
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-scheduled"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key="destroy"),
    )


def test_destroy_scheduled_destroy_cancelled_returns_to_failed_no_ttl_rearm():
    """pre_destroy_state=FAILED: no TTL re-arm even if expires_at is set (only ACTIVE re-arms)."""
    expires_at = AT + timedelta(days=1)
    old = a_cluster(
        state=rec.ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=rec.ClusterState.FAILED,
        expires_at=expires_at,
    )
    event = AN_EVENT[ev.DestroyCancelled]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.FAILED, version=old.version + 1, pre_destroy_state=None
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-scheduled"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key="destroy"),
    )


def test_destroy_scheduled_destroy_cancelled_returns_to_zombie():
    old = a_cluster(
        state=rec.ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=rec.ClusterState.ZOMBIE,
        expires_at=None,
    )
    event = AN_EVENT[ev.DestroyCancelled]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.ZOMBIE, version=old.version + 1, pre_destroy_state=None
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-scheduled"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key="destroy"),
    )


def test_destroy_scheduled_infra_missing_observed_destroys_and_cascades():
    old = a_cluster(state=rec.ClusterState.DESTROY_SCHEDULED, pre_destroy_state=rec.ClusterState.ACTIVE)
    event = AN_EVENT[ev.InfraMissingObserved]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-scheduled"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key=None),
        _gone_cascade(new.id, event.at),
    )


def test_destroy_scheduled_destroy_requested_is_ignored_idempotent_rerequest():
    old = a_cluster(
        state=rec.ClusterState.DESTROY_SCHEDULED, pre_destroy_state=rec.ClusterState.ACTIVE, origin=rec.Origin.MANAGED
    )
    event = AN_EVENT[ev.DestroyRequested]
    result = transition(old, event)
    assert result == TransitionResult(record=old, effects=())


def test_destroy_scheduled_destroy_requested_guard_still_applies_on_discovered():
    old = a_cluster(
        state=rec.ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=rec.ClusterState.ACTIVE,
        origin=rec.Origin.DISCOVERED,
    )
    event = ev.DestroyRequested(at=AT, actor="api:alice", force=False)
    with pytest.raises(InvalidTransition):
        transition(old, event)


def test_destroy_scheduled_destroy_requested_carrying_a_snapshot_is_refused():
    """DR-0043 Erratum E1. A plain re-request is idempotent (above); one CARRYING a
    snapshot request is not, and must not be silently swallowed.

    The armed destroy timer's event is replaced wholesale on re-arm and this machine
    cannot see what it currently holds, so honouring the request would mean re-arming
    blind -- overwriting a TTL's `trigger="ttl_expiry"` with `"operator"` and silently
    disabling the profile-gated auto-snapshot DR-0040 delivers. Refusing is the only
    honest option left: silently accepting the flag and skipping the snapshot is
    indistinguishable from silent data loss (DR-0020, via `api/routers/clusters.py`).
    `InvalidTransition` maps to 409 at the router."""
    old = a_cluster(
        state=rec.ClusterState.DESTROY_SCHEDULED,
        pre_destroy_state=rec.ClusterState.ACTIVE,
        origin=rec.Origin.MANAGED,
    )
    event = ev.DestroyRequested(at=AT, actor="api:alice", snapshot=True)
    with pytest.raises(InvalidTransition, match="POST /api/snapshots"):
        transition(old, event)


# ---------------------------------------------------------------------------
# DESTROYING
# ---------------------------------------------------------------------------


def test_destroying_destroy_succeeded_destroys_and_cascades():
    old = a_cluster(state=rec.ClusterState.DESTROYING)
    event = AN_EVENT[ev.DestroySucceeded]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroying"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key=None),
        _gone_cascade(new.id, event.at),
    )


def test_destroying_destroy_failed_sets_failure_reason():
    old = a_cluster(state=rec.ClusterState.DESTROYING)
    event = AN_EVENT[ev.DestroyFailed]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.DESTROY_FAILED, version=old.version + 1, failure_reason=event.reason
    )
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "destroying"))


def test_destroying_infra_missing_observed_destroys_and_cascades():
    """reconciler won the race; the late DestroySucceeded then Ignores in DESTROYED."""
    old = a_cluster(state=rec.ClusterState.DESTROYING)
    event = AN_EVENT[ev.InfraMissingObserved]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroying"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key=None),
        _gone_cascade(new.id, event.at),
    )


@pytest.mark.parametrize(
    "event",
    [
        ev.DestroyRequested(at=AT, actor="api:alice"),
        ev.DestroyCancelled(at=AT, actor="api:alice"),
        ev.AdoptRequested(at=AT, actor="api:alice"),
        ev.RetryRequested(at=AT, actor="api:alice"),
        ev.CreateRequested(at=AT, actor="api:alice"),
    ],
)
def test_destroying_commands_are_invalid_mid_destroy_not_cancellable(event):
    old = a_cluster(state=rec.ClusterState.DESTROYING)
    with pytest.raises(InvalidTransition):
        transition(old, event)


# ---------------------------------------------------------------------------
# FAILED
# ---------------------------------------------------------------------------


def test_failed_retry_requested_reprovisions():
    old = a_cluster(state=rec.ClusterState.FAILED, failure_reason="boom")
    event = AN_EVENT[ev.RetryRequested]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.ClusterState.PROVISIONING, version=old.version + 1, failure_reason=None
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "failed"),
        ef.RunWorkflow(workflow="provision", cluster_id=new.id),
    )


def test_failed_destroy_requested_due_at_set():
    old = a_cluster(state=rec.ClusterState.FAILED, origin=rec.Origin.MANAGED)
    due_at = AT + timedelta(hours=1)
    event = ev.DestroyRequested(at=AT, actor="api:alice", due_at=due_at)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.FAILED,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "failed"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key="ttl"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=due_at,
            event=ev.DestroyDue(at=due_at, actor="timer:destroy"),
        ),
    )


def test_failed_destroy_requested_due_at_none_uses_event_at():
    old = a_cluster(state=rec.ClusterState.FAILED, origin=rec.Origin.MANAGED)
    event = ev.DestroyRequested(at=AT, actor="api:alice", due_at=None)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.FAILED,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "failed"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key="ttl"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=AT,
            event=ev.DestroyDue(at=AT, actor="timer:destroy"),
        ),
    )


def test_failed_destroy_requested_discovered_without_force_raises():
    old = a_cluster(state=rec.ClusterState.FAILED, origin=rec.Origin.DISCOVERED)
    event = ev.DestroyRequested(at=AT, actor="api:alice", force=False)
    with pytest.raises(InvalidTransition):
        transition(old, event)


def test_failed_ttl_expired_schedules_destroy():
    old = a_cluster(state=rec.ClusterState.FAILED)
    event = AN_EVENT[ev.TtlExpired]
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.FAILED,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "failed"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=event.at,
            event=ev.DestroyDue(at=event.at, actor="timer:destroy", trigger="ttl_expiry"),
        ),
    )


def test_failed_infra_missing_observed_destroys_and_cascades():
    old = a_cluster(state=rec.ClusterState.FAILED)
    event = AN_EVENT[ev.InfraMissingObserved]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "failed"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key=None),
        _gone_cascade(new.id, event.at),
    )


# ---------------------------------------------------------------------------
# DESTROY_FAILED
# ---------------------------------------------------------------------------


def test_destroy_failed_destroy_requested_retries_destruction_no_ttl_cancel():
    """No CT(ttl) here (spec row lists only ST): DESTROY_FAILED clusters have no armed TTL timer."""
    old = a_cluster(state=rec.ClusterState.DESTROY_FAILED, origin=rec.Origin.MANAGED)
    due_at = AT + timedelta(hours=3)
    event = ev.DestroyRequested(at=AT, actor="api:alice", due_at=due_at)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.DESTROY_FAILED,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-failed"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=due_at,
            event=ev.DestroyDue(at=due_at, actor="timer:destroy"),
        ),
    )


def test_destroy_failed_destroy_requested_due_at_none_uses_event_at():
    old = a_cluster(state=rec.ClusterState.DESTROY_FAILED, origin=rec.Origin.MANAGED)
    event = ev.DestroyRequested(at=AT, actor="api:alice", due_at=None)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.DESTROY_FAILED,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-failed"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=AT,
            event=ev.DestroyDue(at=AT, actor="timer:destroy"),
        ),
    )


def test_destroy_failed_adopt_requested_resurrects_with_ttl():
    expires_at = AT + timedelta(days=1)
    old = a_cluster(
        state=rec.ClusterState.DESTROY_FAILED,
        failure_reason="boom",
        pre_destroy_state=rec.ClusterState.ACTIVE,
        expires_at=expires_at,
    )
    event = AN_EVENT[ev.AdoptRequested]
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.ACTIVE,
        version=old.version + 1,
        failure_reason=None,
        pre_destroy_state=None,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-failed"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="ttl",
            fire_at=expires_at,
            event=ev.TtlExpired(at=expires_at, actor="timer:ttl"),
        ),
    )


def test_destroy_failed_adopt_requested_resurrects_without_ttl():
    old = a_cluster(
        state=rec.ClusterState.DESTROY_FAILED,
        failure_reason="boom",
        pre_destroy_state=rec.ClusterState.ACTIVE,
        expires_at=None,
    )
    event = AN_EVENT[ev.AdoptRequested]
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.ACTIVE,
        version=old.version + 1,
        failure_reason=None,
        pre_destroy_state=None,
    )
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "destroy-failed"))


def test_destroy_failed_infra_missing_observed_destroys_and_cascades():
    old = a_cluster(state=rec.ClusterState.DESTROY_FAILED)
    event = AN_EVENT[ev.InfraMissingObserved]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroy-failed"),
        ef.CancelTimer(aggregate_type="cluster", aggregate_id=new.id, timer_key=None),
        _gone_cascade(new.id, event.at),
    )


# ---------------------------------------------------------------------------
# DESTROYED
# ---------------------------------------------------------------------------


def test_destroyed_destroy_requested_reschedules_zombie_cleanup():
    old = a_cluster(state=rec.ClusterState.DESTROYED, origin=rec.Origin.MANAGED)
    due_at = AT + timedelta(hours=1)
    event = ev.DestroyRequested(at=AT, actor="reconciler", due_at=due_at)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.DESTROYED,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroyed"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=due_at,
            event=ev.DestroyDue(at=due_at, actor="timer:destroy"),
        ),
    )


def test_destroyed_adopt_requested_rehabilitates_with_ttl():
    expires_at = AT + timedelta(days=1)
    old = a_cluster(state=rec.ClusterState.DESTROYED, expires_at=expires_at)
    event = AN_EVENT[ev.AdoptRequested]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.ACTIVE, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "destroyed"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="ttl",
            fire_at=expires_at,
            event=ev.TtlExpired(at=expires_at, actor="timer:ttl"),
        ),
    )


def test_destroyed_adopt_requested_rehabilitates_without_ttl():
    old = a_cluster(state=rec.ClusterState.DESTROYED, expires_at=None)
    event = AN_EVENT[ev.AdoptRequested]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.ACTIVE, version=old.version + 1)
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "destroyed"))


def test_destroyed_infra_running_observed_becomes_zombie():
    old = a_cluster(state=rec.ClusterState.DESTROYED)
    event = AN_EVENT[ev.InfraRunningObserved]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.ZOMBIE, version=old.version + 1)
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "destroyed"))


def test_destroyed_infra_running_observed_requires_privileged_actor():
    old = a_cluster(state=rec.ClusterState.DESTROYED)
    event = ev.InfraRunningObserved(at=AT, actor="api:alice")
    with pytest.raises(InvalidTransition):
        transition(old, event)


@pytest.mark.parametrize(
    "event",
    [
        ev.DestroySucceeded(at=AT, actor="engine:run:1"),
        ev.InfraMissingObserved(at=AT, actor="reconciler"),
        ev.DestroyDue(at=AT, actor="timer:destroy"),
        ev.TtlExpired(at=AT, actor="timer:ttl"),
    ],
)
def test_destroyed_late_duplicates_and_stale_timers_are_ignored(event):
    old = a_cluster(state=rec.ClusterState.DESTROYED)
    result = transition(old, event)
    assert result == TransitionResult(record=old, effects=())


# ---------------------------------------------------------------------------
# ZOMBIE
# ---------------------------------------------------------------------------


def test_zombie_destroy_requested_schedules_cleanup():
    old = a_cluster(state=rec.ClusterState.ZOMBIE, origin=rec.Origin.MANAGED)
    due_at = AT + timedelta(hours=1)
    event = ev.DestroyRequested(at=AT, actor="api:alice", due_at=due_at)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.ZOMBIE,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "zombie"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=due_at,
            event=ev.DestroyDue(at=due_at, actor="timer:destroy"),
        ),
    )


def test_zombie_adopt_requested_rehabilitates_with_ttl():
    expires_at = AT + timedelta(days=1)
    old = a_cluster(state=rec.ClusterState.ZOMBIE, expires_at=expires_at)
    event = AN_EVENT[ev.AdoptRequested]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.ACTIVE, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "zombie"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="ttl",
            fire_at=expires_at,
            event=ev.TtlExpired(at=expires_at, actor="timer:ttl"),
        ),
    )


def test_zombie_adopt_requested_rehabilitates_without_ttl():
    old = a_cluster(state=rec.ClusterState.ZOMBIE, expires_at=None)
    event = AN_EVENT[ev.AdoptRequested]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.ACTIVE, version=old.version + 1)
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "zombie"))


def test_zombie_infra_missing_observed_dies_on_own_no_cascade():
    """Deployments were already cascaded on first DESTROYED, so no Casc here, and no CT."""
    old = a_cluster(state=rec.ClusterState.ZOMBIE)
    event = AN_EVENT[ev.InfraMissingObserved]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "zombie"))


# ---------------------------------------------------------------------------
# UNMANAGED
# ---------------------------------------------------------------------------


def test_unmanaged_adopt_requested_activates_origin_stays_discovered():
    old = a_cluster(state=rec.ClusterState.UNMANAGED, origin=rec.Origin.DISCOVERED)
    event = AN_EVENT[ev.AdoptRequested]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.ACTIVE, version=old.version + 1)
    assert result.record == new
    assert new.origin == rec.Origin.DISCOVERED  # guard keeps protecting it
    assert result.effects == (_persist(new, old.version), _cn(new, "unmanaged"))


def test_unmanaged_destroy_requested_is_unguarded_manual_cleanup():
    """NOT dagger-marked: manual cleanup, unguarded even though origin is DISCOVERED."""
    old = a_cluster(state=rec.ClusterState.UNMANAGED, origin=rec.Origin.DISCOVERED)
    due_at = AT + timedelta(hours=1)
    event = ev.DestroyRequested(at=AT, actor="api:alice", due_at=due_at, force=False)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.UNMANAGED,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "unmanaged"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=due_at,
            event=ev.DestroyDue(at=due_at, actor="timer:destroy"),
        ),
    )


def test_unmanaged_destroy_requested_due_at_none_uses_event_at():
    old = a_cluster(state=rec.ClusterState.UNMANAGED, origin=rec.Origin.DISCOVERED)
    event = ev.DestroyRequested(at=AT, actor="api:alice", due_at=None)
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.ClusterState.DESTROY_SCHEDULED,
        version=old.version + 1,
        pre_destroy_state=rec.ClusterState.UNMANAGED,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "unmanaged"),
        ef.ScheduleTimer(
            aggregate_type="cluster",
            aggregate_id=new.id,
            timer_key="destroy",
            fire_at=AT,
            event=ev.DestroyDue(at=AT, actor="timer:destroy"),
        ),
    )


def test_unmanaged_infra_missing_observed_record_hygiene_no_cascade():
    old = a_cluster(state=rec.ClusterState.UNMANAGED)
    event = AN_EVENT[ev.InfraMissingObserved]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.ClusterState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _cn(new, "unmanaged"))


# ---------------------------------------------------------------------------
# Notify payload shape assertions (JSON-safe scalars only)
# ---------------------------------------------------------------------------


def test_active_deployment_pending_cascades_cluster_ready_to_pending_deployments():
    """DR-0031 (+ E1): the rule that makes a redeploy onto a live cluster start.

    ``ClusterReady`` otherwise has exactly ONE emitter -- the
    ``provisioning x ProvisionSucceeded`` cascade -- which an already-ACTIVE cluster can
    never re-enter, so a deployment born onto one waited forever (smoke 4, 2026-08-09).
    """
    old = a_cluster(state=rec.ClusterState.ACTIVE)
    event = ev.DeploymentPending(at=AT, actor="api:alice", deployment_id="deployment-7")
    result = transition(old, event)

    # The cluster does NOT change state; it re-announces readiness. The version still
    # bumps, because effect_id is "{aggregate}/{id}@{to_version}#{ordinal}" and must stay
    # unique across repeated deploy requests against one cluster.
    new = dataclasses.replace(old, state=rec.ClusterState.ACTIVE, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _cn(new, "active"),  # exactly one Notify per Persist -- the table's pinned law
        ef.Cascade(
            cluster_id=new.id,
            where_state=frozenset({rec.DeploymentState.PENDING}),
            event=ev.ClusterReady(at=AT, actor="cluster-machine"),
        ),
    )


@pytest.mark.parametrize(
    "state",
    [s for s in rec.ClusterState if s not in (rec.ClusterState.ACTIVE, rec.ClusterState.NEW)],
)
def test_deployment_pending_is_ignored_in_every_state_but_active(state):
    """Strictly additive: only ACTIVE reacts. PROVISIONING in particular MUST ignore --
    that is the ordinary new-cluster path, where ``ProvisionSucceeded``'s own cascade is
    what will start the deployment a few steps later. A cluster on its way out (or
    already gone) also ignores it: ``ClusterGone`` is the transition that resolves those
    deployments, and smoke 4 confirmed that reconciliation's reap does exactly that."""
    old = a_cluster(state=state)
    event = ev.DeploymentPending(at=AT, actor="api:alice", deployment_id="deployment-7")
    result = transition(old, event)
    assert result.effects == ()
    assert result.record == old  # no version bump, no audit, no SSE


def test_deployment_pending_on_a_new_cluster_is_invalid():
    """NEW is pre-persistence: no row exists that could carry deployments, so this is a
    caller bug rather than a no-op to swallow."""
    old = a_cluster(state=rec.ClusterState.NEW)
    with pytest.raises(InvalidTransition):
        transition(old, ev.DeploymentPending(at=AT, actor="api:alice", deployment_id="deployment-7"))


def test_notify_payload_values_are_json_scalars():
    old = a_cluster(state=rec.ClusterState.ACTIVE)
    event = AN_EVENT[ev.InfraMissingObserved]
    result = transition(old, event)
    notify = next(e for e in result.effects if isinstance(e, ef.Notify))
    for value in notify.payload.values():
        assert isinstance(value, (str, int, float, bool)) or value is None
