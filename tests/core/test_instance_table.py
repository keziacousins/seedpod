"""The instance transition table (DR-0055), exercised through the pure ``transition()``.

Totality over (state x event) lives in ``test_totality.py``, with the cluster and deployment
tables. This file holds what is specific to the instance machine: each rule's effects, the
renewal arithmetic, the timer invariant over random event sequences, and the guarantees the
brief names (no kubeconfig on a VM, ``hard_expires_at`` immutable, a failed destroy can always be
retried).

No ``unittest.mock``: ``transition()`` is pure, so these are input/output checks over
``tests/core/builders.py``.
"""

from __future__ import annotations

import dataclasses
import typing
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from seedpod.core.codec import decode_effect, encode
from seedpod.core.effects import (
    CancelTimer,
    CancelWorkflow,
    Notify,
    Persist,
    RunWorkflow,
    ScheduleTimer,
)
from seedpod.core.events import (
    ClusterEvent,
    InstanceCreateRequested,
    InstanceDestroyFailed,
    InstanceDestroyRequested,
    InstanceDestroySucceeded,
    InstanceEvent,
    InstanceExpired,
    InstanceProvisioned,
    InstanceProvisionFailed,
    InstanceRenewRequested,
    InstanceResourcesAllocated,
    InstanceRunningObserved,
    InstanceStoppedObserved,
    InstanceTimerLost,
)
from seedpod.core.machine import (
    INSTANCE_DESTROY_RETRY_DELAY,
    INSTANCE_RESOURCE_HOLDING_STATES,
    INSTANCE_TABLE,
    InvalidTransition,
    transition,
)
from seedpod.core.records import InstanceRecord, InstanceStatus
from tests.core.builders import AN_EVENT, AT, a_cluster, an_instance

S = InstanceStatus
INSTANCE_EVENT_TYPES: tuple[type, ...] = typing.get_args(InstanceEvent)
HOUR = timedelta(hours=1)


def _kinds(effects) -> list[type]:
    return [type(e) for e in effects]


def _only(effects, kind):
    found = [e for e in effects if isinstance(e, kind)]
    assert len(found) == 1, f"expected exactly one {kind.__name__}, got {found}"
    return found[0]


def _timer(effects, key="expiry") -> ScheduleTimer | None:
    found = [e for e in effects if isinstance(e, ScheduleTimer) and e.timer_key == key]
    assert len(found) <= 1
    return found[0] if found else None


def _a_live_instance(state=S.ACTIVE, **over) -> InstanceRecord:
    """A record whose lease is live at ``AT``: expires one hour after it, hard limit a day."""
    return an_instance(state=state, **{"expires_at": AT + HOUR, **over})


# ---------------------------------------------------------------------------
# birth
# ---------------------------------------------------------------------------


def _newborn(**over) -> InstanceRecord:
    return an_instance(state=S.NEW, version=0, **{"expires_at": AT + HOUR, **over})


def test_birth_moves_to_provisioning_runs_provision_and_arms_expiry_at_expires_at():
    record = _newborn()
    result = transition(record, InstanceCreateRequested(at=AT, actor="api:alice"))

    assert result.record.state is S.PROVISIONING and result.record.version == 1
    persist = _only(result.effects, Persist)
    assert persist.expected_version is None  # a birth INSERTs
    run = _only(result.effects, RunWorkflow)
    assert (run.workflow, run.aggregate_type, run.aggregate_id) == ("provision", "instance", record.id)
    timer = _only(result.effects, ScheduleTimer)
    assert (timer.aggregate_type, timer.aggregate_id, timer.timer_key) == ("instance", record.id, "expiry")
    assert timer.fire_at == record.expires_at
    assert timer.event == InstanceExpired(at=record.expires_at, actor="timer:expiry")
    notify = _only(result.effects, Notify)
    assert notify.topic == "instance_state_changed"
    assert notify.payload == {"instance_id": record.id, "old_status": "", "new_status": "provisioning"}
    assert notify.environment == record.environment


@pytest.mark.parametrize(
    "over, message",
    [
        ({"lease_seconds": 0}, "lease_seconds"),
        ({"lease_seconds": -5}, "lease_seconds"),
        ({"expires_at": AT + 2 * HOUR, "hard_expires_at": AT + HOUR}, "hard_expires_at"),
        ({"expires_at": AT}, "future"),
        ({"expires_at": AT - HOUR}, "future"),
    ],
)
def test_birth_refuses_a_record_that_would_have_no_real_expiry(over, message):
    with pytest.raises(InvalidTransition, match=message):
        transition(_newborn(**over), InstanceCreateRequested(at=AT, actor="api:alice"))


def test_birth_lease_may_equal_the_hard_limit():
    result = transition(
        _newborn(expires_at=AT + HOUR, hard_expires_at=AT + HOUR), InstanceCreateRequested(at=AT, actor="api:alice")
    )
    assert result.record.state is S.PROVISIONING


# ---------------------------------------------------------------------------
# provision outcome
# ---------------------------------------------------------------------------


def test_provision_success_records_address_and_resource_ids_and_goes_active():
    record = _a_live_instance(S.PROVISIONING, resource_ids={"vm": "early"})
    event = InstanceProvisioned(at=AT, actor="engine:run:1", address="192.0.2.7", resource_ids={"disk": "d1"})
    result = transition(record, event)

    assert result.record.state is S.ACTIVE
    assert result.record.address == "192.0.2.7"
    assert dict(result.record.resource_ids) == {"vm": "early", "disk": "d1"}  # merged, not replaced
    assert _kinds(result.effects) == [Persist, Notify]  # the timer is untouched, so still armed


def test_a_vm_has_no_kubeconfig_anywhere():
    assert "kubeconfig_ref" not in {f.name for f in dataclasses.fields(InstanceRecord)}
    assert "kubeconfig_ref" not in {f.name for f in dataclasses.fields(InstanceProvisioned)}
    with pytest.raises(TypeError):
        InstanceProvisioned(at=AT, actor="e", address="a", resource_ids={"vm": "1"}, kubeconfig_ref="x")  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "address, resource_ids", [("", {"vm": "1"}), ("192.0.2.7", {}), ("", {})]
)
def test_provision_success_without_an_address_or_resource_ids_fails_the_instance_and_keeps_its_expiry(
    address, resource_ids
):
    record = _a_live_instance(S.PROVISIONING)
    result = transition(
        record, InstanceProvisioned(at=AT, actor="engine:run:1", address=address, resource_ids=resource_ids)
    )
    assert result.record.state is S.FAILED
    assert result.record.failure_reason
    assert result.record.address is None
    assert resource_ids.items() <= dict(result.record.resource_ids).items()  # destroy needs any id it reported
    assert _kinds(result.effects) == [Persist, Notify]  # the expiry timer is untouched, so still armed


def test_resources_allocated_merges_ids_while_provisioning_and_stays_provisioning():
    record = _a_live_instance(S.PROVISIONING, resource_ids={"a": "1"})
    result = transition(record, InstanceResourcesAllocated(at=AT, actor="engine:run:1", resource_ids={"b": "2"}))
    assert result.record.state is S.PROVISIONING
    assert dict(result.record.resource_ids) == {"a": "1", "b": "2"}
    assert result.record.version == record.version + 1


def test_provision_failure_goes_failed_and_leaves_the_expiry_timer_armed():
    record = _a_live_instance(S.PROVISIONING, resource_ids={"vm": "1"})
    result = transition(record, InstanceProvisionFailed(at=AT, actor="engine:run:1", reason="boom"))

    assert result.record.state is S.FAILED and result.record.failure_reason == "boom"
    assert dict(result.record.resource_ids) == {"vm": "1"}  # kept: a destroy may need them
    assert not any(isinstance(e, CancelTimer) for e in result.effects)  # nothing disarms expiry
    assert _kinds(result.effects) == [Persist, Notify]


# ---------------------------------------------------------------------------
# destroy: by request, by expiry
# ---------------------------------------------------------------------------

_DESTROYABLE_NOT_PROVISIONING = [S.ACTIVE, S.FAILED, S.ZOMBIE, S.DESTROY_FAILED]


@pytest.mark.parametrize("state", _DESTROYABLE_NOT_PROVISIONING)
@pytest.mark.parametrize("event_type", [InstanceDestroyRequested, InstanceExpired])
def test_destroy_or_expiry_from_a_state_holding_a_resource_runs_destroy(state, event_type):
    record = _a_live_instance(state, expires_at=AT)  # due, so an expiry at AT is not stale
    result = transition(record, AN_EVENT[event_type])

    assert result.record.state is S.DESTROYING
    run = _only(result.effects, RunWorkflow)
    assert (run.workflow, run.aggregate_type, run.aggregate_id) == ("destroy", "instance", record.id)
    assert not any(isinstance(e, CancelWorkflow) for e in result.effects)  # no provision to cancel


@pytest.mark.parametrize("event_type", [InstanceDestroyRequested, InstanceExpired])
def test_destroy_while_provisioning_cancels_the_provision_run_first_then_destroys(event_type):
    record = _a_live_instance(S.PROVISIONING, expires_at=AT)
    result = transition(record, AN_EVENT[event_type])

    assert result.record.state is S.DESTROYING
    # Persist, Notify, then CancelWorkflow BEFORE RunWorkflow -- the cluster machine's discipline.
    assert _kinds(result.effects) == [Persist, Notify, CancelWorkflow, RunWorkflow]
    cancel, run = result.effects[2], result.effects[3]
    assert (cancel.workflow, cancel.aggregate_type, cancel.aggregate_id) == ("provision", "instance", record.id)
    assert (run.workflow, run.aggregate_type, run.aggregate_id) == ("destroy", "instance", record.id)


def test_a_stale_expiry_is_ignored_not_obeyed():
    # The timer was built for an expiry the lease has since been renewed past.
    record = _a_live_instance(S.ACTIVE)  # expires AT + 1h
    stale = InstanceExpired(at=AT, actor="timer:expiry")
    result = transition(record, stale)
    assert result.effects == () and result.record == record


@pytest.mark.parametrize("state", [S.DESTROYING, S.DESTROYED])
def test_expiry_in_destroying_or_destroyed_is_ignored(state):
    record = an_instance(state=state)
    assert transition(record, AN_EVENT[InstanceExpired]).effects == ()


@pytest.mark.parametrize("state", [S.DESTROYING, S.DESTROYED])
def test_destroy_request_in_destroying_or_destroyed_is_refused(state):
    with pytest.raises(InvalidTransition):
        transition(an_instance(state=state), AN_EVENT[InstanceDestroyRequested])


def test_destroy_request_on_a_new_record_is_refused():
    with pytest.raises(InvalidTransition):
        transition(an_instance(state=S.NEW), AN_EVENT[InstanceDestroyRequested])


# ---------------------------------------------------------------------------
# destroy outcome
# ---------------------------------------------------------------------------


def test_destroy_success_goes_destroyed_stamps_destroyed_at_and_cancels_every_timer():
    record = an_instance(state=S.DESTROYING)
    at = AT + timedelta(minutes=3)
    result = transition(record, InstanceDestroySucceeded(at=at, actor="engine:run:1"))

    assert result.record.state is S.DESTROYED and result.record.destroyed_at == at
    cancel = _only(result.effects, CancelTimer)
    assert (cancel.aggregate_type, cancel.aggregate_id, cancel.timer_key) == ("instance", record.id, None)  # ALL keys
    assert _timer(result.effects) is None


def test_destroy_failure_goes_destroy_failed_and_arms_a_retry_timer():
    record = an_instance(state=S.DESTROYING, expires_at=AT - HOUR)
    result = transition(record, InstanceDestroyFailed(at=AT, actor="engine:run:1", reason="api 500"))

    assert result.record.state is S.DESTROY_FAILED and result.record.failure_reason == "api 500"
    retry_at = AT + INSTANCE_DESTROY_RETRY_DELAY
    timer = _only(result.effects, ScheduleTimer)
    assert timer.timer_key == "expiry" and timer.fire_at == retry_at
    assert timer.event == InstanceExpired(at=retry_at, actor="timer:expiry")
    assert result.record.expires_at == record.expires_at  # the column does not move: only the timer does


def test_a_failed_destroy_never_moves_expires_at_past_the_hard_limit():
    """A destroy that fails right at the hard limit retries after it. The timer may; the column may not."""
    hard = AT + timedelta(minutes=1)
    record = an_instance(state=S.DESTROYING, expires_at=hard, hard_expires_at=hard)
    result = transition(record, InstanceDestroyFailed(at=AT, actor="engine:run:1", reason="api 500"))

    assert _only(result.effects, ScheduleTimer).fire_at == AT + INSTANCE_DESTROY_RETRY_DELAY > hard
    assert result.record.expires_at == hard <= result.record.hard_expires_at


def test_the_retry_timer_of_a_destroy_requested_before_the_lease_ended_is_obeyed():
    """The destroy was requested early (lease ends in an hour), so its retry time is EARLIER than
    `expires_at`. The staleness check that ignores an old `InstanceExpired` in `active` must not
    ignore this one: it is the only thing that retries the destroy."""
    record = _a_live_instance(S.DESTROYING)  # expires AT + 1h
    failed = transition(record, InstanceDestroyFailed(at=AT, actor="engine:run:1", reason="x"))
    timer = _only(failed.effects, ScheduleTimer)
    assert timer.event.at < failed.record.expires_at  # older than the column

    retried = transition(failed.record, timer.event)
    assert retried.record.state is S.DESTROYING
    assert _only(retried.effects, RunWorkflow).workflow == "destroy"
    assert failed.record.state is S.DESTROY_FAILED


def test_a_stale_expiry_is_still_ignored_in_every_other_state():
    for state in (S.PROVISIONING, S.ACTIVE, S.FAILED, S.ZOMBIE):
        record = _a_live_instance(state)
        assert transition(record, InstanceExpired(at=AT, actor="timer:expiry")).effects == ()


def test_a_failed_destroy_is_retried_by_the_timer_it_armed():
    failed = transition(
        an_instance(state=S.DESTROYING), InstanceDestroyFailed(at=AT, actor="engine:run:1", reason="x")
    )
    timer = _only(failed.effects, ScheduleTimer)

    retried = transition(failed.record, timer.event)  # the stored event, delivered verbatim
    assert retried.record.state is S.DESTROYING
    assert _only(retried.effects, RunWorkflow).workflow == "destroy"


def test_a_failed_destroy_is_retried_by_request():
    failed = transition(
        an_instance(state=S.DESTROYING), InstanceDestroyFailed(at=AT, actor="engine:run:1", reason="x")
    )
    retried = transition(failed.record, InstanceDestroyRequested(at=AT, actor="api:alice"))
    assert retried.record.state is S.DESTROYING


def test_destroy_can_fail_over_and_over_and_always_rearms():
    record = an_instance(state=S.DESTROYING)
    at = AT
    for _ in range(10):
        failed = transition(record, InstanceDestroyFailed(at=at, actor="engine:run:1", reason="x"))
        timer = _only(failed.effects, ScheduleTimer)
        record = transition(failed.record, timer.event).record  # the timer fires: back to destroying
        assert record.state is S.DESTROYING
        at = timer.fire_at


# ---------------------------------------------------------------------------
# observations
# ---------------------------------------------------------------------------


def test_a_stopped_vm_moves_active_to_failed_and_leaves_the_timer_running():
    record = _a_live_instance(S.ACTIVE)
    result = transition(record, AN_EVENT[InstanceStoppedObserved])
    assert result.record.state is S.FAILED and result.record.failure_reason
    assert _kinds(result.effects) == [Persist, Notify]  # no CancelTimer, no new timer


@pytest.mark.parametrize("state", [s for s in S if s not in (S.NEW, S.ACTIVE)])
def test_a_stopped_vm_in_any_other_state_changes_nothing(state):
    record = an_instance(state=state)
    result = transition(record, AN_EVENT[InstanceStoppedObserved])
    assert result.effects == () and result.record == record


def test_a_running_vm_on_a_destroyed_row_is_a_zombie_and_gets_an_expiry_again():
    record = an_instance(state=S.DESTROYED, destroyed_at=AT - HOUR, expires_at=AT - 2 * HOUR)
    result = transition(record, AN_EVENT[InstanceRunningObserved])

    assert result.record.state is S.ZOMBIE and result.record.destroyed_at is None
    timer = _only(result.effects, ScheduleTimer)
    assert timer.timer_key == "expiry" and timer.fire_at == AT == result.record.expires_at


def test_a_zombie_found_after_the_hard_limit_is_clamped_in_the_column_and_destroyed_at_once():
    hard = AT - timedelta(hours=1)
    record = an_instance(state=S.DESTROYED, destroyed_at=AT - HOUR, expires_at=hard, hard_expires_at=hard)
    result = transition(record, AN_EVENT[InstanceRunningObserved])

    assert result.record.expires_at == hard  # never past the hard limit
    timer = _only(result.effects, ScheduleTimer)
    assert timer.fire_at == AT  # but the timer is due now
    assert transition(result.record, timer.event).record.state is S.DESTROYING  # and it is not stale


def test_a_zombie_can_be_destroyed_by_request_and_by_its_own_timer():
    zombie = transition(
        an_instance(state=S.DESTROYED, destroyed_at=AT), AN_EVENT[InstanceRunningObserved]
    )
    timer = _only(zombie.effects, ScheduleTimer)
    for event in (InstanceDestroyRequested(at=AT, actor="api:alice"), timer.event):
        assert transition(zombie.record, event).record.state is S.DESTROYING


# ---------------------------------------------------------------------------
# a lost expiry timer (the liveness sweep's repair)
# ---------------------------------------------------------------------------

_WAITS_ON_THE_TIMER = [S.ACTIVE, S.FAILED, S.ZOMBIE, S.DESTROY_FAILED]


@pytest.mark.parametrize("state", [S.ACTIVE, S.FAILED, S.ZOMBIE])
def test_a_lost_timer_is_rearmed_at_the_lease_end(state):
    record = _a_live_instance(state)
    result = transition(record, InstanceTimerLost(at=AT, actor="sweep:instance-liveness"))

    timer = _only(result.effects, ScheduleTimer)
    assert (timer.aggregate_type, timer.aggregate_id, timer.timer_key) == ("instance", record.id, "expiry")
    assert timer.fire_at == record.expires_at == AT + HOUR
    assert timer.event == InstanceExpired(at=timer.fire_at, actor="timer:expiry")
    assert result.record.state is state and result.record.expires_at == record.expires_at  # nothing but the timer
    _only(result.effects, Persist), _only(result.effects, Notify)  # audited like any other transition


@pytest.mark.parametrize("state", [S.ACTIVE, S.FAILED, S.ZOMBIE])
def test_a_lost_timer_whose_time_has_passed_is_due_at_once_and_its_event_is_not_stale(state):
    record = an_instance(state=state, expires_at=AT - 3 * HOUR)
    result = transition(record, InstanceTimerLost(at=AT, actor="sweep:instance-liveness"))

    timer = _only(result.effects, ScheduleTimer)
    assert timer.fire_at == AT - 3 * HOUR <= AT  # in the past: the timer service fires it on its next poll
    assert transition(result.record, timer.event).record.state is S.DESTROYING  # and the machine obeys it


def test_a_lost_destroy_retry_timer_is_rearmed_one_retry_delay_on():
    record = an_instance(state=S.DESTROY_FAILED, expires_at=AT + 10 * HOUR, hard_expires_at=AT + 20 * HOUR)
    result = transition(record, InstanceTimerLost(at=AT, actor="sweep:instance-liveness"))

    timer = _only(result.effects, ScheduleTimer)
    assert timer.fire_at == AT + INSTANCE_DESTROY_RETRY_DELAY  # not at the lease end, ten hours away
    assert transition(result.record, timer.event).record.state is S.DESTROYING  # and it retries


@pytest.mark.parametrize("state", _WAITS_ON_THE_TIMER)
def test_a_lost_timer_is_never_rearmed_past_the_hard_limit(state):
    hard = AT + timedelta(minutes=1)
    record = an_instance(state=state, expires_at=hard, hard_expires_at=hard)
    result = transition(record, InstanceTimerLost(at=AT, actor="sweep:instance-liveness"))
    assert _only(result.effects, ScheduleTimer).fire_at <= hard
    assert result.record.hard_expires_at == hard


@pytest.mark.parametrize("state", [S.PROVISIONING, S.DESTROYING, S.DESTROYED])
def test_a_lost_timer_means_nothing_where_the_state_waits_on_a_run_or_is_over(state):
    record = an_instance(state=state)
    result = transition(record, InstanceTimerLost(at=AT, actor="sweep:instance-liveness"))
    assert result.effects == () and result.record == record


def test_a_lost_timer_on_a_new_record_is_refused():
    with pytest.raises(InvalidTransition):
        transition(an_instance(state=S.NEW, version=0), InstanceTimerLost(at=AT, actor="sweep:instance-liveness"))


@pytest.mark.parametrize("state", [s for s in S if s not in (S.NEW, S.DESTROYED)])
def test_a_running_vm_anywhere_but_a_destroyed_row_changes_nothing(state):
    record = an_instance(state=state)
    result = transition(record, AN_EVENT[InstanceRunningObserved])
    assert result.effects == () and result.record == record


@pytest.mark.parametrize(
    "event_type", [InstanceStoppedObserved, InstanceRunningObserved]
)
@pytest.mark.parametrize("actor", ["api:alice", "engine:run:1", "timer:expiry"])
def test_an_observation_from_an_unprivileged_actor_is_refused(event_type, actor):
    event = dataclasses.replace(AN_EVENT[event_type], actor=actor)
    for state in S:
        with pytest.raises(InvalidTransition):
            transition(an_instance(state=state), event)


# ---------------------------------------------------------------------------
# renew
# ---------------------------------------------------------------------------


def _renew(record: InstanceRecord, at: datetime):
    return transition(record, InstanceRenewRequested(at=at, actor="api:alice"))


def test_renew_sets_expiry_to_at_plus_lease_and_rearms_the_timer():
    record = an_instance(expires_at=AT + timedelta(minutes=10), lease_seconds=3600)
    result = _renew(record, AT)

    assert result.record.expires_at == AT + HOUR
    assert result.record.hard_expires_at == record.hard_expires_at
    timer = _only(result.effects, ScheduleTimer)
    assert timer.timer_key == "expiry" and timer.fire_at == AT + HOUR
    assert timer.event == InstanceExpired(at=AT + HOUR, actor="timer:expiry")
    assert _kinds(result.effects) == [Persist, Notify, ScheduleTimer]


def test_renew_is_clamped_to_the_hard_limit():
    record = an_instance(
        expires_at=AT + timedelta(minutes=10), lease_seconds=3600, hard_expires_at=AT + timedelta(minutes=40)
    )
    result = _renew(record, AT)
    assert result.record.expires_at == record.hard_expires_at == AT + timedelta(minutes=40)
    assert _only(result.effects, ScheduleTimer).fire_at == record.hard_expires_at


def test_renew_at_the_hard_limit_is_refused_because_it_would_extend_nothing():
    record = an_instance(
        expires_at=AT + timedelta(minutes=40), lease_seconds=3600, hard_expires_at=AT + timedelta(minutes=40)
    )
    with pytest.raises(InvalidTransition, match="not extend"):
        _renew(record, AT)


def test_renew_that_would_shorten_the_lease_is_refused():
    # 5 min lease, renewed early: at + lease is EARLIER than the expiry already granted.
    record = an_instance(expires_at=AT + HOUR, lease_seconds=300)
    with pytest.raises(InvalidTransition, match="not extend"):
        _renew(record, AT)


@pytest.mark.parametrize("offset", [timedelta(0), timedelta(seconds=1), timedelta(days=3)])
def test_renew_at_or_after_the_current_expiry_is_refused(offset):
    record = an_instance(expires_at=AT)
    with pytest.raises(InvalidTransition, match="expired"):
        _renew(record, AT + offset)


def test_renew_one_second_before_expiry_is_allowed():
    record = an_instance(expires_at=AT + timedelta(seconds=1))
    assert _renew(record, AT).record.expires_at == AT + HOUR


@pytest.mark.parametrize("state", [s for s in S if s is not S.ACTIVE])
def test_renew_is_refused_in_every_state_but_active(state):
    with pytest.raises(InvalidTransition):
        _renew(an_instance(state=state, expires_at=AT + HOUR), AT)


def test_two_renewals_at_the_same_instant_second_one_is_refused():
    first = _renew(an_instance(expires_at=AT + timedelta(minutes=1)), AT)
    with pytest.raises(InvalidTransition):
        _renew(first.record, AT)


_aware = st.integers(min_value=0, max_value=10**7).map(lambda s: datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=s))
_secs = st.integers(min_value=1, max_value=86400 * 3)


@settings(deadline=None, max_examples=300)
@given(
    born=_aware,
    lease=_secs,
    hard_after=_secs,
    renewals=st.lists(st.integers(min_value=0, max_value=86400), min_size=1, max_size=12),
)
def test_renew_never_goes_past_the_hard_limit_never_shortens_and_never_moves_the_hard_limit(
    born, lease, hard_after, renewals
):
    hard = born + timedelta(seconds=hard_after)
    expires = min(born + timedelta(seconds=lease), hard)
    record = an_instance(expires_at=expires, hard_expires_at=hard, lease_seconds=lease)
    at = born
    for step in renewals:
        at = at + timedelta(seconds=step)
        before = record
        try:
            result = _renew(record, at)
        except InvalidTransition:
            continue  # refused: nothing changed, which is the point
        record = result.record
        assert record.expires_at <= hard
        assert record.expires_at > before.expires_at  # strictly later: a renewal never shortens
        assert record.expires_at == min(at + timedelta(seconds=lease), hard)
        assert record.hard_expires_at == hard
        assert _only(result.effects, ScheduleTimer).fire_at == record.expires_at  # the timer follows


@settings(deadline=None, max_examples=200)
@given(born=_aware, lease=_secs, hard_after=_secs, delta=st.integers(min_value=-86400, max_value=86400 * 4))
def test_renew_at_or_after_expiry_is_always_refused(born, lease, hard_after, delta):
    hard = born + timedelta(seconds=hard_after)
    expires = min(born + timedelta(seconds=lease), hard)
    record = an_instance(expires_at=expires, hard_expires_at=hard, lease_seconds=lease)
    at = expires + timedelta(seconds=delta)
    if at >= expires:
        with pytest.raises(InvalidTransition):
            _renew(record, at)


# ---------------------------------------------------------------------------
# the hard limit, and the expiry, across every cell and across random sequences
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("state", list(S))
@pytest.mark.parametrize("event_type", INSTANCE_EVENT_TYPES)
def test_no_cell_changes_the_hard_limit(state, event_type):
    record = _a_live_instance(state)
    try:
        result = transition(record, AN_EVENT[event_type])
    except InvalidTransition:
        return
    assert result.record.hard_expires_at == record.hard_expires_at
    assert result.record.lease_seconds == record.lease_seconds
    assert (result.record.id, result.record.slug, result.record.provider) == (record.id, record.slug, record.provider)


def test_instance_events_do_not_address_the_cluster_table():
    # The tables are keyed on (state, type): an instance event is never read as a cluster one.
    with pytest.raises(KeyError):
        transition(a_cluster(), AN_EVENT[InstanceExpired])  # type: ignore[arg-type]
    cluster_types = set(typing.get_args(ClusterEvent))
    assert cluster_types.isdisjoint(INSTANCE_EVENT_TYPES)


_KINDS = [
    "provision_ok", "provision_failed", "allocated", "renew", "destroy", "destroy_ok", "destroy_failed",
    "stopped", "running", "tick", "fire",
]


@dataclasses.dataclass
class _World:
    """The record plus the timers the effects have armed -- the whole of what the Dispatcher keeps."""

    record: InstanceRecord
    timers: dict[str, datetime]
    now: datetime

    def apply(self, event) -> bool:
        try:
            result = transition(self.record, event)
        except InvalidTransition:
            return False
        self.record = result.record
        for eff in result.effects:
            if isinstance(eff, ScheduleTimer):
                assert eff.aggregate_type == "instance" and eff.aggregate_id == self.record.id
                self.timers[eff.timer_key] = eff.fire_at
            elif isinstance(eff, CancelTimer):
                if eff.timer_key is None:
                    self.timers.clear()
                else:
                    self.timers.pop(eff.timer_key, None)
        return True


def _world(lease: int, hard_after: int) -> _World:
    hard = AT + timedelta(seconds=hard_after)
    record = an_instance(
        state=S.NEW, version=0, lease_seconds=lease, expires_at=min(AT + timedelta(seconds=lease), hard),
        hard_expires_at=hard,
    )
    world = _World(record=record, timers={}, now=AT)
    assert world.apply(InstanceCreateRequested(at=AT, actor="api:alice"))
    return world


def _step(world: _World, kind: str, secs: int) -> None:
    world.now += timedelta(seconds=secs)
    at, actor = world.now, "engine:run:1"
    if kind == "tick":
        return
    if kind == "fire":
        # The timer service consumes a due timer and delivers its stored event, whatever it finds.
        due = world.timers.get("expiry")
        if due is None or due > at:
            return
        del world.timers["expiry"]
        world.apply(InstanceExpired(at=due, actor="timer:expiry"))
        return
    event = {
        "provision_ok": lambda: InstanceProvisioned(at=at, actor=actor, address="192.0.2.7", resource_ids={"vm": "1"}),
        "provision_failed": lambda: InstanceProvisionFailed(at=at, actor=actor, reason="x"),
        "allocated": lambda: InstanceResourcesAllocated(at=at, actor=actor, resource_ids={"vm": "1"}),
        "renew": lambda: InstanceRenewRequested(at=at, actor="api:alice"),
        "destroy": lambda: InstanceDestroyRequested(at=at, actor="api:alice"),
        "destroy_ok": lambda: InstanceDestroySucceeded(at=at, actor=actor),
        "destroy_failed": lambda: InstanceDestroyFailed(at=at, actor=actor, reason="x"),
        "stopped": lambda: InstanceStoppedObserved(at=at, actor="reconciler"),
        "running": lambda: InstanceRunningObserved(at=at, actor="reconciler"),
    }[kind]()
    world.apply(event)


@settings(deadline=None, max_examples=400)
@given(
    lease=st.integers(min_value=60, max_value=7200),
    hard_after=st.integers(min_value=60, max_value=7200 * 3),
    ops=st.lists(st.tuples(st.sampled_from(_KINDS), st.integers(min_value=0, max_value=5000)), max_size=40),
)
def test_expiry_stays_armed_while_a_resource_may_exist_and_the_hard_limit_never_moves(lease, hard_after, ops):
    world = _world(lease, hard_after)
    hard = world.record.hard_expires_at
    for kind, secs in ops:
        _step(world, kind, secs)
        rec = world.record
        assert rec.hard_expires_at == hard
        assert rec.expires_at <= hard  # in EVERY state: no retry, re-arm or zombie moves it past the limit
        if rec.state is S.DESTROYED:
            assert world.timers == {}  # nothing left to fire against a destroyed row
        elif rec.state is not S.DESTROYING:
            # Every other state that may hold a resource has an expiry armed. DESTROYING is the
            # one exception: its timer fired (or may still be armed), and the destroy run's
            # terminal event decides -- success cancels, failure re-arms.
            assert rec.state in INSTANCE_RESOURCE_HOLDING_STATES
            assert "expiry" in world.timers, f"{rec.state} with no expiry timer"
            if rec.state is not S.DESTROY_FAILED:
                # The timer is the lease end. Only a destroy retry arms it elsewhere, and a zombie
                # found after the hard limit has it due now while the column is clamped.
                assert world.timers["expiry"] == rec.expires_at or rec.state is S.ZOMBIE


@settings(deadline=None, max_examples=300)
@given(
    lease=st.integers(min_value=60, max_value=7200),
    hard_after=st.integers(min_value=60, max_value=7200 * 3),
    ops=st.lists(st.tuples(st.sampled_from(_KINDS), st.integers(min_value=0, max_value=5000)), max_size=40),
)
def test_from_any_reachable_state_a_resource_can_still_be_destroyed(lease, hard_after, ops):
    """The retry guarantee: whatever has happened, a state that may hold a resource is one step
    from a destroy -- by request, or by the timer it has armed -- except DESTROYING, where a run
    is in flight and its terminal event leaves the state at DESTROYED or DESTROY_FAILED."""
    world = _world(lease, hard_after)
    for kind, secs in ops:
        _step(world, kind, secs)
    rec = world.record
    if rec.state is S.DESTROYED:
        return
    if rec.state is S.DESTROYING:
        failed = transition(rec, InstanceDestroyFailed(at=world.now, actor="engine:run:1", reason="x"))
        assert failed.record.state is S.DESTROY_FAILED
        assert _timer(failed.effects) is not None
        return
    by_request = transition(rec, InstanceDestroyRequested(at=world.now, actor="api:alice"))
    assert by_request.record.state is S.DESTROYING
    due = world.timers["expiry"]
    by_timer = transition(rec, InstanceExpired(at=due, actor="timer:expiry"))
    assert by_timer.record.state is S.DESTROYING


# ---------------------------------------------------------------------------
# the codec carries the new aggregate
# ---------------------------------------------------------------------------


def test_a_persist_of_an_instance_record_round_trips_through_the_codec():
    record = _a_live_instance(S.ACTIVE, address="192.0.2.7", resource_ids={"vm": "1"})
    persist = Persist(record=record, expected_version=3)
    assert decode_effect(encode(persist)) == persist


def test_a_schedule_timer_for_an_instance_round_trips_with_its_event():
    result = transition(_newborn(), InstanceCreateRequested(at=AT, actor="api:alice"))
    for eff in result.effects:
        assert decode_effect(encode(eff)) == eff


def test_every_instance_cell_effect_round_trips():
    for state in S:
        for event_type in INSTANCE_EVENT_TYPES:
            try:
                result = transition(_a_live_instance(state), AN_EVENT[event_type])
            except InvalidTransition:
                continue
            for eff in result.effects:
                assert decode_effect(encode(eff)) == eff, (state, event_type, eff)


def test_the_table_has_a_rule_for_every_cell():
    assert set(INSTANCE_TABLE) == {(s, t) for s in S for t in INSTANCE_EVENT_TYPES}
