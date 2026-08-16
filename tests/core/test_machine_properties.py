"""Machine property tests -- Seam A §K's binding test contract (the `transition()`
bullet list): DESTROY_SCHEDULED entry always schedules exactly one destroy timer and
sets `pre_destroy_state`; destroy-cancel always returns to `pre_destroy_state`;
`DeploySucceeded` from DEPLOYING always cascades a scoped `SupersededBy`; every
Notify payload is JSON scalars; no rule ever emits two Persists or a Persist whose
`expected_version` disagrees with the pre-transition record's version.

No ``unittest.mock`` (CLAUDE.md's core testing posture) -- `transition()` is pure, so
these are plain input/output properties over `tests/core/builders.py`'s record
builders and canonical `AN_EVENT` instances (class-keyed, per that module).
"""

from __future__ import annotations

from datetime import UTC, datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from seedpod.core.effects import Cascade, Notify, Persist, ScheduleTimer
from seedpod.core.events import (
    DeploySucceeded,
    DestroyCancelled,
    DestroyRequested,
    SupersededBy,
    TtlExpired,
)
from seedpod.core.machine import CLUSTER_TABLE, DEPLOYMENT_TABLE, InvalidTransition, transition
from seedpod.core.records import ClusterState, DeploymentState
from tests.core.builders import AN_EVENT, a_cluster, a_deployment

_JSON_SCALAR_TYPES = (str, int, float, bool, type(None))

# Every (state, event) cell that is a real entry INTO DESTROY_SCHEDULED (excludes the
# DESTROY_SCHEDULED x DestroyRequested self-dup, which is an Ignore, not an entry).
_DESTROY_ENTRY_CELLS = [
    (ClusterState.ACTIVE, DestroyRequested),
    (ClusterState.ACTIVE, TtlExpired),
    (ClusterState.FAILED, DestroyRequested),
    (ClusterState.FAILED, TtlExpired),
    (ClusterState.DESTROY_FAILED, DestroyRequested),
    (ClusterState.DESTROYED, DestroyRequested),
    (ClusterState.ZOMBIE, DestroyRequested),
    (ClusterState.UNMANAGED, DestroyRequested),
]

# The states a cluster can be destroy-requested/ttl-expired FROM -- exactly the source
# states of the entry cells above, deduplicated. Also exactly the set of legal
# `pre_destroy_state` values destroy-cancel must be able to return to.
_DESTROY_REQUESTABLE_STATES = sorted({state for state, _ in _DESTROY_ENTRY_CELLS}, key=str)


@settings(deadline=None)
@given(cell=st.sampled_from(_DESTROY_ENTRY_CELLS))
def test_every_destroy_scheduled_entry_schedules_exactly_one_destroy_timer(cell) -> None:
    state, event_type = cell
    record = a_cluster(state=state)
    event = AN_EVENT[event_type]

    result = transition(record, event)

    assert result.record.state == ClusterState.DESTROY_SCHEDULED
    assert result.record.pre_destroy_state == state

    destroy_timers = [e for e in result.effects if isinstance(e, ScheduleTimer) and e.timer_key == "destroy"]
    assert len(destroy_timers) == 1


@settings(deadline=None)
@given(pre_destroy_state=st.sampled_from(_DESTROY_REQUESTABLE_STATES))
def test_destroy_cancel_returns_to_pre_destroy_state_from_every_destroy_requestable_state(
    pre_destroy_state,
) -> None:
    record = a_cluster(state=ClusterState.DESTROY_SCHEDULED, pre_destroy_state=pre_destroy_state)

    result = transition(record, AN_EVENT[DestroyCancelled])

    assert result.record.state == pre_destroy_state
    assert result.record.pre_destroy_state is None


@settings(deadline=None)
@given(
    deployment_id=st.text(alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=1, max_size=10),
    resolved_images=st.one_of(
        st.just(()),
        st.dictionaries(
            keys=st.text(min_size=1, max_size=8), values=st.text(min_size=0, max_size=8), min_size=1, max_size=3
        ),
    ),
)
def test_deploy_succeeded_from_deploying_always_cascades_scoped_superseded_by(
    deployment_id, resolved_images
) -> None:
    record = a_deployment(state=DeploymentState.DEPLOYING, id=deployment_id)
    event = DeploySucceeded(
        at=datetime(2026, 1, 1, tzinfo=UTC), actor="engine:run:r1", resolved_images=resolved_images
    )

    result = transition(record, event)

    cascades = [e for e in result.effects if isinstance(e, Cascade)]
    assert len(cascades) == 1
    (cascade,) = cascades
    assert cascade.where_state == frozenset({DeploymentState.ACTIVE})
    assert cascade.except_id == record.id
    assert isinstance(cascade.event, SupersededBy)
    assert cascade.event.new_deployment_id == record.id


def _all_cluster_cells():
    return sorted(CLUSTER_TABLE.keys(), key=lambda cell: (cell[0].value, cell[1].__name__))


def _all_deployment_cells():
    return sorted(DEPLOYMENT_TABLE.keys(), key=lambda cell: (cell[0].value, cell[1].__name__))


def _assert_notify_payloads_are_json_scalars(effects) -> None:
    for effect in effects:
        if isinstance(effect, Notify):
            for value in effect.payload.values():
                assert isinstance(value, _JSON_SCALAR_TYPES), f"non-scalar Notify payload value: {value!r}"


@settings(deadline=None)
@given(cell=st.sampled_from(_all_cluster_cells()))
def test_cluster_notify_payloads_are_always_json_scalars(cell) -> None:
    state, event_type = cell
    record = a_cluster(state=state)
    event = AN_EVENT[event_type]
    try:
        result = transition(record, event)
    except InvalidTransition:
        return  # only non-raising cells are in scope for this property
    _assert_notify_payloads_are_json_scalars(result.effects)


@settings(deadline=None)
@given(cell=st.sampled_from(_all_deployment_cells()))
def test_deployment_notify_payloads_are_always_json_scalars(cell) -> None:
    state, event_type = cell
    record = a_deployment(state=state)
    event = AN_EVENT[event_type]
    try:
        result = transition(record, event)
    except InvalidTransition:
        return
    _assert_notify_payloads_are_json_scalars(result.effects)


def _assert_persist_law(effects, old_version: int, is_birth_state: bool) -> None:
    persists = [e for e in effects if isinstance(e, Persist)]
    notifies = [e for e in effects if isinstance(e, Notify)]
    if effects:
        # P/N law (coherence review Conflict 8, reworded): every non-Ignore row emits
        # exactly one Persist and exactly one Notify.
        assert len(persists) == 1, f"expected exactly one Persist, got {len(persists)}"
        assert len(notifies) == 1, f"expected exactly one Notify, got {len(notifies)}"
        (persist,) = persists
        assert persist.record.version == old_version + 1
        if is_birth_state:
            assert persist.expected_version is None
        else:
            assert persist.expected_version == old_version
    else:
        assert persists == []  # Ignore: no Persist, no Notify, no version bump


@settings(deadline=None)
@given(cell=st.sampled_from(_all_cluster_cells()), version=st.integers(min_value=0, max_value=50))
def test_cluster_transitions_never_double_persist_or_mismatch_expected_version(cell, version) -> None:
    state, event_type = cell
    record = a_cluster(state=state, version=version)
    event = AN_EVENT[event_type]
    try:
        result = transition(record, event)
    except InvalidTransition:
        return
    _assert_persist_law(result.effects, old_version=version, is_birth_state=state == ClusterState.NEW)


@settings(deadline=None)
@given(cell=st.sampled_from(_all_deployment_cells()), version=st.integers(min_value=0, max_value=50))
def test_deployment_transitions_never_double_persist_or_mismatch_expected_version(cell, version) -> None:
    state, event_type = cell
    record = a_deployment(state=state, version=version)
    event = AN_EVENT[event_type]
    try:
        result = transition(record, event)
    except InvalidTransition:
        return
    _assert_persist_law(result.effects, old_version=version, is_birth_state=state == DeploymentState.NEW)
