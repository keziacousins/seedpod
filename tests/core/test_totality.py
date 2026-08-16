"""Totality tests for the pure transition() function (docs/design/seam-a-core.md §K).

Binding contract:

- iterate ``product(states, event types)`` per machine; every cell exists (no
  ``KeyError``);
- ``InvalidTransition`` may only escape for ``Command`` subclasses or on ``NEW``;
- any non-Ignore result contains exactly one ``Persist`` whose ``record ==
  result.record`` and ``record.version == old.version + 1``, and (coherence review
  Conflict 8's amended P/N law) exactly one ``Notify``;
- ``Ignore`` results equal ``TransitionResult(old_record, ())`` exactly;
- the Observation-actor privilege check and the discovered-cluster force guard are
  each asserted directly.

No ``unittest.mock`` anywhere in this file (CLAUDE.md testing posture) -- the only
harness is ``tests/core/builders.py``.
"""

from __future__ import annotations

import dataclasses
import itertools
import typing

import pytest

from seedpod.core.effects import Notify, Persist
from seedpod.core.events import (
    ClusterEvent,
    Command,
    DeploymentEvent,
    DestroyRequested,
    HealthCheckFailed,
    InfraMissingObserved,
    InfraRunningObserved,
    Observation,
)
from seedpod.core.machine import (
    CLUSTER_TABLE,
    DEPLOYMENT_TABLE,
    InvalidTransition,
    TransitionResult,
    transition,
)
from seedpod.core.records import (
    ClusterState,
    DeploymentState,
    Origin,
)
from tests.core.builders import (
    AN_EVENT,
    AT,
    a_cluster,
    a_deployment,
    assert_an_event_covers_registry,
)

CLUSTER_EVENT_TYPES: tuple[type, ...] = typing.get_args(ClusterEvent)
DEPLOYMENT_EVENT_TYPES: tuple[type, ...] = typing.get_args(DeploymentEvent)


# ---------------------------------------------------------------------------
# Meta-test: AN_EVENT is exhaustive
# ---------------------------------------------------------------------------


def test_an_event_covers_every_registered_kind():
    assert_an_event_covers_registry()


def test_event_type_unions_partition_an_event_exactly():
    # Sanity: ClusterEvent/DeploymentEvent (which drive the totality loop below)
    # partition AN_EVENT exactly -- the 18+9 = 27 registered kinds, no overlap,
    # nothing left over.
    #
    # The DISJOINTNESS assertion below is the load-bearing one: every event kind
    # addresses exactly one aggregate. DR-0031 was ratified proposing that the cluster
    # table grow a `DeployRequested` row, which would have violated it; Erratum E1
    # introduced the cluster-side `DeploymentPending` instead precisely so this law
    # survives untouched (see that event's own docstring). Cluster went 17 -> 18 for
    # that one addition; the partition is intact.
    cluster = set(CLUSTER_EVENT_TYPES)
    deployment = set(DEPLOYMENT_EVENT_TYPES)
    assert cluster & deployment == set()
    assert cluster | deployment == set(AN_EVENT)
    assert len(cluster) == 18
    assert len(deployment) == 9


# ---------------------------------------------------------------------------
# Totality loop
# ---------------------------------------------------------------------------


def _assert_ignore(old_record, result: TransitionResult) -> None:
    assert result == TransitionResult(record=old_record, effects=())


def _assert_non_ignore(old_record, result: TransitionResult) -> None:
    persists = [e for e in result.effects if isinstance(e, Persist)]
    notifies = [e for e in result.effects if isinstance(e, Notify)]
    assert len(persists) == 1, f"expected exactly one Persist, got {len(persists)}: {result.effects}"
    assert len(notifies) == 1, f"expected exactly one Notify, got {len(notifies)}: {result.effects}"
    persist = persists[0]
    assert persist.record == result.record
    assert persist.record.version == old_record.version + 1


@pytest.mark.parametrize("state", list(ClusterState))
@pytest.mark.parametrize("event_type", CLUSTER_EVENT_TYPES)
def test_cluster_totality_cell(state, event_type):
    assert (state, event_type) in CLUSTER_TABLE  # every cell exists -- no KeyError

    record = a_cluster(state=state)
    event = AN_EVENT[event_type]

    try:
        result = transition(record, event)
    except InvalidTransition:
        assert issubclass(event_type, Command) or state is ClusterState.NEW, (
            f"InvalidTransition escaped for non-Command {event_type.__name__} "
            f"outside NEW (state={state.value})"
        )
        return

    if result.effects == ():
        _assert_ignore(record, result)
    else:
        _assert_non_ignore(record, result)


@pytest.mark.parametrize("state", list(DeploymentState))
@pytest.mark.parametrize("event_type", DEPLOYMENT_EVENT_TYPES)
def test_deployment_totality_cell(state, event_type):
    assert (state, event_type) in DEPLOYMENT_TABLE  # every cell exists -- no KeyError

    record = a_deployment(state=state)
    event = AN_EVENT[event_type]

    try:
        result = transition(record, event)
    except InvalidTransition:
        assert issubclass(event_type, Command) or state is DeploymentState.NEW, (
            f"InvalidTransition escaped for non-Command {event_type.__name__} "
            f"outside NEW (state={state.value})"
        )
        return

    if result.effects == ():
        _assert_ignore(record, result)
    else:
        _assert_non_ignore(record, result)


def test_every_cluster_state_and_event_type_pair_is_covered_by_the_loop():
    # Belt-and-braces: the parametrized tests above only prove coverage if pytest
    # actually collected one case per (state, event_type) pair.
    expected = set(itertools.product(ClusterState, CLUSTER_EVENT_TYPES))
    assert expected == set(CLUSTER_TABLE)


def test_every_deployment_state_and_event_type_pair_is_covered_by_the_loop():
    expected = set(itertools.product(DeploymentState, DEPLOYMENT_EVENT_TYPES))
    assert expected == set(DEPLOYMENT_TABLE)


# ---------------------------------------------------------------------------
# Observation-actor privilege check (docs/design/seam-a-core.md §F, point 1)
# ---------------------------------------------------------------------------

_CLUSTER_OBSERVATION_TYPES = tuple(t for t in CLUSTER_EVENT_TYPES if issubclass(t, Observation))


def test_cluster_observation_types_are_exactly_the_documented_three():
    assert set(_CLUSTER_OBSERVATION_TYPES) == {InfraRunningObserved, InfraMissingObserved, HealthCheckFailed}


@pytest.mark.parametrize("event_type", _CLUSTER_OBSERVATION_TYPES)
@pytest.mark.parametrize("bad_actor", ["api:alice", "engine:run:1", "timer:ttl", "cluster-machine", ""])
def test_observation_from_unprivileged_actor_is_always_invalid(event_type, bad_actor):
    canonical = AN_EVENT[event_type]
    event = dataclasses.replace(canonical, actor=bad_actor)
    # Any state -- the privilege check runs before table dispatch, so it must
    # reject an unprivileged actor regardless of what the table would otherwise do.
    for state in ClusterState:
        record = a_cluster(state=state)
        with pytest.raises(InvalidTransition):
            transition(record, event)


@pytest.mark.parametrize("event_type", _CLUSTER_OBSERVATION_TYPES)
@pytest.mark.parametrize("good_actor", ["reconciler", "health"])
def test_observation_from_privileged_actor_never_raises_the_privilege_error(event_type, good_actor):
    canonical = AN_EVENT[event_type]
    event = dataclasses.replace(canonical, actor=good_actor)
    # A privileged actor must never be rejected BY THE PRIVILEGE CHECK. Outside NEW
    # the table may still legitimately Ignore, but never raise (Observations are
    # never Commands, so the totality law forbids InvalidTransition for them except
    # on NEW, where every non-birth event -- privileged Observation included -- is
    # Invalid by construction; see test_cluster_totality_cell for that law).
    for state in ClusterState:
        record = a_cluster(state=state)
        if state is ClusterState.NEW:
            with pytest.raises(InvalidTransition):
                transition(record, event)
            continue
        transition(record, event)  # must not raise


# ---------------------------------------------------------------------------
# Discovered-cluster force guard (docs/design/seam-a-core.md §F, point 2 / †)
# ---------------------------------------------------------------------------

# Dagger-marked (†) rows in §G: origin == DISCOVERED and not event.force => Invalid.
_DAGGER_GUARDED_STATES = [
    ClusterState.ACTIVE,
    ClusterState.DESTROY_SCHEDULED,
    ClusterState.FAILED,
    ClusterState.DESTROY_FAILED,
    ClusterState.DESTROYED,
    ClusterState.ZOMBIE,
]


@pytest.mark.parametrize("state", _DAGGER_GUARDED_STATES)
def test_discovered_cluster_destroy_requires_force(state):
    record = a_cluster(
        state=state, origin=Origin.DISCOVERED, pre_destroy_state=state if state == ClusterState.DESTROY_SCHEDULED else None
    )
    event = DestroyRequested(at=AT, actor="api:alice", force=False)
    with pytest.raises(InvalidTransition):
        transition(record, event)


@pytest.mark.parametrize("state", _DAGGER_GUARDED_STATES)
def test_discovered_cluster_destroy_with_force_is_not_guard_rejected(state):
    record = a_cluster(
        state=state, origin=Origin.DISCOVERED, pre_destroy_state=state if state == ClusterState.DESTROY_SCHEDULED else None
    )
    event = DestroyRequested(at=AT, actor="api:alice", force=True)
    transition(record, event)  # must not raise


def test_managed_cluster_destroy_never_needs_force():
    for state in _DAGGER_GUARDED_STATES:
        record = a_cluster(
            state=state,
            origin=Origin.MANAGED,
            pre_destroy_state=state if state == ClusterState.DESTROY_SCHEDULED else None,
        )
        event = DestroyRequested(at=AT, actor="api:alice", force=False)
        transition(record, event)  # must not raise -- guard only applies to DISCOVERED


def test_unmanaged_state_destroy_is_unguarded_even_when_discovered():
    # UNMANAGED is explicitly NOT dagger-marked in §G: discovered clusters parked
    # in UNMANAGED can be manually cleaned up without force=True (v1 parity -- the
    # intersection guard was only ever wired at v1's ACTIVE destroy site).
    record = a_cluster(state=ClusterState.UNMANAGED, origin=Origin.DISCOVERED)
    event = DestroyRequested(at=AT, actor="api:alice", force=False)
    transition(record, event)  # must not raise


def test_provisioning_and_new_and_destroying_have_no_dagger_row():
    # DestroyRequested is simply not a listed Command at NEW/PROVISIONING/DESTROYING
    # (birth-only, un-abortable-provision, and mid-destroy-not-cancellable per §G/§J
    # respectively), so the guard is moot there -- both force=True and force=False
    # must raise InvalidTransition (it's an unlisted Command, not a guard failure).
    for state in (ClusterState.NEW, ClusterState.PROVISIONING, ClusterState.DESTROYING):
        for force in (False, True):
            record = a_cluster(state=state, origin=Origin.DISCOVERED)
            event = DestroyRequested(at=AT, actor="api:alice", force=force)
            with pytest.raises(InvalidTransition):
                transition(record, event)
