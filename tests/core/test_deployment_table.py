"""Exact-equality effect tests for every row of docs/design/seam-a-core.md §H
(deployment transition table), amended by docs/design/coherence-review.md:

- Conflict 8: ``NEW x RollbackFinished -> Ignore`` (deliberately total-Ignore,
  overriding the NEW-state totality default).
- Conflict 12: ``DEPLOYING x CancelRequested -> CANCELLED`` emits
  ``CancelWorkflow(deploy)`` THEN ``RunWorkflow(rollback, deployment_id=self.id)``
  in that order; ``DEPLOYING x ClusterGone -> DESTROYED`` emits
  ``CancelWorkflow(deploy)`` only (no rollback on a dying cluster).

Binding test contract: docs/design/seam-a-core.md §K. No unittest.mock. Every
assertion is `result.effects == (...)` against frozen dataclasses.
"""

from __future__ import annotations

import dataclasses

import pytest

from seedpod.core import effects as ef
from seedpod.core import events as ev
from seedpod.core import records as rec
from seedpod.core.machine import InvalidTransition, TransitionResult, transition
from tests.core.builders import AN_EVENT, AT, a_deployment


def _dn(new: rec.DeploymentRecord, old_status: str) -> ef.Notify:
    """The v1-shaped deployment_status_changed Notify payload."""
    return ef.Notify(
        topic="deployment_status_changed",
        payload={
            "deployment_id": new.id,
            "cluster_id": new.cluster_id,
            "old_status": old_status,
            "new_status": new.state.value,
        },
        environment=new.environment,
    )


def _persist(new: rec.DeploymentRecord, expected_version: int | None) -> ef.Persist:
    return ef.Persist(record=new, expected_version=expected_version)


# ---------------------------------------------------------------------------
# NEW
# ---------------------------------------------------------------------------


def test_new_deploy_requested_births_pending():
    old = a_deployment(state=rec.DeploymentState.NEW, version=0)
    event = AN_EVENT[ev.DeployRequested]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.DeploymentState.PENDING, version=1, spec_ref=event.spec_ref
    )
    assert result.record == new
    assert result.effects == (_persist(new, None), _dn(new, ""))


def test_new_deploy_rejected_births_rejected():
    old = a_deployment(state=rec.DeploymentState.NEW, version=0)
    event = AN_EVENT[ev.DeployRejected]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.DeploymentState.REJECTED, version=1, failure_reason=event.reason
    )
    assert result.record == new
    assert result.effects == (_persist(new, None), _dn(new, ""))


def test_new_rollback_finished_is_total_ignore():
    """Conflict 8: overrides the NEW-state totality default (which would otherwise
    mark every unlisted, non-birth event Invalid)."""
    old = a_deployment(state=rec.DeploymentState.NEW, version=0)
    event = AN_EVENT[ev.RollbackFinished]
    result = transition(old, event)
    assert result == TransitionResult(record=old, effects=())


# ---------------------------------------------------------------------------
# PENDING
# ---------------------------------------------------------------------------


def test_pending_cluster_ready_starts_deploying():
    old = a_deployment(state=rec.DeploymentState.PENDING)
    event = AN_EVENT[ev.ClusterReady]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.DeploymentState.DEPLOYING, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _dn(new, "pending"),
        ef.RunWorkflow(workflow="deploy", cluster_id=new.cluster_id, deployment_id=new.id),
    )


def test_pending_cancel_requested_cancels_no_extra_effects():
    old = a_deployment(state=rec.DeploymentState.PENDING)
    event = AN_EVENT[ev.CancelRequested]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.DeploymentState.CANCELLED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _dn(new, "pending"))


def test_pending_cluster_gone_destroys_no_extra_effects():
    old = a_deployment(state=rec.DeploymentState.PENDING)
    event = AN_EVENT[ev.ClusterGone]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.DeploymentState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _dn(new, "pending"))


# ---------------------------------------------------------------------------
# DEPLOYING
# ---------------------------------------------------------------------------


def test_deploying_deploy_succeeded_activates_and_cascades_supersede():
    old = a_deployment(state=rec.DeploymentState.DEPLOYING)
    event = AN_EVENT[ev.DeploySucceeded]
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.DeploymentState.ACTIVE,
        version=old.version + 1,
        resolved_images=event.resolved_images,
    )
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _dn(new, "deploying"),
        ef.Cascade(
            cluster_id=new.cluster_id,
            where_state=frozenset({rec.DeploymentState.ACTIVE}),
            event=ev.SupersededBy(new_deployment_id=new.id, at=event.at, actor="cluster-machine"),
            except_id=new.id,
        ),
    )


def test_deploying_deploy_failed_fails_cluster_record_untouched():
    old = a_deployment(state=rec.DeploymentState.DEPLOYING)
    event = AN_EVENT[ev.DeployFailed]
    result = transition(old, event)
    new = dataclasses.replace(
        old, state=rec.DeploymentState.FAILED, version=old.version + 1, failure_reason=event.reason
    )
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _dn(new, "deploying"))


def test_deploying_cancel_requested_cancel_then_rollback_in_order():
    """Conflict 12: CancelWorkflow(deploy) THEN RunWorkflow(rollback, deployment_id=self.id)
    in exactly that order -- the drain-lane admitter processes them in seq order, so
    rollback waits for the cancelled deploy run to reach terminal."""
    old = a_deployment(state=rec.DeploymentState.DEPLOYING)
    event = AN_EVENT[ev.CancelRequested]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.DeploymentState.CANCELLED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _dn(new, "deploying"),
        ef.CancelWorkflow(workflow="deploy", cluster_id=new.cluster_id, deployment_id=new.id),
        ef.RunWorkflow(workflow="rollback", cluster_id=new.cluster_id, deployment_id=new.id),
    )


def test_deploying_cluster_gone_cancel_only_no_rollback():
    """Conflict 12: CancelWorkflow(deploy) only -- no rollback on a dying cluster."""
    old = a_deployment(state=rec.DeploymentState.DEPLOYING)
    event = AN_EVENT[ev.ClusterGone]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.DeploymentState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (
        _persist(new, old.version),
        _dn(new, "deploying"),
        ef.CancelWorkflow(workflow="deploy", cluster_id=new.cluster_id, deployment_id=new.id),
    )


# ---------------------------------------------------------------------------
# ACTIVE
# ---------------------------------------------------------------------------


def test_active_superseded_by_sets_superseded_by():
    old = a_deployment(state=rec.DeploymentState.ACTIVE)
    event = AN_EVENT[ev.SupersededBy]
    result = transition(old, event)
    new = dataclasses.replace(
        old,
        state=rec.DeploymentState.SUPERSEDED,
        version=old.version + 1,
        superseded_by=event.new_deployment_id,
    )
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _dn(new, "active"))


def test_active_deploy_succeeded_is_ignored_monitor_job_won_race():
    old = a_deployment(state=rec.DeploymentState.ACTIVE)
    event = AN_EVENT[ev.DeploySucceeded]
    result = transition(old, event)
    assert result == TransitionResult(record=old, effects=())


def test_active_deploy_failed_is_ignored_stale_failure_after_success():
    old = a_deployment(state=rec.DeploymentState.ACTIVE)
    event = AN_EVENT[ev.DeployFailed]
    result = transition(old, event)
    assert result == TransitionResult(record=old, effects=())


# ---------------------------------------------------------------------------
# ACTIVE / FAILED / SUPERSEDED / CANCELLED / REJECTED x ClusterGone -> DESTROYED
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "state",
    [
        rec.DeploymentState.ACTIVE,
        rec.DeploymentState.FAILED,
        rec.DeploymentState.SUPERSEDED,
        rec.DeploymentState.CANCELLED,
        rec.DeploymentState.REJECTED,
    ],
)
def test_cluster_gone_destroys_from_every_terminal_ish_state(state):
    old = a_deployment(state=state)
    event = AN_EVENT[ev.ClusterGone]
    result = transition(old, event)
    new = dataclasses.replace(old, state=rec.DeploymentState.DESTROYED, version=old.version + 1)
    assert result.record == new
    assert result.effects == (_persist(new, old.version), _dn(new, state.value))


# ---------------------------------------------------------------------------
# DESTROYED: terminal; every non-Command event is Ignore, duplicate cascades harmless
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "event",
    [
        ev.ClusterGone(at=AT, actor="cluster-machine"),
        ev.ClusterReady(at=AT, actor="cluster-machine"),
        ev.DeploySucceeded(at=AT, actor="engine:run:1"),
        ev.DeployFailed(at=AT, actor="engine:run:1", reason="boom"),
        ev.SupersededBy(at=AT, actor="cluster-machine", new_deployment_id="deployment-2"),
        ev.RollbackFinished(at=AT, actor="engine:run:1", ok=False),
    ],
)
def test_destroyed_non_command_events_are_ignored(event):
    old = a_deployment(state=rec.DeploymentState.DESTROYED)
    result = transition(old, event)
    assert result == TransitionResult(record=old, effects=())


@pytest.mark.parametrize(
    "event",
    [
        ev.CancelRequested(at=AT, actor="api:alice"),
        ev.DeployRequested(at=AT, actor="api:alice", spec_ref="audit-1"),
        ev.DeployRejected(at=AT, actor="api:alice", reason="no"),
    ],
)
def test_destroyed_commands_are_invalid(event):
    old = a_deployment(state=rec.DeploymentState.DESTROYED)
    with pytest.raises(InvalidTransition):
        transition(old, event)


# ---------------------------------------------------------------------------
# Notify payload shape assertions (JSON-safe scalars only; v1-shaped)
# ---------------------------------------------------------------------------


def test_notify_payload_values_are_json_scalars():
    old = a_deployment(state=rec.DeploymentState.DEPLOYING)
    event = AN_EVENT[ev.DeployFailed]
    result = transition(old, event)
    notify = next(e for e in result.effects if isinstance(e, ef.Notify))
    for value in notify.payload.values():
        assert isinstance(value, (str, int, float, bool)) or value is None
    assert set(notify.payload) == {"deployment_id", "cluster_id", "old_status", "new_status"}


def test_notify_topic_is_v1_verbatim_name():
    old = a_deployment(state=rec.DeploymentState.PENDING)
    event = AN_EVENT[ev.ClusterReady]
    result = transition(old, event)
    notify = next(e for e in result.effects if isinstance(e, ef.Notify))
    assert notify.topic == "deployment_status_changed"
