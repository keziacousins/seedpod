"""DR-0055 decision 4: ``RunWorkflow``/``CancelWorkflow`` carry ``aggregate_type`` +
``aggregate_id`` instead of ``cluster_id``. The durable outbox replays pending rows
across an upgrade, so the codec must still decode the payloads written before the
change -- exactly as they were written, so these literals are the OLD canonical JSON.
"""

from __future__ import annotations

import json

import pytest

from seedpod.core.codec import canonical_json, decode_effect, encode
from seedpod.core.effects import CancelWorkflow, RunWorkflow

# Verbatim old shapes: sorted keys, `args` always a dict, `deployment_id` always present.
LEGACY_RUN_PROVISION = (
    '{"args": {}, "cluster_id": "c1", "deployment_id": null, "kind": "run_workflow", "workflow": "provision"}'
)
LEGACY_RUN_DESTROY = (
    '{"args": {"snapshot": false, "trigger": "operator"}, "cluster_id": "c2", "deployment_id": null,'
    ' "kind": "run_workflow", "workflow": "destroy"}'
)
LEGACY_RUN_DEPLOY = (
    '{"args": {}, "cluster_id": "c3", "deployment_id": "d3", "kind": "run_workflow", "workflow": "deploy"}'
)
LEGACY_CANCEL = (
    '{"cluster_id": "c3", "deployment_id": "d3", "kind": "cancel_workflow", "reason": "", "workflow": "deploy"}'
)


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (LEGACY_RUN_PROVISION, RunWorkflow(workflow="provision", aggregate_type="cluster", aggregate_id="c1")),
        (
            LEGACY_RUN_DESTROY,
            RunWorkflow(
                workflow="destroy", aggregate_type="cluster", aggregate_id="c2",
                args={"snapshot": False, "trigger": "operator"},
            ),
        ),
        (
            LEGACY_RUN_DEPLOY,
            RunWorkflow(workflow="deploy", aggregate_type="cluster", aggregate_id="c3", deployment_id="d3"),
        ),
        (
            LEGACY_CANCEL,
            CancelWorkflow(workflow="deploy", aggregate_type="cluster", aggregate_id="c3", deployment_id="d3"),
        ),
    ],
)
def test_legacy_cluster_id_payload_decodes_as_a_cluster_effect(payload, expected):
    assert decode_effect(json.loads(payload)) == expected


def test_legacy_cancel_keeps_its_reason():
    legacy = json.loads(LEGACY_CANCEL)
    legacy["reason"] = "operator asked"
    assert decode_effect(legacy).reason == "operator asked"


def test_new_payload_carries_the_aggregate_and_no_cluster_id():
    eff = RunWorkflow(workflow="provision", aggregate_type="cluster", aggregate_id="c1")
    payload = json.loads(canonical_json(eff))
    assert payload == {
        "kind": "run_workflow", "workflow": "provision", "aggregate_type": "cluster",
        "aggregate_id": "c1", "deployment_id": None, "args": {},
    }
    assert decode_effect(payload) == eff


def test_instance_effect_round_trips():
    for eff in (
        RunWorkflow(workflow="provision", aggregate_type="instance", aggregate_id="i1", args={"a": "b"}),
        CancelWorkflow(workflow="destroy", aggregate_type="instance", aggregate_id="i1", reason="x"),
    ):
        assert decode_effect(encode(eff)) == eff


def test_an_explicit_aggregate_wins_over_a_stray_cluster_id():
    """A payload that already has the new shape is never rewritten."""
    payload = {
        "kind": "run_workflow", "workflow": "provision", "aggregate_type": "instance",
        "aggregate_id": "i1", "cluster_id": "c1", "deployment_id": None, "args": {},
    }
    eff = decode_effect(payload)
    assert (eff.aggregate_type, eff.aggregate_id) == ("instance", "i1")


@pytest.mark.parametrize("kind", ["run_workflow", "cancel_workflow"])
def test_a_partial_new_shape_is_never_read_as_a_cluster(kind):
    """An explicit ``aggregate_type`` with a stray ``cluster_id`` is not a legacy row."""
    payload = {
        "kind": kind, "workflow": "provision", "aggregate_type": "instance",
        "cluster_id": "same", "deployment_id": None,
    }
    with pytest.raises(TypeError):
        decode_effect(payload)


def test_a_payload_naming_no_aggregate_is_an_error_not_a_default():
    with pytest.raises(TypeError):
        decode_effect({"kind": "run_workflow", "workflow": "provision", "deployment_id": None, "args": {}})
