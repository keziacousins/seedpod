"""tests/engine/test_dispatch_table.py — WorkflowDispatch (docs/design/coherence-review.md
Conflict 13, AS AMENDED by DR-0022 ruling 2): abstract verb x provider -> concrete
definition + inputs. The destroy arm now returns ``{"cluster_id": ...}`` like
provision -- the DnsRecordRefResolver hook is deleted; ``cluster.load_infra`` reads
the DNS record fresh at run time instead (see the destroy-cloud.yml/destroy-shared.yml
head step).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from seedpod.core.effects import RunWorkflow
from seedpod.core.records import ClusterRecord, ClusterState, Origin
from seedpod.engine.config import parse_workflow
from seedpod.engine.dispatch_table import WorkflowDispatch

WORKFLOWS_DIR = Path(__file__).resolve().parents[2] / "config" / "workflows"

DESTROY_BY_PROVIDER = {
    "digitalocean": "destroy-cloud",
    "tart": "destroy-cloud",
    "kind": "destroy-shared",
    "orbstack": "destroy-shared",
}


def make_cluster(provider: str = "digitalocean") -> ClusterRecord:
    return ClusterRecord(
        id="cluster-1",
        name="test",
        state=ClusterState.PROVISIONING,
        version=1,
        provider=provider,
        environment="ephemeral",
        origin=Origin.MANAGED,
    )


def make_dispatch() -> WorkflowDispatch:
    return WorkflowDispatch(destroy_by_provider=DESTROY_BY_PROVIDER)


def test_provision_resolves_to_provider_specific_definition():
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="provision", cluster_id="cluster-1")
    name, inputs = dispatch.resolve(eff, make_cluster("digitalocean"))
    assert name == "provision-digitalocean"
    assert inputs == {"cluster_id": "cluster-1"}


def test_provision_resolves_per_provider_for_tart():
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="provision", cluster_id="cluster-1")
    name, _inputs = dispatch.resolve(eff, make_cluster("tart"))
    assert name == "provision-tart"


def test_deploy_resolves_to_deploy_waves():
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="deploy", cluster_id="cluster-1", deployment_id="dep-1")
    name, inputs = dispatch.resolve(eff, make_cluster())
    assert name == "deploy-waves"
    assert inputs == {"deployment_id": "dep-1"}


def test_rollback_resolves_to_deploy_rollback():
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="rollback", cluster_id="cluster-1", deployment_id="dep-1")
    name, inputs = dispatch.resolve(eff, make_cluster())
    assert name == "deploy-rollback"
    assert inputs == {"deployment_id": "dep-1"}


def test_destroy_resolves_by_provider_mapping_cloud():
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="destroy", cluster_id="cluster-1")
    name, _inputs = dispatch.resolve(eff, make_cluster("digitalocean"))
    assert name == "destroy-cloud"


def test_destroy_resolves_by_provider_mapping_shared():
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="destroy", cluster_id="cluster-1")
    name, _inputs = dispatch.resolve(eff, make_cluster("kind"))
    assert name == "destroy-shared"


def test_destroy_inputs_carry_cluster_id_and_trigger_only():
    """DR-0022 ruling 2, as amended by DR-0040 (erratum E1).

    Ruling 2's concern is that the dispatch table never smuggles **resolvable state**
    into the run args -- ``cluster.load_infra`` (the destroy workflow's own head step)
    resolves provider/slug/resource_ids/dns_record FRESH at run time, so a dispatch-time
    DNS-record snapshot would be a second, staler source of truth.

    ``trigger`` is the opposite kind of thing: provenance that NOTHING downstream can
    re-derive. Both destroy routes converge on ``DestroyDue``, so by the time the
    workflow starts, "was this an unattended TTL deletion or did an operator ask?" exists
    nowhere else. Carrying it is not smuggling state; it is the only channel there is.

    DR-0043 adds ``snapshot`` on the same reasoning: whether an operator ticked
    ``snapshot_before_destroy`` is likewise unrecoverable downstream, and is a
    different question from ``trigger``'s provenance."""
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="destroy", cluster_id="cluster-1")
    _name, inputs = dispatch.resolve(eff, make_cluster())
    # defaults when the machine stamped neither
    assert inputs == {"cluster_id": "cluster-1", "trigger": "operator", "snapshot": False}


def test_destroy_inputs_carry_the_ttl_trigger_when_the_machine_stamped_one():
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="destroy", cluster_id="cluster-1", args={"trigger": "ttl_expiry"})
    _name, inputs = dispatch.resolve(eff, make_cluster())
    assert inputs == {"cluster_id": "cluster-1", "trigger": "ttl_expiry", "snapshot": False}


def test_destroy_inputs_carry_the_operator_snapshot_request():
    """DR-0043: `snapshot_before_destroy=true` reaches the workflow, where
    `cluster.auto_snapshot` honours it unconditionally."""
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="destroy", cluster_id="cluster-1", args={"snapshot": True})
    _name, inputs = dispatch.resolve(eff, make_cluster())
    assert inputs["snapshot"] is True


@pytest.mark.parametrize("provider", sorted(DESTROY_BY_PROVIDER))
def test_destroy_resolve_supplies_every_input_the_shipped_workflow_declares(provider):
    """**The test that would have caught DR-0043's KeyError, and the reason it exists.**

    `resolve` builds its inputs dict as an explicit ALLOWLIST -- deliberately, per
    DR-0022 ruling 2, so dispatch can never smuggle resolvable state into run args.
    The hazard is that the allowlist and the shipped YAML's `inputs:` block are two
    lists that must agree, and nothing made them.

    DR-0043 threaded `snapshot` through events -> machine -> RunWorkflow.args ->
    workflow YAML -> step Params and missed this one line. Every unit test still
    passed: the machine test asserted the EFFECT's args, the step tests drove Params
    directly, and the API test asserted the armed TIMER's event. Nothing covered the
    seam between the effect and the workflow. On the appliance every destroy then
    died on `KeyError('snapshot')`, stranding a live droplet mid-destroy.

    So this asserts the relationship rather than a hand-written expectation: whatever
    the shipped destroy workflow declares as an input, `resolve` must supply. Adding
    an input to the YAML without adding it here now fails at test time instead of at
    3am against real infrastructure."""
    yaml_path = WORKFLOWS_DIR / f"{DESTROY_BY_PROVIDER[provider]}.yml"
    # `parse_workflow`, not `load_workflow`: this needs the declared `inputs:` block
    # only, and a pure structural parse needs no verb registry to get it.
    definition = parse_workflow(yaml_path.read_text())
    _name, inputs = make_dispatch().resolve(
        RunWorkflow(workflow="destroy", cluster_id="cluster-1"), make_cluster(provider)
    )
    missing = set(definition.inputs) - set(inputs)
    assert not missing, (
        f"{definition.workflow}.yml declares input(s) {sorted(missing)} that "
        f"WorkflowDispatch.resolve never supplies -- the workflow will die on KeyError"
    )


def test_unknown_abstract_verb_raises():
    dispatch = make_dispatch()
    eff = RunWorkflow(workflow="not-a-real-verb", cluster_id="cluster-1")
    with pytest.raises(ValueError):
        dispatch.resolve(eff, make_cluster())
