"""tests/engine/test_shipped_workflows.py — load-time proof that every workflow
definition shipped under config/workflows/ satisfies the frozen grammar (Seam B
§2.2, validator rules V1-V10) against the declared-verbs fixture registry in
tests/engine/declared_verbs.py.

declared_verbs.DECLARED_VERBS IS the interface contract Pillar 3's real steps must
satisfy -- this module's job is only to prove the shipped YAML is internally
consistent against that contract; reconciling the contract against real
`ProviderStep`/domain-step implementations is a later task (Pillar 3).

Zero Mock/patch/monkeypatch: `ShippedWorkflowRegistry` is a typed, static fixture,
not a runtime double.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from seedpod.engine.config import (
    ConfigError,
    WorkflowDefinition,
    load_workflow,
    parse_workflow,
    validate_workflow,
)
from tests.engine.declared_verbs import DECLARED_VERBS, ShippedWorkflowRegistry

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS_DIR = REPO_ROOT / "config" / "workflows"

# The six events coherence-review Conflict 8 deletes outright -- Pillar 1 never
# registered them, so a shipped file naming one would already fail V8 at load
# time; these are asserted explicitly (belt-and-braces) so a future edit that
# reintroduces one of these strings anywhere in an outcome/emit block fails loudly
# and legibly, not just via a generic "not a registered Pillar-1 event" ConfigError.
FORBIDDEN_EVENTS = frozenset(
    {
        "DestroyCompleted",
        "DestroyStalled",
        "ProvisionCancelled",
        "DeployCancelled",
        "DropletReady",
    }
)

SHIPPED_FILES = sorted(WORKFLOWS_DIR.glob("*.yml"))


def _registry() -> ShippedWorkflowRegistry:
    return ShippedWorkflowRegistry()


def _load(path: Path) -> WorkflowDefinition:
    text = path.read_text()
    return load_workflow(text, _registry())


# ---------------------------------------------------------------------------
# Sanity: the fixture actually found the five files this task shipped.
# ---------------------------------------------------------------------------


def test_shipped_files_present():
    names = {p.name for p in SHIPPED_FILES}
    assert names == {
        "deploy-waves.yml",
        "provision-digitalocean.yml",
        "provision-kind.yml",
        "provision-tart.yml",
        "provision-orbstack.yml",
        "destroy-cloud.yml",
        "destroy-shared.yml",
        "deploy-rollback.yml",
    }


# ---------------------------------------------------------------------------
# Every shipped file loads clean against the declared-verbs registry (V1-V10).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", SHIPPED_FILES, ids=lambda p: p.name)
def test_shipped_workflow_loads(path: Path):
    wf = _load(path)
    assert isinstance(wf, WorkflowDefinition)
    # Conflict 13: "workflow_runs.workflow stores the CONCRETE definition name" --
    # the dispatch table (engine/dispatch_table.py) and this fixture both key off
    # the filename stem, so the two must agree.
    assert wf.workflow == path.stem


@pytest.mark.parametrize("path", SHIPPED_FILES, ids=lambda p: p.name)
def test_shipped_workflow_parse_and_validate_are_separately_clean(path: Path):
    """The two-phase pipeline (parse_workflow, then validate_workflow) each pass
    independently -- not just load_workflow's fused call."""
    wf = parse_workflow(path.read_text())
    validate_workflow(wf, _registry())  # raises on the first violation


@pytest.mark.parametrize("path", SHIPPED_FILES, ids=lambda p: p.name)
def test_shipped_workflow_names_no_forbidden_event(path: Path):
    wf = _load(path)
    events = {wf.outcome.succeeded.event, wf.outcome.failed.event, wf.outcome.cancelled.event}
    for entry in wf.steps:
        body = entry.body if hasattr(entry, "body") else (entry,)
        for step in body:
            if step.emit is not None:
                events.add(step.emit.event)
    assert events.isdisjoint(FORBIDDEN_EVENTS)


# ---------------------------------------------------------------------------
# Per-file assertions pinning the specific coherence-review corrections.
# ---------------------------------------------------------------------------


def test_deploy_waves_outcome_corrected_per_conflict_8():
    wf = _load(WORKFLOWS_DIR / "deploy-waves.yml")
    assert wf.outcome.succeeded.event == "DeploySucceeded"
    assert wf.outcome.failed.event == "DeployFailed"
    assert wf.outcome.cancelled.event == "DeployFailed"  # NOT DeployCancelled
    assert wf.on_failure == "report"
    # succeeded payload only carries resolved_images -- DeploySucceeded has no
    # deployment_id field (the engine targets the event by run.deployment_id,
    # not via payload).
    assert set(wf.outcome.succeeded.payload) == {"resolved_images"}
    assert set(wf.outcome.failed.payload) == set()
    assert set(wf.outcome.cancelled.payload) == set()


def test_deploy_waves_plan_step_binds_data_initialization_per_dr_0028_decision_2():
    """DR-0028 decision 2 moved ``data_initialization`` off ``profile`` onto its
    own top-level fact on ``deploy.load_audit``'s Output / ``deploy.plan_waves``'s
    Params (``tests/engine/declared_verbs.py``). Both fields are Optional, so
    V2 (missing-required-keys) would never fail if this binding were forgotten
    -- ``Wave.restore`` would then be permanently unreachable, a silent-skip of
    the ONE route by which a preset deploy's ``restore_from_snapshot``/
    ``restore_from_latest`` ever reaches a cluster. Pinning the binding here
    means a future edit that drops it fails this test loudly, rather than
    shipping a deployment that silently never restores data."""
    wf = _load(WORKFLOWS_DIR / "deploy-waves.yml")
    plan_step = next(s for s in wf.steps if getattr(s, "id", None) == "plan")
    assert plan_step.uses == "deploy.plan_waves"
    assert "data_initialization" in plan_step.with_, (
        "deploy-waves.yml's plan step no longer binds data_initialization -- "
        "Wave.restore would become unreachable (DR-0028 decision 2)"
    )
    assert plan_step.with_["data_initialization"].path == "audit.data_initialization"


def test_provision_digitalocean_head_shrinks_to_cluster_id_per_conflict_10():
    wf = _load(WORKFLOWS_DIR / "provision-digitalocean.yml")
    assert set(wf.inputs) == {"cluster_id"}
    assert wf.inputs["cluster_id"].type == "str"
    assert wf.on_failure == "compensate"
    first_step = wf.steps[0]
    assert first_step.id == "spec"
    assert first_step.uses == "cluster.load_spec"
    create_step = wf.steps[1]
    assert create_step.id == "create"
    assert create_step.uses == "infra.create_instance"
    assert create_step.emit.event == "InfraAllocated"
    assert set(create_step.emit.payload) == {"resource_ids"}


def test_provision_digitalocean_known_hosts_threaded_per_conflict_14():
    wf = _load(WORKFLOWS_DIR / "provision-digitalocean.yml")
    by_id = {s.id: s for s in wf.steps}
    assert by_id["trust_host"].uses == "k3s.trust_host_keys"
    for step_id in ("k3s", "k3s_ready", "kubeconfig"):
        step = by_id[step_id]
        assert "known_hosts" in step.with_, f"{step_id} does not bind known_hosts"
        assert step.with_["known_hosts"].path == "trust_host.known_hosts"


def test_provision_digitalocean_tail_never_carries_raw_kubeconfig_per_conflict_9():
    wf = _load(WORKFLOWS_DIR / "provision-digitalocean.yml")
    by_id = {s.id: s for s in wf.steps}
    assert by_id["kubeconfig"].uses == "k3s.fetch_kubeconfig"
    store = by_id["store"]
    assert store.uses == "cluster.store_kubeconfig"
    assert store.with_["kubeconfig"].path == "kubeconfig.kubeconfig"
    payload = wf.outcome.succeeded.payload
    assert set(payload) == {"public_ip", "kubeconfig_ref"}
    assert payload["kubeconfig_ref"].path == "store.kubeconfig_ref"
    assert wf.outcome.succeeded.event == "ProvisionSucceeded"
    assert wf.outcome.failed.event == "ProvisionFailed"
    assert wf.outcome.cancelled.event == "ProvisionFailed"  # NOT ProvisionCancelled


@pytest.mark.parametrize("filename", ["destroy-cloud.yml", "destroy-shared.yml"])
def test_destroy_outcome_corrected_per_conflict_5_and_8(filename: str):
    wf = _load(WORKFLOWS_DIR / filename)
    assert wf.outcome.succeeded.event == "DestroySucceeded"
    assert wf.outcome.failed.event == "DestroyFailed"  # NOT DestroyStalled
    assert wf.outcome.cancelled.event == "DestroyFailed"  # NOT DestroyStalled
    assert wf.on_failure == "report"
    last_step = wf.steps[-1]
    assert last_step.id == "destroy"
    assert last_step.uses == "infra.destroy_instance"
    assert last_step.gate is not None


def test_destroy_shared_is_destroy_cloud_plus_wipe_namespace():
    cloud = _load(WORKFLOWS_DIR / "destroy-cloud.yml")
    shared = _load(WORKFLOWS_DIR / "destroy-shared.yml")
    cloud_ids = [s.id for s in cloud.steps]
    shared_ids = [s.id for s in shared.steps]
    assert shared_ids == [*cloud_ids[:-1], "wipe", cloud_ids[-1]]
    wipe = next(s for s in shared.steps if s.id == "wipe")
    assert wipe.uses == "kube.wipe_namespace"
    assert wipe.on_failure == "continue"
    # everything else (inputs, outcome, the other steps) matches destroy-cloud
    assert shared.inputs.keys() == cloud.inputs.keys()
    assert shared.outcome == cloud.outcome


def test_deploy_rollback_matches_conflict_12_verbatim():
    wf = _load(WORKFLOWS_DIR / "deploy-rollback.yml")
    assert wf.workflow == "deploy-rollback"
    assert set(wf.inputs) == {"deployment_id"}
    assert wf.on_failure == "report"
    for name in ("succeeded", "failed", "cancelled"):
        assert getattr(wf.outcome, name).event == "RollbackFinished"
    step_ids = [s.id for s in wf.steps]
    assert step_ids == ["kubecfg", "undo"]
    undo = wf.steps[1]
    assert undo.uses == "kube.rollout_undo"
    assert undo.with_["kubeconfig"].path == "kubecfg.kubeconfig"
    assert undo.with_["namespace"] == "default"
    assert undo.retry == "kubectl_default"
    assert undo.timeout_seconds == 120


# ---------------------------------------------------------------------------
# A deliberately broken variant proves the fixture is load-bearing, not a rubber
# stamp: reintroducing a dead event must fail V8, not silently pass.
# ---------------------------------------------------------------------------


def test_fixture_actually_rejects_a_dead_event():
    broken = (WORKFLOWS_DIR / "deploy-waves.yml").read_text().replace(
        "cancelled: {event: DeployFailed}", "cancelled: {event: DeployCancelled}"
    )
    with pytest.raises(ConfigError) as exc_info:
        load_workflow(broken, _registry())
    assert exc_info.value.rule == "V8"


# ---------------------------------------------------------------------------
# V6-companion law (Round-8a "infra-and-do" review finding): V6 only checks
# "gate: only on gateable verbs", never the converse "a gateable verb's step
# MUST carry a gate:". Without a gate, a gateable step's placeholder
# provisional Output (e.g. infra.await_instance's AddressOutput(address=""))
# would persist as a SUCCESSFUL step output (engine/engine.py: `step_def.gate
# is None` branch), silently shipping an empty address downstream. All four
# shipped provision-*.yml carry gates today (latent, not live) -- this test
# makes that a load-bearing, machine-checked property of every shipped
# workflow rather than an accident of the files that happen to exist.
# ---------------------------------------------------------------------------


def _all_steps(wf: WorkflowDefinition):
    """Flattens top-level steps and (per test_shipped_workflow_names_no_forbidden_event's
    own pattern) any `foreach` body steps into one sequence of StepDefs."""
    for entry in wf.steps:
        if hasattr(entry, "body"):
            yield from entry.body
        else:
            yield entry


@pytest.mark.parametrize("path", SHIPPED_FILES, ids=lambda p: p.name)
def test_gateable_verbs_used_in_shipped_workflows_always_carry_a_gate(path: Path):
    wf = _load(path)
    for step in _all_steps(wf):
        fixture = DECLARED_VERBS.get(step.uses)
        if fixture is not None and fixture.gateable:
            assert step.gate is not None, (
                f"{path.name}:{step.id} uses gateable verb {step.uses!r} but declares no gate: block"
            )


# ---------------------------------------------------------------------------
# P7 structural check (Round-8a review finding): "a vendor prefix is permitted
# ONLY for a capability no other provider has, and requires a Seam C command
# plus supported-set gating" -- but no `supported` check exists anywhere in
# the step/engine path, so the ONLY thing preventing a vendor-prefixed verb
# from being bound into the wrong provider's workflow is convention. This
# pins that convention structurally: every `do.*` verb may appear only in
# `provision-digitalocean.yml`.
# ---------------------------------------------------------------------------

_VENDOR_WORKFLOW = {"do": "provision-digitalocean.yml"}


@pytest.mark.parametrize("path", SHIPPED_FILES, ids=lambda p: p.name)
def test_vendor_prefixed_verbs_appear_only_in_their_own_providers_workflow(path: Path):
    wf = _load(path)
    for step in _all_steps(wf):
        prefix = step.uses.split(".", 1)[0]
        expected_file = _VENDOR_WORKFLOW.get(prefix)
        if expected_file is not None:
            assert path.name == expected_file, (
                f"{path.name}:{step.id} uses vendor-prefixed verb {step.uses!r}, "
                f"which P7 permits only in {expected_file!r}"
            )
