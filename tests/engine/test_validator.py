"""Table-driven tests for seedpod/engine/config.py -- the frozen-grammar parser
+ validator (docs/design/seam-b-engine.md section 2.2, V1-V10).

Pure: no DB, no engine, no real Step/registry -- a FakeRegistryView stands in for
``engine/registry.py`` per the RegistryView protocol contract. Zero Mock/patch
(CLAUDE.md testing posture): the fake is a typed class.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field

import pytest
from pydantic import BaseModel

from seedpod.engine.config import ConfigError, Ref, StepDef, load_workflow, parse_workflow

# ---------------------------------------------------------------------------
# Fixtures: pydantic Params/Output models + a FakeRegistryView
# ---------------------------------------------------------------------------


class EmptyParams(BaseModel):
    pass


class EmptyOutput(BaseModel):
    pass


class ClusterIdParams(BaseModel):
    cluster_id: str


class SpecModel(BaseModel):
    cidr: str


class SpecOutput(BaseModel):
    spec: SpecModel


class CreateDropletParams(BaseModel):
    spec: SpecModel


class ResourceIdsOutput(BaseModel):
    resource_ids: Mapping[str, str]


class AwaitDropletParams(BaseModel):
    resource_ids: Mapping[str, str]


class IpOutput(BaseModel):
    ip: str


class HostParams(BaseModel):
    host: str


class WaveModel(BaseModel):
    index: int
    docs: list[str]
    gate_timeout_seconds: int


class WavesOutput(BaseModel):
    waves: list[WaveModel]


class ApplyParams(BaseModel):
    kubeconfig: str
    docs: list[str]


class ChangesOutput(BaseModel):
    changes: str


class DeploymentsParams(BaseModel):
    deployments: list[str]


class OptionalParams(BaseModel):
    maybe: str | None = None


class OptionalOutput(BaseModel):
    value: str | None = None


class RequiredValueParams(BaseModel):
    value: str


class IntOutput(BaseModel):
    n: int


class TimeoutParams(BaseModel):
    pass


@dataclass(frozen=True)
class FakeVerbSpec:
    Params: type[BaseModel]
    Output: type[BaseModel]
    gateable: bool = False
    undoable: bool = False


_BUILTIN_SCALARS: dict[str, type] = {"str": str, "int": int, "float": float, "bool": bool}


def _parse_type_expr(expr: str, named: dict[str, type]) -> type | None:
    expr = expr.strip()
    if expr in named:
        return named[expr]
    if expr in _BUILTIN_SCALARS:
        return _BUILTIN_SCALARS[expr]
    m = re.fullmatch(r"Optional\[(.+)\]", expr)
    if m:
        inner = _parse_type_expr(m.group(1), named)
        return (inner | None) if inner is not None else None
    m = re.fullmatch(r"list\[(.+)\]", expr)
    if m:
        inner = _parse_type_expr(m.group(1), named)
        return list[inner] if inner is not None else None
    return None


@dataclass
class FakeRegistryView:
    """The RegistryView protocol implementation this whole test module uses."""

    verbs: dict[str, FakeVerbSpec] = field(default_factory=dict)
    named_types: dict[str, type] = field(default_factory=dict)

    def verb(self, name: str) -> FakeVerbSpec | None:
        return self.verbs.get(name)

    def resolve_type(self, type_expr: str) -> type | None:
        return _parse_type_expr(type_expr, self.named_types)


@pytest.fixture
def registry() -> FakeRegistryView:
    return FakeRegistryView(
        verbs={
            "noop": FakeVerbSpec(Params=EmptyParams, Output=EmptyOutput),
            "cluster.load_spec": FakeVerbSpec(Params=ClusterIdParams, Output=SpecOutput),
            "infra.create_instance": FakeVerbSpec(Params=CreateDropletParams, Output=ResourceIdsOutput, undoable=True),
            "infra.await_instance": FakeVerbSpec(Params=AwaitDropletParams, Output=IpOutput, gateable=True),
            "k3s.await_ssh": FakeVerbSpec(Params=HostParams, Output=EmptyOutput, gateable=True),
            "deploy.plan_waves": FakeVerbSpec(Params=EmptyParams, Output=WavesOutput),
            "kube.apply_docs": FakeVerbSpec(Params=ApplyParams, Output=ChangesOutput),
            "deploy.await_wave": FakeVerbSpec(Params=DeploymentsParams, Output=EmptyOutput, gateable=True),
            "optional.step": FakeVerbSpec(Params=OptionalParams, Output=OptionalOutput),
            "required.consumer": FakeVerbSpec(Params=RequiredValueParams, Output=EmptyOutput),
            "int.source": FakeVerbSpec(Params=EmptyParams, Output=IntOutput),
        },
        named_types={"ClusterSpec": SpecModel},
    )


def load(text: str, reg: FakeRegistryView) -> object:
    return load_workflow(text, reg)


def expect_rule(text: str, reg: FakeRegistryView, rule: str) -> None:
    with pytest.raises(ConfigError) as exc_info:
        load_workflow(text, reg)
    assert exc_info.value.rule == rule, (
        f"expected rule {rule!r}, got {exc_info.value.rule!r}: {exc_info.value.message}"
    )


# ---------------------------------------------------------------------------
# Shared YAML skeleton
# ---------------------------------------------------------------------------

_HEADER = """
workflow: demo
version: 1
inputs:
  cluster_id: {type: str}
on_failure: report
outcome:
  succeeded: {event: DestroySucceeded}
  failed:    {event: DestroyFailed, payload: {reason: {from: run.cluster_id}}}
  cancelled: {event: DestroyFailed, payload: {reason: {from: run.cluster_id}}}
steps:
"""


def wf(steps_yaml: str) -> str:
    return _HEADER + steps_yaml


# ---------------------------------------------------------------------------
# V1: every `uses` names a registered verb
# ---------------------------------------------------------------------------


def test_v1_valid_registered_verb(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[0].uses == "noop"


def test_v1_violating_unregistered_verb(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: totally.unregistered
"""
    )
    expect_rule(text, registry, "V1")


# ---------------------------------------------------------------------------
# V2: `with` keys exactly satisfy the verb's Params
# ---------------------------------------------------------------------------


def test_v2_valid_with_matches_params(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[0].with_["cluster_id"] == Ref(path="run.cluster_id")


def test_v2_violating_extra_key(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}, bogus: 1}
"""
    )
    expect_rule(text, registry, "V2")


def test_v2_violating_missing_required_key(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {}
"""
    )
    expect_rule(text, registry, "V2")


# ---------------------------------------------------------------------------
# V3: every Ref resolves to a lexically earlier step in the same or enclosing
# scope, or to run.*/the loop alias; steps outside a foreach may not reference
# steps inside it.
# ---------------------------------------------------------------------------


def test_v3_valid_ref_to_earlier_step(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: create
    uses: infra.create_instance
    with: {spec: {from: spec.spec}}
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[1].with_["spec"] == Ref(path="spec.spec")


def test_v3_violating_ref_to_unknown_step(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: create
    uses: infra.create_instance
    with: {spec: {from: nonexistent.spec}}
"""
    )
    expect_rule(text, registry, "V3")


def test_v3_violating_forward_reference(registry: FakeRegistryView) -> None:
    """A step may not reference one that comes later (not yet in scope)."""
    text = wf(
        """
  - id: create
    uses: infra.create_instance
    with: {spec: {from: spec.spec}}
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
"""
    )
    expect_rule(text, registry, "V3")


def test_v3_violating_outside_foreach_references_inside(registry: FakeRegistryView) -> None:
    """Steps outside a foreach may not reference steps inside its body."""
    text = wf(
        """
  - id: waves
    uses: deploy.plan_waves
  - id: wave
    foreach: {items: {from: waves.waves}, as: w}
    body:
      - id: apply
        uses: kube.apply_docs
        with: {kubeconfig: dummy, docs: {from: w.docs}}
  - id: after
    uses: required.consumer
    with: {value: {from: apply.changes}}
"""
    )
    expect_rule(text, registry, "V3")


def test_v3_valid_foreach_body_reads_outer_and_alias(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: waves
    uses: deploy.plan_waves
  - id: wave
    foreach: {items: {from: waves.waves}, as: w}
    body:
      - id: apply
        uses: kube.apply_docs
        with: {kubeconfig: {from: spec.spec.cidr}, docs: {from: w.docs}}
"""
    )
    parsed = load(text, registry)
    foreach_entry = parsed.steps[2]
    assert foreach_entry.body[0].with_["docs"] == Ref(path="w.docs")


# ---------------------------------------------------------------------------
# V4: Ref field paths type-check against the source Output (or input/list-elem
# type), and the resolved type must be assignable to the target Params field
# annotation -- Optional[T] sources bind only Optional[T] params.
# ---------------------------------------------------------------------------


def test_v4_valid_type_matches(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: create
    uses: infra.create_instance
    with: {spec: {from: spec.spec}}
"""
    )
    load(text, registry)  # no raise


def test_v4_violating_type_mismatch(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: created
    uses: infra.create_instance
    with: {spec: {from: run.cluster_id}}
"""
    )
    # run.cluster_id : str, infra.create_instance.spec wants SpecModel
    expect_rule(text, registry, "V4")


def test_v4_violating_unknown_field_on_output(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: create
    uses: infra.create_instance
    with: {spec: {from: spec.nonexistent_field}}
"""
    )
    expect_rule(text, registry, "V4")


def test_v4_optional_binding_rule_widening_allowed(registry: FakeRegistryView) -> None:
    """A required (non-Optional) source may still widen into an Optional param."""
    text = wf(
        """
  - id: n
    uses: int.source
  - id: opt
    uses: optional.step
"""
    )
    # int.source's Output has no 'maybe'-shaped field to bind directly, so instead
    # exercise via a step whose Params field is Optional and whose source is the
    # non-optional 'run.cluster_id' input -- this must be accepted.
    text = wf(
        """
  - id: opt
    uses: optional.step
    with: {maybe: {from: run.cluster_id}}
"""
    )
    load(text, registry)  # no raise: str -> Optional[str] is fine


def test_v4_optional_binding_rule_narrowing_rejected(registry: FakeRegistryView) -> None:
    """An Optional[T] source may only bind to an Optional[T] param, never a bare T."""
    text = wf(
        """
  - id: opt
    uses: optional.step
    with: {maybe: {from: run.cluster_id}}
  - id: consumer
    uses: required.consumer
    with: {value: {from: opt.value}}
"""
    )
    expect_rule(text, registry, "V4")


def test_v4_gate_timeout_ref_must_type_check_to_int(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: create
    uses: infra.create_instance
    with: {spec: {from: spec.spec}}
  - id: droplet
    uses: infra.await_instance
    with: {resource_ids: {from: create.resource_ids}}
    gate: {timeout_seconds: {from: spec.spec}}
"""
    )
    # spec.spec resolves to SpecModel, not int
    expect_rule(text, registry, "V4")


def test_v4_gate_timeout_ref_to_int_is_valid(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: n
    uses: int.source
  - id: create
    uses: infra.create_instance
    with: {spec: {from: run.cluster_id}}
"""
    )
    text = wf(
        """
  - id: n
    uses: int.source
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: create
    uses: infra.create_instance
    with: {spec: {from: spec.spec}}
  - id: droplet
    uses: infra.await_instance
    with: {resource_ids: {from: create.resource_ids}}
    gate: {timeout_seconds: {from: n.n}}
"""
    )
    load(text, registry)  # no raise


# ---------------------------------------------------------------------------
# V5: foreach.items must type-check to list[T]
# ---------------------------------------------------------------------------


def test_v5_valid_list_items(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: waves
    uses: deploy.plan_waves
  - id: wave
    foreach: {items: {from: waves.waves}, as: w}
    body:
      - id: apply
        uses: kube.apply_docs
        with: {kubeconfig: dummy, docs: {from: w.docs}}
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[1].as_ == "w"


def test_v5_violating_non_list_items(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: wave
    foreach: {items: {from: spec.spec}, as: w}
    body:
      - id: noop
        uses: noop
"""
    )
    expect_rule(text, registry, "V5")


# ---------------------------------------------------------------------------
# V6: gate: only on gateable verbs
# ---------------------------------------------------------------------------


def test_v6_valid_gate_on_gateable_verb(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: ssh
    uses: k3s.await_ssh
    with: {host: dummy}
    gate: {timeout_seconds: 60}
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[0].gate.timeout_seconds == 60


def test_v6_violating_gate_on_non_gateable_verb(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
    gate: {timeout_seconds: 60}
"""
    )
    expect_rule(text, registry, "V6")


# ---------------------------------------------------------------------------
# GateDef.settle_seconds (DR-0022 Erratum E2): a post-Ready grace, distinct
# from interval_seconds -- defaults to 0 (no grace), parses when given, and is
# bounded/non-negative like every other gate/retry numeric (V9).
# ---------------------------------------------------------------------------


def test_gate_settle_seconds_defaults_to_zero(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: ssh
    uses: k3s.await_ssh
    with: {host: dummy}
    gate: {timeout_seconds: 60, interval_seconds: 5}
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[0].gate.settle_seconds == 0


def test_gate_settle_seconds_is_parsed_distinctly_from_interval(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: ssh
    uses: k3s.await_ssh
    with: {host: dummy}
    gate: {timeout_seconds: 45, interval_seconds: 5, settle_seconds: 3}
"""
    )
    parsed = load(text, registry)
    gate = parsed.steps[0].gate
    assert gate.interval_seconds == 5
    assert gate.settle_seconds == 3  # NOT folded into interval_seconds (Erratum E2)


def test_gate_settle_seconds_rejects_negative(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: ssh
    uses: k3s.await_ssh
    with: {host: dummy}
    gate: {timeout_seconds: 60, settle_seconds: -1}
"""
    )
    expect_rule(text, registry, "V9")


# ---------------------------------------------------------------------------
# V7: undoable is a verb property, not YAML
# ---------------------------------------------------------------------------


def test_v7_valid_no_compensation_keys(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: create
    uses: infra.create_instance
    with: {spec: {from: run.cluster_id}}
    on_failure: continue
"""
    )
    # note: spec type mismatch would raise V4 first -- use a matching type instead
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
    on_failure: continue
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[0].on_failure == "continue"


@pytest.mark.parametrize("key", ["undo", "undoable", "compensate", "compensator"])
def test_v7_violating_compensation_key_in_yaml(registry: FakeRegistryView, key: str) -> None:
    text = wf(
        f"""
  - id: step1
    uses: noop
    {key}: true
"""
    )
    expect_rule(text, registry, "V7")


# ---------------------------------------------------------------------------
# V8: outcome/emit event names must be Pillar-1 registered events; outcome
# payload Refs may reference top-level scope only.
# ---------------------------------------------------------------------------


def test_v8_valid_emit_registered_event(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: create
    uses: infra.create_instance
    with: {spec: {from: spec.spec}}
    emit: {event: InfraAllocated, payload: {resource_ids: {from: create.resource_ids}}}
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[1].emit.event == "InfraAllocated"


def test_v8_violating_unregistered_event(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
    emit: {event: TotallyMadeUpEvent}
"""
    )
    expect_rule(text, registry, "V8")


def test_v8_violating_deleted_event_name(registry: FakeRegistryView) -> None:
    """DropletReady/DestroyCompleted/etc. were deleted by coherence Conflict 8;
    they must not validate even though they read like plausible event names."""
    text = wf(
        """
  - id: step1
    uses: noop
    emit: {event: DropletReady}
"""
    )
    expect_rule(text, registry, "V8")


def test_v8_violating_outcome_payload_refs_top_level_only(registry: FakeRegistryView) -> None:
    text = """
workflow: demo
version: 1
inputs:
  cluster_id: {type: str}
on_failure: report
outcome:
  succeeded: {event: InfraAllocated, payload: {resource_ids: {from: create.resource_ids}}}
  failed:    {event: DestroyFailed, payload: {reason: {from: run.cluster_id}}}
  cancelled: {event: DestroyFailed, payload: {reason: {from: run.cluster_id}}}
steps:
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: waves
    uses: deploy.plan_waves
  - id: wave
    foreach: {items: {from: waves.waves}, as: w}
    body:
      - id: create
        uses: infra.create_instance
        with: {spec: {from: spec.spec}}
"""
    # 'create' only exists inside the foreach body -- unreachable from outcome
    # (V8's "outcome payload Refs may reference top-level scope only").
    expect_rule(text, registry, "V3")


def test_v8_valid_outcome_payload_refs_top_level(registry: FakeRegistryView) -> None:
    text = """
workflow: demo
version: 1
inputs:
  cluster_id: {type: str}
on_failure: report
outcome:
  succeeded: {event: InfraAllocated, payload: {resource_ids: {from: create.resource_ids}}}
  failed:    {event: DestroyFailed, payload: {reason: {from: run.cluster_id}}}
  cancelled: {event: DestroyFailed, payload: {reason: {from: run.cluster_id}}}
steps:
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: {from: run.cluster_id}}
  - id: create
    uses: infra.create_instance
    with: {spec: {from: spec.spec}}
"""
    load(text, registry)  # no raise


# ---------------------------------------------------------------------------
# V9: ids unique per scope; retry values positive and bounded
# ---------------------------------------------------------------------------


def test_v9_valid_unique_ids(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
  - id: step2
    uses: noop
"""
    )
    load(text, registry)  # no raise


def test_v9_violating_duplicate_ids(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
  - id: step1
    uses: noop
"""
    )
    expect_rule(text, registry, "V9")


def test_v9_violating_duplicate_id_shadows_outer_scope_in_foreach(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: waves
    uses: deploy.plan_waves
  - id: wave
    foreach: {items: {from: waves.waves}, as: w}
    body:
      - id: waves
        uses: noop
"""
    )
    expect_rule(text, registry, "V9")


def test_v9_valid_retry_within_bounds(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
    retry: {max_attempts: 3, base_delay_seconds: 2.0, factor: 2.0, max_delay_seconds: 30.0}
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[0].retry.max_attempts == 3


def test_v9_violating_retry_out_of_bounds(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
    retry: {max_attempts: 0}
"""
    )
    expect_rule(text, registry, "V9")


def test_v9_valid_named_retry_policy(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
    retry: kubectl_default
"""
    )
    parsed = load(text, registry)
    assert parsed.steps[0].retry == "kubectl_default"


# ---------------------------------------------------------------------------
# V10: unknown keys anywhere = load error; any scalar containing '${' = hard
# error.
# ---------------------------------------------------------------------------


def test_v10_valid_no_unknown_keys_no_interpolation(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
"""
    )
    load(text, registry)  # no raise


def test_v10_violating_unknown_top_level_key(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
"""
    ).replace("workflow: demo\n", "workflow: demo\nsurprise: true\n")
    expect_rule(text, registry, "V10")


def test_v10_violating_unknown_step_key(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
    bogus_key: true
"""
    )
    expect_rule(text, registry, "V10")


def test_v10_violating_interpolation_marker_in_scalar(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: step1
    uses: noop
    with: {}
"""
    ).replace("workflow: demo", "workflow: ${demo}")
    expect_rule(text, registry, "V10")


def test_v10_violating_interpolation_marker_nested_in_with(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: spec
    uses: cluster.load_spec
    with: {cluster_id: "prefix-${run.cluster_id}"}
"""
    )
    expect_rule(text, registry, "V10")


def test_v10_no_if_when_for_grammar_exists() -> None:
    """The grammar is frozen: `if`/`when`/`for` aren't recognized keys at all --
    they fall through to the generic unknown-key rejection (V10), not a special
    conditional code path (there is none)."""
    text = wf(
        """
  - id: step1
    uses: noop
    if: {from: run.cluster_id}
"""
    )
    with pytest.raises(ConfigError) as exc_info:
        parse_workflow(text)
    assert exc_info.value.rule == "V10"


# ---------------------------------------------------------------------------
# Extra edge cases explicitly called out by the task: V3 lexical scoping depth,
# V4 Optional rule symmetry, V5 nested list-of-model typing.
# ---------------------------------------------------------------------------


def test_foreach_body_cannot_see_sibling_later_top_level_step(registry: FakeRegistryView) -> None:
    """A foreach body may see steps declared before the foreach at the top
    level, but not ones declared after it (V3, sequential-order enforcement)."""
    text = wf(
        """
  - id: waves
    uses: deploy.plan_waves
  - id: wave
    foreach: {items: {from: waves.waves}, as: w}
    body:
      - id: apply
        uses: kube.apply_docs
        with: {kubeconfig: {from: after.n}, docs: {from: w.docs}}
  - id: after
    uses: int.source
"""
    )
    expect_rule(text, registry, "V3")


def test_nested_foreach_is_rejected_by_grammar(registry: FakeRegistryView) -> None:
    """ForeachDef.body is StepDefs ONLY -- no nesting. A nested `foreach` key
    inside a body entry is an unknown key on a StepDef (V10)."""
    text = wf(
        """
  - id: waves
    uses: deploy.plan_waves
  - id: outer
    foreach: {items: {from: waves.waves}, as: w}
    body:
      - id: inner
        foreach: {items: {from: w.docs}, as: d}
        body:
          - id: noop
            uses: noop
"""
    )
    expect_rule(text, registry, "V10")


def test_v4_optional_field_drilling_stays_optional(registry: FakeRegistryView) -> None:
    """Drilling a field off an Optional-typed value still round-trips through
    the Optional binding rule when read back out."""
    text = wf(
        """
  - id: opt
    uses: optional.step
    with: {maybe: {from: run.cluster_id}}
  - id: opt2
    uses: optional.step
    with: {maybe: {from: opt.value}}
"""
    )
    load(text, registry)  # Optional[str] -> Optional[str] param: fine


def test_v5_foreach_alias_field_access_type_checks(registry: FakeRegistryView) -> None:
    """The foreach alias's fields resolve against the list's element type."""
    text = wf(
        """
  - id: waves
    uses: deploy.plan_waves
  - id: wave
    foreach: {items: {from: waves.waves}, as: w}
    body:
      - id: apply
        uses: kube.apply_docs
        with: {kubeconfig: dummy, docs: {from: w.docs}}
"""
    )
    parsed = load(text, registry)
    body_step = parsed.steps[1].body[0]
    assert isinstance(body_step, StepDef)
    assert body_step.with_["docs"] == Ref(path="w.docs")


def test_v5_foreach_alias_unknown_field_is_v4(registry: FakeRegistryView) -> None:
    text = wf(
        """
  - id: waves
    uses: deploy.plan_waves
  - id: wave
    foreach: {items: {from: waves.waves}, as: w}
    body:
      - id: apply
        uses: kube.apply_docs
        with: {kubeconfig: dummy, docs: {from: w.nonexistent}}
"""
    )
    expect_rule(text, registry, "V4")


# ---------------------------------------------------------------------------
# Grammar-shape sanity (missing required top-level keys, empty steps, etc.)
# ---------------------------------------------------------------------------


def test_grammar_missing_required_top_level_key(registry: FakeRegistryView) -> None:
    text = """
version: 1
on_failure: report
outcome:
  succeeded: {event: DestroySucceeded}
  failed: {event: DestroyFailed, payload: {reason: {from: run.cluster_id}}}
  cancelled: {event: DestroyFailed, payload: {reason: {from: run.cluster_id}}}
steps:
  - id: step1
    uses: noop
"""
    with pytest.raises(ConfigError) as exc_info:
        parse_workflow(text)
    assert exc_info.value.rule == "grammar"


def test_grammar_empty_steps_rejected(registry: FakeRegistryView) -> None:
    text = wf("")
    with pytest.raises(ConfigError) as exc_info:
        parse_workflow(text)
    assert exc_info.value.rule == "grammar"


def test_grammar_outcome_requires_all_three_blocks(registry: FakeRegistryView) -> None:
    text = """
workflow: demo
version: 1
on_failure: report
outcome:
  succeeded: {event: DestroySucceeded}
steps:
  - id: step1
    uses: noop
"""
    with pytest.raises(ConfigError) as exc_info:
        parse_workflow(text)
    assert exc_info.value.rule == "grammar"
