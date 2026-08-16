"""tests/engine/test_registry.py — StepRegistry: DI-built verb -> Step mapping,
for_tests() ergonomics, and interop with engine/config.py's already-landed
RegistryView/VerbSpec protocols (see the coordination note in engine/registry.py's
module docstring). No Mock/patch anywhere.
"""

from __future__ import annotations

import pytest

from seedpod.core.deploy_wave import (
    ApplyChangeSummary,
    DeploymentProfile,
    ManifestDoc,
    SnapshotRestoreSpec,
    Wave,
)
from seedpod.engine.registry import NAMED_TYPES, StepRegistry, UnknownVerbError, resolve_type_expr
from seedpod.engine.schedule import NAMED_POLICIES
from tests.engine.fakes import EchoOutput, EchoParams, FakeEchoStep, FakeGateStep, FakeUndoableStep


def test_for_tests_builds_registry_keyed_by_verb():
    registry = StepRegistry.for_tests(FakeEchoStep(), FakeGateStep())
    assert "test.echo" in registry
    assert "test.gate" in registry
    assert "test.undoable" not in registry


def test_get_returns_the_constructed_instance():
    echo = FakeEchoStep()
    registry = StepRegistry.for_tests(echo)
    assert registry.get("test.echo") is echo


def test_get_unknown_verb_raises():
    registry = StepRegistry.for_tests(FakeEchoStep())
    with pytest.raises(UnknownVerbError):
        registry.get("no.such.verb")


def test_verbs_lists_all_registered_verbs():
    registry = StepRegistry.for_tests(FakeEchoStep(), FakeGateStep(), FakeUndoableStep())
    assert set(registry.verbs()) == {"test.echo", "test.gate", "test.undoable"}


def test_params_and_output_type_lookup():
    registry = StepRegistry.for_tests(FakeEchoStep())
    assert registry.params_type("test.echo") is EchoParams
    assert registry.output_type("test.echo") is EchoOutput


def test_gateable_and_undoable_flags():
    registry = StepRegistry.for_tests(FakeEchoStep(), FakeGateStep(), FakeUndoableStep())
    assert registry.is_gateable("test.echo") is False
    assert registry.is_gateable("test.gate") is True
    assert registry.is_undoable("test.echo") is False
    assert registry.is_undoable("test.undoable") is True


def test_default_retry_and_timeout_lookup():
    registry = StepRegistry.for_tests(FakeEchoStep(), FakeGateStep())
    assert registry.default_retry("test.echo") == NAMED_POLICIES["none"]
    assert registry.default_retry("test.gate") == NAMED_POLICIES["kubectl_default"]
    assert registry.default_timeout_seconds("test.echo") == 30


def test_verb_returns_step_instance_and_none_on_miss():
    echo = FakeEchoStep()
    registry = StepRegistry.for_tests(echo)
    assert registry.verb("test.echo") is echo
    assert registry.verb("no.such.verb") is None  # config.py's VerbSpec | None contract


def test_verb_result_structurally_satisfies_config_verb_spec():
    """A Step instance already carries Params/Output/gateable/undoable as
    instance-readable ClassVars — no VerbSpec wrapper needed."""
    registry = StepRegistry.for_tests(FakeGateStep())
    spec = registry.verb("test.gate")
    assert spec is not None
    assert spec.Params is EchoParams
    assert spec.Output is EchoOutput
    assert spec.gateable is True
    assert spec.undoable is False


class _RegistryConfigAdapter:
    """Minimal composition proving StepRegistry.verb() is a drop-in VerbSpec source
    for engine/config.py's RegistryView. ``resolve_type`` is a test-local stand-in for
    whatever workflow-input type-name resolver the composition root eventually wires
    in — it is deliberately NOT StepRegistry's job (see engine/registry.py's module
    docstring)."""

    def __init__(self, registry: StepRegistry, types: dict[str, type] | None = None) -> None:
        self._registry = registry
        self._types = types or {}

    def verb(self, name: str):
        return self._registry.verb(name)

    def resolve_type(self, type_expr: str):
        return self._types.get(type_expr)


def test_step_registry_interoperates_with_engine_config_validator():
    """End-to-end proof against the real, concurrently-landed engine/config.py:
    StepRegistry needs no adaptation beyond .verb() to drive load_workflow()."""
    from seedpod.engine.config import load_workflow

    registry = StepRegistry.for_tests(FakeEchoStep())
    adapter = _RegistryConfigAdapter(registry)
    workflow_yaml = """
workflow: test-echo
version: 1
on_failure: report
outcome:
  succeeded: {event: RollbackFinished}
  failed: {event: RollbackFinished}
  cancelled: {event: RollbackFinished}
steps:
  - id: step1
    uses: test.echo
    with: {message: hi}
"""
    wf = load_workflow(workflow_yaml, adapter)
    assert wf.workflow == "test-echo"
    assert wf.steps[0].uses == "test.echo"


def test_step_registry_interop_surfaces_v1_violation_for_unregistered_verb():
    from seedpod.engine.config import ConfigError, load_workflow

    registry = StepRegistry.for_tests(FakeEchoStep())
    adapter = _RegistryConfigAdapter(registry)
    workflow_yaml = """
workflow: test-echo
version: 1
on_failure: report
outcome:
  succeeded: {event: RollbackFinished}
  failed: {event: RollbackFinished}
  cancelled: {event: RollbackFinished}
steps:
  - id: step1
    uses: not.a.registered.verb
"""
    with pytest.raises(ConfigError) as exc_info:
        load_workflow(workflow_yaml, adapter)
    assert exc_info.value.rule == "V1"


def test_registry_construction_is_explicit_di_not_global():
    """Two independently constructed registries with different fakes for the same
    verb must not interfere — there is no module-level global backing StepRegistry."""
    step_a = FakeEchoStep()
    step_b = FakeEchoStep()
    registry_a = StepRegistry.for_tests(step_a)
    registry_b = StepRegistry.for_tests(step_b)
    assert registry_a.get("test.echo") is step_a
    assert registry_b.get("test.echo") is step_b
    assert registry_a.get("test.echo") is not registry_b.get("test.echo")


# ---------------------------------------------------------------------------
# NAMED_TYPES / resolve_type_expr — the five DR-0028 deploy-path DTOs
# (docs/decisions/DR-0028-deploy-path-dtos.md, seedpod/core/deploy_wave.py),
# registered alongside ClusterSpecification/DnsRecordRef (Round 10 "dtos"
# component, Deliverable 2). No shipped workflow currently types an `inputs:`
# block with any of these five (grep-verified against config/workflows/*.yml —
# every `inputs:` entry today is `{type: str}`), so this module is the only
# place their resolution is exercised at all before a workflow actually needs
# one.
# ---------------------------------------------------------------------------

_DEPLOY_WAVE_DTOS = {
    "ManifestDoc": ManifestDoc,
    "DeploymentProfile": DeploymentProfile,
    "SnapshotRestoreSpec": SnapshotRestoreSpec,
    "Wave": Wave,
    "ApplyChangeSummary": ApplyChangeSummary,
}


@pytest.mark.parametrize("name,expected", list(_DEPLOY_WAVE_DTOS.items()), ids=list(_DEPLOY_WAVE_DTOS))
def test_named_types_resolves_each_deploy_wave_dto_by_bare_name(name, expected):
    assert NAMED_TYPES[name] is expected
    assert resolve_type_expr(name, NAMED_TYPES) is expected


@pytest.mark.parametrize("name,expected", list(_DEPLOY_WAVE_DTOS.items()), ids=list(_DEPLOY_WAVE_DTOS))
def test_named_types_resolves_each_deploy_wave_dto_inside_list(name, expected):
    assert resolve_type_expr(f"list[{name}]", NAMED_TYPES) == list[expected]


@pytest.mark.parametrize("name,expected", list(_DEPLOY_WAVE_DTOS.items()), ids=list(_DEPLOY_WAVE_DTOS))
def test_named_types_resolves_each_deploy_wave_dto_inside_optional(name, expected):
    assert resolve_type_expr(f"Optional[{name}]", NAMED_TYPES) == (expected | None)


def test_named_types_resolves_nested_optional_list_combination():
    """resolve_type_expr's grammar nests -- Optional[list[T]] and list[Optional[T]]
    both matter here: Wave.docs is a plain list[ManifestDoc], but
    DeployLoadAuditOutput/PlanWavesParams's new data_initialization field
    (tests/engine/declared_verbs.py) is Optional[SnapshotRestoreSpec], and a
    hypothetical `inputs:` block combining the two forms must resolve too."""
    assert resolve_type_expr("Optional[list[ManifestDoc]]", NAMED_TYPES) == (list[ManifestDoc] | None)
    assert resolve_type_expr("list[Optional[SnapshotRestoreSpec]]", NAMED_TYPES) == list[SnapshotRestoreSpec | None]


def test_step_registry_default_named_types_resolves_deploy_wave_dtos():
    """StepRegistry's default constructor (named_types=None) falls back to
    production's own NAMED_TYPES (engine/registry.py) -- proving the five DTOs
    are reachable through the REAL composition-root path, not only the
    module-level helper tested above."""
    registry = StepRegistry.for_tests(FakeEchoStep())
    assert registry.resolve_type("ApplyChangeSummary") is ApplyChangeSummary
    assert registry.resolve_type("list[Wave]") == list[Wave]
    assert registry.resolve_type("Optional[DeploymentProfile]") == (DeploymentProfile | None)


def test_a_workflow_declaring_a_deploy_wave_dto_input_validates_against_the_real_registry():
    """Round 10 "dtos" brief's own check: no shipped ``config/workflows/*.yml``
    currently types an ``inputs:`` block with any of the five DR-0028 DTOs --
    every shipped ``inputs:`` entry today is plain ``{type: str}``
    (grep-verified against all 8 files). This is the proof that validation
    WOULD succeed the moment one does, run end to end through the real
    ``engine/config.py`` pipeline (``load_workflow`` = ``parse_workflow`` +
    ``validate_workflow``), not just the bare ``resolve_type_expr`` call tested
    above."""
    from seedpod.engine.config import load_workflow

    registry = StepRegistry.for_tests(FakeEchoStep())
    adapter = _RegistryConfigAdapter(registry, types=dict(NAMED_TYPES))
    workflow_yaml = """
workflow: test-deploy-wave-dto-input
version: 1
inputs:
  profile: {type: DeploymentProfile}
on_failure: report
outcome:
  succeeded: {event: RollbackFinished}
  failed: {event: RollbackFinished}
  cancelled: {event: RollbackFinished}
steps:
  - id: step1
    uses: test.echo
    with: {message: hi}
"""
    wf = load_workflow(workflow_yaml, adapter)
    assert wf.inputs["profile"].type == "DeploymentProfile"


def test_declared_verbs_named_types_matches_the_real_registry_named_types():
    """tests/engine/declared_verbs.py's own NAMED_TYPES (what the shipped YAML
    validates against in test_shipped_workflows.py) must resolve every
    workflow-`inputs:` type name identically to production's -- this module's
    own docstring's "cannot drift on type identity" claim, checked directly here
    rather than merely asserted in prose. Reconciles the fixture and production
    dicts key-for-key, not just each dict's own internal consistency."""
    from tests.engine.declared_verbs import NAMED_TYPES as FIXTURE_NAMED_TYPES

    assert set(FIXTURE_NAMED_TYPES) == set(NAMED_TYPES)
    for name, real_type in NAMED_TYPES.items():
        assert FIXTURE_NAMED_TYPES[name] is real_type, f"{name}: fixture and production NAMED_TYPES disagree"
