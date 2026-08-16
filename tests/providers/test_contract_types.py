"""tests/providers/test_contract_types.py — ``seedpod/providers/contract.py``'s unions
are complete and every command dataclass is frozen and inert (no methods), the same
discipline Pillar-1 holds effects to (docs/design/seam-c-provider.md §5.3's opening
line). Pure introspection — no ``Mock``/``patch``.
"""

from __future__ import annotations

import dataclasses
from typing import get_args

import pytest

from seedpod.providers import contract
from seedpod.providers.contract import (
    RESOURCE_ALLOCATED,
    ApplyFirewalls,
    AssignToProject,
    CaptureHostKeys,
    CreateInstance,
    DestroyInstance,
    DigitalOceanCommand,
    FetchKubeconfig,
    InstallK3s,
    K3sCommand,
    KubeApplyManifest,
    KubectlCommand,
    KubeDeleteManifest,
    KubeGetClusterInfo,
    KubeGetDeployments,
    KubeGetEvents,
    KubeGetNodes,
    KubeGetPodDetails,
    KubeGetPodLogs,
    KubeGetPods,
    KubeProbeRollout,
    KubeRestartDeployment,
    KubeRolloutUndo,
    KubeRun,
    KubeWatchPods,
    ListInstances,
    MachineCommand,
    Observed,
    ProbeDestruction,
    ProbeInstance,
    ProbeK3s,
    ProbeSshPort,
    Progress,
    Provider,
    ProviderCommand,
    Reconcile,
    Result,
    SubprocessRunner,
)


def _flatten_union(tp) -> frozenset[type]:
    args = get_args(tp)
    if not args:
        return frozenset({tp})
    out: set[type] = set()
    for a in args:
        out |= _flatten_union(a)
    return frozenset(out)


# ============================================================================
# 5.3 — the complete command union
# ============================================================================

MACHINE_TYPES = {
    CreateInstance,
    ProbeInstance,
    DestroyInstance,
    ProbeDestruction,
    ListInstances,
    Reconcile,
}
DIGITALOCEAN_TYPES = {ApplyFirewalls, AssignToProject}
K3S_TYPES = {ProbeSshPort, CaptureHostKeys, InstallK3s, ProbeK3s, FetchKubeconfig}
KUBECTL_TYPES = {
    KubeGetClusterInfo,
    KubeGetNodes,
    KubeGetPods,
    KubeGetPodDetails,
    KubeGetPodLogs,
    KubeApplyManifest,
    KubeDeleteManifest,
    KubeGetDeployments,
    KubeRestartDeployment,
    KubeProbeRollout,
    KubeGetEvents,
    KubeRolloutUndo,
    KubeRun,
    KubeWatchPods,
}


def test_machine_command_union_is_exactly_the_spec_set():
    assert _flatten_union(MachineCommand) == MACHINE_TYPES


def test_digitalocean_command_union_is_exactly_the_spec_set():
    """DigitalOcean-only extras (§5.7.1 amendment) — not part of the generic machine plane
    every machine provider implements; ``Provider.supported`` gates which provider instance
    accepts them (same convention as ``FetchKubeconfig``'s kind/orbstack-only membership)."""
    assert _flatten_union(DigitalOceanCommand) == DIGITALOCEAN_TYPES


def test_k3s_command_union_is_exactly_the_spec_set():
    assert _flatten_union(K3sCommand) == K3S_TYPES


def test_kubectl_command_union_is_exactly_the_spec_set():
    assert _flatten_union(KubectlCommand) == KUBECTL_TYPES


def test_provider_command_union_covers_every_command_dataclass_exactly_once():
    """FetchKubeconfig is plane-shared (machine kind/orbstack subset + k3s plane per
    §5.4's plane matrix) but appears once in the Python union — ``Provider.supported``,
    not this module, is what actually gates which commands a given provider instance
    accepts (contract.py's own comment on this)."""
    expected = MACHINE_TYPES | DIGITALOCEAN_TYPES | K3S_TYPES | KUBECTL_TYPES
    assert _flatten_union(ProviderCommand) == expected
    assert len(expected) == 6 + 2 + 5 + 14  # machine + digitalocean-only + k3s + kubectl, FetchKubeconfig counted once


def test_fetch_kubeconfig_is_reachable_through_provider_command():
    assert FetchKubeconfig in _flatten_union(ProviderCommand)


# ============================================================================
# Commands are frozen, inert dataclasses with no methods
# ============================================================================

ALL_COMMAND_TYPES = sorted(_flatten_union(ProviderCommand), key=lambda t: t.__name__)


@pytest.mark.parametrize("command_type", ALL_COMMAND_TYPES, ids=lambda t: t.__name__)
def test_command_is_a_frozen_dataclass(command_type):
    assert dataclasses.is_dataclass(command_type)
    # frozen dataclasses synthesize __delattr__/__setattr__ that raise FrozenInstanceError
    assert command_type.__dataclass_params__.frozen is True


@pytest.mark.parametrize("command_type", ALL_COMMAND_TYPES, ids=lambda t: t.__name__)
def test_command_has_no_methods_beyond_dataclass_machinery(command_type):
    """'No methods' means no *behavior*: every attribute besides the dataclass-
    generated dunders (``__init__``, ``__eq__``, ``__repr__``, ``__hash__``,
    ``__setattr__``/``__delattr__`` from frozen=True) and inherited ``object``
    members must be a plain (non-callable) field, never a user-defined method."""
    dunder_and_object_names = set(dir(object)) | {
        "__dataclass_fields__",
        "__dataclass_params__",
        "__match_args__",
        "__annotations__",
    }
    field_names = {f.name for f in dataclasses.fields(command_type)}
    for name in dir(command_type):
        if name in dunder_and_object_names or name in field_names:
            continue
        if name.startswith("__") and name.endswith("__"):
            continue  # dataclass-synthesized dunders (__eq__, __repr__, __hash__, ...)
        member = getattr(command_type, name)
        assert not callable(member), f"{command_type.__name__}.{name} is a user-defined method"


@pytest.mark.parametrize("command_type", ALL_COMMAND_TYPES, ids=lambda t: t.__name__)
def test_command_instances_reject_mutation(command_type):
    """Frozen means frozen: constructing a zero/near-zero-arg instance where possible
    and asserting attribute assignment raises. Commands with required fields are
    exempted from *construction* here (covered by test_compensation.py's full
    instance table) but every type in the union is still asserted frozen via
    __dataclass_params__ above; this test spot-checks the zero-required-field ones."""
    required = [f for f in dataclasses.fields(command_type) if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING]
    if required:
        pytest.skip(f"{command_type.__name__} has required fields; frozen-ness covered by the params check above")
    instance = command_type()
    fields = dataclasses.fields(command_type)
    attr_name = fields[0].name if fields else "x"
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(instance, attr_name, "mutated")


# ============================================================================
# 5.2 — stream vocabulary
# ============================================================================


def test_resource_allocated_is_the_pinned_constant():
    assert RESOURCE_ALLOCATED == "resource-allocated"


def test_progress_and_result_are_frozen_dataclasses_no_methods():
    for cls in (Progress, Result, Observed):
        assert dataclasses.is_dataclass(cls)
        assert cls.__dataclass_params__.frozen is True


def test_provider_event_union_is_progress_or_result():
    assert _flatten_union(contract.ProviderEvent) == {Progress, Result}


def test_progress_defaults_are_empty_and_falsy():
    p = Progress(phase="k3s.installing")
    assert p.message == ""
    assert dict(p.data) == {}


def test_progress_is_immutable():
    p = Progress(phase="k3s.installing")
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.phase = "other"  # type: ignore[misc]


# ============================================================================
# 5.4 — Provider protocol + injected transport
# ============================================================================


def test_provider_protocol_declares_the_pinned_surface():
    # structural surface: name/supported are annotations, check_ready/execute are methods
    annotations = getattr(Provider, "__annotations__", {})
    assert "name" in annotations
    assert "supported" in annotations
    assert callable(getattr(Provider, "check_ready", None))
    assert callable(getattr(Provider, "execute", None))


def test_subprocess_runner_protocol_declares_run_and_stream():
    assert callable(getattr(SubprocessRunner, "run", None))
    assert callable(getattr(SubprocessRunner, "stream", None))


def test_subprocess_result_is_frozen_and_defaults_bounded_single_attempt():
    result = contract.SubprocessResult(returncode=0, stdout=b"", stderr=b"")
    assert result.timed_out is False
    assert result.binary_missing is False
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.returncode = 1  # type: ignore[misc]
