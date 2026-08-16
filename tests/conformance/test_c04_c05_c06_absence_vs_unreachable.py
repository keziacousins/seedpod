"""tests/conformance/test_c04_c05_c06_absence_vs_unreachable.py — C-04/C-05/C-06 (Seam C §5.6
table), crown jewel #1: "absence is DATA, unreachable is a RAISE — never conflated."

    C-04 | test_unreachable_raises | machine+kubectl | injected control-plane outage on any
    state-determining call ⇒ InfrastructureUnreachableError with host set — never Permanent,
    never an absent-looking Result
    C-05 | test_absence_is_data | all + services (⊂) | reachable backend, nonexistent thing ⇒
    typed Result (phase="absent", found=False, ready=False, [], existed=False), no exception
    C-06 | test_absent_vs_unreachable_never_conflated | machine | authoritative absence
    (docker rc!=0, DO empty list w/ 200) ⇒ absent; connectivity failure ⇒ raise — parametrized
    over both, asserting they diverge

``_absence_probe`` maps each harness to the command+predicate a smoke test already proved
demonstrates absence-as-data (mined from ``test_{provider}_smoke.py``); providers with no
resource-identity absence concept at all (``orbstack`` — deliberate "never orphans"/"no absent
phase" deviation, its module docstring; ``ssh-k3s`` — ``ProbeK3s`` targets a fixed host, never a
resource id, so it has no "nonexistent thing" to probe) are capability-skipped with a reason.
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import InfrastructureUnreachableError
from seedpod.providers.contract import KubeGetPodDetails, ProbeInstance
from tests.conformance._support import (
    HARNESS_CLASSES,
    MACHINE_HARNESS_CLASSES,
    MACHINE_NAMES,
    classification_rows,
    drain,
    skip_if,
)
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG, NAMESPACE

pytestmark = pytest.mark.asyncio

# ---------------------------------------------------------------------------
# C-04 — Unreachable-raising subset of the classification table, machine+kubectl only
# ---------------------------------------------------------------------------

_C04_ROWS = [
    row
    for row in classification_rows()
    if row.values[2] is InfrastructureUnreachableError and (row.values[0].name in MACHINE_NAMES or row.values[0].name == "kubectl")
]


@pytest.mark.parametrize("harness_cls,fault,expected_cls,expected_code", _C04_ROWS)
async def test_unreachable_raises_with_host_set(harness_cls, fault, expected_cls, expected_code):
    harness = harness_cls()
    provider = harness.provider(fault)
    cmd = harness.classification_command(fault)
    with pytest.raises(InfrastructureUnreachableError) as excinfo:
        await drain(provider, cmd)
    assert excinfo.value.host, "InfrastructureUnreachableError must carry a non-empty host"


# ---------------------------------------------------------------------------
# C-05 / C-06 — absence-as-data, and its divergence from unreachable
# ---------------------------------------------------------------------------

_ABSENCE_SKIPS = {
    "orbstack": "orbstack has no 'absent' phase (deliberate deviation — module docstring); "
    "a reachable backend never yields absence-as-data here, only running or a raise",
    "ssh-k3s": "ssh-k3s's ProbeK3s targets a fixed host, not a resource id — it has no "
    "resource-identity absence concept to probe (row 19 is readiness-only)",
}


def _absence_probe(harness):
    """(command, predicate) pair proving absence-as-data for this provider, or ``None`` if
    absence is structurally inapplicable (see ``_ABSENCE_SKIPS``)."""
    name = harness.name
    if name == "digitalocean":
        return ProbeInstance(resource_ids={"droplet_id": "does-not-exist"}), lambda v: v.phase == "absent"
    if name == "kind":
        return ProbeInstance(resource_ids={"kind_cluster_name": "seedpod-ghost"}), lambda v: v.phase == "absent"
    if name == "tart":
        return ProbeInstance(resource_ids={"tart_vm_name": "seedpod-ghost"}), lambda v: v.phase == "absent"
    if name == "kubectl":
        return (
            KubeGetPodDetails(kubeconfig=FAKE_KUBECONFIG, pod_name="definitely-a-ghost-pod", namespace=NAMESPACE),
            lambda v: v.found is False,
        )
    return None


@pytest.mark.parametrize("harness_cls", HARNESS_CLASSES, ids=[c.name for c in HARNESS_CLASSES])
async def test_absence_is_data_no_exception(harness_cls):
    harness = harness_cls()
    skip_if(_ABSENCE_SKIPS, harness.name)
    cmd, is_absent = _absence_probe(harness)

    (result,) = await drain(harness.provider(), cmd)
    assert is_absent(result.value), f"{harness.name}: reachable-but-nonexistent must be typed absence data"


@pytest.mark.parametrize("harness_cls", MACHINE_HARNESS_CLASSES, ids=[c.name for c in MACHINE_HARNESS_CLASSES])
async def test_absent_vs_unreachable_never_conflated(harness_cls):
    harness = harness_cls()
    skip_if(_ABSENCE_SKIPS, harness.name)
    cmd, is_absent = _absence_probe(harness)

    (absent_result,) = await drain(harness.provider(), cmd)
    assert is_absent(absent_result.value)

    unreachable_rows = [row for row in classification_rows() if row.values[0].name == harness.name and row.values[2] is InfrastructureUnreachableError]
    assert unreachable_rows, f"{harness.name} must have at least one Unreachable classification row for this test to be meaningful"
    _, fault, _, _ = unreachable_rows[0].values
    broken = harness.provider(fault)
    with pytest.raises(InfrastructureUnreachableError):
        await drain(broken, cmd)
