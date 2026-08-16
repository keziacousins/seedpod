"""tests/conformance/test_c18_c19_kubeconfig.py — C-18/C-19 (Seam C §5.6 table).

    C-18 | test_kubeconfig_is_parameter | kubectl | every kubectl command works given only
    kubeconfig=, against a fake transport, no env/DB access; garbage kubeconfig ⇒
    Permanent(AUTH|INVALID_INPUT); no "kubeconfig_not_found" string anywhere
    C-19 | test_kubeconfig_rewrite_variants | machine+k3s | golden tests: kind matches
    127.0.0.1|localhost|0.0.0.0 and substitutes host and port; orbstack preserves source
    port; ssh variant preserves scheme+port, count=1 per entry

C-19 is driven entirely by each machine/k3s harness's own ``rewrite_cases()`` hook (Seam C
§5.6) — digitalocean/tart/kubectl return ``()`` (no ``FetchKubeconfig`` — §5.4 plane matrix)
and are skipped by construction, not by an explicit skip list.
"""

from __future__ import annotations

import re

import pytest

from seedpod.core.errors import InfrastructureUnreachableError, PermanentError
from seedpod.providers.contract import (
    Kubeconfig,
    KubeGetClusterInfo,
    KubeGetDeployments,
    KubeGetEvents,
    KubeGetNodes,
    KubeGetPodDetails,
    KubeGetPodLogs,
    KubeGetPods,
    Result,
)
from tests.conformance._support import drain
from tests.conformance.kubectl_harness import (
    FAKE_KUBECONFIG,
    NAMESPACE,
    SEEDED_POD,
    KubectlHarness,
)

pytestmark = pytest.mark.asyncio

# ---------------------------------------------------------------------------
# C-18 — kubeconfig is always a command field
# ---------------------------------------------------------------------------


async def test_every_read_command_works_given_only_kubeconfig_parameter():
    harness = KubectlHarness()
    provider = harness.provider()
    commands = (
        KubeGetClusterInfo(kubeconfig=FAKE_KUBECONFIG),
        KubeGetNodes(kubeconfig=FAKE_KUBECONFIG),
        KubeGetPods(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE),
        KubeGetPodDetails(kubeconfig=FAKE_KUBECONFIG, pod_name=SEEDED_POD, namespace=NAMESPACE),
        KubeGetPodLogs(kubeconfig=FAKE_KUBECONFIG, pod_name=SEEDED_POD, namespace=NAMESPACE),
        KubeGetDeployments(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE),
        KubeGetEvents(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE),
    )
    for cmd in commands:
        (result,) = await drain(provider, cmd)
        assert isinstance(result, Result), cmd


async def test_garbage_kubeconfig_is_typed_never_kubeconfig_not_found_string():
    harness = KubectlHarness()
    provider = harness.provider()
    with pytest.raises((PermanentError, InfrastructureUnreachableError)) as excinfo:
        await drain(provider, KubeGetClusterInfo(kubeconfig="not: valid: yaml: at: all:"))
    assert "kubeconfig_not_found" not in str(excinfo.value)


# ---------------------------------------------------------------------------
# C-19 — kubeconfig rewrite golden cases (crown jewel #6)
# ---------------------------------------------------------------------------


async def test_kubeconfig_rewrite_variants(harness):
    cases = harness.rewrite_cases()
    if not cases:
        pytest.skip(f"{harness.name} does not implement FetchKubeconfig (§5.4 plane matrix)")

    provider = harness.provider()
    for name, cmd, expected_pattern in cases:
        (result,) = await drain(provider, cmd)
        assert isinstance(result.value, Kubeconfig), name
        matches = re.findall(expected_pattern, result.value.yaml_text)
        assert len(matches) == 1, f"{name}: expected exactly one server-URL rewrite, found {len(matches)}"
