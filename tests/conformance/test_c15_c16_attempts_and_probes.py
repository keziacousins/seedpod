"""tests/conformance/test_c15_c16_attempts_and_probes.py — C-15/C-16 (Seam C §5.6 table).

    C-15 | test_single_attempt_no_internal_retry | all + services (⊂) | TRANSIENT_ONCE fault
    ⇒ exactly one transport attempt then TransientError; wall time shows no internal sleep;
    second execution succeeds (H4-H6)
    C-16 | test_probes_are_one_iteration | machine+k3s+kubectl | ProbeInstance/ProbeK3s/
    KubeProbeRollout return not-ready promptly; they never block until ready

A generous but meaningful wall-clock budget (2s) stands in for "no internal sleep loop":
every fake transport here answers in microseconds, so a bug that reintroduces a v1-style
``retry_delay`` sleep (H4-H6) would blow well past it.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from seedpod.providers.contract import KubeProbeRollout, Result
from tests.conformance._support import drain
from tests.conformance.harness import Fault
from tests.conformance.kubectl_harness import (
    FAKE_KUBECONFIG,
    NAMESPACE,
    SEEDED_DEPLOYMENT,
    KubectlHarness,
)

pytestmark = pytest.mark.asyncio

_NO_SLEEP_BUDGET_S = 2.0


async def test_single_attempt_no_internal_retry_then_succeeds_on_reinvocation(harness):
    rows = {fault: (cls, code) for fault, cls, code in harness.classification_cases()}
    if Fault.TRANSIENT_ONCE not in rows:
        pytest.skip(f"{harness.name} declares no TRANSIENT_ONCE classification_cases() row")
    expected_cls, _ = rows[Fault.TRANSIENT_ONCE]

    provider = harness.provider(Fault.TRANSIENT_ONCE)
    cmd = harness.classification_command(Fault.TRANSIENT_ONCE)

    before = harness.backend_attempts()
    start = time.monotonic()
    with pytest.raises(expected_cls):
        await drain(provider, cmd)
    elapsed = time.monotonic() - start

    assert harness.backend_attempts() - before == 1, "exactly one transport attempt, no internal retry loop"
    assert elapsed < _NO_SLEEP_BUDGET_S, f"{harness.name}: {elapsed:.2f}s suggests an internal retry/sleep loop"

    # The fault is single-shot (TRANSIENT_ONCE): the same provider instance must now succeed.
    await drain(provider, cmd)


# ---------------------------------------------------------------------------
# C-16 — probes are one bounded iteration, never a blocking wait-until-ready
# ---------------------------------------------------------------------------


async def test_probe_instance_or_probe_k3s_is_one_iteration(harness):
    if harness.name == "kubectl":
        pytest.skip("kubectl's probe (KubeProbeRollout) is covered by its own test below")
    provider = harness.provider()
    start = time.monotonic()
    events = await asyncio.wait_for(drain(provider, harness.observe_command()), timeout=_NO_SLEEP_BUDGET_S)
    elapsed = time.monotonic() - start

    assert len(events) == 1
    assert isinstance(events[0], Result)
    assert elapsed < _NO_SLEEP_BUDGET_S


async def test_kube_probe_rollout_is_one_iteration():
    harness = KubectlHarness()
    harness.backend.deployments[(NAMESPACE, SEEDED_DEPLOYMENT)]["status"]["readyReplicas"] = 0
    provider = harness.provider()

    start = time.monotonic()
    events = await asyncio.wait_for(
        drain(provider, KubeProbeRollout(kubeconfig=FAKE_KUBECONFIG, deployment=SEEDED_DEPLOYMENT, namespace=NAMESPACE)),
        timeout=_NO_SLEEP_BUDGET_S,
    )
    elapsed = time.monotonic() - start

    assert len(events) == 1
    (result,) = events
    assert isinstance(result, Result)
    assert result.value.complete is False, "still-progressing rollout is a Result, not a raise, and not a block"
    assert elapsed < _NO_SLEEP_BUDGET_S
