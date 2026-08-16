"""tests/conformance/test_kubectl_smoke.py — smoke coverage proving the ``kubectl`` provider
streams per Seam C §5.2 against its fake transport, and that ``KubectlHarness`` is wired
correctly. The full parametrized C-01..C-24 suite is written by a later agent against
``tests/conformance/harness.Harness``; this file is a narrower, provider-local proof (stream
shape, H18 kubeconfig-is-a-parameter, row 27-31 classification/absence, the H17 apply-manifest
two-file leak fix, KubeRolloutUndo's partial-success crown jewel, KubeRun's bytes channel,
KubeWatchPods's hardening) so that agent's suite has a known-good provider to slot in against.

No ``Mock``/``patch`` anywhere — every fault is injected at ``FakeKubectlTransport``.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from seedpod.core.errors import InfrastructureUnreachableError, PermanentError
from seedpod.providers.contract import (
    KubeApplyManifest,
    KubectlOutput,
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
    PodDetailsResult,
    Progress,
    Result,
    RolloutState,
    RolloutUndoResult,
    WatchEnded,
)
from seedpod.providers.kubectl import KubectlConfig, KubectlProvider
from tests.conformance.fake_kubectl import (
    INVALID_MANIFEST_MARKER,
    FakeKubectlTransport,
)
from tests.conformance.harness import Fault
from tests.conformance.kubectl_harness import (
    FAKE_KUBECONFIG,
    NAMESPACE,
    SEEDED_DEPLOYMENT,
    SEEDED_POD,
    KubectlHarness,
)

pytestmark = pytest.mark.asyncio

_MANIFEST = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: demo
  namespace: default
spec:
  replicas: 1
"""


async def _drain(provider, cmd):
    events = []
    async for ev in provider.execute(cmd):
        events.append(ev)
    return events


# ---------------------------------------------------------------------------
# check_ready / C-01
# ---------------------------------------------------------------------------


async def test_check_ready_succeeds_against_healthy_backend():
    harness = KubectlHarness()
    provider = harness.provider()
    await provider.check_ready()  # must not raise


async def test_check_ready_fails_fast_on_broken_environment():
    harness = KubectlHarness()
    with harness.broken_environment() as provider:
        with pytest.raises(PermanentError) as excinfo:
            await provider.check_ready()
        assert excinfo.value.code == "not_found"


# ---------------------------------------------------------------------------
# stream shape / C-02, H18 kubeconfig-is-a-parameter / C-18
# ---------------------------------------------------------------------------


async def test_get_cluster_info_stream_shape_result_only():
    harness = KubectlHarness()
    provider = harness.provider()
    events = await _drain(provider, KubeGetClusterInfo(kubeconfig=FAKE_KUBECONFIG))
    assert len(events) == 1
    assert isinstance(events[0], Result)
    assert "10.96.0.1" in events[0].value


async def test_get_nodes_parses_dtos():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeGetNodes(kubeconfig=FAKE_KUBECONFIG))
    (node,) = result.value
    assert node.name == "node-1"
    assert node.status == "Ready"
    assert node.roles == "control-plane"
    assert node.version == "v1.29.0+k3s1"


async def test_get_pods_namespace_and_all_namespaces():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeGetPods(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE))
    (pod,) = result.value
    assert pod.name == SEEDED_POD
    assert pod.ready == "1/1"
    assert pod.image == "web:1.0"

    (all_result,) = await _drain(provider, KubeGetPods(kubeconfig=FAKE_KUBECONFIG, namespace=None))
    assert len(all_result.value) == 1
    assert "-A" in harness.backend.call_log[-1]


async def test_get_pod_logs():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeGetPodLogs(kubeconfig=FAKE_KUBECONFIG, pod_name=SEEDED_POD, namespace=NAMESPACE))
    assert "log line 1" in result.value


async def test_no_kubeconfig_not_found_string_anywhere():
    # H18: kubeconfig is ALWAYS a command field. Even a garbage kubeconfig produces a typed
    # classification, never the v1 "kubeconfig_not_found" string.
    harness = KubectlHarness()
    provider = harness.provider()
    with pytest.raises((PermanentError, InfrastructureUnreachableError)) as excinfo:
        await _drain(provider, KubeGetClusterInfo(kubeconfig="not: valid: yaml: at: all:"))
    assert "kubeconfig_not_found" not in str(excinfo.value)


# ---------------------------------------------------------------------------
# row 30 — get NotFound is absence-as-data, never conflated with unreachable (crown jewel #1)
# ---------------------------------------------------------------------------


async def test_get_pod_details_found():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeGetPodDetails(kubeconfig=FAKE_KUBECONFIG, pod_name=SEEDED_POD, namespace=NAMESPACE))
    assert isinstance(result.value, PodDetailsResult)
    assert result.value.found is True
    assert result.value.details.name == SEEDED_POD


async def test_get_pod_details_not_found_is_data_not_raise():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeGetPodDetails(kubeconfig=FAKE_KUBECONFIG, pod_name="ghost", namespace=NAMESPACE))
    assert result.value == PodDetailsResult(found=False, details=None)


async def test_absent_vs_unreachable_never_conflated():
    # Same command shape, two different backend symptoms: authoritative absence -> Result;
    # connectivity failure -> raise. They must diverge (crown jewel #1).
    harness = KubectlHarness()
    provider = harness.provider()
    (absent,) = await _drain(provider, KubeGetPodDetails(kubeconfig=FAKE_KUBECONFIG, pod_name="ghost", namespace=NAMESPACE))
    assert absent.value.found is False

    unreachable_provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(unreachable_provider, KubeGetPodDetails(kubeconfig=FAKE_KUBECONFIG, pod_name=SEEDED_POD, namespace=NAMESPACE))


# ---------------------------------------------------------------------------
# rows 27/28/29 — classification table / C-17
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fault,expected_cls,expected_code",
    KubectlHarness().classification_cases(),
    ids=lambda v: v.value if isinstance(v, Fault) else str(v),
)
async def test_classification_table(fault, expected_cls, expected_code):
    harness = KubectlHarness()
    provider = harness.provider(fault)
    with pytest.raises(expected_cls) as excinfo:
        await _drain(provider, harness.observe_command())
    assert excinfo.value.code == expected_code


async def test_row27_unreachable_carries_apiserver_host():
    harness = KubectlHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError) as excinfo:
        await _drain(provider, KubeGetNodes(kubeconfig=FAKE_KUBECONFIG))
    assert excinfo.value.host == "https://10.96.0.1:6443"


async def test_row29_apply_validation_error_is_invalid_input():
    harness = KubectlHarness()
    provider = harness.provider()
    manifest = _MANIFEST + INVALID_MANIFEST_MARKER + "\n"
    with pytest.raises(PermanentError) as excinfo:
        await _drain(provider, KubeApplyManifest(kubeconfig=FAKE_KUBECONFIG, manifest_yaml=manifest))
    assert excinfo.value.code == "invalid_input"


# ---------------------------------------------------------------------------
# row 31 — rollout progressing is a Result, never a raise
# ---------------------------------------------------------------------------


async def test_probe_rollout_complete():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeProbeRollout(kubeconfig=FAKE_KUBECONFIG, deployment=SEEDED_DEPLOYMENT, namespace=NAMESPACE))
    assert isinstance(result.value, RolloutState)
    assert result.value.complete is True


async def test_probe_rollout_still_progressing_is_result_not_raise():
    harness = KubectlHarness()
    harness.backend.deployments[(NAMESPACE, SEEDED_DEPLOYMENT)]["status"]["readyReplicas"] = 0
    provider = harness.provider()
    (result,) = await _drain(provider, KubeProbeRollout(kubeconfig=FAKE_KUBECONFIG, deployment=SEEDED_DEPLOYMENT, namespace=NAMESPACE))
    assert result.value.complete is False


# ---------------------------------------------------------------------------
# H17 — apply-manifest two-file leak fix + delete-manifest (NEW command, §5.7.3)
# ---------------------------------------------------------------------------


async def test_apply_then_delete_manifest_roundtrip_leaves_backend_clean():
    harness = KubectlHarness()
    provider = harness.provider()
    await _drain(provider, KubeApplyManifest(kubeconfig=FAKE_KUBECONFIG, manifest_yaml=_MANIFEST))
    assert await harness.backend_resources() == frozenset({"Deployment/default/demo"})

    await _drain(provider, KubeDeleteManifest(kubeconfig=FAKE_KUBECONFIG, manifest_yaml=_MANIFEST))
    assert await harness.backend_resources() == frozenset()


async def test_delete_manifest_ignore_not_found_is_idempotent():
    harness = KubectlHarness()
    provider = harness.provider()
    # Never applied — deleting with the default ignore_not_found=True must still succeed.
    await _drain(provider, KubeDeleteManifest(kubeconfig=FAKE_KUBECONFIG, manifest_yaml=_MANIFEST))
    await _drain(provider, KubeDeleteManifest(kubeconfig=FAKE_KUBECONFIG, manifest_yaml=_MANIFEST))  # twice: still success


async def test_apply_manifest_tempfiles_cleaned_up_no_leak():
    harness = KubectlHarness()
    provider = harness.provider()
    registry_root = provider._tempfiles.root
    await _drain(provider, KubeApplyManifest(kubeconfig=FAKE_KUBECONFIG, manifest_yaml=_MANIFEST))
    if registry_root.exists():
        assert list(registry_root.iterdir()) == []


# ---------------------------------------------------------------------------
# KubeGetDeployments / KubeRestartDeployment
# ---------------------------------------------------------------------------


async def test_get_deployments():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeGetDeployments(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE))
    (deployment,) = result.value
    assert deployment.name == SEEDED_DEPLOYMENT
    assert deployment.ready_replicas == 1


async def test_restart_deployment():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeRestartDeployment(kubeconfig=FAKE_KUBECONFIG, deployment=SEEDED_DEPLOYMENT, namespace=NAMESPACE))
    assert "restarted" in result.value


# ---------------------------------------------------------------------------
# KubeGetEvents — sort desc + limit, salvaged verbatim
# ---------------------------------------------------------------------------


async def test_get_events_sorted_desc_and_limited():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeGetEvents(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE, limit=1))
    assert len(result.value) == 1
    assert result.value[0].reason == "Started"  # later last_timestamp sorts first


# ---------------------------------------------------------------------------
# KubeRolloutUndo — crown jewel #13, partial-success semantics EXACT
# ---------------------------------------------------------------------------


async def test_rollout_undo_no_deployments_is_trivial_success():
    harness = KubectlHarness()
    harness.backend.deployments.clear()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeRolloutUndo(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE))
    assert result.value == RolloutUndoResult(succeeded=0, failed=0, outputs="No deployments to undo", errors="")


async def test_rollout_undo_all_succeed():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeRolloutUndo(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE))
    assert result.value.succeeded == 1
    assert result.value.failed == 0


async def test_rollout_undo_partial_success_still_succeeds():
    harness = KubectlHarness()
    harness.backend.deployments[(NAMESPACE, "web-2")] = {
        "metadata": {"name": "web-2", "namespace": NAMESPACE}, "spec": {"replicas": 1},
        "status": {"readyReplicas": 1, "availableReplicas": 1, "updatedReplicas": 1},
    }
    harness.backend.rollout_undo_failures = frozenset({"web-2"})
    provider = harness.provider()
    (result,) = await _drain(provider, KubeRolloutUndo(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE))
    assert result.value.succeeded == 1
    assert result.value.failed == 1
    assert "web-2" in result.value.errors


async def test_rollout_undo_all_fail_raises_permanent_with_aggregated_errors():
    harness = KubectlHarness()
    harness.backend.rollout_undo_failures = frozenset({SEEDED_DEPLOYMENT})
    provider = harness.provider()
    with pytest.raises(PermanentError) as excinfo:
        await _drain(provider, KubeRolloutUndo(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE))
    assert SEEDED_DEPLOYMENT in excinfo.value.detail["errors"]


async def test_rollout_undo_connectivity_symptom_mid_loop_raises_not_swallowed():
    # Genuine improvement over v1: a connectivity symptom mid-loop is never folded into the
    # per-deployment failure tally.
    harness = KubectlHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, KubeRolloutUndo(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE))


# ---------------------------------------------------------------------------
# KubeRun — crown jewel #14, bytes channel
# ---------------------------------------------------------------------------


async def test_run_binary_output_not_decoded():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(
        provider, KubeRun(kubeconfig=FAKE_KUBECONFIG, args=("exec", "pod", "--", "pg_dump", "-Fc"), binary=True)
    )
    assert isinstance(result.value, KubectlOutput)
    assert isinstance(result.value.stdout, bytes)
    assert result.value.stdout.startswith(b"\x50\x47\x44\x4d\x50")


async def test_run_text_output_decoded():
    harness = KubectlHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, KubeRun(kubeconfig=FAKE_KUBECONFIG, args=("cluster-info",)))
    assert isinstance(result.value.stdout, str)


# ---------------------------------------------------------------------------
# KubeWatchPods — all v1 hardening
# ---------------------------------------------------------------------------


async def test_watch_pods_skips_non_json_and_non_dict_lines():
    harness = KubectlHarness()
    harness.backend.watch_lines = [
        b"not json at all",
        b'"just a string"',  # valid JSON, not a dict
        json.dumps({"type": "ADDED", "object": {"metadata": {"name": "p1", "namespace": "default"}, "status": {"phase": "Pending"}, "spec": {"containers": []}}}).encode(),
    ]
    provider = harness.provider()
    events = await _drain(provider, KubeWatchPods(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE, timeout_s=5))
    *progress, terminal = events
    assert len(progress) == 1
    assert progress[0].data["event"].pod_name == "p1"
    assert isinstance(terminal, Result)
    assert isinstance(terminal.value, WatchEnded)
    assert terminal.value.reason == "stream_ended"


async def test_watch_pods_heartbeat_does_not_abort_before_deadline():
    harness = KubectlHarness()
    harness.backend.watch_lines = [
        json.dumps({"type": "MODIFIED", "object": {"metadata": {"name": "p1", "namespace": "default"}, "status": {}, "spec": {"containers": []}}}).encode(),
    ]
    harness.backend.watch_line_delay_s = 0.05
    provider = KubectlProvider(KubectlConfig(watch_heartbeat_s=0.01), FakeKubectlTransport(harness.backend, frozenset()))
    events = await _drain(provider, KubeWatchPods(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE, timeout_s=5))
    progress_events = [e for e in events if isinstance(e, Progress)]
    assert len(progress_events) == 1  # the heartbeat timeout retried, didn't abort the watch


async def test_watch_pods_overall_timeout():
    harness = KubectlHarness()
    harness.backend.watch_lines = [b'{"type": "ADDED", "object": {}}'] * 5
    harness.backend.watch_line_delay_s = 0.05
    provider = KubectlProvider(KubectlConfig(), FakeKubectlTransport(harness.backend, frozenset()))
    events = await _drain(provider, KubeWatchPods(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE, timeout_s=0.01))
    terminal = events[-1]
    assert isinstance(terminal.value, WatchEnded)
    assert terminal.value.reason == "timeout"


async def test_watch_pods_cancellation_reraises_cancelled_error():
    harness = KubectlHarness()
    harness.backend.watch_lines = [b'{"type": "ADDED", "object": {}}'] * 1000
    harness.backend.watch_line_delay_s = 0.05
    provider = harness.provider()

    async def _consume():
        async for _ in provider.execute(KubeWatchPods(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE, timeout_s=60)):
            pass

    task = asyncio.ensure_future(_consume())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_watch_pods_connectivity_fault_raises():
    harness = KubectlHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, KubeWatchPods(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE, timeout_s=5))


# ---------------------------------------------------------------------------
# unsupported command / C-24
# ---------------------------------------------------------------------------


async def test_unsupported_command_rejected_with_zero_backend_traffic():
    harness = KubectlHarness()
    provider = harness.provider()
    before = harness.backend_attempts()
    with pytest.raises(PermanentError) as excinfo:
        provider.execute(ListInstances())
    assert excinfo.value.code == "unsupported"
    assert harness.backend_attempts() == before


# ---------------------------------------------------------------------------
# single attempt, no internal retry / C-15
# ---------------------------------------------------------------------------


async def test_single_attempt_no_internal_retry_then_succeeds_on_reinvocation():
    harness = KubectlHarness()
    provider = harness.provider(Fault.TRANSIENT_ONCE)

    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, KubeGetNodes(kubeconfig=FAKE_KUBECONFIG))
    assert harness.backend_attempts() == 1  # exactly one transport attempt, no internal retry loop

    (result,) = await _drain(provider, KubeGetNodes(kubeconfig=FAKE_KUBECONFIG))
    assert len(result.value) == 1
