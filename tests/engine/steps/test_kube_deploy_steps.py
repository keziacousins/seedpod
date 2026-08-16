"""tests/engine/steps/test_kube_deploy_steps.py — the two thin ``kube.*`` verbs the
deploy/rollback workflows bind (Round 8b): ``kube.cluster_info``
(``deploy-waves.yml``'s preflight) and ``kube.rollout_undo``
(``deploy-rollback.yml``'s one actuating step).

Against the REAL ``KubectlProvider`` backed by the shared conformance harness's FAKE
TRANSPORT (``tests/conformance/fake_kubectl.py``) — never ``Mock``/``patch``.

The load-bearing part is **crown jewel #13**: ``rollout undo``'s partial-success rule.
It lives in ``KubectlProvider._rollout_undo`` — that method raises ``PermanentError``
itself when ``succeeded == 0``, and yields a ``RolloutUndoResult`` tally otherwise.
(``RolloutUndoResult``'s docstring still says "the caller raises"; that wording
predates the provider taking the job. The step deliberately does NOT re-implement the
rule — one crown jewel, one home.)

What these tests assert is that the STEP behaves correctly at all three arms of that
rule rather than swallowing or distorting it: full success, PARTIAL success (>=1 undo
succeeded ⇒ success — the jewel), and every-attempt-failed (surfaced, not swallowed).
Plus the case a naive ``if not succeeded: raise`` would break: v1's explicit "no
deployments to undo ⇒ trivial success", which reaches the step as
``succeeded=0, failed=0``.
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import InfrastructureUnreachableError, PermanentError
from seedpod.engine.step import EmptyOutput, StepServices
from seedpod.engine.steps.kube import (
    KubeClusterInfo,
    KubeconfigParams,
    KubeRolloutUndoStep,
    RolloutUndoParams,
)
from seedpod.providers.contract import KubeGetClusterInfo, KubeRolloutUndo
from tests.conformance.harness import Fault
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG, KubectlHarness
from tests.engine.fakes import FakeSubprocessManager, make_step_context

_NS = "default"


def _ctx(providers):
    return make_step_context(services=StepServices(subprocess_manager=FakeSubprocessManager(), providers=providers))


def _seed_deployments(harness: KubectlHarness, *names: str) -> None:
    harness.backend.deployments = {
        (_NS, name): {
            "metadata": {"name": name, "namespace": _NS},
            "spec": {"replicas": 1},
            "status": {"readyReplicas": 1, "availableReplicas": 1, "updatedReplicas": 1},
        }
        for name in names
    }


def test_declares_the_dr_0022_contract_for_both():
    for step, verb in ((KubeClusterInfo(), "kube.cluster_info"), (KubeRolloutUndoStep(), "kube.rollout_undo")):
        assert step.verb == verb
        assert step.provider_name == "kubectl"
        assert step.plane == "provider"
        assert step.thin is True
        assert step.gateable is False
        assert step.undoable is False


# ---------------------------------------------------------------------------
# kube.cluster_info -- the preflight connectivity check.
# ---------------------------------------------------------------------------


def test_cluster_info_command_is_pure_and_maps_the_kubeconfig():
    step = KubeClusterInfo()
    params = KubeconfigParams(kubeconfig=FAKE_KUBECONFIG)

    first = step.command(params)
    second = step.command(params)

    assert first == second == KubeGetClusterInfo(kubeconfig=FAKE_KUBECONFIG)


async def test_cluster_info_succeeds_against_a_reachable_cluster():
    step = KubeClusterInfo()
    harness = KubectlHarness()

    output = await step.execute(KubeconfigParams(kubeconfig=FAKE_KUBECONFIG), _ctx({"kubectl": harness.provider()}))

    assert isinstance(output, EmptyOutput)


async def test_cluster_info_unreachable_propagates_as_itself():
    """The entire point of the preflight: an unreachable cluster must surface as
    "cannot determine state", never be swallowed into a success. `deploy-waves.yml`
    puts `retry: kubectl_default` behind this (H6) -- v1 ran it once and failed the
    whole deploy on a single blip."""
    step = KubeClusterInfo()
    harness = KubectlHarness()

    with pytest.raises(InfrastructureUnreachableError):
        await step.execute(
            KubeconfigParams(kubeconfig=FAKE_KUBECONFIG), _ctx({"kubectl": harness.provider(Fault.UNREACHABLE)})
        )


# ---------------------------------------------------------------------------
# kube.rollout_undo -- crown jewel #13, all three arms.
# ---------------------------------------------------------------------------


def test_rollout_undo_command_is_pure_and_maps_all_fields():
    step = KubeRolloutUndoStep()
    params = RolloutUndoParams(kubeconfig=FAKE_KUBECONFIG, namespace="traefik")

    first = step.command(params)
    second = step.command(params)

    assert first == second
    assert first == KubeRolloutUndo(kubeconfig=FAKE_KUBECONFIG, namespace="traefik")


def test_rollout_undo_namespace_defaults_to_default():
    assert RolloutUndoParams(kubeconfig=FAKE_KUBECONFIG).namespace == "default"


async def test_rollout_undo_succeeds_when_every_deployment_undoes():
    step = KubeRolloutUndoStep()
    harness = KubectlHarness()
    _seed_deployments(harness, "web", "api")

    output = await step.execute(
        RolloutUndoParams(kubeconfig=FAKE_KUBECONFIG, namespace=_NS), _ctx({"kubectl": harness.provider()})
    )

    assert isinstance(output, EmptyOutput)


async def test_rollout_undo_succeeds_on_PARTIAL_success():
    """Crown jewel #13's whole point: >=1 success is success. Failing the rollback
    because one of three deployments could not be undone would leave the other two
    rolled back and the run marked failed."""
    step = KubeRolloutUndoStep()
    harness = KubectlHarness()
    _seed_deployments(harness, "web", "api", "worker")
    harness.backend.rollout_undo_failures = frozenset({"api", "worker"})

    output = await step.execute(
        RolloutUndoParams(kubeconfig=FAKE_KUBECONFIG, namespace=_NS), _ctx({"kubectl": harness.provider()})
    )

    assert isinstance(output, EmptyOutput)


async def test_rollout_undo_raises_when_EVERY_attempt_failed():
    """The rule lives in `KubectlProvider._rollout_undo` (it raises on succeeded==0);
    this asserts the STEP surfaces it rather than swallowing it into a success."""
    step = KubeRolloutUndoStep()
    harness = KubectlHarness()
    _seed_deployments(harness, "web", "api")
    harness.backend.rollout_undo_failures = frozenset({"web", "api"})

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(
            RolloutUndoParams(kubeconfig=FAKE_KUBECONFIG, namespace=_NS), _ctx({"kubectl": harness.provider()})
        )

    assert "all 2 deployment undo(s) failed" in str(exc_info.value)


async def test_rollout_undo_on_an_EMPTY_namespace_is_trivial_success_not_failure():
    """v1 kept an explicit "no deployments to undo" success case
    (reference-code/.../kubernetes.py:965-966), which reaches the step as
    succeeded=0, failed=0. A naive `if not succeeded: raise` would fail a rollback in
    an empty namespace -- an empty namespace is not a failure to undo anything in."""
    step = KubeRolloutUndoStep()
    harness = KubectlHarness()
    harness.backend.deployments = {}

    output = await step.execute(
        RolloutUndoParams(kubeconfig=FAKE_KUBECONFIG, namespace=_NS), _ctx({"kubectl": harness.provider()})
    )

    assert isinstance(output, EmptyOutput)


async def test_rollout_undo_unreachable_propagates_and_is_never_a_failure_tally():
    """The provider raises for a genuine connectivity symptom mid-loop rather than
    folding it into `failed` (crown jewel #1 extended to that loop), so a nonzero
    `failed` reaching the step really does mean per-deployment rejections."""
    step = KubeRolloutUndoStep()
    harness = KubectlHarness()
    _seed_deployments(harness, "web")

    with pytest.raises(InfrastructureUnreachableError):
        await step.execute(
            RolloutUndoParams(kubeconfig=FAKE_KUBECONFIG, namespace=_NS),
            _ctx({"kubectl": harness.provider(Fault.UNREACHABLE)}),
        )
