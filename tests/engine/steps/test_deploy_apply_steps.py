"""tests/engine/steps/test_deploy_apply_steps.py -- Round 10's "apply-and-wait"
component: ``kube.apply_docs``, ``deploy.ensure_rollouts``, ``deploy.await_wave``
(``seedpod/engine/steps/deploy_apply.py``).

Against the REAL ``KubectlProvider`` over the shared conformance fake transport
(``tests/conformance/kubectl_harness.py``/``tests/conformance/fake_kubectl.py``) --
never ``Mock``/``patch`` (CLAUDE.md). The apply-output PARSING itself
(``_parse_apply_output``) is exercised directly against literal kubectl stdout
strings via ``KubeApplyDocs.output_from`` -- a pure function, no IO, so no fake
transport is needed to cover all three verdict shapes plus the mixed/unparseable
cases DR-0028 decision 4 calls out by name.

Three load-bearing things this module pins:

1. ``kube.apply_docs`` is total on empty input (DR-0028 Erratum E1 point 3) and
   ``undoable=False`` (DR-0022 ruling 3, D1's own named correctness fix).
2. ``deploy.ensure_rollouts``' restart rule -- "restart iff every resource was
   unchanged" -- in BOTH directions, written so an inverted condition fails.
3. ``deploy.await_wave``'s ``poll_ready`` issues ONE round of probes per call
   (no loop, no sleep), handles Jobs via the generic ``KubeRun`` escape hatch,
   and raises ``PermanentError`` on a Job's own ``condition=Failed``.
"""

from __future__ import annotations

import pytest

from seedpod.core.deploy_wave import ApplyChangeSummary, ManifestDoc
from seedpod.core.errors import InfrastructureUnreachableError, PermanentError
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepServices
from seedpod.engine.steps.deploy_apply import (
    ApplyOutput,
    ApplyParams,
    DeployAwaitWave,
    DeployEnsureRollouts,
    KubeApplyDocs,
    RolloutRestartParams,
    WaveReadyParams,
)
from tests.conformance.harness import Fault
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG, KubectlHarness
from tests.engine.fakes import FakeSubprocessManager, RecordingProgressSink, make_step_context

_NS = "default"


def _ctx(providers, *, progress_sink: RecordingProgressSink | None = None):
    return make_step_context(
        services=StepServices(subprocess_manager=FakeSubprocessManager(), providers=providers),
        progress_sink=progress_sink,
    )


def _doc(kind: str, name: str, namespace: str = _NS) -> ManifestDoc:
    return ManifestDoc(
        kind=kind,
        name=name,
        namespace=namespace,
        body={"apiVersion": "v1" if kind != "Deployment" else "apps/v1", "kind": kind, "metadata": {"name": name, "namespace": namespace}},
    )


def _seed_deployments(harness: KubectlHarness, *names: str, ready: bool = True) -> None:
    harness.backend.deployments = {
        (_NS, name): {
            "metadata": {"name": name, "namespace": _NS},
            "spec": {"replicas": 1},
            "status": {
                "readyReplicas": 1 if ready else 0,
                "availableReplicas": 1 if ready else 0,
                "updatedReplicas": 1 if ready else 0,
            },
        }
        for name in names
    }


def _seed_job(harness: KubectlHarness, name: str, *, conditions: list[dict]) -> None:
    harness.backend.jobs[(_NS, name)] = {
        "metadata": {"name": name, "namespace": _NS},
        "status": {"conditions": conditions},
    }


# ---------------------------------------------------------------------------
# DR-0022 contract
# ---------------------------------------------------------------------------


def test_declares_the_dr_0022_contract():
    apply_step = KubeApplyDocs()
    assert apply_step.verb == "kube.apply_docs"
    assert apply_step.provider_name == "kubectl"
    assert apply_step.plane == "provider"
    assert apply_step.thin is True
    assert apply_step.gateable is False
    assert apply_step.undoable is False  # DR-0022 ruling 3 (D1's own named fix)

    restart_step = DeployEnsureRollouts()
    assert restart_step.verb == "deploy.ensure_rollouts"
    assert restart_step.plane == "provider"
    assert restart_step.thin is False
    assert restart_step.gateable is False
    assert restart_step.undoable is False

    wave_step = DeployAwaitWave()
    assert wave_step.verb == "deploy.await_wave"
    assert wave_step.plane == "provider"
    assert wave_step.thin is False
    assert wave_step.gateable is True  # DR-0022 P3: named await_x
    assert wave_step.undoable is False


# ---------------------------------------------------------------------------
# kube.apply_docs -- parsing all three verdict forms, mixed, unparseable.
# ---------------------------------------------------------------------------


def test_apply_output_parses_all_three_verdict_forms():
    step = KubeApplyDocs()
    stdout = "deployment.apps/foo configured\nservice/bar unchanged\nconfigmap/baz created\n"

    output = step.output_from(stdout)

    assert isinstance(output, ApplyOutput)
    assert output.changes == ApplyChangeSummary(
        configured=["deployment.apps/foo"], created=["configmap/baz"], unchanged=["service/bar"]
    )


def test_apply_output_parses_mixed_output_with_repeated_verdicts():
    step = KubeApplyDocs()
    stdout = (
        "deployment.apps/api configured\n"
        "deployment.apps/web unchanged\n"
        "service/api unchanged\n"
        "secret/ghcr-pull created\n"
    )

    output = step.output_from(stdout)

    assert output.changes.configured == ["deployment.apps/api"]
    assert output.changes.created == ["secret/ghcr-pull"]
    assert output.changes.unchanged == ["deployment.apps/web", "service/api"]
    assert output.changes.all_unchanged is False  # one resource was configured


def test_apply_output_all_unchanged_true_when_every_resource_unchanged():
    step = KubeApplyDocs()
    stdout = "deployment.apps/web unchanged\nservice/web unchanged\n"

    output = step.output_from(stdout)

    assert output.changes.all_unchanged is True


def test_apply_output_unparseable_line_is_dropped_not_bucketed():
    """DR-0028 decision 4's "unknown => assume changed": a line this parser cannot
    recognise contributes to NO bucket at all -- never guessed, never raised. See
    module docstring's own worked consequence: this can silently skip a restart that
    would have been needed, deliberately (not papered over)."""
    step = KubeApplyDocs()
    stdout = "deployment.apps/web unchanged\nWarning: some deprecation notice\n\n"

    output = step.output_from(stdout)

    assert output.changes == ApplyChangeSummary(configured=[], created=[], unchanged=["deployment.apps/web"])


def test_apply_output_entirely_unparseable_is_never_vacuously_all_unchanged():
    step = KubeApplyDocs()
    stdout = "some garbage kubectl will never actually print\n"

    output = step.output_from(stdout)

    assert output.changes == ApplyChangeSummary()
    assert output.changes.all_unchanged is False


def test_apply_output_from_wrong_type_raises_permanent_error():
    step = KubeApplyDocs()
    with pytest.raises(PermanentError):
        step.output_from(12345)


# ---------------------------------------------------------------------------
# kube.apply_docs -- total on empty input (DR-0028 Erratum E1 point 3).
# ---------------------------------------------------------------------------


async def test_apply_docs_empty_input_is_a_total_noop_issuing_no_command():
    step = KubeApplyDocs()
    harness = KubectlHarness()
    params = ApplyParams(kubeconfig=FAKE_KUBECONFIG, docs=[])

    before = harness.backend_attempts()
    output = await step.execute(params, _ctx({"kubectl": harness.provider()}))
    after = harness.backend_attempts()

    assert output == ApplyOutput(changes=ApplyChangeSummary())
    assert after == before, "empty docs must issue NO KubeApplyManifest at all"


async def test_apply_docs_command_maps_params_to_a_single_serialized_yaml_string():
    step = KubeApplyDocs()
    params = ApplyParams(kubeconfig=FAKE_KUBECONFIG, docs=[_doc("Deployment", "web"), _doc("Service", "web")])

    command = step.command(params)

    assert command.kubeconfig == FAKE_KUBECONFIG
    assert "kind: Deployment" in command.manifest_yaml
    assert "kind: Service" in command.manifest_yaml


async def test_apply_docs_end_to_end_against_the_real_provider():
    step = KubeApplyDocs()
    harness = KubectlHarness()
    params = ApplyParams(kubeconfig=FAKE_KUBECONFIG, docs=[_doc("Deployment", "web")])

    output = await step.execute(params, _ctx({"kubectl": harness.provider()}))

    assert isinstance(output, ApplyOutput)
    assert "deployment.apps/web" in (output.changes.configured + output.changes.created + output.changes.unchanged)
    assert ("Deployment", _NS, "web") in harness.backend.applied_manifests


# ---------------------------------------------------------------------------
# deploy.ensure_rollouts -- the restart-only-if-all-unchanged rule, both ways.
# ---------------------------------------------------------------------------


async def test_ensure_rollouts_restarts_when_every_resource_was_unchanged():
    """v1's real rule (deployment_job.py:609-626): force a restart iff EVERY
    resource kubectl reported was 'unchanged'. Inverting this condition (restart
    only when something CHANGED) must turn this test red."""
    step = DeployEnsureRollouts()
    harness = KubectlHarness()
    _seed_deployments(harness, "web", "api")
    params = RolloutRestartParams(
        kubeconfig=FAKE_KUBECONFIG,
        deployments=["web", "api"],
        changes=ApplyChangeSummary(unchanged=["deployment.apps/web", "deployment.apps/api"]),
    )

    output = await step.execute(params, _ctx({"kubectl": harness.provider()}))

    assert isinstance(output, EmptyOutput)
    restart_calls = [call for call in harness.backend.call_log if call[1:3] == ("rollout", "restart")]
    assert {call[3] for call in restart_calls} == {"deployment/web", "deployment/api"}


async def test_ensure_rollouts_does_not_restart_when_anything_changed():
    """The other arm: if ANYTHING was configured/created, kubectl already triggered
    the rollout -- restarting again is redundant churn, not merely harmless
    (v1's own comment: "skipping rollout restart... kubectl apply already
    triggered rollout"). Inverting the condition (restart when something changed)
    must turn THIS test red too -- together the two pin both directions."""
    step = DeployEnsureRollouts()
    harness = KubectlHarness()
    _seed_deployments(harness, "web")
    params = RolloutRestartParams(
        kubeconfig=FAKE_KUBECONFIG,
        deployments=["web"],
        changes=ApplyChangeSummary(configured=["deployment.apps/web"]),
    )

    output = await step.execute(params, _ctx({"kubectl": harness.provider()}))

    assert isinstance(output, EmptyOutput)
    assert not any(call[1:3] == ("rollout", "restart") for call in harness.backend.call_log)


async def test_ensure_rollouts_does_not_restart_on_an_entirely_unparseable_apply():
    """The 'unknown' case: no resource counted unchanged at all is never
    ``all_unchanged`` (ApplyChangeSummary's own pinned property) -- so this verb
    must not restart on an entirely-empty/unparseable ApplyChangeSummary either."""
    step = DeployEnsureRollouts()
    harness = KubectlHarness()
    _seed_deployments(harness, "web")
    params = RolloutRestartParams(kubeconfig=FAKE_KUBECONFIG, deployments=["web"], changes=ApplyChangeSummary())

    await step.execute(params, _ctx({"kubectl": harness.provider()}))

    assert not any(call[1:3] == ("rollout", "restart") for call in harness.backend.call_log)


async def test_ensure_rollouts_with_no_deployments_is_a_noop_even_when_unchanged():
    step = DeployEnsureRollouts()
    harness = KubectlHarness()
    params = RolloutRestartParams(
        kubeconfig=FAKE_KUBECONFIG, deployments=[], changes=ApplyChangeSummary(unchanged=["configmap/x"])
    )

    output = await step.execute(params, _ctx({"kubectl": harness.provider()}))

    assert isinstance(output, EmptyOutput)
    assert not any(call[1:3] == ("rollout", "restart") for call in harness.backend.call_log)


async def test_ensure_rollouts_one_failing_restart_does_not_abort_the_wave():
    """v1's own posture (`_restart_deployments`, deployment_job.py:743-767): non-fatal,
    best-effort, and the loop CONTINUES to the next deployment on a per-deployment
    failure. `Fault.AUTH` fails every command uniformly, so both deployments' restart
    calls fail here -- proving both that a failing restart never raises out of
    `execute()`, and that the loop does not stop at the first failure (both
    deployments are attempted, and both failures are reported via `ctx.progress`)."""
    step = DeployEnsureRollouts()
    harness = KubectlHarness()
    _seed_deployments(harness, "web", "api")
    sink = RecordingProgressSink()
    params = RolloutRestartParams(
        kubeconfig=FAKE_KUBECONFIG,
        deployments=["web", "api"],
        changes=ApplyChangeSummary(unchanged=["deployment.apps/web", "deployment.apps/api"]),
    )

    output = await step.execute(params, _ctx({"kubectl": harness.provider(Fault.AUTH)}, progress_sink=sink))

    assert isinstance(output, EmptyOutput)  # never raises -- non-fatal
    messages = [call[5] for call in sink.calls]
    assert len(messages) == 2, "both deployments attempted -- the loop does not stop at the first failure"
    assert any("web" in msg for msg in messages)
    assert any("api" in msg for msg in messages)


async def test_ensure_rollouts_infrastructure_unreachable_propagates_not_swallowed():
    """The one exception NOT tolerated -- CLAUDE.md's error-taxonomy hard rule:
    `InfrastructureUnreachableError` means "cannot determine state" and must never be
    read as a mere best-effort restart failure. Mirrors `deploy.prepare_wave`'s
    identical split for its own Job-delete loop."""
    step = DeployEnsureRollouts()
    harness = KubectlHarness()
    _seed_deployments(harness, "web")
    params = RolloutRestartParams(
        kubeconfig=FAKE_KUBECONFIG,
        deployments=["web"],
        changes=ApplyChangeSummary(unchanged=["deployment.apps/web"]),
    )

    with pytest.raises(InfrastructureUnreachableError):
        await step.execute(params, _ctx({"kubectl": harness.provider(Fault.UNREACHABLE)}))


# ---------------------------------------------------------------------------
# deploy.await_wave -- P3 gate: execute() a true no-op.
# ---------------------------------------------------------------------------


async def test_await_wave_execute_is_a_true_noop_never_touching_providers():
    class _ExplodingProviders(dict):
        def __getitem__(self, key):
            raise AssertionError(f"execute() must never look up a provider, got {key!r}")

    step = DeployAwaitWave()
    ctx = _ctx(_ExplodingProviders())
    params = WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=["web"], jobs=["migrate"])

    output = await step.execute(params, ctx)

    assert isinstance(output, EmptyOutput)


async def test_await_wave_poll_ready_issues_exactly_one_round_of_probes_and_never_sleeps():
    """No loop, no retry, no sleep inside the step -- the engine owns the gate loop
    (Seam C taste call 2). One `rollout status` per deployment, one `get jobs` for
    however many jobs are named (never zero commands per named job)."""
    step = DeployAwaitWave()
    harness = KubectlHarness()
    _seed_deployments(harness, "web", "api")
    _seed_job(harness, "migrate", conditions=[{"type": "Complete", "status": "True"}])
    params = WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=["web", "api"], jobs=["migrate"])

    before = harness.backend_attempts()
    result = await step.poll_ready(params, EmptyOutput(), _ctx({"kubectl": harness.provider()}))
    after = harness.backend_attempts()

    assert after - before == 3, "exactly 2 rollout probes + 1 jobs list, never more"
    assert isinstance(result, Ready)


async def test_await_wave_ready_when_all_deployments_and_jobs_are_done():
    step = DeployAwaitWave()
    harness = KubectlHarness()
    _seed_deployments(harness, "web")
    _seed_job(harness, "migrate", conditions=[{"type": "Complete", "status": "True"}])
    params = WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=["web"], jobs=["migrate"])

    result = await step.poll_ready(params, EmptyOutput(), _ctx({"kubectl": harness.provider()}))

    assert isinstance(result, Ready)
    assert result.outputs == EmptyOutput()


async def test_await_wave_not_ready_while_a_deployment_is_still_rolling_out():
    step = DeployAwaitWave()
    harness = KubectlHarness()
    _seed_deployments(harness, "web", ready=False)
    params = WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=["web"], jobs=[])

    result = await step.poll_ready(params, EmptyOutput(), _ctx({"kubectl": harness.provider()}))

    assert isinstance(result, NotReady)


async def test_await_wave_not_ready_while_a_job_has_not_completed_yet():
    step = DeployAwaitWave()
    harness = KubectlHarness()
    _seed_job(harness, "migrate", conditions=[])

    result = await step.poll_ready(
        WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=[], jobs=["migrate"]),
        EmptyOutput(),
        _ctx({"kubectl": harness.provider()}),
    )

    assert isinstance(result, NotReady)


async def test_await_wave_not_ready_when_a_named_job_has_not_appeared_yet():
    """A Job absent from the listing entirely (not yet reflected by the API server,
    or not yet applied) is NotReady, never raised -- mirrors kube.await_rollout's
    own never-fatal-for-a-transient-absence posture."""
    step = DeployAwaitWave()
    harness = KubectlHarness()

    result = await step.poll_ready(
        WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=[], jobs=["not-applied-yet"]),
        EmptyOutput(),
        _ctx({"kubectl": harness.provider()}),
    )

    assert isinstance(result, NotReady)


async def test_await_wave_raises_infrastructure_unreachable_on_non_json_jobs_listing():
    """`_job_statuses`' own ``json.JSONDecodeError`` arm: a garbage (non-JSON) `get
    jobs` body must raise ``InfrastructureUnreachableError``, never read as "no Jobs
    exist yet" -- CLAUDE.md's error-taxonomy hard rule. Pins the taxonomy discipline
    so a later change can't soften this arm into a silent ``{}``."""
    step = DeployAwaitWave()
    harness = KubectlHarness()
    harness.backend.get_jobs_raw_stdout_override = b"not json at all"

    with pytest.raises(InfrastructureUnreachableError):
        await step.poll_ready(
            WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=[], jobs=["migrate"]),
            EmptyOutput(),
            _ctx({"kubectl": harness.provider()}),
        )


async def test_await_wave_raises_infrastructure_unreachable_on_jobs_listing_missing_items():
    """Valid JSON that is not a List-kind object (or whose `items` isn't itself a
    list) is a malformed response, not "zero Jobs" -- a real `kubectl get jobs -o
    json` ALWAYS carries an `items` array. Without this guard every name in
    `params.jobs` would silently fall through to NotReady, spinning the gate to a
    timeout instead of surfacing the real cause."""
    step = DeployAwaitWave()
    harness = KubectlHarness()
    harness.backend.get_jobs_raw_stdout_override = b'{"kind": "Status", "status": "Failure"}'

    with pytest.raises(InfrastructureUnreachableError):
        await step.poll_ready(
            WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=[], jobs=["migrate"]),
            EmptyOutput(),
            _ctx({"kubectl": harness.provider()}),
        )


async def test_await_wave_connectivity_failure_propagates_not_read_as_not_ready():
    """A genuine connectivity/auth symptom (``Fault.UNREACHABLE``) must propagate
    unmodified out of ``poll_ready`` -- never converted to a NotReady/absence
    reading (CLAUDE.md's error-taxonomy hard rule)."""
    step = DeployAwaitWave()
    harness = KubectlHarness()

    with pytest.raises(InfrastructureUnreachableError):
        await step.poll_ready(
            WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=[], jobs=["migrate"]),
            EmptyOutput(),
            _ctx({"kubectl": harness.provider(Fault.UNREACHABLE)}),
        )


async def test_await_wave_raises_permanent_error_on_a_failed_job():
    """engine/step.py's own poll_ready docstring, verbatim: "may raise
    PermanentError for definitive failure (a K8s Job with condition=Failed)"."""
    step = DeployAwaitWave()
    harness = KubectlHarness()
    _seed_job(harness, "migrate", conditions=[{"type": "Failed", "status": "True"}])

    with pytest.raises(PermanentError) as exc_info:
        await step.poll_ready(
            WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=[], jobs=["migrate"]),
            EmptyOutput(),
            _ctx({"kubectl": harness.provider()}),
        )

    assert "migrate" in str(exc_info.value)


async def test_await_wave_emits_progress_with_per_resource_status():
    """Replaces v1's bespoke `_watch_pods_and_emit_events` SSE task -- see module
    docstring's final paragraph before the namespace note."""
    step = DeployAwaitWave()
    harness = KubectlHarness()
    _seed_deployments(harness, "web")
    _seed_job(harness, "migrate", conditions=[{"type": "Complete", "status": "True"}])
    sink = RecordingProgressSink()

    await step.poll_ready(
        WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=["web"], jobs=["migrate"]),
        EmptyOutput(),
        _ctx({"kubectl": harness.provider()}, progress_sink=sink),
    )

    assert len(sink.calls) == 1
    fields = sink.calls[0][-1]
    assert fields["deployments"] == {"web": "ready"}
    assert fields["jobs"] == {"migrate": "complete"}


async def test_await_wave_no_jobs_named_issues_no_get_jobs_call():
    step = DeployAwaitWave()
    harness = KubectlHarness()
    _seed_deployments(harness, "web")

    await step.poll_ready(
        WaveReadyParams(kubeconfig=FAKE_KUBECONFIG, deployments=["web"], jobs=[]),
        EmptyOutput(),
        _ctx({"kubectl": harness.provider()}),
    )

    assert not any(call[1:3] == ("get", "jobs") for call in harness.backend.call_log)
