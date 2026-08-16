"""engine/steps/deploy_apply.py -- Round 10's "apply-and-wait" component: the
three verbs that actually reach a cluster and confirm each wave converged --
``kube.apply_docs``, ``deploy.ensure_rollouts``, ``deploy.await_wave``. All
three are ``plane="provider"`` (DR-0022 Erratum E12: every ``ProviderStep``
wraps a Seam C ``ProviderCommand``); only ``kube.apply_docs`` is ``thin``
(exactly one ``KubeApplyManifest`` per non-empty call) -- the other two are
composites issuing 0..N commands, matching ``tests/engine/declared_verbs.py``'s
own ``DECLARED_VERBS`` fixture rows for these three verbs, which this module's
Steps must reconcile against verbatim.

A separate module from ``seedpod/engine/steps/deploy.py`` (this round's
"load-and-plan" component, which explicitly disclaims registering these
three -- see that module's own docstring) and from ``seedpod/engine/steps/
kube.py`` (the provision-path ``kube.*`` shim, which likewise disclaims
``kube.apply_docs`` by name), so neither module's own "this module does not
register or import X" claim goes stale by this component's landing.

No new provider method is added and no committed provider logic is
reimplemented (this round's own brief, both frozen): every command below is
either an existing typed Seam C command (``KubeApplyManifest``,
``KubeRestartDeployment``, ``KubeProbeRollout``) or the generic ``KubeRun``
escape hatch already used by ``deploy.py``/``kube.py``'s own composites
(``deploy.prepare_wave``, ``kube.wipe_namespace``) -- used here because no
typed Seam C command exists for reading a Kubernetes ``Job``'s own status
(there is no ``KubeGetJobs``); the generic escape hatch is exactly what it
exists for.

**kube.apply_docs** -- DR-0028 decision 1 + decision 4, and DR-0022's own
named correctness fix (D1): ``undoable=False`` closes a latent deploy-time
DATA-LOSS path (a failed *application* deploy must never auto-delete the
user's own manifests -- see ``providers/compensation.py``'s own
``KubeApplyManifest`` arm, which documents this verb as the one
STRUCTURALLY unable to reach ``undo_for`` at all: ``ProviderStep.undo`` is
never even called for an ``undoable=False`` step, ``engine/engine.py:1069``).
Serializes ``params.docs`` back to one YAML string
(``seedpod/core/deploy_wave.py``'s ``serialize_manifest_documents``, DR-0028
decision 1's own explicit ask) for ``KubeApplyManifest.manifest_yaml: str``,
then parses the command's own ``Result`` (a plain ``str`` -- ``providers/
kubectl.py``'s ``_apply_manifest`` yields ``Result(result.stdout.decode(...))``
directly, NOT wrapped in ``KubectlOutput``, unlike the generic ``KubeRun``
below) into the three ``ApplyChangeSummary`` buckets, reading v1's own
documented line shape verbatim (``reference-code/seedpod/seedpod/jobs/state/
deployment_job.py:598-604``): ``"<kind>.<group>/<name> configured"`` /
``"... unchanged"`` / ``"... created"``.

**Total on empty input (DR-0028 Erratum E1 point 3, ratified 2026-08-08).**
``execute()`` short-circuits BEFORE touching ``ctx.services.providers`` at
all when ``params.docs`` is empty, returning an empty ``ApplyChangeSummary``
directly -- issuing NO ``KubeApplyManifest`` (a real `kubectl apply` against
an empty manifest file errors, so this is not merely an optimisation). The
frozen workflow grammar has no conditional with which a shipped workflow
could skip this step for a wave with no docs (CLAUDE.md: wanting one is the
stop signal, not a judgment call), so any verb reachable with empty input
must be total. This is DEFENSIVE ONLY, per E1's own words: E1 also deleted
the invented empty-docs wave that made an empty call reachable in the first
place (DR-0029 -- the restore attaches DIRECTLY to the persistence wave, no
separate empty-``docs`` wave exists any more), so this module builds nothing
that RELIES on an empty ``docs`` list ever actually reaching this verb.

**"unknown => assume changed" (Seam B's own rule, DR-0028 decision 4).** A
kubectl apply output line this module's parser cannot recognise (blank,
a stray warning, a future kubectl wording change) is silently NOT bucketed
into ``configured``/``created``/``unchanged`` at all -- never raised, never
guessed into a bucket. This is deliberate, not an oversight, and it is
NOT SAFE in one direction, named loudly rather than papered over (DR-0028's
own words, and this round's brief repeats it): ``ApplyChangeSummary.
all_unchanged`` (``seedpod/core/deploy_wave.py``) is true only when at least
one resource was seen AND every one of them was ``unchanged`` -- so an
unparseable line that (unluckily) represented a genuinely-unchanged resource
silently DROPS a rollout restart ``deploy.ensure_rollouts`` would otherwise
have forced, exactly the failure mode v1's own "assume changed" comment
(``deployment_job.py:614-616``, "Unexpected kubectl apply output format...
assume changed to avoid double rollout") accepted. The DIRECTION this
protects against -- forcing a REDUNDANT restart on top of a kubectl-apply
that already triggered one -- is merely wasteful, never destructive; the
direction it does NOT protect against -- silently skipping a restart a
genuinely-stuck deployment needed -- is real and is v1's own accepted
trade-off, salvaged as-is rather than "fixed" by inventing a stricter,
unreviewed heuristic this round's brief did not ask for.

**deploy.ensure_rollouts** -- the whole point of this component, salvaged
from v1's real rule (``deployment_job.py:598-636``, gotcha 4): FORCE a
rollout restart ONLY IF EVERY resource ``kube.apply_docs`` reported was
``unchanged`` -- expressed structurally via ``ApplyChangeSummary.
all_unchanged`` rather than v1's own whole-stdout substring scan
(``"configured" in output_lower or "created" in output_lower``). If anything
was ``configured``/``created``, kubectl already triggered the rollout itself,
and restarting again would be redundant churn, not merely harmless (v1's own
comment: "skipping rollout restart (kubectl apply already triggered
rollout)"). Runs once PER WAVE (``config/workflows/deploy-waves.yml``'s
``restart`` step, inside the ``wave`` foreach), unlike v1's single
whole-deployment decision -- ``ApplyChangeSummary``'s own docstring names
this precisely as the thing v1's ``is_update`` gate does not carry over
cleanly, and deliberately does not resolve it for this verb either: with no
whole-deployment "is this an update at all" fact available per-wave, this
verb's own rule is exactly what ``all_unchanged`` alone can express --
"restart iff this apply changed nothing" -- which is v1's rule minus the
``is_update`` gate v1 only ever used to avoid restarting a FRESH deployment
that had never rolled out at all. A brand-new wave's first apply reports
every resource as ``created`` (never ``unchanged``), so ``all_unchanged`` is
already false for it and no spurious restart fires -- the ``is_update`` gate
turns out to be redundant with ``all_unchanged`` for every real case it was
guarding, not merely dropped.

**Non-fatal per-deployment restart (salvaged, not dropped).** v1's own
``_restart_deployments`` is explicitly best-effort -- its own docstring:
"bool: True if restart succeeded, False if it failed (non-fatal)"
(``deployment_job.py:743``) -- it warns and CONTINUES to the next deployment
on a per-deployment failure (``:766``), and its caller logs "Rollout restart
encountered issues ... continuing anyway" and proceeds to the rollout check
regardless (``:627-629``). This module's ``execute()`` reproduces that
posture explicitly: a ``ProviderError`` from one deployment's
``KubeRestartDeployment`` is caught, reported via ``ctx.progress``, and the
loop continues to the next deployment -- one stuck/vanished Deployment must
not abort the whole wave, matching ``deploy.prepare_wave``'s identical split
in the same round (Job-delete/stuck-pod sweep, ``deploy.py``'s own class
docstring). ``InfrastructureUnreachableError`` is the one exception NOT
tolerated here, same split as ``deploy.prepare_wave``: CLAUDE.md's
error-taxonomy hard rule means "cannot determine state" must propagate,
never be read as a mere best-effort restart failure. Catching per-deployment
also keeps the loop retry-safe under the workflow's own
``retry: kubectl_default`` on the ``restart`` step (``config/workflows/
deploy-waves.yml``): because this step itself never raises for an ordinary
per-deployment restart failure, a retried run never re-issues `rollout
restart` for deployments that already succeeded on a prior attempt.

``command()``/``output_from()`` are deliberately NOT overridden: this
composite issues zero or N structurally-identical ``KubeRestartDeployment``
commands (one per ``params.deployments`` entry, ONLY when
``params.changes.all_unchanged``), and unlike ``deploy.prepare_wave``/
``kube.wipe_namespace`` (each of which always issues at least one command
unconditionally, so those modules give ``command()`` a real, always-issued
representative), there is no such unconditional command here to name
truthfully. ``execute()`` is fully overridden; ``ProviderStep.command()``'s
inherited ``NotImplementedError`` stub is never reached: the only other
caller is ``ProviderStep.undo()``, and this verb is ``undoable=False`` (the
``DECLARED_VERBS`` fixture default, unset by DR-0022's table -- a restart
issued as part of a deploy's own convergence has no sensible inverse either,
matching ``deploy.prepare_wave``'s identical reasoning), so
``engine/engine.py``'s own ``if not step.undoable: skip`` means
``.undo()`` -- and therefore ``.command()`` -- is never called for it at all.

**deploy.await_wave** -- THE readiness gate (DR-0022 P3: named ``await_x``
because it is ``gateable`` and its ``execute()`` is a true no-op --
``tests/engine/declared_verbs.py``'s own ``WaveReadyParams``/``gateable=True``
row). Replaces v1's bespoke ``_watch_pods_and_emit_events`` background SSE
task (``deployment_job.py``'s ``_wait_for_rollout``) with the engine's own
gate loop (Seam C taste call 2: no command waits, all waiting is an engine
gate) -- ``poll_ready`` issues ONE cheap round of probes per call, never a
loop, never a sleep.

Probes BOTH resource kinds a wave can carry, per ``config/workflows/
deploy-waves.yml``'s own ``ready`` step comment (normative, not merely
descriptive: "Deployments: rollout status --watch=false; Jobs:
condition=complete -- Failed => PermanentError naming wave+resource"):

- **Deployments**: exactly ``kube.await_rollout``'s own idiom
  (``engine/steps/kube.py``'s ``KubeAwaitRollout.poll_ready``), issuing one
  ``KubeProbeRollout`` per deployment name and reading ``RolloutState.
  complete`` -- a still-progressing rollout is NotReady, never raised (seam-
  c-provider.md fault table row 26, "rollout slow after apply -- not an
  error"); a genuine connectivity/auth/not-found symptom propagates
  unmodified (CLAUDE.md's error-taxonomy hard rule), never caught, never
  converted to absence.
- **Jobs**: no typed Seam C command exists for a Job's own status, so this
  reads it via the generic ``KubeRun`` escape hatch (``kubectl get jobs -n
  default -o json``, the SAME list-then-classify shape ``kube.py``'s
  ``_list_names``/``deploy.py``'s stuck-pod sweep already use for an
  analogous "no typed command" gap), and inspects each named Job's own
  ``.status.conditions`` for a ``type=Complete,status=True`` (ready) or
  ``type=Failed,status=True`` (a K8s Job's own DEFINITIVE failure -- raises
  ``PermanentError`` per ``engine/step.py``'s own ``poll_ready`` docstring
  example, verbatim: "may raise PermanentError for definitive failure (a K8s
  Job with condition=Failed)"). A Job absent from the listing (not yet
  reflected by the API server, or not yet applied) reads as NotReady, never
  raised -- deliberately mirroring ``kube.await_rollout``'s own
  never-fatal-for-a-transient-absence posture, not the pod-list's
  StuckPod-detection posture (a genuinely missing Job is exactly what a
  brand-new wave's ``ready`` gate is waiting to see appear).

Every probe emits ``ctx.progress`` once per ``poll_ready`` call with each
resource's own status (the workflow's own comment: "poll_ready emits
ctx.progress per poll with per-resource status (replaces the bespoke
watch_pods SSE task)").

Both the deployment and Job probes hardcode namespace ``"default"``, matching
v1's OWN hardcode across the entire deploy path (``deployment_job.py``'s
``_wait_for_rollout``/``_execute_kubectl_apply`` both always operate against
``namespace="default"``) and this round's own declared ``WaveReadyParams``/
``RolloutRestartParams`` fixture rows, neither of which carries a
``namespace`` field."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import ClassVar

from pydantic import BaseModel, SecretStr

from seedpod.core.deploy_wave import ApplyChangeSummary, ManifestDoc, serialize_manifest_documents
from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
)
from seedpod.engine.provider_step import ProviderStep
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepContext
from seedpod.providers.contract import (
    KubeApplyManifest,
    KubectlOutput,
    KubeProbeRollout,
    KubeRestartDeployment,
    KubeRun,
    Result,
    RolloutState,
)

__all__ = [
    "ApplyParams",
    "ApplyOutput",
    "KubeApplyDocs",
    "RolloutRestartParams",
    "DeployEnsureRollouts",
    "WaveReadyParams",
    "DeployAwaitWave",
]

# v1's own hardcode, everywhere along the deploy path (deployment_job.py never binds
# a namespace anywhere along this path either) -- see module docstring's final
# paragraph.
_NAMESPACE = "default"

_PROBE_TIMEOUT_S = 15.0


async def _drain(provider: object, command) -> object | None:
    """Run one command to completion, returning its ``Result.value`` (``Progress``
    events are ignored -- none of the commands this module issues emit any). A local
    copy of ``engine/steps/kube.py``/``engine/steps/deploy.py``'s own module-private
    ``_drain`` (not importable across modules -- neither carries a leading-underscore
    export); identical shape, same reasoning."""
    value: object | None = None
    async for ev in provider.execute(command):  # type: ignore[attr-defined]
        if isinstance(ev, Result):
            value = ev.value
    return value


def _expect(value: object, expected: type, *, verb: str) -> object:
    """Loud Result-shape guard (mirrors ``deploy.py``/``kube.py``'s own ``_expect``)."""
    if not isinstance(value, expected):
        raise PermanentError(
            f"{verb}: expected Result value of type {expected.__name__}, got {type(value).__name__}",
            code=ErrorCode.INVALID_INPUT,
            provider="engine",
            command=verb,
        )
    return value


# ---------------------------------------------------------------------------
# kube.apply_docs -- undoable=False (DR-0022 ruling 3, D1's fix).
# ---------------------------------------------------------------------------

# v1's own three literal verdict substrings (deployment_job.py:598-604), read off one
# per line: "<kind>.<group-or-'apps'>/<name> <verdict>". `resource` intentionally keeps
# the FULL leading token (e.g. "deployment.apps/foo", "service/bar") as kubectl itself
# prints it -- "resource identity", not a re-derived name -- so a caller can always
# trace a bucket entry back to the exact stdout line it came from.
_APPLY_LINE_RE = re.compile(r"^(?P<resource>\S+)\s+(?P<verdict>configured|created|unchanged)\s*$")


def _parse_apply_output(stdout: str) -> ApplyChangeSummary:
    """The ONE place ``kube.apply_docs``' own ``Result`` (kubectl apply's raw stdout,
    plain-text default output -- no ``-o`` override) is ever parsed. See module
    docstring's "unknown => assume changed" paragraph for why an unrecognised line is
    silently dropped rather than bucketed or raised."""
    buckets: dict[str, list[str]] = {"configured": [], "created": [], "unchanged": []}
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _APPLY_LINE_RE.match(line)
        if match is None:
            continue  # unparseable -> assume changed BY OMISSION; see module docstring.
        buckets[match.group("verdict")].append(match.group("resource"))
    return ApplyChangeSummary(configured=buckets["configured"], created=buckets["created"], unchanged=buckets["unchanged"])


class ApplyParams(BaseModel):
    kubeconfig: SecretStr
    docs: list[ManifestDoc]


class ApplyOutput(BaseModel):
    changes: ApplyChangeSummary


class KubeApplyDocs(ProviderStep[ApplyParams, ApplyOutput]):
    verb = "kube.apply_docs"
    provider_name: ClassVar[str] = "kubectl"
    Params = ApplyParams
    Output = ApplyOutput
    undoable = False  # DR-0022 ruling 3 (D1's fix) -- ProviderStep hard-defaults True;
    # see module docstring's opening paragraph for the data-loss path this closes.
    thin = True

    def command(self, params: ApplyParams) -> KubeApplyManifest:
        return KubeApplyManifest(
            kubeconfig=params.kubeconfig.get_secret_value(),
            manifest_yaml=serialize_manifest_documents(params.docs),
        )

    def output_from(self, value: object) -> ApplyOutput:
        stdout = _expect(value, str, verb=self.verb)
        assert isinstance(stdout, str)  # narrowed by _expect
        return ApplyOutput(changes=_parse_apply_output(stdout))

    async def execute(self, params: ApplyParams, ctx: StepContext) -> ApplyOutput:
        if not params.docs:
            # DR-0028 Erratum E1 point 3: total on empty input, issuing NO
            # KubeApplyManifest at all -- see module docstring.
            return ApplyOutput(changes=ApplyChangeSummary())
        return await super().execute(params, ctx)


# ---------------------------------------------------------------------------
# deploy.ensure_rollouts -- the restart-only-if-all-unchanged rule.
# ---------------------------------------------------------------------------


class RolloutRestartParams(BaseModel):
    kubeconfig: SecretStr
    deployments: list[str]
    changes: ApplyChangeSummary


class DeployEnsureRollouts(ProviderStep[RolloutRestartParams, EmptyOutput]):
    verb = "deploy.ensure_rollouts"
    provider_name: ClassVar[str] = "kubectl"
    Params = RolloutRestartParams
    Output = EmptyOutput
    undoable = False  # see module docstring's own paragraph for why.
    thin = False  # zero or N KubeRestartDeployment commands -- see module docstring.

    async def execute(self, params: RolloutRestartParams, ctx: StepContext) -> EmptyOutput:
        """Deliberately does NOT call ``super().execute()``/``self.command()`` --
        there is no single command this verb issues unconditionally (see module
        docstring). ``all_unchanged`` is the WHOLE decision (DR-0028 decision 4 /
        v1's own rule, ``deployment_job.py:609-626``): restart every one of
        ``params.deployments`` iff every resource ``kube.apply_docs`` reported for
        this wave was ``unchanged`` -- inverting this condition is exactly the bug
        this verb exists to not have (a test pins both directions)."""
        if not params.changes.all_unchanged:
            return EmptyOutput()
        provider = ctx.services.providers[self.provider_name]
        kubeconfig = params.kubeconfig.get_secret_value()
        for deployment in params.deployments:
            try:
                await _drain(
                    provider,
                    KubeRestartDeployment(kubeconfig=kubeconfig, deployment=deployment, namespace=_NAMESPACE),
                )
            except InfrastructureUnreachableError:
                # NOT tolerated -- CLAUDE.md's error-taxonomy hard rule: "cannot
                # determine state" must propagate, never be conflated with a merely
                # best-effort failure. Mirrors deploy.prepare_wave's identical split.
                raise
            except ProviderError as exc:
                # Best-effort, exactly v1's own posture (`_restart_deployments`,
                # deployment_job.py:743-767): its own docstring says "bool: True if
                # restart succeeded, False if it failed (non-fatal)", it warns and
                # CONTINUES to the next deployment on a per-deployment failure
                # (:766), and its caller logs "Rollout restart encountered issues
                # ... continuing anyway" and proceeds to the rollout check
                # (:627-629) rather than aborting the deployment. One deployment's
                # restart failing (a transient API error, or a Deployment that
                # vanished between apply and restart) must not abort the whole
                # wave, and must not fail this STEP either -- swallowing it here
                # (rather than merely relying on the workflow's own `on_failure:`)
                # also keeps the loop retry-safe: a `retry: kubectl_default`
                # re-run of this step never re-issues `rollout restart` for
                # deployments that already succeeded, because this step itself
                # never raises for a per-deployment restart failure.
                await ctx.progress(
                    f"deploy.ensure_rollouts: failed to restart deployment/{deployment}: {exc}"
                )
        return EmptyOutput()


# ---------------------------------------------------------------------------
# deploy.await_wave -- THE readiness gate; P3 (await_x <=> gateable + execute() no-op).
# ---------------------------------------------------------------------------


def _job_status(item: Mapping[str, object]) -> str:
    """"failed" | "complete" | "pending", read off one Job's own ``.status.conditions``
    (kubectl's own convention -- see module docstring's Jobs paragraph). Checks
    ``Failed`` before ``Complete``: a Job's terminal condition is definitive, and
    "failed" must win if a malformed/transitional document somehow carried both."""
    status = item.get("status")
    conditions = status.get("conditions") if isinstance(status, Mapping) else None
    if not isinstance(conditions, list):
        return "pending"
    for condition in conditions:
        if isinstance(condition, Mapping) and condition.get("type") == "Failed" and condition.get("status") == "True":
            return "failed"
    for condition in conditions:
        if isinstance(condition, Mapping) and condition.get("type") == "Complete" and condition.get("status") == "True":
            return "complete"
    return "pending"


async def _job_statuses(provider: object, kubeconfig: str, *, verb: str) -> dict[str, str]:
    """``kubectl get jobs -n default -o json`` -> ``{job_name: "failed"|"complete"|
    "pending"}`` for every Job the API server currently reports in the namespace. A
    Job named in ``params.jobs`` but absent from this mapping (not yet applied, or not
    yet reflected) reads as "pending" by the CALLER, not here -- this function only
    reports what it saw."""
    value = await _drain(
        provider,
        KubeRun(kubeconfig=kubeconfig, args=("get", "jobs", "-n", _NAMESPACE, "-o", "json"), timeout_s=_PROBE_TIMEOUT_S),
    )
    result = _expect(value, KubectlOutput, verb=verb)
    assert isinstance(result, KubectlOutput)  # narrowed by _expect
    stdout = result.stdout
    text = stdout.decode(errors="replace") if isinstance(stdout, bytes) else stdout
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise InfrastructureUnreachableError(
            f"{verb}: kubectl returned non-JSON output listing Jobs",
            code=ErrorCode.MALFORMED_RESPONSE,
            provider="kubectl",
            command=verb,
        ) from exc
    items = data.get("items") if isinstance(data, Mapping) else None
    if not isinstance(items, list):
        # Valid JSON that is not a List-kind object (or whose `items` isn't itself a
        # list) is a malformed response, not "zero Jobs" -- kubectl's own `-o json`
        # for a List kind ALWAYS carries an `items` array (empty when there are no
        # Jobs). Silently treating this as `{}` would render "I cannot determine
        # what Jobs exist" as "the Jobs are not ready yet" to every caller (every
        # name in `params.jobs` falls through to NotReady), spinning the gate to a
        # timeout instead of surfacing the real cause -- exactly the conflation
        # CLAUDE.md's error-taxonomy hard rule forbids. Mirrors the adjacent
        # JSONDecodeError arm above and `KubectlProvider._parse_json`'s own
        # discipline (providers/kubectl.py: "never a crash, never a
        # silently-empty result").
        raise InfrastructureUnreachableError(
            f"{verb}: kubectl returned a Jobs listing with no `items` array",
            code=ErrorCode.MALFORMED_RESPONSE,
            provider="kubectl",
            command=verb,
        )
    statuses: dict[str, str] = {}
    for item in items:
        if not isinstance(item, Mapping):
            continue
        metadata = item.get("metadata")
        name = metadata.get("name") if isinstance(metadata, Mapping) else None
        if not name:
            continue
        statuses[str(name)] = _job_status(item)
    return statuses


class WaveReadyParams(BaseModel):
    kubeconfig: SecretStr
    deployments: list[str]
    jobs: list[str]


class DeployAwaitWave(ProviderStep[WaveReadyParams, EmptyOutput]):
    verb = "deploy.await_wave"
    provider_name: ClassVar[str] = "kubectl"
    Params = WaveReadyParams
    Output = EmptyOutput
    gateable = True
    undoable = False  # ProviderStep hard-defaults True; a pure gate has nothing to
    # compensate (mirrors kube.await_rollout/k3s.await_ssh/infra.await_instance).
    thin = False  # 0..len(deployments) KubeProbeRollout + 0..1 KubeRun(get jobs).

    async def execute(self, params: WaveReadyParams, ctx: StepContext) -> EmptyOutput:
        """True no-op (DR-0022 P3) -- never touches ``ctx.services.providers``.
        Deliberately does NOT call ``super().execute()``/``self.command()``."""
        return EmptyOutput()

    async def poll_ready(
        self, params: WaveReadyParams, provisional: EmptyOutput, ctx: StepContext
    ) -> Ready[EmptyOutput] | NotReady:
        provider = ctx.services.providers[self.provider_name]
        kubeconfig = params.kubeconfig.get_secret_value()

        not_ready: list[str] = []
        deployment_status: dict[str, str] = {}
        for deployment in params.deployments:
            value = await _drain(
                provider,
                KubeProbeRollout(kubeconfig=kubeconfig, deployment=deployment, namespace=_NAMESPACE),
            )
            state = _expect(value, RolloutState, verb=self.verb)
            assert isinstance(state, RolloutState)  # narrowed by _expect
            deployment_status[deployment] = "ready" if state.complete else "waiting"
            if not state.complete:
                not_ready.append(f"deployment/{deployment}")

        job_status: dict[str, str] = {}
        if params.jobs:
            statuses = await _job_statuses(provider, kubeconfig, verb=self.verb)
            for job in params.jobs:
                status = statuses.get(job, "pending")
                job_status[job] = status
                if status == "failed":
                    # engine/step.py's own poll_ready docstring, verbatim: "may raise
                    # PermanentError for definitive failure (a K8s Job with
                    # condition=Failed)".
                    raise PermanentError(
                        f"{self.verb}: Job {job!r} reports condition Failed=True",
                        code=ErrorCode.SCRIPT_FAILED,
                        provider="engine",
                        command=self.verb,
                        detail={"job": job},
                    )
                if status != "complete":
                    not_ready.append(f"job/{job}")

        # Replaces v1's bespoke `_watch_pods_and_emit_events` SSE task -- see module
        # docstring's final paragraph before the namespace note.
        await ctx.progress("deploy.await_wave: poll", deployments=deployment_status, jobs=job_status)

        if not_ready:
            return NotReady(detail="; ".join(not_ready))
        return Ready(outputs=EmptyOutput())
