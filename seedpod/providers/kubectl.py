"""seedpod/providers/kubectl.py — the ``kubectl`` Provider (Seam C §5.3-5.4, decision-table
rows 27-31, amended by ``docs/design/coherence-review.md`` Conflicts 5-7, 12).

The kubernetes plane (the full ``KubectlCommand`` union). Talks to a target cluster's API
server exclusively via the local ``kubectl`` CLI over an **injected** ``SubprocessRunner``
(§5.4's construction contract) — no ``asyncio.create_subprocess_exec``/
``create_tracked_subprocess`` call inside this module.

Salvaged from ``reference-code/seedpod/seedpod/providers/kubernetes.py`` (``KubectlProvider``)
and ``reference-code/seedpod/seedpod/utils/kubectl.py`` (``execute_kubectl`` and friends — a
near-duplicate of the same subprocess-per-call pattern; nothing there is salvaged that isn't
already salvaged from ``kubernetes.py`` below):

- ``PodInfo``/``PodDetails``/``NodeInfo``/``DeploymentInfo``/``EventInfo``/``PodWatchEvent``/
  ``_format_age`` → copied verbatim to ``seedpod/providers/kube_types.py`` (that module's own
  docstring covers the salvage citation + the frozen-dataclass/explicit-``now`` deviations).
- ``get_cluster_info`` (372-405) → ``_get_cluster_info`` below.
- ``get_nodes`` (407-481) → ``_get_nodes`` + ``_parse_node`` below, field mapping verbatim
  (Ready-condition scan, ``node-role.kubernetes.io/`` label stripping, kubelet version).
- ``get_pods`` (483-574) → ``_get_pods`` + ``_parse_pod`` below, verbatim (ready-count from
  ``containerStatuses``, first-container image, ``-A`` for ``namespace=None``).
- ``get_pod_details`` (576-702) → ``_get_pod_details`` + ``_parse_pod_details`` below, verbatim
  (conditions/init-containers/containers assembly) — **plus** row 30's new NotFound → typed
  absence branch (v1 only had a generic ``kubectl failed`` string).
- ``get_pod_logs`` (704-752) → ``_get_pod_logs`` below, verbatim (``--tail``/``-c``/``--previous``
  flag construction).
- ``apply_manifest`` (754-797) → ``_apply_manifest`` below — the H17 two-file leak ordering is
  fixed here: both the kubeconfig and manifest temp files are created via one
  ``TempFileRegistry.files()`` call (``seedpod/core/tempfiles.py``), which unlinks whatever WAS
  created if creating the second file raises, and unlinks both in ``finally`` on every exit path.
  v1 created the kubeconfig file, then the manifest file in a second, unguarded
  ``NamedTemporaryFile`` block — if that second creation raised, the kubeconfig file leaked
  forever (no ``finally`` had run yet).
- ``get_deployments`` (799-855) → ``_fetch_deployments`` below, verbatim.
- ``restart_deployment`` (857-896) → ``_restart_deployment`` below, verbatim.
- ``wait_for_rollout`` (898-938) → **NOT ported as a blocking wait.** Seam C's taste call 2
  (no command waits, all waiting is an engine gate) replaces it with single-shot
  ``KubeProbeRollout``/``_probe_rollout`` below: one ``kubectl rollout status --watch=false``
  call, ``RolloutState(complete=...)`` Result: the engine's own wait-for-readiness gate is what
  polls this repeatedly. The blocking ``--timeout={timeout}s`` flag and the loop's own
  ``asyncio.wait_for`` wrapper are both gone.
- ``rollout_undo`` (940-1006) → ``_rollout_undo`` below, **crown jewel #13, partial-success
  semantics preserved EXACTLY**: fetch every deployment in the namespace, attempt
  ``kubectl rollout undo`` on each, tally successes/failures, success iff ``>=1`` succeeded —
  but see this module's own genuine improvement below (a connectivity symptom mid-loop is no
  longer silently folded into the per-deployment failure tally; it raises immediately, since
  "cannot determine state" must never be conflated with an ordinary per-deployment failure,
  crown jewel #1 extended to this loop).
- ``get_events`` (1008-1087) → ``_get_events`` below, verbatim (``--all-namespaces`` for
  ``namespace=None``, ``last_timestamp`` descending sort, then ``limit``).
- ``watch_pods`` (1089-1256) → ``_watch_pods`` below. **ALL v1 hardening salvaged**:
  ``--output-watch-events`` framing (v1:1125), skip non-JSON lines (v1:1173-1178), skip non-dict
  JSON (v1:1180-1183), the 30s readline-heartbeat pattern that lets the loop notice the overall
  deadline without blocking forever on a quiet stream (v1:1146-1155, now
  ``KubectlConfig.watch_heartbeat_s``), stderr harvest + terminate→kill at stream end
  (v1:1158-1166, 1244-1250 — now the injected ``SubprocessRunner.stream()``'s own ``__aexit__``
  responsibility, per that method's contract docstring; this module only consumes the resulting
  line iterator), ``CancelledError`` re-raised (v1:1238-1240 — here, simply never caught, so it
  propagates through the ``async with`` unwind on its own).
- ``run_kubectl`` (1258-1307) → ``_run`` below, verbatim — **crown jewel #14**: ``binary=True``
  returns ``stdout`` as raw, undecoded ``bytes`` (required for ``pg_dump -Fc`` snapshot
  streaming; decoding as text would corrupt binary output).

Deliberately NOT ported (§5.7.4 "v1 bugs deliberately not pinned" + scope decisions flagged here
per CLAUDE.md's citation requirement):

- Every ``_get_kubeconfig(cluster_id)`` / ``"kubeconfig_not_found"`` branch. H18 is closed:
  ``kubeconfig: str`` is a field on every command (§5.3's kubernetes-plane header comment); the
  caller (``engine/provider_step.py``, bound from the cluster repository) always has one before
  calling ``execute()``. "kubeconfig not found" is the caller's impossibility now, not this
  module's string to invent.
- The per-call ``tempfile.NamedTemporaryFile(delete=False)`` + bare ``os.unlink()`` pattern,
  repeated ~13 times across both v1 files — replaced wholesale by
  ``seedpod/core/tempfiles.py``'s ``TempFileRegistry`` (0600 mode, one registry dir, startup
  sweep — H17).
- ``get_kubernetes_provider()``/``set_kubernetes_provider()`` module-level singleton (v1
  ``kubernetes.py``:1310-1326) — explicit construction only (§5.4's construction contract).
- The abstract ``KubernetesProvider`` base class's client-library-implementation seam (v1 never
  had a second implementation; the typed ``Provider`` Protocol in ``contract.py`` is the only
  seam v2 needs).
- Bare ``except Exception`` catch-and-stringify in every v1 method (e.g. ``return False, "", str(e)``)
  — every failure here is classified into one of the three taxonomy leaves at the edge
  (``_classify_failure`` below), never swallowed into an ad hoc string.

Genuinely NEW (§5.7.3, not salvage — needs real review, not silent invention):

- ``KubeDeleteManifest`` (v1 had no manifest inverse at all — this is ``KubeApplyManifest``'s
  literal ``undo_for`` target for infra shims, ``providers/compensation.py``).
- Row 30's typed ``PodDetailsResult(found=False)`` absence branch on ``KubeGetPodDetails`` — v1
  collapsed "NotFound" and every other kubectl failure into the same
  ``(False, None, "kubectl failed: ...")`` shape; the typed contract requires this crown-jewel-#1
  style split (absence is DATA) even though v1 never drew it for this particular read.
- ``check_ready()`` — v1's ``KubectlProvider`` had no startup preflight at all (fail at
  startup, not mid-provision — the same move every provider in this pillar makes).
- The kubectl-specific AUTH/INVALID_INPUT stderr pre-mapping (``_AUTH_STDERR_PHRASES``/
  ``_INVALID_INPUT_STDERR_PHRASES`` below) that makes decision rows 28/29 reachable at all: v1
  never distinguished these from a generic non-zero exit. This mirrors the *pattern*
  ``tart.py``'s own local "already exists"/"not found" pre-mapping already establishes (provider-
  specific stderr sniffing lives next to the provider that needs it; ``providers/classify.py``
  stays the one home for the *generic*, cross-provider connectivity/clean-non-zero-exit rule
  every provider's fallback delegates to) — not a new pattern this module invents.

Decision-table rows this module is responsible for (docs/design/seam-c-provider.md §5.1):

| # | Site | Symptom | Classification |
|---|---|---|---|
| 27 | kubectl, any | conn refused / i/o timeout / no route to apiserver | Unreachable / ``ENDPOINT_UNREACHABLE``, host=apiserver URL |
| 28 | kubectl, any | 401/403 (bad kubeconfig) | Permanent / ``AUTH`` |
| 29 | kubectl, apply | validation / immutable-field error | Permanent / ``INVALID_INPUT`` |
| 30 | kubectl, get | resource NotFound | **Result** ``found=False`` |
| 31 | kubectl, rollout | still progressing | **Result** ``RolloutState(complete=False)`` |

Row 27's ``host=`` is the apiserver URL parsed out of the command's own ``kubeconfig`` field
(``_apiserver_host`` below) — never a fixed provider-wide constant, since a ``kubectl`` command
can be talking to a different cluster on every single invocation (H18).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar

import yaml

from seedpod.core.clock import Clock, SystemClock
from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
)
from seedpod.core.tempfiles import TempFileRegistry
from seedpod.providers.classify import TRANSIENT_STDERR_PHRASES, classify_subprocess
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
    PodDetailsResult,
    Progress,
    ProviderCommand,
    ProviderEvent,
    Result,
    RolloutState,
    RolloutUndoResult,
    SubprocessResult,
    SubprocessRunner,
    WatchEnded,
)
from seedpod.providers.kube_types import (
    DeploymentInfo,
    EventInfo,
    NodeInfo,
    PodDetails,
    PodInfo,
    PodWatchEvent,
    format_age,
)

__all__ = ["KubectlConfig", "KubectlProvider"]

_UNKNOWN_HOST = "apiserver"  # kubeconfig unparseable / no clusters entry: best effort still needed

# Provider-local stderr pre-mapping (see module docstring's "Genuinely NEW" section for why this
# lives here rather than in providers/classify.py): rows 28/29's kubectl-specific symptoms are
# checked BEFORE falling back to the shared classify_subprocess connectivity/generic-failure
# split, exactly the way tart.py pre-maps its own "already exists"/"not found" symptoms locally.
_AUTH_STDERR_PHRASES = (
    "unauthorized",
    "forbidden",
    "must be logged in to the server",
    "the server has asked for the client to provide credentials",
)
_INVALID_INPUT_STDERR_PHRASES = (
    "error validating data",
    "is invalid:",
    "field is immutable",
    "cannot be changed",
    "unknown field",
    "error loading config file",
    "invalid configuration",
    "couldn't get current server api group list",
    "no configuration has been provided",
    "error parsing",
)
_NOT_FOUND_STDERR_PHRASES = ("notfound", "not found")


def _stderr_matches(stderr: str, phrases: tuple[str, ...]) -> bool:
    lowered = (stderr or "").lower()
    return any(phrase in lowered for phrase in phrases)


def _apiserver_host(kubeconfig_yaml: str) -> str:
    """Best-effort extraction of the current cluster's ``server`` URL, for the ``host=`` field
    on row 27's ``InfrastructureUnreachableError``. Never raises — a genuinely garbage
    kubeconfig still needs *some* ``host`` value on whatever error it produces, and rejecting
    garbage input is ``kubectl`` itself's job (rows 28/29), not this helper's."""
    try:
        doc = yaml.safe_load(kubeconfig_yaml)
        clusters = doc.get("clusters") if isinstance(doc, dict) else None
        if clusters:
            server = (clusters[0] or {}).get("cluster", {}).get("server")
            if server:
                return str(server)
    except Exception:
        pass
    return _UNKNOWN_HOST


@dataclass(frozen=True)
class KubectlConfig:
    """IO-free construction data (§5.4's construction contract). Most ``KubectlCommand``
    dataclasses carry no per-invocation timeout field (only ``KubeApplyManifest``/``KubeRun``/
    ``KubeWatchPods`` do); every other command's bounded-attempt timeout lives here instead,
    mirroring ``TartConfig``/``SshK3sConfig``'s per-command timeout fields.
    """

    check_ready_timeout_s: float = 10.0
    cluster_info_timeout_s: float = 15.0
    get_timeout_s: float = 15.0
    get_pod_details_timeout_s: float = 10.0
    logs_timeout_s: float = 20.0
    delete_timeout_s: float = 60.0
    restart_timeout_s: float = 30.0
    rollout_status_timeout_s: float = 15.0
    rollout_undo_timeout_s: float = 30.0
    events_timeout_s: float = 20.0

    # v1's readline-heartbeat poll interval (reference-code .../kubernetes.py:1151,
    # `min(remaining, 30.0)`) — preserved as DATA per Seam C §5.4 ("physics constants become
    # named parameters"), still consumed as a real per-line wait_for timeout inside
    # `_watch_pods` below (not a sleep loop this module owns retry through — it exists purely so
    # a quiet watch stream still notices `KubeWatchPods.timeout_s` expiring promptly).
    watch_heartbeat_s: float = 30.0


class KubectlProvider:
    name: ClassVar[str] = "kubectl"
    supported: ClassVar[frozenset[type]] = frozenset(
        {
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
    )

    def __init__(self, config: KubectlConfig, transport: SubprocessRunner, clock: Clock | None = None) -> None:
        """IO-free (§5.4's construction contract): stores config, the injected transport, and a
        ``Clock`` (``seedpod.core.clock`` — the project's time-injection convention, extended
        here per ``kube_types.py``'s own docstring, since ``format_age`` needs a real "now" and
        nothing in this module may call the wall clock directly). ``transport`` is a
        ``SubprocessRunner`` — conformance fault injection happens at that seam
        (``tests/conformance/``), never ``Mock``/``patch``."""
        self.config = config
        self.transport = transport
        self.clock = clock or SystemClock()
        self._tempfiles = TempFileRegistry()

    # ------------------------------------------------------------------
    # startup preflight
    # ------------------------------------------------------------------

    async def check_ready(self) -> None:
        """``kubectl`` binary on PATH — fail at startup, not mid-provision (genuinely new, v1
        had no such preflight; see module docstring). Deliberately does NOT attempt to reach any
        apiserver: this preflight runs once, with no kubeconfig in hand (H18 — every apiserver
        reachability check happens per-command, per-cluster, via row 27, not once globally at
        startup for a fleet of clusters this provider hasn't been told about yet)."""
        result = await self.transport.run(["kubectl", "version", "--client"], timeout=self.config.check_ready_timeout_s)
        if result.binary_missing:
            raise PermanentError(
                "kubectl.check_ready: `kubectl` binary not found on PATH",
                code=ErrorCode.NOT_FOUND,
                provider=self.name,
                command="check_ready",
            )

    # ------------------------------------------------------------------
    # Provider protocol
    # ------------------------------------------------------------------

    def execute(self, cmd: ProviderCommand) -> AsyncIterator[ProviderEvent]:
        """Unsupported ⇒ ``PermanentError(UNSUPPORTED)`` raised synchronously, before any
        backend traffic (§5.4) — not deferred to the first ``__anext__()``."""
        if type(cmd) not in self.supported:
            raise PermanentError(
                f"kubectl: unsupported command {type(cmd).__name__}",
                code=ErrorCode.UNSUPPORTED,
                provider=self.name,
                command=type(cmd).__name__,
            )
        if isinstance(cmd, KubeGetClusterInfo):
            return self._get_cluster_info(cmd)
        if isinstance(cmd, KubeGetNodes):
            return self._get_nodes(cmd)
        if isinstance(cmd, KubeGetPods):
            return self._get_pods(cmd)
        if isinstance(cmd, KubeGetPodDetails):
            return self._get_pod_details(cmd)
        if isinstance(cmd, KubeGetPodLogs):
            return self._get_pod_logs(cmd)
        if isinstance(cmd, KubeApplyManifest):
            return self._apply_manifest(cmd)
        if isinstance(cmd, KubeDeleteManifest):
            return self._delete_manifest(cmd)
        if isinstance(cmd, KubeGetDeployments):
            return self._get_deployments(cmd)
        if isinstance(cmd, KubeRestartDeployment):
            return self._restart_deployment(cmd)
        if isinstance(cmd, KubeProbeRollout):
            return self._probe_rollout(cmd)
        if isinstance(cmd, KubeGetEvents):
            return self._get_events(cmd)
        if isinstance(cmd, KubeRolloutUndo):
            return self._rollout_undo(cmd)
        if isinstance(cmd, KubeRun):
            return self._run(cmd)
        if isinstance(cmd, KubeWatchPods):
            return self._watch_pods(cmd)
        raise AssertionError(f"unreachable: {cmd!r} is in `supported` but has no handler")  # pragma: no cover

    # ------------------------------------------------------------------
    # commands — reads
    # ------------------------------------------------------------------

    async def _get_cluster_info(self, cmd: KubeGetClusterInfo) -> AsyncIterator[ProviderEvent]:
        result = await self._kubectl(
            cmd.kubeconfig, ["cluster-info", "--request-timeout=10s"],
            timeout=self.config.cluster_info_timeout_s, command_name="get_cluster_info",
        )
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="get_cluster_info")
        yield Result(result.stdout.decode(errors="replace"))

    async def _get_nodes(self, cmd: KubeGetNodes) -> AsyncIterator[ProviderEvent]:
        result = await self._kubectl(
            cmd.kubeconfig, ["get", "nodes", "-o", "json"], timeout=self.config.get_timeout_s, command_name="get_nodes"
        )
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="get_nodes")
        data = self._parse_json(result, kubeconfig=cmd.kubeconfig, command_name="get_nodes")
        now = self.clock.now()
        yield Result(tuple(_parse_node(item, now=now) for item in data.get("items", [])))

    async def _get_pods(self, cmd: KubeGetPods) -> AsyncIterator[ProviderEvent]:
        args = ["get", "pods", "-o", "json"]
        args += ["-n", cmd.namespace] if cmd.namespace else ["-A"]
        result = await self._kubectl(cmd.kubeconfig, args, timeout=self.config.get_timeout_s, command_name="get_pods")
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="get_pods")
        data = self._parse_json(result, kubeconfig=cmd.kubeconfig, command_name="get_pods")
        now = self.clock.now()
        yield Result(tuple(_parse_pod(item, now=now) for item in data.get("items", [])))

    async def _get_pod_details(self, cmd: KubeGetPodDetails) -> AsyncIterator[ProviderEvent]:
        result = await self._kubectl(
            cmd.kubeconfig, ["get", "pod", cmd.pod_name, "-n", cmd.namespace, "-o", "json"],
            timeout=self.config.get_pod_details_timeout_s, command_name="get_pod_details",
        )
        if result.returncode != 0 and not result.timed_out and not result.binary_missing:
            stderr_text = result.stderr.decode(errors="replace")
            if _stderr_matches(stderr_text, _NOT_FOUND_STDERR_PHRASES):
                # Row 30: authoritative absence, never conflated with "cannot reach the
                # apiserver" (crown jewel #1) — a typed Result, never raised.
                yield Result(PodDetailsResult(found=False, details=None))
                return
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="get_pod_details")
        item = self._parse_json(result, kubeconfig=cmd.kubeconfig, command_name="get_pod_details")
        yield Result(PodDetailsResult(found=True, details=_parse_pod_details(item, now=self.clock.now())))

    async def _get_pod_logs(self, cmd: KubeGetPodLogs) -> AsyncIterator[ProviderEvent]:
        args = ["logs", cmd.pod_name, "-n", cmd.namespace, f"--tail={cmd.tail_lines}"]
        if cmd.container:
            args += ["-c", cmd.container]
        if cmd.previous:
            args.append("--previous")
        result = await self._kubectl(cmd.kubeconfig, args, timeout=self.config.logs_timeout_s, command_name="get_pod_logs")
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="get_pod_logs")
        yield Result(result.stdout.decode(errors="replace"))

    async def _get_deployments(self, cmd: KubeGetDeployments) -> AsyncIterator[ProviderEvent]:
        yield Result(await self._fetch_deployments(cmd.kubeconfig, cmd.namespace))

    async def _get_events(self, cmd: KubeGetEvents) -> AsyncIterator[ProviderEvent]:
        args = ["get", "events", "-o", "json"]
        args += ["-n", cmd.namespace] if cmd.namespace else ["--all-namespaces"]
        result = await self._kubectl(cmd.kubeconfig, args, timeout=self.config.events_timeout_s, command_name="get_events")
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="get_events")
        data = self._parse_json(result, kubeconfig=cmd.kubeconfig, command_name="get_events")
        events = [_parse_event(item) for item in data.get("items", [])]
        # Salvaged verbatim: sort by last_timestamp descending (most recent first), then limit.
        events.sort(key=lambda e: e.last_timestamp or "", reverse=True)
        yield Result(tuple(events[: cmd.limit]))

    # ------------------------------------------------------------------
    # commands — manifests
    # ------------------------------------------------------------------

    async def _apply_manifest(self, cmd: KubeApplyManifest) -> AsyncIterator[ProviderEvent]:
        with self._tempfiles.files(cmd.kubeconfig, cmd.manifest_yaml, suffix=".yml") as (kubeconfig_path, manifest_path):
            env = {"KUBECONFIG": str(kubeconfig_path)}
            result = await self.transport.run(
                ["kubectl", "apply", "-f", str(manifest_path)], env=env, timeout=cmd.timeout_s
            )
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="apply_manifest")
        yield Result(result.stdout.decode(errors="replace"))

    async def _delete_manifest(self, cmd: KubeDeleteManifest) -> AsyncIterator[ProviderEvent]:
        """NEW command (v1 had no manifest inverse) — §5.7.3."""
        with self._tempfiles.files(cmd.kubeconfig, cmd.manifest_yaml, suffix=".yml") as (kubeconfig_path, manifest_path):
            env = {"KUBECONFIG": str(kubeconfig_path)}
            argv = ["kubectl", "delete", "-f", str(manifest_path)]
            if cmd.ignore_not_found:
                argv.append("--ignore-not-found")
            result = await self.transport.run(argv, env=env, timeout=self.config.delete_timeout_s)
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="delete_manifest")
        yield Result(result.stdout.decode(errors="replace"))

    # ------------------------------------------------------------------
    # commands — deployments / rollouts
    # ------------------------------------------------------------------

    async def _restart_deployment(self, cmd: KubeRestartDeployment) -> AsyncIterator[ProviderEvent]:
        result = await self._kubectl(
            cmd.kubeconfig, ["rollout", "restart", f"deployment/{cmd.deployment}", "-n", cmd.namespace],
            timeout=self.config.restart_timeout_s, command_name="restart_deployment",
        )
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="restart_deployment")
        yield Result(result.stdout.decode(errors="replace"))

    async def _probe_rollout(self, cmd: KubeProbeRollout) -> AsyncIterator[ProviderEvent]:
        """``kubectl rollout status --watch=false`` — single-shot; the engine's own
        wait-for-readiness gate is what polls this (Seam C taste call 2). Row 31: still
        progressing is a typed Result, never an error.

        Real ``kubectl rollout status --watch=false`` (all currently-supported kubectl
        versions) exits NON-ZERO both when the rollout is still progressing AND on a genuine
        failure (deployment not found, connectivity loss, auth) — only a *completed* rollout
        exits 0. So the still-progressing case cannot be detected by ``returncode == 0``; it
        must be recognized from ``stdout`` BEFORE falling through to ``_classify_failure``,
        else every real still-progressing probe would raise and abort the run (masking this
        row's whole point). client-go's polymorphichelpers always prefixes the not-yet-done
        message with ``"Waiting for deployment"`` on stdout with an empty stderr; anything else
        (a connectivity/auth symptom, "not found", timeout, missing binary) still raises via
        the normal classification path.
        """
        result = await self._kubectl(
            cmd.kubeconfig, ["rollout", "status", f"deployment/{cmd.deployment}", "-n", cmd.namespace, "--watch=false"],
            timeout=self.config.rollout_status_timeout_s, command_name="probe_rollout",
        )
        if result.timed_out or result.binary_missing:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="probe_rollout")
        message = result.stdout.decode(errors="replace").strip()
        stderr_text = result.stderr.decode(errors="replace").strip()
        if result.returncode != 0:
            if not stderr_text and message.startswith("Waiting for deployment"):
                yield Result(RolloutState(complete=False, message=message))
                return
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="probe_rollout")
        complete = "successfully rolled out" in message
        yield Result(RolloutState(complete=complete, message=message))

    async def _rollout_undo(self, cmd: KubeRolloutUndo) -> AsyncIterator[ProviderEvent]:
        """Crown jewel #13, partial-success semantics preserved EXACTLY: undo every deployment
        in the namespace, success iff ``>=1`` undo succeeded, else raise ``PermanentError``
        carrying the aggregated ``errors`` (``RolloutUndoResult``'s own docstring,
        ``contract.py``). v1's own "no deployments ⇒ trivial success" special case (reference-
        code .../kubernetes.py:965-966) is kept exactly — an empty namespace is not a failure to
        undo anything in."""
        deployments = await self._fetch_deployments(cmd.kubeconfig, cmd.namespace)
        if not deployments:
            yield Result(RolloutUndoResult(succeeded=0, failed=0, outputs="No deployments to undo", errors=""))
            return

        outputs: list[str] = []
        errors: list[str] = []
        succeeded = 0
        failed = 0
        for deployment in deployments:
            result = await self._kubectl(
                cmd.kubeconfig, ["rollout", "undo", f"deployment/{deployment.name}", "-n", cmd.namespace],
                timeout=self.config.rollout_undo_timeout_s, command_name="rollout_undo",
            )
            stderr_text = result.stderr.decode(errors="replace")
            if result.timed_out or result.binary_missing or _stderr_matches(stderr_text, TRANSIENT_STDERR_PHRASES):
                # A genuine "cannot determine state" symptom mid-loop is never silently folded
                # into the per-deployment failure tally (crown jewel #1, extended to this loop —
                # a real improvement over v1, which had no such distinction here at all).
                raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="rollout_undo")
            if result.returncode == 0:
                succeeded += 1
                outputs.append(f"{deployment.name}: {result.stdout.decode(errors='replace').strip()}")
            else:
                failed += 1
                errors.append(f"{deployment.name}: {stderr_text.strip()}")

        combined_outputs = "\n".join(outputs)
        combined_errors = "\n".join(errors)
        if succeeded == 0:
            raise PermanentError(
                f"kubectl.rollout_undo: all {failed} deployment undo(s) failed in namespace {cmd.namespace!r}",
                code=ErrorCode.SCRIPT_FAILED,
                provider=self.name,
                command="rollout_undo",
                detail={"errors": combined_errors},
            )
        yield Result(RolloutUndoResult(succeeded=succeeded, failed=failed, outputs=combined_outputs, errors=combined_errors))

    async def _fetch_deployments(self, kubeconfig: str, namespace: str) -> tuple[DeploymentInfo, ...]:
        result = await self._kubectl(
            kubeconfig, ["get", "deployments", "-n", namespace, "-o", "json"],
            timeout=self.config.get_timeout_s, command_name="get_deployments",
        )
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=kubeconfig, result=result, command_name="get_deployments")
        data = self._parse_json(result, kubeconfig=kubeconfig, command_name="get_deployments")
        return tuple(_parse_deployment(item) for item in data.get("items", []))

    # ------------------------------------------------------------------
    # commands — escape hatch + watch
    # ------------------------------------------------------------------

    async def _run(self, cmd: KubeRun) -> AsyncIterator[ProviderEvent]:
        """Crown jewel #14: ``binary=True`` returns ``stdout`` undecoded, required for
        ``pg_dump -Fc`` snapshot streaming — decoding as text would corrupt binary output.

        ``cmd.stdin`` is the input counterpart, added for snapshot restore: the
        transport has always accepted ``stdin`` (``SubprocessRunner.run``), and this
        method simply never passed it, so ``pg_restore`` was exec'd against an empty
        stdin. Forwarded verbatim — no encoding, since a ``-Fc`` dump is binary."""
        with self._tempfiles.file(cmd.kubeconfig, suffix=".yml") as kubeconfig_path:
            env = {"KUBECONFIG": str(kubeconfig_path)}
            result = await self.transport.run(
                ["kubectl", *cmd.args], stdin=cmd.stdin, env=env, timeout=cmd.timeout_s
            )
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify_failure(kubeconfig=cmd.kubeconfig, result=result, command_name="run")
        stdout: str | bytes = result.stdout if cmd.binary else result.stdout.decode(errors="replace")
        yield Result(KubectlOutput(stdout=stdout, stderr=result.stderr.decode(errors="replace")))

    async def _watch_pods(self, cmd: KubeWatchPods) -> AsyncIterator[ProviderEvent]:
        """The one natively streaming command. ``CancelledError`` is never caught here — it
        propagates through the ``async with`` unwind on its own, into the injected transport's
        ``stream()`` context manager, whose ``__aexit__`` owns the terminate→kill escalation +
        final stderr harvest (``contract.py``'s ``SubprocessRunner.stream`` docstring).

        Cancellation-safety note for whoever implements the real ``SubprocessRunner``: the
        heartbeat loop below repeatedly wraps ``lines.__anext__()`` in ``asyncio.wait_for`` and
        lets it time out on purpose (that's the heartbeat). This is only safe if the returned
        ``AsyncIterator[bytes]``'s ``__anext__`` is an ordinary method backed by persistent
        state (exactly what ``asyncio.StreamReader`` — ``process.stdout`` — already is: each
        call is a fresh, independent ``readline()`` against a buffer that outlives any single
        cancelled read). It is **not** safe if ``__anext__`` is implemented as a plain async
        *generator*: cancelling a suspended generator frame permanently closes it, so every
        later call would raise ``StopAsyncIteration`` instead of resuming — a real bug this
        module's own fake transport (``tests/conformance/fake_kubectl.py``) hit and fixed by
        using a class-based iterator over a decoupled producer/``asyncio.Queue`` instead of a
        generator; see that module's ``stream()`` for the full explanation."""
        argv = ["kubectl", "get", "pods", "-w", "--output-watch-events", "-o", "json", "-n", cmd.namespace]
        loop = asyncio.get_event_loop()
        deadline = loop.time() + cmd.timeout_s
        reason = "stream_ended"
        with self._tempfiles.file(cmd.kubeconfig, suffix=".yml") as kubeconfig_path:
            env = {"KUBECONFIG": str(kubeconfig_path)}
            async with self.transport.stream(argv, env=env) as lines:
                while True:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        reason = "timeout"
                        break
                    try:
                        # 30s readline heartbeat (v1:1146-1155, salvaged): a quiet stream still
                        # notices the overall deadline promptly instead of blocking forever.
                        line = await asyncio.wait_for(lines.__anext__(), timeout=min(remaining, self.config.watch_heartbeat_s))
                    except TimeoutError:
                        continue
                    except StopAsyncIteration:
                        reason = "stream_ended"
                        break
                    line_str = line.decode(errors="replace").strip()
                    if not line_str:
                        continue
                    try:
                        data = json.loads(line_str)
                    except json.JSONDecodeError:
                        continue  # v1:1173-1178, salvaged: skip non-JSON lines
                    if not isinstance(data, dict):
                        continue  # v1:1180-1183, salvaged: skip non-dict JSON
                    event = _parse_watch_event(data, default_namespace=cmd.namespace)
                    if event is None:
                        continue  # v1:1198-1201, salvaged: skip events with no pod name
                    yield Progress(phase="pods.watch", data={"event": event})
        yield Result(WatchEnded(reason=reason))

    # ------------------------------------------------------------------
    # internals — subprocess plumbing + classification
    # ------------------------------------------------------------------

    async def _kubectl(self, kubeconfig: str, args: list[str], *, timeout: float, command_name: str) -> SubprocessResult:
        """One bounded ``kubectl`` invocation via the injected transport (no internal retry —
        H4-H6, the engine's Schedule owns retry). Writes ``kubeconfig`` to a 0600 registered
        temp file, unlinked in ``finally`` — closes H18 (kubeconfig always a command field) and
        H17 (deterministic cleanup on every exit path, incl. cancellation)."""
        with self._tempfiles.file(kubeconfig, suffix=".yml") as kubeconfig_path:
            env = {"KUBECONFIG": str(kubeconfig_path)}
            return await self.transport.run(["kubectl", *args], env=env, timeout=timeout)

    def _classify_failure(self, *, kubeconfig: str, result: SubprocessResult, command_name: str) -> ProviderError:
        """Rows 27-29 + the generic ``SCRIPT_FAILED`` fallback. Pre-maps kubectl-specific
        AUTH/INVALID_INPUT stderr symptoms (see module docstring's "Genuinely NEW" section)
        ahead of the shared ``classify_subprocess`` connectivity/clean-non-zero-exit split,
        which every other symptom — including every connectivity phrase — still falls through
        to unchanged."""
        stderr_text = result.stderr.decode(errors="replace")
        host = _apiserver_host(kubeconfig)
        if not result.timed_out and not result.binary_missing:
            if _stderr_matches(stderr_text, _AUTH_STDERR_PHRASES):
                return PermanentError(
                    f"kubectl.{command_name}: authentication failed against {host}",
                    code=ErrorCode.AUTH, provider=self.name, command=command_name, detail={"stderr": stderr_text},
                )
            if _stderr_matches(stderr_text, _INVALID_INPUT_STDERR_PHRASES):
                return PermanentError(
                    f"kubectl.{command_name}: invalid input",
                    code=ErrorCode.INVALID_INPUT, provider=self.name, command=command_name, detail={"stderr": stderr_text},
                )
        return classify_subprocess(
            provider=self.name,
            command=command_name,
            host=host,
            rc=result.returncode,
            stderr=stderr_text,
            timed_out=result.timed_out,
            binary_missing=result.binary_missing,
            observing_infra=True,  # kubectl talks to the cluster's control plane (row 27)
        )

    def _parse_json(self, result: SubprocessResult, *, kubeconfig: str, command_name: str) -> dict[str, Any]:
        """Garbage body from a machine-provider-style state read ⇒ Unreachable (mirrors
        tart.py's ``_list_vms`` / DO's ``classify_http(malformed_body=True)`` — "treat like
        timeout"), never a crash, never a silently-empty result."""
        try:
            return json.loads(result.stdout.decode(errors="replace"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise InfrastructureUnreachableError(
                f"kubectl.{command_name}: kubectl returned non-JSON output",
                code=ErrorCode.MALFORMED_RESPONSE,
                provider=self.name,
                command=command_name,
                detail={"stdout_prefix": result.stdout[:200].decode(errors="replace")},
                host=_apiserver_host(kubeconfig),
            ) from e


# ============================================================================
# internals — pure DTO parsing (salvaged field mappings, module-level so they stay trivially
# unit-testable without a provider instance)
# ============================================================================


def _parse_node(item: Mapping[str, Any], *, now: datetime) -> NodeInfo:
    """Salvaged verbatim from ``get_nodes`` (reference-code .../kubernetes.py:435-469)."""
    metadata = item.get("metadata", {}) or {}
    status = item.get("status", {}) or {}

    node_status = "Unknown"
    for condition in status.get("conditions", []) or []:
        if condition.get("type") == "Ready":
            node_status = "Ready" if condition.get("status") == "True" else "NotReady"
            break

    labels = metadata.get("labels", {}) or {}
    roles = [
        key.removeprefix("node-role.kubernetes.io/")
        for key in labels
        if key.startswith("node-role.kubernetes.io/") and key.removeprefix("node-role.kubernetes.io/")
    ]
    roles_str = ",".join(roles) if roles else "<none>"

    created = metadata.get("creationTimestamp", "")
    version = (status.get("nodeInfo", {}) or {}).get("kubeletVersion", "")

    return NodeInfo(
        name=metadata.get("name", ""), status=node_status, roles=roles_str,
        age=format_age(created, now=now), version=version,
    )


def _parse_pod(item: Mapping[str, Any], *, now: datetime) -> PodInfo:
    """Salvaged verbatim from ``get_pods`` (reference-code .../kubernetes.py:523-562)."""
    metadata = item.get("metadata", {}) or {}
    spec = item.get("spec", {}) or {}
    status = item.get("status", {}) or {}

    container_statuses = status.get("containerStatuses") or []
    total_containers = len(spec.get("containers", []) or [])
    restarts = sum(cs.get("restartCount", 0) for cs in container_statuses)
    ready_count = sum(1 for cs in container_statuses if cs.get("ready", False))
    ready_str = f"{ready_count}/{total_containers}"

    created = metadata.get("creationTimestamp", "")
    phase = status.get("phase", "Unknown")

    containers = spec.get("containers") or []
    image = containers[0].get("image", "") if containers else ""

    return PodInfo(
        name=metadata.get("name", ""), namespace=metadata.get("namespace", ""), status=phase,
        ready=ready_str, restarts=restarts, age=format_age(created, now=now), created=created,
        node=spec.get("nodeName", ""), ip=status.get("podIP", ""), image=image,
    )


def _parse_pod_details(item: Mapping[str, Any], *, now: datetime) -> PodDetails:
    """Salvaged verbatim from ``get_pod_details`` (reference-code .../kubernetes.py:606-690)."""
    metadata = item.get("metadata", {}) or {}
    spec = item.get("spec", {}) or {}
    status = item.get("status", {}) or {}

    created = metadata.get("creationTimestamp", "")
    phase = status.get("phase", "Unknown")

    conditions = [
        {
            "type": c.get("type", ""), "status": c.get("status", ""), "reason": c.get("reason", ""),
            "message": c.get("message", ""), "lastTransitionTime": c.get("lastTransitionTime", ""),
        }
        for c in status.get("conditions", []) or []
    ]

    def _containers(spec_key: str, status_key: str) -> list[dict[str, Any]]:
        statuses_by_name = {cs.get("name"): cs for cs in status.get(status_key, []) or []}
        out = []
        for c in spec.get(spec_key, []) or []:
            container_name = c.get("name", "")
            cs = statuses_by_name.get(container_name)
            out.append(
                {
                    "name": container_name, "image": c.get("image", ""),
                    "ready": cs.get("ready", False) if cs else False,
                    "restarts": cs.get("restartCount", 0) if cs else 0,
                    "state": cs.get("state", {}) if cs else {},
                    "ports": c.get("ports", []),
                    "env": [{"name": e.get("name", ""), "value": e.get("value", "")} for e in c.get("env", []) or []],
                }
            )
        return out

    return PodDetails(
        name=metadata.get("name", ""), namespace=metadata.get("namespace", ""), status=phase,
        age=format_age(created, now=now), created=created, node=spec.get("nodeName", ""),
        ip=status.get("podIP", ""), host_ip=status.get("hostIP", ""),
        labels=metadata.get("labels", {}) or {}, annotations=metadata.get("annotations", {}) or {},
        conditions=conditions,
        init_containers=_containers("initContainers", "initContainerStatuses"),
        containers=_containers("containers", "containerStatuses"),
        volumes=spec.get("volumes", []) or [],
    )


def _parse_deployment(item: Mapping[str, Any]) -> DeploymentInfo:
    """Salvaged verbatim from ``get_deployments`` (reference-code .../kubernetes.py:831-843)."""
    metadata = item.get("metadata", {}) or {}
    spec = item.get("spec", {}) or {}
    status = item.get("status", {}) or {}
    return DeploymentInfo(
        name=metadata.get("name", ""), namespace=metadata.get("namespace", ""),
        ready_replicas=status.get("readyReplicas", 0), desired_replicas=spec.get("replicas", 0),
        available_replicas=status.get("availableReplicas", 0), updated_replicas=status.get("updatedReplicas", 0),
    )


def _parse_event(item: Mapping[str, Any]) -> EventInfo:
    """Salvaged verbatim from ``get_events`` (reference-code .../kubernetes.py:1048-1069)."""
    metadata = item.get("metadata", {}) or {}
    involved_object = item.get("involvedObject", {}) or {}
    source = item.get("source", {}) or {}
    first_ts = item.get("firstTimestamp") or item.get("eventTime") or ""
    last_ts = item.get("lastTimestamp") or item.get("eventTime") or ""
    return EventInfo(
        namespace=metadata.get("namespace", ""), name=metadata.get("name", ""), type=item.get("type", "Normal"),
        reason=item.get("reason", ""), message=item.get("message", ""),
        involved_object_kind=involved_object.get("kind", ""), involved_object_name=involved_object.get("name", ""),
        count=item.get("count", 1), first_timestamp=first_ts, last_timestamp=last_ts,
        source_component=source.get("component", ""),
    )


def _parse_watch_event(data: Mapping[str, Any], *, default_namespace: str) -> PodWatchEvent | None:
    """Salvaged verbatim from ``watch_pods`` (reference-code .../kubernetes.py:1185-1236)."""
    event_type = data.get("type", "MODIFIED")
    pod_data = data.get("object", data)
    if not isinstance(pod_data, dict):
        return None  # v1:1189-1190, salvaged

    metadata = pod_data.get("metadata", {}) or {}
    pod_name = metadata.get("name", "")
    if not pod_name:
        return None  # v1:1198-1201, salvaged

    status = pod_data.get("status", {}) or {}
    spec = pod_data.get("spec", {}) or {}
    namespace = metadata.get("namespace") or default_namespace
    phase = status.get("phase", "Unknown")

    container_statuses = status.get("containerStatuses", []) or []
    ready_count = sum(1 for c in container_statuses if c.get("ready", False))
    total_count = len(spec.get("containers", []) or [])
    ready_str = f"{ready_count}/{total_count}"

    conditions = status.get("conditions", []) or []

    message = status.get("message")
    if not message:
        for cs in container_statuses:
            waiting = (cs.get("state", {}) or {}).get("waiting", {}) or {}
            if waiting.get("reason"):
                message = f"{cs.get('name')}: {waiting.get('reason')} - {waiting.get('message', '')}"
                break

    return PodWatchEvent(
        event_type=str(event_type), pod_name=pod_name, namespace=namespace, phase=phase, ready=ready_str,
        conditions=conditions, containers=container_statuses, message=message,
    )
