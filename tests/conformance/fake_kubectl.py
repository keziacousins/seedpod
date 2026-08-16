"""tests/conformance/fake_kubectl.py — a typed FAKE TRANSPORT simulating enough of a ``kubectl``
CLI + a target cluster's API server (nodes, pods, deployments, events, applied manifests, a pod
watch stream) for ``seedpod.providers.kubectl.KubectlProvider`` conformance testing (Seam C
§5.6).

``FakeKubectlBackend`` is the in-memory "cluster": plain mutable stores of nodes/pods/
deployments/events/applied-manifest objects, plus a queue of raw watch-stream lines (including
deliberately-malformed ones, to exercise the provider's own JSON/non-dict skip hardening).
``FakeKubectlTransport`` implements the ``SubprocessRunner`` protocol
(``seedpod.providers.contract.SubprocessRunner``) — installed directly as the provider's
``transport`` — so fault injection happens at the actual transport seam the provider talks to,
never ``Mock``/``patch`` (CLAUDE.md).

Routing mirrors how ``kubectl.py`` actually invokes the CLI: ``argv[0] == "kubectl"`` dispatches
on ``argv[1]`` (``version`` / ``cluster-info`` / ``get`` / ``logs`` / ``apply`` / ``delete`` /
``rollout`` / arbitrary ``KubeRun`` args); ``KubeWatchPods`` goes through ``stream()`` instead of
``run()``, exactly as the real ``kubectl get pods -w --output-watch-events`` invocation would.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import yaml

from seedpod.core.errors import ErrorCode, InfrastructureUnreachableError
from seedpod.providers.contract import SubprocessResult
from tests.conformance.harness import Fault

__all__ = ["FakeKubectlBackend", "FakeKubectlTransport"]

_APISERVER = "https://10.96.0.1:6443"

# A manifest containing this marker is rejected by `apply` with a validation-style error,
# exercising row 29 (Permanent/INVALID_INPUT) deterministically without needing a real
# schema validator in the fake.
INVALID_MANIFEST_MARKER = "# seedpod-conformance: TRIGGER_VALIDATION_ERROR"


def _default_nodes() -> list[dict]:
    return [
        {
            "metadata": {"name": "node-1", "creationTimestamp": "2026-01-01T00:00:00Z", "labels": {"node-role.kubernetes.io/control-plane": ""}},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "nodeInfo": {"kubeletVersion": "v1.29.0+k3s1"},
            },
        }
    ]


def _default_pods(namespace: str = "default") -> dict[tuple[str, str], dict]:
    key = (namespace, "web-abc123")
    return {
        key: {
            "metadata": {
                "name": "web-abc123", "namespace": namespace, "creationTimestamp": "2026-01-01T00:00:00Z",
                "labels": {"app": "web"}, "annotations": {},
            },
            "spec": {"nodeName": "node-1", "containers": [{"name": "web", "image": "web:1.0", "ports": [], "env": []}]},
            "status": {
                "phase": "Running", "podIP": "10.42.0.7", "hostIP": "10.0.0.1",
                "containerStatuses": [{"name": "web", "ready": True, "restartCount": 0, "state": {"running": {}}}],
                "conditions": [{"type": "Ready", "status": "True"}],
            },
        }
    }


def _default_deployments(namespace: str = "default") -> dict[tuple[str, str], dict]:
    key = (namespace, "web")
    return {
        key: {
            "metadata": {"name": "web", "namespace": namespace},
            "spec": {"replicas": 1},
            "status": {"readyReplicas": 1, "availableReplicas": 1, "updatedReplicas": 1},
        }
    }


def _default_events(namespace: str = "default") -> list[dict]:
    return [
        {
            "metadata": {"namespace": namespace, "name": "web-abc123.17"},
            "type": "Normal", "reason": "Scheduled", "message": "Successfully assigned",
            "involvedObject": {"kind": "Pod", "name": "web-abc123"},
            "count": 1, "firstTimestamp": "2026-01-01T00:00:00Z", "lastTimestamp": "2026-01-01T00:00:01Z",
            "source": {"component": "default-scheduler"},
        },
        {
            "metadata": {"namespace": namespace, "name": "web-abc123.18"},
            "type": "Normal", "reason": "Started", "message": "Started container web",
            "involvedObject": {"kind": "Pod", "name": "web-abc123"},
            "count": 1, "firstTimestamp": "2026-01-01T00:00:02Z", "lastTimestamp": "2026-01-01T00:00:02Z",
            "source": {"component": "kubelet"},
        },
    ]


def _manifest_object_key(doc: Mapping[str, object]) -> tuple[str, str, str] | None:
    kind = doc.get("kind")
    metadata = doc.get("metadata") if isinstance(doc.get("metadata"), Mapping) else {}
    name = metadata.get("name") if isinstance(metadata, Mapping) else None
    namespace = (metadata.get("namespace") if isinstance(metadata, Mapping) else None) or "default"
    if not kind or not name:
        return None
    return (str(kind), str(namespace), str(name))


def _names_or_json(args: Sequence[str], names: list[str], items: list[dict]) -> SubprocessResult:
    """`-o jsonpath={.items[*].metadata.name}` -> space-separated names (the shape v1's
    own service sweep parsed); anything else -> the usual JSON items envelope."""
    output = _flag(args, "-o") or ""
    if "jsonpath" in output and "metadata.name" in output:
        return SubprocessResult(returncode=0, stdout=" ".join(names).encode(), stderr=b"")
    return _json_ok({"items": items})


@dataclass
class FakeKubectlBackend:
    """The in-memory "cluster". Every field defaults to a small, healthy, already-populated
    cluster so a harness only has to override what a given test cares about."""

    nodes: list[dict] = field(default_factory=_default_nodes)
    pods: dict[tuple[str, str], dict] = field(default_factory=_default_pods)
    deployments: dict[tuple[str, str], dict] = field(default_factory=_default_deployments)
    events: list[dict] = field(default_factory=_default_events)

    # object-key -> raw manifest text, for KubeApplyManifest/KubeDeleteManifest leak checks.
    applied_manifests: dict[tuple[str, str, str], str] = field(default_factory=dict)

    # Round 8b (destroy path): `kube.delete_daemonset` and `kube.wipe_namespace`.
    # `services` is seeded with the built-in `kubernetes` service every real cluster
    # has -- v1 deleted services one-by-one BY NAME precisely to preserve it, and a
    # wipe that removed it would break the cluster it is supposed to leave standing.
    daemonsets: dict[tuple[str, str], dict] = field(default_factory=dict)
    services: dict[tuple[str, str], dict] = field(default_factory=lambda: {("default", "kubernetes"): {}})

    # Round 10 (deploy path): `deploy.prepare_wave` deletes named Jobs
    # (`kubectl delete job NAME --ignore-not-found=true`) before a wave's own
    # re-apply -- a Job is immutable, so it must be gone first. No `_default_*`
    # seed (unlike pods/deployments/nodes): a test only cares about the specific
    # job names it names in `DeleteJobsParams.jobs`.
    jobs: dict[tuple[str, str], dict] = field(default_factory=dict)

    # Round 10 (deploy path, apply-and-wait fix pass): when set, `kubectl get jobs -o json`
    # returns this raw stdout VERBATIM instead of the usual `{"items": [...]}` envelope built
    # from `jobs` above -- lets a test simulate a malformed-but-200 response (non-JSON text, or
    # valid JSON that isn't a List-kind object) at the actual transport seam, matching how
    # `watch_lines` below lets a test queue deliberately-malformed stream frames.
    get_jobs_raw_stdout_override: bytes | None = None

    # rollout undo bookkeeping: names of deployments whose undo should fail this call.
    rollout_undo_failures: frozenset[str] = frozenset()

    # queued watch-stream lines (bytes or str; malformed entries included by callers on demand
    # to exercise the provider's own skip-non-JSON/skip-non-dict hardening).
    watch_lines: list[bytes | str] = field(default_factory=list)
    watch_line_delay_s: float = 0.0  # per-line artificial delay, for heartbeat-timing tests

    apiserver_url: str = _APISERVER
    call_log: list[tuple[str, ...]] = field(default_factory=list)
    attempt_count: int = 0

    def present_manifest_keys(self) -> frozenset[str]:
        """``backend_resources()``'s raw truth: currently-applied object identities, ``kind/ns/name``."""
        return frozenset("/".join(key) for key in self.applied_manifests)


class FakeKubectlTransport:
    """Implements ``seedpod.providers.contract.SubprocessRunner`` against a
    ``FakeKubectlBackend``."""

    def __init__(self, backend: FakeKubectlBackend, faults: frozenset[Fault]) -> None:
        self.backend = backend
        self.faults = faults
        self._transient_once_consumed = False

    # ------------------------------------------------------------------
    # bounded request/response
    # ------------------------------------------------------------------

    async def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        cluster_id: str | None = None,
    ) -> SubprocessResult:
        self.backend.attempt_count += 1
        self.backend.call_log.append(tuple(argv))
        binary = argv[0]

        if Fault.MISSING_SOURCE in self.faults:
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"", binary_missing=True)

        if Fault.UNREACHABLE in self.faults:
            return SubprocessResult(
                returncode=1, stdout=b"",
                stderr=f"Unable to connect to the server: dial tcp {self.backend.apiserver_url}: connection refused".encode(),
            )

        if Fault.TRANSIENT_ONCE in self.faults and not self._transient_once_consumed:
            self._transient_once_consumed = True
            return SubprocessResult(returncode=1, stdout=b"", stderr=b"Unable to connect to the server: net/http: i/o timeout")

        if Fault.AUTH in self.faults:
            return SubprocessResult(returncode=1, stdout=b"", stderr=b"error: You must be logged in to the server (Unauthorized)")

        if binary != "kubectl":
            return SubprocessResult(returncode=127, stdout=b"", stderr=f"command not found: {binary}".encode())

        sub = argv[1] if len(argv) > 1 else ""
        if sub != "version":
            # Mirrors real kubectl: every subcommand except `version --client` loads the
            # KUBECONFIG file first and fails fast on garbage content — exercising rows 28/29's
            # "no kubeconfig_not_found string anywhere" requirement (C-18) with a genuinely
            # invalid kubeconfig, not just an unreachable/auth symptom.
            invalid = self._validate_kubeconfig(env)
            if invalid is not None:
                return invalid

        return self._handle_kubectl(argv[1:])

    def _validate_kubeconfig(self, env: Mapping[str, str] | None) -> SubprocessResult | None:
        path = (env or {}).get("KUBECONFIG")
        if not path:
            return None
        try:
            with open(path) as f:
                doc = yaml.safe_load(f.read())
        except Exception:
            return SubprocessResult(returncode=1, stdout=b"", stderr=b"error: error loading config file: yaml: could not parse")
        if not isinstance(doc, dict) or "clusters" not in doc:
            return SubprocessResult(
                returncode=1, stdout=b"", stderr=b"error: invalid configuration: no configuration has been provided"
            )
        return None

    def _handle_kubectl(self, args: Sequence[str]) -> SubprocessResult:
        if not args:
            return SubprocessResult(returncode=1, stdout=b"", stderr=b"kubectl: missing command")

        sub = args[0]

        if sub == "version":
            return SubprocessResult(returncode=0, stdout=b"Client Version: v1.29.0\n", stderr=b"")

        if sub == "cluster-info":
            return SubprocessResult(
                returncode=0, stdout=f"Kubernetes control plane is running at {self.backend.apiserver_url}\n".encode(), stderr=b""
            )

        if sub == "get":
            return self._handle_get(args[1:])

        if sub == "logs":
            return self._handle_logs(args[1:])

        if sub == "apply":
            return self._handle_apply(args[1:])

        if sub == "delete":
            return self._handle_delete(args[1:])

        if sub == "rollout":
            return self._handle_rollout(args[1:])

        if sub == "exec":
            # Crown jewel #14's KubeRun exercise surface — arbitrary passthrough command,
            # simulating e.g. `kubectl exec pod -- pg_dump -Fc ...`.
            return SubprocessResult(returncode=0, stdout=b"\x50\x47\x44\x4d\x50fake-binary-dump", stderr=b"")

        return SubprocessResult(returncode=1, stdout=b"", stderr=f"error: unknown command {sub!r}".encode())

    def _handle_get(self, args: Sequence[str]) -> SubprocessResult:
        kind = args[0] if args else ""
        namespace = _flag(args, "-n") or "default"
        all_namespaces = "-A" in args or "--all-namespaces" in args

        if kind == "nodes":
            return _json_ok({"items": self.backend.nodes})

        if kind == "pod":
            # `get pod NAME -n ns` — single-resource read (row 30's NotFound surface).
            name = args[1] if len(args) > 1 else ""
            item = self.backend.pods.get((namespace, name))
            if item is None:
                return SubprocessResult(
                    returncode=1, stdout=b"", stderr=f'Error from server (NotFound): pods "{name}" not found\n'.encode()
                )
            return _json_ok(item)

        if kind == "pods":
            items = [p for (ns, _name), p in self.backend.pods.items() if all_namespaces or ns == namespace]
            return _json_ok({"items": items})

        if kind == "deployments":
            items = [d for (ns, _name), d in self.backend.deployments.items() if ns == namespace]
            return _json_ok({"items": items})

        if kind in ("daemonsets", "daemonset"):
            names = sorted(n for (ns, n) in self.backend.daemonsets if ns == namespace)
            return _names_or_json(args, names, [self.backend.daemonsets[(namespace, n)] for n in names])

        if kind in ("services", "service", "svc"):
            names = sorted(n for (ns, n) in self.backend.services if ns == namespace)
            return _names_or_json(args, names, [self.backend.services[(namespace, n)] for n in names])

        if kind in ("jobs", "job"):
            if self.backend.get_jobs_raw_stdout_override is not None:
                return SubprocessResult(returncode=0, stdout=self.backend.get_jobs_raw_stdout_override, stderr=b"")
            names = sorted(n for (ns, n) in self.backend.jobs if ns == namespace)
            return _names_or_json(args, names, [self.backend.jobs[(namespace, n)] for n in names])

        if kind == "events":
            items = [e for e in self.backend.events if all_namespaces or e["metadata"]["namespace"] == namespace]
            return _json_ok({"items": items})

        return SubprocessResult(returncode=1, stdout=b"", stderr=f"error: unknown resource {kind!r}".encode())

    def _handle_logs(self, args: Sequence[str]) -> SubprocessResult:
        pod_name = args[0] if args else ""
        namespace = _flag(args, "-n") or "default"
        if (namespace, pod_name) not in self.backend.pods:
            return SubprocessResult(returncode=1, stdout=b"", stderr=f'Error from server (NotFound): pods "{pod_name}" not found\n'.encode())
        return SubprocessResult(returncode=0, stdout=f"log line 1 from {pod_name}\nlog line 2\n".encode(), stderr=b"")

    def _handle_apply(self, args: Sequence[str]) -> SubprocessResult:
        manifest_path = _flag(args, "-f")
        with open(manifest_path) as f:  # the real `kubectl` binary reads this same temp file
            manifest_text = f.read()

        if INVALID_MANIFEST_MARKER in manifest_text:
            return SubprocessResult(
                returncode=1, stdout=b"",
                stderr=b"error: error validating data: ValidationError(Deployment.spec): unknown field \"bogus\"",
            )

        applied = []
        for doc in yaml.safe_load_all(manifest_text):
            if not isinstance(doc, dict):
                continue
            key = _manifest_object_key(doc)
            if key is None:
                continue
            self.backend.applied_manifests[key] = manifest_text
            applied.append(f"{key[0].lower()}.apps/{key[2]} configured")
        return SubprocessResult(returncode=0, stdout=("\n".join(applied) + "\n").encode(), stderr=b"")

    def _handle_delete(self, args: Sequence[str]) -> SubprocessResult:
        if _flag(args, "-f") is None:
            return self._handle_delete_by_name(args)
        manifest_path = _flag(args, "-f")
        ignore_not_found = "--ignore-not-found" in args
        with open(manifest_path) as f:
            manifest_text = f.read()

        removed = []
        missing = []
        for doc in yaml.safe_load_all(manifest_text):
            if not isinstance(doc, dict):
                continue
            key = _manifest_object_key(doc)
            if key is None:
                continue
            if key in self.backend.applied_manifests:
                del self.backend.applied_manifests[key]
                removed.append(f"{key[0].lower()}.apps \"{key[2]}\" deleted")
            else:
                missing.append(key)

        if missing and not ignore_not_found:
            name = missing[0][2]
            return SubprocessResult(returncode=1, stdout=b"", stderr=f'Error from server (NotFound): "{name}" not found\n'.encode())
        return SubprocessResult(returncode=0, stdout=("\n".join(removed) + "\n").encode(), stderr=b"")

    def _handle_delete_by_name(self, args: Sequence[str]) -> SubprocessResult:
        """`kubectl delete <types> --all -n ns` and `kubectl delete <type> NAME... -n ns`
        (Round 8b: kube.wipe_namespace / kube.delete_daemonset). Unknown types are
        silently ignored rather than erroring: real kubectl deletes every type it
        recognizes in a comma-separated list, and v1's wipe passed eleven of them."""
        namespace = _flag(args, "-n") or "default"
        ignore_not_found = "--ignore-not-found" in args or "--ignore-not-found=true" in args
        types = {tp.strip() for tp in (args[0] if args else "").split(",") if tp.strip()}
        stores = {
            "daemonsets": self.backend.daemonsets, "daemonset": self.backend.daemonsets,
            "services": self.backend.services, "service": self.backend.services, "svc": self.backend.services,
            "deployments": self.backend.deployments, "deployment": self.backend.deployments,
            "pods": self.backend.pods, "pod": self.backend.pods,
            "jobs": self.backend.jobs, "job": self.backend.jobs,
        }
        removed: list[str] = []

        if "--all" in args:
            for tp in types:
                store = stores.get(tp)
                if store is None:
                    continue
                for key in [k for k in store if k[0] == namespace]:
                    del store[key]
                    removed.append(f'{tp} "{key[1]}" deleted')
            return SubprocessResult(returncode=0, stdout=("\n".join(removed) + "\n").encode(), stderr=b"")

        # `delete <type> NAME [NAME...]` -- every positional after the type that is
        # not a flag or a flag's value.
        store = stores.get(args[0] if args else "")
        names = [a for a in args[1:] if not a.startswith("-") and a != namespace]
        if store is None:
            return SubprocessResult(returncode=1, stdout=b"", stderr=f"error: unknown resource {args[0]!r}".encode())
        missing = [n for n in names if (namespace, n) not in store]
        for name in names:
            if (namespace, name) in store:
                del store[(namespace, name)]
                removed.append(f'{args[0]} "{name}" deleted')
        if missing and not ignore_not_found:
            return SubprocessResult(
                returncode=1, stdout=b"",
                stderr=f'Error from server (NotFound): {args[0]} "{missing[0]}" not found\n'.encode(),
            )
        return SubprocessResult(returncode=0, stdout=("\n".join(removed) + "\n").encode(), stderr=b"")

    def _handle_rollout(self, args: Sequence[str]) -> SubprocessResult:
        action = args[0] if args else ""
        namespace = _flag(args, "-n") or "default"
        target = args[1] if len(args) > 1 else ""
        deployment_name = target.removeprefix("deployment/")

        if action == "restart":
            return SubprocessResult(returncode=0, stdout=f'deployment.apps/{deployment_name} restarted\n'.encode(), stderr=b"")

        if action == "status":
            deployment = self.backend.deployments.get((namespace, deployment_name))
            if deployment is None:
                return SubprocessResult(returncode=1, stdout=b"", stderr=f'error: deployments.apps "{deployment_name}" not found\n'.encode())
            status = deployment["status"]
            desired = deployment["spec"]["replicas"]
            if status.get("readyReplicas", 0) >= desired:
                return SubprocessResult(returncode=0, stdout=f'deployment "{deployment_name}" successfully rolled out\n'.encode(), stderr=b"")
            # Real `kubectl rollout status --watch=false` exits NON-ZERO while still
            # progressing (all currently-supported kubectl versions) — only a completed
            # rollout exits 0. Returning 0 here would mask the row-31 dead-code bug the
            # provider must guard against (see kubectl.py's `_probe_rollout`).
            return SubprocessResult(
                returncode=1,
                stdout=f'Waiting for deployment "{deployment_name}" rollout to finish: {status.get("readyReplicas", 0)} of {desired} updated replicas are available...\n'.encode(),
                stderr=b"",
            )

        if action == "undo":
            if deployment_name in self.backend.rollout_undo_failures:
                return SubprocessResult(returncode=1, stdout=b"", stderr=f"error: deployment {deployment_name!r} has no rollout history\n".encode())
            return SubprocessResult(returncode=0, stdout=f'deployment.apps/{deployment_name} rolled back\n'.encode(), stderr=b"")

        return SubprocessResult(returncode=1, stdout=b"", stderr=f"error: unknown rollout action {action!r}".encode())

    # ------------------------------------------------------------------
    # streaming
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cluster_id: str | None = None,
    ) -> AsyncIterator[AsyncIterator[bytes]]:
        self.backend.attempt_count += 1
        self.backend.call_log.append(tuple(argv))

        if Fault.UNREACHABLE in self.faults:
            raise InfrastructureUnreachableError(
                "fake kubectl watch: connection refused",
                code=ErrorCode.ENDPOINT_UNREACHABLE, provider="kubectl", command="watch_pods",
                host=self.backend.apiserver_url,
            )
        if Fault.TRANSIENT_ONCE in self.faults and not self._transient_once_consumed:
            self._transient_once_consumed = True
            raise InfrastructureUnreachableError(
                "fake kubectl watch: i/o timeout",
                code=ErrorCode.API_TIMEOUT, provider="kubectl", command="watch_pods",
                host=self.backend.apiserver_url,
            )

        # Decoupled producer/queue consumed through a plain CLASS-based iterator, not an async
        # generator: the real transport's line iterator sits on top of an OS pipe + asyncio
        # `StreamReader`, whose `__anext__` is an ordinary method that calls `readline()` fresh
        # every time — cancelling one in-flight call (the 30s heartbeat's `wait_for`,
        # kubectl.py's `_watch_pods`) never loses already-arrived bytes, since the *next*
        # `readline()` call just resumes reading the same persistent buffer. An async
        # *generator*'s `__anext__` does NOT have that property: cancelling it mid-`await`
        # throws into the suspended generator FRAME, which — left uncaught — permanently closes
        # the generator, so every later call raises `StopAsyncIteration` instead of actually
        # resuming. `_QueueLineIterator` below is a plain class (no generator frame) for exactly
        # this reason, backed by an `asyncio.Queue` a decoupled producer task fills
        # independently of whatever the consumer is currently awaiting.
        queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def _produce() -> None:
            for line in self.backend.watch_lines:
                if self.backend.watch_line_delay_s:
                    await asyncio.sleep(self.backend.watch_line_delay_s)
                await queue.put(line.encode() if isinstance(line, str) else line)
            await queue.put(None)  # sentinel: end of stream

        producer = asyncio.ensure_future(_produce())
        try:
            yield _QueueLineIterator(queue)
        finally:
            producer.cancel()
            try:
                await producer
            except asyncio.CancelledError:
                pass


class _QueueLineIterator:
    """Plain class-based ``AsyncIterator[bytes]`` — see the ``stream()`` comment above for why
    this must NOT be an async generator."""

    def __init__(self, queue: asyncio.Queue[bytes | None]) -> None:
        self._queue = queue

    def __aiter__(self) -> _QueueLineIterator:
        return self

    async def __anext__(self) -> bytes:
        item = await self._queue.get()
        if item is None:
            raise StopAsyncIteration
        return item


def _flag(args: Sequence[str], name: str) -> str | None:
    if name in args:
        idx = args.index(name)
        if idx + 1 < len(args):
            return args[idx + 1]
    return None


def _json_ok(data: object) -> SubprocessResult:
    return SubprocessResult(returncode=0, stdout=json.dumps(data).encode(), stderr=b"")
