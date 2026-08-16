"""tests/conformance/fake_kind.py — a typed FAKE TRANSPORT simulating enough of the local
``kind`` CLI + Docker daemon (control-plane container lifecycle, port bindings, kubeconfig
retrieval) for ``seedpod.providers.kind.KindProvider`` conformance testing (Seam C §5.6).

``FakeKindBackend`` is the in-memory "Docker host": a plain mutable store of kind clusters
(name -> running/port/kubeconfig). ``FakeKindTransport`` implements the ``SubprocessRunner``
protocol (``seedpod.providers.contract.SubprocessRunner``) — installed directly as the
provider's ``transport`` — so fault injection happens at the actual transport seam the provider
talks to, never ``Mock``/``patch`` (CLAUDE.md).

Routing mirrors how ``kind.py`` actually invokes each binary: ``argv[0] == "kind"`` dispatches
on ``argv[1]`` (``version`` / ``create`` / ``delete`` / ``get clusters`` / ``get kubeconfig``);
``argv[0] == "docker"`` dispatches on the trailing container name for ``inspect --format``. The
``kind create cluster --config <path>`` call legitimately reads the temp file
``seedpod/core/tempfiles.py`` wrote to disk — exactly what the real ``kind`` binary does — to
learn the allocated API port and CIDRs, rather than the fake reaching into the provider's
internals.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field

import yaml

from seedpod.providers.contract import SubprocessResult
from tests.conformance.harness import Fault

__all__ = ["FakeKindBackend", "FakeKindTransport"]


def _kubeconfig_yaml(port: int) -> str:
    return f"""\
apiVersion: v1
kind: Config
clusters:
- name: kind-fake
  cluster:
    server: https://0.0.0.0:{port}
    certificate-authority-data: ZmFrZS1jYQ==
contexts:
- name: kind-fake
  context:
    cluster: kind-fake
    user: kind-fake
current-context: kind-fake
users:
- name: kind-fake
  user:
    token: fake-token
"""


@dataclass
class FakeKindBackend:
    """The in-memory Docker host. Every "cluster" is a control-plane container: ``present``
    (docker container exists), ``running`` (``docker inspect .State.Running``), and ``port``
    (the ``6443/tcp`` host binding — read from the ``kind create`` config file, mirroring what
    the real ``docker inspect``/``kind get kubeconfig`` would report).
    """

    clusters: dict[str, dict[str, object]] = field(default_factory=dict)
    call_log: list[tuple[str, ...]] = field(default_factory=list)
    attempt_count: int = 0

    def seed_cluster(self, name: str, *, port: int = 6443, running: bool = True) -> None:
        """Test setup helper (not part of the kind/docker CLI surface): directly inserts a
        cluster, bypassing a real ``kind create cluster`` round-trip, for harness pre-seeding."""
        self.clusters[name] = {"running": running, "port": port}

    def present_names(self) -> frozenset[str]:
        return frozenset(self.clusters.keys())


class FakeKindTransport:
    """Implements ``seedpod.providers.contract.SubprocessRunner`` against a ``FakeKindBackend``.
    """

    def __init__(self, backend: FakeKindBackend, faults: frozenset[Fault]) -> None:
        self.backend = backend
        self.faults = faults
        self._transient_once_consumed = False

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

        # MISSING_SOURCE's closest kind equivalent (Fault docstring: "not every provider has a
        # literal match") — kind has no pre-pulled-base-image precondition the way tart does, so
        # this maps onto the ``kind`` binary itself being absent (broken_environment / C-01).
        if Fault.MISSING_SOURCE in self.faults and binary == "kind":
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"", binary_missing=True)

        if Fault.UNREACHABLE in self.faults:
            return SubprocessResult(
                returncode=1, stdout=b"", stderr=b"Cannot connect to the Docker daemon. Is the docker daemon running?"
            )

        if Fault.TRANSIENT_ONCE in self.faults and not self._transient_once_consumed:
            self._transient_once_consumed = True
            return SubprocessResult(returncode=1, stdout=b"", stderr=b"dial unix docker.sock: connection refused")

        if binary == "docker":
            return self._handle_docker(argv)
        if binary == "kind":
            return self._handle_kind(argv)
        return SubprocessResult(returncode=127, stdout=b"", stderr=f"command not found: {binary}".encode())

    def _handle_docker(self, argv: Sequence[str]) -> SubprocessResult:
        if len(argv) >= 2 and argv[1] == "version":
            return SubprocessResult(returncode=0, stdout=b"Docker version 24.0.0, build deadbee\n", stderr=b"")
        if not (len(argv) >= 2 and argv[1] == "inspect"):
            return SubprocessResult(returncode=127, stdout=b"", stderr=f"fake docker: no route for {argv!r}".encode())

        # docker inspect --format {fmt} {container_name}
        fmt, container_name = argv[-2], argv[-1]
        name = container_name.removesuffix("-control-plane")
        cluster = self.backend.clusters.get(name)
        if cluster is None:
            return SubprocessResult(returncode=1, stdout=b"", stderr=f"Error: No such object: {container_name}".encode())
        if fmt == "{{.State.Running}}":
            return SubprocessResult(returncode=0, stdout=str(cluster["running"]).lower().encode(), stderr=b"")
        if fmt == "{{json .NetworkSettings.Ports}}":
            body = f'{{"6443/tcp":[{{"HostIp":"0.0.0.0","HostPort":"{cluster["port"]}"}}]}}'
            return SubprocessResult(returncode=0, stdout=body.encode(), stderr=b"")
        return SubprocessResult(returncode=1, stdout=b"", stderr=b"fake docker: unknown --format")

    def _handle_kind(self, argv: Sequence[str]) -> SubprocessResult:
        sub = argv[1] if len(argv) > 1 else ""

        if sub == "version":
            return SubprocessResult(returncode=0, stdout=b"kind v0.20.0 go1.21.1\n", stderr=b"")

        if sub == "get" and argv[2] == "clusters":
            names = sorted(self.backend.present_names())
            body = "\n".join(names)
            return SubprocessResult(returncode=0, stdout=body.encode(), stderr=b"")

        if sub == "get" and argv[2] == "kubeconfig":
            name = argv[argv.index("--name") + 1]
            cluster = self.backend.clusters.get(name)
            if cluster is None:
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"unknown cluster")
            return SubprocessResult(returncode=0, stdout=_kubeconfig_yaml(int(cluster["port"])).encode(), stderr=b"")

        if sub == "create":
            name = argv[argv.index("--name") + 1]
            config_path = argv[argv.index("--config") + 1]
            with open(config_path) as f:  # the real `kind` binary reads this same file
                config = yaml.safe_load(f)
            port = config["nodes"][0]["extraPortMappings"][0]["hostPort"]

            if Fault.DIE_MID_CREATE in self.faults:
                # Simulates `kind create cluster` leaving a partial docker container behind
                # before failing (the C1 window this provider's undo_for(CreateInstance) must
                # close) — no connectivity phrase, so this classifies as a clean non-zero exit
                # (row 24: Permanent/SCRIPT_FAILED), not Unreachable.
                self.backend.seed_cluster(name, port=port, running=False)
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"ERROR: failed to create cluster: node(s) not ready")

            self.backend.seed_cluster(name, port=port, running=True)
            return SubprocessResult(returncode=0, stdout=b"", stderr=f'Creating cluster "{name}" ...\n'.encode())

        if sub == "delete":
            name = argv[argv.index("--name") + 1]
            self.backend.clusters.pop(name, None)
            return SubprocessResult(returncode=0, stdout=b"", stderr=f"Deleted nodes: [\"{name}-control-plane\"]\n".encode())

        return SubprocessResult(returncode=127, stdout=b"", stderr=f"fake kind: no route for {argv!r}".encode())

    def stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cluster_id: str | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]:
        """kind never streams (only ``KubeWatchPods`` does, on the ``kubectl`` provider) —
        present only to satisfy the ``SubprocessRunner`` protocol shape."""
        raise NotImplementedError("kind never calls SubprocessRunner.stream()")
