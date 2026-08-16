"""tests/conformance/fake_orbstack.py — a typed FAKE TRANSPORT simulating enough of the local
``kubectl`` binary talking to OrbStack's built-in cluster (``cluster-info``/``config view`` against
the ``orbstack`` context) for ``seedpod.providers.orbstack.OrbstackProvider`` conformance testing
(Seam C §5.6).

``FakeOrbstackBackend`` is the in-memory "local machine": whether the ``orbstack`` kubectl context
exists at all (``context_exists`` — OrbStack.app installed/ever-configured), whether ``kubectl``
itself is on PATH (``kubectl_missing``), and the port OrbStack's API server is bound to
(``api_port`` — the crown-jewel-#6 rewrite case this port must survive unchanged).
``FakeOrbstackTransport`` implements the ``SubprocessRunner`` protocol
(``seedpod.providers.contract.SubprocessRunner``) — installed directly as the provider's
``transport`` — so fault injection happens at the actual transport seam the provider talks to,
never ``Mock``/``patch`` (CLAUDE.md).

Routing mirrors how ``orbstack.py`` actually invokes the binary: ``argv[0] == "kubectl"``
dispatches on ``argv[1]`` (``cluster-info`` / ``config``), matching the exact stderr phrasing a
real ``kubectl`` prints for the two failure modes this provider's ``classify_subprocess`` usage
splits on (module docstring's opening paragraph): "connection ... refused" (a
``TRANSIENT_STDERR_PHRASES`` match — connectivity symptom) vs. ``error: context "orbstack" does
not exist`` (a clean non-zero exit with no connectivity phrase — the context-missing case).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field

from seedpod.providers.contract import SubprocessResult
from tests.conformance.harness import Fault

__all__ = ["FakeOrbstackBackend", "FakeOrbstackTransport"]


@dataclass
class FakeOrbstackBackend:
    """The in-memory "local machine" ``orbstack`` provider talks to. There is exactly one
    resource — the built-in cluster's kubectl context — so, unlike every other fake backend in
    this directory, there is no per-resource dict: just whether that one context is configured
    at all (``context_exists``) and whether ``kubectl`` itself is even installed
    (``kubectl_missing``)."""

    context: str = "orbstack"
    context_exists: bool = True
    kubectl_missing: bool = False
    api_port: int = 32770  # OrbStack's real high, ephemeral-looking API port — the C-19 case
    call_log: list[tuple[str, ...]] = field(default_factory=list)
    attempt_count: int = 0

    def break_context(self) -> None:
        """Test setup helper (not part of the kubectl CLI surface): simulates OrbStack.app never
        having been installed/configured on this machine — the ``orbstack`` context is simply
        absent from the local kubeconfig (module docstring's "structural context-missing" case,
        distinct from a live connectivity symptom)."""
        self.context_exists = False

    def remove_kubectl(self) -> None:
        """Test setup helper: simulates ``kubectl`` itself not being on PATH."""
        self.kubectl_missing = True

    def present_names(self) -> frozenset[str]:
        return frozenset({self.context}) if self.context_exists else frozenset()

    def kubeconfig_yaml(self) -> str:
        return f"""\
apiVersion: v1
kind: Config
clusters:
- name: orbstack
  cluster:
    server: https://127.0.0.1:{self.api_port}
    certificate-authority-data: ZmFrZS1jYQ==
contexts:
- name: orbstack
  context:
    cluster: orbstack
    user: orbstack
current-context: orbstack
users:
- name: orbstack
  user:
    token: fake-token
"""


class FakeOrbstackTransport:
    """Implements ``seedpod.providers.contract.SubprocessRunner`` against a
    ``FakeOrbstackBackend``."""

    def __init__(self, backend: FakeOrbstackBackend, faults: frozenset[Fault]) -> None:
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

        if self.backend.kubectl_missing:
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"", binary_missing=True)

        if Fault.UNREACHABLE in self.faults:
            # A live OrbStack.app whose API server isn't answering — a genuine connectivity
            # symptom (stderr phrase "cannot connect", TRANSIENT_STDERR_PHRASES) => Unreachable.
            return SubprocessResult(
                returncode=1,
                stdout=b"",
                stderr=b"The connection to the server 127.0.0.1:32770 was refused - "
                b"did you specify the right host or port? (cannot connect)",
            )

        if Fault.TRANSIENT_ONCE in self.faults and not self._transient_once_consumed:
            self._transient_once_consumed = True
            return SubprocessResult(returncode=1, stdout=b"", stderr=b"dial tcp 127.0.0.1:32770: i/o timeout")

        # MISSING_SOURCE's closest orbstack equivalent (Fault docstring: "not every provider has
        # a literal match") — OrbStack has no pre-pulled-base-image precondition the way
        # tart/kind do, so this maps onto the module docstring's OTHER failure mode: the
        # `orbstack` context missing from the local kubeconfig entirely (never installed), a
        # clean non-zero exit with no connectivity phrase.
        if Fault.MISSING_SOURCE in self.faults or not self.backend.context_exists:
            return SubprocessResult(
                returncode=1, stdout=b"", stderr=f'error: context "{self.backend.context}" does not exist'.encode()
            )

        if argv[0] != "kubectl":
            return SubprocessResult(returncode=127, stdout=b"", stderr=f"command not found: {argv[0]}".encode())

        return self._handle_kubectl(list(argv[1:]))

    def _handle_kubectl(self, args: list[str]) -> SubprocessResult:
        if args[:1] == ["cluster-info"]:
            body = f"Kubernetes control plane is running at https://127.0.0.1:{self.backend.api_port}\n"
            return SubprocessResult(returncode=0, stdout=body.encode(), stderr=b"")

        if args[:2] == ["config", "view"]:
            return SubprocessResult(returncode=0, stdout=self.backend.kubeconfig_yaml().encode(), stderr=b"")

        return SubprocessResult(returncode=127, stdout=b"", stderr=f"fake kubectl: no route for {args!r}".encode())

    def stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cluster_id: str | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]:
        """orbstack never streams (only ``KubeWatchPods`` does, on the ``kubectl`` provider) —
        present only to satisfy the ``SubprocessRunner`` protocol shape."""
        raise NotImplementedError("orbstack never calls SubprocessRunner.stream()")
