"""tests/conformance/fake_sshd.py — a typed FAKE TRANSPORT simulating enough of a booting VM's
``sshd``/``cloud-init``/``k3s`` lifecycle for ``seedpod.providers.ssh_k3s.SshK3sProvider``
conformance testing (Seam C §5.6).

``FakeSshdBackend`` is the in-memory "guest VM": a plain mutable state machine (host keys
available yet? cloud-init done? k3s systemd unit active? k3s API responding? kubeconfig
content). ``FakeSshTransport`` implements the ``SubprocessRunner`` protocol
(``seedpod.providers.contract.SubprocessRunner``) — installed directly as the provider's
``transport`` — so fault injection happens at the actual transport seam the provider talks to,
never ``Mock``/``patch`` (CLAUDE.md).
"""

from __future__ import annotations

import asyncio
import base64
import re
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field

from seedpod.providers.contract import SubprocessResult
from tests.conformance.harness import Fault

__all__ = ["FakeSshdBackend", "FakeSshTransport"]

_DEFAULT_KUBECONFIG = """\
apiVersion: v1
kind: Config
clusters:
- name: default
  cluster:
    server: https://127.0.0.1:6443
    certificate-authority-data: ZmFrZS1jYQ==
contexts:
- name: default
  context:
    cluster: default
    user: default
current-context: default
users:
- name: default
  user:
    token: fake-token
"""


@dataclass
class FakeSshdBackend:
    """The in-memory guest VM. Every field defaults to the "fully booted, healthy" state so a
    harness only has to override what a given test cares about.
    """

    host_keys_available: bool = True
    k3s_active: bool = True
    k3s_api_ready: bool = True
    kubeconfig_yaml: str = _DEFAULT_KUBECONFIG
    traefik_hostport_written: bool = False
    # DR-0036: the DECODED manifest, so tests can assert what actually reached the
    # host rather than only that a write happened.
    traefik_manifest: str | None = None
    install_flags_seen: str | None = None
    call_log: list[tuple[str, ...]] = field(default_factory=list)
    attempt_count: int = 0

    # C-22 cancellation-cleanup hook (additive Harness/fake extension — the seam table
    # names `InstallK3s` as one of the two commands C-22 must exercise, but nothing in
    # `run()` blocks long enough to be cancelled without this): an artificial delay
    # applied only to the `INSTALL_K3S_EXEC` remote command, so a caller can
    # `task.cancel()` while the fake is still "mid-install".
    install_delay_s: float = 0.0


class FakeSshTransport:
    """Implements ``seedpod.providers.contract.SubprocessRunner`` against a
    ``FakeSshdBackend``. Routes on ``argv[0]`` (``ssh`` / ``ssh-keyscan``) and, for ``ssh``, on
    the trailing remote-command string — mirroring how the real OpenSSH client is invoked by
    ``ssh_k3s.py``.
    """

    def __init__(self, backend: FakeSshdBackend, faults: frozenset[Fault]) -> None:
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

        # MISSING_SOURCE's closest ssh-k3s equivalent: a required prerequisite (the ssh/
        # ssh-keyscan binary itself) is absent — used by Harness.broken_environment() (C-01).
        if Fault.MISSING_SOURCE in self.faults:
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"", binary_missing=True)

        if Fault.UNREACHABLE in self.faults:
            return SubprocessResult(
                returncode=255, stdout=b"", stderr=b"ssh: connect to host: Connection refused"
            )

        if Fault.TRANSIENT_ONCE in self.faults and not self._transient_once_consumed:
            self._transient_once_consumed = True
            return SubprocessResult(returncode=255, stdout=b"", stderr=b"ssh: connection timed out")

        # ssh-k3s's closest structural equivalent of "auth": a rejected key. Per decision-table
        # row 17 this classifies the same as any other non-connectivity non-zero exit
        # (Permanent/SCRIPT_FAILED) — there is no distinct ssh-level AUTH ErrorCode in the
        # taxonomy for this provider.
        if Fault.AUTH in self.faults:
            return SubprocessResult(returncode=255, stdout=b"", stderr=b"Permission denied (publickey).")

        if binary == "ssh-keyscan":
            if not self.backend.host_keys_available:
                return SubprocessResult(returncode=0, stdout=b"", stderr=b"")
            return SubprocessResult(
                returncode=0,
                stdout=b"host.example ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI1\nhost.example ssh-rsa AAAAB3NzaC1yc2EAAAA1\n",
                stderr=b"",
            )

        if binary == "ssh":
            if len(argv) == 2 and argv[1] == "-V":
                return SubprocessResult(returncode=0, stdout=b"", stderr=b"OpenSSH_9.0\n")
            command = argv[-1]
            if self.backend.install_delay_s and "INSTALL_K3S_EXEC" in command:
                await asyncio.sleep(self.backend.install_delay_s)
            return self._handle_ssh_command(command)

        return SubprocessResult(returncode=127, stdout=b"", stderr=f"command not found: {binary}".encode())

    def _handle_ssh_command(self, command: str) -> SubprocessResult:
        if "cloud-init status --wait" in command:
            return SubprocessResult(returncode=0, stdout=b"status: done\n", stderr=b"")

        if "systemctl is-active k3s" in command:
            if self.backend.k3s_active:
                return SubprocessResult(returncode=0, stdout=b"active\n", stderr=b"")
            return SubprocessResult(returncode=3, stdout=b"inactive\n", stderr=b"")

        if "kubectl get nodes" in command:
            if self.backend.k3s_api_ready:
                return SubprocessResult(returncode=0, stdout=b"node1   Ready    <none>   1m   v1.28.1+k3s1\n", stderr=b"")
            return SubprocessResult(
                returncode=1,
                stdout=b"",
                stderr=b"The connection to the server localhost:6443 was refused - did you specify the right host or port?\n",
            )

        if "base64 -d" in command and "traefik-config.yaml" in command:
            self.backend.traefik_hostport_written = True
            match = re.search(r"echo ([A-Za-z0-9+/=]+) \| base64 -d", command)
            if match:
                self.backend.traefik_manifest = base64.b64decode(match.group(1)).decode()
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"")

        if "INSTALL_K3S_EXEC" in command:
            self.backend.install_flags_seen = command
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"")

        if "cat /etc/rancher/k3s/k3s.yaml" in command:
            return SubprocessResult(returncode=0, stdout=self.backend.kubeconfig_yaml.encode(), stderr=b"")

        return SubprocessResult(returncode=127, stdout=b"", stderr=f"fake sshd: no route for {command!r}".encode())

    def stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cluster_id: str | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]:
        """ssh-k3s never streams (only ``KubeWatchPods`` does, on the ``kubectl`` provider) —
        present only to satisfy the ``SubprocessRunner`` protocol shape."""
        raise NotImplementedError("ssh-k3s never calls SubprocessRunner.stream()")
