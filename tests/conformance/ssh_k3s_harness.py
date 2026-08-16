"""tests/conformance/ssh_k3s_harness.py — ``Harness`` implementation for the ``ssh-k3s``
provider (Seam C §5.6), backed by ``tests/conformance/fake_sshd.py``.

ssh-k3s is k3s-plane only — it has no ``CreateInstance``/``Reconcile`` concept at all (that is
the machine planes' job). ``create_command()``/``reconcile_truth_table()`` are structurally
inapplicable here; per the Harness protocol's own docstrings ("machine providers only") the
later shared suite's capability skip list (Seam C §5.6) is expected to skip this harness for
those cases. ``create_command()`` raises ``NotImplementedError`` (there is no well-typed "empty"
``CreateInstance`` to return); ``reconcile_truth_table()`` returns ``()`` since its return type
is already a ``Sequence``.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager

from seedpod.core.errors import ErrorCode, PermanentError, ProviderError, TransientError
from seedpod.providers.contract import (
    CaptureHostKeys,
    CreateInstance,
    FetchKubeconfig,
    IngressConfig,
    InstallK3s,
    ProbeK3s,
    Provider,
    ProviderCommand,
    SSHTarget,
)
from seedpod.providers.ssh_k3s import SshK3sConfig, SshK3sProvider
from tests.conformance.fake_sshd import FakeSshdBackend, FakeSshTransport
from tests.conformance.harness import Fault, ReconcileCase

__all__ = ["SshK3sHarness"]

_HOST = "10.42.0.7"
_KNOWN_HOSTS = "10.42.0.7 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI1\n"


class SshK3sHarness:
    name = "ssh-k3s"

    def __init__(self) -> None:
        self.backend = FakeSshdBackend()

    # ------------------------------------------------------------------
    # Harness protocol
    # ------------------------------------------------------------------

    def provider(self, *faults: Fault) -> Provider:
        transport = FakeSshTransport(self.backend, frozenset(faults))
        return SshK3sProvider(SshK3sConfig(), transport)

    @contextmanager
    def broken_environment(self) -> Iterator[Provider]:
        yield self.provider(Fault.MISSING_SOURCE)

    async def backend_resources(self) -> frozenset[str]:
        """ssh-k3s manages no backend resource ids of its own (the VM's lifecycle belongs to
        the machine provider that created it) — nothing to leak-check here."""
        return frozenset()

    def backend_attempts(self) -> int:
        return self.backend.attempt_count

    def create_command(self) -> CreateInstance:
        raise NotImplementedError("ssh-k3s has no CreateInstance concept — k3s plane only (see module docstring)")

    def observe_command(self) -> ProviderCommand:
        return self.probe_k3s_command()

    def ssh_target(self, *, host: str = _HOST) -> SSHTarget:
        return SSHTarget(host=host, user="root", private_key_path="/tmp/fake-key", command_timeout_s=30)

    def capture_host_keys_command(self) -> CaptureHostKeys:
        return CaptureHostKeys(ssh=self.ssh_target(), cloud_init_wait_timeout_s=30, keyscan_timeout_s=5)

    def install_k3s_command(self, *, known_hosts: str = _KNOWN_HOSTS) -> InstallK3s:
        return InstallK3s(
            ssh=self.ssh_target(),
            known_hosts=known_hosts,
            pod_cidr="10.42.7.0/24",
            service_cidr="10.43.7.0/24",
            tls_sans=("10.42.0.7",),
            ingress=IngressConfig(ingress_type="traefik", enabled=True, expose_method="loadbalancer"),
        )

    def probe_k3s_command(self) -> ProbeK3s:
        return ProbeK3s(ssh=self.ssh_target(), known_hosts=_KNOWN_HOSTS)

    def fetch_kubeconfig_command(self, *, rewrite_to: str = "cluster.example.internal") -> FetchKubeconfig:
        return FetchKubeconfig(rewrite_server_to=rewrite_to, ssh=self.ssh_target(), known_hosts=_KNOWN_HOSTS)

    def reconcile_truth_table(self) -> Sequence[ReconcileCase]:
        return ()  # no Reconcile concept on the k3s plane

    def rewrite_cases(self) -> Sequence[tuple[str, FetchKubeconfig, str]]:
        return (
            (
                "ssh_preserves_scheme_and_port",
                FetchKubeconfig(rewrite_server_to="vm.tailnet.ts.net", ssh=self.ssh_target(), known_hosts=_KNOWN_HOSTS),
                r"https://vm\.tailnet\.ts\.net:6443",
            ),
            (
                "ssh_no_rewrite_when_target_empty",
                FetchKubeconfig(rewrite_server_to="", ssh=self.ssh_target(), known_hosts=_KNOWN_HOSTS),
                r"https://127\.0\.0\.1:6443",
            ),
        )

    def classification_command(self, fault: Fault) -> ProviderCommand:
        """C-04/C-17's representative command per fault (additive Harness extension — mined
        verbatim from ``test_ssh_k3s_smoke.py``'s ``test_classification_table``, which always
        uses ``capture_host_keys_command()``)."""
        return self.capture_host_keys_command()

    def classification_cases(self) -> Sequence[tuple[Fault, type[ProviderError], ErrorCode]]:
        return (
            # ssh-k3s never raises InfrastructureUnreachableError (§5.1 point 3) — its closest
            # structural equivalent of an "unreachable backend" is an ordinary connectivity
            # symptom, which classifies as Transient/ENDPOINT_UNREACHABLE (decision row 16).
            (Fault.UNREACHABLE, TransientError, ErrorCode.ENDPOINT_UNREACHABLE),
            (Fault.TRANSIENT_ONCE, TransientError, ErrorCode.ENDPOINT_UNREACHABLE),
            # No distinct ssh-level AUTH code in the taxonomy (see fake_sshd.py's comment) —
            # rejected key is just another non-connectivity non-zero exit (row 17).
            (Fault.AUTH, PermanentError, ErrorCode.SCRIPT_FAILED),
        )
