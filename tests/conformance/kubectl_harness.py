"""tests/conformance/kubectl_harness.py — ``Harness`` implementation for the ``kubectl``
provider (Seam C §5.6), backed by ``tests/conformance/fake_kubectl.py``.

kubectl is kubernetes-plane only — it has no ``CreateInstance``/``Reconcile``/
``FetchKubeconfig`` concept at all (those are the machine/k3s planes' job). Per the Harness
protocol's own docstrings ("machine providers only" / "empty for providers that don't implement
FetchKubeconfig"), ``create_command()`` raises ``NotImplementedError`` (mirrors
``ssh_k3s_harness.py``'s identical omission), ``reconcile_truth_table()``/``rewrite_cases()``
both return ``()``.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager

from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
)
from seedpod.providers.contract import (
    CreateInstance,
    FetchKubeconfig,
    KubeGetPodDetails,
    Provider,
    ProviderCommand,
)
from seedpod.providers.kubectl import KubectlConfig, KubectlProvider
from tests.conformance.fake_kubectl import FakeKubectlBackend, FakeKubectlTransport
from tests.conformance.harness import Fault, ReconcileCase

__all__ = ["KubectlHarness", "FAKE_KUBECONFIG", "NAMESPACE", "SEEDED_POD", "SEEDED_DEPLOYMENT"]

NAMESPACE = "default"
SEEDED_POD = "web-abc123"
SEEDED_DEPLOYMENT = "web"

FAKE_KUBECONFIG = """\
apiVersion: v1
kind: Config
clusters:
- name: fake
  cluster:
    server: https://10.96.0.1:6443
    certificate-authority-data: ZmFrZS1jYQ==
contexts:
- name: fake
  context:
    cluster: fake
    user: fake
current-context: fake
users:
- name: fake
  user:
    token: fake-token
"""


class KubectlHarness:
    name = "kubectl"

    def __init__(self) -> None:
        self.backend = FakeKubectlBackend()

    # ------------------------------------------------------------------
    # Harness protocol
    # ------------------------------------------------------------------

    def provider(self, *faults: Fault) -> Provider:
        transport = FakeKubectlTransport(self.backend, frozenset(faults))
        return KubectlProvider(KubectlConfig(), transport)

    @contextmanager
    def broken_environment(self) -> Iterator[Provider]:
        yield self.provider(Fault.MISSING_SOURCE)

    async def backend_resources(self) -> frozenset[str]:
        return self.backend.present_manifest_keys()

    def backend_attempts(self) -> int:
        return self.backend.attempt_count

    def create_command(self) -> CreateInstance:
        raise NotImplementedError("kubectl has no CreateInstance concept — kubernetes plane only (see module docstring)")

    def observe_command(self) -> ProviderCommand:
        return KubeGetPodDetails(kubeconfig=FAKE_KUBECONFIG, pod_name=SEEDED_POD, namespace=NAMESPACE)

    def reconcile_truth_table(self) -> Sequence[ReconcileCase]:
        return ()  # no Reconcile concept on the kubernetes plane

    def rewrite_cases(self) -> Sequence[tuple[str, FetchKubeconfig, str]]:
        return ()  # kubectl never implements FetchKubeconfig (that's ssh-k3s/kind/orbstack)

    def classification_command(self, fault: Fault) -> ProviderCommand:
        """C-04/C-17's representative command per fault (additive Harness extension — mined
        verbatim from ``test_kubectl_smoke.py``'s ``test_classification_table``, which always
        uses ``observe_command()``)."""
        return self.observe_command()

    def classification_cases(self) -> Sequence[tuple[Fault, type[ProviderError], ErrorCode]]:
        # Fault.MISSING_SOURCE deliberately absent here: it is a check_ready-only concept for
        # kubectl (binary missing at startup — see broken_environment() above), mirroring
        # ssh_k3s_harness.py/kind_harness.py's identical omission for the same reason.
        return (
            (Fault.UNREACHABLE, InfrastructureUnreachableError, ErrorCode.ENDPOINT_UNREACHABLE),
            (Fault.TRANSIENT_ONCE, InfrastructureUnreachableError, ErrorCode.ENDPOINT_UNREACHABLE),
            (Fault.AUTH, PermanentError, ErrorCode.AUTH),
        )
