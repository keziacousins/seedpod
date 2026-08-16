"""tests/engine/steps/test_k3s_steps.py — ``seedpod/engine/steps/k3s.py``'s five
verbs (Round 8a, "k3s-family" component): ``k3s.await_ssh``,
``k3s.trust_host_keys``, ``k3s.install``, ``k3s.await_api``,
``k3s.fetch_kubeconfig``.

Against the REAL, already-conformance-tested ``SshK3sProvider``
(``seedpod/providers/ssh_k3s.py``), backed by the shared conformance harness's
FAKE TRANSPORT (``tests/conformance/ssh_k3s_harness.py`` / ``fake_sshd.py``) --
never ``Mock``/``patch`` anywhere (CLAUDE.md testing posture). ``ctx`` is a
real ``StepContext`` built via ``tests/engine/fakes.py``'s
``make_step_context``.

Covers this task's own checklist:
- each verb maps to exactly the right Seam C command with the right params
  (``command()`` is a pure params -> command mapping, asserted twice-equal);
- DR-0023: SSH identity arrives as typed ``Params`` (``ssh_user``/
  ``ssh_private_key_path``), never a module-level constant -- both DigitalOcean's
  (``root``/expanded ``id_exampleco_testing``) and tart's (``admin``/expanded
  ``id_ed25519``) real identities flow through ``command()`` into the
  ``SSHTarget`` unmodified;
- ``known_hosts`` threads from ``k3s.trust_host_keys``' Output into
  ``k3s.install``/``k3s.await_api``/``k3s.fetch_kubeconfig`` -- the EXACT
  value propagates, not just a non-empty string;
- the TOFU pair (cloud-init wait THEN keyscan) stays atomic and ordered
  end-to-end through the ``k3s.trust_host_keys`` binding -- this binding
  neither reorders nor splits it (that ordering is the provider's own job;
  this module only proves the Step layer doesn't get in the way);
- ``k3s.await_ssh``/``k3s.await_api`` issue exactly ONE probe per
  ``poll_ready`` call (never a loop), and their ``execute()`` is a true no-op
  that never touches ``ctx.services.providers`` (DR-0022 P3/Erratum E4b);
- no in-step retry loop anywhere in the family: a transient connectivity
  fault that would be masked by even one internal retry instead propagates
  on the Step's first and only attempt;
- no module-level SSH identity constant survives anywhere under
  ``seedpod/engine/steps/`` (DR-0023 point 5: no fallback identity), asserted
  structurally by inspecting the real module source/namespace, not by intent;
- an absent (``None``) identity -- the shape ``cluster.load_spec`` produces
  for ``kind``/``orbstack``, which have no SSH plane -- is never silently
  passed to ``SSHTarget``; ``command()`` raises loudly instead.
"""

from __future__ import annotations

import inspect

import pytest

from seedpod.core.acme import AcmeConfig
from seedpod.core.cluster_spec import ClusterConfiguration, ClusterSpecification, NodeSpecification
from seedpod.core.errors import PermanentError, TransientError
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepServices
from seedpod.engine.steps import k3s as k3s_module
from seedpod.engine.steps.k3s import (
    FetchKubeconfigParams,
    HostParams,
    InstallK3sParams,
    K3sAwaitApi,
    K3sAwaitReadyParams,
    K3sAwaitSsh,
    K3sFetchKubeconfig,
    K3sInstall,
    K3sTrustHostKeys,
    KnownHostsOutput,
    TrustHostKeysParams,
)
from seedpod.providers.contract import (
    CaptureHostKeys,
    FetchKubeconfig,
    InstallK3s,
    ProbeK3s,
    ProbeSshPort,
    Provider,
    ProviderCommand,
)
from tests.conformance.harness import Fault
from tests.conformance.ssh_k3s_harness import SshK3sHarness
from tests.engine.fakes import FakeSubprocessManager, make_step_context

# DR-0023's own table, v1's exact per-provider identities (config/providers/
# {digitalocean,tart}.yml, `~` already expanded by the composition root before
# these ever reach a Step -- see app/factory.py's `_ssh_identities()` and its
# own tests in tests/app/test_factory.py, which cover that expansion directly).
_DIGITALOCEAN_USER = "root"
_DIGITALOCEAN_KEY = "/home/test/.ssh/id_exampleco_testing"
_TART_USER = "admin"
_TART_KEY = "/home/test/.ssh/id_ed25519"


def _spec(*, ingress_strategy: dict | None = None) -> ClusterSpecification:
    return ClusterSpecification(
        node_specification=NodeSpecification(cpu_cores=1, memory_gb=1, region_hint="europe-west"),
        cluster_config=ClusterConfiguration(
            pod_cidr="10.42.9.0/24", service_cidr="10.43.9.0/24", ingress_strategy=ingress_strategy
        ),
    )


def _ctx(providers):
    return make_step_context(services=StepServices(subprocess_manager=FakeSubprocessManager(), providers=providers))


class _CountingProvider:
    """Wraps a real ``Provider``, counting ``execute()`` calls -- the
    Step-layer's own responsibility (issue exactly one probe per
    ``poll_ready``), decoupled from how many raw subprocess calls the
    provider's own implementation happens to make per command."""

    def __init__(self, inner: Provider) -> None:
        self._inner = inner
        self.execute_calls = 0

    def execute(self, command: ProviderCommand):
        self.execute_calls += 1
        return self._inner.execute(command)


# ---------------------------------------------------------------------------
# Declared-contract sanity (mirrors test_infra_steps.py's own such test).
# ---------------------------------------------------------------------------


def test_declares_the_dr_0022_contract_for_all_five():
    cases = [
        (K3sAwaitSsh(), "k3s.await_ssh", True, False),
        (K3sTrustHostKeys(), "k3s.trust_host_keys", False, False),
        (K3sInstall(), "k3s.install", False, False),
        (K3sAwaitApi(), "k3s.await_api", True, False),
        (K3sFetchKubeconfig(), "k3s.fetch_kubeconfig", False, False),
    ]
    for step, verb, gateable, undoable in cases:
        assert step.verb == verb
        assert step.provider_name == "ssh-k3s"
        assert step.plane == "provider"
        assert step.thin is True
        assert step.gateable is gateable
        assert step.undoable is undoable
        assert step.idempotent is True  # Step's own default; none of these five pin non-idempotent.


# ---------------------------------------------------------------------------
# DR-0023 -- no module-level SSH identity constant survives anywhere under
# seedpod/engine/steps/ (structural, not a matter of intent/naming).
# ---------------------------------------------------------------------------


def test_no_module_level_ssh_identity_constant_anywhere_in_engine_steps():
    """DR-0023 point 5: the pre-DR-0023 placeholder (``_SSH_USER``,
    ``_SSH_PRIVATE_KEY_PATH``) is deleted, not defaulted. Checked two ways:
    (1) the attributes are simply gone from the real module's namespace, and
    (2) no OTHER module under ``seedpod/engine/steps/`` reintroduced an
    equivalently-named module-level identity constant -- a source-text
    grep across the whole package, not just an attribute check on ``k3s``."""
    assert not hasattr(k3s_module, "_SSH_USER")
    assert not hasattr(k3s_module, "_SSH_PRIVATE_KEY_PATH")

    import pkgutil

    import seedpod.engine.steps as steps_package

    for _, name, _ in pkgutil.iter_modules(steps_package.__path__):
        module = __import__(f"seedpod.engine.steps.{name}", fromlist=["_"])
        source = inspect.getsource(module)
        assert "_SSH_USER" not in source, f"{name}: module-level SSH identity constant found"
        assert "_SSH_PRIVATE_KEY_PATH" not in source, f"{name}: module-level SSH identity constant found"


# ---------------------------------------------------------------------------
# k3s.await_ssh -- command mapping, true no-op execute, exactly one probe.
# Unaffected by DR-0023: ProbeSshPort needs only host/port, no SSHTarget.
# ---------------------------------------------------------------------------


def test_await_ssh_command_is_pure_and_maps_host_and_default_port():
    step = K3sAwaitSsh()
    params = HostParams(host="10.42.0.7")

    first = step.command(params)
    second = step.command(params)

    assert first == second == ProbeSshPort(host="10.42.0.7")


async def test_await_ssh_execute_is_a_noop_never_touches_providers():
    class _ExplodingProviders(dict):
        def __getitem__(self, key):
            raise AssertionError(f"execute() must never look up a provider, got {key!r}")

    step = K3sAwaitSsh()
    ctx = _ctx(_ExplodingProviders())

    output = await step.execute(HostParams(host="10.42.0.7"), ctx)

    assert isinstance(output, EmptyOutput)


class _FixedSshPortProvider:
    """Hand-written fake (CLAUDE.md testing posture: no ``Mock``/``patch``
    anywhere) standing in for a ``Provider`` that always answers
    ``ProbeSshPort`` with a fixed, controllable ``SshPortState`` -- decouples
    ``poll_ready``'s Ready/NotReady INTERPRETATION from ``ProbeSshPort``'s own
    real-TCP-dial behaviour, which ``tests/conformance/test_ssh_k3s_smoke.py``'s
    ``test_probe_ssh_port_open_and_closed`` already covers directly against a
    real loopback listener."""

    def __init__(self, *, open: bool, detail: str = "") -> None:
        self._open = open
        self._detail = detail

    def execute(self, command: ProviderCommand):
        from seedpod.providers.contract import Result, SshPortState

        async def _events():
            yield Result(SshPortState(open=self._open, detail=self._detail))

        return _events()


async def test_await_ssh_poll_ready_issues_exactly_one_probe_and_never_sleeps():
    """Uses `_FixedSshPortProvider`, not `SshK3sHarness().provider()`. The harness
    fakes the SUBPROCESS transport, but `ProbeSshPort` is not a subprocess: the real
    provider does a genuine `asyncio.open_connection(host, 22)`, so this test used to
    spend 3.00s -- 13% of the entire suite -- dialling a live RFC1918 address on
    whatever LAN the developer happens to be on (and 10.42.x is k3s's own default pod
    CIDR, so something could answer). What is under test here is the STEP's contract
    (exactly one provider.execute per poll_ready, no sleeping), which needs no real
    socket; `tests/conformance/test_ssh_k3s_smoke.py::test_probe_ssh_port_open_and_closed`
    covers the real dial against a real loopback listener. Round-8a gate finding m-6."""
    step = K3sAwaitSsh()
    counting = _CountingProvider(_FixedSshPortProvider(open=True))
    ctx = _ctx({"ssh-k3s": counting})

    result = await step.poll_ready(HostParams(host="10.42.0.7"), EmptyOutput(), ctx)

    assert counting.execute_calls == 1
    assert isinstance(result, Ready)


async def test_await_ssh_poll_ready_ready_when_port_open():
    step = K3sAwaitSsh()
    ctx = _ctx({"ssh-k3s": _FixedSshPortProvider(open=True)})

    result = await step.poll_ready(HostParams(host="10.42.0.7"), EmptyOutput(), ctx)

    assert isinstance(result, Ready)
    assert result.outputs == EmptyOutput()


async def test_await_ssh_poll_ready_not_ready_when_port_closed():
    step = K3sAwaitSsh()
    ctx = _ctx({"ssh-k3s": _FixedSshPortProvider(open=False)})

    result = await step.poll_ready(HostParams(host="10.42.0.7"), EmptyOutput(), ctx)

    assert isinstance(result, NotReady)
    assert result.detail == "ssh port not open yet"  # nothing to add when the provider is silent


async def test_await_ssh_not_ready_detail_carries_the_providers_error():
    """DR-0033: the whole point of backlog #15. A macOS Local Network denial and a VM that
    is merely still booting both collapse to ``open=False``; only this string tells them
    apart, and the gate puts it in the timeout message."""
    step = K3sAwaitSsh()
    # The exact string a denied dial produces, measured on macOS 15.7.2 rather than invented --
    # asyncio passes strerror through for errno 65 and never emits the name "EHOSTUNREACH".
    ctx = _ctx({"ssh-k3s": _FixedSshPortProvider(open=False, detail="[Errno 65] No route to host")})

    result = await step.poll_ready(HostParams(host="192.168.65.6"), EmptyOutput(), ctx)

    assert isinstance(result, NotReady)
    assert result.detail == "ssh port not open yet: [Errno 65] No route to host"


async def test_await_ssh_detail_is_diagnostic_only_and_never_decides_readiness():
    """DR-0033 pins this rather than leaving it to convention: ``open`` is the SOLE decision
    input. An open port with a detail attached is still Ready -- if this ever inverts, a
    transient diagnostic string would start failing healthy provisions."""
    step = K3sAwaitSsh()
    ctx = _ctx({"ssh-k3s": _FixedSshPortProvider(open=True, detail="[Errno 65] No route to host")})

    result = await step.poll_ready(HostParams(host="10.42.0.7"), EmptyOutput(), ctx)

    assert isinstance(result, Ready)


# ---------------------------------------------------------------------------
# k3s.trust_host_keys -- command mapping, TOFU ordering preserved, output.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ssh_user", "ssh_key"),
    [(_DIGITALOCEAN_USER, _DIGITALOCEAN_KEY), (_TART_USER, _TART_KEY)],
    ids=["digitalocean", "tart"],
)
def test_trust_host_keys_command_is_pure_and_carries_the_params_identity(ssh_user, ssh_key):
    step = K3sTrustHostKeys()
    params = TrustHostKeysParams(host="10.42.0.9", ssh_user=ssh_user, ssh_private_key_path=ssh_key)

    first = step.command(params)
    second = step.command(params)

    assert first == second
    assert isinstance(first, CaptureHostKeys)
    assert first.ssh.host == "10.42.0.9"
    assert first.ssh.user == ssh_user
    assert first.ssh.private_key_path == ssh_key


def test_trust_host_keys_command_raises_loudly_when_identity_is_none():
    """The shape ``cluster.load_spec`` produces for kind/orbstack (no SSH
    plane): DR-0023 point 5 forbids a silent fallback identity, so this must
    fail loudly rather than construct an ``SSHTarget`` with a ``None`` user."""
    step = K3sTrustHostKeys()
    params = TrustHostKeysParams(host="10.42.0.9", ssh_user=None, ssh_private_key_path=None)

    with pytest.raises(PermanentError):
        step.command(params)


async def test_trust_host_keys_execute_returns_known_hosts_and_preserves_tofu_ordering():
    step = K3sTrustHostKeys()
    harness = SshK3sHarness()
    ctx = _ctx({"ssh-k3s": harness.provider()})
    params = TrustHostKeysParams(host="10.42.0.9", ssh_user=_DIGITALOCEAN_USER, ssh_private_key_path=_DIGITALOCEAN_KEY)

    output = await step.execute(params, ctx)

    assert isinstance(output, KnownHostsOutput)
    assert "ssh-ed25519" in output.known_hosts

    # TOFU ordering (crown jewel #2): the binding must not reorder/split the
    # provider's own atomic cloud-init-then-keyscan pair -- exactly one
    # StrictHostKeyChecking=no call (the cloud-init wait), preceding the keyscan.
    calls = harness.backend.call_log
    cloud_init_idx = next(i for i, c in enumerate(calls) if "cloud-init status --wait" in c[-1])
    keyscan_idx = next(i for i, c in enumerate(calls) if c[0] == "ssh-keyscan")
    assert cloud_init_idx < keyscan_idx
    insecure_calls = [c for c in calls if "StrictHostKeyChecking=no" in c]
    assert len(insecure_calls) == 1


async def test_trust_host_keys_no_in_step_retry_loop_on_transient_fault():
    """A single TRANSIENT_ONCE fault (consumed after one attempt) must
    propagate on the Step's own first call -- an in-step retry loop would
    silently consume it and succeed instead."""
    step = K3sTrustHostKeys()
    harness = SshK3sHarness()
    ctx = _ctx({"ssh-k3s": harness.provider(Fault.TRANSIENT_ONCE)})
    params = TrustHostKeysParams(host="10.42.0.9", ssh_user=_DIGITALOCEAN_USER, ssh_private_key_path=_DIGITALOCEAN_KEY)

    with pytest.raises(TransientError):
        await step.execute(params, ctx)

    # The fault trips on the very first ssh invocation (the cloud-init wait) --
    # an in-step retry loop would consume it and reach the keyscan on a second
    # attempt; it must not.
    assert harness.backend.attempt_count == 1
    assert not any(c[0] == "ssh-keyscan" for c in harness.backend.call_log)


# ---------------------------------------------------------------------------
# k3s.install -- known_hosts consumption, CIDRs, tls_sans, ingress translation.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ssh_user", "ssh_key"),
    [(_DIGITALOCEAN_USER, _DIGITALOCEAN_KEY), (_TART_USER, _TART_KEY)],
    ids=["digitalocean", "tart"],
)
def test_install_command_is_pure_and_maps_all_fields(ssh_user, ssh_key):
    step = K3sInstall()
    params = InstallK3sParams(
        host="10.42.0.11",
        spec=_spec(ingress_strategy={"type": "traefik"}),
        extra_tls_san="10.42.0.11",
        known_hosts="10.42.0.11 ssh-ed25519 AAAA\n",
        ssh_user=ssh_user,
        ssh_private_key_path=ssh_key,
    )

    first = step.command(params)
    second = step.command(params)

    assert first == second
    assert isinstance(first, InstallK3s)
    assert first.ssh.host == "10.42.0.11"
    assert first.ssh.user == ssh_user
    assert first.ssh.private_key_path == ssh_key
    assert first.known_hosts == "10.42.0.11 ssh-ed25519 AAAA\n"
    assert first.pod_cidr == "10.42.9.0/24"
    assert first.service_cidr == "10.43.9.0/24"
    assert first.tls_sans == ("10.42.0.11",)


def test_install_command_raises_loudly_when_identity_is_none():
    step = K3sInstall()
    params = InstallK3sParams(
        host="h", spec=_spec(), extra_tls_san="h", known_hosts="known", ssh_user=None, ssh_private_key_path=None
    )

    with pytest.raises(PermanentError):
        step.command(params)


@pytest.mark.parametrize(
    ("ingress_strategy", "expected_type", "expected_enabled", "expected_expose"),
    [
        (None, "none", True, "loadbalancer"),
        ({"type": "traefik"}, "traefik", True, "loadbalancer"),
        ({"type": "traefik", "traefik": {"enabled": False}}, "traefik", False, "loadbalancer"),
        (
            {"type": "traefik", "traefik": {"enabled": True, "expose_method": "hostport"}},
            "traefik",
            True,
            "hostport",
        ),
    ],
)
def test_install_translates_ingress_strategy_dict_to_ingress_config(
    ingress_strategy, expected_type, expected_enabled, expected_expose
):
    step = K3sInstall()
    params = InstallK3sParams(
        host="h",
        spec=_spec(ingress_strategy=ingress_strategy),
        extra_tls_san="h",
        known_hosts="known",
        ssh_user=_DIGITALOCEAN_USER,
        ssh_private_key_path=_DIGITALOCEAN_KEY,
    )

    cmd = step.command(params)

    assert cmd.ingress.ingress_type == expected_type
    assert cmd.ingress.enabled is expected_enabled
    assert cmd.ingress.expose_method == expected_expose


def test_install_raises_loudly_when_spec_missing_cidrs():
    step = K3sInstall()
    spec = ClusterSpecification(
        node_specification=NodeSpecification(cpu_cores=1, memory_gb=1, region_hint="europe-west"),
        cluster_config=ClusterConfiguration(),  # pod_cidr/service_cidr left None
    )
    params = InstallK3sParams(
        host="h",
        spec=spec,
        extra_tls_san="h",
        known_hosts="known",
        ssh_user=_DIGITALOCEAN_USER,
        ssh_private_key_path=_DIGITALOCEAN_KEY,
    )

    with pytest.raises(PermanentError):
        step.command(params)


async def test_install_execute_consumes_known_hosts_from_trust_host_keys_and_installs():
    """known_hosts threads verbatim (coherence-review.md Conflict 14): the
    EXACT value trust_host_keys captured is what install's provider call
    receives -- not merely a non-empty string."""
    trust_step = K3sTrustHostKeys()
    install_step = K3sInstall()
    harness = SshK3sHarness()
    ctx = _ctx({"ssh-k3s": harness.provider()})

    trust_output = await trust_step.execute(
        TrustHostKeysParams(host="10.42.0.11", ssh_user=_DIGITALOCEAN_USER, ssh_private_key_path=_DIGITALOCEAN_KEY),
        ctx,
    )
    install_params = InstallK3sParams(
        host="10.42.0.11",
        spec=_spec(ingress_strategy={"type": "traefik"}),
        extra_tls_san="10.42.0.11",
        known_hosts=trust_output.known_hosts,
        ssh_user=_DIGITALOCEAN_USER,
        ssh_private_key_path=_DIGITALOCEAN_KEY,
    )

    assert install_step.command(install_params).known_hosts == trust_output.known_hosts

    output = await install_step.execute(install_params, ctx)

    assert isinstance(output, EmptyOutput)
    assert harness.backend.install_flags_seen is not None
    assert "--cluster-cidr=10.42.9.0/24" in harness.backend.install_flags_seen
    assert "--service-cidr=10.43.9.0/24" in harness.backend.install_flags_seen
    assert "--tls-san=10.42.0.11" in harness.backend.install_flags_seen


async def test_install_rejects_empty_known_hosts_install_before_keys_is_unrepresentable():
    step = K3sInstall()
    harness = SshK3sHarness()
    ctx = _ctx({"ssh-k3s": harness.provider()})
    params = InstallK3sParams(
        host="10.42.0.11",
        spec=_spec(),
        extra_tls_san="10.42.0.11",
        known_hosts="",
        ssh_user=_DIGITALOCEAN_USER,
        ssh_private_key_path=_DIGITALOCEAN_KEY,
    )

    with pytest.raises(PermanentError):
        await step.execute(params, ctx)


async def test_install_no_in_step_retry_loop_on_transient_fault():
    step = K3sInstall()
    harness = SshK3sHarness()
    ctx = _ctx({"ssh-k3s": harness.provider(Fault.TRANSIENT_ONCE)})
    params = InstallK3sParams(
        host="10.42.0.11",
        spec=_spec(),
        extra_tls_san="10.42.0.11",
        known_hosts="known-hosts",
        ssh_user=_DIGITALOCEAN_USER,
        ssh_private_key_path=_DIGITALOCEAN_KEY,
    )

    with pytest.raises(TransientError):
        await step.execute(params, ctx)

    # Exactly ONE attempt was made (the faulted one) -- an in-step retry loop
    # would consume the fault and reach a second, succeeding attempt instead.
    assert harness.backend.attempt_count == 1
    assert harness.backend.install_flags_seen is None, "the faulted attempt must never have succeeded"


# ---------------------------------------------------------------------------
# k3s.await_api -- command mapping, true no-op execute, exactly one probe.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ssh_user", "ssh_key"),
    [(_DIGITALOCEAN_USER, _DIGITALOCEAN_KEY), (_TART_USER, _TART_KEY)],
    ids=["digitalocean", "tart"],
)
def test_await_api_command_is_pure_and_maps_host_and_known_hosts(ssh_user, ssh_key):
    step = K3sAwaitApi()
    params = K3sAwaitReadyParams(
        host="10.42.0.13", known_hosts="10.42.0.13 ssh-ed25519 AAAA\n", ssh_user=ssh_user, ssh_private_key_path=ssh_key
    )

    first = step.command(params)
    second = step.command(params)

    assert first == second
    assert isinstance(first, ProbeK3s)
    assert first.ssh.host == "10.42.0.13"
    assert first.ssh.user == ssh_user
    assert first.ssh.private_key_path == ssh_key
    assert first.known_hosts == "10.42.0.13 ssh-ed25519 AAAA\n"


def test_await_api_command_raises_loudly_when_identity_is_none():
    step = K3sAwaitApi()
    params = K3sAwaitReadyParams(host="h", known_hosts="known", ssh_user=None, ssh_private_key_path=None)

    with pytest.raises(PermanentError):
        step.command(params)


async def test_await_api_execute_is_a_noop_never_touches_providers():
    class _ExplodingProviders(dict):
        def __getitem__(self, key):
            raise AssertionError(f"execute() must never look up a provider, got {key!r}")

    step = K3sAwaitApi()
    ctx = _ctx(_ExplodingProviders())
    params = K3sAwaitReadyParams(
        host="10.42.0.13", known_hosts="known-hosts", ssh_user=_DIGITALOCEAN_USER, ssh_private_key_path=_DIGITALOCEAN_KEY
    )

    output = await step.execute(params, ctx)

    assert isinstance(output, EmptyOutput)


async def test_await_api_poll_ready_issues_exactly_one_probe_and_never_sleeps():
    harness = SshK3sHarness()
    counting = _CountingProvider(harness.provider())
    step = K3sAwaitApi()
    ctx = _ctx({"ssh-k3s": counting})
    params = K3sAwaitReadyParams(
        host="10.42.0.13", known_hosts="known-hosts", ssh_user=_DIGITALOCEAN_USER, ssh_private_key_path=_DIGITALOCEAN_KEY
    )

    result = await step.poll_ready(params, EmptyOutput(), ctx)

    assert counting.execute_calls == 1
    assert isinstance(result, Ready)


async def test_await_api_not_ready_while_k3s_not_yet_active():
    harness = SshK3sHarness()
    harness.backend.k3s_active = False
    step = K3sAwaitApi()
    ctx = _ctx({"ssh-k3s": harness.provider()})
    params = K3sAwaitReadyParams(
        host="10.42.0.13", known_hosts="known-hosts", ssh_user=_DIGITALOCEAN_USER, ssh_private_key_path=_DIGITALOCEAN_KEY
    )

    result = await step.poll_ready(params, EmptyOutput(), ctx)

    assert isinstance(result, NotReady)


async def test_await_api_not_ready_when_active_but_api_down():
    """The crown jewel: active-but-API-down is distinct from not-active-yet."""
    harness = SshK3sHarness()
    harness.backend.k3s_active = True
    harness.backend.k3s_api_ready = False
    step = K3sAwaitApi()
    ctx = _ctx({"ssh-k3s": harness.provider()})
    params = K3sAwaitReadyParams(
        host="10.42.0.13", known_hosts="known-hosts", ssh_user=_DIGITALOCEAN_USER, ssh_private_key_path=_DIGITALOCEAN_KEY
    )

    result = await step.poll_ready(params, EmptyOutput(), ctx)

    assert isinstance(result, NotReady)
    assert "not responding" in result.detail


# ---------------------------------------------------------------------------
# k3s.fetch_kubeconfig -- known_hosts consumption, rewrite, command mapping.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ssh_user", "ssh_key"),
    [(_DIGITALOCEAN_USER, _DIGITALOCEAN_KEY), (_TART_USER, _TART_KEY)],
    ids=["digitalocean", "tart"],
)
def test_fetch_kubeconfig_command_is_pure_and_maps_all_fields(ssh_user, ssh_key):
    step = K3sFetchKubeconfig()
    params = FetchKubeconfigParams(
        host="10.42.0.15",
        rewrite_server_to="cluster.example.internal",
        known_hosts="known-hosts",
        ssh_user=ssh_user,
        ssh_private_key_path=ssh_key,
    )

    first = step.command(params)
    second = step.command(params)

    assert first == second
    assert isinstance(first, FetchKubeconfig)
    assert first.ssh.host == "10.42.0.15"
    assert first.ssh.user == ssh_user
    assert first.ssh.private_key_path == ssh_key
    assert first.known_hosts == "known-hosts"
    assert first.rewrite_server_to == "cluster.example.internal"


def test_fetch_kubeconfig_command_raises_loudly_when_identity_is_none():
    step = K3sFetchKubeconfig()
    params = FetchKubeconfigParams(
        host="h", rewrite_server_to="h", known_hosts="known", ssh_user=None, ssh_private_key_path=None
    )

    with pytest.raises(PermanentError):
        step.command(params)


async def test_fetch_kubeconfig_execute_consumes_known_hosts_and_rewrites_server():
    trust_step = K3sTrustHostKeys()
    fetch_step = K3sFetchKubeconfig()
    harness = SshK3sHarness()
    ctx = _ctx({"ssh-k3s": harness.provider()})

    trust_output = await trust_step.execute(
        TrustHostKeysParams(host="10.42.0.15", ssh_user=_DIGITALOCEAN_USER, ssh_private_key_path=_DIGITALOCEAN_KEY),
        ctx,
    )
    fetch_params = FetchKubeconfigParams(
        host="10.42.0.15",
        rewrite_server_to="cluster.example.internal",
        known_hosts=trust_output.known_hosts,
        ssh_user=_DIGITALOCEAN_USER,
        ssh_private_key_path=_DIGITALOCEAN_KEY,
    )

    assert fetch_step.command(fetch_params).known_hosts == trust_output.known_hosts

    output = await fetch_step.execute(fetch_params, ctx)

    assert output.kubeconfig.get_secret_value()
    assert "cluster.example.internal" in output.kubeconfig.get_secret_value()


async def test_fetch_kubeconfig_no_in_step_retry_loop_on_transient_fault():
    step = K3sFetchKubeconfig()
    harness = SshK3sHarness()
    ctx = _ctx({"ssh-k3s": harness.provider(Fault.TRANSIENT_ONCE)})
    params = FetchKubeconfigParams(
        host="10.42.0.15",
        rewrite_server_to="host",
        known_hosts="known-hosts",
        ssh_user=_DIGITALOCEAN_USER,
        ssh_private_key_path=_DIGITALOCEAN_KEY,
    )

    with pytest.raises(TransientError):
        await step.execute(params, ctx)


# ---------------------------------------------------------------------------
# DR-0036 — acme must survive the Params -> IngressConfig translation.
# ---------------------------------------------------------------------------


def _install_params(*, acme, ingress_strategy=None):
    return InstallK3sParams(
        host="h",
        spec=_spec(ingress_strategy=ingress_strategy if ingress_strategy is not None else {"type": "traefik"}),
        extra_tls_san="h",
        known_hosts="known",
        ssh_user=_DIGITALOCEAN_USER,
        ssh_private_key_path=_DIGITALOCEAN_KEY,
        acme=acme,
    )


def test_install_threads_acme_into_the_ingress_config():
    """**Smoke 12's regression test.** `_ingress_for` gained an `acme` parameter and
    never passed it into `IngressConfig`, so the certresolver silently never reached
    the provider and Traefik served its default cert on a real cluster -- with every
    other layer (profile, provider_config, load_spec output, step params, manifest
    builder) verified correct and green.

    The DR-0036 tests all constructed `IngressConfig(acme=...)` by hand and drove the
    PROVIDER, so they proved the manifest renders and never that anything reaches it.
    That is the "pins the decision, misses the consequence" shape this repo already
    knows (backlog #13): test the seam, not just the two things it joins."""
    acme = AcmeConfig(email="kezia@example.com")

    cmd = K3sInstall().command(_install_params(acme=acme))

    assert cmd.ingress.acme == acme


def test_install_carries_no_acme_when_the_profile_had_none():
    cmd = K3sInstall().command(_install_params(acme=None))
    assert cmd.ingress.acme is None


def test_install_drops_acme_when_ingress_is_not_traefik():
    """No traefik, no certresolver to configure -- carrying it into a config nothing
    reads would be a lie in the command."""
    cmd = K3sInstall().command(
        _install_params(acme=AcmeConfig(email="a@b.c"), ingress_strategy={"type": "nodeport"})
    )
    assert cmd.ingress.acme is None
