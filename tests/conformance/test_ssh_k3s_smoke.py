"""tests/conformance/test_ssh_k3s_smoke.py — smoke coverage proving the ``ssh-k3s`` provider
streams per Seam C §5.2 against its fake transport, and that ``SshK3sHarness`` is wired
correctly. The full parametrized C-01..C-24 suite is written by a later agent against
``tests/conformance/harness.Harness``; this file is a narrower, provider-local proof (stream
shape, TOFU ordering, the known_hosts="" rejection, ProbeK3s's active-but-API-down carve-out,
kubeconfig rewrite, classification table, unsupported-command rejection) so that agent's suite
has a known-good provider to slot in against.

No ``Mock``/``patch`` anywhere — every fault is injected at ``FakeSshTransport``.
"""

from __future__ import annotations

import asyncio
import errno
import re

import pytest

from seedpod.core.acme import AcmeConfig
from seedpod.core.errors import PermanentError, TransientError
from seedpod.providers.contract import (
    FetchKubeconfig,
    HostKeys,
    IngressConfig,
    InstallK3s,
    K3sInstalled,
    K3sReadiness,
    Kubeconfig,
    ListInstances,
    ProbeSshPort,
    Progress,
    Result,
    SshPortState,
    SubprocessResult,
)
from seedpod.providers.ssh_k3s import SshK3sConfig, SshK3sProvider
from tests.conformance.harness import Fault
from tests.conformance.ssh_k3s_harness import SshK3sHarness

pytestmark = pytest.mark.asyncio


async def _drain(provider, cmd):
    events = []
    async for ev in provider.execute(cmd):
        events.append(ev)
    return events


# ---------------------------------------------------------------------------
# check_ready / C-01
# ---------------------------------------------------------------------------


async def test_check_ready_succeeds_against_healthy_backend():
    harness = SshK3sHarness()
    provider = harness.provider()
    await provider.check_ready()  # must not raise


async def test_check_ready_fails_fast_on_broken_environment():
    harness = SshK3sHarness()
    with harness.broken_environment() as provider:
        with pytest.raises(PermanentError) as excinfo:
            await provider.check_ready()
        assert excinfo.value.code == "not_found"


# ---------------------------------------------------------------------------
# stream shape / C-02
# ---------------------------------------------------------------------------


async def test_capture_host_keys_stream_shape_result_only():
    harness = SshK3sHarness()
    provider = harness.provider()
    events = await _drain(provider, harness.capture_host_keys_command())
    assert len(events) == 1
    assert isinstance(events[0], Result)
    assert isinstance(events[0].value, HostKeys)
    assert "ssh-ed25519" in events[0].value.known_hosts


async def test_install_k3s_stream_shape_progress_then_result():
    harness = SshK3sHarness()
    provider = harness.provider()
    events = await _drain(provider, harness.install_k3s_command())

    assert events
    *progress_events, terminal = events
    assert all(isinstance(ev, Progress) for ev in progress_events)
    assert isinstance(terminal, Result)
    assert isinstance(terminal.value, K3sInstalled)


async def test_probe_k3s_stream_shape_result_only():
    harness = SshK3sHarness()
    provider = harness.provider()
    events = await _drain(provider, harness.probe_k3s_command())
    assert len(events) == 1
    assert isinstance(events[0], Result)
    assert isinstance(events[0].value, K3sReadiness)


async def test_probe_ssh_port_open_and_closed():
    async def _handle(reader, writer):
        writer.close()

    server = await asyncio.start_server(_handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        harness = SshK3sHarness()
        provider = harness.provider()

        (open_result,) = await _drain(provider, ProbeSshPort(host="127.0.0.1", port=port))
        assert isinstance(open_result.value, SshPortState)
        assert open_result.value.open is True
        assert open_result.value.detail == ""  # DR-0033: nothing to explain when it opened

        (closed_result,) = await _drain(provider, ProbeSshPort(host="127.0.0.1", port=1))
        assert closed_result.value.open is False
        # DR-0033: the collapse to open=False is unchanged, but the errno no longer
        # vanishes -- this is the difference between "gate timed out after 180.0s" and a
        # message naming the actual refusal. Asserted against a REAL refused dial (no
        # Mock/patch -- fault injection lives at the transport seam), so it also pins that
        # the OSError branch produces a non-empty, errno-bearing string.
        assert closed_result.value.detail != ""
        # Assert the ERRNO, not prose. `asyncio.open_connection` rewrites the OSError into
        # "[Errno 61] Connect call failed ('127.0.0.1', 1)" -- it does NOT say "refused" --
        # so the number is the part that actually identifies the failure, and it is exactly
        # what distinguishes ECONNREFUSED (61, still booting) from EHOSTUNREACH (65, the
        # macOS Local Network denial of backlog #15).
        assert f"[Errno {errno.ECONNREFUSED}]" in closed_result.value.detail
    finally:
        server.close()
        await server.wait_closed()


# ---------------------------------------------------------------------------
# TOFU ordering / C-20 (crown jewel #2)
# ---------------------------------------------------------------------------


async def test_tofu_ordering_cloud_init_before_keyscan():
    harness = SshK3sHarness()
    provider = harness.provider()
    await _drain(provider, harness.capture_host_keys_command())

    calls = harness.backend.call_log
    cloud_init_idx = next(i for i, c in enumerate(calls) if "cloud-init status --wait" in c[-1])
    keyscan_idx = next(i for i, c in enumerate(calls) if c[0] == "ssh-keyscan")
    assert cloud_init_idx < keyscan_idx

    # The cloud-init call is the ONLY StrictHostKeyChecking=no invocation.
    insecure_calls = [c for c in calls if "StrictHostKeyChecking=no" in c]
    assert len(insecure_calls) == 1
    assert insecure_calls[0] == calls[cloud_init_idx]


async def test_install_known_hosts_empty_rejected_zero_backend_traffic():
    harness = SshK3sHarness()
    provider = harness.provider()
    before = harness.backend_attempts()

    with pytest.raises(PermanentError) as excinfo:
        await _drain(provider, harness.install_k3s_command(known_hosts=""))
    assert excinfo.value.code == "invalid_input"
    assert harness.backend_attempts() == before


async def test_post_capture_ssh_uses_strict_checking():
    harness = SshK3sHarness()
    provider = harness.provider()
    await _drain(provider, harness.install_k3s_command())

    calls = harness.backend.call_log
    strict_calls = [c for c in calls if c[0] == "ssh" and len(c) > 2 and "-i" in c]
    assert strict_calls, "expected at least one strict ssh invocation"
    for call in strict_calls:
        assert "StrictHostKeyChecking=yes" in call
        assert "StrictHostKeyChecking=no" not in call


# ---------------------------------------------------------------------------
# host-keys-pending / row 18
# ---------------------------------------------------------------------------


async def test_keyscan_empty_output_is_transient_host_keys_pending():
    harness = SshK3sHarness()
    harness.backend.host_keys_available = False
    provider = harness.provider()

    with pytest.raises(TransientError) as excinfo:
        await _drain(provider, harness.capture_host_keys_command())
    assert excinfo.value.code == "host_keys_pending"


# ---------------------------------------------------------------------------
# ProbeK3s active-but-API-down / row 19
# ---------------------------------------------------------------------------


async def test_probe_k3s_not_active_yet():
    harness = SshK3sHarness()
    harness.backend.k3s_active = False
    provider = harness.provider()

    (result,) = await _drain(provider, harness.probe_k3s_command())
    assert result.value.ready is False
    assert "not active" in result.value.detail


async def test_probe_k3s_active_but_api_down_is_result_not_raise():
    harness = SshK3sHarness()
    harness.backend.k3s_active = True
    harness.backend.k3s_api_ready = False
    provider = harness.provider()

    (result,) = await _drain(provider, harness.probe_k3s_command())
    assert result.value.ready is False
    assert "API not responding" in result.value.detail


async def test_probe_k3s_ready():
    harness = SshK3sHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, harness.probe_k3s_command())
    assert result.value.ready is True


# ---------------------------------------------------------------------------
# install_k3s flag construction — salvaged verbatim
# ---------------------------------------------------------------------------


async def test_install_k3s_flags_verbatim():
    harness = SshK3sHarness()
    provider = harness.provider()
    await _drain(provider, harness.install_k3s_command())

    cmd = harness.backend.install_flags_seen
    assert cmd is not None
    assert "--write-kubeconfig-mode=644" in cmd
    assert "--cluster-cidr=10.42.7.0/24" in cmd
    assert "--service-cidr=10.43.7.0/24" in cmd
    assert "--tls-san=10.42.0.7" in cmd
    assert "--disable=servicelb" in cmd
    assert "--disable=traefik" not in cmd  # traefik loadbalancer: keep enabled


async def test_install_k3s_disables_traefik_when_ingress_none():
    harness = SshK3sHarness()
    provider = harness.provider()

    cmd = harness.install_k3s_command()
    cmd = InstallK3s(
        ssh=cmd.ssh, known_hosts=cmd.known_hosts, pod_cidr=cmd.pod_cidr, service_cidr=cmd.service_cidr,
        tls_sans=cmd.tls_sans, ingress=IngressConfig(ingress_type="none"),
    )
    await _drain(provider, cmd)
    assert "--disable=traefik" in harness.backend.install_flags_seen


async def test_install_k3s_hostport_pre_creates_helmchartconfig_before_install():
    harness = SshK3sHarness()
    provider = harness.provider()

    base = harness.install_k3s_command()
    cmd = InstallK3s(
        ssh=base.ssh, known_hosts=base.known_hosts, pod_cidr=base.pod_cidr, service_cidr=base.service_cidr,
        tls_sans=base.tls_sans,
        ingress=IngressConfig(ingress_type="traefik", enabled=True, expose_method="hostport"),
    )
    await _drain(provider, cmd)

    assert harness.backend.traefik_hostport_written is True
    calls = harness.backend.call_log
    traefik_idx = next(i for i, c in enumerate(calls) if "traefik-config.yaml" in c[-1])
    install_idx = next(i for i, c in enumerate(calls) if "INSTALL_K3S_EXEC" in c[-1])
    assert traefik_idx < install_idx, "HelmChartConfig must be written BEFORE k3s installs"


# ---------------------------------------------------------------------------
# fetch_kubeconfig / C-19 rewrite golden cases (crown jewel #6)
# ---------------------------------------------------------------------------


async def test_fetch_kubeconfig_rewrite_cases():
    harness = SshK3sHarness()
    provider = harness.provider()
    for name, cmd, expected_pattern in harness.rewrite_cases():
        (result,) = await _drain(provider, cmd)
        assert isinstance(result.value, Kubeconfig), name
        assert re.search(expected_pattern, result.value.yaml_text), name


async def test_fetch_kubeconfig_requires_ssh_and_known_hosts():
    harness = SshK3sHarness()
    provider = harness.provider()
    with pytest.raises(PermanentError) as excinfo:
        await _drain(provider, FetchKubeconfig(rewrite_server_to="x"))
    assert excinfo.value.code == "invalid_input"


# ---------------------------------------------------------------------------
# unsupported command / C-24
# ---------------------------------------------------------------------------


async def test_unsupported_command_rejected_with_zero_backend_traffic():
    harness = SshK3sHarness()
    provider = harness.provider()
    before = harness.backend_attempts()
    with pytest.raises(PermanentError) as excinfo:
        provider.execute(ListInstances())
    assert excinfo.value.code == "unsupported"
    assert harness.backend_attempts() == before


# ---------------------------------------------------------------------------
# classification table / C-17
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fault,expected_cls,expected_code",
    SshK3sHarness().classification_cases(),
    ids=lambda v: v.value if isinstance(v, Fault) else str(v),
)
async def test_classification_table(fault, expected_cls, expected_code):
    harness = SshK3sHarness()
    provider = harness.provider(fault)
    with pytest.raises(expected_cls) as excinfo:
        await _drain(provider, harness.capture_host_keys_command())
    assert excinfo.value.code == expected_code


# ---------------------------------------------------------------------------
# single attempt, no internal retry / C-15
# ---------------------------------------------------------------------------


async def test_single_attempt_no_internal_retry_then_succeeds_on_reinvocation():
    # CaptureHostKeys (not ProbeK3s — a gate-poll command that never raises by design, see
    # ssh_k3s.py's _probe_k3s docstring) is the command that legitimately raises on a
    # connectivity fault (row 16).
    harness = SshK3sHarness()
    provider = harness.provider(Fault.TRANSIENT_ONCE)

    with pytest.raises(TransientError):
        await _drain(provider, harness.capture_host_keys_command())
    assert harness.backend_attempts() == 1  # exactly one transport attempt, no internal retry loop

    # Second execution (fault already consumed) succeeds.
    (result,) = await _drain(provider, harness.capture_host_keys_command())
    assert isinstance(result.value, HostKeys)


# ---------------------------------------------------------------------------
# sshd-up-but-not-ready: ssh's own exit 255 is a transport failure, not an answer
# (found by smoke 8, 2026-08-09)
# ---------------------------------------------------------------------------


class _FixedResultTransport:
    """Hand-written transport (CLAUDE.md: fault injection lives at the transport seam,
    never ``Mock``/``patch``) that answers every invocation with one canned
    ``SubprocessResult`` -- the shape a real sshd produces mid-restart, which the
    enum-driven ``Fault`` set has no member for."""

    def __init__(self, *, returncode: int, stderr: bytes) -> None:
        self._returncode = returncode
        self._stderr = stderr
        self.calls = 0

    async def run(self, argv, *, timeout=None, cluster_id=None):  # noqa: ANN001, ARG002
        self.calls += 1
        return SubprocessResult(returncode=self._returncode, stdout=b"", stderr=self._stderr)


@pytest.mark.parametrize(
    "stderr",
    [
        b"kex_exchange_identification: Connection closed by remote host",
        b"Connection closed by 10.42.0.7 port 22",
        b"Connection reset by 10.42.0.7 port 22",
        b"ssh: connect to host 10.42.0.7 port 22: Operation timed out",
    ],
    ids=["kex", "closed", "reset", "op-timeout"],
)
async def test_cloud_init_wait_treats_ssh_transport_failure_as_transient(stderr):
    """Smoke 8 (2026-08-09): a droplet whose sshd blipped between `k3s.await_ssh` (a bare
    TCP dial, which had just passed) and the cloud-init wait failed the WHOLE provision
    PERMANENTLY -- so `trust_host`'s `retry: ssh_default` never fired and compensation
    destroyed the droplet. An immediate re-run provisioned fine, proving it transient.

    `_run_insecure`'s remote command is `cloud-init status --wait || true`, which cannot
    exit non-zero, so a non-zero rc is always ssh's own failure and never an authoritative
    answer for `classify_subprocess`'s clean-non-zero-exit rule to report.
    """
    transport = _FixedResultTransport(returncode=255, stderr=stderr)
    provider = SshK3sProvider(SshK3sConfig(), transport)

    with pytest.raises(TransientError):
        await _drain(provider, SshK3sHarness().capture_host_keys_command())
    assert transport.calls == 1  # still exactly one attempt -- the engine owns retry


async def test_cloud_init_wait_keeps_auth_failure_permanent():
    """The other half, and the reason this is not a blanket "255 is transient" rule: ssh
    uses 255 for a rejected key too, and no number of retries fixes a wrong key. The
    conformance classification table pins `Fault.AUTH => PermanentError` for exactly this.
    """
    transport = _FixedResultTransport(returncode=255, stderr=b"Permission denied (publickey).")
    provider = SshK3sProvider(SshK3sConfig(), transport)

    with pytest.raises(PermanentError):
        await _drain(provider, SshK3sHarness().capture_host_keys_command())


# ---------------------------------------------------------------------------
# DR-0036 — the ACME certresolver rides the SAME HelmChartConfig, before install.
# ---------------------------------------------------------------------------


def _install_with(harness, *, expose_method: str, acme):
    base = harness.install_k3s_command()
    return InstallK3s(
        ssh=base.ssh, known_hosts=base.known_hosts, pod_cidr=base.pod_cidr, service_cidr=base.service_cidr,
        tls_sans=base.tls_sans,
        ingress=IngressConfig(
            ingress_type="traefik", enabled=True, expose_method=expose_method, acme=acme
        ),
    )


async def test_acme_resolver_is_written_into_the_hostport_config_before_install():
    """DR-0036 decision 1. v1 wrote a SECOND, competing HelmChartConfig for the same
    object at the DEPLOYING transition (`_apply_traefik_config`); v2 keeps one writer,
    landing before Traefik's initial install so there is no reconfigure-and-restart."""
    harness = SshK3sHarness()
    acme = AcmeConfig(email="kezia@example.com", server="https://acme-staging-v02.api.letsencrypt.org/directory")
    await _drain(harness.provider(), _install_with(harness, expose_method="hostport", acme=acme))

    manifest = harness.backend.traefik_manifest
    assert manifest is not None
    # both halves, one document
    assert "hostPort: 443" in manifest
    assert "certificatesResolvers:" in manifest
    assert "letsencrypt:" in manifest
    assert "email: kezia@example.com" in manifest
    assert "caServer: https://acme-staging-v02.api.letsencrypt.org/directory" in manifest
    assert "storage: /data/acme.json" in manifest
    assert "httpChallenge:" in manifest and "entryPoint: web" in manifest
    # and still before the installer runs
    calls = harness.backend.call_log
    traefik_idx = next(i for i, c in enumerate(calls) if "traefik-config.yaml" in c[-1])
    install_idx = next(i for i, c in enumerate(calls) if "INSTALL_K3S_EXEC" in c[-1])
    assert traefik_idx < install_idx


async def test_the_resolver_name_is_the_one_the_ingress_annotations_reference():
    """The templates render `router.tls.certresolver: letsencrypt`. If this name ever
    drifts, every Ingress silently falls back to Traefik's default cert -- which IS
    backlog #24."""
    harness = SshK3sHarness()
    await _drain(
        harness.provider(),
        _install_with(harness, expose_method="hostport", acme=AcmeConfig(email="a@b.c")),
    )
    assert "      letsencrypt:" in harness.backend.traefik_manifest


async def test_tls_challenge_renders_v1s_other_branch():
    harness = SshK3sHarness()
    acme = AcmeConfig(email="a@b.c", challenge="tlsChallenge")
    await _drain(harness.provider(), _install_with(harness, expose_method="hostport", acme=acme))

    manifest = harness.backend.traefik_manifest
    assert "tlsChallenge: {}" in manifest
    assert "httpChallenge" not in manifest


async def test_hostport_without_acme_is_byte_identical_to_before():
    """Regression guard for every profile that does NOT enable both blocks -- the two
    shipped `-nodns` ones deliberately want Traefik's self-signed cert."""
    harness = SshK3sHarness()
    await _drain(harness.provider(), _install_with(harness, expose_method="hostport", acme=None))

    manifest = harness.backend.traefik_manifest
    assert "hostPort: 80" in manifest
    assert "certificatesResolvers" not in manifest
    assert "acme" not in manifest


async def test_loadbalancer_with_acme_still_gets_a_resolver_but_no_ports_block():
    """DR-0036 decision 4: the writer used to fire on `hostport` ALONE, so a
    loadbalancer profile with ssl+dns would render certresolver annotations and get no
    resolver -- #24 one branch over. The ports/service block stays gated on hostport so
    a loadbalancer profile's service type is untouched."""
    harness = SshK3sHarness()
    await _drain(
        harness.provider(),
        _install_with(harness, expose_method="loadbalancer", acme=AcmeConfig(email="a@b.c")),
    )

    manifest = harness.backend.traefik_manifest
    assert manifest is not None
    assert "certificatesResolvers:" in manifest
    assert "hostPort" not in manifest
    assert "type: ClusterIP" not in manifest


async def test_loadbalancer_without_acme_writes_nothing_at_all():
    """Unchanged behaviour: nothing to configure, so no manifest."""
    harness = SshK3sHarness()
    await _drain(harness.provider(), _install_with(harness, expose_method="loadbalancer", acme=None))

    assert harness.backend.traefik_hostport_written is False
    assert harness.backend.traefik_manifest is None
