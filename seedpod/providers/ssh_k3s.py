"""seedpod/providers/ssh_k3s.py — the ``ssh-k3s`` Provider (Seam C §5.3-5.4, decision-table
rows 16-19, amended by ``docs/design/coherence-review.md`` Conflicts 5-7, 12).

The k3s plane (``ProbeSshPort | CaptureHostKeys | InstallK3s | ProbeK3s | FetchKubeconfig``) —
the shared SSH + K3s installation helper any real-VM provider (DigitalOcean, Tart) delegates to
once a machine has an address. Talks to the remote host exclusively over ``ssh``/``ssh-keyscan``
through an **injected** ``SubprocessRunner`` (§5.4's construction contract) — no
``create_tracked_subprocess`` call inside this module; that lives behind the transport the
composition root wires up.

Salvaged from ``reference-code/seedpod/seedpod/providers/_ssh_k3s_installer.py``
(``SSHBasedK3sInstaller``):

- ``ensure_host_keys`` (lines 160-181) + ``_ssh_keyscan`` (183-221) + ``_run_insecure_ssh``
  (223-269) → ``_capture_host_keys`` below. **TOFU ordering is the crown jewel (#2), preserved
  exactly**: the cloud-init wait is the *only* ``StrictHostKeyChecking=no`` invocation, and it
  always runs before the keyscan — both inside one bounded ``CaptureHostKeys.execute()`` call, no
  interleaving with any other host operation is possible because both steps share one command.
- ``install_k3s`` (441-493) + ``create_traefik_hostport_config`` (532-581) → ``_install_k3s``
  below, salvaged **verbatim**: ``--write-kubeconfig-mode=644``, ``--disable=servicelb``, CIDR
  flags, ``--tls-san`` per SAN, the traefik disable/hostport branching, and the HelmChartConfig
  base64-over-SSH write happening *before* the k3s installer runs (so Traefik picks up the
  HostPort override on its very first install). One deviation from byte-identical salvage: v1's
  ``traefik_cfg.get("http_port", 80)``/``https_port`` came from a provider-YAML dict that
  ``IngressConfig`` (§5.3) has no field for — the 80/443 defaults are hardcoded here rather than
  configurable, since that YAML shape does not survive into the v2 contract.
- ``execute`` (275-384) → ``_run_strict`` below, with the internal ``retry_attempts``/
  ``retry_delay`` loop **stripped** per the task's explicit instruction — the engine's
  ``ssh_default`` ``Schedule`` now owns that retry (H4-H6). One bounded attempt per call; a
  connectivity symptom raises ``TransientError`` (row 16) for the *engine* to retry, not this
  module to loop on internally.
- ``get_kubeconfig`` (390-435) → ``_fetch_kubeconfig`` below: ``sudo cat
  /etc/rancher/k3s/k3s.yaml`` over strict SSH, YAML-parsed **in memory** (crown jewel #6 — never
  sed-over-SSH), the scheme+port-preserving ``re.sub`` rewrite copied verbatim.
- ``wait_for_k3s_ready`` (495-530) → ``_probe_k3s`` below: the *loop* is stripped (one bounded
  attempt, engine gate polls per the contract's ``ProbeK3s`` docstring / decision row 19), but
  **both checks are kept** — ``systemctl is-active`` AND ``k3s kubectl get nodes`` — because
  "active but API not responding" is a real, distinct not-ready state a single ``is-active``
  check cannot see.
- ``wait_for_ssh_ready`` (128-154) → ``_probe_ssh_port`` below, on the contract's own terms:
  ``ProbeSshPort``'s docstring is explicit that this is "raw TCP connect_ex" — a bare TCP dial,
  not a subprocess call — so it is the one command in this module that does not go through the
  injected ``SubprocessRunner``. It cannot classify-fail: any connect error (refused, timeout,
  DNS failure) collapses to ``SshPortState(open=False)``, exactly as v1's ``except Exception:
  pass`` did inside its poll loop — only the loop itself (and the 5s post-open settle sleep) is
  gone, both now the engine gate's job.

Deliberate, documented deviation beyond salvage: ``SSHTarget.port`` (§5.3) is a field v1's
``SSHConfig`` never had (v1 hardcoded port 22 into every ``ssh``/``ssh-keyscan`` invocation). This
module threads ``ssh.port`` into every command via ``-p`` so a non-22 SSH port is actually
honored — a genuinely new obligation the typed contract introduces, not a v1 bug reintroduced.

Decision-table rows this module is responsible for (docs/design/seam-c-provider.md §5.1):

| # | Site | Symptom | Classification |
|---|---|---|---|
| 16 | ssh-k3s, execute | stderr in ``TRANSIENT_STDERR_PHRASES``, or timeout | Transient / ``ENDPOINT_UNREACHABLE`` |
| 17 | ssh-k3s, execute | other non-zero exit | Permanent / ``SCRIPT_FAILED`` (stderr in ``detail``) |
| 18 | ssh-k3s, keyscan | empty output | Transient / ``HOST_KEYS_PENDING`` |
| 19 | ssh-k3s, probe | k3s active but ``kubectl get nodes`` fails | **Result** ``K3sReadiness(ready=False)`` |

Row 19 is ``ProbeK3s``'s *only* classification-table entry, and it covers both of the command's
internal checks, not just the second: v1's ``wait_for_k3s_ready`` (reference-code
.../_ssh_k3s_installer.py:495-530) wrapped **both** ``systemctl is-active`` and
``kubectl get nodes`` in the same "any failure ⇒ not ready yet, keep polling" handling —
``systemctl is-active`` itself exits non-zero by design for "inactive"/"activating", which is
authoritative not-ready DATA, not a script failure to raise on. So ``_probe_k3s`` below folds a
failure of *either* check into a typed ``K3sReadiness(ready=False)`` Result, distinguishing "not
active yet" from "active but API not responding" in ``detail`` — never raising, matching rows
16-18's general "connectivity symptom ⇒ raise" rule being the exception here, not the default,
for this one gate-poll command (engine's gate hysteresis, ``Step.poll_ready`` in
``engine/step.py``, is where retry-on-transient-noise for gate commands belongs generally; this
particular command simply never needs to hand it a raised error to do that job).

``InstallK3s(known_hosts="")`` ⇒ ``PermanentError(INVALID_INPUT)``: install-before-keys is
unrepresentable (§5.3's ``InstallK3s`` docstring) — not a v1 behavior (v1's ``execute()`` raised a
*programming-error*-flavored ``ClusterCreationError`` if ``ensure_host_keys()`` was never called;
the typed contract turns that same "impossible input" into data the caller can construct and this
module can reject up front, with zero backend traffic).
"""

from __future__ import annotations

import asyncio
import base64
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import ClassVar

import yaml

from seedpod.core.acme import AcmeConfig
from seedpod.core.errors import ErrorCode, PermanentError, ProviderError, TransientError
from seedpod.core.tempfiles import TempFileRegistry
from seedpod.providers.classify import classify_subprocess
from seedpod.providers.contract import (
    CaptureHostKeys,
    FetchKubeconfig,
    HostKeys,
    InstallK3s,
    K3sInstalled,
    K3sReadiness,
    Kubeconfig,
    ProbeK3s,
    ProbeSshPort,
    Progress,
    ProviderCommand,
    ProviderEvent,
    Result,
    SshPortState,
    SSHTarget,
    SubprocessRunner,
)

__all__ = ["SshK3sConfig", "SshK3sProvider"]

_SERVER_RE = re.compile(r"(https?://)[^:/]+(:\d+)?")

# ssh(1) reserves 255 for its OWN failures -- a remote command's exit status is passed
# through unchanged, so 255 means "ssh could not deliver the command", not "the command
# answered 255". `_run_insecure` uses this; see the comment there for why it is
# unambiguous at that one call site and deliberately NOT applied to `_run_strict`, whose
# remote commands can and do exit non-zero with a real, authoritative answer.
_SSH_TRANSPORT_FAILURE_RC = 255

# ...but 255 covers BOTH "could not reach it yet" and "will never be let in", and only the
# first is worth retrying. These are the second kind: a wrong/missing key or a host-key
# mismatch is a config error that every retry will reproduce, and the conformance suite
# pins `Fault.AUTH => PermanentError` precisely so this distinction cannot be lost.
_SSH_AUTH_STDERR_PHRASES = (
    "permission denied",
    "too many authentication failures",
    "host key verification failed",
    "no such identity",
    "unprotected private key file",
    "bad permissions",
)


@dataclass(frozen=True)
class SshK3sConfig:
    """IO-free construction data (Seam C §5.4's construction contract). Unlike the machine
    providers, almost everything ssh-k3s needs travels per-command on ``SSHTarget``/
    ``CaptureHostKeys``/``InstallK3s`` — this config only carries the handful of knobs that are
    provider-wide rather than per-invocation.
    """

    connect_timeout_s: float = 3.0  # ProbeSshPort's single bounded TCP dial
    check_ready_timeout_s: float = 5.0


def _rewrite_server(server: str, rewrite_to: str) -> str:
    """Salvaged verbatim from ``get_kubeconfig``
    (reference-code/seedpod/seedpod/providers/_ssh_k3s_installer.py:416-429): scheme+port-
    preserving, count=1 per entry (crown jewel #6's ssh variant)."""
    if not rewrite_to or not server:
        return server
    return _SERVER_RE.sub(lambda m: f"{m.group(1)}{rewrite_to}{m.group(2) or ''}", server, count=1)


def _traefik_helm_config(
    *, http_port: int, https_port: int, hostport: bool, acme: AcmeConfig | None
) -> str:
    """The ONE ``HelmChartConfig`` v2 writes for Traefik (DR-0036 decision 1).

    The ports/service half is salvaged verbatim from ``create_traefik_hostport_config``
    (reference-code/seedpod/seedpod/providers/_ssh_k3s_installer.py:548-573). The
    ``certificatesResolvers`` half is transcribed from v1's OTHER Traefik config,
    ``_apply_traefik_config`` (reference-code .../core/state_manager.py:1066-1082) --
    the only place in v1 that ever wrote one.

    **v1 had two writers for this single object** and they raced: the installer wrote
    this manifest into ``/var/lib/rancher/k3s/server/manifests/`` before k3s started,
    then ``_apply_traefik_config`` ``kubectl apply``-ed a second HelmChartConfig with
    the same name/namespace at the DEPLOYING transition, whose ports block was
    strictly poorer. v2 keeps the earlier, richer writer -- it lands before Traefik's
    initial install, so there is no reconfigure-and-restart -- and folds ACME into it
    rather than re-creating the race."""
    lines = [
        "apiVersion: helm.cattle.io/v1",
        "kind: HelmChartConfig",
        "metadata:",
        "  name: traefik",
        "  namespace: kube-system",
        "spec:",
        "  valuesContent: |-",
    ]
    if hostport:
        lines += [
            "    ports:",
            "      web:",
            "        port: 8000",
            "        expose:",
            "          default: true",
            f"        exposedPort: {http_port}",
            "        protocol: TCP",
            f"        hostPort: {http_port}",
            "      websecure:",
            "        port: 8443",
            "        expose:",
            "          default: true",
            f"        exposedPort: {https_port}",
            "        protocol: TCP",
            f"        hostPort: {https_port}",
            "    service:",
            "      type: ClusterIP",
        ]
    if acme is not None:
        # v1's block, field for field. `storage` is v1's literal path; the challenge
        # branch is v1's own two-valued `if challenge_type == "httpChallenge"`.
        lines += [
            "    certificatesResolvers:",
            "      letsencrypt:",
            "        acme:",
            f"          email: {acme.email}",
            "          storage: /data/acme.json",
            f"          caServer: {acme.server}",
        ]
        if acme.uses_http_challenge:
            lines += ["          httpChallenge:", "            entryPoint: web"]
        else:
            lines += ["          tlsChallenge: {}"]
    return "\n".join(lines) + "\n"


class SshK3sProvider:
    name: ClassVar[str] = "ssh-k3s"
    supported: ClassVar[frozenset[type]] = frozenset(
        {ProbeSshPort, CaptureHostKeys, InstallK3s, ProbeK3s, FetchKubeconfig}
    )

    def __init__(self, config: SshK3sConfig, transport: SubprocessRunner) -> None:
        """IO-free (§5.4's construction contract): stores config and the injected transport
        only. ``transport`` is a ``SubprocessRunner`` — conformance fault injection happens at
        that seam (``tests/conformance/``), never ``Mock``/``patch``."""
        self.config = config
        self.transport = transport
        self._tempfiles = TempFileRegistry()

    # ------------------------------------------------------------------
    # startup preflight
    # ------------------------------------------------------------------

    async def check_ready(self) -> None:
        """``ssh``/``ssh-keyscan`` on PATH — fail at startup, not mid-provision (replaces v1's
        implicit assumption that these binaries exist). Deterministic, quick invocations (no
        network dependency, no hang risk): ``ssh -V`` prints its version and exits; ``ssh-keyscan
        -T 1 localhost`` bails out within ~1s regardless of whether anything is listening."""
        for argv in (["ssh", "-V"], ["ssh-keyscan", "-T", "1", "localhost"]):
            result = await self.transport.run(argv, timeout=self.config.check_ready_timeout_s)
            if result.binary_missing:
                raise PermanentError(
                    f"ssh-k3s.check_ready: required binary {argv[0]!r} not found on PATH",
                    code=ErrorCode.NOT_FOUND,
                    provider=self.name,
                    command="check_ready",
                    detail={"binary": argv[0]},
                )

    # ------------------------------------------------------------------
    # Provider protocol
    # ------------------------------------------------------------------

    def execute(self, cmd: ProviderCommand) -> AsyncIterator[ProviderEvent]:
        """Unsupported ⇒ ``PermanentError(UNSUPPORTED)`` raised synchronously, before any
        backend traffic (§5.4) — not deferred to the first ``__anext__()``."""
        if type(cmd) not in self.supported:
            raise PermanentError(
                f"ssh-k3s: unsupported command {type(cmd).__name__}",
                code=ErrorCode.UNSUPPORTED,
                provider=self.name,
                command=type(cmd).__name__,
            )
        if isinstance(cmd, ProbeSshPort):
            return self._probe_ssh_port(cmd)
        if isinstance(cmd, CaptureHostKeys):
            return self._capture_host_keys(cmd)
        if isinstance(cmd, InstallK3s):
            return self._install_k3s(cmd)
        if isinstance(cmd, ProbeK3s):
            return self._probe_k3s(cmd)
        if isinstance(cmd, FetchKubeconfig):
            return self._fetch_kubeconfig(cmd)
        raise AssertionError(f"unreachable: {cmd!r} is in `supported` but has no handler")  # pragma: no cover

    # ------------------------------------------------------------------
    # commands
    # ------------------------------------------------------------------

    async def _probe_ssh_port(self, cmd: ProbeSshPort) -> AsyncIterator[ProviderEvent]:
        """Raw TCP connect_ex (contract's own words) — one bounded attempt, never raises. Any
        connect failure (refused, timeout, DNS) is "not open yet" data, exactly as v1's ``except
        Exception: pass`` inside its poll loop treated it; only the loop and the 5s post-open
        settle sleep are gone (both are now the engine gate's job).

        DR-0033: the collapse to ``open=False`` is unchanged -- it is still the only thing any
        caller may branch on -- but the error is no longer *discarded*. It rides along in
        ``SshPortState.detail`` so ``k3s.await_ssh`` can name it and the gate can report it on
        timeout. Backlog #15 is the worked example: ~60 polls each got ``EHOSTUNREACH`` from a
        macOS Local Network denial and the run still failed with a bare ``gate timed out``."""
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(cmd.host, cmd.port), timeout=self.config.connect_timeout_s
            )
        except (OSError, TimeoutError) as exc:
            # ONE branch on purpose: TimeoutError IS an OSError subclass (PEP 3151), so a
            # kernel ETIMEDOUT arrives here as TimeoutError(60, 'Operation timed out') and
            # must keep its errno rather than being flattened into our own wording. The
            # only message-less case is asyncio.wait_for's bare TimeoutError() -- str()s to
            # "" -- and for that the budget we gave up after IS the actionable fact.
            yield Result(SshPortState(
                open=False, detail=str(exc) or f"connect timed out after {self.config.connect_timeout_s}s"
            ))
            return
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
        yield Result(SshPortState(open=True))

    async def _capture_host_keys(self, cmd: CaptureHostKeys) -> AsyncIterator[ProviderEvent]:
        # (1) The ONLY StrictHostKeyChecking=no call, always first (crown jewel #2's ordering).
        await self._run_insecure(
            cmd.ssh,
            "cloud-init status --wait || true",
            timeout=cmd.cloud_init_wait_timeout_s,
            command_name="capture_host_keys.cloud_init_wait",
        )
        # (2) Capture stable host keys.
        keys = await self._ssh_keyscan(cmd.ssh, timeout=cmd.keyscan_timeout_s)
        yield Result(HostKeys(known_hosts=keys))

    async def _install_k3s(self, cmd: InstallK3s) -> AsyncIterator[ProviderEvent]:
        if not cmd.known_hosts:
            # install-before-keys is unrepresentable (§5.3) — zero backend traffic.
            raise PermanentError(
                "ssh-k3s.install_k3s: known_hosts is empty — CaptureHostKeys must run first",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="install_k3s",
            )

        flags: list[str] = ["--write-kubeconfig-mode=644"]
        if cmd.pod_cidr:
            flags.append(f"--cluster-cidr={cmd.pod_cidr}")
        if cmd.service_cidr:
            flags.append(f"--service-cidr={cmd.service_cidr}")
        for san in cmd.tls_sans:
            flags.append(f"--tls-san={san}")

        if cmd.ingress.ingress_type == "traefik":
            if not cmd.ingress.enabled:
                flags.append("--disable=traefik")
            elif cmd.ingress.expose_method == "hostport" or cmd.ingress.acme is not None:
                # Pre-create the HelmChartConfig BEFORE k3s starts, so Traefik picks up the
                # HostPort override -- and the ACME resolver -- on its initial install
                # (crown jewel). DR-0036 decision 4 widened the condition: this used to
                # fire on `hostport` ALONE, so a `loadbalancer` profile with ssl+dns would
                # render certresolver annotations and get no resolver -- the same silent
                # half-configuration #24 was, one branch over. The ports/service block
                # below stays gated strictly on hostport, so a loadbalancer profile's
                # service type is untouched.
                yield Progress(phase="k3s.traefik_config")
                await self._create_traefik_config(
                    cmd.ssh,
                    cmd.known_hosts,
                    http_port=80,
                    https_port=443,
                    hostport=cmd.ingress.expose_method == "hostport",
                    acme=cmd.ingress.acme,
                )
        else:
            flags.append("--disable=traefik")

        flags.append("--disable=servicelb")

        flag_str = " ".join(flags)
        install_cmd = f"curl -sfL https://get.k3s.io | sudo INSTALL_K3S_EXEC='{flag_str}' sh -"
        yield Progress(phase="k3s.installing")
        await self._run_strict(cmd.ssh, cmd.known_hosts, install_cmd, command_name="install_k3s")
        yield Result(K3sInstalled())

    async def _probe_k3s(self, cmd: ProbeK3s) -> AsyncIterator[ProviderEvent]:
        """Both checks are wrapped: v1's ``wait_for_k3s_ready`` (reference-code
        .../_ssh_k3s_installer.py:495-530) caught ``ClusterCreationError`` around *both* the
        ``systemctl is-active`` call and the ``kubectl get nodes`` call, folding any failure of
        either into "not ready yet, keep polling" — ``systemctl is-active`` itself exits
        non-zero for "inactive"/"activating" by design, which is authoritative not-ready DATA,
        not a script failure. This is ONE bounded attempt at each (the retry loop is gone; the
        engine gate polls), but neither check is allowed to turn into a raised
        ``ProviderError`` — row 19 is this command's only classification-table entry, and it is
        a **Result**, never an exception."""
        try:
            active_raw = await self._run_strict(
                cmd.ssh, cmd.known_hosts, "sudo systemctl is-active k3s", command_name="probe_k3s.systemctl"
            )
        except ProviderError as e:
            # Realistic ``systemctl is-active`` behavior: it exits non-zero (typically 3) for
            # "inactive"/"activating", routing here via the shared classify_subprocess path
            # rather than the string-check branch below — still "not active" data, not a
            # script failure.
            yield Result(K3sReadiness(ready=False, detail=f"k3s not active (systemctl check failed: {e})"))
            return
        if active_raw.strip() != "active":
            yield Result(K3sReadiness(ready=False, detail=f"k3s not active (systemctl: {active_raw.strip() or 'unknown'})"))
            return

        # The crown jewel: active-but-API-down is a distinct not-ready state from "not active
        # yet" — both fold into a typed Result, never raised.
        try:
            await self._run_strict(
                cmd.ssh, cmd.known_hosts, "sudo k3s kubectl get nodes --no-headers", command_name="probe_k3s.kubectl"
            )
        except ProviderError:
            yield Result(K3sReadiness(ready=False, detail="k3s active but API not responding"))
            return
        yield Result(K3sReadiness(ready=True))

    async def _fetch_kubeconfig(self, cmd: FetchKubeconfig) -> AsyncIterator[ProviderEvent]:
        if cmd.ssh is None or not cmd.known_hosts:
            raise PermanentError(
                "ssh-k3s.fetch_kubeconfig: ssh target and known_hosts are required for the ssh-k3s variant",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="fetch_kubeconfig",
            )
        raw = await self._run_strict(
            cmd.ssh, cmd.known_hosts, "sudo cat /etc/rancher/k3s/k3s.yaml", command_name="fetch_kubeconfig"
        )
        if not raw.strip():
            raise PermanentError(
                f"ssh-k3s.fetch_kubeconfig: empty kubeconfig retrieved from {cmd.ssh.host}",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="fetch_kubeconfig",
            )
        try:
            doc = yaml.safe_load(raw)
        except yaml.YAMLError as e:
            raise PermanentError(
                f"ssh-k3s.fetch_kubeconfig: kubeconfig from {cmd.ssh.host} is not valid YAML: {e}",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="fetch_kubeconfig",
            ) from e
        if not isinstance(doc, dict) or "clusters" not in doc:
            raise PermanentError(
                f"ssh-k3s.fetch_kubeconfig: kubeconfig from {cmd.ssh.host} missing 'clusters' section",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="fetch_kubeconfig",
            )

        # In-memory rewrite, never sed-over-SSH (crown jewel #6).
        for entry in doc.get("clusters", []):
            cluster = entry.get("cluster", {}) if isinstance(entry, dict) else {}
            server = cluster.get("server", "")
            new_server = _rewrite_server(server, cmd.rewrite_server_to)
            if new_server != server:
                cluster["server"] = new_server

        yield Result(Kubeconfig(yaml_text=yaml.safe_dump(doc, default_flow_style=False, sort_keys=False)))

    # ------------------------------------------------------------------
    # SSH internals
    # ------------------------------------------------------------------

    async def _run_insecure(self, ssh: SSHTarget, command: str, *, timeout: int, command_name: str) -> str:
        """One SSH invocation that accepts any host key. Used ONLY for the cloud-init wait
        before host-key scanning (salvaged from ``_run_insecure_ssh``,
        reference-code/seedpod/seedpod/providers/_ssh_k3s_installer.py:223-269, retry loop
        stripped — one bounded attempt, engine Schedule owns retry)."""
        argv = [
            "ssh",
            "-i", ssh.private_key_path,
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "GlobalKnownHostsFile=/dev/null",
            "-o", "LogLevel=ERROR",
            "-o", f"ConnectTimeout={ssh.connection_timeout_s}",
            "-p", str(ssh.port),
            "-T",
            f"{ssh.user}@{ssh.host}",
            command,
        ]
        result = await self.transport.run(argv, timeout=timeout)
        stdout_text = result.stdout.decode(errors="replace").strip()
        stderr_text = result.stderr.decode(errors="replace").strip()
        if (
            result.returncode == _SSH_TRANSPORT_FAILURE_RC
            and not result.timed_out
            and not result.binary_missing
            and not any(p in stderr_text.lower() for p in _SSH_AUTH_STDERR_PHRASES)
        ):
            # The ONLY caller passes `cloud-init status --wait || true`, so the REMOTE
            # command cannot exit non-zero -- a non-zero rc here is always ssh's own
            # failure, and ssh reserves 255 for exactly that. `classify_subprocess`'s
            # generic rule ("a clean non-zero exit is an AUTHORITATIVE answer =>
            # Permanent") is right in general and wrong here, because there is no remote
            # answer to be authoritative about.
            #
            # Found by smoke 8 (2026-08-09): a droplet whose sshd blipped between
            # `k3s.await_ssh` (a bare TCP dial, which had just succeeded) and this call
            # failed the WHOLE provision permanently -- `trust_host` declares
            # `retry: ssh_default` and never got to use it, and compensation destroyed
            # the droplet. An immediate re-run provisioned fine, which is what proved it
            # transient. Raising Transient hands the retry to the engine's Schedule,
            # where this module's docstring says retry belongs.
            raise TransientError(
                f"ssh-k3s.{command_name}: ssh transport failure (exit 255) to {ssh.host}: "
                f"{stderr_text or 'no stderr'}",
                code=ErrorCode.ENDPOINT_UNREACHABLE,
                provider=self.name,
                command=command_name,
                detail={"exit_code": str(result.returncode), "stderr": stderr_text},
            )
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise classify_subprocess(
                provider=self.name,
                command=command_name,
                host=ssh.host,
                rc=result.returncode,
                stderr=stderr_text,
                timed_out=result.timed_out,
                binary_missing=result.binary_missing,
                observing_infra=False,  # ssh-k3s never raises Unreachable (§5.1)
            )
        return stdout_text

    async def _ssh_keyscan(self, ssh: SSHTarget, *, timeout: int) -> str:
        """Salvaged verbatim from ``_ssh_keyscan``
        (reference-code/seedpod/seedpod/providers/_ssh_k3s_installer.py:183-221). Row 18: rc=0
        but empty output ⇒ ``Transient(HOST_KEYS_PENDING)`` (host still booting)."""
        argv = ["ssh-keyscan", "-T", str(timeout), "-p", str(ssh.port), "-t", "ed25519,rsa,ecdsa", ssh.host]
        result = await self.transport.run(argv, timeout=timeout + 5)
        stdout_text = result.stdout.decode(errors="replace")
        stderr_text = result.stderr.decode(errors="replace").strip()
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise classify_subprocess(
                provider=self.name,
                command="capture_host_keys.keyscan",
                host=ssh.host,
                rc=result.returncode,
                stderr=stderr_text,
                timed_out=result.timed_out,
                binary_missing=result.binary_missing,
                observing_infra=False,
            )
        if not stdout_text.strip():
            raise TransientError(
                f"ssh-k3s.capture_host_keys: ssh-keyscan returned no host keys for {ssh.host}",
                code=ErrorCode.HOST_KEYS_PENDING,
                provider=self.name,
                command="capture_host_keys.keyscan",
            )
        return stdout_text

    async def _run_strict(self, ssh: SSHTarget, known_hosts: str, command: str, *, command_name: str) -> str:
        """Strict host-key-checked SSH (salvaged from ``execute``,
        reference-code/seedpod/seedpod/providers/_ssh_k3s_installer.py:275-384) — the
        ``retry_attempts``/``retry_delay`` loop is STRIPPED per the task: one bounded attempt,
        the engine's ``ssh_default`` Schedule replaces it (H4-H6). Rows 16/17: connectivity
        symptom ⇒ Transient/``ENDPOINT_UNREACHABLE``; other non-zero exit ⇒
        Permanent/``SCRIPT_FAILED``."""
        with self._tempfiles.file(known_hosts, suffix=".known_hosts") as known_hosts_path:
            argv = [
                "ssh",
                "-i", ssh.private_key_path,
                "-o", f"UserKnownHostsFile={known_hosts_path}",
                "-o", "GlobalKnownHostsFile=/dev/null",
                "-o", "StrictHostKeyChecking=yes",
                "-o", "LogLevel=ERROR",
                "-o", f"ConnectTimeout={ssh.connection_timeout_s}",
                "-p", str(ssh.port),
                "-T",
                f"{ssh.user}@{ssh.host}",
                command,
            ]
            result = await self.transport.run(argv, timeout=ssh.command_timeout_s)
        stdout_text = result.stdout.decode(errors="replace").strip()
        stderr_text = result.stderr.decode(errors="replace").strip()
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise classify_subprocess(
                provider=self.name,
                command=command_name,
                host=ssh.host,
                rc=result.returncode,
                stderr=stderr_text,
                timed_out=result.timed_out,
                binary_missing=result.binary_missing,
                observing_infra=False,
            )
        return stdout_text

    async def _create_traefik_config(
        self,
        ssh: SSHTarget,
        known_hosts: str,
        *,
        http_port: int,
        https_port: int,
        hostport: bool = True,
        acme: AcmeConfig | None = None,
    ) -> None:
        """Salvaged verbatim from ``create_traefik_hostport_config``
        (reference-code/seedpod/seedpod/providers/_ssh_k3s_installer.py:532-581): base64-over-
        SSH so the manifest content stays robust against shell-quoting edge cases -- which
        now also keeps the ACME block's ``@`` and ``/`` characters away from the shell."""
        manifest = _traefik_helm_config(
            http_port=http_port, https_port=https_port, hostport=hostport, acme=acme
        )
        encoded = base64.b64encode(manifest.encode()).decode()
        write_cmd = (
            "sudo mkdir -p /var/lib/rancher/k3s/server/manifests && "
            f"echo {encoded} | base64 -d | "
            "sudo tee /var/lib/rancher/k3s/server/manifests/traefik-config.yaml > /dev/null"
        )
        await self._run_strict(ssh, known_hosts, write_cmd, command_name="install_k3s.traefik_config")
