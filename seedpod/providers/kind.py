"""seedpod/providers/kind.py — the ``kind`` Provider (Seam C §5.3-5.4, decision-table rows
20-26, amended by ``docs/design/coherence-review.md`` Conflicts 5-7, 12).

Machine plane **including** ``FetchKubeconfig`` (§5.4 plane matrix: "kind/orbstack ⇒ machine
plane incl. FetchKubeconfig"). Talks to the local ``kind`` CLI and Docker daemon exclusively
over an **injected** ``SubprocessRunner`` (§5.4's construction contract) — no
``create_tracked_subprocess``/``asyncio.create_subprocess_exec`` call inside this module; that
lives behind the transport the composition root wires up.

Salvaged from ``reference-code/seedpod/seedpod/providers/kind.py`` (``KindProvider``):

- ``_make_cluster_name`` (280-289) → ``_cluster_name`` below, verbatim sanitization
  (lowercase, ``[^a-z0-9-]`` → ``-``, dedupe hyphens, strip, 63-char DNS truncation).
- ``translate_node_spec`` (151-180) → ``_worker_count`` below: ``"cpu,mem"`` size-key lookup
  into ``node_size_mapping`` for a worker count (kind has no real per-node sizing — CPU/memory
  map to node *count*, not instance type).
- ``_resolve_host_ip`` (291-323) → ``_resolve_host_ip`` below, verbatim (IP-literal passthrough,
  ``"localhost"`` → ``127.0.0.1``, DNS resolve-or-fall-back-to-hostname for anything else).
- ``_generate_kind_config`` (420-511) → ``_generate_kind_config`` below, verbatim shape:
  ``kubeadmConfigPatches`` (ingress-ready node label + ``certSANs`` + ``etcd unsafe-no-fsync``),
  the ``extraPortMappings`` API-server binding (``containerPort: 6443``,
  ``listenAddress: "0.0.0.0"``) plus config-supplied extra mappings (Traefik host ports), worker
  node count. **Crown jewel #7**: ``podSubnet``/``serviceSubnet`` always come from *provider*
  config (``KindConfig.pod_subnet``/``service_subnet``, kindnet needs a real ``/16`` — the
  engine-supplied Tailscale ``/24``s on ``CreateInstance.pod_cidr``/``service_cidr`` are too
  small for kindnet's CNI and are never passed to the kind config), and are echoed back verbatim
  as ``InstanceCreated.effective_pod_cidr``/``effective_service_cidr`` so the engine sees what
  was *actually* installed rather than what it asked for.
- ``_allocate_port``/``_refresh_allocated_ports``/``_get_cluster_port`` (325-418) →
  ``_allocate_port``/``_cluster_port`` below: scan every existing kind cluster's
  ``docker inspect`` port mapping, salvaged **as-is** (linear scan of the configured range,
  first free wins) — the in-memory ``_allocated_ports`` cache dies (providers are stateless;
  every call re-scans docker, which is cheap and always authoritative).
- ``_get_kubeconfig`` (652-681) → ``_fetch_kubeconfig`` below: ``kind get kubeconfig --name``,
  in-memory YAML rewrite (crown jewel #6's kind variant — **the one that substitutes both host
  AND port**, not just host): ``127.0.0.1``/``localhost``/``0.0.0.0`` all match, verbatim regex.
- ``_list_kind_clusters``/``_get_cluster_port``/``_is_cluster_running`` (347-419, 1066-1122) →
  ``_list_kind_clusters``/``_cluster_port``/``_is_cluster_running`` below, folded onto one
  shared ``_docker_inspect`` helper (see its docstring for the row 22/23 split this closes).

Deliberately NOT ported (§5.7.4 "v1 bugs deliberately not pinned" + scope decisions flagged here
per CLAUDE.md's citation requirement):

- **``list_clusters``'s swallow-to-``[]`` on ``InfrastructureUnreachableError`` (v1 lines
  913-919) — explicitly named in §5.7.4.** ``_list_kind_clusters`` here lets that exception
  propagate; a caller that cannot reach Docker gets a raise, never a lying empty list.
- **``retain_on_failure`` config knob + self-cleanup-on-failure branch (v1 ``_provision_cluster``
  lines 632-637)** — row 24 "generalizes ``retain_on_failure=false``": a failed
  ``kind create cluster`` now *always* raises ``PermanentError(SCRIPT_FAILED)`` and lets the
  engine's ``undo_for(CreateInstance, observed) -> DestroyInstance`` clean up (the C1 close, per
  ``RESOURCE_ALLOCATED`` having already been emitted with the deterministic name/port before the
  create call — see ``_create_instance`` below). The provider itself never runs its own
  best-effort ``kind delete cluster`` on failure anymore; that would race the engine's undo.
- **Traefik deployment (v1 ``_deploy_traefik``/``_wait_for_traefik``, lines 683-774) leaves
  provider code entirely** — §5.4's plane-matrix note: "Traefik parity shims leave provider code
  entirely: they become ``kubectl-apply`` workflow steps over the copied
  ``traefik-kind.yaml`` ... with a non-fatal ``KubeProbeRollout`` gate (``on_failure: warn`` —
  crown jewel #10 preserved as workflow policy)." Row 26 ("rollout slow after apply — not an
  error") is a **kubectl-provider + workflow-config** concern, not this module's.
- **``get_cluster_status`` (v1 lines 775-795)** — v1's own docstring calls it out as
  "limited functionality" (it cannot recover a UUID from a cluster name); replaced wholesale by
  the typed, resource-ids-driven ``ProbeInstance``.
- **Background-scheduler dispatch (v1 ``_provision_cluster`` via ``get_scheduler()``/
  ``asyncio.create_task``, lines 254-274) and every ``state_manager.advance_cluster_provisioning``
  call** — structurally impossible per §5.7.2 (C-03): provider self-scheduling and
  ``state_manager`` upward calls are gone; ``kind create cluster --wait`` runs to completion
  inside one bounded ``CreateInstance.execute()`` call and the engine's workflow steps own
  sequencing.

Genuinely NEW (§5.7.3, not salvage): **re-invocation adoption by deterministic cluster name**
(conformance C-07). v1 never retried creates; kind cluster names are deterministic from
``slug`` (unlike DO's random droplet id / random-tag adoption), so a second ``CreateInstance``
for the same slug/cluster_uuid naturally recomputes the same name — ``_create_instance`` checks
``kind get clusters`` for that name *before* allocating a port or writing any config, and adopts
(``adopted_existing=True``) instead of risking ``kind create cluster``'s own "cluster already
exists" failure.

Deliberate, documented deviation beyond salvage: **docker-inspect connectivity vs absence split
(rows 22/23) is NEW, not v1 behavior.** v1's ``_is_cluster_running``/``_get_cluster_port`` both
treated *any* non-zero ``docker inspect`` exit as authoritative "not running"/"no port" — with
zero stderr inspection — which would have silently mis-reported a genuinely unreachable Docker
daemon as "container gone" (a false-orphan risk, exactly what crown jewel #1 forbids). This
provider's shared ``_docker_inspect`` helper checks ``TRANSIENT_STDERR_PHRASES`` before treating
a non-zero exit as absence, raising ``InfrastructureUnreachableError`` for a genuine connectivity
symptom instead (row 23) and reserving absence-as-data for everything else (row 22).
"""

from __future__ import annotations

import json
import re
import socket
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import ClassVar

import yaml

from seedpod.core.cluster_spec import NodeSpecification
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.core.reconciliation_intents import OrphanIntent, ReconciliationIntent, ZombieIntent
from seedpod.core.tempfiles import TempFileRegistry
from seedpod.providers.classify import TRANSIENT_STDERR_PHRASES, classify_subprocess
from seedpod.providers.contract import (
    RESOURCE_ALLOCATED,
    CreateInstance,
    DestroyInstance,
    DestroyOutcome,
    DestroyStatus,
    FetchKubeconfig,
    InstanceCreated,
    InstanceState,
    InstanceSummary,
    Kubeconfig,
    ListInstances,
    ProbeDestruction,
    ProbeInstance,
    Progress,
    ProviderCommand,
    ProviderEvent,
    Reconcile,
    Result,
    SubprocessRunner,
)

__all__ = ["KindConfig", "KindProvider"]

# Cluster states (seedpod/core/records.py ClusterState, hyphenated wire values) excluded from
# Phase-B orphan detection: terminal-ish/self-explaining drift. Mirrors digitalocean.py's
# identical list (docs/design/coherence-review.md: "DESTROYING+missing ⇒ Orphan completion
# backstop everywhere" — the same exclusion set applies uniformly across machine providers).
_ORPHAN_EXCLUDED_STATES = frozenset({"destroyed", "zombie", "destroy-scheduled", "failed", "destroying"})

_KIND_SERVER_RE = re.compile(r"https://(?:127\.0\.0\.1|localhost|0\.0\.0\.0):(\d+)")


def _has_transient_phrase(stderr: str) -> bool:
    lowered = (stderr or "").lower()
    return any(phrase in lowered for phrase in TRANSIENT_STDERR_PHRASES)


def _cluster_name(slug: str) -> str:
    """Salvaged verbatim from ``_make_cluster_name``
    (reference-code/seedpod/seedpod/providers/kind.py:280-289)."""
    name = f"seedpod-{slug}"
    name = re.sub(r"[^a-z0-9-]", "-", name.lower())
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:63]


def _resolve_host_ip(hostname: str) -> str:
    """Salvaged verbatim from ``_resolve_host_ip``
    (reference-code/seedpod/seedpod/providers/kind.py:291-323)."""
    try:
        socket.inet_aton(hostname)
        return hostname
    except OSError:
        pass
    if hostname == "localhost":
        return "127.0.0.1"
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return hostname


def _rewrite_kind_server(server: str, rewrite_to: str, port: str | None) -> str:
    """Salvaged verbatim regex from ``_get_kubeconfig``
    (reference-code/seedpod/seedpod/providers/kind.py:672-678): matches
    ``127.0.0.1``/``localhost``/``0.0.0.0`` and substitutes BOTH host and port — crown jewel
    #6's kind variant. ``port`` is the tracked ``resource_ids["api_port"]`` (preferred, in case
    the raw kubeconfig's embedded port ever drifts from the allocated one); falls back to the
    port already present in ``server`` if the caller didn't track one."""
    if not rewrite_to or not server:
        return server
    match = _KIND_SERVER_RE.search(server)
    if not match:
        return server
    target_port = port or match.group(1)
    return _KIND_SERVER_RE.sub(f"https://{rewrite_to}:{target_port}", server, count=1)


@dataclass(frozen=True)
class KindConfig:
    """IO-free construction data (§5.4's construction contract), loaded by the composition root
    from ``config/providers/kind.yml``.
    """

    api_server_host: str = "localhost"
    port_range_start: int = 6443
    port_range_end: int = 6543

    node_image: str = "kindest/node:v1.29.2"
    wait_timeout: str = "5m"

    # kindnet CNI needs a real /16 — see module docstring's crown jewel #7 note.
    pod_subnet: str = "10.244.0.0/16"
    service_subnet: str = "10.96.0.0/12"

    # (container_port, host_port, protocol) triples — v1's cluster_defaults.extra_port_mappings.
    extra_port_mappings: tuple[tuple[int, int, str], ...] = ()

    node_size_mapping: Mapping[str, int] = field(default_factory=dict)  # "cpu,mem" -> workers
    default_workers: int = 0

    check_ready_timeout_s: float = 5.0
    docker_timeout_s: float = 10.0
    list_timeout_s: float = 30.0
    create_timeout_s: float = 330.0  # v1: _parse_timeout(wait_timeout) + 30s cleanup buffer
    delete_timeout_s: float = 60.0
    fetch_kubeconfig_timeout_s: float = 30.0


class KindProvider:
    name: ClassVar[str] = "kind"
    supported: ClassVar[frozenset[type]] = frozenset(
        {CreateInstance, ProbeInstance, DestroyInstance, ProbeDestruction, ListInstances, Reconcile, FetchKubeconfig}
    )

    def __init__(self, config: KindConfig, transport: SubprocessRunner) -> None:
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
        """``kind``/``docker`` on PATH, AND Docker actually up (Provider protocol's "docker up"
        clause) — fail at startup, not mid-provision. Row 20: either binary missing ⇒
        ``Permanent(NOT_FOUND)``. Docker present but unreachable ⇒ whatever
        ``classify_subprocess`` decides from the symptom (typically
        ``InfrastructureUnreachableError`` — a daemon that is merely not running yet is not a
        "refuse to start forever" condition, but the caller does learn about it immediately
        rather than at first provision)."""
        for binary, argv in (("kind", ["kind", "version"]), ("docker", ["docker", "version"])):
            result = await self.transport.run(argv, timeout=self.config.check_ready_timeout_s)
            if result.binary_missing:
                raise PermanentError(
                    f"kind.check_ready: required binary {binary!r} not found on PATH",
                    code=ErrorCode.NOT_FOUND,
                    provider=self.name,
                    command="check_ready",
                    detail={"binary": binary},
                )
            if binary == "docker" and (result.timed_out or result.returncode != 0):
                stderr_text = result.stderr.decode(errors="replace").strip()
                raise classify_subprocess(
                    provider=self.name,
                    command="check_ready",
                    host=self.config.api_server_host,
                    rc=result.returncode,
                    stderr=stderr_text,
                    timed_out=result.timed_out,
                    binary_missing=False,
                    observing_infra=True,
                )

    # ------------------------------------------------------------------
    # Provider protocol
    # ------------------------------------------------------------------

    def execute(self, cmd: ProviderCommand) -> AsyncIterator[ProviderEvent]:
        """Unsupported ⇒ ``PermanentError(UNSUPPORTED)`` raised synchronously, before any
        backend traffic (§5.4) — not deferred to the first ``__anext__()``."""
        if type(cmd) not in self.supported:
            raise PermanentError(
                f"kind: unsupported command {type(cmd).__name__}",
                code=ErrorCode.UNSUPPORTED,
                provider=self.name,
                command=type(cmd).__name__,
            )
        if isinstance(cmd, CreateInstance):
            return self._create_instance(cmd)
        if isinstance(cmd, ProbeInstance):
            return self._probe_instance(cmd)
        if isinstance(cmd, DestroyInstance):
            return self._destroy_instance(cmd)
        if isinstance(cmd, ProbeDestruction):
            return self._probe_destruction(cmd)
        if isinstance(cmd, ListInstances):
            return self._list_instances(cmd)
        if isinstance(cmd, Reconcile):
            return self._reconcile(cmd)
        if isinstance(cmd, FetchKubeconfig):
            return self._fetch_kubeconfig(cmd)
        raise AssertionError(f"unreachable: {cmd!r} is in `supported` but has no handler")  # pragma: no cover

    # ------------------------------------------------------------------
    # commands — machine plane
    # ------------------------------------------------------------------

    async def _create_instance(self, cmd: CreateInstance) -> AsyncIterator[ProviderEvent]:
        name = _cluster_name(cmd.slug)

        # NEW (§5.7.3, C-07): re-invocation adoption by deterministic name — kind cluster
        # names come from `slug`, not a backend-assigned id, so a repeated CreateInstance for
        # the same cluster naturally recomputes the same name. Check before allocating a port
        # or writing any config: zero backend mutation on the adopt path.
        existing_port = await self._existing_cluster_port(name)
        if existing_port is not None:
            resource_ids = {"kind_cluster_name": name, "api_port": existing_port}
            yield Progress(phase=RESOURCE_ALLOCATED, data={"resource_ids": resource_ids})
            yield Result(
                InstanceCreated(
                    resource_ids=resource_ids,
                    # v1 reference-code .../kind.py:590-594,616: "public_ip should always be an
                    # IP address, not a hostname" — resolved via _resolve_host_ip, not the raw
                    # (possibly-hostname) config value. Downstream consumers (kubeconfig server
                    # rewrite, clusters.public_ip column) both assume an IP-shaped value.
                    address=_resolve_host_ip(self.config.api_server_host),
                    effective_pod_cidr=self.config.pod_subnet,
                    effective_service_cidr=self.config.service_subnet,
                    adopted_existing=True,
                )
            )
            return

        api_port = str(cmd.api_port) if cmd.api_port is not None else await self._allocate_port()
        resource_ids = {"kind_cluster_name": name, "api_port": api_port}

        # Tag-before-boot analog: the identity (name + port) is fixed and emitted BEFORE the
        # `kind create cluster` call that actually creates docker containers, so a death
        # mid-create still carries an id for undo_for -> DestroyInstance to compensate
        # (row 24's "generalizes retain_on_failure=false" — the module docstring's C1 note).
        yield Progress(phase=RESOURCE_ALLOCATED, data={"resource_ids": resource_ids})

        kind_config_yaml = self._generate_kind_config(name=name, api_port=int(api_port), cmd=cmd)
        with self._tempfiles.file(kind_config_yaml, suffix=".kind.yaml") as config_path:
            yield Progress(phase="kind.creating", data={"cluster_name": name})
            await self._run_kind(
                ["create", "cluster", "--name", name, "--config", str(config_path), "--wait", self.config.wait_timeout],
                timeout=self.config.create_timeout_s,
                command_name="create_instance",
            )

        yield Result(
            InstanceCreated(
                resource_ids=resource_ids,
                # Resolved IP, not the raw hostname — see the adopt-path comment above.
                address=_resolve_host_ip(self.config.api_server_host),
                effective_pod_cidr=self.config.pod_subnet,
                effective_service_cidr=self.config.service_subnet,
                adopted_existing=False,
            )
        )

    async def _probe_instance(self, cmd: ProbeInstance) -> AsyncIterator[ProviderEvent]:
        name = cmd.resource_ids.get("kind_cluster_name")
        if not name:
            yield Result(InstanceState(phase="absent", address=None, detail="no kind_cluster_name on record"))
            return
        running_raw = await self._docker_inspect(
            f"{name}-control-plane", "{{.State.Running}}", command_name="probe_instance"
        )
        if running_raw is None:
            # Row 22: docker inspect rc != 0 (non-connectivity) is AUTHORITATIVE absence.
            yield Result(InstanceState(phase="absent", address=None, detail="container not found"))
            return
        if running_raw.strip().lower() == "true":
            yield Result(
                InstanceState(phase="running", address=_resolve_host_ip(self.config.api_server_host), detail="running")
            )
        else:
            yield Result(InstanceState(phase="stopped", address=None, detail="container not running"))

    async def _destroy_instance(self, cmd: DestroyInstance) -> AsyncIterator[ProviderEvent]:
        name = cmd.resource_ids.get("kind_cluster_name") or _cluster_name(cmd.slug)
        existing = await self._docker_inspect(f"{name}-control-plane", "{{.State.Running}}", command_name="destroy_instance.check")
        if existing is None:
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED, note="kind cluster already absent"))
            return
        try:
            await self._run_kind(
                ["delete", "cluster", "--name", name], timeout=self.config.delete_timeout_s, command_name="destroy_instance"
            )
        except PermanentError as e:
            # v1 kind's "error" status maps to DESTROY_FAILED (§5.3 DestroyStatus docstring).
            # A connectivity symptom (Transient/Unreachable) is NOT caught here — it propagates
            # so the engine parks rather than lying about a failed destroy (Conflict 5).
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROY_FAILED, error=str(e), stuck_resources=(name,)))
            return
        yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED))

    async def _probe_destruction(self, cmd: ProbeDestruction) -> AsyncIterator[ProviderEvent]:
        name = cmd.resource_ids.get("kind_cluster_name")
        if not name:
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED, note="no resources to probe"))
            return
        existing = await self._docker_inspect(f"{name}-control-plane", "{{.State.Running}}", command_name="probe_destruction")
        if existing is None:
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED))
            return
        # kind's own delete is synchronous (no in-flight "destroying" window this command would
        # observe); still present after a destroy attempt is a genuine stuck failure, not "in
        # progress" — mirrors v1's kind "error" -> DESTROY_FAILED mapping (§5.3 docstring).
        yield Result(
            DestroyOutcome(status=DestroyStatus.DESTROY_FAILED, error="kind container still present", stuck_resources=(name,))
        )

    async def _list_instances(self, cmd: ListInstances) -> AsyncIterator[ProviderEvent]:
        names = await self._list_kind_clusters()
        summaries = []
        for name in names:
            if not name.startswith("seedpod-"):
                continue
            port = await self._cluster_port(name)
            resource_ids = {"kind_cluster_name": name}
            if port is not None:
                resource_ids["api_port"] = port
            summaries.append(InstanceSummary(name=name, resource_ids=resource_ids))
        yield Result(tuple(summaries))

    async def _reconcile(self, cmd: Reconcile) -> AsyncIterator[ProviderEvent]:
        """Salvaged mapping from ``reconcile``
        (reference-code/seedpod/seedpod/providers/kind.py:925-1064), reshaped onto the shared
        Phase A/B pattern (digitalocean.py's ``_reconcile``): DESTROYED+present ⇒ Zombie;
        non-excluded status + cluster missing ⇒ Orphan; non-excluded status + cluster present
        but container **stopped** ⇒ Orphan too (crown jewel — v1's "container-stopped ⇒ Orphan",
        the one thing DO's Phase B has no analogue for); DESTROYING+missing ⇒ Orphan completion
        backstop. ``_list_kind_clusters``/``_is_cluster_running`` both raise
        ``InfrastructureUnreachableError`` on a genuine connectivity symptom (never the v1
        swallow-to-``[]`` — §5.7.4), which propagates out of this generator uncaught: the engine
        skips every cluster in ``cmd.clusters`` and touches nothing (crown jewel #1). kind has
        no ``CreateUnmanagedIntent`` analogue: unlike DO's tag-addressable droplets, an untracked
        kind cluster's name cannot be mapped back to a ``cluster_uuid`` (v1 never attempted
        this either — not a new gap).
        """
        present = set(await self._list_kind_clusters())
        running_cache: dict[str, bool] = {}
        intents: list[ReconciliationIntent] = []

        for snapshot in cmd.clusters:
            resolved_name = snapshot.resource_ids.get("kind_cluster_name") or _cluster_name(snapshot.slug)

            if snapshot.status == "destroyed":
                if resolved_name in present:
                    intents.append(ZombieIntent(cluster_id=snapshot.cluster_uuid, droplet_id=resolved_name, droplet_ip=None))
                continue

            if snapshot.status in _ORPHAN_EXCLUDED_STATES:
                continue

            if resolved_name not in present:
                intents.append(
                    OrphanIntent(cluster_id=snapshot.cluster_uuid, reason=f"kind cluster '{resolved_name}' not found")
                )
                continue

            if resolved_name not in running_cache:
                running_cache[resolved_name] = await self._is_cluster_running(resolved_name)
            if not running_cache[resolved_name]:
                intents.append(
                    OrphanIntent(
                        cluster_id=snapshot.cluster_uuid,
                        reason=f"kind cluster '{resolved_name}' exists but container not running",
                    )
                )

        # DESTROYING+missing completion backstop (single intent per cluster, everywhere).
        for snapshot in cmd.clusters:
            if snapshot.status != "destroying":
                continue
            resolved_name = snapshot.resource_ids.get("kind_cluster_name") or _cluster_name(snapshot.slug)
            if resolved_name not in present:
                intents.append(
                    OrphanIntent(
                        cluster_id=snapshot.cluster_uuid,
                        reason=f"kind cluster '{resolved_name}' destroyed - marking DESTROYED",
                    )
                )

        yield Result(tuple(intents))

    # ------------------------------------------------------------------
    # commands — kind's FetchKubeconfig variant
    # ------------------------------------------------------------------

    async def _fetch_kubeconfig(self, cmd: FetchKubeconfig) -> AsyncIterator[ProviderEvent]:
        name = cmd.resource_ids.get("kind_cluster_name")
        if not name:
            raise PermanentError(
                "kind.fetch_kubeconfig: resource_ids missing 'kind_cluster_name'",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="fetch_kubeconfig",
            )
        raw = await self._run_kind(
            ["get", "kubeconfig", "--name", name], timeout=self.config.fetch_kubeconfig_timeout_s, command_name="fetch_kubeconfig"
        )
        try:
            doc = yaml.safe_load(raw)
        except yaml.YAMLError as e:
            raise PermanentError(
                f"kind.fetch_kubeconfig: kubeconfig for {name!r} is not valid YAML: {e}",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="fetch_kubeconfig",
            ) from e
        if not isinstance(doc, dict) or "clusters" not in doc:
            raise PermanentError(
                f"kind.fetch_kubeconfig: kubeconfig for {name!r} missing 'clusters' section",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="fetch_kubeconfig",
            )

        port = cmd.resource_ids.get("api_port")
        for entry in doc.get("clusters", []):
            cluster = entry.get("cluster", {}) if isinstance(entry, dict) else {}
            server = cluster.get("server", "")
            new_server = _rewrite_kind_server(server, cmd.rewrite_server_to, port)
            if new_server != server:
                cluster["server"] = new_server

        yield Result(Kubeconfig(yaml_text=yaml.safe_dump(doc, default_flow_style=False, sort_keys=False)))

    # ------------------------------------------------------------------
    # internals — kind config generation
    # ------------------------------------------------------------------

    def _worker_count(self, spec: NodeSpecification) -> int:
        """Salvaged from ``translate_node_spec``
        (reference-code/seedpod/seedpod/providers/kind.py:151-180): CPU/memory map to worker
        node *count*, not an instance type (kind containers use host resources directly)."""
        key = f"{spec.cpu_cores},{spec.memory_gb}"
        return self.config.node_size_mapping.get(key, self.config.default_workers)

    def _generate_kind_config(self, *, name: str, api_port: int, cmd: CreateInstance) -> str:
        """Salvaged verbatim from ``_generate_kind_config``
        (reference-code/seedpod/seedpod/providers/kind.py:420-511) — see module docstring's
        crown jewel #7 note on ``podSubnet``/``serviceSubnet`` always coming from provider
        config, never ``cmd.pod_cidr``/``cmd.service_cidr``."""
        api_host = self.config.api_server_host
        api_ip = _resolve_host_ip(api_host)
        cert_sans = [api_host, "localhost", "127.0.0.1"]
        if api_ip and api_ip not in cert_sans:
            cert_sans.insert(1, api_ip)
        for san in cmd.tls_sans:
            if san not in cert_sans:
                cert_sans.append(san)
        cert_sans_yaml = "\n".join(f'    - "{san}"' for san in cert_sans)

        control_plane: dict[str, object] = {
            "role": "control-plane",
            "kubeadmConfigPatches": [
                'kind: InitConfiguration\nnodeRegistration:\n  kubeletExtraArgs:\n    node-labels: "ingress-ready=true"\n',
                "kind: ClusterConfiguration\napiServer:\n  certSANs:\n"
                f"{cert_sans_yaml}\netcd:\n  local:\n    extraArgs:\n"
                '      unsafe-no-fsync: "true"\n',
            ],
            "extraPortMappings": [
                {"containerPort": 6443, "hostPort": api_port, "listenAddress": "0.0.0.0", "protocol": "TCP"},
            ],
        }
        for container_port, host_port, protocol in self.config.extra_port_mappings:
            control_plane["extraPortMappings"].append(
                {"containerPort": container_port, "hostPort": host_port, "protocol": protocol}
            )

        nodes: list[dict[str, object]] = [control_plane]
        nodes.extend({"role": "worker"} for _ in range(self._worker_count(cmd.spec.node_specification)))

        kind_config = {
            "kind": "Cluster",
            "apiVersion": "kind.x-k8s.io/v1alpha4",
            "name": name,
            "nodes": nodes,
            "networking": {"podSubnet": self.config.pod_subnet, "serviceSubnet": self.config.service_subnet},
        }
        return yaml.dump(kind_config, default_flow_style=False)

    # ------------------------------------------------------------------
    # internals — port allocation (salvaged as-is)
    # ------------------------------------------------------------------

    async def _existing_cluster_port(self, name: str) -> str | None:
        names = await self._list_kind_clusters()
        if name not in names:
            return None
        port = await self._cluster_port(name)
        return port

    async def _allocate_port(self) -> str:
        """Salvaged from ``_allocate_port``/``_refresh_allocated_ports``
        (reference-code/seedpod/seedpod/providers/kind.py:325-360): scan every existing kind
        cluster's bound port, first free port in the configured range wins. Row 25: range
        exhausted ⇒ ``Permanent(CAPACITY)``."""
        used_ports: set[str] = set()
        for existing_name in await self._list_kind_clusters():
            port = await self._cluster_port(existing_name)
            if port is not None:
                used_ports.add(port)
        for port in range(self.config.port_range_start, self.config.port_range_end + 1):
            if str(port) not in used_ports:
                return str(port)
        raise PermanentError(
            f"kind.create_instance: no available ports in range "
            f"{self.config.port_range_start}-{self.config.port_range_end}",
            code=ErrorCode.CAPACITY,
            provider=self.name,
            command="create_instance",
            detail={"port_range_start": str(self.config.port_range_start), "port_range_end": str(self.config.port_range_end)},
        )

    # ------------------------------------------------------------------
    # internals — kind/docker subprocess plumbing
    # ------------------------------------------------------------------

    async def _list_kind_clusters(self) -> list[str]:
        """Salvaged from ``_list_kind_clusters``
        (reference-code/seedpod/seedpod/providers/kind.py:362-388) MINUS the swallow-to-``[]``
        on ``InfrastructureUnreachableError`` (§5.7.4 — deliberately not pinned): a connectivity
        symptom here now propagates out of ``_run_kind`` uncaught."""
        output = await self._run_kind(["get", "clusters"], timeout=self.config.list_timeout_s, command_name="list_instances.get_clusters")
        return output.splitlines() if output else []

    async def _cluster_port(self, name: str) -> str | None:
        """Salvaged from ``_get_cluster_port``
        (reference-code/seedpod/seedpod/providers/kind.py:390-418)."""
        raw = await self._docker_inspect(
            f"{name}-control-plane", "{{json .NetworkSettings.Ports}}", command_name="port_lookup"
        )
        if raw is None:
            return None
        try:
            ports_json = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        mappings = ports_json.get("6443/tcp") or []
        if not mappings:
            return None
        host_port = mappings[0].get("HostPort")
        return str(host_port) if host_port else None

    async def _is_cluster_running(self, name: str) -> bool:
        """Salvaged from ``_is_cluster_running``
        (reference-code/seedpod/seedpod/providers/kind.py:1066-1122)."""
        raw = await self._docker_inspect(f"{name}-control-plane", "{{.State.Running}}", command_name="reconcile.is_running")
        return raw is not None and raw.strip().lower() == "true"

    async def _docker_inspect(self, container_name: str, fmt: str, *, command_name: str) -> str | None:
        """Shared ``docker inspect --format {fmt} {container}`` helper closing rows 22/23's
        split (module docstring's "deliberate, documented deviation" note): a non-zero exit
        WITHOUT a connectivity symptom is authoritative absence (``None``, row 22 — v1's
        unconditional ``rc != 0 -> not found/not running``, still correct for the common case);
        a non-zero exit WITH a connectivity symptom (timeout, binary missing, or a
        ``TRANSIENT_STDERR_PHRASES`` match in stderr — daemon down, socket refused) raises via
        ``classify_subprocess`` instead (row 23 — the NEW correctness fix, never false-orphan)."""
        result = await self.transport.run(
            ["docker", "inspect", "--format", fmt, container_name], timeout=self.config.docker_timeout_s
        )
        stderr_text = result.stderr.decode(errors="replace").strip()
        if result.timed_out or result.binary_missing or (result.returncode != 0 and _has_transient_phrase(stderr_text)):
            raise classify_subprocess(
                provider=self.name,
                command=command_name,
                host=self.config.api_server_host,
                rc=result.returncode,
                stderr=stderr_text,
                timed_out=result.timed_out,
                binary_missing=result.binary_missing,
                observing_infra=True,
            )
        if result.returncode != 0:
            return None
        return result.stdout.decode(errors="replace").strip()

    async def _run_kind(self, args: list[str], *, timeout: float, command_name: str) -> str:
        """One bounded ``kind`` CLI invocation (no internal retry — H4-H6, the engine's Schedule
        owns retry). Row 21: timeout ⇒ Unreachable/``API_TIMEOUT``. Row 24: non-zero,
        non-connectivity exit ⇒ Permanent/``SCRIPT_FAILED``."""
        result = await self.transport.run(["kind", *args], timeout=timeout)
        stdout_text = result.stdout.decode(errors="replace").strip()
        stderr_text = result.stderr.decode(errors="replace").strip()
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise classify_subprocess(
                provider=self.name,
                command=command_name,
                host=self.config.api_server_host,
                rc=result.returncode,
                stderr=stderr_text,
                timed_out=result.timed_out,
                binary_missing=result.binary_missing,
                observing_infra=True,
            )
        return stdout_text
