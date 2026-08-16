"""seedpod/providers/tart.py — the ``tart`` Provider (Seam C §5.3-5.4, decision-table rows 1-8,
amended by ``docs/design/coherence-review.md`` Conflicts 5-7, 12, and its item 5 "Tart specifics
carried intact inside the adapter").

Machine plane **minus** ``FetchKubeconfig`` (§5.4 plane matrix: "digitalocean/tart ⇒ machine
plane minus FetchKubeconfig" — k3s install + kubeconfig retrieval, once a VM has an address,
flow through the shared ``ssh-k3s`` provider, exactly like DigitalOcean). Talks to the local
``tart`` CLI exclusively over an **injected** ``SubprocessRunner`` (§5.4's construction
contract) — no ``asyncio.create_subprocess_exec``/``create_tracked_subprocess`` call inside
this module (see the ``_run_detached`` docstring below for the one deliberate, documented
exception this rule needs).

Salvaged from ``reference-code/seedpod/seedpod/providers/tart.py`` (``TartProvider``) and
``reference-code/seedpod/seedpod/providers/_tart_cli.py``:

- ``_vm_name`` (159-161) → ``_vm_name`` below, verbatim ``f"{VM_NAME_PREFIX}{slug}"``.
- ``_vm_resources`` (163-180) → ``_vm_resources`` below, verbatim ``"cpu,mem"`` size-key lookup
  into ``node_size_mapping`` with ``defaults``/``spec.disk_gb`` fallback (``translate_node_spec``
  runs INSIDE the impl now, no longer public — mirrors kind/DO).
- ``_tart_cli``'s typed-error edge mapping (``TartDaemonUnreachable``/``TartNotFound``/
  ``TartAlreadyExists``, reference-code .../_tart_cli.py:43-64) is folded directly into this
  module's ``classify_subprocess`` usage plus bespoke pre-mapping in ``_clone``/``_stop``/
  ``_delete``/``_get_ip`` below, one function per decision-table row (1-8): binary-missing/
  timeout ⇒ ``InfrastructureUnreachableError`` via the shared classifier (never a distinct
  ``TartDaemonUnreachable`` type — the taxonomy's one home, Conflict 6); "not found" stderr ⇒
  absence-as-data, folded straight into the calling command's typed Result, never raised; "already
  exists" for OUR deterministic name ⇒ ``adopted_existing=True`` (C-07).
- ``list_clusters`` (511-555) → ``_list_instances`` below: ``seedpod-`` prefix + ``is_local``
  filter, verbatim.
- ``reconcile`` (557-668) → ``_reconcile`` below, reshaped onto the same Phase-B-only pattern
  ``kind.py`` uses (module docstring's "no ``CreateUnmanagedIntent`` analogue" note applies
  identically here: a bare Tart VM name, like a bare kind cluster name, cannot be mapped back to
  a ``cluster_uuid`` without a DB row — v1's own ``reconcile`` never attempted this either, only
  Phase B/backstop): ACTIVE-ish + VM missing ⇒ Orphan; ACTIVE-ish + VM present-but-stopped ⇒
  Orphan; DESTROYED + VM present ⇒ Zombie; DESTROYING + VM missing ⇒ Orphan completion backstop.
  The exact excluded-status set (``_ORPHAN_EXCLUDED_STATES``) mirrors kind.py/digitalocean.py
  verbatim — coherence-review's "DESTROYING+missing ⇒ Orphan completion backstop everywhere"
  demands one uniform set across every machine provider, not v1's provider-local
  ``active_states`` include-list (reference-code .../tart.py:593-598).

Deliberately NOT ported (§5.7.4 "v1 bugs deliberately not pinned", plus scope decisions flagged
here per CLAUDE.md's citation requirement):

- ``_provision_cluster``'s entire background-scheduler dispatch (``get_scheduler()``,
  ``asyncio.create_task``, every ``state_manager.advance_cluster_provisioning`` call — reference-
  code .../tart.py:242-404) — structurally impossible per §5.7.2 (C-03): provider self-scheduling
  and ``state_manager`` upward calls are gone. ``CreateInstance`` runs to completion inside one
  bounded ``execute()`` call; the k3s install/readiness/kubeconfig-fetch steps that follow
  (``CaptureHostKeys``/``InstallK3s``/``ProbeK3s``/``FetchKubeconfig``) are the ENGINE's workflow
  steps against the ``ssh-k3s`` provider, driven once ``InstanceCreated.address`` (or a later
  ``ProbeInstance``'s ``InstanceState.address``) is known — the exact same split DigitalOcean
  uses (§5.4 plane matrix), not a v1 pattern.
- ``get_cluster_status`` (440-445) — v1's own docstring: "reconciliation is the authoritative
  path... return None here." Replaced wholesale by the typed, resource-ids-driven
  ``ProbeInstance``.
- ``__init__``'s synchronous ``_check_base_image_sync``/``tart_cli.find_tart_binary`` fail-fast
  (reference-code .../tart.py:42-145, run via bare ``subprocess.run`` inside ``__init__``) —
  becomes ``check_ready()`` below: async, IO-free construction (§5.4's construction contract),
  called once by the composition root before serving. Same move every provider in this pillar
  makes ("fail at startup, not mid-provision" replaces v1's sync-subprocess-in-``__init__``
  pattern) — not tart-specific.

Genuinely NEW (§5.7.3, not salvage — needs real review, not silent invention): re-invocation
adoption by deterministic VM name (conformance C-07). v1 never retried creates; Tart VM names are
deterministic from ``slug`` (``seedpod-{slug}``, ``VM_NAME_PREFIX``) exactly like kind's cluster
names, not backend-assigned like DO's droplet id — ``_create_instance`` below attempts ``tart
clone`` and, on the CLI's own "already exists" answer for OUR deterministic name (decision row
3), adopts (``adopted_existing=True``) instead of raising, rather than risking a duplicate.

**Coherence-review item 5 — "Tart specifics carried intact inside the adapter"**, preserved:
detached ``run --no-graphics [--rosetta=rosetta] <name>`` (``start_new_session=True``, all three
of stdin/stdout/stderr → DEVNULL, the child never awaited to completion so the VM survives a
seedpod restart — v1 ``_tart_cli.run_detached``, reference-code .../_tart_cli.py:220-253); the
virtio-fs ``rosetta`` magic tag and the 6.8-kernel/``AT_HWCAP3`` pin are documented on
``TartConfig.rosetta_enabled`` and ``config/providers/tart.yml`` (unchanged, not re-litigated
here); base-image-absent ⇒ ``check_ready`` raises ``Permanent(NOT_FOUND)`` (row 2); the 2s ``tart
ip`` poll interval (v1 ``_wait_for_vm_ip``, reference-code .../tart.py:424-438,
``asyncio.sleep(2)``) becomes ``TartConfig.ip_poll_interval_s`` — DATA for the engine's
wait-for-readiness gate (a repeated ``ProbeInstance``, workflow-owned), never slept on inside this
module (Seam C §5.4's "become named interval/settle_seconds parameters ... deleted as sleeps").

**On ``_run_detached`` and the injected-transport rule:** every other tart command in this module
is a normal bounded request/response — clone/set/ip/stop/delete all run to completion quickly and
report ``(returncode, stdout, stderr)``. ``tart run --no-graphics <name>`` does not: it is the
VM's hypervisor process, and blocks in the foreground for the VM's entire lifetime (exactly why
v1 spawned it with ``start_new_session=True`` and never awaited it — the antithesis of "run to
completion"). This module still issues exactly ONE ``self.transport.run(...)`` call for it, same
as every other command (fault injection stays at the transport seam, ``tests/conformance/``,
never ``Mock``/``patch``) — it is the concrete ``SubprocessRunner`` backing this Provider's job
to recognize this specific invocation shape and realize v1's detached-launch semantics (spawn,
DEVNULL stdio, ``start_new_session=True``, report success without waiting for the child to exit)
rather than the bounded collect-all-output shape every other tart command needs. This provider
neither knows nor cares which strategy the transport used; ``tests/conformance/fake_tart.py``
simulates a successful launch by marking the VM ``running`` (with no IP yet) and returning
immediately, mirroring what a correctly-detached spawn would report.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import ClassVar

from seedpod.core.cluster_spec import NodeSpecification
from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
    TransientError,
)
from seedpod.core.reconciliation_intents import OrphanIntent, ReconciliationIntent, ZombieIntent
from seedpod.providers.classify import TART_NOT_FOUND_PHRASES, classify_subprocess
from seedpod.providers.contract import (
    RESOURCE_ALLOCATED,
    CreateInstance,
    DestroyInstance,
    DestroyOutcome,
    DestroyStatus,
    InstanceCreated,
    InstanceState,
    InstanceSummary,
    ListInstances,
    ProbeDestruction,
    ProbeInstance,
    Progress,
    ProviderCommand,
    ProviderEvent,
    Reconcile,
    Result,
    SubprocessResult,
    SubprocessRunner,
)

__all__ = ["TartConfig", "TartProvider", "VM_NAME_PREFIX"]

_HOST = "localhost"  # tart is local-only (v1: single-machine topology, no remote daemon)

VM_NAME_PREFIX = "seedpod-"

# Cluster states (seedpod/core/records.py ClusterState, hyphenated wire values) excluded from
# Phase-B orphan detection: terminal-ish/self-explaining drift. Verbatim from kind.py/
# digitalocean.py — see module docstring's "one uniform set across every machine provider" note.
_ORPHAN_EXCLUDED_STATES = frozenset({"destroyed", "zombie", "destroy-scheduled", "failed", "destroying"})


def _is_not_found(stderr: str) -> bool:
    """Salvaged verbatim from ``_tart_cli._classify_not_found``
    (reference-code/seedpod/seedpod/providers/_tart_cli.py:106-114), reusing the shared
    ``TART_NOT_FOUND_PHRASES`` list (``providers/classify.py``) rather than a private copy."""
    lowered = (stderr or "").lower()
    return any(phrase in lowered for phrase in TART_NOT_FOUND_PHRASES)


def _vm_name(slug: str) -> str:
    """Salvaged verbatim from ``TartProvider._vm_name``
    (reference-code/seedpod/seedpod/providers/tart.py:159-161)."""
    return f"{VM_NAME_PREFIX}{slug}"


@dataclass(frozen=True)
class _TartVM:
    """One entry from ``tart list --format json`` — provider-internal, never crosses the
    contract seam (mirrors v1's ``_tart_cli.TartVM``, reference-code .../_tart_cli.py:24-36,
    trimmed to the fields this adapter actually consumes)."""

    name: str
    source: str  # "local" or "OCI"
    running: bool

    @property
    def is_local(self) -> bool:
        return self.source == "local"


@dataclass(frozen=True)
class TartConfig:
    """IO-free construction data (§5.4's construction contract), loaded by the composition root
    from ``config/providers/tart.yml``.
    """

    base_image_name: str = "local-dev-base-rosetta"
    defaults: Mapping[str, int] = field(default_factory=lambda: {"memory_mb": 4096, "cpu_cores": 4, "disk_gb": 50})
    node_size_mapping: Mapping[str, Mapping[str, int]] = field(default_factory=dict)  # "cpu,mem" -> resources

    # coherence-review item 5: the virtio-fs "rosetta" magic tag + 6.8-kernel/AT_HWCAP3 pin are
    # base-image/host setup concerns (config/providers/tart.yml, CLAUDE.md's "Tart Provider"
    # section) — this flag only controls whether `--rosetta=rosetta` is passed to `tart run`.
    rosetta_enabled: bool = True

    delete_on_destroy: bool = True  # False: destroy stops but never deletes (disk usage grows)

    check_ready_timeout_s: float = 15.0
    list_timeout_s: float = 15.0
    clone_timeout_s: float = 120.0
    set_resources_timeout_s: float = 30.0
    run_timeout_s: float = 10.0  # one bounded attempt to launch the detached VM process
    get_ip_timeout_s: float = 10.0
    stop_timeout_s: float = 60.0
    delete_timeout_s: float = 30.0

    # v1's `tart ip` poll-loop physics constant (reference-code .../tart.py:424-438,
    # `_wait_for_vm_ip`'s `asyncio.sleep(2)`) — preserved as DATA per Seam C §5.4, never slept on
    # here: the engine's wait-for-readiness gate (a repeated ProbeInstance) consumes this as its
    # poll interval.
    ip_poll_interval_s: float = 2.0


class TartProvider:
    name: ClassVar[str] = "tart"
    supported: ClassVar[frozenset[type]] = frozenset(
        {CreateInstance, ProbeInstance, DestroyInstance, ProbeDestruction, ListInstances, Reconcile}
    )

    def __init__(self, config: TartConfig, transport: SubprocessRunner) -> None:
        """IO-free (§5.4's construction contract): stores config and the injected transport
        only. ``transport`` is a ``SubprocessRunner`` — conformance fault injection happens at
        that seam (``tests/conformance/``), never ``Mock``/``patch``."""
        self.config = config
        self.transport = transport

    # ------------------------------------------------------------------
    # startup preflight
    # ------------------------------------------------------------------

    async def check_ready(self) -> None:
        """``tart`` binary reachable AND the configured base image present in ``tart list`` —
        fail at startup, not mid-provision (replaces v1's sync-subprocess-in-``__init__``
        ``_check_base_image_sync``/``find_tart_binary``, reference-code .../tart.py:42-145).
        Row 1: binary missing / daemon hung ⇒ ``InfrastructureUnreachableError`` (``_list_vms``
        raises this via the shared classifier — note this is NOT ``Permanent`` here, unlike
        kind's ``check_ready``: tart's own decision table (row 1, "tart, any") makes binary-
        missing/timeout Unreachable everywhere, including at startup, since a daemon that
        merely isn't running yet is a "cannot determine state" condition, not a "provably
        misconfigured, refuse to start" one). Row 2: base image absent ⇒
        ``Permanent(NOT_FOUND)`` — checked against ANY entry (local or OCI), matching v1's own
        unconditional name scan (reference-code .../tart.py:76-78)."""
        vms = await self._list_vms(command_name="check_ready")
        if any(vm.name == self.config.base_image_name for vm in vms):
            return
        local_images = sorted(vm.name for vm in vms if vm.is_local)
        raise PermanentError(
            f"tart.check_ready: base image {self.config.base_image_name!r} not found in "
            f"`tart list` (local images: {local_images})",
            code=ErrorCode.NOT_FOUND,
            provider=self.name,
            command="check_ready",
            detail={"base_image": self.config.base_image_name},
        )

    # ------------------------------------------------------------------
    # Provider protocol
    # ------------------------------------------------------------------

    def execute(self, cmd: ProviderCommand) -> AsyncIterator[ProviderEvent]:
        """Unsupported ⇒ ``PermanentError(UNSUPPORTED)`` raised synchronously, before any
        backend traffic (§5.4) — not deferred to the first ``__anext__()``."""
        if type(cmd) not in self.supported:
            raise PermanentError(
                f"tart: unsupported command {type(cmd).__name__}",
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
        raise AssertionError(f"unreachable: {cmd!r} is in `supported` but has no handler")  # pragma: no cover

    # ------------------------------------------------------------------
    # commands — machine plane
    # ------------------------------------------------------------------

    async def _create_instance(self, cmd: CreateInstance) -> AsyncIterator[ProviderEvent]:
        name = _vm_name(cmd.slug)
        resource_ids = {"tart_vm_name": name, "tart_base_image": self.config.base_image_name}

        # Tag-before-boot analog (§5.3): the identity is deterministic from `slug` alone, known
        # BEFORE any backend mutation — a mid-create death still carries an id for
        # undo_for -> DestroyInstance to compensate (mirrors kind.py's identical reasoning).
        yield Progress(phase=RESOURCE_ALLOCATED, data={"resource_ids": resource_ids})

        adopted = await self._clone(self.config.base_image_name, name)
        if not adopted:
            resources = self._vm_resources(cmd.spec.node_specification)
            await self._set_resources(
                name,
                memory_mb=resources["memory_mb"],
                cpu_cores=resources["cpu_cores"],
                disk_gb=resources["disk_gb"],
            )
            await self._run_detached(name)

        # One bounded IP read — NOT a wait loop (the loop is gone; TartConfig.ip_poll_interval_s
        # is DATA for the engine's own wait-for-readiness gate). A freshly-booted VM very likely
        # has no IP yet; that is expected, not an error (row 5).
        address = await self._get_ip(name, command_name="create_instance.get_ip")

        yield Result(
            InstanceCreated(
                resource_ids=resource_ids,
                address=address,
                effective_pod_cidr=cmd.pod_cidr,
                effective_service_cidr=cmd.service_cidr,
                adopted_existing=adopted,
            )
        )

    async def _probe_instance(self, cmd: ProbeInstance) -> AsyncIterator[ProviderEvent]:
        name = cmd.resource_ids.get("tart_vm_name")
        if not name:
            yield Result(InstanceState(phase="absent", address=None, detail="no tart_vm_name on record"))
            return
        vms = await self._list_vms(command_name="probe_instance")
        vm = next((v for v in vms if v.name == name), None)
        if vm is None:
            # Row: authoritative absence (VM genuinely not in `tart list`) — never conflated
            # with "cannot reach the daemon" (crown jewel #1).
            yield Result(InstanceState(phase="absent", address=None, detail="VM not found"))
            return
        if not vm.running:
            yield Result(InstanceState(phase="stopped", address=None, detail="VM present but not running"))
            return
        ip = await self._get_ip(name, command_name="probe_instance.get_ip")
        if ip is None:
            # Row 5: rc≠0 (or empty), VM present ⇒ "no IP yet" DATA, never an error.
            yield Result(InstanceState(phase="provisioning", address=None, detail="VM running, no IP assigned yet"))
            return
        yield Result(InstanceState(phase="running", address=ip, detail="running"))

    async def _destroy_instance(self, cmd: DestroyInstance) -> AsyncIterator[ProviderEvent]:
        name = cmd.resource_ids.get("tart_vm_name") or _vm_name(cmd.slug)
        found = await self._stop(name)
        if not found:
            # Row 7: TartNotFound on stop/delete ⇒ absence-as-data, destroy succeeds.
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED, note="VM was already absent"))
            return
        if self.config.delete_on_destroy:
            try:
                await self._delete(name)
            except TransientError as e:
                # Row 8: delete-after-successful-stop failure ⇒ Transient/RESOURCE_BUSY, folded
                # into DESTROYING vocabulary (gate retries) rather than failing the step outright
                # — the VM IS stopped, so this is progress, not failure.
                yield Result(DestroyOutcome(status=DestroyStatus.DESTROYING, note=f"VM stopped; delete failed: {e}"))
                return
        yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED))

    async def _probe_destruction(self, cmd: ProbeDestruction) -> AsyncIterator[ProviderEvent]:
        name = cmd.resource_ids.get("tart_vm_name")
        if not name:
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED, note="no resources to probe"))
            return
        vms = await self._list_vms(command_name="probe_destruction")
        if not any(vm.name == name for vm in vms):
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED))
            return
        # tart's own stop+delete is synchronous (no separate "destroying" window this command
        # would observe beyond row 8's RESOURCE_BUSY retry, which is the engine Schedule's job on
        # DestroyInstance itself) — still present after a destroy attempt is a genuine stuck
        # failure, mirroring kind.py's identical "container still present" mapping.
        yield Result(DestroyOutcome(status=DestroyStatus.DESTROY_FAILED, error="tart VM still present", stuck_resources=(name,)))

    async def _list_instances(self, cmd: ListInstances) -> AsyncIterator[ProviderEvent]:
        vms = await self._list_vms(command_name="list_instances")
        summaries = tuple(
            InstanceSummary(name=vm.name, resource_ids={"tart_vm_name": vm.name})
            for vm in vms
            if vm.is_local and vm.name.startswith(VM_NAME_PREFIX)
        )
        yield Result(summaries)

    async def _reconcile(self, cmd: Reconcile) -> AsyncIterator[ProviderEvent]:
        """Salvaged mapping from ``reconcile``
        (reference-code/seedpod/seedpod/providers/tart.py:557-668), reshaped onto the shared
        Phase-B-only pattern (module docstring's kind.py note). ``_list_vms`` raises
        ``InfrastructureUnreachableError`` on a genuine connectivity symptom (never a v1-style
        swallow), which propagates out of this generator uncaught: the engine skips every
        cluster in ``cmd.clusters`` and touches nothing (crown jewel #1)."""
        vms = await self._list_vms(command_name="reconcile")
        by_name = {vm.name: vm for vm in vms if vm.is_local}
        intents: list[ReconciliationIntent] = []

        for snapshot in cmd.clusters:
            resolved_name = snapshot.resource_ids.get("tart_vm_name") or _vm_name(snapshot.slug)

            if snapshot.status == "destroyed":
                if resolved_name in by_name:
                    intents.append(ZombieIntent(cluster_id=snapshot.cluster_uuid, droplet_id=resolved_name, droplet_ip=None))
                continue

            if snapshot.status in _ORPHAN_EXCLUDED_STATES:
                continue

            vm = by_name.get(resolved_name)
            if vm is None:
                intents.append(
                    OrphanIntent(cluster_id=snapshot.cluster_uuid, reason=f"Tart VM '{resolved_name}' not found")
                )
            elif not vm.running:
                intents.append(
                    OrphanIntent(
                        cluster_id=snapshot.cluster_uuid,
                        reason=f"Tart VM '{resolved_name}' present but stopped — treating as orphan",
                    )
                )

        # DESTROYING+missing completion backstop (single intent per cluster, everywhere).
        for snapshot in cmd.clusters:
            if snapshot.status != "destroying":
                continue
            resolved_name = snapshot.resource_ids.get("tart_vm_name") or _vm_name(snapshot.slug)
            if resolved_name not in by_name:
                intents.append(
                    OrphanIntent(
                        cluster_id=snapshot.cluster_uuid,
                        reason=f"Tart VM '{resolved_name}' already gone — destruction completed",
                    )
                )

        yield Result(tuple(intents))

    # ------------------------------------------------------------------
    # internals — node spec translation
    # ------------------------------------------------------------------

    def _vm_resources(self, spec: NodeSpecification) -> dict[str, int]:
        """Salvaged verbatim from ``_vm_resources``
        (reference-code/seedpod/seedpod/providers/tart.py:163-180)."""
        key = f"{spec.cpu_cores},{spec.memory_gb}"
        entry = self.config.node_size_mapping.get(key) or {}
        defaults = self.config.defaults
        return {
            "memory_mb": int(entry.get("memory_mb", defaults.get("memory_mb", 4096))),
            "cpu_cores": int(entry.get("cpu_cores", defaults.get("cpu_cores", 4))),
            "disk_gb": int(entry.get("disk_gb", spec.disk_gb or defaults.get("disk_gb", 50))),
        }

    # ------------------------------------------------------------------
    # internals — tart CLI plumbing
    # ------------------------------------------------------------------

    async def _run_tart(self, args: list[str], *, timeout: float, command_name: str) -> SubprocessResult:
        """One bounded ``tart`` CLI invocation via the injected transport (no internal retry —
        H4-H6, the engine's Schedule owns retry). Returns the raw result; callers interpret
        rc/stderr per their own decision-table row rather than a single shared classification —
        ``tart``'s idiomatic non-zero exits ("already exists", "not found", "not running") are
        pre-mapped, non-error answers ``classify_subprocess`` must never see (CLAUDE.md: "caller
        pre-mapped it to absence-as-data")."""
        return await self.transport.run(["tart", *args], timeout=timeout)

    def _classify(self, result: SubprocessResult, *, command_name: str) -> ProviderError:
        return classify_subprocess(
            provider=self.name,
            command=command_name,
            host=_HOST,
            rc=result.returncode,
            stderr=result.stderr.decode(errors="replace"),
            timed_out=result.timed_out,
            binary_missing=result.binary_missing,
            observing_infra=True,  # tart is a machine provider (row 1: "tart, any")
        )

    async def _list_vms(self, *, command_name: str) -> list[_TartVM]:
        """``tart list --format json`` (v1 ``_tart_cli.list_vms``, reference-code
        .../_tart_cli.py:130-156). Row 1: binary missing / timeout / non-zero exit ⇒
        ``InfrastructureUnreachableError`` — this is the read every other command in this
        module funnels through for presence checks, so absence is only ever reported once this
        call has succeeded (never conflated with "cannot reach the daemon", crown jewel #1)."""
        result = await self._run_tart(["list", "--format", "json"], timeout=self.config.list_timeout_s, command_name=command_name)
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify(result, command_name=command_name)
        try:
            data = json.loads(result.stdout.decode(errors="replace"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            # Garbage body from a machine provider's own state read ⇒ Unreachable (v1: "treat
            # like timeout" — same rule DO's classify_http(malformed_body=True) applies).
            raise InfrastructureUnreachableError(
                f"tart.{command_name}: `tart list` returned non-JSON output",
                code=ErrorCode.MALFORMED_RESPONSE,
                provider=self.name,
                command=command_name,
                detail={"stdout_prefix": result.stdout[:200].decode(errors="replace")},
                host=_HOST,
            ) from e
        return [_TartVM(name=entry["Name"], source=entry.get("Source", ""), running=bool(entry.get("Running", False))) for entry in data]

    async def _clone(self, source: str, name: str) -> bool:
        """``tart clone <source> <name>`` (v1 ``_tart_cli.clone``, reference-code
        .../_tart_cli.py:168-187). Returns ``True`` iff adopted (VM already existed under OUR
        deterministic name — row 3, C-07); ``False`` iff freshly cloned. Row 4: the SOURCE image
        missing ⇒ ``Permanent(NOT_FOUND)`` — checked only once "already exists" (a completely
        different symptom about the TARGET name) has been ruled out, preserving v1's own
        ordering."""
        result = await self._run_tart(["clone", source, name], timeout=self.config.clone_timeout_s, command_name="create_instance.clone")
        if result.timed_out or result.binary_missing:
            raise self._classify(result, command_name="create_instance.clone")
        if result.returncode == 0:
            return False
        stderr_text = result.stderr.decode(errors="replace")
        lowered = stderr_text.lower()
        if "already exists" in lowered or "already has a vm" in lowered:
            return True  # row 3: adoption, zero further backend mutation this call
        if _is_not_found(stderr_text):
            raise PermanentError(
                f"tart.create_instance: source image {source!r} not found",
                code=ErrorCode.NOT_FOUND,
                provider=self.name,
                command="create_instance.clone",
                detail={"source": source, "stderr": stderr_text},
            )
        raise self._classify(result, command_name="create_instance.clone")

    async def _set_resources(self, name: str, *, memory_mb: int, cpu_cores: int, disk_gb: int) -> None:
        """``tart set <name> --memory M --cpu C --disk-size D`` (v1 ``_tart_cli.set_resources``,
        reference-code .../_tart_cli.py:190-217)."""
        args = ["set", name, "--memory", str(memory_mb), "--cpu", str(cpu_cores), "--disk-size", str(disk_gb)]
        result = await self._run_tart(args, timeout=self.config.set_resources_timeout_s, command_name="create_instance.set_resources")
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify(result, command_name="create_instance.set_resources")

    async def _run_detached(self, name: str) -> None:
        """``tart run --no-graphics [--rosetta=rosetta] <name>``, detached — see the module
        docstring's "On ``_run_detached`` and the injected-transport rule" section for why this
        is still exactly one ``transport.run()`` call despite the real launch never being
        awaited to completion."""
        args = ["run", "--no-graphics"]
        if self.config.rosetta_enabled:
            args.append("--rosetta=rosetta")
        args.append(name)
        result = await self._run_tart(args, timeout=self.config.run_timeout_s, command_name="create_instance.run")
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise self._classify(result, command_name="create_instance.run")

    async def _get_ip(self, name: str, *, command_name: str) -> str | None:
        """``tart ip <name>`` (v1 ``_tart_cli.get_ip``, reference-code .../_tart_cli.py:256-271).
        Row 5: any non-zero exit that is not a genuine connectivity symptom is "no IP yet" DATA
        (``None``), never raised — including a not-found race, since every caller here already
        confirmed the VM's presence immediately before calling this (``_create_instance``: the
        VM just cloned; ``_probe_instance``: a name just found in a freshly-fetched VM list) —
        matching v1's own "typical transient: not found" fallback, which returned ``None``
        rather than raising for anything but its own explicit not-found phrase match."""
        result = await self._run_tart(["ip", name], timeout=self.config.get_ip_timeout_s, command_name=command_name)
        if result.timed_out or result.binary_missing:
            raise self._classify(result, command_name=command_name)
        if result.returncode == 0:
            ip = result.stdout.decode(errors="replace").strip()
            return ip or None
        return None

    async def _stop(self, name: str) -> bool:
        """``tart stop <name>`` (v1 ``_tart_cli.stop``, reference-code .../_tart_cli.py:274-288).
        Row 6: "not running"/"already stopped" ⇒ idempotent success. Row 7: "not found" ⇒
        absence-as-data. Returns ``True`` iff the VM was found (stopped now or already stopped);
        ``False`` iff it was already absent."""
        result = await self._run_tart(["stop", name], timeout=self.config.stop_timeout_s, command_name="destroy_instance.stop")
        if result.timed_out or result.binary_missing:
            raise self._classify(result, command_name="destroy_instance.stop")
        if result.returncode == 0:
            return True
        stderr_text = result.stderr.decode(errors="replace")
        if _is_not_found(stderr_text):
            return False  # row 7
        lowered = stderr_text.lower()
        if "not running" in lowered or "already stopped" in lowered:
            return True  # row 6
        raise self._classify(result, command_name="destroy_instance.stop")

    async def _delete(self, name: str) -> None:
        """``tart delete <name>`` (v1 ``_tart_cli.delete``, reference-code
        .../_tart_cli.py:291-300). Idempotent on absence. Row 8: any OTHER failure (the VM is
        confirmed stopped by the time this runs) classifies ``Transient(RESOURCE_BUSY)`` — never
        ``Permanent`` — so ``_destroy_instance`` can fold it into ``DestroyOutcome(DESTROYING)``
        (gate retries) rather than failing the whole destroy step over a disk/lock hiccup."""
        result = await self._run_tart(["delete", name], timeout=self.config.delete_timeout_s, command_name="destroy_instance.delete")
        if result.timed_out or result.binary_missing:
            raise self._classify(result, command_name="destroy_instance.delete")
        if result.returncode == 0:
            return
        stderr_text = result.stderr.decode(errors="replace")
        if _is_not_found(stderr_text):
            return  # idempotent
        raise TransientError(
            f"tart.destroy_instance: delete failed for {name!r} after stop succeeded: {stderr_text.strip()}",
            code=ErrorCode.RESOURCE_BUSY,
            provider=self.name,
            command="destroy_instance.delete",
            detail={"stderr": stderr_text},
        )
