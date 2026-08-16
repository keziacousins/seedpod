"""seedpod/providers/orbstack.py — the ``orbstack`` Provider (Seam C §5.3-5.4, decision-table
row 26, amended by ``docs/design/coherence-review.md`` Conflicts 5-7, 12).

Machine plane **including** ``FetchKubeconfig`` (§5.4 plane matrix: "kind/orbstack ⇒ machine
plane incl. FetchKubeconfig"). Talks to the local ``kubectl`` binary exclusively over an
**injected** ``SubprocessRunner`` (§5.4's construction contract) — no
``asyncio.create_subprocess_exec``/``create_tracked_subprocess`` call inside this module.

**The one structural fact every method here follows from:** OrbStack provides a single,
persistent, pre-existing built-in Kubernetes cluster (``kubectl config get-contexts`` context
``"orbstack"``) that this provider never creates and never destroys — only observes. There is no
backend-assigned identity and no boot, so ``ProbeInstance``/``Reconcile`` never have an
authoritative "this cluster was destroyed" signal to report as data the way a missing droplet or
stopped container does — see ``_verify_reachable`` below. ``kubectl cluster-info --context
orbstack`` (the one call every read here funnels through) can still fail two structurally
different ways, and the shared ``classify_subprocess`` (``providers/classify.py``) is left to
split them exactly as it does everywhere else: a connectivity symptom (timeout, missing binary,
"connection refused" — OrbStack.app not currently running) ⇒ ``InfrastructureUnreachableError``;
a clean non-zero exit with no connectivity phrase (the ``orbstack`` context missing from the
local kubeconfig entirely — OrbStack never installed/configured on this machine) ⇒
``PermanentError(SCRIPT_FAILED)``. Both are RAISES, never a ``phase="absent"`` Result — neither
is evidence the shared singleton cluster itself is "gone", only that *this machine* can't
currently confirm it, which is exactly why ``_reconcile`` below never emits an intent for
either.

Salvaged from ``reference-code/seedpod/seedpod/providers/orbstack.py`` (``OrbStackProvider``):

- ``_verify_orbstack_running`` (236-254) → ``_verify_reachable`` below: ``kubectl cluster-info
  --context orbstack``, one bounded attempt (the v1 background-provisioning retry context this
  call ran inside is gone — see "deliberately NOT ported" below).
- ``_get_kubeconfig`` (255-297) → ``_fetch_kubeconfig`` below: ``kubectl config view --raw
  --minify --context orbstack``, in-memory YAML rewrite. Crown jewel #6's **orbstack variant** —
  the one that preserves the SOURCE port via a backreference rather than substituting an
  allocated one (TLS cert validity: OrbStack's cert is only valid for the port it was issued on).
  Salvaged regex verbatim: ``r"https://(127\\.0\\.0\\.1|localhost)(:\\d+)"`` →
  ``f"https://{rewrite_to}\\2"`` (unlike kind, ``0.0.0.0`` is never a match — v1 never wrote
  that host into an OrbStack kubeconfig).
- ``destroy_cluster`` (423-448) → ``_destroy_instance`` below, verbatim no-op: the v1 body never
  calls ``_verify_orbstack_running`` at all and unconditionally returns
  ``{"status": "destroyed", "note": "OrbStack cluster preserved, only deployed resources cleaned
  up"}`` — carried forward as ``DestroyOutcome(DESTROYED, note=...)`` with **zero transport
  calls**, matching v1's own "nothing to destroy at the infra level" comment.
- ``list_clusters`` (449-469) → ``_list_instances`` below: the single built-in entry, MINUS the
  swallow-to-``[]`` on any exception (§5.7.4 — same fix already applied to ``kind.py``'s
  ``list_clusters``: a connectivity failure now raises rather than lying with an empty list).
- ``reconcile`` (470-491) → ``_reconcile`` below, salvaged **exactly**: reachable ⇒ empty intents
  unconditionally (the module docstring's "orbstack never orphans" — no per-``ClusterSnapshot``
  analysis at all, unlike every other machine provider's Phase A/B loop); unreachable ⇒ raise
  (v1's ``ProviderReconciliationResult.unreachable(...)`` becomes the uniform raise, Conflict 5).

Deliberately NOT ported (§5.7.4 "v1 bugs deliberately not pinned", plus scope decisions flagged
here per CLAUDE.md's citation requirement):

- **Background-scheduler dispatch** (``create_cluster``'s ``get_scheduler()``/
  ``asyncio.create_task`` fire-and-forget of ``_provision_cluster``, lines 92-234) —
  structurally impossible per §5.7.2 (C-03): ``CreateInstance`` runs the (trivial, since there
  is nothing to boot) verification to completion inside one bounded ``execute()`` call.
- **Traefik deployment** (``_deploy_traefik``/``_wait_for_traefik``, lines 317-405) — leaves
  provider code entirely, exactly like kind's identical removal: it becomes a ``kubectl-apply``
  workflow step over ``traefik-orbstack.yaml`` with a non-fatal ``KubeProbeRollout`` gate
  (§5.4 plane-matrix note; **row 26** — "rollout slow after apply" is explicitly **not an
  error**, `ProbeRollout ⇒ complete=False`, a kubectl-provider + workflow-config concern, not
  this module's).
- **``get_cluster_status``** (406-422) — replaced wholesale by the typed, resource-ids-driven
  ``ProbeInstance``.
- **``translate_node_spec``** (81-90) — OrbStack node specs were always informational-only
  placeholders (``ProviderSpecificConfig(instance_type="orbstack", region="local", ...)``); v2's
  ``CreateInstance.spec`` is accepted but unused here, same as v1's own "informational only" note.

Genuinely NEW (§5.7.3, not salvage): unlike every other machine provider, ``CreateInstance``
**always** adopts (``adopted_existing=True`` unconditionally) — there is no "freshly created"
state to distinguish from, since this provider never mutates backend state at all. Re-invocation
safety (C-07) is trivially total: the identity (``{"orbstack_context": ...}``) is a fixed
constant, not backend-assigned, so a second ``CreateInstance`` for any ``cluster_uuid`` computes
byte-identical ``resource_ids`` with zero risk of a duplicate (there is nothing to duplicate).

**Deliberate, loudly-documented deviation: no "absent" ``InstanceState`` phase.** Every other
machine provider's ``ProbeInstance``/``Reconcile`` distinguishes authoritative absence (backend
said "no") from unreachability (backend didn't answer) — crown jewel #1. OrbStack has no backend
call shaped like "absent" (there is no droplet/container id to look up and fail to find) — the
one call every read here makes, ``kubectl cluster-info --context orbstack``, only ever answers
"reachable" or "raise" (connectivity symptom ⇒ Unreachable, structural context-missing ⇒
Permanent, per the module docstring's opening paragraph), never a third "positively confirmed
gone" branch. So ``check_ready``/``_create_instance``/``_probe_instance``/``_list_instances``/
``_reconcile`` all fold through the one shared ``_verify_reachable``/``_run_kubectl`` helper below
and let ``classify_subprocess`` raise whichever of the two it decides — never an authoritative
``phase="absent"`` Result. This is the direct mechanical reason ``_reconcile`` below can honestly
promise "never orphans": there is no backend signal this provider is willing to call "gone" for a
resource nothing here ever created.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import ClassVar

import yaml

from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.providers.classify import classify_subprocess
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

__all__ = ["OrbstackConfig", "OrbstackProvider", "ORBSTACK_CONTEXT"]

# Salvaged verbatim from v1's module-level constant (reference-code .../orbstack.py:39).
ORBSTACK_CONTEXT = "orbstack"

_DESTROY_NOTE = "OrbStack cluster preserved, only deployed resources cleaned up"

# Salvaged verbatim regex from ``_get_kubeconfig`` (reference-code .../orbstack.py:283-288) —
# crown jewel #6's orbstack variant: host-only substitution, port preserved via backreference
# (unlike kind's variant, which also substitutes the port).
_ORBSTACK_SERVER_RE = re.compile(r"https://(127\.0\.0\.1|localhost)(:\d+)")


def _rewrite_orbstack_server(server: str, rewrite_to: str) -> str:
    """Salvaged verbatim from ``_get_kubeconfig`` (reference-code .../orbstack.py:283-288): only
    ``127.0.0.1``/``localhost`` match (never ``0.0.0.0`` — v1 never wrote that host into an
    OrbStack kubeconfig); the port group is echoed back via ``\\2``, never replaced, because
    OrbStack's TLS certificate is only valid for the port it was actually issued on."""
    if not rewrite_to or not server:
        return server
    if not _ORBSTACK_SERVER_RE.search(server):
        return server
    return _ORBSTACK_SERVER_RE.sub(f"https://{rewrite_to}\\2", server, count=1)


@dataclass(frozen=True)
class OrbstackConfig:
    """IO-free construction data (§5.4's construction contract), loaded by the composition root
    from ``config/providers/orbstack.yml``.
    """

    context: str = ORBSTACK_CONTEXT  # config/providers/orbstack.yml: kubectl_context
    host: str = "localhost"  # config/providers/orbstack.yml: api_server.host
    # config/providers/orbstack.yml: public_hostname — "Separate from api_server.host since the
    # K8s API cert only covers localhost/k8s.orb.local". v1 (reference-code .../orbstack.py:130)
    # stores `self.config.get("public_hostname") or api_host` into
    # `provider_resource_ids["provider_hostname"]`; four v1 modules (api/presets.py,
    # api/clusters.py, data/models.py, orchestrator/cluster_manager.py) prefer that value over
    # public_ip for externally-reachable service/ingress URLs (the "hostname strategy") because
    # the K8s API host and the network-reachable hostname legitimately differ for OrbStack.
    # None ⇒ falls back to `host` (mirrors v1's `or api_host`).
    public_hostname: str | None = None

    check_ready_timeout_s: float = 5.0
    cluster_info_timeout_s: float = 10.0
    fetch_kubeconfig_timeout_s: float = 10.0


class OrbstackProvider:
    name: ClassVar[str] = "orbstack"
    supported: ClassVar[frozenset[type]] = frozenset(
        {CreateInstance, ProbeInstance, DestroyInstance, ProbeDestruction, ListInstances, Reconcile, FetchKubeconfig}
    )

    def __init__(self, config: OrbstackConfig, transport: SubprocessRunner) -> None:
        """IO-free (§5.4's construction contract): stores config and the injected transport
        only. ``transport`` is a ``SubprocessRunner`` — conformance fault injection happens at
        that seam (``tests/conformance/``), never ``Mock``/``patch``."""
        self.config = config
        self.transport = transport

    # ------------------------------------------------------------------
    # startup preflight
    # ------------------------------------------------------------------

    async def check_ready(self) -> None:
        """``kubectl`` on PATH AND the OrbStack cluster reachable — fail at startup, not
        mid-provision. Binary missing ⇒ ``Permanent(NOT_FOUND)`` (refuse to start, mirrors
        kind's/ssh-k3s's identical check_ready special-case); anything else that keeps
        ``kubectl cluster-info`` from answering funnels through the shared classifier (module
        docstring's opening paragraph): a connectivity symptom (API down, timeout) ⇒
        ``InfrastructureUnreachableError`` — a not-yet-started OrbStack.app is a "come back
        later" condition, not a "provably misconfigured, refuse to start forever" one, so it
        does NOT get the same ``Permanent`` treatment an actually-missing binary does here — but
        a clean non-zero exit (the ``orbstack`` context missing from the local kubeconfig
        entirely) still legitimately IS the "refuse to start" case, and correctly raises
        ``Permanent(SCRIPT_FAILED)`` via that same shared path."""
        result = await self.transport.run(
            ["kubectl", "cluster-info", "--context", self.config.context], timeout=self.config.check_ready_timeout_s
        )
        if result.binary_missing:
            raise PermanentError(
                "orbstack.check_ready: required binary 'kubectl' not found on PATH",
                code=ErrorCode.NOT_FOUND,
                provider=self.name,
                command="check_ready",
                detail={"binary": "kubectl"},
            )
        if result.timed_out or result.returncode != 0:
            raise classify_subprocess(
                provider=self.name,
                command="check_ready",
                host=self.config.host,
                rc=result.returncode,
                stderr=result.stderr.decode(errors="replace"),
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
                f"orbstack: unsupported command {type(cmd).__name__}",
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
        """'Create' == verify + adopt the single pre-existing cluster (module docstring's
        genuinely-NEW note): the identity is a fixed constant, known before any backend call, so
        ``RESOURCE_ALLOCATED`` is emitted first (tag-before-boot analog, even though there is no
        boot) and ``adopted_existing`` is unconditionally ``True``. CIDRs are echoed back
        unchanged (unlike kind, OrbStack's built-in cluster owns its own networking — nothing
        here overrides ``cmd.pod_cidr``/``cmd.service_cidr``, mirrors tart.py's identical
        echo-through)."""
        # provider_hostname carried forward into resource_ids (v1's storage location, verbatim
        # key name) — the network-reachable hostname downstream manifest/ingress URL generation
        # needs, distinct from `address`/api_server.host (module docstring's public_hostname
        # note above).
        resource_ids = {
            "orbstack_context": self.config.context,
            "provider_hostname": self.config.public_hostname or self.config.host,
        }
        yield Progress(phase=RESOURCE_ALLOCATED, data={"resource_ids": resource_ids})

        await self._verify_reachable(command_name="create_instance")

        yield Result(
            InstanceCreated(
                resource_ids=resource_ids,
                address=self.config.host,
                effective_pod_cidr=cmd.pod_cidr,
                effective_service_cidr=cmd.service_cidr,
                adopted_existing=True,
            )
        )

    async def _probe_instance(self, cmd: ProbeInstance) -> AsyncIterator[ProviderEvent]:
        """No ``"absent"`` phase — see module docstring. Reachable ⇒ ``"running"``; anything else
        raises."""
        await self._verify_reachable(command_name="probe_instance")
        yield Result(InstanceState(phase="running", address=self.config.host, detail="OrbStack cluster reachable"))

    async def _destroy_instance(self, cmd: DestroyInstance) -> AsyncIterator[ProviderEvent]:
        """Salvaged verbatim from ``destroy_cluster`` (reference-code .../orbstack.py:423-448):
        unconditional no-op, **zero transport calls** — the cluster is never actually destroyed,
        so there is nothing to verify or fail on (v1's own comment: "nothing to destroy at the
        infra level")."""
        yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED, note=_DESTROY_NOTE))

    async def _probe_destruction(self, cmd: ProbeDestruction) -> AsyncIterator[ProviderEvent]:
        """Mirrors ``_destroy_instance``'s no-op: destruction is instantaneous and always
        already complete, so there is never a ``DESTROYING``/``DESTROY_FAILED`` window here for
        the engine's gate to observe."""
        yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED, note=_DESTROY_NOTE))

    async def _list_instances(self, cmd: ListInstances) -> AsyncIterator[ProviderEvent]:
        """Salvaged from ``list_clusters`` (reference-code .../orbstack.py:449-469) MINUS the
        swallow-to-``[]`` on any exception (§5.7.4, same fix as kind.py's ``list_clusters``): a
        connectivity failure now raises rather than lying with an empty list."""
        await self._verify_reachable(command_name="list_instances")
        yield Result((InstanceSummary(name=self.config.context, resource_ids={"orbstack_context": self.config.context}),))

    async def _reconcile(self, cmd: Reconcile) -> AsyncIterator[ProviderEvent]:
        """Salvaged exactly from ``reconcile`` (reference-code .../orbstack.py:470-491): reachable
        ⇒ empty intents, **unconditionally** — no per-``ClusterSnapshot`` Orphan/Zombie analysis
        at all (module docstring's "orbstack never orphans"; see its closing paragraph for why
        this is the only honest answer this provider can give). Unreachable ⇒ raise
        ``InfrastructureUnreachableError`` (Conflict 5): the engine skips every cluster in
        ``cmd.clusters`` and touches nothing."""
        await self._verify_reachable(command_name="reconcile")
        yield Result(())

    # ------------------------------------------------------------------
    # commands — orbstack's FetchKubeconfig variant
    # ------------------------------------------------------------------

    async def _fetch_kubeconfig(self, cmd: FetchKubeconfig) -> AsyncIterator[ProviderEvent]:
        """Salvaged from ``_get_kubeconfig`` (reference-code .../orbstack.py:255-297):
        ``kubectl config view --raw --minify --context orbstack`` — a local kubeconfig-file read,
        not an API call (no network round trip), then the crown-jewel-#6 port-preserving
        rewrite. In-memory, never sed-over-SSH."""
        context = cmd.resource_ids.get("orbstack_context") or self.config.context
        raw = await self._run_kubectl(
            ["config", "view", "--raw", "--minify", "--context", context],
            timeout=self.config.fetch_kubeconfig_timeout_s,
            command_name="fetch_kubeconfig",
        )
        try:
            doc = yaml.safe_load(raw)
        except yaml.YAMLError as e:
            raise PermanentError(
                f"orbstack.fetch_kubeconfig: kubeconfig for context {context!r} is not valid YAML: {e}",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="fetch_kubeconfig",
            ) from e
        if not isinstance(doc, dict) or "clusters" not in doc:
            raise PermanentError(
                f"orbstack.fetch_kubeconfig: kubeconfig for context {context!r} missing 'clusters' section",
                code=ErrorCode.INVALID_INPUT,
                provider=self.name,
                command="fetch_kubeconfig",
            )

        for entry in doc.get("clusters", []):
            cluster = entry.get("cluster", {}) if isinstance(entry, dict) else {}
            server = cluster.get("server", "")
            new_server = _rewrite_orbstack_server(server, cmd.rewrite_server_to)
            if new_server != server:
                cluster["server"] = new_server

        yield Result(Kubeconfig(yaml_text=yaml.safe_dump(doc, default_flow_style=False, sort_keys=False)))

    # ------------------------------------------------------------------
    # internals — kubectl subprocess plumbing
    # ------------------------------------------------------------------

    async def _verify_reachable(self, *, command_name: str) -> None:
        """Salvaged from ``_verify_orbstack_running`` (reference-code .../orbstack.py:236-254),
        minus the retry context it used to run inside (v1's background-provisioning task; the
        engine's Schedule owns retry now, H4-H6). Raises for any failure — never returns a
        "not reachable" value — via ``_run_kubectl``'s shared classification split (module
        docstring's opening paragraph): a connectivity symptom ⇒
        ``InfrastructureUnreachableError``; a structural context-missing exit ⇒
        ``PermanentError(SCRIPT_FAILED)``. Neither is treated as authoritative absence-as-data
        here — see the module docstring's "no absent phase" note."""
        await self._run_kubectl(
            ["cluster-info", "--context", self.config.context], timeout=self.config.cluster_info_timeout_s, command_name=command_name
        )

    async def _run_kubectl(self, args: list[str], *, timeout: float, command_name: str) -> str:
        """One bounded ``kubectl`` CLI invocation via the injected transport (no internal retry
        — H4-H6, the engine's Schedule owns retry). Every failure funnels through the shared
        classifier with ``observing_infra=True``, its ordinary split (``providers/classify.py``):
        binary missing / timeout / connectivity phrase ⇒ Unreachable; any other clean non-zero
        exit (e.g. "context does not exist") ⇒ ``Permanent(SCRIPT_FAILED)`` — this provider never
        treats a clean non-zero ``kubectl`` exit as authoritative absence-as-data the way kind's
        docker inspect does (module docstring's "no absent phase" note); it is always one of
        these two raises instead."""
        result = await self.transport.run(["kubectl", *args], timeout=timeout)
        if result.timed_out or result.binary_missing or result.returncode != 0:
            raise classify_subprocess(
                provider=self.name,
                command=command_name,
                host=self.config.host,
                rc=result.returncode,
                stderr=result.stderr.decode(errors="replace"),
                timed_out=result.timed_out,
                binary_missing=result.binary_missing,
                observing_infra=True,
            )
        return result.stdout.decode(errors="replace")
