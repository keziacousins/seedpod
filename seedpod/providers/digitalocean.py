"""seedpod/providers/digitalocean.py — the ``digitalocean`` Provider (Seam C §5.3-5.4,
decision-table rows 9-15, amended by ``docs/design/coherence-review.md`` Conflicts 5-7).

Machine plane **minus** ``FetchKubeconfig`` (Seam C §5.4 plane matrix: kubeconfig for a
DigitalOcean cluster comes from the ``ssh-k3s`` provider, not this one). Talks to the
real DigitalOcean REST API v2 over an **injected** ``httpx.AsyncClient`` (§5.4's "or a
shared ``httpx.AsyncClient``" transport option) — no ``python-digitalocean`` SDK, no
sync-subprocess-in-``__init__`` (v1's pattern at
``reference-code/seedpod/seedpod/providers/digitalocean.py:45-70``).

Salvaged from ``reference-code/seedpod/seedpod/providers/digitalocean.py``:

- ``translate_node_spec``/``_find_closest_droplet_size`` (lines 124-193) → ``_translate_node_spec``/
  ``_closest_droplet_size`` below, bit-identical region/size mapping + closest-match fallback,
  now provider-internal (contract note: "translate_node_spec runs INSIDE the impl, no longer
  public").
- ``_extract_cluster_identifiers`` (lines 1046-1057) → ``_extract_identifiers`` below, verbatim
  tag-parsing rule.
- ``reconcile``/``_get_managed_droplets`` (lines 1109-1294): Phase A (untracked-droplet →
  ``CreateUnmanagedIntent``, DESTROYED-in-DB-but-present → ``ZombieIntent``) + Phase B
  (active-in-DB-but-absent → ``OrphanIntent``, DESTROYING+missing completion backstop) ported
  with the two deliberate Seam C §5.3 changes: the internal "backend unreachable" catch becomes
  a **raise** (no ``.unreachable()`` result type exists any more — deleted from
  ``core/reconciliation_intents.py`` by design) and the "any other exception ⇒ success([])"
  swallow also becomes a **raise** (logged, retried next reconciliation tick) — never
  fabricate an empty, "everything's fine" result from an unexpected error.
- ``destroy_cluster``/``poll_destruction_status`` (lines 855-1044): the ``api_call_succeeded``
  discipline — idempotent-on-absence is only trustworthy when the API call that reported
  absence actually succeeded; a timeout/garbage body must never be read as "destroyed". Legacy
  ``cluster-{slug}`` tag fallback preserved (v1 lines 909-913) for records whose
  ``resource_ids`` predates the ``droplet_id`` convention.
- ``_create_droplet``'s tag list (lines 317-321): the engine now supplies
  ``cluster-uuid:{uuid}``/``cluster-{slug}``/``ttl-{h}`` directly in ``CreateInstance.tags``
  (§5.3's ``CreateInstance`` docstring) — this adapter no longer constructs them, only merges
  provider-level default tags (``droplet_config.default_tags``, config/providers/digitalocean.yml).
- ``_ensure_vpc_exists`` (lines 537-575) → ``_ensure_vpc_exists`` below: GET-then-POST
  lookup/create of the region's ``{name_prefix}-{region}`` VPC, ``vpc_uuid`` threaded into the
  droplet create payload. Seam C §5.7.1 names ``cleanup_expired_clusters`` as "the only v1
  provider capability with no v2 command" — VPC placement is not on that list, so it is salvaged
  here rather than dropped; warn-and-proceed on ordinary failure preserved (v1's caller-side
  ``except Exception: logger.warning(...); proceed without VPC``), with the same Unreachable-
  propagates/ordinary-error-swallows split documented below for project assignment.
- ``_ensure_firewall_exists``/firewall attachment (lines 589-657, invoked from
  ``_provision_cluster`` lines 471-483) → the ``ApplyFirewalls`` command (``contract.py``) +
  ``_apply_firewalls`` below: management (SSH/K3s-API/internal-cluster-comms/flannel) +
  application (HTTP/HTTPS) firewalls, GET-then-POST ensure-exists by
  ``{name_prefix}-{region}`` name, droplet attached to both. Backed by the workflow's
  ``do.apply_firewalls`` step (``config/workflows/provision-digitalocean.yml``, already declared
  by Seam B §2.2 Proof 2 / ``seam-b-engine.md:331-332`` citing v1's own warn-and-continue at
  line 477) — the step's ``on_failure: continue`` is what carries v1's warn-and-proceed forward,
  not swallowing inside this provider.

Deliberately NOT ported (Seam C §5.7.4 "v1 bugs deliberately not pinned", plus scope decisions
flagged here per CLAUDE.md's citation requirement):

- ``get_cluster_status``'s ``NameError`` (v1 line 841: ``f"unknown-{droplet_id}"`` referencing an
  undefined name) — not pinned; replaced wholesale by the typed, bug-free ``ProbeInstance``.
- Triple project-assignment (v1 fires ``_assign_droplet_to_project`` at lines 353, 451, and 454 —
  three ``asyncio.create_task`` fire-and-forget calls across create+provision, each internally
  ``asyncio.sleep(5)``-ing first) collapses to **two** ``await``ed, best-effort calls: one early
  (inside ``_create_instance``, unchanged from the original port — still closes the C1
  mid-create-death window, conformance C-09) and one late (the ``AssignToProject`` command,
  fired from its own workflow step positioned after K3s install — see the fix note below).
  §5.7.4 authorizes collapsing the triple assignment "to one step"; going from three call sites
  down to two (rather than one) restores v1's actual reliable-timing attempt — the one that "is
  when assignment reliably succeeded" per v1's own placement after ``get_kubeconfig`` — without
  reintroducing v1's redundant third call or any provider-side sleep (H4-H6: one bounded
  attempt, no retry/sleep loops; the physics constant a 5s settle guarded against is subsumed by
  this step firing minutes into provisioning rather than by a delay this provider enacts
  itself).
- ``_get_or_create_ssh_key`` (v1 lines 281-299): the "or_create" in the name lied — it never
  created a key, only looked one up and raised if absent. §5.7.4 lists this specifically as a
  v1 bug (misleading name) not to pin; ``_resolve_ssh_key_id`` below does the same lookup+raise
  under an honest name.
- ``cleanup_expired_clusters``/``_is_droplet_expired`` (v1 lines 964-987, 1059-1082): removed
  from the seam entirely per §5.7.1 — TTL expiry is a Pillar-1 ``ScheduleTimer`` decision now;
  the ``ttl-{h}`` tag is still written (informationally, by the engine) but no provider code
  parses it.

Genuinely NEW (not salvage — §5.7.3, needs the review this docstring gives it): re-invocation
adoption by ``cluster-uuid:{uuid}`` tag in ``CreateInstance`` (conformance C-07). v1 never
retried creates; a second ``CreateInstance`` for the same ``cluster_uuid`` here is answered by
looking the identity tag up first and adopting the existing droplet (``adopted_existing=True``)
rather than risking a duplicate.

Deliberate v2 improvement (documented, not silent): v1's ``_assign_droplet_to_project`` swallows
*any* ``Exception`` (line 532, "Don't fail the entire cluster creation if project assignment
fails"). The EARLY, inline call (``_assign_to_project`` below, used only from
``_create_instance``) keeps that non-fatal behavior for ordinary business errors
(``TransientError``/``PermanentError`` — project not found, bad permissions, rate limited) but
lets a genuine ``InfrastructureUnreachableError`` propagate: v1's blanket swallow could paper
over real DO connectivity loss happening *after* a droplet was already allocated, which is
exactly the C1 half-created-resource window §5.5's ``undo_for`` exists to close. Letting that one
class through means a truncated-mid-create stream (conformance C-09, Fault.DIE_MID_CREATE) still
carries the allocated ``resource_ids`` for compensation instead of silently continuing as if
nothing happened. The LATE call (``AssignToProject``/``_assign_to_project_command`` below, its
own workflow step) does not swallow anything itself — like ``ApplyFirewalls``, it raises
normally and relies on that step's own ``on_failure: continue`` (the v2 home for v1's
warn-and-proceed at this late point in provisioning) rather than duplicating the swallow
in-provider.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import ClassVar

import httpx

from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
    TransientError,
)
from seedpod.core.reconciliation_intents import (
    CreateUnmanagedIntent,
    OrphanIntent,
    ReconciliationIntent,
    ZombieIntent,
)
from seedpod.providers.classify import classify_http
from seedpod.providers.contract import (
    RESOURCE_ALLOCATED,
    ApplyFirewalls,
    AssignToProject,
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
)

__all__ = ["DigitalOceanConfig", "DigitalOceanProvider"]

_HOST = "api.digitalocean.com"

# Cluster states (seedpod/core/records.py ClusterState, hyphenated wire values) that are
# NOT candidates for Phase B orphan detection: terminal-ish/self-explaining drift. Mirrors
# v1's `excluded_states` (reference-code .../digitalocean.py:1181-1186) plus DESTROYING,
# which v1 double-counted via its separate `destroying_clusters` loop (lines 1193-1216) —
# producing two OrphanIntents for the same cluster. That duplication is not pinned (§5.7.4
# reads it as a v1 quirk, not a locked behavior); DESTROYING+missing gets exactly one
# OrphanIntent here, still with its own "completion backstop" reason.
_ORPHAN_EXCLUDED_STATES = frozenset({"destroyed", "zombie", "destroy-scheduled", "failed", "destroying"})


@dataclass(frozen=True)
class DigitalOceanConfig:
    """IO-free construction data (Seam C §5.4's construction contract). Loaded by the
    composition root from ``config/providers/digitalocean.yml`` + the ``DIGITALOCEAN_TOKEN``
    secret; this module never reads a file or an environment variable itself.
    """

    api_token: str
    project_id: str | None = None
    api_base_url: str = f"https://{_HOST}/v2"

    region_mapping: Mapping[str, str] = field(default_factory=dict)
    node_size_mapping: Mapping[str, str] = field(default_factory=dict)
    default_region: str = "ams3"
    default_droplet_size: str = "s-2vcpu-4gb"
    default_image: str = "ubuntu-22-04-x64"

    ssh_key_name: str = "exampleco-testing"
    managed_tag: str = "seedpod-managed"
    default_tags: tuple[str, ...] = ("seedpod-managed", "k3s-cluster")

    enable_ipv6: bool = True
    enable_private_networking: bool = True
    enable_monitoring: bool = True
    enable_backups: bool = False

    request_timeout_s: float = 30.0
    list_timeout_s: float = 45.0
    create_timeout_s: float = 60.0
    delete_timeout_s: float = 45.0

    # v1's post-active warmup (reference-code .../digitalocean.py:671, `asyncio.sleep(30)`
    # inside `_wait_for_droplet_ready`) — a physics constant, preserved as DATA per Seam C
    # §5.4 ("become named interval/settle_seconds parameters ... deleted as sleeps"). This
    # provider never sleeps on it. NOT wired to any gate: v1's comment on the sleep was
    # "wait a bit more for SSH to be ready", and `provision-digitalocean.yml`'s dedicated
    # `k3s.await_ssh` gate (DR-0022) now polls for exactly that condition directly instead
    # of a fixed post-active delay -- a strict improvement, not a v1 regression. Kept here
    # for provenance/parity documentation only; no current verb consumes this field.
    settle_seconds: float = 30.0

    # v1's project-assignment settle (reference-code .../digitalocean.py:527,
    # `await asyncio.sleep(5)` inside `_assign_droplet_to_project`) — another crown-jewel-#17
    # physics constant, preserved as DATA. This provider never sleeps on it either: the early
    # inline assignment in `_create_instance` fires immediately (unchanged, still closes C1 —
    # see module docstring), and the late `AssignToProject` command fires from its own
    # workflow step positioned minutes into provisioning, structurally past this window without
    # needing to consume the field as an actual delay. Exposed for symmetry with
    # `settle_seconds` and for a future engine-owned gate to consume directly if one is added.
    project_assign_settle_seconds: float = 5.0

    # VPC (v1 `vpc_config`, config/providers/digitalocean.yml) — see `_ensure_vpc_exists`.
    vpc_name_prefix: str = "seedpod-vpc"
    vpc_ip_range: str = "10.0.0.0/16"
    vpc_description: str = "Seedpod Infrastructure Manager VPC"
    vpc_create_if_missing: bool = True

    # Firewalls (v1 `firewall_config`/`ssh_access`/`k8s_api_access`,
    # config/providers/digitalocean.yml) — see `_apply_firewalls`.
    management_firewall_name_prefix: str = "seedpod-mgmt"
    application_firewall_name_prefix: str = "seedpod-apps"
    ssh_allowed_sources: tuple[str, ...] = ("0.0.0.0/0",)
    k8s_api_allowed_sources: tuple[str, ...] = ("0.0.0.0/0",)


def _public_ipv4(droplet: Mapping[str, object]) -> str | None:
    networks = droplet.get("networks")
    if not isinstance(networks, Mapping):
        return None
    for entry in networks.get("v4", []) or []:
        if isinstance(entry, Mapping) and entry.get("type") == "public":
            ip = entry.get("ip_address")
            if isinstance(ip, str):
                return ip
    return None


def _extract_identifiers(tags: Sequence[str]) -> tuple[str | None, str | None]:
    """Salvaged verbatim from ``_extract_cluster_identifiers``
    (reference-code/seedpod/seedpod/providers/digitalocean.py:1046-1057)."""
    uuid: str | None = None
    slug: str | None = None
    for tag in tags:
        if tag.startswith("cluster-uuid:"):
            uuid = tag[len("cluster-uuid:") :]
        elif tag.startswith("cluster-") and not tag.startswith("cluster-uuid:"):
            slug = tag[len("cluster-") :]
    return uuid, slug


class DigitalOceanProvider:
    name: ClassVar[str] = "digitalocean"
    supported: ClassVar[frozenset[type]] = frozenset(
        {
            CreateInstance,
            ProbeInstance,
            DestroyInstance,
            ProbeDestruction,
            ListInstances,
            Reconcile,
            ApplyFirewalls,
            AssignToProject,
        }
    )

    def __init__(self, config: DigitalOceanConfig, transport: httpx.AsyncClient) -> None:
        """IO-free (§5.4's construction contract): stores config and the injected transport
        only. ``transport`` is a plain ``httpx.AsyncClient`` — conformance fault injection
        happens at its ``httpx.AsyncBaseTransport`` seam (``tests/conformance/``), never
        ``Mock``/``patch``."""
        self.config = config
        self.transport = transport

    # ------------------------------------------------------------------
    # startup preflight
    # ------------------------------------------------------------------

    async def check_ready(self) -> None:
        """Token present + valid, and the configured SSH key exists — fail at startup, not
        mid-provision (replaces v1's sync-subprocess-in-``__init__`` pattern)."""
        response, _ = await self._request_json("GET", "/account", timeout=self.config.request_timeout_s, command="check_ready")
        if response.status_code != 200:
            self._raise_for_status(response, "check_ready")
        await self._resolve_ssh_key_id(command="check_ready")

    # ------------------------------------------------------------------
    # Provider protocol
    # ------------------------------------------------------------------

    def execute(self, cmd: ProviderCommand) -> AsyncIterator[ProviderEvent]:
        """Unsupported ⇒ ``PermanentError(UNSUPPORTED)`` raised synchronously, before any
        backend traffic (§5.4) — not deferred to the first ``__anext__()``."""
        if type(cmd) not in self.supported:
            raise PermanentError(
                f"digitalocean: unsupported command {type(cmd).__name__}",
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
        if isinstance(cmd, ApplyFirewalls):
            return self._apply_firewalls(cmd)
        if isinstance(cmd, AssignToProject):
            return self._assign_to_project_command(cmd)
        raise AssertionError(f"unreachable: {cmd!r} is in `supported` but has no handler")  # pragma: no cover

    # ------------------------------------------------------------------
    # commands — machine plane
    # ------------------------------------------------------------------

    async def _create_instance(self, cmd: CreateInstance) -> AsyncIterator[ProviderEvent]:
        # NEW (§5.7.3, C-07): re-invocation adoption by cluster-uuid tag, not v1 salvage —
        # v1 never retried creates. Same cluster_uuid a second time adopts the tagged
        # droplet instead of risking a duplicate.
        uuid_tag = f"cluster-uuid:{cmd.cluster_uuid}"
        response, body = await self._request_json(
            "GET",
            f"/droplets?tag_name={uuid_tag}",
            timeout=self.config.list_timeout_s,
            command="create_instance.adopt_check",
        )
        if response.status_code != 200:
            self._raise_for_status(response, "create_instance.adopt_check")
        existing = body.get("droplets") or []
        if existing:
            droplet = existing[0]
            resource_ids = {"droplet_id": str(droplet["id"])}
            yield Progress(phase=RESOURCE_ALLOCATED, data={"resource_ids": resource_ids})
            yield Result(
                InstanceCreated(
                    resource_ids=resource_ids,
                    address=_public_ipv4(droplet),
                    effective_pod_cidr=cmd.pod_cidr,
                    effective_service_cidr=cmd.service_cidr,
                    adopted_existing=True,
                )
            )
            return

        region, size, image = self._translate_node_spec(cmd.spec.node_specification)
        ssh_key_id = await self._resolve_ssh_key_id(command="create_instance")
        vpc_uuid = await self._ensure_vpc_exists(region, command="create_instance.ensure_vpc")

        # Engine supplies cluster-uuid:{uuid}/cluster-{slug}/ttl-{h} directly (§5.3's
        # CreateInstance docstring) — merge with provider-level default tags only.
        tags = list(dict.fromkeys((*self.config.default_tags, *cmd.tags)))
        payload = {
            "name": f"k3s-{cmd.slug}",
            "region": region,
            "size": size,
            "image": image,
            "ssh_keys": [ssh_key_id],
            "backups": self.config.enable_backups,
            "ipv6": self.config.enable_ipv6,
            "private_networking": self.config.enable_private_networking,
            "monitoring": self.config.enable_monitoring,
            "tags": tags,
        }
        if vpc_uuid:
            payload["vpc_uuid"] = vpc_uuid
        response, body = await self._request_json(
            "POST", "/droplets", json_body=payload, timeout=self.config.create_timeout_s, command="create_instance"
        )
        if response.status_code == 422:
            # Row 14: DO create quota exceeded ⇒ Permanent/CAPACITY. DO's droplet-limit error
            # has no distinct HTTP status from other validation failures (both are 422); this
            # is the one place this provider treats a structural DO response shape as
            # capacity rather than routing through classify_http's generic INVALID_INPUT
            # fallback — no free-form message sniffing, keyed on the status DO actually uses
            # for "no more droplets can be created" in practice.
            message = str(body.get("message", "")) if isinstance(body, dict) else ""
            raise PermanentError(
                f"digitalocean.create_instance: {message or 'droplet creation rejected (422)'}",
                code=ErrorCode.CAPACITY,
                provider=self.name,
                command="create_instance",
                detail={"status": "422", "message": message},
            )
        if response.status_code not in (200, 201, 202):
            self._raise_for_status(response, "create_instance")

        droplet = body["droplet"]
        resource_ids = {"droplet_id": str(droplet["id"])}
        # Tag-before-boot: DO assigns `tags` atomically in the create payload above — no
        # separate tagging call, so there is no window where the droplet exists untagged.
        yield Progress(phase=RESOURCE_ALLOCATED, data={"resource_ids": resource_ids})

        # §5.7.4: v1's triple project-assignment (create + twice in _provision_cluster)
        # collapses to this one best-effort, awaited call.
        await self._assign_to_project(droplet["id"], command="create_instance.assign_project")

        yield Result(
            InstanceCreated(
                resource_ids=resource_ids,
                address=_public_ipv4(droplet),
                effective_pod_cidr=cmd.pod_cidr,
                effective_service_cidr=cmd.service_cidr,
                adopted_existing=False,
            )
        )

    async def _probe_instance(self, cmd: ProbeInstance) -> AsyncIterator[ProviderEvent]:
        droplet_id = cmd.resource_ids.get("droplet_id")
        if not droplet_id:
            yield Result(InstanceState(phase="absent", address=None, detail="no droplet_id on record"))
            return
        response, body = await self._request_json(
            "GET", f"/droplets/{droplet_id}", timeout=self.config.request_timeout_s, command="probe_instance"
        )
        if response.status_code == 404:
            # Row 13: absence is AUTHORITATIVE only because the API call itself succeeded.
            yield Result(InstanceState(phase="absent", address=None, detail="droplet not found"))
            return
        if response.status_code != 200:
            self._raise_for_status(response, "probe_instance")
        droplet = body["droplet"]
        status = droplet.get("status")
        if status == "active":
            phase = "running"
        elif status == "new":
            phase = "provisioning"
        else:  # "off" / "archive" / anything else DO returns
            phase = "stopped"
        yield Result(InstanceState(phase=phase, address=_public_ipv4(droplet), detail=str(status or "")))

    async def _destroy_instance(self, cmd: DestroyInstance) -> AsyncIterator[ProviderEvent]:
        droplet_id = cmd.resource_ids.get("droplet_id")
        if droplet_id:
            response, _ = await self._request_json(
                "GET", f"/droplets/{droplet_id}", timeout=self.config.request_timeout_s, command="destroy_instance"
            )
            if response.status_code == 404:
                yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED, note="droplet already absent"))
                return
            if response.status_code != 200:
                # v1 api_call_succeeded discipline (row 9/13): never fabricate DESTROYED when
                # we could not confirm state — raise and let the caller keep the run parked.
                self._raise_for_status(response, "destroy_instance")
            del_response, _ = await self._request_json(
                "DELETE", f"/droplets/{droplet_id}", timeout=self.config.delete_timeout_s, command="destroy_instance"
            )
            if del_response.status_code not in (204, 404):
                self._raise_for_status(del_response, "destroy_instance")
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYING))
            return

        # Legacy cluster-{slug} tag fallback (v1 reference-code .../digitalocean.py:909-913),
        # preserved for records whose resource_ids predate the droplet_id convention.
        tag = f"cluster-{cmd.slug}"
        response, body = await self._request_json(
            "GET", f"/droplets?tag_name={tag}", timeout=self.config.list_timeout_s, command="destroy_instance.legacy_lookup"
        )
        if response.status_code != 200:
            self._raise_for_status(response, "destroy_instance.legacy_lookup")
        droplets = body.get("droplets") or []
        if not droplets:
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED, note="no resources found"))
            return
        for droplet in droplets:
            del_response, _ = await self._request_json(
                "DELETE",
                f"/droplets/{droplet['id']}",
                timeout=self.config.delete_timeout_s,
                command="destroy_instance.legacy_lookup",
            )
            if del_response.status_code not in (204, 404):
                self._raise_for_status(del_response, "destroy_instance.legacy_lookup")
        yield Result(DestroyOutcome(status=DestroyStatus.DESTROYING, note=f"destroying via legacy tag {tag}"))

    async def _probe_destruction(self, cmd: ProbeDestruction) -> AsyncIterator[ProviderEvent]:
        droplet_id = cmd.resource_ids.get("droplet_id")
        if not droplet_id:
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED, note="no resources to probe"))
            return
        response, body = await self._request_json(
            "GET", f"/droplets/{droplet_id}", timeout=self.config.request_timeout_s, command="probe_destruction"
        )
        if response.status_code == 404:
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYED))
            return
        if response.status_code != 200:
            # Transient/garbage-body ⇒ Unreachable; engine keeps polling ("stay destroying").
            self._raise_for_status(response, "probe_destruction")
        droplet = body["droplet"]
        status = droplet.get("status")
        if status == "active":
            yield Result(
                DestroyOutcome(
                    status=DestroyStatus.DESTROY_FAILED,
                    error="droplet still active",
                    stuck_resources=(str(droplet["id"]),),
                )
            )
        else:  # "archive" / "off" (in flight) or any other non-active, non-404 state
            yield Result(DestroyOutcome(status=DestroyStatus.DESTROYING))

    async def _list_instances(self, cmd: ListInstances) -> AsyncIterator[ProviderEvent]:
        response, body = await self._request_json(
            "GET",
            f"/droplets?tag_name={self.config.managed_tag}",
            timeout=self.config.list_timeout_s,
            command="list_instances",
        )
        if response.status_code != 200:
            self._raise_for_status(response, "list_instances")
        yield Result(
            tuple(
                InstanceSummary(name=d["name"], resource_ids={"droplet_id": str(d["id"])})
                for d in body.get("droplets") or []
            )
        )

    async def _reconcile(self, cmd: Reconcile) -> AsyncIterator[ProviderEvent]:
        response, body = await self._request_json(
            "GET",
            f"/droplets?tag_name={self.config.managed_tag}",
            timeout=self.config.list_timeout_s,
            command="reconcile",
        )
        if response.status_code != 200:
            # Deliberate change #1 from v1 (§5.3 Reconcile docstring): the internal
            # catch-to-`.unreachable()` becomes a raise — the engine skips every cluster in
            # `cmd.clusters` and touches nothing (crown jewel #1).
            self._raise_for_status(response, "reconcile")
        droplets = body.get("droplets") or []

        by_uuid: dict[str, dict] = {}
        intents: list[ReconciliationIntent] = []
        db_by_uuid = {snapshot.cluster_uuid: snapshot for snapshot in cmd.clusters}

        # Phase A: DigitalOcean -> Seedpod.
        for droplet in droplets:
            uuid, slug = _extract_identifiers(droplet.get("tags") or ())
            if not uuid:
                continue  # unmanaged-droplet skip: no cluster-uuid tag, not ours to reconcile
            by_uuid[uuid] = droplet
            snapshot = db_by_uuid.get(uuid)
            if snapshot is None:
                intents.append(CreateUnmanagedIntent(cluster_id=uuid, droplet=droplet, slug=slug))
            elif snapshot.status == "destroyed":
                intents.append(
                    ZombieIntent(cluster_id=uuid, droplet_id=str(droplet["id"]), droplet_ip=_public_ipv4(droplet))
                )

        # Phase B: Seedpod -> DigitalOcean.
        for snapshot in cmd.clusters:
            if snapshot.status in _ORPHAN_EXCLUDED_STATES:
                continue
            if snapshot.cluster_uuid not in by_uuid:
                intents.append(OrphanIntent(cluster_id=snapshot.cluster_uuid))

        # DESTROYING+missing completion backstop (single intent — see _ORPHAN_EXCLUDED_STATES
        # comment on the v1 duplicate-intent quirk this dedupes).
        for snapshot in cmd.clusters:
            if snapshot.status == "destroying" and snapshot.cluster_uuid not in by_uuid:
                intents.append(
                    OrphanIntent(
                        cluster_id=snapshot.cluster_uuid,
                        reason="Cluster is DESTROYING but droplet is gone - marking DESTROYED",
                    )
                )

        yield Result(tuple(intents))

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _translate_node_spec(self, spec) -> tuple[str, str, str]:
        """Salvaged from ``translate_node_spec``
        (reference-code/seedpod/seedpod/providers/digitalocean.py:124-166)."""
        region = self.config.region_mapping.get(spec.region_hint, self.config.default_region)
        size_key = f"{spec.cpu_cores},{spec.memory_gb}"
        size = self.config.node_size_mapping.get(size_key) or self._closest_droplet_size(spec.cpu_cores, spec.memory_gb)
        return region, size, self.config.default_image

    def _closest_droplet_size(self, cpu_cores: int, memory_gb: int) -> str:
        """Salvaged verbatim from ``_find_closest_droplet_size``
        (reference-code/seedpod/seedpod/providers/digitalocean.py:168-193)."""
        best_match: str | None = None
        best_score = float("inf")
        for size_key, droplet_size in self.config.node_size_mapping.items():
            try:
                available_cpu, available_mem = (int(part) for part in size_key.split(","))
            except ValueError:
                continue
            if available_cpu >= cpu_cores and available_mem >= memory_gb:
                score = (available_cpu - cpu_cores) + (available_mem - memory_gb)
                if score < best_score:
                    best_score, best_match = score, droplet_size
        return best_match or self.config.default_droplet_size

    async def _resolve_ssh_key_id(self, *, command: str) -> int:
        """Honestly named replacement for v1's misleadingly-named ``_get_or_create_ssh_key``
        (reference-code/seedpod/seedpod/providers/digitalocean.py:281-299), which never
        created a key either — §5.7.4 flags the name as the bug, not the lookup+raise
        behavior, which is preserved."""
        response, body = await self._request_json(
            "GET", "/account/keys?per_page=200", timeout=self.config.list_timeout_s, command=command
        )
        if response.status_code != 200:
            self._raise_for_status(response, command)
        for key in body.get("ssh_keys") or []:
            if key.get("name") == self.config.ssh_key_name:
                return key["id"]
        raise PermanentError(
            f"digitalocean.{command}: SSH key '{self.config.ssh_key_name}' not found in DigitalOcean account",
            code=ErrorCode.NOT_FOUND,
            provider=self.name,
            command=command,
            detail={"ssh_key_name": self.config.ssh_key_name},
        )

    async def _assign_to_project(self, droplet_id: int, *, command: str) -> None:
        """Best-effort, single attempt (§5.7.4 — collapses v1's triple fire-and-forget
        assignment to one awaited call). Ordinary business failures (project not found, bad
        permissions, rate limited) are swallowed exactly as v1 did (reference-code
        .../digitalocean.py:532-535, "Don't fail the entire cluster creation if project
        assignment fails") — but a genuine ``InfrastructureUnreachableError`` propagates
        (module docstring's "deliberate v2 improvement" note): v1's blanket
        ``except Exception`` could paper over real DO connectivity loss happening after a
        droplet was already allocated, silently reopening the C1 window this call now guards.
        """
        if not self.config.project_id:
            return
        try:
            await self._request_json(
                "POST",
                f"/projects/{self.config.project_id}/resources",
                json_body={"resources": [f"do:droplet:{droplet_id}"]},
                timeout=self.config.request_timeout_s,
                command=command,
            )
        except InfrastructureUnreachableError:
            raise
        except (TransientError, PermanentError):
            return

    async def _assign_to_project_command(self, cmd: AssignToProject) -> AsyncIterator[ProviderEvent]:
        """The LATE ``AssignToProject`` command (module docstring): fired from its own
        workflow step positioned after K3s install, structurally past the "not yet fully
        created" window without this provider sleeping on ``project_assign_settle_seconds``.
        Does NOT swallow — raises normally like any other command; the workflow step's own
        ``on_failure: continue`` is the v2 home for v1's warn-and-proceed here (unlike the
        early inline ``_assign_to_project`` above, which has no workflow step of its own to
        carry that policy and so keeps the swallow in-provider)."""
        droplet_id = cmd.resource_ids.get("droplet_id")
        if droplet_id and self.config.project_id:
            response, _ = await self._request_json(
                "POST",
                f"/projects/{self.config.project_id}/resources",
                json_body={"resources": [f"do:droplet:{droplet_id}"]},
                timeout=self.config.request_timeout_s,
                command="assign_project",
            )
            if response.status_code != 200:
                self._raise_for_status(response, "assign_project")
        yield Result(None)

    async def _ensure_vpc_exists(self, region: str, *, command: str) -> str | None:
        """Salvaged from ``_ensure_vpc_exists`` (reference-code .../digitalocean.py:537-575):
        GET-then-POST lookup/create of the region's ``{vpc_name_prefix}-{region}`` VPC.
        Warn-and-proceed on ordinary failure (v1's caller-side ``except Exception:
        logger.warning(...); proceed without VPC`` at line 314) — but a genuine
        ``InfrastructureUnreachableError`` propagates, same "deliberate v2 improvement" split
        documented for ``_assign_to_project`` above: real DO connectivity loss must not be
        silently swallowed into "proceed without VPC", only ordinary business failures
        (VPC name collision, quota, bad permissions) should be."""
        name = f"{self.config.vpc_name_prefix}-{region}"
        try:
            response, body = await self._request_json(
                "GET", "/vpcs", timeout=self.config.list_timeout_s, command=command
            )
            if response.status_code != 200:
                self._raise_for_status(response, command)
            for vpc in body.get("vpcs") or []:
                if vpc.get("name") == name and vpc.get("region") == region:
                    return str(vpc["id"])
            if not self.config.vpc_create_if_missing:
                return None
            payload = {
                "name": name,
                "region": region,
                "ip_range": self.config.vpc_ip_range,
                "description": self.config.vpc_description,
            }
            response, body = await self._request_json(
                "POST", "/vpcs", json_body=payload, timeout=self.config.create_timeout_s, command=command
            )
            if response.status_code not in (200, 201, 202):
                self._raise_for_status(response, command)
            return str(body["vpc"]["id"])
        except InfrastructureUnreachableError:
            raise
        except (TransientError, PermanentError):
            return None

    async def _apply_firewalls(self, cmd: ApplyFirewalls) -> AsyncIterator[ProviderEvent]:
        """Salvaged from ``_ensure_firewall_exists`` + the firewall-attach block (reference-
        code .../digitalocean.py:589-657, 471-483): management (SSH/K3s-API/internal-cluster-
        comms/flannel) and application (HTTP/HTTPS) firewalls, GET-then-POST ensure-exists by
        ``{name_prefix}-{region}`` name, droplet attached to both. Raises normally on failure
        (no in-provider swallow) — the workflow's ``do.apply_firewalls`` step declares
        ``on_failure: continue``, which is the v2 home for v1's warn-and-proceed here."""
        droplet_id = cmd.resource_ids.get("droplet_id")
        if not droplet_id:
            yield Result(None)
            return
        region, _, _ = self._translate_node_spec(cmd.spec.node_specification)
        for name_prefix, rules in (
            (self.config.management_firewall_name_prefix, self._management_firewall_rules()),
            (self.config.application_firewall_name_prefix, self._application_firewall_rules()),
        ):
            firewall_id = await self._ensure_firewall_exists(name_prefix, region, rules, command="apply_firewalls")
            await self._attach_droplet_to_firewall(firewall_id, droplet_id, command="apply_firewalls")
        yield Result(None)

    def _management_firewall_rules(self) -> list[dict]:
        """v1 ``firewall_config.management.rules`` (config/providers/digitalocean.yml):
        SSH (22) and K3s API (6443) from the configured CIDR sources AND from droplets
        carrying ``managed_tag`` (droplet-to-droplet), plus internal cluster comms (10250/tcp)
        and flannel VXLAN (8472/udp) from tagged droplets."""
        return [
            {"protocol": "tcp", "ports": "22", "sources": {"addresses": list(self.config.ssh_allowed_sources)}},
            {"protocol": "tcp", "ports": "22", "sources": {"tags": [self.config.managed_tag]}},
            {"protocol": "tcp", "ports": "6443", "sources": {"addresses": list(self.config.k8s_api_allowed_sources)}},
            {"protocol": "tcp", "ports": "6443", "sources": {"tags": [self.config.managed_tag]}},
            {"protocol": "tcp", "ports": "10250", "sources": {"tags": [self.config.managed_tag]}},
            {"protocol": "udp", "ports": "8472", "sources": {"tags": [self.config.managed_tag]}},
        ]

    def _application_firewall_rules(self) -> list[dict]:
        """v1 ``firewall_config.application.rules``: HTTP/HTTPS open to the internet."""
        return [
            {"protocol": "tcp", "ports": "80", "sources": {"addresses": ["0.0.0.0/0"]}},
            {"protocol": "tcp", "ports": "443", "sources": {"addresses": ["0.0.0.0/0"]}},
        ]

    async def _ensure_firewall_exists(
        self, name_prefix: str, region: str, inbound_rules: list[dict], *, command: str
    ) -> str:
        name = f"{name_prefix}-{region}"
        response, body = await self._request_json("GET", "/firewalls", timeout=self.config.list_timeout_s, command=command)
        if response.status_code != 200:
            self._raise_for_status(response, command)
        for fw in body.get("firewalls") or []:
            if fw.get("name") == name:
                return str(fw["id"])
        payload = {
            "name": name,
            "inbound_rules": inbound_rules,
            # v1's outbound_rules (reference-code .../digitalocean.py:632-639): allow all
            # outbound tcp/udp.
            "outbound_rules": [
                {"protocol": "tcp", "ports": "all", "destinations": {"addresses": ["0.0.0.0/0"]}},
                {"protocol": "udp", "ports": "all", "destinations": {"addresses": ["0.0.0.0/0"]}},
            ],
        }
        response, body = await self._request_json(
            "POST", "/firewalls", json_body=payload, timeout=self.config.create_timeout_s, command=command
        )
        if response.status_code not in (200, 201, 202):
            self._raise_for_status(response, command)
        return str(body["firewall"]["id"])

    async def _attach_droplet_to_firewall(self, firewall_id: str, droplet_id: str, *, command: str) -> None:
        response, _ = await self._request_json(
            "POST",
            f"/firewalls/{firewall_id}/droplets",
            json_body={"droplet_ids": [int(droplet_id)]},
            timeout=self.config.request_timeout_s,
            command=command,
        )
        if response.status_code != 204:
            self._raise_for_status(response, command)

    async def _raw_request(
        self, method: str, path: str, *, json_body: object = None, timeout: float, command: str
    ) -> httpx.Response:
        url = f"{self.config.api_base_url}{path}"
        headers = {"Authorization": f"Bearer {self.config.api_token}"}
        try:
            return await self.transport.request(method, url, json=json_body, headers=headers, timeout=timeout)
        except httpx.TimeoutException as e:
            # Row 9: timeout from the API call ⇒ Unreachable/API_TIMEOUT (v1 _run_sync's
            # asyncio.wait_for TimeoutError, reference-code .../digitalocean.py:93-101).
            raise InfrastructureUnreachableError(
                f"digitalocean.{command}: timed out calling {path}",
                code=ErrorCode.API_TIMEOUT,
                provider=self.name,
                command=command,
                detail={"path": path},
                host=_HOST,
            ) from e
        except httpx.TransportError as e:
            raise InfrastructureUnreachableError(
                f"digitalocean.{command}: could not reach {_HOST}: {e}",
                code=ErrorCode.ENDPOINT_UNREACHABLE,
                provider=self.name,
                command=command,
                detail={"path": path},
                host=_HOST,
            ) from e

    async def _request_json(
        self, method: str, path: str, *, json_body: object = None, timeout: float, command: str
    ) -> tuple[httpx.Response, dict]:
        """One bounded attempt (no internal retry — H4-H6, the engine's Schedule owns retry).
        Row 10: garbage/empty JSON body ⇒ Unreachable/MALFORMED_RESPONSE regardless of HTTP
        status ("v1: treat like timeout")."""
        response = await self._raw_request(method, path, json_body=json_body, timeout=timeout, command=command)
        if response.status_code == 204:
            return response, {}
        if not response.content:
            raise classify_http(
                provider=self.name, command=command, host=_HOST, status=response.status_code,
                malformed_body=True, observing_infra=True,
            )
        try:
            body = response.json()
        except ValueError as e:
            raise classify_http(
                provider=self.name, command=command, host=_HOST, status=response.status_code,
                malformed_body=True, observing_infra=True,
            ) from e
        if not isinstance(body, dict):
            raise classify_http(
                provider=self.name, command=command, host=_HOST, status=response.status_code,
                malformed_body=True, observing_infra=True,
            )
        return response, body

    def _raise_for_status(self, response: httpx.Response, command: str) -> ProviderError:
        """Always raises. Return type is ``ProviderError`` only so callers can (optionally)
        write ``raise self._raise_for_status(...)`` for the type checker; every call site
        instead relies on this never returning."""
        retry_after: float | None = None
        if "retry-after" in response.headers:
            try:
                retry_after = float(response.headers["retry-after"])
            except ValueError:
                retry_after = None
        raise classify_http(
            provider=self.name,
            command=command,
            host=_HOST,
            status=response.status_code,
            rate_limited=response.status_code == 429,
            retry_after=retry_after,
            observing_infra=True,
        )
