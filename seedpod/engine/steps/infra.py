"""engine/steps/infra.py — the late-bound ``infra.*`` verb family (DR-0022 ruling
1) plus the two DO-only ``do.*`` verbs (P7): ``infra.create_instance``,
``infra.await_instance``, ``infra.fetch_kubeconfig``, ``do.apply_firewalls``,
``do.assign_project`` (Round 8a, "infra-and-do" component — ``infra.destroy_instance``
and ``cluster.load_infra`` are a LATER component of this same round, per
DR-0022's own table; this module never registers or imports them).

All five are THIN bindings over already-built, already-conformance-tested
providers (``seedpod/providers/{digitalocean,kind,tart,orbstack}.py``) — a
Params model, a pure ``command()`` mapping, and an ``output_from()``. No
provider logic is reimplemented here; no retry/sleep/poll loop is added (Seam C
taste call 2: "no command waits, all waiting is an engine gate" — the gate loop
for ``infra.await_instance`` lives entirely in ``engine/engine.py``, never
here).

``infra.create_instance``/``infra.await_instance``/``infra.fetch_kubeconfig``
subclass ``LateBoundProviderStep`` (``engine/steps/late_bound.py``): the
adapter is resolved purely by ``params.provider`` (a dict lookup into
``ctx.services.providers``), never a branch (DR-0004's stated fear, answered
structurally). ``do.apply_firewalls``/``do.assign_project`` are DO-only (P7: "a
vendor prefix is permitted ONLY for a capability no other provider has") and
are plain ``ProviderStep``s with a fixed ``provider_name = "digitalocean"`` —
never late-bound, since no other provider implements ``ApplyFirewalls``/
``AssignToProject`` at all.

**The ``cluster_id``/``slug`` Params fields (Round-8a review finding).** Seam
C's ``CreateInstance`` requires ``cluster_uuid``/``slug`` (conformance C-07's
idempotency key and the DO/kind/tart naming convention — DO's `k3s-{slug}`
droplet name, `cluster-{slug}` legacy tag fallback; kind's cluster name is
`_cluster_name(cmd.slug)` verbatim), but neither is derivable from `spec:
ClusterSpecification` alone, and ``command(self, params)`` is pure — no ``ctx``
(``engine/provider_step.py``/``engine/steps/late_bound.py``: "MUST stay pure...
no ctx on every concrete subclass"). DR-0022 P8 is exactly the mechanism for
this: "every fact a provider step needs is produced by a `cluster.load_*` head
and bound in YAML, so V4 type-checks it and `command(params)` stays pure" — so
``cluster.load_spec``'s Output gained a `slug: str` field (mirroring
`cluster.load_infra`'s own `LoadInfraOutput.slug` on the destroy side,
``engine/steps/cluster.py``), and every `provision-*.yml`'s `create` step now
binds `cluster_id: {from: run.cluster_id}, slug: {from: spec.slug}` alongside
`provider`/`spec`. See ``engine/steps/cluster.py``'s ``LoadSpecOutput``
docstring for the full rationale; this module just consumes the two fields.

**Tags (Round-8a review finding — blocker).** Built here, once, for every
provider, in v1's own order (``reference-code/seedpod/seedpod/providers/
digitalocean.py:317-321``, ``_create_droplet``): the user-declared
`spec.cluster_config.tags` list FIRST (dropped entirely by an earlier revision
of this function — the exact "silently regressing edge behavior v1 already got
right" CLAUDE.md warns against; all five shipped `config/deployment-
profiles/*.yml` populate it), then `f"cluster-{slug}"`, then
`f"cluster-uuid:{cluster_id}"`, and finally (only if the spec declares a TTL)
`f"ttl-{h}"` (v1's exact `int(tag[4:])` parse target,
``reference-code/seedpod/seedpod/providers/digitalocean.py:1068`` --
v2-invented, informational only, no provider code parses it back). Each
adapter (``providers/digitalocean.py``'s own comment: "this adapter no longer
constructs them, only merges provider-level default tags") prepends its own
`default_tags` and dedupes (`dict.fromkeys`), so the provider-level default
tags always land first in the final list regardless of the order built here.

**``infra.await_instance`` is the P3 exemplar.** ``execute()`` is overridden to
a true no-op (DR-0022 Erratum E4b: "returns a provisional Output and invokes no
provider") — it never touches ``ctx.services.providers``, unlike the inherited
``LateBoundProviderStep.execute()`` this class deliberately does NOT use. Its
``poll_ready`` is NOT overridden (Erratum E4a: the dict-lookup site for gate
polls lives once, in ``LateBoundProviderStep.poll_ready``'s template method) —
this class implements ``probe()`` instead, which the template calls with the
adapter already resolved.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import ClassVar, TypeVar

from pydantic import BaseModel, SecretStr

from seedpod.core.cluster_spec import ClusterSpecification
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.engine.provider_step import Provider, ProviderStep
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepContext
from seedpod.engine.steps.late_bound import LateBoundProviderStep
from seedpod.providers.contract import (
    ApplyFirewalls,
    AssignToProject,
    CreateInstance,
    DestroyInstance,
    DestroyOutcome,
    DestroyStatus,
    FetchKubeconfig,
    InstanceCreated,
    InstanceState,
    Kubeconfig,
    ProbeDestruction,
    ProbeInstance,
    Result,
)

__all__ = [
    "CreateInstanceParams",
    "InstanceCreatedOutput",
    "InfraCreateInstance",
    "AwaitInstanceParams",
    "AddressOutput",
    "InfraAwaitInstance",
    "FetchKubeconfigByResourceIdsParams",
    "InfraFetchKubeconfigOutput",
    "InfraFetchKubeconfig",
    "ApplyFirewallsParams",
    "DoApplyFirewalls",
    "AssignToProjectParams",
    "DoAssignToProject",
    "DestroyInstanceParams",
    "InfraDestroyInstance",
]


def _tags_for(*, cluster_id: str, slug: str, spec: ClusterSpecification) -> tuple[str, ...]:
    """v1's own tag order (`reference-code/seedpod/seedpod/providers/
    digitalocean.py:317-321`, `_create_droplet`): `spec.cluster_config.tags`
    (the user-declared list every shipped deployment profile populates) FIRST,
    then `cluster-{slug}`, then `cluster-uuid:{cluster_id}`, then (only if the
    spec declares a TTL) `ttl-{h}`. Each provider adapter merges its own
    provider-level default tags with this tuple and dedupes
    (`dict.fromkeys`/equivalent), so order here only matters for byte-parity
    with v1's own list, not for correctness. `ttl-{h}` matches v1's exact
    parse target (`int(tag[4:])`, reference-code .../digitalocean.py:1068) --
    informational only (seam-c-provider.md §5.7.1: TTL expiry is a Pillar-1
    ScheduleTimer decision, no provider code parses this tag). Rounded UP
    (`math.ceil`), not truncated, so a sub-hour TTL (e.g. 0.5) tags `ttl-1`
    rather than the actively-misleading `ttl-0`."""
    tags = (*spec.cluster_config.tags, f"cluster-{slug}", f"cluster-uuid:{cluster_id}")
    ttl_hours = spec.cluster_config.ttl_hours
    if ttl_hours is not None:
        tags = (*tags, f"ttl-{math.ceil(ttl_hours)}")
    return tags


_ResultT = TypeVar("_ResultT")


def _expect(value: object, expected: type[_ResultT], *, verb: str) -> _ResultT:
    """Loud Result-shape guard (this module's own stated rule, `_cidrs_for`'s
    docstring): "raised loudly (not asserted, which `-O` strips) rather than
    silently propagating `None`/the wrong shape into a Seam C command" -- here,
    the mirror case, a *Provider's* Result value failing to match the one
    dataclass shape each command's own contract promises. A bare `assert`
    vanishes under `python -O`, degrading a contract violation into a
    downstream `pydantic.ValidationError` or `AttributeError` instead of the
    intended loud, attributable failure (Round-8a review finding)."""
    if not isinstance(value, expected):
        raise PermanentError(
            f"{verb}: expected Result value of type {expected.__name__}, got {type(value).__name__}",
            code=ErrorCode.INVALID_INPUT,
            provider="engine",
            command=verb,
        )
    return value


def _cidrs_for(spec: ClusterSpecification, *, verb: str) -> tuple[str, str]:
    """`cluster.load_spec` (`engine/steps/cluster.py`) always overlays
    `allocate_cluster_cidrs()`'s real pod/service CIDRs onto `cluster_config`
    before this verb ever sees `spec` — never `None` in practice. Raised
    loudly (not asserted, which `-O` strips) rather than silently propagating
    `None` into a Seam C command whose `pod_cidr`/`service_cidr` fields are
    typed `str`, not `str | None`."""
    pod_cidr = spec.cluster_config.pod_cidr
    service_cidr = spec.cluster_config.service_cidr
    if pod_cidr is None or service_cidr is None:
        raise PermanentError(
            f"{verb}: spec.cluster_config.pod_cidr/service_cidr must be set by cluster.load_spec",
            code=ErrorCode.INVALID_INPUT,
            provider="engine",
            command=verb,
        )
    return pod_cidr, service_cidr


# ---------------------------------------------------------------------------
# infra.create_instance
# ---------------------------------------------------------------------------


class CreateInstanceParams(BaseModel):
    """See this module's own docstring for why `cluster_id`/`slug` ride
    alongside `provider`/`spec` (P8; mirrors `cluster.load_infra`'s
    `LoadInfraOutput.slug` on the destroy side)."""

    provider: str
    spec: ClusterSpecification
    cluster_id: str
    slug: str


class InstanceCreatedOutput(BaseModel):
    """Seam C `InstanceCreated`, narrowed to what workflow YAML binds from.
    `address` is `None` until a separate `infra.await_instance` gate enriches
    it (DO/kind/tart); already set at create time for orbstack (Erratum E5).

    **`effective_pod_cidr`/`effective_service_cidr` are deliberately NOT
    surfaced here (Round-8a review finding, record-and-decide).** Seam C's
    `InstanceCreated` carries both so kind can echo back the real kindnet `/16`
    it actually installed (`providers/kind.py`: "so the engine sees what was
    *actually* installed rather than what it asked for") in place of the
    engine-supplied Tailscale `/24`. No workflow YAML binds either field today,
    no emit payload carries them, and `clusters` has no CIDR column to persist
    them into (`cluster.load_spec` re-derives the requested `/24`s
    deterministically from `cluster_id` on every load rather than reading any
    stored "effective" value back) -- so the echo's audience is conformance/
    debug only (kind's own C-07/C-08 coverage observes it directly off the
    Seam C `Result`), not this step's YAML-facing Output. A later component
    that needs to persist or branch on the actually-installed CIDR should add
    both fields here then, rather than carrying them unread from day one."""

    resource_ids: Mapping[str, str]
    address: str | None = None
    adopted_existing: bool = False


class InfraCreateInstance(LateBoundProviderStep[CreateInstanceParams, InstanceCreatedOutput]):
    verb = "infra.create_instance"
    Params = CreateInstanceParams
    Output = InstanceCreatedOutput
    thin = True
    idempotent = False  # Step's own default is True; DECLARED_VERBS says False.
    # seam-b-engine.md:493/524: "exactly one verb is non-idempotent
    # (do.create_droplet)" -- DR-0022 renamed it infra.create_instance without
    # ratifying a flip. A crash mid-execute (a step row left `status='running'`)
    # must mark_failed("interrupted; non-idempotent") -> compensate rather than
    # blindly re-entering execute() up to resume_replay_limit (engine.py:519).
    # undoable=True (ProviderStep's own default) matches DR-0022's fixture: the
    # C1 fix -- undo_for(CreateInstance) -> DestroyInstance, closing v1's
    # CRITICAL C1 "no droplet cleanup on failure" (compensation.py, verbatim).

    def command(self, params: CreateInstanceParams) -> CreateInstance:
        pod_cidr, service_cidr = _cidrs_for(params.spec, verb=self.verb)
        return CreateInstance(
            cluster_uuid=params.cluster_id,
            slug=params.slug,
            spec=params.spec,
            pod_cidr=pod_cidr,
            service_cidr=service_cidr,
            tags=_tags_for(cluster_id=params.cluster_id, slug=params.slug, spec=params.spec),
        )

    def output_from(self, value: object) -> InstanceCreatedOutput:
        created = _expect(value, InstanceCreated, verb=self.verb)
        return InstanceCreatedOutput(
            resource_ids=created.resource_ids, address=created.address, adopted_existing=created.adopted_existing
        )
    # result_value_from: the base identity default suffices -- InstanceCreatedOutput
    # already exposes `.resource_ids`, exactly what compensation.py's
    # `undo_for(CreateInstance(...), observed)` reads off `observed.value`.


# ---------------------------------------------------------------------------
# infra.await_instance
# ---------------------------------------------------------------------------


class AwaitInstanceParams(BaseModel):
    provider: str
    resource_ids: Mapping[str, str]


class AddressOutput(BaseModel):
    """DR-0022 P6 (glossary nouns): one field name, `address`, across every
    provider (replaces the pre-DR-0022 `ip`/`address` split)."""

    address: str


class InfraAwaitInstance(LateBoundProviderStep[AwaitInstanceParams, AddressOutput]):
    verb = "infra.await_instance"
    Params = AwaitInstanceParams
    Output = AddressOutput
    gateable = True
    undoable = False  # ProviderStep hard-defaults True; DECLARED_VERBS says False -- a
    # pure gate has nothing to compensate (subsumed by infra.create_instance's undo).
    thin = True

    def command(self, params: AwaitInstanceParams) -> ProbeInstance:
        return ProbeInstance(resource_ids=params.resource_ids)

    async def execute(self, params: AwaitInstanceParams, ctx: StepContext) -> AddressOutput:
        """DR-0022 P3 / Erratum E4b: "execute emits no Seam C command" -- a
        true no-op, never ``ctx.services.providers[...]``. Deliberately does
        NOT call ``super().execute()`` (``LateBoundProviderStep``'s inherited
        template, which WOULD emit ``self.command(params)`` immediately --
        exactly what an ``await_``-named verb must not do). The placeholder
        `address` is provisional only -- the gate's first `Ready` REPLACES it
        (``engine/step.py``'s ``Ready.outputs``), never read before then."""
        return AddressOutput(address="")

    async def probe(
        self, params: AwaitInstanceParams, provisional: AddressOutput, provider: Provider, ctx: StepContext
    ) -> Ready[AddressOutput] | NotReady:
        value: object | None = None
        async for ev in provider.execute(self.command(params)):
            if isinstance(ev, Result):
                value = ev.value
        state = _expect(value, InstanceState, verb=self.verb)
        if state.phase == "running" and state.address is not None:
            return Ready(outputs=AddressOutput(address=state.address))
        if state.phase == "absent":
            # contract.py/seam-c-provider.md: "'absent' is AUTHORITATIVE (API
            # said so). Cannot-answer => raise Unreachable. Never conflate."
            # (Round-8a review finding.) A destroyed-out-of-band resource (or a
            # `kind create` that produced no container) is a TERMINAL answer,
            # not "not yet" -- raising here lets the gate's own
            # `except PermanentError` arm (engine.py) fail immediately rather
            # than polling a known-gone resource for the full gate timeout.
            raise PermanentError(
                f"{self.verb}: instance is absent (resource_ids={dict(params.resource_ids)})",
                code=ErrorCode.NOT_FOUND,
                provider="engine",
                command=self.verb,
            )
        return NotReady(detail=f"phase={state.phase}")


# ---------------------------------------------------------------------------
# infra.fetch_kubeconfig (the resource_ids variant -- kind/orbstack)
# ---------------------------------------------------------------------------


class FetchKubeconfigByResourceIdsParams(BaseModel):
    """The kind/orbstack variant of Seam C `FetchKubeconfig` -- identifies the
    local cluster via `resource_ids`, not an ssh `SSHTarget`/`known_hosts` pair
    (that ssh-k3s variant is the distinct, fixed-provider `k3s.fetch_kubeconfig`,
    a later Round-8a component; this module never binds it)."""

    provider: str
    resource_ids: Mapping[str, str]
    rewrite_server_to: str


class InfraFetchKubeconfigOutput(BaseModel):
    kubeconfig: SecretStr


class InfraFetchKubeconfig(LateBoundProviderStep[FetchKubeconfigByResourceIdsParams, InfraFetchKubeconfigOutput]):
    verb = "infra.fetch_kubeconfig"
    Params = FetchKubeconfigByResourceIdsParams
    Output = InfraFetchKubeconfigOutput
    undoable = False  # ProviderStep hard-defaults True; DECLARED_VERBS says False --
    # seam-c-provider.md §5.5: "CaptureHostKeys/InstallK3s/FetchKubeconfig -> none".
    thin = True

    def command(self, params: FetchKubeconfigByResourceIdsParams) -> FetchKubeconfig:
        return FetchKubeconfig(rewrite_server_to=params.rewrite_server_to, resource_ids=params.resource_ids)

    def output_from(self, value: object) -> InfraFetchKubeconfigOutput:
        kubeconfig = _expect(value, Kubeconfig, verb=self.verb)
        return InfraFetchKubeconfigOutput(kubeconfig=SecretStr(kubeconfig.yaml_text))


# ---------------------------------------------------------------------------
# do.apply_firewalls / do.assign_project -- DO-only (P7), fixed provider_name,
# never late-bound (no other provider implements these commands at all).
# ---------------------------------------------------------------------------


class ApplyFirewallsParams(BaseModel):
    resource_ids: Mapping[str, str]
    spec: ClusterSpecification


class DoApplyFirewalls(ProviderStep[ApplyFirewallsParams, EmptyOutput]):
    verb = "do.apply_firewalls"
    provider_name: ClassVar[str] = "digitalocean"
    Params = ApplyFirewallsParams
    Output = EmptyOutput
    undoable = False  # ProviderStep hard-defaults True; seam-c-provider.md §5.3's own
    # ApplyFirewalls docstring: "NOT undoable: ensure-exists is itself idempotent".
    thin = True

    def command(self, params: ApplyFirewallsParams) -> ApplyFirewalls:
        return ApplyFirewalls(resource_ids=params.resource_ids, spec=params.spec)

    def output_from(self, value: object) -> EmptyOutput:
        return EmptyOutput()


class AssignToProjectParams(BaseModel):
    resource_ids: Mapping[str, str]


class DoAssignToProject(ProviderStep[AssignToProjectParams, EmptyOutput]):
    verb = "do.assign_project"
    provider_name: ClassVar[str] = "digitalocean"
    Params = AssignToProjectParams
    Output = EmptyOutput
    undoable = False  # ProviderStep hard-defaults True; seam-c-provider.md §5.3's own
    # AssignToProject docstring: "NOT undoable (no side effect to reverse on failure)".
    thin = True

    def command(self, params: AssignToProjectParams) -> AssignToProject:
        return AssignToProject(resource_ids=params.resource_ids)

    def output_from(self, value: object) -> EmptyOutput:
        return EmptyOutput()


# ---------------------------------------------------------------------------
# infra.destroy_instance -- the destroy path's terminal verb (both destroy files).
# ---------------------------------------------------------------------------


class DestroyInstanceParams(BaseModel):
    """Typed Params (DR-0022 P8: no ``EmptyParams`` provider verb), fed entirely by
    ``cluster.load_infra``'s fresh read. ``slug`` is carried because Seam C's
    ``DestroyInstance`` needs it for DigitalOcean's legacy ``cluster-{slug}`` tag
    fallback."""

    provider: str
    slug: str
    resource_ids: Mapping[str, str]


class InfraDestroyInstance(LateBoundProviderStep[DestroyInstanceParams, EmptyOutput]):
    """The last step of both ``destroy-cloud.yml`` and ``destroy-shared.yml``.

    **The second of DR-0022 P3's two named actuate-and-gate verbs** (with
    ``kube.delete_daemonset``): ``execute()`` really actuates -- it INITIATES the
    destroy -- and the gate then polls for the resource actually being gone. Hence
    ``gateable=True`` with an actuator name, and ``thin=False`` (two Seam C commands:
    ``DestroyInstance`` then ``ProbeDestruction``).

    **Conflict 5's park law, and the three ways to get it wrong.** An
    ``InfrastructureUnreachableError`` from the probe is never caught here, so it
    propagates and the engine PARKS the run. That is deliberate and load-bearing:

    1. It must not become ``NotReady``. ``NotReady`` means "not yet, keep polling on
       the gate's clock"; unreachable means "cannot determine state", and the gate's
       overall timeout is suspended while parked rather than burning down.
    2. It must not feed ``max_consecutive_poll_failures``. That counter is
       ``TransientError``-only (the workflow sets it to 5); an unreachable API would
       otherwise exhaust it and fail a destroy that was merely unobservable.
    3. It must NEVER flip to "gone". Reporting a cluster destroyed because its API
       could not be reached is the worst available outcome -- the row goes terminal
       while the droplet keeps billing, and nothing points at it any more. Absence is
       only ever concluded from a ``DestroyOutcome`` the provider actually returned.

    ``DestroyOutcome`` is data, not exceptions (Seam C's absence-is-data discipline),
    so interpreting its three statuses is THIS step's job -- and the SAME status means
    different things on the two paths: ``DESTROY_FAILED`` from the initiate call is
    terminal, from a probe it is "still in flight, keep polling" (see ``probe``) -- unlike
    ``kube.rollout_undo``, where the provider owns the verdict.

    **Gotcha 1 (``check_enabled=False`` ALWAYS) is NOT fully preserved -- see
    ``_resolve_provider``.**"""

    verb = "infra.destroy_instance"
    Params = DestroyInstanceParams
    Output = EmptyOutput
    gateable = True
    undoable = False  # destroying a destroy is not a thing; teardown is terminal
    thin = False

    def command(self, params: DestroyInstanceParams) -> DestroyInstance:
        return DestroyInstance(slug=params.slug, resource_ids=params.resource_ids)

    def output_from(self, value: object) -> EmptyOutput:
        """``execute()``'s outcome -- the INITIATE verdict, mirroring v1's own
        three-way branch on ``destruction_result["status"]``: "destroyed" and
        "destroying" both continue to the gate, anything else is a failure.

        ``DESTROY_FAILED`` is terminal HERE and deliberately not on the probe path:
        the provider rejecting the delete request itself is a real answer, whereas a
        probe still seeing the resource moments later is just an asynchronous
        teardown in flight (see ``probe``)."""
        outcome = _expect(value, DestroyOutcome, verb=self.verb)
        assert isinstance(outcome, DestroyOutcome)  # narrowed by _expect
        _raise_if_destroy_failed(outcome, verb=self.verb, phase="initiate")
        return EmptyOutput()

    async def execute(self, params: DestroyInstanceParams, ctx: StepContext) -> EmptyOutput:
        _resolve_provider(params.provider, ctx, verb=self.verb)
        return await super().execute(params, ctx)

    async def probe(
        self, params: DestroyInstanceParams, provisional: EmptyOutput, provider: Provider, ctx: StepContext
    ) -> Ready[EmptyOutput] | NotReady:
        value: object | None = None
        async for ev in provider.execute(ProbeDestruction(resource_ids=params.resource_ids)):
            if isinstance(ev, Result):
                value = ev.value
        outcome = _expect(value, DestroyOutcome, verb=self.verb)
        assert isinstance(outcome, DestroyOutcome)  # narrowed by _expect
        if outcome.status is DestroyStatus.DESTROYED:
            return Ready(outputs=EmptyOutput())
        if outcome.status is DestroyStatus.DESTROY_FAILED:
            # NOT a raise. DigitalOcean's delete is ASYNCHRONOUS: the droplet keeps
            # reporting `status: active` for several seconds after a successful
            # delete call, and DO's probe maps active -> DESTROY_FAILED. Treating the
            # first such probe as terminal fails a destroy that is merely still in
            # flight -- observed on real infrastructure 2026-08-03, where this step
            # raised 2.2s after initiating a destroy that then completed fine.
            #
            # "Still fully present" only MEANS stuck once enough time has passed, and
            # deciding how much is precisely what the gate's own `timeout_seconds:
            # 900` is for. So this stays NotReady and lets the gate adjudicate; the
            # stuck resources are surfaced as progress each poll so a genuinely stuck
            # teardown is visible long before the timeout.
            await ctx.progress(
                "destroy still reports the resource present",
                stuck_resources=list(outcome.stuck_resources),
                detail=outcome.error or "",
            )
        return NotReady()


def _raise_if_destroy_failed(outcome: DestroyOutcome, *, verb: str, phase: str) -> None:
    """``DESTROY_FAILED`` from the INITIATE call (``DestroyInstance``): the provider
    rejected the request, which is a real, terminal answer. ``stuck_resources`` rides
    into the error detail so the failure names what is left behind.

    Deliberately NOT used on the probe path -- see ``InfraDestroyInstance.probe``."""
    if outcome.status is not DestroyStatus.DESTROY_FAILED:
        return
    detail = {"phase": phase}
    if outcome.stuck_resources:
        detail["stuck_resources"] = ",".join(outcome.stuck_resources)
    raise PermanentError(
        f"{verb}: provider reports destroy_failed ({outcome.error or 'no error detail'})",
        code=ErrorCode.SCRIPT_FAILED,
        provider="engine",
        command=verb,
        detail=detail,
    )


def _resolve_provider(name: str, ctx: StepContext, *, verb: str) -> Provider:
    """**Gotcha 1 gap, surfaced loudly rather than as a bare ``KeyError``.**

    v1 resolved the destroy provider with ``check_enabled=False`` precisely "because
    we need to destroy clusters even if provider is now disabled"
    (``reference-code/.../jobs/state/destruction_job.py``), and both destroy workflows
    still carry that comment on this step. v2 cannot honour it as things stand:
    ``app/factory.py``'s ``load_enabled_providers`` makes a disabled provider ABSENT
    from the mapping ("disabled = absent ... no ``ProviderDisabledError`` type exists
    in v2", Decision 8 step 5), and ``ctx.services.providers`` is the only way a
    late-bound verb can reach one.

    So a cluster whose provider was disabled after it was provisioned CANNOT be
    destroyed today. Closing that needs a design change (destroy resolving against an
    all-providers mapping, or providers carrying an enabled flag instead of being
    omitted) -- i.e. a DR, not a decision this step may take on its own. Until then it
    fails with a message that names the actual cause, so the operator can re-enable
    the provider and retry rather than debugging a ``KeyError`` from a dict lookup."""
    try:
        return ctx.services.providers[name]
    except KeyError:
        raise PermanentError(
            f"{verb}: provider {name!r} is not enabled, so this cluster cannot be destroyed. "
            "v1 destroyed with check_enabled=False (gotcha 1); v2 omits disabled providers entirely. "
            "Re-enable the provider and retry the destroy.",
            code=ErrorCode.INVALID_INPUT,
            provider="engine",
            command=verb,
            detail={"provider": name},
        ) from None
