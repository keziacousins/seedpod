"""engine/steps/cluster.py — the two ``cluster.*`` DOMAIN steps every provision
workflow's head/tail bind from (DR-0022's verb table; coherence-review.md
Conflicts 9 and 10).

Both steps here are ``plane="domain"``/``thin=False`` (DR-0022 P2): they use
``seedpod/data/repositories.py``'s ``ClusterRepository`` + (for the second)
``seedpod/services/crypto.py``'s ``CryptoService`` directly, never a Seam C
``Provider`` — no ``ctx.services.providers`` lookup, no ``ProviderCommand``.
Per ``engine/registry.py``'s own docstring ("Construction ... happens once, at
composition-root build time"), dependencies are constructor-injected by
``app/factory.py::_build_step_registry()`` — NOT read off ``ctx.services`` at
call time, which is why both classes take ``uow``/``clusters``/(``crypto``/
``clock``) in ``__init__`` rather than reaching into ``StepContext``. DR-0023
adds a third dependency to ``LoadSpec`` alone: ``ssh_identities``, a
``Mapping[str, SshIdentity]`` the composition root builds from each
provider's own ``config/providers/<provider>.yml`` (``app/factory.py``'s
``_ssh_identities()``) — see that function's and ``LoadSpecOutput``'s own
docstrings for why.

``cluster.load_spec`` — Conflict 10 (the provision head): the pure machine
emitting ``RunWorkflow(provision)`` cannot build a ``ClusterSpecification``
(Seam A: "args are refs only"), so this step does it, reading
``clusters.provider_config`` (Conflict 10's own comment: "cluster row +
provider_config -> ClusterSpecification, incl. salvaged
allocate_cluster_cidrs()") and calling the COMMITTED
``seedpod.core.cluster_spec.allocate_cluster_cidrs`` — never reimplemented —
so the pod/service CIDRs stay bit-identical to v1's Tailscale-critical hash
allocation for the same cluster uuid. ``provider_config`` is birthed from a
deployment profile's own ``cluster_spec`` block by
``seedpod/app/services/deployment_service.py``'s ``_birth_cluster_row``
(Round-8a review finding: row synthesis is the API-layer service's job per
coherence-review.md, not this step's) — an empty/incomplete
``provider_config`` (e.g. a birthed-but-never-profile-backed row) still
surfaces as a plain pydantic ``ValidationError``, which Seam B's own
``Step.execute`` contract classifies "any other exception ≡ Permanent" — not
something this step swallows or papers over. ``_cluster_specification_from``
also mirrors a v1 parity trap verbatim: ``ingress_strategy`` is a SIBLING of
``cluster_config`` in 3 of the 5 shipped ``config/deployment-profiles/*.yml``
(the other 2 nest it directly inside ``cluster_config``) — both shapes are
overlaid/preserved here, never dropped (``reference-code/seedpod/seedpod/core/
cluster_spec.py:396-399``'s own inline warning comment names this exact trap).

``cluster.store_kubeconfig`` (replaces DR-0004's ``kubeconfig.store``) —
Conflict 9 (the kubeconfig tail): the kubeconfig plaintext NEVER rides an
event and never touches disk here (it lives only in the calling workflow's
persisted-but-Fernet-encrypted step output and this step's own locals) — this
step Fernet-encrypts it via the committed ``CryptoService`` and writes
``clusters.encrypted_kubeconfig``/``kubeconfig_key_class`` (the exact two
columns ``provision-*.yml``'s ``store`` step comment names), then mints and
returns the opaque ``kubeconfig_ref`` (``"cluster-kubeconfig:{cluster_id}"``,
per those same YAML comments) that ``ProvisionSucceeded`` carries onward —
``kubeconfig_ref`` itself is machine-owned (``ClusterRecord.kubeconfig_ref``)
and is set later by the Dispatcher when the engine posts that event, NOT by
this step (CLAUDE.md: "state changes go through Dispatcher.apply() only").

**Repository addition (idiom-matched, not invented).** No write path for
``encrypted_kubeconfig``/``kubeconfig_key_class`` existed anywhere in
committed code — only the read side (``cluster_service.py``'s
``_kubeconfig_for``). ``ClusterRepository.set_kubeconfig`` was added in the
same shape as its siblings already in that class (``set_health_failures``/
``update_cost``/``set_expires_at``: a plain UPDATE, no CAS, no ``version``
bump, for a row-only bookkeeping column the pure machine never reads) — the
only mechanical way to satisfy this step's own YAML-documented contract
without reaching around the repository layer with raw SQL from here, which
``seedpod/data/repositories.py``'s own docstring calls out as the exact
bypass Decision 6 already closed. Unlike those fire-and-forget siblings,
though, a lost write here is not harmless (this step mints ``kubeconfig_ref``
right after it), so ``set_kubeconfig`` reports its rowcount and this step
raises rather than minting a ref for a write that silently affected zero rows
(Round-8a review finding).

DR-0008 discipline: both steps hoist non-DB work (CIDR allocation, Fernet
encryption) OUTSIDE any open ``uow()`` block; ``store``'s encrypt happens
between two short, separate transactions (read row -> encrypt -> write row)
rather than under one held open across the crypto call.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, SecretStr

from seedpod.core.acme import AcmeConfig
from seedpod.core.clock import Clock
from seedpod.core.cluster_spec import (
    ClusterConfiguration,
    ClusterSpecification,
    NodeSpecification,
    allocate_cluster_cidrs,
)
from seedpod.core.dns_record import DnsIntent, DnsRecordRef
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.data.repositories import ClusterRepository
from seedpod.data.uow import UnitOfWork
from seedpod.engine.step import EmptyOutput, EmptyParams, Step, StepContext
from seedpod.services.crypto import CryptoService

__all__ = [
    "SshIdentity",
    "LoadSpecParams",
    "LoadSpecOutput",
    "LoadSpec",
    "StoreKubeconfigParams",
    "StoreKubeconfigOutput",
    "StoreKubeconfig",
    "LoadInfraParams",
    "LoadInfraOutput",
    "LoadInfra",
    "KubeconfigOutput",
    "OptionalKubeconfigOutput",
    "LoadKubeconfig",
    "LoadKubeconfigOptional",
]


@dataclass(frozen=True)
class SshIdentity:
    """DR-0023: v1 used a genuinely DIFFERENT SSH identity per provider
    (``digitalocean`` = ``root``/``~/.ssh/id_exampleco_testing``; ``tart`` =
    ``admin``/``~/.ssh/id_ed25519``), constructed from that provider's OWN
    ``config/providers/<provider>.yml`` section -- the same file v1 read, no
    second source of truth. Both fields are ``None`` for providers with no SSH
    plane (``kind``, ``orbstack``); ``LoadSpec`` never invents a fallback
    identity (DR-0023 point 5: "no fallback identity may survive"). ``~`` is
    ALREADY expanded by the composition root before this reaches here (DR-0023
    point 4, ``SSHTarget.private_key_path``'s own documented contract) --
    this type just carries the two strings, it does no path IO itself."""

    user: str | None = None
    private_key_path: str | None = None


def _not_found(verb: str, cluster_id: str) -> PermanentError:
    return PermanentError(
        f"{verb}: cluster {cluster_id!r} not found",
        code=ErrorCode.NOT_FOUND,
        provider="engine",
        command=verb,
        detail={"cluster_id": cluster_id},
    )


def _cluster_specification_from(provider_config: Mapping[str, Any], *, pod_cidr: str, service_cidr: str) -> ClusterSpecification:
    """``provider_config`` is shaped exactly like a deployment profile's own
    ``cluster_spec`` block (``config/deployment-profiles/*.yml``:
    ``{node_specification: {...}, cluster_config: {...}}``) -- see this
    module's docstring for where that dict is meant to come from.
    ``pod_cidr``/``service_cidr`` are ALWAYS overlaid from the caller's
    ``allocate_cluster_cidrs()`` call, never read back from ``provider_config``
    (deterministic on ``cluster_id`` alone, so recomputing on every load is
    always bit-identical to whatever was there before -- the one thing this
    function must never do is trust or reimplement the allocation itself).

    v1 parity trap (``reference-code/seedpod/seedpod/core/cluster_spec.py``
    :396-399, ``create_cluster_spec_from_template``): ``ingress_strategy`` is
    a SIBLING of ``cluster_config`` inside the ``cluster_spec`` block for 3 of
    the 5 shipped ``config/deployment-profiles/*.yml`` (the other 2 nest it
    inside ``cluster_config`` directly) -- v1 explicitly overlays the sibling
    into ``cluster_config`` before validating, UNCONDITIONALLY (v1's own code:
    ``if ingress_strategy: cluster_config["ingress_strategy"] = ingress_strategy``
    -- the sibling always wins when present, never guarded on what
    ``cluster_config`` already carries). Mirrored verbatim here (Round-8a
    review finding: an earlier revision of this function inverted v1's
    precedence by guarding the overlay on ``"ingress_strategy" not in
    cluster_config``, which no shipped profile happens to exercise since none
    carries both shapes at once, but which silently diverged from v1 all the
    same); either shape still reaches ``ClusterConfiguration.ingress_strategy``
    so neither drops the traefik/hostport config on the floor (which would
    otherwise fall through to ``ssh_k3s.py``'s ``--disable=traefik``
    default)."""
    node_specification = dict(provider_config.get("node_specification") or {})
    cluster_config = dict(provider_config.get("cluster_config") or {})
    ingress_strategy = provider_config.get("ingress_strategy")
    if ingress_strategy:
        cluster_config["ingress_strategy"] = ingress_strategy
    cluster_config["pod_cidr"] = pod_cidr
    cluster_config["service_cidr"] = service_cidr
    return ClusterSpecification(
        node_specification=NodeSpecification(**node_specification),
        cluster_config=ClusterConfiguration(**cluster_config),
    )


class LoadSpecParams(BaseModel):
    cluster_id: str


class LoadSpecOutput(BaseModel):
    """DR-0022's verb table: "output gains ``provider: str``" (reused by
    ``infra.create_instance``'s late-binding ``provider`` param, ruling 1).

    ``slug`` (Round-8a "infra-and-do" component finding): Seam C's
    ``CreateInstance`` requires ``cluster_uuid``/``slug`` (conformance C-07's
    idempotency key and the DO/kind/tart droplet/VM/cluster naming convention
    respectively), but ``ProviderStep``/``LateBoundProviderStep``'s
    ``command(self, params)`` is pure -- no ``ctx`` (``engine/provider_step.py``,
    ``engine/steps/late_bound.py``: "MUST stay pure (no ctx) on every concrete
    subclass"). P8 ("every fact a provider step needs is produced by a
    ``cluster.load_*`` head and bound in YAML, so V4 type-checks it and
    ``command(params)`` stays pure") is exactly the mechanism for exactly this
    fact -- ``cluster_id`` is already a workflow input (bindable straight from
    ``run.cluster_id``), but ``slug`` has no other source anywhere in the
    grammar/Params/StepContext, so it is threaded through here rather than
    invented ad hoc inside ``infra.create_instance``. Mirrors the destroy
    path's own precedent exactly: ``cluster.load_infra``'s ``LoadInfraOutput``
    already carries ``slug`` for ``infra.destroy_instance``'s typed Params.

    ``ssh_user``/``ssh_private_key_path`` (DR-0023): every Seam C command in
    the k3s plane except ``ProbeSshPort`` needs an ``SSHTarget``, whose
    ``user``/``private_key_path`` are required with no defaults, and nothing
    else in the grammar/Params/StepContext carries them. Both are ``None``
    for providers with no SSH plane (``kind``, ``orbstack``).

    Their optionality is NOT what enforces the plane matrix. DR-0023
    decision 1 originally claimed it was ("a type error to bind them into a
    ``k3s.*`` verb ... enforced by types, not convention"); that DR's
    **Erratum E1** retracts the claim on both halves: this Output is a single
    global ``str | None``, never narrowed per provider, so requiring ``str``
    on the k3s Params would reject DigitalOcean's and tart's legitimate
    bindings too -- and the protection was unreachable anyway, because
    ``provision-{kind,orbstack}.yml`` contain no ``k3s.*`` step at all. The
    plane matrix is enforced by workflow COMPOSITION (which verbs a
    provider's file contains). Identity presence is enforced instead by
    ``k3s.py``'s ``_target()``, which raises a loud ``PermanentError`` rather
    than building an ``SSHTarget`` from a ``None`` (DR-0023 point 5).

    ``dns_intent`` (**DR-0034 decision 3**): what this cluster's profile asked
    for in DNS terms, read from ``provider_config["dns_config"]`` -- the block
    ``_provider_config_from`` copies at birth when, and only when, the profile
    enabled DNS (v1's own rule, ``cluster_manager.py``:318-321). ``None`` for
    every profile that did not, which is most of them.

    This is an Output extension to an already-ratified verb, of the same kind
    Erratum E13 recorded for ``slug``, and DR-0034 authorizes it explicitly.
    ``cluster.load_spec`` is already the head of all four provision workflows
    and already exists to answer "what does this cluster's provisioning need?",
    so the alternative -- a second domain verb whose whole job is one more read
    of the same row -- would have bought a catalog entry and nothing else. It
    also mirrors the destroy side exactly: ``cluster.load_infra`` yields
    ``dns_record``, this yields ``dns_intent``."""

    spec: ClusterSpecification
    provider: str
    slug: str
    ssh_user: str | None
    ssh_private_key_path: str | None
    dns_intent: DnsIntent | None = None
    # DR-0036: the Let's Encrypt certresolver, read from `provider_config["ssl_config"]`
    # and gated on ssl.enabled AND dns.enabled (v1's `use_acme_certs`). Bound into
    # `k3s.install`, which writes it into the SAME HelmChartConfig the hostport strategy
    # already lays down before k3s starts. None for every profile that did not enable both.
    acme: AcmeConfig | None = None


class LoadSpec(Step[LoadSpecParams, LoadSpecOutput]):
    verb = "cluster.load_spec"
    Params = LoadSpecParams
    Output = LoadSpecOutput
    plane = "domain"
    thin = False
    # gateable=False, undoable=False, idempotent=True: Step's own defaults are
    # already truthful here -- a read-only load has nothing to gate or undo.

    def __init__(
        self,
        *,
        uow: UnitOfWork,
        clusters: ClusterRepository,
        ssh_identities: Mapping[str, SshIdentity],
    ) -> None:
        """``ssh_identities`` (DR-0023): keyed by provider name, sourced by the
        composition root from each provider's OWN ``config/providers/*.yml``
        (``app/factory.py``'s ``_ssh_identities()``) -- REQUIRED, like this
        step's other dependencies (Round-8a review finding: a ``None``-
        defaulted signature would let a wiring omission ship a silently-wrong
        SSH identity, which DR-0023 point 5 explicitly forbids; a missing
        keyword is now a boot-time ``TypeError`` instead). A provider absent
        from the mapping (``kind``/``orbstack``, which have no SSH plane)
        resolves to ``SshIdentity()`` -- ``(None, None)`` -- never a fallback
        identity."""
        self._uow = uow
        self._clusters = clusters
        self._ssh_identities = ssh_identities

    async def execute(self, params: LoadSpecParams, ctx: StepContext) -> LoadSpecOutput:
        async with self._uow() as tx:
            row = self._clusters.get(tx, params.cluster_id)
        if row is None:
            raise _not_found(self.verb, params.cluster_id)
        # Tailscale-critical: call the committed allocator, never reimplement it.
        pod_cidr, service_cidr = allocate_cluster_cidrs(row.id)
        spec = _cluster_specification_from(row.provider_config, pod_cidr=pod_cidr, service_cidr=service_cidr)
        identity = self._ssh_identities.get(row.provider, SshIdentity())
        return LoadSpecOutput(
            spec=spec,
            provider=row.provider,
            slug=row.slug,
            ssh_user=identity.user,
            ssh_private_key_path=identity.private_key_path,
            dns_intent=DnsIntent.from_provider_config(row.provider_config),
            acme=AcmeConfig.from_provider_config(row.provider_config),
        )


class StoreKubeconfigParams(BaseModel):
    cluster_id: str
    kubeconfig: SecretStr


class StoreKubeconfigOutput(BaseModel):
    kubeconfig_ref: str


class StoreKubeconfig(Step[StoreKubeconfigParams, StoreKubeconfigOutput]):
    verb = "cluster.store_kubeconfig"
    Params = StoreKubeconfigParams
    Output = StoreKubeconfigOutput
    plane = "domain"
    thin = False

    def __init__(self, *, uow: UnitOfWork, clusters: ClusterRepository, crypto: CryptoService, clock: Clock) -> None:
        self._uow = uow
        self._clusters = clusters
        self._crypto = crypto
        self._clock = clock

    async def execute(self, params: StoreKubeconfigParams, ctx: StepContext) -> StoreKubeconfigOutput:
        async with self._uow() as tx:
            row = self._clusters.get(tx, params.cluster_id)
        if row is None:
            raise _not_found(self.verb, params.cluster_id)
        # Hoisted out of any open transaction (DR-0008): key-class lookup + Fernet
        # encrypt are pure/CPU-bound, never DB statements.
        key_class = self._crypto.key_class_for_environment(row.environment)
        ciphertext = self._crypto.encrypt(params.kubeconfig.get_secret_value(), key_class)
        async with self._uow() as tx:
            wrote = self._clusters.set_kubeconfig(
                tx, params.cluster_id, encrypted_kubeconfig=ciphertext, key_class=key_class, clock=self._clock
            )
        if not wrote:
            # The row vanished between the read above and this write (e.g. a
            # concurrent destroy) -- never mint a kubeconfig_ref for a write
            # that silently affected zero rows (see repositories.py's
            # set_kubeconfig docstring).
            raise _not_found(self.verb, params.cluster_id)
        # Opaque handle (Conflict 9) -- kubeconfig_ref itself is machine-owned and
        # set later by the Dispatcher when the engine posts ProvisionSucceeded;
        # this step never writes ClusterRecord.kubeconfig_ref directly.
        return StoreKubeconfigOutput(kubeconfig_ref=f"cluster-kubeconfig:{params.cluster_id}")


# ---------------------------------------------------------------------------
# cluster.store_dns_record -- the persistence half of DR-0034.
# ---------------------------------------------------------------------------


class StoreDnsRecordParams(BaseModel):
    cluster_id: str
    record: DnsRecordRef | None = None


class StoreDnsRecord(Step[StoreDnsRecordParams, EmptyOutput]):
    """DR-0034 decisions 1 and 4: write what ``dns.create_record`` made onto the
    cluster row -- ``dns_hostname``, ``dns_zone``, ``dns_record_id``.

    **Why this is a second verb rather than part of the create.** ``plane="service"``
    steps do not touch repositories and ``plane="domain"`` steps do; splitting the
    Cloudflare call from the row write is the same shape the kubeconfig already
    ships (``k3s.fetch_kubeconfig`` produces the fact, ``cluster.store_kubeconfig``
    persists it), and it puts the engine's step-output persistence between them, so
    the record id is durable in the step row before anything tries to write the
    cluster row.

    **What it closes.** ``clusters.dns_hostname`` had no writer at all -- birthed
    ``None`` and never set -- while ``api/routers/clusters.py`` has always derived
    ``cluster_url`` from it. The column, the API and the SPA were all ready for a
    value nobody wrote (backlog #22). It also feeds ``cluster.load_infra``, which is
    what makes the already-shipped ``dns.delete_record`` delete a real record
    (backlog #6)."""

    verb = "cluster.store_dns_record"
    Params = StoreDnsRecordParams
    Output = EmptyOutput
    plane = "domain"
    thin = False
    # undoable=False: the inverse of this write is deleting the record itself, which
    # is `dns.create_record`'s undo (it owns the `created` flag that makes deleting
    # safe). idempotent=True: writing the same three values twice is the same row.

    def __init__(self, *, uow: UnitOfWork, clusters: ClusterRepository, clock: Clock) -> None:
        self._uow = uow
        self._clusters = clusters
        self._clock = clock

    async def execute(self, params: StoreDnsRecordParams, ctx: StepContext) -> EmptyOutput:
        record = params.record
        if record is None:
            # No intent, so no record -- the majority of clusters. Bound straight
            # through from `dns.create_record`'s own Optional Output.
            return EmptyOutput()
        async with self._uow() as tx:
            wrote = self._clusters.set_dns_record(
                tx,
                params.cluster_id,
                hostname=record.hostname,
                zone=record.zone,
                record_id=record.record_id,
                clock=self._clock,
            )
        if not wrote:
            # The row vanished between create and store (e.g. a concurrent destroy).
            # Raising is what gets the record deleted: this failure compensates the
            # run, and `dns.create_record`'s undo removes what it created. Reporting
            # success here would leak the record forever -- nothing would ever name
            # it again (see repositories.py's set_dns_record docstring).
            raise _not_found(self.verb, params.cluster_id)
        await ctx.progress(
            "dns record persisted", hostname=record.hostname, zone=record.zone, record_id=record.record_id
        )
        return EmptyOutput()


# ---------------------------------------------------------------------------
# cluster.load_infra -- the DESTROY head (DR-0022 ruling 2).
# ---------------------------------------------------------------------------


class LoadInfraParams(BaseModel):
    cluster_id: str


class LoadInfraOutput(BaseModel):
    """DR-0022 ruling 2's destroy head, replacing the dispatch table's
    ``dns_record_ref(cluster)`` snapshot-at-dispatch-time hook: read FRESH at run
    time (stale-on-retry-proof), feeding ``infra.destroy_instance``'s typed Params
    and ``dns.delete_record``'s ``record`` binding from ONE load.

    ``slug`` is carried for the same reason ``LoadSpecOutput`` carries it: Seam C's
    ``DestroyInstance`` takes it for DigitalOcean's legacy ``cluster-{slug}`` tag
    fallback, and nothing else in the grammar/Params/StepContext supplies it."""

    provider: str
    slug: str
    resource_ids: Mapping[str, str]
    dns_record: DnsRecordRef | None = None


class LoadInfra(Step[LoadInfraParams, LoadInfraOutput]):
    verb = "cluster.load_infra"
    Params = LoadInfraParams
    Output = LoadInfraOutput
    plane = "domain"
    thin = False
    # gateable/undoable False, idempotent True (Step's defaults): a read-only load.

    def __init__(self, *, uow: UnitOfWork, clusters: ClusterRepository) -> None:
        self._uow = uow
        self._clusters = clusters

    async def execute(self, params: LoadInfraParams, ctx: StepContext) -> LoadInfraOutput:
        async with self._uow() as tx:
            row = self._clusters.get(tx, params.cluster_id)
        if row is None:
            raise _not_found(self.verb, params.cluster_id)
        # `provider` comes from the COLUMN, matching v1's own destroy path: v1
        # resolved the provider to destroy with via
        # `get_provider_for_cluster(db_record, check_enabled=False)`, which reads
        # `cluster_record.provider`. v1's separate
        # `(provider_config or {}).get("provider") or db_record.provider` read
        # (destruction_job.py:61) is NOT ported and is NOT a regression: its only
        # use was choosing whether to wipe deployed resources first (the
        # `provider_name in ("orbstack","kind")` branch), and v2 makes that choice
        # by WORKFLOW COMPOSITION instead -- `_DESTROY_BY_PROVIDER` dispatches
        # kind/orbstack to `destroy-shared.yml` (which has the `kube.wipe_namespace`
        # step) and digitalocean/tart to `destroy-cloud.yml` (which does not).
        return LoadInfraOutput(
            provider=row.provider,
            slug=row.slug,
            # provisioning OUTPUTS (Seam C `InstanceCreated.resource_ids`, persisted
            # by the InfraAllocated event) -- NOT `provider_config`, which holds the
            # provisioning INPUTS. v1 conflated the two in one blob; v2's schema
            # splits them, and the destroy path wants the outputs.
            resource_ids=dict(row.provider_resources or {}),
            # COLUMNS, not `provider_config` (DR-0034 decision 4). This used to read
            # v1's `provider_config` blob, which v2 never wrote -- so it always
            # yielded None and `dns.delete_record` always no-opped while reporting
            # success (backlog #6/#22). `cluster.store_dns_record` writes the three
            # columns; this reads them back.
            dns_record=DnsRecordRef.from_columns(
                record_id=row.dns_record_id, zone=row.dns_zone, hostname=row.dns_hostname
            ),
        )


# ---------------------------------------------------------------------------
# cluster.load_kubeconfig{,_optional} -- the decrypt inverse of store_kubeconfig.
# ---------------------------------------------------------------------------


class KubeconfigOutput(BaseModel):
    kubeconfig: SecretStr


class OptionalKubeconfigOutput(BaseModel):
    kubeconfig: SecretStr | None = None


class _LoadKubeconfigBase(Step[BaseModel, BaseModel]):
    """Shared read+decrypt for both kubeconfig loaders. Both take ``EmptyParams``
    (no ``with:`` block in any shipped workflow) and read ``ctx.cluster_id``
    implicitly -- the canonical shape ``declared_verbs.py``'s ``EmptyParams``
    docstring describes.

    The exact inverse of ``StoreKubeconfig``: read the row, then Fernet-decrypt
    OUTSIDE the transaction (DR-0008 -- a transaction encloses only DB statements),
    using the row's OWN recorded ``kubeconfig_key_class`` rather than re-deriving it
    from the environment, so a cluster whose environment changed after provisioning
    still decrypts with the key its ciphertext was written under."""

    plane = "domain"
    thin = False

    def __init__(self, *, uow: UnitOfWork, clusters: ClusterRepository, crypto: CryptoService) -> None:
        self._uow = uow
        self._clusters = clusters
        self._crypto = crypto

    async def _load(self, ctx: StepContext) -> SecretStr | None:
        async with self._uow() as tx:
            row = self._clusters.get(tx, ctx.cluster_id)
        if row is None:
            raise _not_found(self.verb, ctx.cluster_id)
        if not row.encrypted_kubeconfig or not row.kubeconfig_key_class:
            return None
        plaintext = self._crypto.decrypt(row.encrypted_kubeconfig, row.kubeconfig_key_class)
        return SecretStr(plaintext)


class LoadKubeconfig(_LoadKubeconfigBase):
    """``deploy-waves.yml``/``deploy-rollback.yml``'s head. A cluster with no stored
    kubeconfig is a genuine workflow-configuration error here -- both workflows exist
    to act ON a provisioned cluster -- so absence raises rather than yielding None."""

    verb = "cluster.load_kubeconfig"
    Params = EmptyParams
    Output = KubeconfigOutput

    async def execute(self, params: EmptyParams, ctx: StepContext) -> KubeconfigOutput:
        kubeconfig = await self._load(ctx)
        if kubeconfig is None:
            raise PermanentError(
                f"{self.verb}: cluster {ctx.cluster_id!r} has no stored kubeconfig",
                code=ErrorCode.NOT_FOUND,
                provider="engine",
                command=self.verb,
                detail={"cluster_id": ctx.cluster_id},
            )
        return KubeconfigOutput(kubeconfig=kubeconfig)


class LoadKubeconfigOptional(_LoadKubeconfigBase):
    """Both destroy workflows' head. Absence is NORMAL and must not fail the
    teardown: a cluster whose provisioning died before `cluster.store_kubeconfig`
    still has real infrastructure to destroy. Downstream steps bind this as
    ``Optional`` and no-op on None (`kube.delete_daemonset`'s
    ``on_failure: continue`` comment says exactly that), so returning None here is
    what lets a half-provisioned cluster still be torn down."""

    verb = "cluster.load_kubeconfig_optional"
    Params = EmptyParams
    Output = OptionalKubeconfigOutput

    async def execute(self, params: EmptyParams, ctx: StepContext) -> OptionalKubeconfigOutput:
        return OptionalKubeconfigOutput(kubeconfig=await self._load(ctx))


# ---------------------------------------------------------------------------
# DR-0040: auto_snapshot on an unattended destroy
# ---------------------------------------------------------------------------


class AutoSnapshotParams(BaseModel):
    cluster_id: str
    trigger: str = "operator"
    snapshot: bool = False  # DR-0043: the operator asked, explicitly and unconditionally


class AutoSnapshotOutput(BaseModel):
    snapshot_id: str | None = None
    skipped_reason: str | None = None


class AutoSnapshot(Step[AutoSnapshotParams, AutoSnapshotOutput]):
    """DR-0040: honour the deployment profile's ``auto_snapshot`` block when a cluster
    is deleted with nobody watching.

    **What was wrong.** Three shipped profiles declare ``auto_snapshot: {enabled: true}``
    and nothing read it. A TTL expiry destroys through the pure machine
    (``ACTIVE x TtlExpired -> DESTROY_SCHEDULED -> DestroyDue -> RunWorkflow "destroy"``),
    which touches no service, so DR-0020's pre-destroy snapshot -- a call inside
    ``ClusterService.destroy`` -- never ran for it. Verified on 2026-08-13: a real
    8-hour TTL expired, the destroy was flawless (55ms after ``expires_at``), and the
    ``snapshots`` table was empty.

    **It now serves BOTH destroy routes (DR-0043).** It used to fire only when
    unattended, because the operator path snapshotted inline in ``ClusterService.
    destroy`` before dispatching and firing here too would have written two snapshots
    for one destroy. That inline call was also the reason ``DELETE /api/clusters/{id}
    ?snapshot_before_destroy=true`` blocked for up to 300s per service on a response
    body the SPA never reads, so DR-0043 moved it here -- to the step that already sat
    in the right place in the workflow, while the cluster is still whole.

    Two independent inputs now ride in from the machine on ``DestroyDue``:
    ``trigger`` (DR-0040 provenance: was this unattended) and ``snapshot`` (DR-0043:
    did an operator explicitly ask). The no-double-snapshot guarantee is preserved by
    ``execute``'s precedence rule rather than by this step declining to run -- see the
    comment there, where the ORDER of the two checks is the load-bearing part.
    v1 drew the same line with the same word
    (``_attempt_auto_snapshot(..., trigger="ttl_expiry"|"manual_destroy")``).

    **Where the profile comes from.** ``SnapshotService`` already resolves it from the
    cluster's active deployment's ``manifest_version`` (``_kubeconfig_and_profile``),
    which is v2's existing, shipping answer -- DR-0040 E1 withdrew the extra column
    this step was first written against.

    **Fail-open, and loudly.** ``SnapshotService.attempt_pre_destroy_snapshot`` never
    raises -- DR-0020 -- and the workflow step additionally carries
    ``on_failure: continue``. A TTL destroy is a DEADLINE: a snapshot that cannot be
    taken must never strand a cluster the TTL says must die. Every skip states its
    reason through ``ctx.progress`` and the Output, because "no snapshot exists" is
    otherwise indistinguishable from "the feature is broken again"."""

    verb = "cluster.auto_snapshot"
    Params = AutoSnapshotParams
    Output = AutoSnapshotOutput
    plane = "domain"
    thin = False
    # undoable=False: a snapshot is not something a compensating destroy should delete
    # -- it exists precisely because the cluster is going away. idempotent=False: two
    # runs legitimately produce two snapshots (the engine's step-row replay makes a
    # re-run of THIS run reuse the recorded output instead).

    def __init__(
        self, *, uow: UnitOfWork, clusters: ClusterRepository, snapshots: Any, clock: Clock
    ) -> None:
        self._uow = uow
        self._clusters = clusters
        self._snapshots = snapshots
        self._clock = clock

    async def execute(self, params: AutoSnapshotParams, ctx: StepContext) -> AutoSnapshotOutput:
        # DR-0043: `snapshot` is tested FIRST, and the order is load-bearing rather than
        # stylistic. An operator's `snapshot_before_destroy=true` is an EXPLICIT,
        # unconditional request (DR-0020); `trigger="ttl_expiry"` asks a different
        # question -- does this cluster's PROFILE opt into auto-snapshotting (DR-0040).
        # Both can be set on one destroy. Testing `trigger` first would route the
        # operator's explicit request into the profile-gated path and silently skip it
        # whenever that profile has `auto_snapshot` disabled -- the operator asked, the
        # profile said no, and nobody would be told. Exactly one snapshot in every
        # combination, and an explicit request always wins.
        if params.snapshot:
            requested_by, take = "operator", self._snapshots.attempt_pre_destroy_snapshot
        elif params.trigger == "ttl_expiry":
            requested_by, take = "timer:ttl", self._snapshots.attempt_auto_snapshot
        else:
            return await self._skip(
                ctx, f"trigger={params.trigger!r} with no snapshot requested"
            )

        async with self._uow() as tx:
            row = self._clusters.get(tx, params.cluster_id)
        if row is None:
            return await self._skip(ctx, "cluster row is gone")

        snapshot = await take(row, actor=requested_by)
        if snapshot is None:
            # Every reason -- auto_snapshot disabled, no profile resolvable, no
            # persistable services, no kubeconfig, a live kubectl failure -- is
            # swallowed by the service, which is DR-0020's ratified fail-open posture.
            return await self._skip(
                ctx, "no snapshot taken (disabled, nothing to persist, or it failed)"
            )

        await ctx.progress(
            f"snapshot {snapshot.name!r} created before destroy (requested by {requested_by})",
            snapshot_id=snapshot.id,
        )
        return AutoSnapshotOutput(snapshot_id=snapshot.id)

    async def _skip(self, ctx: StepContext, reason: str) -> AutoSnapshotOutput:
        await ctx.progress(f"auto-snapshot skipped: {reason}")
        return AutoSnapshotOutput(skipped_reason=reason)
