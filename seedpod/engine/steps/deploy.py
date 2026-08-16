"""engine/steps/deploy.py — Round 10's "load-and-plan" component: the three
``deploy.*`` verbs that load a deployment's audited manifests and plan how they
reach a cluster in waves. ``deploy.load_audit``/``deploy.plan_waves`` are
``plane="domain"`` (no Seam C command at all); ``deploy.prepare_wave`` is
``plane="provider"``/``thin=False`` (a composite issuing N ``KubeRun`` commands
over the already-built, already-conformance-tested ``KubectlProvider`` --
``seedpod/providers/kubectl.py``). No provider logic is reimplemented and no
new provider method is added (both frozen per this round's brief); every
command below is either an existing typed Seam C command (``KubeGetPods``) or
the generic ``KubeRun`` escape hatch already used by ``kube.py``'s own
composites (``kube.wipe_namespace``, ``kube.delete_daemonset``).

The remaining four ``deploy.*``/``kube.apply_docs`` verbs
(``kube.apply_docs``, ``deploy.restore_snapshot``, ``deploy.ensure_rollouts``,
``deploy.await_wave``) are later components of this same round -- this module
does not register or import them.

**deploy.load_audit** -- reads one ``deployment_audits`` row via the
``deployments.spec_ref -> deployment_audits.id`` pointer (see
``DeployLoadAudit``'s own class docstring, below, for why THAT is the right
join, not ``DeploymentAuditRepository.get_by_deployment_id``), parses its
``resolved_manifests`` string into typed ``ManifestDoc``\\ s via the ONE
shared parser (``seedpod/core/deploy_wave.py``'s ``parse_manifest_documents``,
through ``seedpod/services/manifests.py``'s ``normalize_resolved_manifests``
for the gotcha-12 str/dict tolerance -- neither reimplemented), and reads
``persistence_services``/``deploy_wave``/``rollout_timeout_seconds``/
``data_initialization`` back off ``resolved_config`` "like every other
resolved fact" (DR-0028 decision 2's own words; DR-0029 §2/§8 for
``deploy_wave``).

**DR-0025 Erratum E2 (the DEFERRED case) is THIS verb's obligation, not a
later one's.** An audit row may legitimately carry no rendered manifests at
all -- a ``provider_host`` profile whose host was unknowable at decision
time, pending deploy-time re-render once the cluster is ``ACTIVE``. Reading
such a row verbatim and returning ``manifests=[]`` would silently deploy
NOTHING, "the worst failure in this whole class" (this round's own brief).
``DeployLoadAudit.execute`` therefore recognises the DEFERRED case as an
explicit, queryable ``resolved_config`` fact
(``seedpod.core.deploy_wave.DEFERRED_MANIFEST_RENDERING_KEY`` -- never
inferred from emptiness) and RAISES rather than returns whenever it is set,
and separately raises -- distinguishably -- whenever manifests are empty for
ANY other, unmarked reason (a data-integrity defect, not a legitimate deferred
render). Neither case is this component's to resolve: the actual re-render +
in-place audit rewrite, and the ``deployment_service.py`` write path that sets
the marker in the first place, are the LATER restore-and-rehydrate
component's job (this round's own brief, verbatim: "Do not build those
here"). See ``DeployLoadAudit.execute``'s own docstring for the full seam.

**deploy.plan_waves** is the round's crown jewel, and it is now a BUILD, not a
port (DR-0029, docs/decisions/DR-0029-wave-orchestration-is-built.md,
superseding DR-0028 decision 5 and Erratum E1's withdrawn framing outright).
``docs/design/seam-b-engine.md:214-226`` specifies a per-service ``deploy_wave``
ranking (default 3) with unmatched documents falling to wave 0 -- a feature
``reference-code/seedpod/PLAN-wave-orchestration.md`` DESIGNED but v1 never
actually shipped (zero occurrences outside that plan document; v1's real,
shipped mechanism is 20+ busybox init-container polls across 11 manifest
templates, which the plan's own "Problem" section documents as broken on
redeploy). v2 realises that plan here. v1's own three-heuristic matcher
(``_split_manifests_by_service``, reference-code/seedpod/seedpod/jobs/state/
deployment_job.py:66-129) is still salvaged FAITHFULLY -- see ``_service_for``
below (the ONE implementation of the three heuristics -- an earlier revision of
this module carried a second, unused copy named ``_matches_service``; deleted,
not merely left dead, per the round-8b lesson that one rule belongs in one
home) -- but now answers "WHICH service does this document belong to" (to look
up a ``deploy_wave`` rank) rather than v1's original binary "is it a database
service". Two judgment calls this module makes and defends, each documented in
full at its point of decision in ``PlanWaves.execute``'s own docstring: (1) the
malformed-YAML fail-open v1 has at split time cannot arise here at all, because
parsing already happened once, earlier, in ``deploy.load_audit``; (2) when
``persistence_services`` resolve to more than one distinct ``deploy_wave``
rank (a profile inconsistency no shipped profile exercises), the resolved
``restore`` attaches to the LOWEST rank, deterministically. DR-0028 Erratum E2
(surviving DR-0029 unchanged, DR-0029 §5's own words) also binds here: a
restore REQUESTED against a profile declaring no ``persistence_services``
raises a ``PermanentError`` -- v1's own silent-drop of that exact case
(deployment_job.py:530) is a genuine bug, deliberately NOT ported.

**deploy.prepare_wave** salvages v1's pre-apply cleanup
(``_execute_kubectl_apply``, deployment_job.py:926-1017): delete this wave's
Jobs (immutable -- a Job cannot be ``kubectl apply``-updated, only replaced)
and force-delete cluster-wide "stuck" pods, both best-effort. See
``DeployPrepareWave``'s own docstring for the two things this module does
NOT carry forward and says so loudly: the substring-match "stuck pod"
detector v1 wrote is a known-mostly-dead heuristic (documented, not silently
fixed or silently pinned), and v1's post-cleanup ``asyncio.sleep(2)`` settle
cannot be ported at all -- CLAUDE.md's hard rule forbids a step-internal
sleep, and DR-0022 declares this verb non-gateable, so there is no engine-
owned wait mechanism available to carry the grace into either.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel, SecretStr

from seedpod.app.services.deployment_service import rehydrate_cluster_hostname
from seedpod.app.services.profiles import load_deployment_profile
from seedpod.core.deploy_wave import (
    DEFERRED_MANIFEST_RENDERING_KEY,
    MANIFEST_RENDERING_REHYDRATED_KEY,
    DeploymentProfile,
    ManifestDoc,
    SnapshotRestoreSpec,
    Wave,
    parse_manifest_documents,
)
from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
)
from seedpod.data.repositories import (
    ClusterRepository,
    DeploymentAuditRepository,
    DeploymentAuditRow,
    DeploymentRepository,
    DeploymentRow,
)
from seedpod.data.uow import UnitOfWork
from seedpod.engine.provider_step import ProviderStep
from seedpod.engine.step import EmptyOutput, Step, StepContext
from seedpod.providers.contract import KubeGetPods, KubeRun, PodInfo, Result
from seedpod.services.manifests import ManifestResolver, normalize_resolved_manifests

__all__ = [
    "DeployLoadAuditParams",
    "DeployLoadAuditOutput",
    "DeployLoadAudit",
    "PlanWavesParams",
    "PlanWavesOutput",
    "PlanWaves",
    "DeleteJobsParams",
    "DeployPrepareWave",
]


async def _drain(provider: object, command) -> object | None:
    """Run one command to completion, returning its ``Result.value`` (``Progress``
    events are ignored -- none of the commands this module issues emit any).
    A local copy of ``engine/steps/kube.py``'s own module-private ``_drain``
    (not importable across modules -- it carries no leading-underscore export);
    identical shape, same reasoning."""
    value: object | None = None
    async for ev in provider.execute(command):
        if isinstance(ev, Result):
            value = ev.value
    return value


# ---------------------------------------------------------------------------
# deploy.load_audit
# ---------------------------------------------------------------------------


class DeployLoadAuditParams(BaseModel):
    deployment_id: str


class DeployLoadAuditOutput(BaseModel):
    """Conflict 8 grew ``resolved_images``; DR-0028 decision 2 grows
    ``data_initialization`` alongside it -- both top-level, read off the audit's
    ``resolved_config`` "like every other resolved fact" (that decision's own
    words), never nested inside ``profile`` (see ``DeploymentProfile``'s own
    docstring, ``seedpod/core/deploy_wave.py``, for why that field is not a
    profile fact)."""

    manifests: list[ManifestDoc]
    profile: DeploymentProfile
    rollout_timeout_seconds: int
    resolved_images: Mapping[str, str] = {}
    data_initialization: SnapshotRestoreSpec | None = None


def _deployment_not_found(deployment_id: str) -> PermanentError:
    return PermanentError(
        f"deploy.load_audit: deployment {deployment_id!r} not found",
        code=ErrorCode.NOT_FOUND,
        provider="engine",
        command="deploy.load_audit",
        detail={"deployment_id": deployment_id},
    )


def _manifest_rendering_deferred(deployment_id: str, spec_ref: str) -> PermanentError:
    """DR-0025 Erratum E2's DEFERRED case, recognised via the explicit
    ``resolved_config[DEFERRED_MANIFEST_RENDERING_KEY]`` marker
    (``seedpod/core/deploy_wave.py`` -- see that constant's own docstring for
    the full reasoning) -- NEVER inferred from ``resolved_manifests`` being
    empty.

    **Restore-and-rehydrate component, landed.** This is no longer the
    unconditional raise an earlier revision of this module used to stand in
    for real rehydration. ``DeployLoadAudit.execute`` now ATTEMPTS the real
    deploy-time re-render (``_rehydrate``, below) whenever it sees the marker;
    this specific error is raised only if that attempt itself cannot proceed
    -- the one remaining case being the cluster still has no known
    ``public_ip`` (defensive: ``core/machine.py``'s own transition table only
    ever fires ``RunWorkflow(workflow="deploy", ...)`` from
    ``_deployment_pending_cluster_ready`` -- i.e. AFTER ``ClusterReady``, so a
    live deploy-waves run should never actually observe an unset
    ``public_ip`` -- but a step must not silently proceed on an assumption it
    cannot verify). Raising here rather than ever returning ``manifests=[]``
    matches every other unrecoverable-as-is case this module already raises
    for (``_deployment_not_found``/the ``spec_ref``-unset/dangling-audit
    cases just above): the frozen workflow grammar has no ``if``/conditional
    a later step could use to notice an Output-level "still pending" flag and
    skip applying anyway (CLAUDE.md: "no if/when/expressions ... ever"), so a
    raise is the one mechanism available for "this run must not proceed"."""
    return PermanentError(
        f"deploy.load_audit: deployment {deployment_id!r}'s audit {spec_ref!r} has no "
        "rendered manifests yet -- DR-0025 Erratum E2's DEFERRED case (a provider_host "
        "profile whose host was unknowable at decision time), and its cluster still has "
        "no known public_ip to rehydrate against. Returning manifests=[] here would "
        "silently deploy nothing, so this raises instead.",
        code=ErrorCode.NOT_FOUND,
        provider="engine",
        command="deploy.load_audit",
        detail={"deployment_id": deployment_id, "spec_ref": spec_ref, "reason": "manifest_rendering_deferred"},
    )


def _empty_manifests_not_deferred(deployment_id: str, spec_ref: str) -> PermanentError:
    """The converse of ``_manifest_rendering_deferred`` above: manifests parsed
    empty and the audit is NOT marked pending deploy-time rendering. This
    round's own brief is explicit that this must "stay distinguishable from a
    deferred one, and must still be an error" -- so an empty, unmarked audit
    is treated as a data-integrity defect (e.g. a profile that resolved to
    zero services/templates -- unreachable for any shipped profile today, but
    not otherwise ruled out) rather than silently tolerated the way an
    earlier revision of this step did. Distinguished from
    ``_manifest_rendering_deferred`` by both ``code`` (``INVALID_INPUT``, not
    ``NOT_FOUND``) and ``detail["reason"]``, so the two can never be confused
    by a caller inspecting the raised error."""
    return PermanentError(
        f"deploy.load_audit: deployment {deployment_id!r}'s audit {spec_ref!r} has empty "
        "resolved_manifests and is NOT marked pending deploy-time rendering -- DR-0025 "
        "Erratum E2 requires the deferred case to be recognised via an explicit "
        f"resolved_config[{DEFERRED_MANIFEST_RENDERING_KEY!r}] marker, never inferred "
        "from manifests being empty, so an audit that is empty for any OTHER reason is "
        "a data integrity defect, not a legitimate deferred render -- applying it would "
        "silently deploy nothing.",
        code=ErrorCode.INVALID_INPUT,
        provider="engine",
        command="deploy.load_audit",
        detail={"deployment_id": deployment_id, "spec_ref": spec_ref, "reason": "empty_resolved_manifests"},
    )


class DeployLoadAudit(Step[DeployLoadAuditParams, DeployLoadAuditOutput]):
    """**Why this joins through ``deployments.spec_ref``, not
    ``DeploymentAuditRepository.get_by_deployment_id``.** v1's own audit lookup
    (``execute_deployment_job``, deployment_job.py:393-409) is keyed BY the
    deployment id directly (``get_audit_by_deployment_id`` -- v1's own comment:
    "The audit record's deployment_id IS the deployment record's ID"), and v1
    even back-fills that link after the fact when it is missing
    (deployment_job.py:440-446, 471-477: "Link the audit record to this
    deployment"). v2 never populates ``DeploymentAuditRow.deployment_id`` at
    all -- every ``DeploymentAuditRepository.insert`` call site
    (``seedpod/app/services/deployment_service.py``'s ``_audit_row``/
    ``redeploy``) constructs the row with ``deployment_id=None``, unconditionally
    -- so ``get_by_deployment_id`` would find nothing for every real deployment
    this step will ever be asked to load. v2's ACTUAL wiring is the reverse
    pointer: ``DeployRequested(spec_ref=audit_id)`` (``_deploy``,
    ``deployment_service.py:597``) stamps ``deployments.spec_ref`` with the
    audit's own id (``core/records.py``: "the deployment_audits row pointer",
    Conflict 11) -- exactly the join
    ``seedpod/api/routers/deployments.py``'s ``get_deployment`` already uses
    in production (``if row.spec_ref is not None: ... deployment_audits.get(t,
    row.spec_ref)``) for the identical "give me the audit for this
    deployment" question. This step mirrors that established idiom rather than
    reaching for the alternate repository method whose backing column no
    production write path ever populates.

    Reads TWO rows (``deployments`` then ``deployment_audits``, keyed off the
    first read's ``spec_ref``) inside ONE short transaction -- no non-DB IO
    happens between them, so this is still "a transaction encloses only
    database statements" (DR-0008), just two SELECTs instead of one, matching
    ``cluster.load_infra``'s own single-``uow()``-block shape for a comparably
    small read.

    **DR-0025 Erratum E2 (the DEFERRED case) -- restore-and-rehydrate component,
    landed.** An audit row this step reads may legitimately carry no rendered
    manifests yet, marked pending deploy-time rendering (``seedpod.core.
    deploy_wave.DEFERRED_MANIFEST_RENDERING_KEY`` on ``resolved_config``) -- a
    ``provider_host`` profile whose host was unknowable at DECISION time
    (``seedpod/app/services/deployment_service.py``'s ``_deploy``, which now
    DEFERS rather than rejects for exactly this case). ``execute`` below
    recognises that marker explicitly and calls ``_rehydrate`` (below): render
    against the cluster's now-known ``public_ip`` and rewrite THE SAME audit
    row in place (``DeploymentAuditRepository.update_rendered_manifests``),
    then continue with the freshly rendered manifests as if they had been
    there all along. Only if the cluster's host is STILL unknown (defensive --
    see ``_manifest_rendering_deferred``'s own docstring for why this should be
    unreachable on any live run) does this fall back to raising, rather than
    ever returning ``manifests=[]``. A non-deferred audit whose manifests are
    empty for any OTHER reason also raises (``_empty_manifests_not_deferred``),
    distinguishably -- "empty" is never silently tolerated as "nothing to
    deploy" on this path, regardless of cause."""

    verb = "deploy.load_audit"
    Params = DeployLoadAuditParams
    Output = DeployLoadAuditOutput
    plane = "domain"
    thin = False
    # gateable/undoable False, idempotent True (Step's own defaults): a
    # crash mid-rehydration re-enters execute() from scratch -- deterministic
    # (same profile + same now-ACTIVE cluster.public_ip => same rendered
    # output) and self-idempotent besides: a SECOND `execute()` after a
    # successful-but-not-yet-observed rewrite simply reads the marker back as
    # already-cleared and skips `_rehydrate` entirely.

    def __init__(
        self,
        *,
        uow: UnitOfWork,
        deployments: DeploymentRepository,
        deployment_audits: DeploymentAuditRepository,
        clusters: ClusterRepository,
        manifest_resolver: ManifestResolver,
        config_dir: Path,
    ) -> None:
        self._uow = uow
        self._deployments = deployments
        self._deployment_audits = deployment_audits
        self._clusters = clusters
        self._manifest_resolver = manifest_resolver
        self._config_dir = config_dir

    async def execute(self, params: DeployLoadAuditParams, ctx: StepContext) -> DeployLoadAuditOutput:
        """The stored ``resolved_manifests`` is persisted as a plain YAML STRING
        by every v2 write path (``DeploymentAuditRow.resolved_manifests: str``,
        ``seedpod/data/repositories.py``), but ``normalize_resolved_manifests``
        (``seedpod/services/manifests.py`` -- the ONE shared home for the
        gotcha-12 tolerance, not reimplemented here per this round's own brief)
        is still applied before parsing: it is the same defensive str/dict
        normalization v1 ran inline at this exact call site
        (deployment_job.py:482-493), preserved for a value that predates this
        column's own type discipline or arrives from any future write path
        that is less careful than today's.

        This is the ONE place a rendered manifest's raw YAML text is ever
        parsed in v2's redesigned pipeline (``parse_manifest_documents``'s own
        docstring, ``seedpod/core/deploy_wave.py``) -- ``deploy.plan_waves``
        below receives already-typed ``ManifestDoc``\\ s and never touches raw
        text again. A malformed manifest raises ``PermanentError`` HERE,
        loudly, rather than v1's own fail-open at split time (that function's
        own docstring has the full genuine-correctness-fix reasoning).

        **DR-0025 Erratum E2.** Before any of that parsing runs, the audit's
        ``resolved_config`` is checked for the DEFERRED marker
        (``DEFERRED_MANIFEST_RENDERING_KEY``) UNCONDITIONALLY -- independent
        of whatever ``resolved_manifests`` happens to contain, per
        ``_manifest_rendering_deferred``'s own docstring ("a stale or partial
        value sitting alongside the marker must not be silently applied
        either"). Only once that marker is confirmed absent do manifests get
        parsed and checked for emptiness on their own terms
        (``_empty_manifests_not_deferred``) -- so "recognised as pending" and
        "empty for some other reason" can never be confused with each other,
        and neither is ever allowed to fall through to a returned
        ``manifests=[]``."""
        async with self._uow() as tx:
            deployment = self._deployments.get(tx, params.deployment_id)
            if deployment is None:
                raise _deployment_not_found(params.deployment_id)
            if deployment.spec_ref is None:
                raise PermanentError(
                    f"deploy.load_audit: deployment {params.deployment_id!r} has no resolved "
                    "manifest (spec_ref is unset) -- a deploy workflow should never run before "
                    "DeployRequested carries one",
                    code=ErrorCode.NOT_FOUND,
                    provider="engine",
                    command=self.verb,
                    detail={"deployment_id": params.deployment_id},
                )
            audit = self._deployment_audits.get(tx, deployment.spec_ref)
            if audit is None:
                raise PermanentError(
                    f"deploy.load_audit: deployment {params.deployment_id!r}'s spec_ref "
                    f"{deployment.spec_ref!r} does not resolve to any deployment_audits row",
                    code=ErrorCode.NOT_FOUND,
                    provider="engine",
                    command=self.verb,
                    detail={"deployment_id": params.deployment_id, "spec_ref": deployment.spec_ref},
                )

        resolved_config = audit.resolved_config
        if resolved_config.get(DEFERRED_MANIFEST_RENDERING_KEY):
            audit = await self._rehydrate(params.deployment_id, deployment, audit)
            resolved_config = audit.resolved_config

        manifests_yaml = normalize_resolved_manifests(audit.resolved_manifests)
        manifests = parse_manifest_documents(manifests_yaml)
        if not manifests:
            raise _empty_manifests_not_deferred(params.deployment_id, deployment.spec_ref)

        profile = DeploymentProfile(
            persistence_services=list(resolved_config.get("persistence_services") or []),
            # DR-0029 §2/§8: the service-name-to-deploy_wave mapping is written
            # into resolved_config by `_build_resolved_config`
            # (seedpod/app/services/deployment_service.py) "like every other
            # resolved fact" -- read back here unchanged, defaulting to `{}`
            # for an audit row written before DR-0029 (no key at all), which
            # `deploy.plan_waves` treats identically to "no services declared"
            # (`DeploymentProfile`'s own docstring: the degenerate case).
            deploy_wave=dict(resolved_config.get("deploy_wave") or {}),
        )
        raw_data_initialization = resolved_config.get("data_initialization")
        data_initialization = (
            SnapshotRestoreSpec.model_validate(raw_data_initialization)
            if raw_data_initialization
            else None
        )
        return DeployLoadAuditOutput(
            manifests=manifests,
            profile=profile,
            rollout_timeout_seconds=resolved_config.get("rollout_timeout_seconds", 300),
            resolved_images=dict(audit.resolved_images),
            data_initialization=data_initialization,
        )

    async def _rehydrate(
        self, deployment_id: str, deployment: DeploymentRow, audit: DeploymentAuditRow
    ) -> DeploymentAuditRow:
        """DR-0025 Erratum E2 point (ii), the deploy-time half: render against the
        real, now-known provisioned host and rewrite THE SAME audit row in place
        (``DeploymentAuditRepository.update_rendered_manifests``). Only ever
        called from ``execute`` above, and only when ``resolved_config`` carries
        the DEFERRED marker.

        DR-0008 discipline, THREE separate short phases, never nested inside one
        another or inside ``execute``'s own already-closed read ``uow()``: (1) a
        short DB-only read for the cluster row; (2) non-DB IO -- disk profile
        load, then pure-in-memory Jinja rendering (``render_only``, ``seedpod/
        services/manifests.py``) -- with NO transaction open; (3) a short DB-only
        write. Mirrors ``deployment_service.py``'s own ``_deploy`` (profile
        load + config build + resolve, all strictly BETWEEN two ``uow()``
        blocks) rather than inventing a new sequencing shape for the identical
        constraint.

        Re-renders ONLY the hostname-dependent half (``render_only``'s own
        docstring, ``seedpod/services/manifests.py``): ``resolved_images``/
        ``resolved_secrets`` are read back off the ALREADY-DECIDED audit
        unchanged, never re-resolved -- the image an operator already saw at
        decision time is the image that gets applied, never silently swapped
        for whatever GHCR/branch discovery would pick NOW."""
        async with self._uow() as tx:
            cluster = self._clusters.get(tx, deployment.cluster_id)

        if cluster is None or not cluster.public_ip:
            # Defensive -- see _manifest_rendering_deferred's own docstring for
            # why a live deploy-waves run should never actually reach this.
            # `audit.id` (never None) stands in for `deployment.spec_ref` here --
            # the two are the same value by construction (this method is only
            # ever called with the audit `execute` just loaded VIA that pointer).
            raise _manifest_rendering_deferred(deployment_id, audit.id)

        profile, raw_profile = load_deployment_profile(self._config_dir, audit.deployment_profile_name)
        hostname_present, cluster_hostname = rehydrate_cluster_hostname(
            raw_profile, cluster_slug=cluster.slug, provider_host=cluster.public_ip
        )

        new_resolved_config: dict[str, object] = dict(audit.resolved_config)
        new_resolved_config[DEFERRED_MANIFEST_RENDERING_KEY] = False
        new_resolved_config[MANIFEST_RENDERING_REHYDRATED_KEY] = True
        if hostname_present:
            # Covers BOTH "resolved to a real value" and "this profile
            # deliberately has no hostname" (`cluster_hostname is None` with
            # `hostname_present` True, strategy `"none"`) -- Erratum E1's
            # "key PRESENT, feature gates evaluate false cleanly" case.
            # `rehydrate_cluster_hostname`'s own `(present, value)` pair is
            # what makes this branch correct rather than merely convenient --
            # see that function's own docstring for why a bare `str | None`
            # could not express this distinction to its caller.
            new_resolved_config["cluster_hostname"] = cluster_hostname
        # else: the key stays OMITTED -- this is Erratum E1's UNKNOWABLE
        # branch specifically ("a strategy wanted a host and could not
        # produce one"), never the deliberately-no-hostname branch (handled
        # above) -- render_only's own StrictUndefined raises, naming
        # "cluster_hostname", the identical failure decision time itself
        # would have produced for a genuinely unresolvable strategy; this
        # branch does not paper over that.

        template_files, rendered_manifests = self._manifest_resolver.render_only(
            profile,
            resolved_image_urls=dict(audit.resolved_images),
            resolved_secrets=dict(audit.resolved_secrets),
            resolved_config=new_resolved_config,
        )

        async with self._uow() as tx:
            persisted = self._deployment_audits.update_rendered_manifests(
                tx,
                audit.id,
                resolved_manifests=rendered_manifests,
                resolved_config=new_resolved_config,
                template_files_used=template_files,
                key_class=audit.key_class,
            )
        if not persisted:
            raise PermanentError(
                f"deploy.load_audit: rehydration of audit {audit.id!r} could not be "
                "persisted -- the row was gone by the time this step tried to rewrite it",
                code=ErrorCode.NOT_FOUND,
                provider="engine",
                command=self.verb,
                detail={"deployment_id": deployment_id, "spec_ref": audit.id},
            )

        return dataclasses.replace(
            audit,
            resolved_manifests=rendered_manifests,
            resolved_config=new_resolved_config,
            template_files_used=tuple(template_files),
        )


# ---------------------------------------------------------------------------
# deploy.plan_waves -- the crown jewel. DR-0029: v2 BUILDS wave orchestration
# (docs/decisions/DR-0029-wave-orchestration-is-built.md), realising
# reference-code/seedpod/PLAN-wave-orchestration.md -- a design plan v1 itself
# never executed -- rather than porting a v1 binary split. This supersedes an
# earlier revision of this module that implemented DR-0028 decision 5's
# withdrawn "generalizes a v1 binary split" framing (three tiers: infra/
# persistence/app, no per-service rank); see ``Wave``'s and
# ``DeploymentProfile``'s own docstrings (``seedpod/core/deploy_wave.py``) for
# the full supersession story.
# ---------------------------------------------------------------------------


class PlanWavesParams(BaseModel):
    manifests: list[ManifestDoc]
    profile: DeploymentProfile
    rollout_timeout_seconds: int
    data_initialization: SnapshotRestoreSpec | None = None


class PlanWavesOutput(BaseModel):
    waves: list[Wave]


def _service_for(doc: ManifestDoc, services: frozenset[str]) -> str | None:
    """DR-0029 §3: which declared service (out of ``DeploymentProfile.
    deploy_wave``'s own key set -- EVERY service the profile declares, not
    just ``persistence_services``) a document belongs to, or ``None`` when it
    matches none (DR-0029's own "documents matching no service go to wave 0").

    THE ONE implementation of v1's three-heuristic matcher
    (``_split_manifests_by_service``, reference-code/seedpod/seedpod/jobs/
    state/deployment_job.py:96-119, which tested "is this doc a member of THE
    database-service SET" via three heuristics ORed together): (a)
    ``metadata.name`` EQUALS or STARTS WITH the service name -- v1's own
    comment names the real case, "postgres-deployment" matches service
    "postgres"; (b) ``metadata.labels.app`` OR
    ``spec.template.metadata.labels.app`` equals the service name; (c)
    ``spec.selector.matchLabels.app`` equals the service name, independent of
    (b). DR-0029 §3 reuses the identical three heuristics to answer a
    DIFFERENT question than v1's binary one -- "which ONE service" rather
    than "is it in THE database-service set" -- so this function returns the
    matched service name (or ``None``), not a bool. Dropping any one
    heuristic silently misclassifies real, shipped manifests exactly as it
    did before this DR (deploying a service's Secret into the wrong wave, or
    stranding a StatefulSet in wave 0 -- DR-0029's own "Not a kind test").
    (b)/(c) read ``doc.body`` directly (not denormalized onto ``ManifestDoc``),
    the same nested ``.get(key, {})``-chained reads v1 itself uses, guarding
    an explicit ``null`` at any level the same way
    ``parse_manifest_documents`` already guards ``metadata``.

    Evaluated in v1's own heuristic ORDER (name, then label, then selector --
    mirroring ``_split_manifests_by_service``'s sequential ``if not
    is_database_manifest:`` chain, deployment_job.py:99-119), because v1's
    original question ("is this doc IN the set") only ever needed ANY
    heuristic to fire -- this question ("which ONE service") needs a
    tie-break the moment more than one service could satisfy the SAME
    heuristic, and no shipped profile exercises that today. Only the
    name-prefix heuristic (a) can ever produce more than one
    candidate (a label/selector ``==`` comparison is exact, so at most one
    service can ever match it) -- e.g. a hypothetical services ``postgres`` and
    ``postgres-replica`` both prefix-matching a doc named
    ``postgres-replica-0``; the LONGEST (most specific) matching prefix wins,
    the same "more specific wins" rule a router or a glob matcher would apply,
    never left to set/dict iteration order."""
    if not services:
        return None

    name = doc.name
    if name:
        prefix_matches = [s for s in services if name == s or name.startswith(s)]
        if prefix_matches:
            return max(prefix_matches, key=len)

    metadata = doc.body.get("metadata") or {}
    spec = doc.body.get("spec") or {}
    labels = metadata.get("labels") or {}
    template_labels = ((spec.get("template") or {}).get("metadata") or {}).get("labels") or {}
    app_label = labels.get("app") or template_labels.get("app")
    if app_label in services:
        return app_label

    match_labels = (spec.get("selector") or {}).get("matchLabels") or {}
    selector_label = match_labels.get("app")
    if selector_label in services:
        return selector_label

    return None


def _wave_index_for(doc: ManifestDoc, deploy_wave: Mapping[str, int]) -> int:
    """DR-0029 §1/§2/§3: look up the matched service's rank in
    ``DeploymentProfile.deploy_wave`` (``.get(service, 3)`` is a DEFENSIVE
    fallback only -- the real writer, ``_build_resolved_config``
    (``seedpod/app/services/deployment_service.py``), already fills every
    declared service's key with 3 at WRITE time when its YAML sets no
    explicit ``deploy_wave``, per ``DeploymentProfile``'s own docstring: "is
    this service's key present" and "is this service declared at all" are the
    SAME question by construction). A document matching no service (the key
    lookup itself, via ``_service_for``, returns ``None``) is unconditionally
    WAVE 0 -- DR-0029 §3's own words, verbatim: "documents matching no service
    go to wave 0"."""
    service = _service_for(doc, frozenset(deploy_wave.keys()))
    if service is None:
        return 0
    return deploy_wave.get(service, 3)


def _persistence_wave_index(deploy_wave: Mapping[str, int], persistence_services: frozenset[str]) -> int:
    """The restore-attachment half of DR-0029 §5 ("the restore attaches to the
    wave carrying persistence_services") MUST share ``_wave_index_for``'s own
    "is this service's key present" test -- not silently diverge from it.
    ``DeploymentProfile``'s own docstring states the invariant this relies
    on: by construction the WRITER (``_build_resolved_config``) puts an
    entry in ``deploy_wave`` for EVERY service the profile declares, so "is
    this service's key present" and "is this service declared at all" are
    the SAME question, always -- for a genuinely-declared persistence
    service, this ``.get(service, 0)`` default is never actually reached.

    It exists only for the defensive, off-the-happy-path case ``deploy.
    load_audit`` itself already names (an audit row written before DR-0029,
    or any other row missing the ``deploy_wave`` key entirely): if this used
    ``.get(service, 3)`` instead -- a REACHABLE default, unlike
    ``_wave_index_for``'s own -- a persistence service absent from
    ``deploy_wave`` would compute a restore rank of 3 while every one of
    that SAME service's documents (via ``_service_for``, which can only ever
    match a key actually PRESENT in ``deploy_wave``) falls to wave 0 --
    manufacturing an empty wave 3 to carry the restore while the real
    database documents sit in wave 0, applying everything (app pods
    included) before restoring, the exact inverse of the intended ordering,
    silently. ``.get(service, 0)`` instead keeps the two paths in lockstep:
    an absent key means "no service matched this document either", so the
    restore falls to the SAME wave 0 a persistence document with no
    ``deploy_wave`` entry would."""
    return min(deploy_wave.get(service, 0) for service in persistence_services)


def _wave_from(
    index: int, docs: list[ManifestDoc], gate_timeout_seconds: int, *, restore: SnapshotRestoreSpec | None = None
) -> Wave:
    """``jobs``/``deployments`` are the bare NAMES of this wave's own ``Job``/
    ``Deployment`` documents -- ``deploy.prepare_wave`` deletes the former by
    name before re-apply (Jobs are immutable), ``deploy.await_wave`` polls
    both by name (``kubectl rollout status``/``condition=complete`` need a
    name), and ``deploy.ensure_rollouts`` restarts the latter by name. Neither
    list needs a namespace: every shipped manifest template lives in one fixed
    namespace ("default") throughout this pipeline."""
    jobs = [doc.name for doc in docs if doc.kind == "Job" and doc.name]
    deployments = [doc.name for doc in docs if doc.kind == "Deployment" and doc.name]
    return Wave(
        index=index, docs=list(docs), jobs=jobs, deployments=deployments,
        gate_timeout_seconds=gate_timeout_seconds, restore=restore,
    )


def _restore_requested_without_persistence_service(persistence_services: list[str]) -> PermanentError:
    """DR-0028 Erratum E2 (ratified 2026-08-06), SURVIVING DR-0029 UNCHANGED
    (DR-0029 §5: "DR-0028 Erratum E2 also survives unchanged"): v1's real gate
    is compound -- ``if data_initialization and database_services:``
    (``deployment_job.py:530``) -- so a deploy request that explicitly asks for
    a snapshot restore against a profile declaring NO ``persistence_services``
    gets NO restore and NO error in v1: the ``else`` branch just applies
    everything, silently ignoring ``data_initialization``. **That is a v1 bug
    and is deliberately NOT ported.** An operator who believes their data was
    restored when it was not is strictly worse off than one whose deployment
    failed (the erratum's own words) -- this raises instead, naming the
    mismatch, rather than silently dropping the requested restore the way v1
    does."""
    return PermanentError(
        "deploy.plan_waves: data_initialization was requested but the profile declares no "
        "persistence_services to restore into -- v1 silently drops this exact combination "
        "(reference-code/seedpod/seedpod/jobs/state/deployment_job.py:530's compound "
        "`if data_initialization and database_services:` gate), a genuine v1 bug NOT ported "
        "(DR-0028 Erratum E2, surviving DR-0029 unchanged): an operator who believes their "
        "data was restored when it was not is strictly worse off than one whose deployment "
        "failed outright.",
        code=ErrorCode.INVALID_INPUT,
        provider="engine",
        command="deploy.plan_waves",
        detail={"persistence_services": ",".join(persistence_services) or "<none>"},
    )


class PlanWaves(Step[PlanWavesParams, PlanWavesOutput]):
    """THE crown-jewel salvage-BECOMES-BUILD of this component (DR-0029). Pure
    -- no IO, no ``ctx`` access beyond the contract's own signature -- so this
    class needs no constructor dependencies at all."""

    verb = "deploy.plan_waves"
    Params = PlanWavesParams
    Output = PlanWavesOutput
    plane = "domain"
    thin = False
    # gateable/undoable False, idempotent True (Step's own defaults): a pure
    # function of its Params has nothing to gate, undo, or worry about re-entering.

    async def execute(self, params: PlanWavesParams, ctx: StepContext) -> PlanWavesOutput:
        """**DR-0029 governs the wave model this method builds** (docs/decisions/
        DR-0029-wave-orchestration-is-built.md), superseding DR-0028 decision 5
        and Erratum E1's withdrawn "three-tier infra/persistence/app" framing
        outright. ``docs/design/seam-b-engine.md:214-226`` is correct as
        written and needed no amendment; what changed is HOW documents reach a
        wave index: match a document to a declared SERVICE (``_service_for``
        above, the v1 three-heuristic salvage, now answering "which service"
        rather than "is it a database service"), then look up that service's
        ``deploy_wave`` rank (default 3, back-compat single apply) in
        ``params.profile.deploy_wave``. A document matching no declared
        service is unconditionally WAVE 0 (RBAC/ConfigMaps/Secrets/
        ghcr-secret -- DR-0029 §3, correcting seam-b's own "gotcha 17"
        mis-citation, which this DR's Consequences fixed at the seam-b source).

        **Judgment call 1 -- the malformed-YAML fail-open does not arise here
        at all, and is not re-decided by this step.** v1's
        ``_split_manifests_by_service`` fails OPEN on a YAML parse error
        (deployment_job.py:126-129: ``except yaml.YAMLError: ...; return "",
        rendered_manifests`` -- silently downgrading a malformed manifest into
        "no database manifests, apply everything as one wave, skip the
        restore phase entirely", logged but never surfaced). DR-0028 flagged
        this as a candidate not-ported and this round's own brief asks for a
        deliberate call. The call was already made, once, upstream:
        ``deploy.load_audit`` (this module, above) is the ONE place a
        rendered manifest's raw YAML text is EVER parsed in v2's redesigned
        pipeline -- ``parse_manifest_documents`` (``seedpod/core/
        deploy_wave.py``) raises loudly there instead of falling open, and
        that decision's own docstring has the full reasoning. This step
        receives ALREADY-VALID, ALREADY-TYPED ``list[ManifestDoc]`` -- there
        is no raw text left to fail open OR closed on here, and no second
        parse to protect.

        **Judgment call 2 -- a persistence-service's rank may resolve
        ambiguously (more than one distinct ``deploy_wave`` value across
        ``persistence_services``), and this step picks the LOWEST rank as
        "the" persistence wave a resolved ``restore`` attaches to.** DR-0029
        §5's own words describe a single wave ("the wave carrying
        ``persistence_services`` -- wave 1 in the plan's example"), which
        presumes every persistence service shares one rank -- true of every
        shipped profile's worked example, but not a constraint this DTO or
        this step enforces structurally (``DeploymentProfile.deploy_wave`` is
        an unconstrained ``Mapping[str, int]``). Picking the lowest rank
        deterministically (rather than raising on the inconsistency, or
        picking arbitrarily by set/dict order) mirrors v1's own "restore
        happens once, as early as the data phase can support it" intent
        without inventing a NEW validation rule DR-0029 never asked for.

        **DR-0028 Erratum E2, surviving DR-0029 unchanged (DR-0029 §5's own
        words) -- a requested restore is never silently dropped.** v1's
        compound gate (deployment_job.py:530) means a request that explicitly
        asks for a snapshot restore against a profile declaring NO
        ``persistence_services`` gets no restore and no error in v1 --
        deliberately NOT ported (see
        ``_restore_requested_without_persistence_service`` above for the full
        reasoning): this step raises a ``PermanentError`` instead, checked
        FIRST, before any classification runs.

        **No separate empty-docs restore wave** (DR-0028 Erratum E1 point 2
        survives DR-0029 unchanged -- only its wave-MODEL framing was
        withdrawn, DR-0029 §5): when a restore is requested, the wave carrying
        it is forced to exist even with zero matching docs in THIS apply (a
        persistence service declared but nothing of its own is being deployed
        this run remains a legitimate case a resolved ``restore`` must still
        attach to). No other empty wave is ever manufactured: every other
        index present in the output is present because at least one document
        actually landed there.

        ``gate_timeout_seconds`` (every wave below): v1's own database-pod
        wait is a bare hardcoded ``timeout=180`` (deployment_job.py:555),
        which this round's brief explicitly forbids re-hardcoding. Every wave
        this step returns instead uses ``params.rollout_timeout_seconds`` --
        the SAME value v1 itself reads off ``resolved_config`` (deployment_job.py
        :415, `resolved_config.get("rollout_timeout_seconds", 300)`) and later
        uses as the OVERALL post-apply rollout-wait budget
        (deployment_job.py:638-639, `_wait_for_rollout(cluster_id,
        k8s_provider, rollout_timeout)`) -- not a new invented source, but the
        one numeric budget v1 already threads through this exact deployment,
        now generalized from "the final wait only" to "every wave's own
        gate"."""
        persistence_services = frozenset(params.profile.persistence_services)

        if params.data_initialization is not None and not persistence_services:
            raise _restore_requested_without_persistence_service(params.profile.persistence_services)

        deploy_wave = params.profile.deploy_wave
        buckets: dict[int, list[ManifestDoc]] = {}
        for doc in params.manifests:
            buckets.setdefault(_wave_index_for(doc, deploy_wave), []).append(doc)

        persistence_wave_index: int | None = None
        if params.data_initialization is not None:
            # Erratum E2's check above already guarantees `persistence_services`
            # is non-empty on this branch, so `_persistence_wave_index` never
            # sees an empty iterable. `_persistence_wave_index` (not an inline
            # `.get(service, 3)`) is the fix for a real drift: see its own
            # docstring for why this rank lookup MUST share `_wave_index_for`'s
            # "is this service's key present" test rather than diverging from it.
            persistence_wave_index = _persistence_wave_index(deploy_wave, persistence_services)
            buckets.setdefault(persistence_wave_index, [])

        waves = [
            _wave_from(
                index,
                buckets[index],
                params.rollout_timeout_seconds,
                restore=params.data_initialization if index == persistence_wave_index else None,
            )
            for index in sorted(buckets)
        ]

        return PlanWavesOutput(waves=waves)


# ---------------------------------------------------------------------------
# deploy.prepare_wave -- Jobs (immutable, delete-before-reapply) + stuck pods.
# ---------------------------------------------------------------------------


class DeleteJobsParams(BaseModel):
    kubeconfig: SecretStr
    jobs: list[str]


# v1's own defaults (reference-code/seedpod/seedpod/jobs/state/deployment_job.py):
# run_kubectl's own default timeout (30.0s, kubernetes.py:1262) for the Job delete;
# the EXPLICIT 10.0s override v1 gives the stuck-pod force-delete specifically
# ("Shorter timeout since we're forcing deletion", deployment_job.py:1001).
_JOB_DELETE_TIMEOUT_S = 30.0
_STUCK_POD_DELETE_TIMEOUT_S = 10.0

# v1's own stuck-pod substring list, verbatim (deployment_job.py:978-986). See
# DeployPrepareWave's own docstring for why most of these can never actually
# match what this step (and v1 itself) tests them against.
_STUCK_POD_STATUS_SUBSTRINGS: tuple[str, ...] = (
    "CrashLoopBackOff",
    "Error",
    "ImagePullBackOff",
    "ErrImagePull",
    "Failed",
    "Pending",
    "Init:",
)


def _is_stuck_pod(status: str) -> bool:
    return any(substring in status for substring in _STUCK_POD_STATUS_SUBSTRINGS)


class DeployPrepareWave(ProviderStep[DeleteJobsParams, EmptyOutput]):
    """Salvages v1's pre-apply cleanup (``_execute_kubectl_apply``, reference-code/
    seedpod/seedpod/jobs/state/deployment_job.py:926-1017), run before EVERY wave's
    own ``kube.apply_docs`` (``config/workflows/deploy-waves.yml``'s ``prep`` step,
    ``on_failure: continue`` -- "best-effort, as in v1", gotcha 5).

    **Job deletion is the load-bearing half.** A Kubernetes ``Job`` is immutable:
    re-applying an unchanged (or even changed) Job spec over an existing one FAILS
    outright, rather than converging like a Deployment does. v1 deletes every Job
    the about-to-be-applied manifest set names, with ``--ignore-not-found`` (a
    missing Job is success, not an error), before ``kubectl apply`` ever runs
    (deployment_job.py:927-957). v2 threads the job NAMES in as typed ``Params``
    (``Wave.jobs``, computed once by ``deploy.plan_waves`` from THIS wave's own
    docs -- ``_wave_from``, above) rather than re-parsing the about-to-be-applied
    YAML a second time the way v1 does inline; the deletion itself is unchanged.

    **Stuck-pod force-deletion is salvaged FAITHFULLY, including a known v1
    weakness this docstring names loudly rather than silently fixing or silently
    pinning.** v1 lists every pod CLUSTER-WIDE (``k8s_provider.get_pods(cluster_id)``
    with no namespace -- deployment_job.py:963, matching this step's own
    ``KubeGetPods(kubeconfig, namespace=None)`` below, which resolves to ``-A``
    the identical way), and force-deletes (``--force --grace-period=0``) any pod
    whose status matches one of seven substrings meant to catch
    CrashLoopBackOff/Error/ImagePullBackOff/ErrImagePull/Failed/Pending/stuck-Init
    pods (deployment_job.py:975-986). **What v1 actually tests those substrings
    against is ``PodInfo.status``, which both v1's OWN ``get_pods`` (reference-code/
    seedpod/seedpod/providers/kubernetes.py:544,554: ``status.get("phase",
    "Unknown")``) and v2's identically-salvaged ``KubectlProvider``/``KubeGetPods``
    (``providers/kube_types.py``/``providers/kubectl.py``'s own ``_parse_pod``,
    same field) populate with the bare Kubernetes pod PHASE --
    ``Pending``/``Running``/``Succeeded``/``Failed``/``Unknown``, nothing else.
    None of those five values can ever contain the substrings
    ``CrashLoopBackOff``/``Error``/``ImagePullBackOff``/``ErrImagePull``/``Init:``
    -- those are CONTAINER-level waiting reasons (``status.containerStatuses[]
    .state.waiting.reason``, what ``kubectl get pods``' own STATUS column
    computes, and clearly what v1's own comment ("Detect stuck pods:
    CrashLoopBackOff, Error, ImagePullBackOff, Pending, etc.") intended to catch),
    a value ``PodInfo``/``get_pods`` never carries at all. In practice, only two
    of the seven conditions are EVER reachable against this field
    (``'Pending' in status`` and ``'Failed' in status``) -- a genuine v1 bug,
    not a design choice, that has shipped unnoticed because a merely-Pending or
    -Failed pod still gets swept, which covers a real (if narrower) slice of
    "stuck".

    This is ported VERBATIM anyway -- same seven substrings, same field, same
    outcome -- rather than "fixed" by querying container-level waiting reasons
    (which this step COULD do, via the ``KubeRun`` escape hatch and its own raw
    JSON parsing, without touching any frozen provider file). Three reasons,
    stated so a future change is a deliberate, reviewed edit rather than a
    rediscovery: (1) this round's own brief singles out JOB deletion, not stuck-
    pod detection, as "the piece to look for" -- the crown-jewel 3-heuristic
    classification this round explicitly asks to get right is
    ``deploy.plan_waves``'s, not this step's; (2) this whole step is
    already-established best-effort hygiene (every failure inside it is caught
    and logged, never raised -- below), not the sole safety net for a stuck
    deployment -- a genuinely stuck pod this sweep misses still gets superseded
    by the SAME wave's own ``kube.apply_docs``/``deploy.ensure_rollouts``/
    Kubernetes's own rollout mechanics; (3) inventing a richer, unreviewed
    detector here -- for a FORCE-DELETE operation, which is inherently a little
    dangerous if over-eager -- is exactly the scope expansion CLAUDE.md's
    salvage discipline exists to rule out, not something this component's brief
    asks for or authorizes.

    **v1's own post-cleanup ``await asyncio.sleep(2)`` (deployment_job.py:1011,
    "Give Kubernetes a moment to cleanup and start recreating pods") is NOT
    ported, and cannot be.** CLAUDE.md's hard rule forbids any step-internal
    sleep/poll/retry loop ("no command waits, all waiting is an engine gate" --
    Seam C taste call 2); the physics-constant-becomes-gate-data pattern that
    already carried an analogous v1 grace forward (``kube.delete_daemonset``'s
    own ``settle_seconds``, DR-0022 Erratum E2) is not available here either --
    this verb is declared NOT gateable in the DR-0022 vocabulary
    (``tests/engine/declared_verbs.py``'s own fixture: no ``gateable=True``),
    a decision this round's brief treats as LAW ("the vocabulary... [is] LAW...
    not yours to revisit"). There is accordingly no engine-owned wait mechanism
    this verb could carry even 2 seconds of settle time through. The
    SUBSEQUENT ``apply`` step (``kube.apply_docs``, ``retry: kubectl_default``)
    provides some cushion for any residual just-deleted-pod inconsistency, but
    that is a pre-existing mitigation this step relies on, not a replacement
    for v1's grace -- named here so the omission is a recorded, deliberate
    consequence of the hard rule, not a silently dropped line of v1 behaviour.

    ``thin=False``: this step issues however many ``KubeRun`` delete commands
    ``params.jobs`` names, plus one ``KubeGetPods`` list, plus however many
    ``KubeRun`` force-deletes the stuck-pod sweep finds -- never exactly one
    Seam C command. ``undoable=False`` (``ProviderStep`` hard-defaults ``True``):
    deleting a Job/pod as part of a deploy's own pre-apply cleanup has no
    sensible inverse, and none of v1's own Job/pod deletions here were ever
    reversible either."""

    verb = "deploy.prepare_wave"
    provider_name: ClassVar[str] = "kubectl"
    Params = DeleteJobsParams
    Output = EmptyOutput
    undoable = False
    thin = False

    def command(self, params: DeleteJobsParams) -> KubeGetPods:
        """The ONE command every call to this step issues unconditionally
        (Job-count-independent), kept implemented -- matching ``kube.wipe_
        namespace``'s own precedent -- so the ``ProviderStep`` contract stays
        honest even though ``execute()`` below is fully overridden and never
        calls ``super().execute()``/``self.command()`` through the inherited
        template. Never actually consulted for undo: ``undoable=False`` means
        the engine never calls ``.undo()`` for this verb at all."""
        return KubeGetPods(kubeconfig=params.kubeconfig.get_secret_value(), namespace=None)

    def output_from(self, value: object) -> EmptyOutput:
        return EmptyOutput()

    async def execute(self, params: DeleteJobsParams, ctx: StepContext) -> EmptyOutput:
        provider = ctx.services.providers[self.provider_name]
        kubeconfig = params.kubeconfig.get_secret_value()

        for job_name in params.jobs:
            try:
                await _drain(
                    provider,
                    KubeRun(
                        kubeconfig=kubeconfig,
                        args=("delete", "job", job_name, "--ignore-not-found=true"),
                        timeout_s=_JOB_DELETE_TIMEOUT_S,
                    ),
                )
            except ProviderError as exc:
                # Best-effort, exactly v1 (deployment_job.py:950-957: logs a
                # warning and continues -- "Don't fail the deployment").
                await ctx.progress(f"deploy.prepare_wave: failed to delete Job {job_name!r}: {exc}")

        pods: tuple[PodInfo, ...] = ()
        try:
            value = await _drain(provider, self.command(params))
            # KubeGetPods's Result is always tuple[PodInfo, ...] (contract.py) --
            # this step issues no other command that could hand `command()` a
            # different shape, so a plain isinstance guard (never raising) is
            # enough; see class docstring for why listing failures are tolerated
            # the same way v1 tolerates them (deployment_job.py:965-967).
            pods = value if isinstance(value, tuple) else ()
        except InfrastructureUnreachableError:
            # NOT tolerated, unlike the bare ProviderError case just below.
            # CLAUDE.md's hard rule: InfrastructureUnreachableError means
            # "cannot determine state" -- swallowing it here would turn
            # "I don't know what pods exist" into "there are no stuck pods",
            # a conflation the hard rule exists to forbid. Left to propagate:
            # the workflow's own `prep` step already runs `on_failure:
            # continue` (deploy-waves.yml), so a raise here still doesn't
            # block the wave's `apply` -- it just records this step's own
            # attempt honestly (failed, not a vacuous success) instead of
            # silently sweeping nothing.
            raise
        except ProviderError as exc:
            await ctx.progress(f"deploy.prepare_wave: failed to list pods for the stuck-pod sweep: {exc}")

        for pod in pods:
            if not _is_stuck_pod(pod.status):
                continue
            try:
                await _drain(
                    provider,
                    KubeRun(
                        kubeconfig=kubeconfig,
                        args=("delete", "pod", pod.name, "-n", pod.namespace, "--force", "--grace-period=0"),
                        timeout_s=_STUCK_POD_DELETE_TIMEOUT_S,
                    ),
                )
            except ProviderError as exc:
                await ctx.progress(
                    f"deploy.prepare_wave: failed to force-delete stuck pod {pod.namespace}/{pod.name}: {exc}"
                )

        return EmptyOutput()
