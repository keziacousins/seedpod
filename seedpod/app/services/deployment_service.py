"""``DeploymentService`` -- THE version-update orchestration (the parity spine),
plus the deployment CRUD surface ``api/deployments.py`` (a later Round-6
component) calls.

Constructor shape starts from docs/design/seam-d-foundation.md Decision 8 step 9's
``DeploymentService(dispatcher, repos, uow, rules=rules, crypto=crypto,
clock=clock)``, extended exactly as ``seedpod/app/factory.py``'s own TODO comment
already documents (``manifest_resolver=manifest_resolver, dns=dns`` -- DR-0015's
supporting services), plus three more this class's real job needs and no version
of the Decision-8/factory sketch names a collaborator for: ``id_gen`` (deployment
and, when needed, cluster ids), ``config_dir`` (deployment-profile disk loading,
``seedpod/app/services/profiles.py``), and ``deployment_audits`` (the
``DeploymentAuditRepository`` -- not in the Dispatcher-facing ``Repositories``
bundle per that module's own docstring: "the next component wires them alongside
the four app-services that need them" -- this is that component).

``dns`` is accepted (DR-0015 wires it fully at the composition root) but not yet
consumed by any method below -- binding a cluster's DNS hostname is a workflow-
step concern once the verb catalog lands (this round's brief forbids inventing
that), not something ``version_update`` itself does. Flagged here rather than
silently dropped from the constructor, matching ``seedpod/app/factory.py``'s own
module-docstring discipline for documented partial states.

**Salvage + scope narrowing (LOUD, per CLAUDE.md "don't silently regress"):**
``version_update``'s decision->response mapping is salvaged from
``reference-code/seedpod/seedpod/api/deployments.py``'s ``version_update`` (:237)
and ``orchestrator/cluster_manager.py``'s ``request_deployment``/
``_resolve_deployment_target`` (:1165). Deliberately NOT ported (v1 responsibilities
with no v2 owner yet, matching ``seedpod/services/manifests.py``'s own scope-
narrowing precedent):

- v1's ``_resolve_deployment_target``'s full "existing vs new cluster" state
  analysis (concurrent-deploy locking, redeploy-vs-new heuristics, ``TargetInfo.
  should_abort``) narrows here to one rule: reuse the one ACTIVE cluster already
  matching ``(repository, branch, environment)``, else birth a new one. Concurrent-
  deploy conflict handling is the workflow run-admitter's job (Conflict 2's
  ``ux_wr_one_active`` unique index), not this service's.
- v1's full ``ClusterSpecification`` construction (``core/cluster_spec.py``'s
  ``create_cluster_spec_from_template``) is not ported here -- that is
  ``cluster.load_spec``'s job (coherence-review.md Conflict 10), the engine
  domain step that reads this row's ``provider_config`` back out and builds the
  real ``ClusterSpecification``. What IS this service's job, per
  coherence-review.md's own assignment ("row synthesis ... is the API-layer
  service's job"), is birthing ``provider_config`` in the first place:
  ``_birth_cluster_row`` copies the profile's own ``cluster_spec`` block
  straight onto the row (``_provider_config_from``, below) -- verbatim,
  including whichever of the two shipped ``ingress_strategy`` shapes
  (sibling-of- or nested-in-``cluster_config``) the profile uses, since
  ``cluster.load_spec`` normalizes either shape on read. (Round 9: this
  service now ALSO calls the same committed ``allocate_cluster_cidrs()`` --
  see ``_build_resolved_config`` below -- but for a different consumer:
  ``cluster.load_spec``'s call feeds the real K3s ``--cluster-cidr``/
  ``--service-cidr`` install flags off ``provider_config``; this service's call
  feeds ``config.pod_cidr``/``config.service_cidr`` into manifest TEMPLATES.
  Both calls are pure functions of the same ``cluster_id`` and are therefore
  always bit-identical -- no round-trip through storage, no drift possible.)
- v1's slug naming-strategy engine (``core/naming_strategy.py``) is replaced with
  a minimal, deterministic slugifier (``_slugify`` below) -- stable names /
  presets' custom naming strategies are out of this round's scope.

**Round 9 (the resolved-config component) additions, LOUD per the same rule:**

- ``_build_resolved_config``/``_resolve_hostname`` (below) salvage v1's
  ``ManifestResolver._build_resolved_config``/``_resolve_hostname``
  (reference-code/seedpod/seedpod/orchestrator/manifest_resolver.py:694-836).
  They live HERE, not in ``seedpod/services/manifests.py``, because they need
  the RAW parsed profile mapping (``hostname``/``dns``/``ssl``/``cluster_spec``/
  ``rollout_timeout_seconds`` -- v1 ``ManifestConfig`` fields), and
  ``ManifestProfile`` deliberately does not carry any of those (that
  dataclass's own docstring: trimmed to "the fields this resolver's own logic
  reads"). This service already holds ``raw_profile`` (``load_deployment_
  profile``'s second return value) for exactly this reason
  (``_provider_config_from``'s own precedent, above) -- ``ManifestResolver.
  resolve()`` keeps taking a FINISHED ``config`` mapping and passing it
  through unchanged (``seedpod/services/manifests.py``'s own contract,
  restated in its module docstring's "CLOSED, Round 9" bullets).
- ``secrets: SecretRepository`` (constructor, below) is the SAME established
  idiom ``deployment_audits: DeploymentAuditRepository`` already uses --
  ``seedpod/app/services/secret_service.py``'s own module docstring notes
  ``DeploymentService`` already takes a standalone repository directly rather
  than through the Dispatcher-facing ``Repositories`` bundle. ``_deploy`` now
  loads real decrypted secrets (``_load_decrypted_secrets``) and threads them
  into ``resolve(secrets=...)`` -- previously silently omitted (``secrets=None``
  at both call sites), which is what let a tailscale-auth ``Secret`` ship with
  an empty key and deploy green (``seedpod/services/manifests.py``'s own
  "silent-empty decision" paragraph is the other half of this fix).
- ``deployment_preview`` gains a DIFFERENT secrets source, per **DR-0026**
  (docs/decisions/DR-0026-preview-render-context-and-error-mapping.md):
  METADATA-only key names from the SAME ``SecretRepository``, each mapped to a
  redaction sentinel, NEVER a decrypt call -- see that method's own docstring
  and ``_redacted_secrets_for_preview``, below. This is deliberately NOT the
  same secrets a real ``_deploy`` loads: preview returns its result to any
  caller holding ``deployments:read``, and decrypting there would hand real
  secret material to a permission level that is not ``secrets:read``.
  **Residual note (Round 9, org-and-ghcr component):** the SAME
  ``self._manifest_resolver.resolve()`` this method calls also auto-generates
  a REAL ``secrets["ghcr_dockerconfig_json"]`` internally (never through this
  method's own redacted mapping -- ``seedpod/services/manifests.py``'s
  ``ManifestResolver._add_ghcr_auth_if_needed``), whenever a resolved image
  references ``ghcr.io`` and a GHCR token is configured -- identically for
  ``_deploy`` and this method, because ``resolve()`` has no notion of preview
  vs deploy. This is currently safe ONLY because ``DeploymentPreviewResponse``
  (below) never surfaces ``resolved.resolved_secrets``/``resolved.
  rendered_manifests`` to a caller -- unlike ``_audit_row``'s deliberate,
  restricted-access plaintext persistence of the same field for a real
  ``_deploy`` (matching v1's own "Secrets in deployment_audits are in
  plaintext ... restricted access" posture). If ``DeploymentPreviewResponse``
  is ever extended to return either field, this stops being safe and needs a
  real fix at that point (e.g. an explicit "preview never gets real GHCR auth"
  parameter on ``resolve()``) -- flagged here rather than silently trusted to
  stay true, matching this docstring's own DR-0015 ``self._dns`` precedent for
  a known-but-not-yet-closed edge.
- **DR-0027** (docs/decisions/DR-0027-secret-scope-is-the-rule-derived-
  environment.md) rules on WHICH environment scopes the secrets DR-0026 (just
  above) governs the SHAPE of -- Round 9 wired real secrets into two call
  sites that previously had none to disagree about. ``_deploy`` already uses
  the RULE-DERIVED ``environment`` (the value stamped on the cluster/
  deployment rows, that ``key_class_for_environment`` uses for the audit, and
  that scopes SSE/REST visibility) -- unchanged by this ruling.
  ``deployment_preview`` gains an OPTIONAL ``environment`` parameter: exact
  when the caller supplies one, falling back to the profile's own
  ``environment_type`` otherwise (that method's own docstring has the full
  reasoning) -- optional, so no existing caller's behavior changes.
  **v1's behaviour here is a BUG, not an edge to preserve, and is
  DELIBERATELY NOT PORTED**: v1 scoped EVERY deployment's secrets by the
  PROFILE's ``environment_type``, never by the rule/request-derived
  environment a deployment actually gets recorded under
  (``reference-code/seedpod/seedpod/orchestrator/cluster_manager.py:
  1651-1655``: ``deployment_environment = manifest_config.get(
  'environment_type', 'ephemeral'); secret_manager = create_secret_manager(
  deployment_environment)``). This genuinely diverges on SHIPPED config, not
  just in theory: ``config/deployment-rules.yml`` carries
  ``action: staging_then_manual`` (a rule-derived ``staging`` environment)
  while all five shipped profiles declare ``environment_type: "ephemeral"``,
  so a v1-faithful port would render that STAGING deployment's manifests
  against EPHEMERAL secrets the first time that rule fires -- a deployment
  row that misdescribes its own contents. Recorded here loudly, per
  CLAUDE.md's "don't silently regress" / not-ported-bug discipline, rather
  than dropped without comment.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any

from seedpod.app.services.profiles import load_deployment_profile
from seedpod.core.clock import Clock
from seedpod.core.cluster_spec import allocate_cluster_cidrs
from seedpod.core.deploy_wave import DEFERRED_MANIFEST_RENDERING_KEY
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.core.events import (
    CancelRequested,
    CreateRequested,
    DeployRejected,
    DeployRequested,
    Event,
)
from seedpod.core.records import DeploymentState, Origin
from seedpod.data.repositories import (
    ClusterRow,
    DeploymentAuditRepository,
    DeploymentAuditRow,
    DeploymentRow,
    Repositories,
    SecretRepository,
)
from seedpod.data.uow import UnitOfWork
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.services.crypto import CryptoService
from seedpod.services.dns import DnsService
from seedpod.services.manifests import ManifestResolver, ResolvedManifest
from seedpod.services.rules import RuleEngine

__all__ = [
    "DeploymentService",
    "DeploymentResponse",
    "DeploymentPreviewResponse",
    "DeploymentNotFound",
    "rehydrate_cluster_hostname",
]

_DEFAULT_DEPLOYMENT_PROFILE = "ephemeral-stack"  # v1's default (api/deployments.py:185)

# DR-0026 (docs/decisions/DR-0026-preview-render-context-and-error-mapping.md):
# THE one, obvious redaction marker `deployment_preview` substitutes for every
# secret KEY that exists in an environment -- visibly not a real value, so a
# preview response can never be mistaken for the real rendered manifest.
_PREVIEW_SECRET_REDACTED = "<redacted-for-preview>"


class DeploymentNotFound(LookupError):
    pass


@dataclass(frozen=True)
class DeploymentResponse:
    """The ``POST /api/version-update`` wire DTO (ui-contract). ``status`` is a
    SYNTHESIZED response literal (v1's ``DeploymentResult.status``), distinct from
    ``deployments.status`` -- salvaged verbatim as a separate vocabulary because the
    parity gate (tests/acceptance/test_deployment_flow.py) asserts these exact
    strings: ``no_action``/``queued``/``manifest_resolution_failed``."""

    deployment_id: str
    cluster_id: str | None
    status: str
    message: str
    environment: str
    # DR-0046 decision 4: name the provider that was actually chosen. Whether a call
    # just created a BILLING droplet or a free local VM is the single most
    # consequential thing about it, and until now you learned it by querying the
    # cluster afterwards -- or from the invoice. `None` where no infrastructure was
    # decided (no_action, a failed resolution, a redeploy onto an existing cluster).
    provider: str | None = None


@dataclass(frozen=True)
class DeploymentPreviewResponse:
    status: str
    deployment_profile: str
    triggering_repo: str
    triggering_branch: str
    triggering_image: str
    resolution_strategy: str
    resolved_images: Mapping[str, str]
    registry_queries: tuple[Mapping[str, Any], ...]
    template_files: tuple[str, ...]


_SLUG_RUN = re.compile(r"[^a-z0-9]+")


def _slugify(*parts: str, suffix: str) -> str:
    base = "-".join(parts).lower()
    base = _SLUG_RUN.sub("-", base).strip("-")[:40].strip("-")
    return f"{base}-{suffix}" if base else suffix


def _cluster_slug_for(cluster_id: str, *, repo: str, branch: str) -> str:
    """The ONE slug formula. ``_birth_cluster_row`` (a cluster that already, or is
    about to, exist) and ``_deploy``/``deployment_preview`` (computing the slug a
    NOT-YET-BIRTHED cluster WILL get, before the birth uow() even opens -- Round 9:
    ``resolve()`` now needs ``cluster_slug`` and runs before that uow, DR-0008) both
    call this SAME function rather than each re-deriving ``_slugify(...)``
    independently, so the two can never drift apart."""
    return _slugify(repo, branch, suffix=cluster_id[:8])


class DeploymentService:
    def __init__(
        self,
        dispatcher: Dispatcher,
        repos: Repositories,
        uow: UnitOfWork,
        *,
        rules: RuleEngine,
        crypto: CryptoService,
        clock: Clock,
        manifest_resolver: ManifestResolver,
        dns: DnsService | None,
        id_gen: Callable[[], str],
        config_dir: Path,
        deployment_audits: DeploymentAuditRepository,
        secrets: SecretRepository,
        default_provider: str = "digitalocean",
        default_profile: str = _DEFAULT_DEPLOYMENT_PROFILE,
    ) -> None:
        self._dispatcher = dispatcher
        self._repos = repos
        self._uow = uow
        self._rules = rules
        self._crypto = crypto
        self._clock = clock
        self._manifest_resolver = manifest_resolver
        self._dns = dns  # see module docstring: DR-0015-wired, not yet consumed here
        self._id_gen = id_gen
        self._config_dir = config_dir
        self._deployment_audits = deployment_audits
        self._secrets = secrets  # module docstring: same standalone-repository idiom
        self._default_provider = default_provider
        self._default_profile = default_profile

    async def _load_decrypted_secrets(self, environment: str) -> dict[str, str]:
        """Every secret currently stored for ``environment``, decrypted. Mirrors v1's
        ``SecretManager.get_all_secrets_decrypted`` (reference-code/seedpod/seedpod/
        core/auth.py:142-166): list, then decrypt each -- ``SecretRepository.
        list_for_environment`` deliberately never decrypts (that repository's own
        docstring: "secrets:read = metadata only"), so this composes it with
        ``SecretRepository.get`` (the single-secret DECRYPTING read) instead of
        reaching for a bulk-decrypt method that would blur that permission-model
        line for every OTHER caller of ``list_for_environment``.

        DR-0008: secrets are a DB read, so this opens its OWN short ``uow()`` here --
        sequenced entirely BEFORE ``_deploy``'s later birth/dispatch ``uow()`` opens
        (never nested, never overlapping; see that method's own load-bearing
        comment). ``list_for_environment`` + N ``get`` calls all run inside the SAME
        transaction/lock, so there is no TOCTOU window a concurrent writer could
        exploit between the list and the per-key decrypt reads.

        ``key_class_for_environment`` is called for its VALIDATION side effect only
        (raises ``PermanentError`` on an environment outside the known DEV/PROD
        mapping, gotcha 8) -- the actual decrypt always uses each ROW's own STAMPED
        ``key_class`` (``SecretRepository.get``, never re-derived from
        ``environment`` -- ``crypto.py``'s own "deviation 2"). Without this guard, an
        unrecognised/typo'd ``environment`` would silently resolve to "zero secrets
        found" -- indistinguishable from a real, valid, empty environment -- rather
        than the loud failure a bad ``environment`` value deserves; that conflation
        of absence with invalidity is exactly what CLAUDE.md's crown-jewel-#1 posture
        exists to rule out."""
        self._crypto.key_class_for_environment(environment)
        async with self._uow() as tx:
            metadata = self._secrets.list_for_environment(tx, environment)
            decrypted: dict[str, str] = {}
            for m in metadata:
                row = self._secrets.get(tx, environment, m.key_name)
                # `SecretRepository.get` is typed `-> SecretRow | None` (its own
                # signature) because a caller can ask for a key that was never
                # written -- but `m.key_name` was just listed by
                # `list_for_environment`, in this SAME transaction (docstring,
                # above), so `None` here would mean the row vanished between the
                # list and this read despite the shared lock/transaction that
                # rules that out. Asserted explicitly, not silently trusted, so a
                # future refactor that ever splits this into two transactions
                # turns into a loud, named failure here rather than an
                # ``AttributeError`` swallowed by ``_deploy``'s ``except
                # Exception`` into a confusing "manifest resolution failed:
                # 'NoneType' object has no attribute 'value'".
                assert row is not None, (
                    f"secret {m.key_name!r} for {environment!r} was listed by "
                    "list_for_environment but is missing from get() in the SAME "
                    "transaction -- the shared-transaction invariant this method "
                    "relies on (docstring, above) has been broken"
                )
                decrypted[m.key_name] = row.value
            return decrypted

    async def _redacted_secrets_for_preview(self, environment: str) -> dict[str, str]:
        """DR-0026 part 1 (docs/decisions/DR-0026-preview-render-context-and-
        error-mapping.md): every secret KEY NAME currently stored for
        ``environment``, mapped to ``_PREVIEW_SECRET_REDACTED`` -- NEVER the real
        value. Deliberately NOT ``_load_decrypted_secrets`` (above): this calls
        ONLY ``SecretRepository.list_for_environment`` (metadata -- ciphertext
        untouched, that repository's own discipline) and NEVER
        ``SecretRepository.get`` (the single-secret DECRYPTING read) -- there is
        no decrypt call anywhere on this path, and therefore no plaintext secret
        material this method could leak even by accident. Unlike
        ``_load_decrypted_secrets`` this does NOT call
        ``key_class_for_environment`` first: an unrecognised ``environment`` here
        just means "no secrets known" (``list_for_environment`` returns ``[]``),
        which is the correct, harmless preview outcome -- there is no decrypt
        step downstream for a bad ``environment`` to silently corrupt.

        A key a template references that has no row in ``environment`` at all is
        deliberately NOT in the returned mapping -- ``StrictUndefined`` still
        raises for it downstream, which DR-0026 names as correct and useful for a
        preview to report, not a gap this method should paper over."""
        async with self._uow() as tx:
            metadata = self._secrets.list_for_environment(tx, environment)
        return {m.key_name: _PREVIEW_SECRET_REDACTED for m in metadata}

    # -------------------------------------------------------------------
    # The parity spine
    # -------------------------------------------------------------------

    async def version_update(
        self,
        *,
        repo: str,
        branch: str,
        image: str,
        commit: str,
        tag: str | None = None,
        actor: str,
    ) -> DeploymentResponse:
        decision = self._rules.evaluate(repo, branch, tag)
        if decision.action == "no_action":
            return DeploymentResponse(
                deployment_id=self._id_gen(),
                cluster_id=None,
                status="no_action",
                message=decision.reason,
                environment="none",
            )

        environment = decision.environment or "none"
        ttl_hours = decision.config.get("ttl_hours") if environment == "ephemeral" else None
        profile_name = decision.config.get("deployment_profile", self._default_profile)
        return await self._deploy(
            profile_name=profile_name, environment=environment, repo=repo, branch=branch, image=image,
            commit=commit, ttl_hours=ttl_hours, actor=actor, reason=decision.reason,
        )

    async def deploy_direct(
        self,
        *,
        profile_name: str,
        environment: str,
        repo: str,
        branch: str,
        image: str,
        commit: str,
        ttl_hours: float | None = None,
        provider_override: str | None = None,
        image_overrides: Mapping[str, str] | None = None,
        reason: str = "",
        data_initialization: Mapping[str, Any] | None = None,
        actor: str,
    ) -> DeploymentResponse:
        """The preset-deploy entrypoint (Round 6, api-features): the SAME
        birth/manifest-resolution/audit/``Dispatcher.apply()`` pipeline
        ``version_update`` runs, minus rule evaluation -- ``profile_name``/
        ``environment`` are given directly (a preset's own config) rather than
        derived from ``RuleEngine.evaluate()``. ``PresetService.deploy`` is the
        one caller; ``image_overrides`` carries the preset's (+ request's)
        per-service tag overrides straight through to
        ``ManifestResolver.resolve()`` (which already accepts them -- see that
        module's own ``image_overrides`` parameter), so a preset whose services
        are all overridden makes zero GHCR calls, same as v1's
        ``_build_image_overrides`` short-circuit
        (``reference-code/seedpod/seedpod/api/presets.py:427-457``).

        ``data_initialization`` (DR-0028 decision 2, Round 10): the ONLY v2
        entrypoint that can carry one -- it originates in the deploy REQUEST
        (``PresetService.deploy``'s own parameter, ``seedpod/api/routers/
        presets.py``'s ``DeployFromPresetRequest.data_initialization``), never
        in profile YAML or a rule decision, so ``version_update`` (rule-
        derived, no request to source one from) has no equivalent parameter
        and always passes ``None`` implicitly via ``_deploy``'s own default.
        Threaded straight to ``_build_resolved_config`` -- see that function's
        own docstring for why the key is omitted, not empty, when absent."""
        return await self._deploy(
            profile_name=profile_name, environment=environment, repo=repo, branch=branch, image=image,
            commit=commit, ttl_hours=ttl_hours, actor=actor, reason=reason,
            provider_override=provider_override, image_overrides=image_overrides,
            data_initialization=data_initialization,
        )

    async def _deploy(
        self,
        *,
        profile_name: str,
        environment: str,
        repo: str,
        branch: str,
        image: str,
        commit: str,
        ttl_hours: float | None,
        actor: str,
        reason: str,
        provider_override: str | None = None,
        image_overrides: Mapping[str, str] | None = None,
        data_initialization: Mapping[str, Any] | None = None,
    ) -> DeploymentResponse:
        """The birth/manifest-resolution/audit pipeline shared by
        ``version_update`` (rule-derived ``profile_name``/``environment``) and
        ``deploy_direct`` (preset-derived). Identical body to what
        ``version_update`` used to run inline -- extracted verbatim, not
        rewritten, so ``version_update``'s own already-tested behavior is
        unchanged (``provider_override``/``image_overrides`` both default to
        ``None``, ``version_update``'s own call site never passes them).

        **DR-0025 Erratum E2 (the restore-and-rehydrate component).** A
        ``provider_host`` (or ``custom``-needing-a-host) profile used to make
        this whole block degrade to a rejected ``manifest_resolution_failed``
        deployment, the SAME way a GHCR outage does (comment below). It no
        longer does: ``_hostname_deferred`` recognises that specific case and
        ``resolve(..., render=False)`` runs image/secret/config resolution in
        full while skipping the render that would otherwise raise -- so this
        method's existing ``resolution_error`` branching is UNCHANGED (a
        deferred resolve is still ``resolution_error is None``, taking the
        SAME success path below as any other deploy, audit row and all) and
        only what `resolved_config`/`resolved.rendered_manifests` actually
        CONTAIN differs for this one case."""
        now = self._clock.now()

        async with self._uow() as tx:
            existing = self._repos.clusters.find_active_cluster_by_branch(tx, repo, branch, environment)

        deployment_id = self._id_gen()

        # -- profile load + config build + secret load + manifest resolution: IO,
        # NO open uow (DR-0008). v1 (orchestrator/cluster_manager.py:1501-1507,
        # ":1594-1598" comment on `_ensure_target_cluster`) fetches the
        # deployment-profile config BEFORE creating a cluster: "deployment_profile_
        # config ... must be fetched before cluster creation". A missing/
        # unparseable profile (load_deployment_profile raising PermanentError) is
        # therefore NOT allowed to birth real infrastructure for a deployment that
        # can never run -- only once the profile is known loadable do we decide a
        # NEW cluster is needed. GHCR/image-resolution failures (resolve() raising
        # with a good profile) are a separate, faithfully-ported sub-case: v1 DID
        # already create the cluster by that point, so those still birth-then-
        # reject. Building `resolved_config` (`_build_resolved_config`, pure, no
        # IO) and loading real secrets (`_load_decrypted_secrets`, its own short
        # uow -- DR-0008 again: fully closed before this uow() block reopens
        # below, never nested inside it) are threaded into this SAME phase,
        # sequenced strictly BEFORE `resolve()`, and a failure in EITHER
        # (including `_build_resolved_config` raising on a typo'd hostname
        # strategy -- `_resolve_hostname`'s own docstring) degrades a deployment
        # the exact same birth-then-reject way a GHCR outage already does
        # (Round 9): all three must share ONE try block, or a raise from config-
        # building would escape uncaught and turn a bad profile into an unhandled
        # 500 instead of a recorded rejection. -- Weighed and kept even for
        # `_load_decrypted_secrets`'s own DB read: v1's analogous call
        # (`SecretManager.get_all_secrets_decrypted`, reference-code/seedpod/
        # seedpod/core/auth.py:142-166) re-raises rather than degrading, so a
        # transient DB failure there is a real divergence, not a v1 edge kept --
        # but `seedpod/data/`'s sync-SQLAlchemy-over-`StaticPool` layer
        # (`seedpod/data/uow.py`'s own docstring) never actually raises
        # `TransientError`/`InfrastructureUnreachableError` (those are
        # provider-IO taxonomy leaves, `seedpod/core/errors.py`'s own docstring
        # -- "Seam C's 17 members", nothing DB-layer defines a sibling), so
        # there is no live case today where this folds a genuinely-retryable
        # "cannot determine state" failure into "your config is bad" -- only a
        # SQLAlchemy-level exception a bad `environment` string or a real outage
        # could raise, which IS config-shaped or already birth-then-reject-shaped
        # like every other failure this block handles. Revisit if a future
        # backend (a real network DB) makes a genuinely transient failure mode
        # reachable here; until then, splitting this one call out into its own,
        # differently-handled try would be inventing a distinction the current
        # error surface can't actually produce.
        resolved: ResolvedManifest | None = None
        resolution_error: Exception | None = None
        raw_profile: dict[str, Any] = {}
        profile = None
        try:
            profile, raw_profile = load_deployment_profile(self._config_dir, profile_name)
        except Exception as exc:  # noqa: BLE001 -- see comment above; degrades to a
            # rejected-but-recorded deployment (matches v1's audit-trail-on-abort
            # behavior, api/deployments.py :1184-1213), never an unhandled 500.
            resolution_error = exc

        if resolution_error is None and existing is None:
            cluster_id = self._id_gen()
        elif existing is not None:
            cluster_id = existing.id
        else:
            # Profile load failed AND there is no existing cluster to attach a
            # (schema-required, NOT NULL) deployment.cluster_id to -- abort before
            # any write, matching v1's abort-before-cluster-creation for this case.
            return DeploymentResponse(
                deployment_id=deployment_id,
                cluster_id=None,
                status="manifest_resolution_failed",
                message=reason,
                environment=environment,
            )

        if resolution_error is None:
            assert profile is not None
            # cluster_slug: EXACTLY what _birth_cluster_row (below) will assign for
            # a NEW cluster (both call the SAME _cluster_slug_for -- never re-derive
            # _slugify independently, which could drift), or the row's OWN real slug
            # for a REUSED one. Never a fabricated value (Round 9 brief).
            cluster_slug = (
                existing.slug if existing is not None else _cluster_slug_for(cluster_id, repo=repo, branch=branch)
            )
            try:
                resolved_config = _build_resolved_config(
                    cluster_id, environment, raw_profile, config_overrides={},
                    cluster_slug=cluster_slug, profile_name=profile_name,
                    data_initialization=data_initialization,
                )
                resolved_secrets = await self._load_decrypted_secrets(environment)
            except Exception as exc:  # noqa: BLE001 -- see comment above: a config-
                # build or secret-loading failure is the same "profile loads,
                # resolution doesn't" shape as a GHCR outage -- birth-then-reject,
                # never an unhandled 500.
                resolution_error = exc
            else:
                # DR-0025 Erratum E2 point (i): DEFER, do not reject, when
                # `cluster_hostname` is omitted for a reason THIS deployment's own
                # provisioning will resolve (`_hostname_deferred`, above). Marked
                # on `resolved_config` itself -- the SAME mapping `resolve()`
                # copies verbatim into `ResolvedManifest.resolved_config` and
                # `_audit_row` persists unchanged -- so `deploy.load_audit`
                # (`seedpod/engine/steps/deploy.py`) reads the marker back "like
                # every other resolved fact" (DR-0028 decision 2's own words), the
                # SAME discipline this file already uses for `persistence_
                # services`/`deploy_wave`/`data_initialization`.
                deferred = _hostname_deferred(raw_profile, resolved_config)
                if deferred:
                    resolved_config[DEFERRED_MANIFEST_RENDERING_KEY] = True
                try:
                    resolved = await self._manifest_resolver.resolve(
                        profile,
                        triggering_repo=repo,
                        triggering_branch=branch,
                        triggering_image=image,
                        commit_sha=commit,
                        image_overrides=image_overrides,
                        config=resolved_config,
                        secrets=resolved_secrets,
                        render=not deferred,
                    )
                except Exception as exc:  # noqa: BLE001 -- see comment above.
                    resolution_error = exc

        provider = provider_override or raw_profile.get("provider", self._default_provider)

        async with self._uow() as tx:
            if existing is None:
                cluster_row = _birth_cluster_row(
                    cluster_id, repo=repo, branch=branch, environment=environment,
                    provider=provider, node_count=_node_count(raw_profile), ttl_hours=ttl_hours, now=now,
                    provider_config=_provider_config_from(raw_profile),
                )
                await self._dispatcher.apply(
                    "cluster", cluster_id, CreateRequested(at=now, actor=actor), tx=tx, record=cluster_row
                )

            if resolution_error is not None:
                deployment_row = _birth_deployment_row(
                    deployment_id, cluster_id=cluster_id, environment=environment,
                    manifest_version=profile_name, now=now, deployed_by=actor,
                )
                await self._dispatcher.apply(
                    "deployment",
                    deployment_id,
                    DeployRejected(at=now, actor=actor, reason=f"manifest resolution failed: {resolution_error}"),
                    tx=tx,
                    record=deployment_row,
                )
                status = "manifest_resolution_failed"
            else:
                assert resolved is not None
                audit_id = self._id_gen()
                self._deployment_audits.insert(tx, _audit_row(
                    audit_id, cluster_id=cluster_id, environment=environment, repo=repo, branch=branch,
                    image=image, commit=commit, profile_name=profile_name, raw_profile=raw_profile,
                    resolved=resolved, key_class=self._crypto.key_class_for_environment(environment), now=now,
                ))
                resolved_images = {name: img.image_url for name, img in resolved.resolved_images.items()}
                deployment_row = _birth_deployment_row(
                    deployment_id, cluster_id=cluster_id, environment=environment,
                    manifest_version=profile_name, now=now, resolved_images=resolved_images,
                    deployed_by=actor,
                )
                await self._dispatcher.apply(
                    "deployment",
                    deployment_id,
                    DeployRequested(at=now, actor=actor, spec_ref=audit_id),
                    tx=tx,
                    record=deployment_row,
                )
                status = "queued"

        return DeploymentResponse(
            deployment_id=deployment_id,
            cluster_id=cluster_id,
            status=status,
            message=reason,
            environment=environment,
            provider=provider,  # DR-0046 decision 4
        )

    async def deployment_preview(
        self,
        *,
        deployment_profile_name: str,
        triggering_repo: str,
        triggering_branch: str,
        triggering_image: str,
        commit_sha: str | None = None,
        environment: str | None = None,
    ) -> DeploymentPreviewResponse:
        """Mirrors ``version_update``'s manifest-resolution half without
        PERSISTING anything (unchanged contract: no ``Dispatcher.apply()``, no
        row ever written by this method). Round 9: now builds the SAME
        ``config=`` ``_build_resolved_config`` produces for a real ``_deploy``,
        so a preview exercises the identical template-rendering path (the same
        'environment_variables is undefined' crash the 2026-08-03 smoke hit
        reproduces identically from either call site). ``cluster_id``/
        ``cluster_slug`` are SYNTHETIC (``self._id_gen()`` is pure/in-memory, no
        persistence -- never a DB id) purely so ``allocate_cluster_cidrs`` and
        template labels have a deterministic value to hash/format; neither is
        ever returned to the caller (``DeploymentPreviewResponse`` carries no
        ``cluster_id`` field).

        **DR-0026** (docs/decisions/DR-0026-preview-render-context-and-error-
        mapping.md) governs ``secrets=``, and OVERRULES an earlier version of
        this docstring that left ``secrets={}`` always: under ``seedpod/
        services/manifests.py``'s ``StrictUndefined`` policy, an always-empty
        ``secrets`` mapping makes EVERY secret-bearing profile preview as a
        failure (98 ``secrets.*`` references across ``config/manifest-
        templates/`` -- most shipped profiles) -- a real regression on a live
        API surface invisible to the acceptance parity gate, whose own preview
        assertion runs against a fixture profile carrying no secrets. This
        method now calls ``_redacted_secrets_for_preview`` (above) instead:
        METADATA only (key NAMES known for this environment -- NEVER a decrypt
        call, NEVER ciphertext) with one obvious redaction sentinel substituted
        as every value. A key that IS present in the environment therefore
        renders (against the sentinel, never the real value); a key referenced
        by a template but genuinely ABSENT from the environment still raises --
        correct, and useful for a preview to say. No plaintext secret material
        ever leaves this process through preview.

        This costs preview a SHORT READ-ONLY ``uow()`` (one metadata SELECT, no
        write, no provider IO) -- permitted under DR-0008, whose actual law is
        "a transaction encloses only database statements", not "preview may
        never open one". This docstring's "no persisting" contract, above,
        covers what DR-0008 actually cares about; it was never a blanket ban on
        reading.

        **DR-0027** (docs/decisions/DR-0027-secret-scope-is-the-rule-derived-
        environment.md) governs WHICH environment the ``secrets=`` lookup just
        described scopes against -- a question DR-0026 itself doesn't answer.
        ``environment``, when the caller supplies it, is used EXACTLY: the
        same rule-derived value a real ``_deploy`` triggered the same way
        would use. Omitted (the default -- every caller before this
        parameter existed keeps working unchanged), preview falls back to the
        profile's OWN ``environment_type`` instead -- an explicitly
        APPROXIMATE stand-in, since preview evaluates no rules (its signature
        takes a profile name plus triggering repo/branch/image, never a rule
        decision) and therefore has no rule-derived environment of its own to
        fall back on. This keeps DR-0026's "preview predicts deployment"
        premise honest at the environment layer too: exact when the caller
        knows what a real deployment would be recorded under, explicitly
        approximate when nobody does -- never silently one or the other."""
        profile, raw_profile = load_deployment_profile(self._config_dir, deployment_profile_name)
        preview_cluster_id = self._id_gen()
        # DR-0027: exact when supplied, else the profile's own environment_type
        # (an explicit approximation -- this method's own docstring above).
        preview_environment = environment if environment is not None else raw_profile.get("environment_type", "ephemeral")
        preview_cluster_slug = _cluster_slug_for(preview_cluster_id, repo=triggering_repo, branch=triggering_branch)
        resolved_config = _build_resolved_config(
            preview_cluster_id, preview_environment, raw_profile, config_overrides={},
            cluster_slug=preview_cluster_slug, profile_name=deployment_profile_name,
        )
        redacted_secrets = await self._redacted_secrets_for_preview(preview_environment)
        # DR-0025 Erratum E2, same test `_deploy` applies (`_hostname_deferred`, above):
        # a `provider_host`/`custom` profile legitimately OMITS `cluster_hostname` until
        # its own cluster is provisioned. Preview has no cluster and never will, so
        # without this it rendered `{{ cluster_hostname }}` against StrictUndefined and
        # failed -- meaning EVERY provider_host profile (both shipped `-nodns` ones, i.e.
        # exactly what a tart developer runs) could never be pre-flighted at all, while a
        # real deploy of the same profile succeeded. That also falsified this method's own
        # "exercises the identical template-rendering path" contract.
        #
        # A SYNTHETIC hostname, NOT `_deploy`'s `render=False` deferral. Deferring is right
        # for `_deploy` (the manifests get re-rendered at deploy time once the real host is
        # known) but wrong here: `resolve(render=False)` returns `template_files=()`, so
        # preview would answer "success" having rendered NOTHING -- silently losing the
        # undefined-variable and missing-secret checks that are the entire point of a
        # pre-flight, and regressing `exampleco-web-2-kind`, a shipped provider_host profile
        # that previews fine today because no template of its own reads the hostname.
        # This mirrors the SYNTHETIC `cluster_id`/`cluster_slug` a few lines above (see
        # this method's docstring): a deterministic stand-in so templates have something
        # to format, never persisted and never returned. `.invalid` is RFC 2606 reserved,
        # so a value that escaped into a rendered manifest could not resolve to a real
        # host. A genuinely-wrong hostname strategy still raises, unchanged -- E2 point
        # (iii) keeps `dns`-with-missing-config and pattern-less `custom` loud, and
        # `_hostname_deferred` is the function that draws that line.
        if _hostname_deferred(raw_profile, resolved_config):
            resolved_config["cluster_hostname"] = "preview-host.invalid"
        resolved = await self._manifest_resolver.resolve(
            profile,
            triggering_repo=triggering_repo,
            triggering_branch=triggering_branch,
            triggering_image=triggering_image,
            commit_sha=commit_sha,
            config=resolved_config,
            secrets=redacted_secrets,
        )
        return DeploymentPreviewResponse(
            status="success",
            deployment_profile=deployment_profile_name,
            triggering_repo=triggering_repo,
            triggering_branch=triggering_branch,
            triggering_image=triggering_image,
            resolution_strategy=raw_profile.get("resolution_strategy", "branch_discovery_with_fallback"),
            resolved_images={name: img.image_url for name, img in resolved.resolved_images.items()},
            registry_queries=tuple(dataclasses.asdict(q) for q in resolved.registry_queries),
            template_files=resolved.template_files,
        )

    # -------------------------------------------------------------------
    # CRUD
    # -------------------------------------------------------------------

    async def get(self, deployment_id: str) -> DeploymentRow:
        async with self._uow() as tx:
            row = self._repos.deployments.get(tx, deployment_id)
        if row is None:
            raise DeploymentNotFound(deployment_id)
        return row

    async def list(self, *, cluster_id: str | None = None, show_history: bool = False) -> list[DeploymentRow]:
        async with self._uow() as tx:
            if cluster_id is not None:
                rows = self._repos.deployments.list_for_cluster(tx, cluster_id)
                if not show_history:
                    hidden = {DeploymentState.SUPERSEDED.value, DeploymentState.DESTROYED.value}
                    rows = [r for r in rows if r.status not in hidden]
                return rows
            return self._repos.deployments.list_all(tx, show_history=show_history)

    async def cancel(self, deployment_id: str, *, actor: str, reason: str = "") -> DeploymentRow:
        """ui-contract: "Cancel no longer touches cluster state" -- exactly what
        the pure machine's ``DEPLOYING x CancelRequested`` row already does (no
        Cascade emitted); this method is a thin ``Dispatcher.apply()`` call."""
        event: Event = CancelRequested(at=self._clock.now(), actor=actor, reason=reason)
        await self._dispatcher.apply("deployment", deployment_id, event)
        return await self.get(deployment_id)

    async def redeploy(self, deployment_id: str, *, actor: str) -> DeploymentResponse:
        """Re-apply the SAME audited manifest (no new GHCR/registry IO) as a new
        deployment birth on the SAME cluster. Salvaged intent from
        ``reference-code/seedpod/seedpod/api/deployments.py``'s ``redeploy_deployment``
        (:911), narrowed to "same audit, new deployment row" -- v1's rollback-
        target bookkeeping is a workflow-run concern once the verb catalog lands."""
        original = await self.get(deployment_id)
        if original.spec_ref is None:
            raise DeploymentNotFound(f"deployment {deployment_id} has no resolved manifest to redeploy")
        now = self._clock.now()
        new_id = self._id_gen()
        async with self._uow() as tx:
            audit = self._deployment_audits.get(tx, original.spec_ref)
            new_audit_id = self._id_gen()
            self._deployment_audits.insert(tx, dataclasses.replace(
                audit, id=new_audit_id, deployment_id=None, created_at=now,
            ))
            row = _birth_deployment_row(
                new_id, cluster_id=original.cluster_id, environment=original.environment,
                manifest_version=original.manifest_version, now=now, resolved_images=original.resolved_images,
                deployed_by=actor,
            )
            await self._dispatcher.apply(
                "deployment", new_id, DeployRequested(at=now, actor=actor, spec_ref=new_audit_id), tx=tx, record=row
            )
        return DeploymentResponse(
            deployment_id=new_id, cluster_id=original.cluster_id, status="queued",
            message=f"redeployed from {deployment_id}", environment=original.environment,
        )

    async def retrigger(self, deployment_id: str, *, actor: str) -> DeploymentResponse:
        """Re-run the FULL version-update decision (fresh manifest resolution)
        for the original deployment's triggering repo/branch/image, per ui-
        contract's ``result.new_deployment_id``."""
        original = await self.get(deployment_id)
        if original.spec_ref is None:
            raise DeploymentNotFound(f"deployment {deployment_id} has no audit trail to retrigger from")
        async with self._uow() as tx:
            audit = self._deployment_audits.get(tx, original.spec_ref)
        return await self.version_update(
            repo=audit.triggering_repo,
            branch=audit.triggering_branch,
            image=audit.triggering_image,
            commit=audit.commit_sha or "",
            actor=actor,
        )


# ---------------------------------------------------------------------------
# Resolved-config construction (Round 9) -- salvaged from v1's
# ManifestResolver._resolve_hostname / _build_resolved_config (reference-code/
# seedpod/seedpod/orchestrator/manifest_resolver.py:694-836). See the class
# docstring's "Round 9 additions" paragraph for why these live HERE rather than in
# seedpod/services/manifests.py: they need the RAW parsed profile mapping
# (hostname/dns/ssl/cluster_spec/rollout_timeout_seconds), which ManifestProfile
# deliberately does not carry.
# ---------------------------------------------------------------------------


def _hostname_strategy(
    raw_profile: Mapping[str, Any], config_overrides: Mapping[str, Any]
) -> tuple[str, str | None]:
    """The strategy name (+ ``custom_pattern``, when one is declared) a profile's
    ``hostname:``/``dns:`` blocks resolve to. Verbatim from v1's own inference
    (reference-code .../manifest_resolver.py:694-719): an explicit ``hostname:``
    section wins outright; its absence falls back to inferring ``"dns"`` from an
    ENABLED ``dns:`` block (v1's backward-compat path, for profiles predating the
    ``hostname:`` section), else ``"none"`` -- ``config/deployment-profiles/
    exampleco-web-2.yml`` declares neither, so it infers ``"none"``.

    Split out of ``_resolve_hostname`` (below), which used to inline this, so
    ``_build_resolved_config`` can ask "does this profile even WANT a hostname?"
    on its own. **DR-0025 Erratum E1**
    (docs/decisions/DR-0025-hostname-resolution-ordering.md) needs exactly that
    question answered BEFORE it asks "what value, if any, did the strategy
    produce?": ``_resolve_hostname`` returns bare ``None`` for BOTH "the strategy
    is deliberately none" AND "the strategy wanted a host and couldn't produce
    one" -- only the STRATEGY NAME tells those two facts apart, and
    ``_build_resolved_config`` (the one caller that turns a hostname into a
    config key) is where that distinction actually has to be made. A second,
    independent re-inference there -- rather than sharing this one -- would be
    exactly the drift risk CLAUDE.md's salvage discipline exists to rule out."""
    hostname_raw = raw_profile.get("hostname")
    if hostname_raw is None:
        dns_config = raw_profile.get("dns") or config_overrides.get("dns_config")
        if dns_config and dns_config.get("enabled", False):
            return "dns", None
        return "none", None
    return hostname_raw.get("strategy", "dns"), hostname_raw.get("custom_pattern")


def _resolve_hostname(
    raw_profile: Mapping[str, Any],
    config_overrides: Mapping[str, Any],
    cluster_slug: str | None,
    provider_host: str | None,
) -> str | None:
    """Verbatim per-strategy resolution from v1's ``_resolve_hostname`` (reference-
    code .../manifest_resolver.py:694-767): ``none``/``dns``/``provider_host``/
    ``custom``, dispatched on ``_hostname_strategy`` (above -- which carries v1's
    backward-compat ``dns:``-block inference) -- ``config/deployment-profiles/
    exampleco-web-2.yml`` declares neither ``hostname:`` nor ``dns:``, so it infers
    "none" and correctly yields ``None`` -- pinned by ``tests/services/
    test_manifests.py``'s end-to-end exampleco-web-2 render test.

    Genuine correctness fix, not a v1 bug pin: v1's final ``else: logger.error(...);
    return None`` branch for an unrecognised strategy string is UNREACHABLE in v1 --
    ``HostnameConfig.strategy``'s own pydantic ``field_validator`` already rejects
    anything outside ``{none, dns, provider_host, custom}`` at profile-LOAD time,
    before ``_resolve_hostname`` ever runs. ``raw_profile`` here is an unvalidated
    dict, so that same typo CAN reach this function in v2 -- raising
    ``PermanentError`` (the one taxonomy home) beats silently dropping the hostname
    (and every ``{% if cluster_hostname %}`` template conditional riding on it) for
    a profile with a typo'd ``hostname.strategy:``.

    **This function's own return value stays a plain ``str | None``** -- that
    ``None`` still conflates "strategy is none" with "strategy wanted a host and
    couldn't produce one"; telling those apart is ``_build_resolved_config``'s job
    (below), per **DR-0025 Erratum E1**
    (docs/decisions/DR-0025-hostname-resolution-ordering.md). What every branch
    here DOES guarantee, unconditionally: an unresolvable hostname is ``None``,
    NEVER ``""`` or any other placeholder string -- ``provider_host`` is the
    load-bearing case (a NEW cluster has no droplet/VM/IP yet at ``resolve()``
    time, so this legitimately returns ``None`` for every real deployment until
    Round 10 re-resolves against the provisioned host), but the rule applies to
    every branch equally."""
    strategy, custom_pattern = _hostname_strategy(raw_profile, config_overrides)

    if strategy == "none":
        return None

    if strategy == "dns":
        dns_config = raw_profile.get("dns") or config_overrides.get("dns_config")
        if not dns_config or not cluster_slug:
            return None
        # `zone`'s own `.get(..., "")` is verbatim v1 (module docstring), NOT a
        # DR-0025 instance: v1's `dns_config` was a validated pydantic model with
        # `zone` a required field, so this default was unreachable there; here
        # `dns_config` is an unvalidated raw dict, so a profile that sets `dns.
        # enabled: true` but omits `zone` is a MALFORMED PROFILE (bad config
        # authoring), not an unresolved-infrastructure value -- a different class
        # of problem to DR-0025's. No shipped profile omits `zone` when `dns.
        # enabled` is true (grep-verified across `config/deployment-profiles/`),
        # so this is presently dead code; left as v1 wrote it rather than
        # inventing new validation behaviour for a case nothing exercises.
        zone = dns_config.get("zone", "")
        subdomain_pattern = dns_config.get("subdomain_pattern", "{cluster_slug}")
        return f"{subdomain_pattern.format(cluster_slug=cluster_slug)}.{zone}"

    if strategy == "provider_host":
        host = provider_host or config_overrides.get("provider_host")
        return host or None

    if strategy == "custom":
        if not custom_pattern:
            return None
        # DR-0025 (docs/decisions/DR-0025-hostname-resolution-ordering.md),
        # applied to the one branch that used to embed an empty segment instead
        # of following it: the SAME `host = provider_host or config_overrides.
        # get("provider_host")` expression the `provider_host` branch above
        # already uses, with NO `""` default. If the pattern doesn't even
        # reference `{provider_host}`, a missing host is irrelevant to it and
        # must not block an otherwise-resolvable custom hostname (e.g. a pattern
        # built purely from `{cluster_slug}`) -- so the None-guard is scoped to
        # patterns that actually need one. `cluster_slug`'s own `or ""` is left
        # as is: unlike `provider_host`, `cluster_slug` is never "derived from
        # provisioned infrastructure" (DR-0025's own generalization) -- it is
        # computed locally, before `resolve()` even runs, and is never None on
        # either real `DeploymentService` call site (`_cluster_slug_for`'s own
        # docstring); only a direct unit test can supply `None` for it.
        host = provider_host or config_overrides.get("provider_host")
        if "{provider_host}" in custom_pattern and not host:
            return None
        return custom_pattern.format(cluster_slug=cluster_slug or "", provider_host=host or "")

    raise PermanentError(
        f"deployment-service._resolve_hostname: unknown hostname strategy {strategy!r}",
        code=ErrorCode.INVALID_INPUT,
        provider="deployment-service",
        command="_resolve_hostname",
        detail={"strategy": strategy},
    )


def _strategy_needs_a_provisioned_host(raw_profile: Mapping[str, Any], config_overrides: Mapping[str, Any]) -> bool:
    """DR-0025 Erratum E2's own distinction, made concrete: does THIS strategy's
    resolution path depend on a ``provider_host`` value that only exists once
    the cluster has provisioned? True for ``provider_host`` itself (its only
    input) and for ``custom`` when the declared pattern actually references
    ``{provider_host}`` (``_resolve_hostname``'s own two branches, above). False
    for ``dns`` (deterministic from ``cluster_slug`` alone, no provisioned
    infrastructure needed) and for a ``custom`` strategy with NO pattern at all
    (a genuine config error no amount of provisioning fixes) -- see
    ``_hostname_deferred``'s own docstring for why this split is what separates
    Erratum E2's DEFERRED case from its unchanged, still-raises-now case
    (E2 point (iii))."""
    strategy, custom_pattern = _hostname_strategy(raw_profile, config_overrides)
    if strategy == "provider_host":
        return True
    return bool(strategy == "custom" and custom_pattern and "{provider_host}" in custom_pattern)


def _hostname_deferred(raw_profile: Mapping[str, Any], resolved_config: Mapping[str, Any]) -> bool:
    """DR-0025 Erratum E2's DECISION-TIME test: does THIS deployment's
    ``resolved_config`` need to be marked DEFERRED (rather than causing
    ``resolve()`` to raise and the deployment to be REJECTED)?

    True iff ``resolved_config`` OMITTED ``cluster_hostname`` (Erratum E1's
    OMITTED branch -- ``_build_resolved_config``'s own docstring) for a reason
    that provisioning THIS deployment's own cluster will resolve. ``"cluster_
    hostname" in resolved_config`` covers BOTH the resolved-value case and the
    deliberate-``None`` case (strategy ``"none"``) -- neither needs deferral,
    there is nothing to re-resolve. Only when the key is truly absent does
    ``_strategy_needs_a_provisioned_host`` decide DEFER (``provider_host``/
    ``custom``-needing-a-host) vs RAISE-NOW-UNCHANGED (``dns`` with missing
    config, or ``custom`` with no pattern at all -- both genuine config errors
    E2 point (iii) explicitly keeps raising loudly: "a profile whose hostname
    strategy is simply wrong must still be rejected")."""
    if "cluster_hostname" in resolved_config:
        return False
    return _strategy_needs_a_provisioned_host(raw_profile, {})


def rehydrate_cluster_hostname(
    raw_profile: Mapping[str, Any], *, cluster_slug: str, provider_host: str
) -> tuple[bool, str | None]:
    """DR-0025 Erratum E2 point (ii)'s DEPLOY-TIME half, the public entrypoint
    ``seedpod/engine/steps/deploy.py``'s ``DeployLoadAudit`` (the restore-and-
    rehydrate component) calls once a deferred deployment's cluster is ``ACTIVE``
    and its real address is known. Re-runs the exact SAME hostname-resolution
    algorithm ``_build_resolved_config`` ran at decision time
    (``_hostname_strategy``/``_resolve_hostname``, both already committed and
    already cited against v1 line-for-line) -- never a second, independently
    written copy, so decision-time and deploy-time resolution can never drift
    apart. ``config_overrides={}`` always: DR-0025 part 2 is specifically about
    supplying a freshly-known ``provider_host`` at deploy time, not about
    reintroducing the (never-yet-used) config-override plumbing
    ``_build_resolved_config``'s own docstring already discusses.

    **Returns a ``(present, cluster_hostname)`` pair, not a bare ``str | None``
    -- Erratum E1's own three-state split kept apart, not re-conflated.** A
    bare ``str | None`` return cannot distinguish "this profile deliberately
    has no hostname" (``cluster_hostname = None``, key PRESENT, feature gates
    evaluate false cleanly) from "a strategy wanted a host and could not
    produce one" (key OMITTED entirely, ``StrictUndefined`` raises loudly) --
    exactly the distinction a fix-pass review found this function's caller
    (``_rehydrate``, ``seedpod/engine/steps/deploy.py``) collapsing back
    together via a bare ``if cluster_hostname is not None: set-key else:
    omit-key``, mis-citing the omitted branch as E1's split when E1's omitted
    branch is only the UNKNOWABLE case:

    - ``strategy: "none"`` -> ``(True, None)``: present, deliberately empty.
      Unreachable via this function's one real caller TODAY (DR-0025 Erratum
      E2 only ever defers ``provider_host``/``custom`` strategies needing a
      host, never ``"none"`` -- see ``_strategy_needs_a_provisioned_host``),
      but the return SHAPE no longer depends on that being true forever: a
      future strategy, or a hand-edited audit row, reaching this function
      with strategy ``"none"`` now renders cleanly instead of raising.
    - ``_resolve_hostname`` produces a real value -> ``(True, <value>)``.
    - ``_resolve_hostname`` still returns ``None`` (a genuinely unexpected
      case -- e.g. a ``custom`` pattern's OTHER required piece newly missing)
      -> ``(False, None)``: absent, the caller must treat this as a real
      failure, not a silent re-omission, exactly as decision time itself
      treats a still-unresolvable hostname."""
    strategy, _ = _hostname_strategy(raw_profile, {})
    if strategy == "none":
        return True, None
    resolved = _resolve_hostname(raw_profile, {}, cluster_slug, provider_host)
    return resolved is not None, resolved


def _validated_deploy_wave(service_name: str, raw_value: Any) -> int:
    """DR-0029's ``services.<name>.deploy_wave`` field, guarded the same way
    every other computed key in ``_build_resolved_config`` guards its input
    (this function's own caller already checks ``isinstance(service_raw,
    dict)`` on the same line -- this is the analogous check one level
    deeper, for the VALUE at that key). An earlier revision called
    ``int(service_raw.get("deploy_wave", 3))`` unguarded: a profile YAML with
    ``deploy_wave: null``, ``deploy_wave: "one"``, or a list raised a bare
    ``TypeError``/``ValueError`` straight out of ``_build_resolved_config``,
    escaping the one error-taxonomy home (CLAUDE.md) -- caught only by
    ``_deploy``'s blanket ``except Exception`` (see this module's own
    docstring bullet on ``resolution_error``) and reported as a generic
    config-resolution failure, so the operator never learns WHICH service's
    ``deploy_wave`` was malformed. Matches ``core/environment_config.py``'s
    ``create_environment_variables_from_dict`` idiom: raise
    ``PermanentError(ErrorCode.INVALID_INPUT)`` naming the service and the
    bad value, rather than let the exception surface unnamed.

    A negative rank is rejected too (bare ``int(...)`` would accept it and
    silently sort it ahead of wave 0 -- worse than merely malformed input,
    since it LOOKS like a valid, if unusual, wave index instead of failing
    loudly). ``bool`` is rejected explicitly even though
    ``isinstance(True, int)`` is true in Python: a YAML ``deploy_wave: true``
    is almost certainly a copy-paste of the WRONG field (v1 has no boolean
    deploy_wave concept anywhere; ``DeploymentProfile.data_initialization``
    -- DR-0028 decision 2 -- is the field that WAS a bool in an earlier,
    corrected stand-in, and that mix-up is exactly the shape a lenient
    ``int(True) == 1`` would hide silently)."""
    if isinstance(raw_value, bool) or not isinstance(raw_value, int):
        raise PermanentError(
            f"deployment-service._build_resolved_config: service {service_name!r}'s "
            f"deploy_wave must be an integer, got {raw_value!r} ({type(raw_value).__name__})",
            code=ErrorCode.INVALID_INPUT,
            provider="deployment-service",
            command="_build_resolved_config",
            detail={"service": service_name, "value": repr(raw_value)},
        )
    if raw_value < 0:
        raise PermanentError(
            f"deployment-service._build_resolved_config: service {service_name!r}'s "
            f"deploy_wave must be >= 0, got {raw_value}",
            code=ErrorCode.INVALID_INPUT,
            provider="deployment-service",
            command="_build_resolved_config",
            detail={"service": service_name, "value": repr(raw_value)},
        )
    return raw_value


def _build_resolved_config(
    cluster_id: str,
    environment: str,
    raw_profile: Mapping[str, Any],
    config_overrides: Mapping[str, Any],
    cluster_slug: str | None,
    profile_name: str,
    data_initialization: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Salvaged from v1's ``_build_resolved_config`` (reference-code .../
    manifest_resolver.py:769-836). Five keys the shipped templates actually read
    via ``config.*`` (``environment``/``cluster_id``/``cluster_slug``/``pod_cidr``/
    ``service_cidr``), plus what v1 additionally produced, ported ONLY where a v2
    consumer exists or is imminent (each noted below):

    - ``pod_cidr``/``service_cidr``: the ALREADY-COMMITTED, pure
      ``core.cluster_spec.allocate_cluster_cidrs(cluster_id)`` -- called directly,
      never re-derived, never read back from storage (v1 round-tripped these
      through ``cluster.provider_config``; v2 doesn't need to, since the same pure
      function of the same ``cluster_id`` is always bit-identical -- class
      docstring's "Round 9 additions" paragraph).
    - ``cluster_hostname``: **DR-0025 Erratum E1**
      (docs/decisions/DR-0025-hostname-resolution-ordering.md) -- NOT v1's own
      ``if cluster_hostname: config['cluster_hostname'] = cluster_hostname`` (that
      collapses two different facts into one "omitted" outcome, which is the
      defect the erratum exists to fix). Two distinct outcomes instead, decided by
      ``_hostname_strategy`` (above), not by ``_resolve_hostname``'s return value
      alone:

      * The profile DELIBERATELY has no hostname (strategy ``"none"``, or no
        strategy resolvable to one at all -- v1's own backward-compat inference)
        -> the key is PRESENT, valued ``None``. A real, shipped
        ``{% if cluster_hostname %}`` feature gate (``config/manifest-templates/
        exampleco-stack/*.yaml``) must evaluate FALSE cleanly, not raise -- ``None``
        is falsy in Jinja same as Python; an OMITTED name would instead trip
        ``StrictUndefined`` and crash every ``hostname.strategy: none`` profile
        at its first ``{% if %}``.
      * A strategy WANTED a host and could not produce one (``provider_host``
        before provisioning is the load-bearing case) -> the key is OMITTED
        ENTIRELY. There is no "off" state for a value real ``environment_
        variables:`` interpolate unconditionally into URLs
        (``"https://{{ cluster_hostname }}/auth"``) -- ``StrictUndefined`` must
        raise, naming ``cluster_hostname``, rather than render a placeholder.

      Never ``""`` on either path. ``seedpod/services/manifests.py``'s
      ``_render_templates`` (the consumer of this ``config`` mapping) mirrors this
      exact presence/value split when it builds its OWN Jinja contexts -- see that
      module's own docstring for the render-side half.
    - ``data_initialization``: **DR-0028 decision 2, closing Erratum E2's own
      "inert" gap** (docs/decisions/DR-0028-deploy-path-dtos.md) -- NOT a
      ``raw_profile`` key at all (no shipped profile ever declares it, and it
      is "a per-deployment choice, not a property of a profile", decision 2's
      own words); sourced from the DEPLOY REQUEST instead
      (``PresetService.deploy``'s own ``data_initialization`` parameter,
      threaded here via ``deploy_direct``/``_deploy``, mirroring
      ``seedpod/api/routers/presets.py``'s already-committed
      ``DataInitialization`` request shape one-for-one). Written into this
      mapping ONLY when truthy (matching ``persistence_services``'s own
      "if x: config[...] = x" pattern immediately below) so an ordinary
      deploy with no restore requested leaves the key OMITTED, never a
      spurious empty dict -- ``deploy.load_audit``
      (``seedpod/engine/steps/deploy.py``) reads ``resolved_config.get(
      "data_initialization")`` and treats an absent/falsy key as "nothing to
      restore" (``SnapshotRestoreSpec | None``), the identical
      present-vs-absent discipline ``cluster_hostname`` above already uses.
      Before this wiring, nothing wrote this key at all: an operator-requested
      restore was silently dropped between the API accepting it and the
      engine ever seeing it -- "strictly worse than failing, because the
      operator believes their data was restored" (DR-0028 Erratum E2's own
      words). ``version_update``'s automatic (non-preset) deploys and
      ``deployment_preview`` never pass one -- neither has a deploy REQUEST
      carrying ``data_initialization`` to source it from -- so this parameter
      defaults to ``None`` and the key stays omitted for both.
    - ``deploy_wave``: **DR-0029** (docs/decisions/DR-0029-wave-orchestration-
      is-built.md) -- NOT a v1 key at all (``deploy_wave`` never appears
      outside ``reference-code/seedpod/PLAN-wave-orchestration.md``, a design
      plan v1 never executed). Every service the profile declares gets an
      entry, defaulted to 3 at WRITE time (not looked up with a default
      later) when its own YAML sets no explicit ``deploy_wave`` -- see
      ``seedpod/core/deploy_wave.py``'s ``DeploymentProfile.deploy_wave`` for
      why the KEY SET, not just the values, is this field's real contract.
      Read back by ``deploy.load_audit``/``deploy.plan_waves``
      (``seedpod/engine/steps/deploy.py``, Round 10's "load-and-plan"
      component) to group manifests into waves by SERVICE NAME.
    - ``_config_versions``/``rollout_timeout_seconds``/``persistence_services``/
      ``ingress_strategy``: v1 keys with no v2 template OR service consumer yet --
      they feed Round 10's deploy verbs, not manifest rendering. Ported because
      they come straight off ``raw_profile`` at near-zero cost; no consumer is
      invented for them here. ``ingress_strategy`` reads BOTH shipped shapes, not
      v1's nested-only read: ``seedpod/engine/steps/cluster.py``'s
      ``_cluster_specification_from`` already identified v1's own
      ``ingress_strategy``-nested-inside-``cluster_config`` read
      (reference-code .../core/cluster_spec.py:396-399) as a parity trap --
      ``ingress_strategy`` is a SIBLING of ``cluster_config`` for 3 of the 5
      shipped ``config/deployment-profiles/*.yml`` (``exampleco-dev-stack-nodns``,
      ``exampleco-staging-stack[-nodns]``) and nested inside it for the other 2
      (``exampleco-web-2[-kind]``) -- a nested-only read is silently absent for
      exactly the three profiles that actually configure ingress. This function
      reuses that SAME sibling-overlay rule (the sibling wins when present,
      falling back to the nested value otherwise), rather than re-deriving a
      second, independent normalization that could drift from the committed one.
    - ``ssl_enabled``/``dns_enabled``: NOT a v1 ``_build_resolved_config`` key (v1
      computed these inline inside ``_render_templates`` instead, reference-code
      .../manifest_resolver.py:871-886, from ``manifest_config.ssl``/``.dns`` --
      fields only the RAW profile carries). Ported here, to THIS mapping, because
      it is the only place in v2 that has both ``raw_profile`` and produces the
      ``config`` a caller threads through -- see ``seedpod/services/
      manifests.py``'s "silent-empty decision" for why these are a real, immediate
      consumer (five shipped ``exampleco-stack`` templates), not an invented one.
    - v1's ``timestamp`` (``datetime.now(UTC)``) is DELIBERATELY NOT ported:
      ``seedpod/core`` bans ambient ``now()`` (CLAUDE.md) and this module already
      threads an injected ``Clock`` -- but no shipped template reads
      ``config.timestamp`` and no v2 consumer wants one, so there is nothing worth
      injecting a clock parameter here FOR. Left out entirely rather than wired to
      an unused ``Clock``.

    ``config_overrides`` is applied LAST (v1's own precedence, verbatim) --
    currently always ``{}`` from both ``DeploymentService`` call sites (neither
    ``_deploy`` nor ``deployment_preview`` threads a cluster's real
    ``provider_host``/``dns_config`` yet; a real cluster's public IP/DNS config
    isn't known until AFTER provisioning completes, which is after this function
    runs for a NEW cluster -- a future round can extend ``config_overrides`` with
    an EXISTING reused cluster's row-carried values once a real consumer needs the
    ``provider_host``/``dns`` hostname strategies to resolve to more than ``None``
    for that case)."""
    pod_cidr, service_cidr = allocate_cluster_cidrs(cluster_id)
    config: dict[str, Any] = {
        "cluster_id": cluster_id,
        "environment": environment,
        "cluster_slug": cluster_slug,
        "pod_cidr": pod_cidr,
        "service_cidr": service_cidr,
    }

    # DR-0025 Erratum E1's None-vs-omitted split (docs/decisions/DR-0025-
    # hostname-resolution-ordering.md -- see this function's own docstring bullet
    # above for the full reasoning): ask "does this profile even WANT a
    # hostname?" (`_hostname_strategy`) SEPARATELY from "what value, if any, did
    # the strategy produce?" (`_resolve_hostname`) -- `_resolve_hostname` alone
    # cannot tell those two `None`-producing cases apart (its own docstring).
    strategy, _ = _hostname_strategy(raw_profile, config_overrides)
    if strategy == "none":
        config["cluster_hostname"] = None  # deliberately no hostname -- key PRESENT
    else:
        cluster_hostname = _resolve_hostname(
            raw_profile, config_overrides, cluster_slug, config_overrides.get("provider_host")
        )
        if cluster_hostname is not None:
            config["cluster_hostname"] = cluster_hostname
        # else: key OMITTED entirely -- this strategy WANTED a host and could not
        # produce one yet. StrictUndefined raises downstream, naming
        # "cluster_hostname", instead of a plausible-looking placeholder.

    config["_config_versions"] = {
        "deployment_profile_version": raw_profile.get("version", "1.0"),
        "deployment_profile_name": profile_name,
        "resolution_strategy": raw_profile.get("resolution_strategy", "branch_discovery_with_fallback"),
    }
    config["rollout_timeout_seconds"] = raw_profile.get("rollout_timeout_seconds", 300)

    persistence_services = [
        name
        for name, service_raw in (raw_profile.get("services") or {}).items()
        if isinstance(service_raw, dict) and service_raw.get("persistence") is not None
    ]
    if persistence_services:
        config["persistence_services"] = persistence_services

    # DR-0029 §2/§8 (docs/decisions/DR-0029-wave-orchestration-is-built.md):
    # the service-name-to-deploy_wave mapping, written for EVERY service the
    # profile declares (not just persistence ones) -- a service whose YAML
    # never sets `deploy_wave` is filled with the plan's own default, 3, HERE
    # at write time, not looked up with a default later by the reader
    # (`deploy.plan_waves`/`DeploymentProfile`'s own docstring:
    # "is this service's key present" and "is this service declared at all"
    # are the SAME question, always, by construction). Omitted entirely (not
    # `{}`) when the profile declares no services at all -- matching
    # `persistence_services`'s own "if x:" presence discipline immediately
    # above, and `DeploymentProfile.deploy_wave`'s own default `{}` for that
    # degenerate case.
    deploy_wave = {
        name: _validated_deploy_wave(name, service_raw.get("deploy_wave", 3))
        for name, service_raw in (raw_profile.get("services") or {}).items()
        if isinstance(service_raw, dict)
    }
    if deploy_wave:
        config["deploy_wave"] = deploy_wave

    if data_initialization:
        config["data_initialization"] = dict(data_initialization)

    # Same sibling-overlay rule as `cluster.load_spec`'s own
    # `_cluster_specification_from` (seedpod/engine/steps/cluster.py) -- see this
    # function's own docstring bullet above: the SIBLING `cluster_spec.
    # ingress_strategy` wins when present (never guarded on what `cluster_config`
    # already carries, matching v1's own unconditional overlay), falling back to
    # the shape NESTED inside `cluster_config` otherwise.
    cluster_spec = raw_profile.get("cluster_spec") or {}
    cluster_config = cluster_spec.get("cluster_config") or {}
    ingress_strategy = cluster_spec.get("ingress_strategy") or cluster_config.get("ingress_strategy")
    if ingress_strategy:
        config["ingress_strategy"] = ingress_strategy

    config["ssl_enabled"] = bool((raw_profile.get("ssl") or {}).get("enabled", False))
    config["dns_enabled"] = bool((raw_profile.get("dns") or {}).get("enabled", False))

    config.update(config_overrides)
    return config


# ---------------------------------------------------------------------------
# Birth-row / event construction helpers
# ---------------------------------------------------------------------------


def _node_count(raw_profile: Mapping[str, Any]) -> int:
    cluster_spec = raw_profile.get("cluster_spec") or {}
    cluster_config = cluster_spec.get("cluster_config") or {}
    return int(cluster_config.get("node_count", 1))


def _provider_config_from(raw_profile: Mapping[str, Any]) -> dict[str, Any]:
    """Row synthesis (coherence-review.md: "row synthesis ... is the API-layer
    service's job", Conflict 10) -- ``clusters.provider_config`` is birthed
    straight from the profile's own ``cluster_spec`` block (verbatim: both the
    sibling- and nested-``ingress_strategy`` shapes ``config/deployment-
    profiles/*.yml`` actually ship are passed through unchanged, since
    ``cluster.load_spec``'s own ``_cluster_specification_from`` already
    normalizes either shape -- this function does not need to pick one).

    **Plus the profile's ``dns:`` block, when and only when it is enabled**
    (DR-0034 decision 3). v1 did exactly this, at its own row-synthesis site
    (``reference-code/seedpod/seedpod/orchestrator/cluster_manager.py``:318-321:
    ``if dns_config.get("enabled", False): provider_config_update["dns_config"]
    = dns_config``), and it is what carries the zone/pattern/ttl/proxied a
    provisioning run needs to CREATE the record onto the cluster row -- the
    provision workflows see the row, never the profile. The "only when enabled"
    half is load-bearing: it makes absence in the blob mean absence of intent,
    so ``DnsIntent.from_provider_config`` needs no second opinion.

    ``_cluster_specification_from`` reads only named keys
    (``node_specification``/``cluster_config``/``ingress_strategy``), so the
    extra key is inert for ``ClusterSpecification`` construction."""
    cluster_spec = raw_profile.get("cluster_spec") or {}
    provider_config = dict(cluster_spec)
    dns_config = raw_profile.get("dns") or {}
    if dns_config.get("enabled", False):
        provider_config["dns_config"] = dict(dns_config)
    # And the `ssl:` block on the same terms (DR-0036 decision 3) -- v1 wrote both at
    # the same site, each guarded on its own `enabled` (cluster_manager.py:318-332).
    # `AcmeConfig.from_provider_config` needs BOTH blocks, since a certresolver is only
    # worth configuring for a name that will resolve.
    ssl_config = raw_profile.get("ssl") or {}
    if ssl_config.get("enabled", False):
        provider_config["ssl_config"] = dict(ssl_config)
    return provider_config


def _birth_cluster_row(
    cluster_id: str, *, repo: str, branch: str, environment: str, provider: str,
    node_count: int, ttl_hours: float | None, now, provider_config: Mapping[str, Any] = MappingProxyType({}),
) -> ClusterRow:
    name = f"{repo}-{branch}"
    slug = _cluster_slug_for(cluster_id, repo=repo, branch=branch)
    expires_at = now + timedelta(hours=ttl_hours) if ttl_hours else None
    return ClusterRow(
        id=cluster_id, name=name, slug=slug, origin=Origin.MANAGED, environment=environment,
        repository=repo, branch=branch, status="new", pre_destroy_state=None, version=0,
        provider=provider, provider_config=dict(provider_config), provider_resources={},
        # All three are written later, by `cluster.store_dns_record` during
        # provisioning, and only for a profile that enabled DNS (DR-0034).
        dns_hostname=None, dns_zone=None, dns_record_id=None,
        public_ip=None, node_count=node_count, encrypted_kubeconfig=None, kubeconfig_key_class=None,
        kubeconfig_ref=None, cost_per_hour=0.0, total_cost=0.0, consecutive_health_failures=0,
        failure_reason=None, last_reconciled_at=None, created_at=now, updated_at=now, expires_at=expires_at,
    )


def _birth_deployment_row(
    deployment_id: str, *, cluster_id: str, environment: str, manifest_version: str, now,
    resolved_images: Mapping[str, str] = (), deployed_by: str | None = None,
) -> DeploymentRow:
    """``deployed_by`` is the actor string (``api:<user>``), per DR-0032 -- the same
    value the state audit records for this request, at every entry point. It is a
    ROW-ONLY column: ``DeploymentRepository.persist`` CAS-updates only the columns
    ``DeploymentRecord`` carries, so what is written here at birth survives every
    later transition verbatim (pinned by ``test_deployment_birth_via_row_uniform``)."""
    return DeploymentRow(
        id=deployment_id, cluster_id=cluster_id, environment=environment, status="new", version=0,
        manifest_version=manifest_version, spec_ref=None, resolved_images=resolved_images,
        superseded_by=None, deployed_by=deployed_by, failure_reason=None, created_at=now, updated_at=now,
    )


def _audit_row(
    audit_id: str, *, cluster_id: str, environment: str, repo: str, branch: str, image: str,
    commit: str, profile_name: str, raw_profile: Mapping[str, Any], resolved: ResolvedManifest,
    key_class: str, now,
) -> DeploymentAuditRow:
    return DeploymentAuditRow(
        id=audit_id, deployment_id=None, cluster_id=cluster_id, environment=environment,
        triggering_repo=repo, triggering_branch=branch, triggering_image=image, commit_sha=commit,
        deployment_profile_name=profile_name,
        resolution_strategy=raw_profile.get("resolution_strategy", "branch_discovery_with_fallback"),
        registry_queries=tuple(dataclasses.asdict(q) for q in resolved.registry_queries),
        resolved_images={name: img.image_url for name, img in resolved.resolved_images.items()},
        resolved_config=resolved.resolved_config, resolved_manifests=resolved.rendered_manifests,
        resolved_secrets=resolved.resolved_secrets, key_class=key_class,
        template_files_used=resolved.template_files, created_at=now,
    )
