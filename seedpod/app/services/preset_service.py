"""``PresetService`` -- CRUD + deploy over the ``deployment_presets`` table
(Round 6, api-features component). Backs ``GET/POST/PUT/DELETE /api/presets``
and ``POST /api/presets/{id}/deploy`` (``seedpod/api/routers/presets.py``).

Constructor shape follows the established Round-6 app-service precedent
(``SecretService``/``ApiKeyService``'s own module docstrings: "``repos`` here is
the standalone repository directly -- not in the Dispatcher-facing
``Repositories`` bundle... the next component wires them alongside the app-
services that need them" -- this is that component, for ``PresetRepository``).
``deployments`` is the already-committed, already-tested ``DeploymentService``
(a sibling app-service, not the domain layer) -- ``PresetService.deploy``
delegates the actual birth/manifest-resolution/``Dispatcher.apply()`` pipeline
to ``DeploymentService.deploy_direct`` (this round's new entrypoint, added
alongside this file) rather than re-implementing it, matching CLAUDE.md's "State
changes go through ``Dispatcher.apply()`` only" -- this service itself never
opens a ``uow()`` around a cluster/deployment write, only around the preset row.

Salvaged request/response shapes + deploy-merge logic from
``reference-code/seedpod/seedpod/api/presets.py`` (``create_preset`` :121,
``deploy_from_preset`` :461), adapted to v2:

- Preset ``environment`` is derived from the target deployment profile's
  ``environment_type`` key (every real profile under ``config/deployment-
  profiles/*.yml`` uses this name, e.g. ``environment_type: "ephemeral"`` --
  v1's ``_load_profile(...).get('environment', 'ephemeral')`` read a key
  (``environment``) that no shipped v1 OR v2 profile ever actually sets, so in
  practice v1's own presets always silently defaulted to ``'ephemeral'``
  regardless of the profile. Reading the field profiles genuinely use is a
  correctness fix (CLAUDE.md: "don't pin v1 bugs"), not a reinterpretation --
  falls back to ``'ephemeral'`` only if NEITHER key is present, matching v1's
  own default for the one case it actually hit.
- ``deploy``'s synthetic triggering context (v1's ``DeploymentContext(repo=
  "preset-deploy", ...)``, presets.py :534-541) is scoped per-PRESET here
  (``f"preset:{preset.name}"``), not one shared literal constant: v1's single
  ``"preset-deploy"`` repo name means ``DeploymentService``'s "reuse the one
  ACTIVE cluster already matching (repository, branch, environment)" rule
  (``deployment_service.py``'s own module docstring) would silently reuse ONE
  ANY-preset's active cluster for every other preset sharing a branch+env --
  never the intended behavior for two distinct saved configurations. Scoping
  the synthetic repo per preset name is the reuse rule's own intent applied
  correctly (redeploying the SAME preset reuses its cluster; a DIFFERENT preset
  never collides with it), not new product behavior.
- ``data_initialization`` (restore-from-snapshot on deploy): **fixed, Round 10
  (DR-0028 decision 2, closing Erratum E2's own "inert" gap,
  docs/decisions/DR-0028-deploy-path-dtos.md).** An earlier version of this
  docstring documented ``data_initialization`` as accepted-but-not-executed --
  round-tripped only into ``DeploymentResponse.message`` -- because no
  committed engine verb could restore a snapshot yet. Round 10 built
  ``deploy.restore_snapshot`` (and the wave orchestration that reaches it), so
  that gap is now closed at the SOURCE: this method threads
  ``data_initialization`` straight through to
  ``DeploymentService.deploy_direct``, which carries it into
  ``resolved_config["data_initialization"]`` (``_build_resolved_config``'s own
  docstring) -- the same place ``deploy.load_audit`` reads every other
  resolved fact from, "like every other resolved fact" (DR-0028's own words).
  It is STILL echoed into the response ``message`` below (never a silent
  no-op either way), but it is no longer merely echoed -- an operator's
  restore request now actually reaches ``deploy.plan_waves``/
  ``deploy.restore_snapshot`` once the deployment's ``deploy-waves`` workflow
  run executes.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from seedpod.app.services.deployment_service import DeploymentResponse, DeploymentService
from seedpod.app.services.profiles import load_deployment_profile
from seedpod.core.clock import Clock
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.data.repositories import PresetRepository, PresetRow
from seedpod.data.uow import UnitOfWork

__all__ = ["PresetService", "PresetNotFound", "PresetNameExists"]

_GHCR_BASE = "ghcr.io"
_DEFAULT_TTL_HOURS = 24


class PresetNotFound(LookupError):
    pass


class PresetNameExists(PermanentError):
    def __init__(self, name: str) -> None:
        super().__init__(
            f"preset named {name!r} already exists",
            code=ErrorCode.ALREADY_EXISTS,
            provider="preset-service",
            command="create",
            detail={"name": name},
        )


def _profile_environment(raw_profile: Mapping[str, Any]) -> str:
    """Every shipped profile names this ``environment_type`` -- see module
    docstring. ``environment`` is tried second only for forward/backward
    tolerance with a profile that might use the v1-literal key name."""
    return raw_profile.get("environment_type") or raw_profile.get("environment") or "ephemeral"


def _build_image_overrides(raw_profile: Mapping[str, Any], overrides: Mapping[str, Mapping[str, str]]) -> dict[str, str]:
    """``{service: {"tag": "..."}}`` -> ``{service: "ghcr.io/<repo>:<tag>"}``,
    salvaged from v1's ``_build_image_overrides``
    (``reference-code/seedpod/seedpod/api/presets.py:427-457``). ``repository``
    resolves through the profile's own ``services[name].repository`` when the
    service is declared there (may differ from the service's own key), else
    falls back to the service name itself -- same tolerance v1 gave an override
    for a service the profile doesn't declare ("will pass through")."""
    services_cfg = raw_profile.get("services") or {}
    result: dict[str, str] = {}
    for service_name, override in overrides.items():
        tag = override.get("tag") if isinstance(override, Mapping) else None
        if not tag:
            continue
        service_cfg = services_cfg.get(service_name) or {}
        repo_name = service_cfg.get("repository", service_name)
        result[service_name] = f"{_GHCR_BASE}/{repo_name}:{tag}"
    return result


class PresetService:
    def __init__(
        self,
        presets: PresetRepository,
        deployments: DeploymentService,
        uow: UnitOfWork,
        clock: Clock,
        id_gen: Callable[[], str],
        config_dir: Path,
    ) -> None:
        self._presets = presets
        self._deployments = deployments
        self._uow = uow
        self._clock = clock
        self._id_gen = id_gen
        self._config_dir = config_dir

    # -------------------------------------------------------------------
    # CRUD
    # -------------------------------------------------------------------

    async def get(self, preset_id: str) -> PresetRow:
        async with self._uow() as tx:
            row = self._presets.get(tx, preset_id)
        if row is None:
            raise PresetNotFound(preset_id)
        return row

    async def list(self, *, profile: str | None = None) -> list[PresetRow]:
        async with self._uow() as tx:
            return self._presets.list(tx, profile=profile)

    async def create(
        self,
        *,
        name: str,
        description: str | None,
        profile_name: str,
        service_overrides: Mapping[str, Mapping[str, str]] | None,
        default_branch: str | None,
        default_ttl_hours: int | None,
        default_provider: str | None = None,  # DR-0046
        naming_strategy: Mapping[str, Any] | None,
        created_by: str,
    ) -> PresetRow:
        _, raw_profile = load_deployment_profile(self._config_dir, profile_name)  # PermanentError(NOT_FOUND) propagates
        environment = _profile_environment(raw_profile)
        now = self._clock.now()
        preset_id = self._id_gen()
        row = PresetRow(
            id=preset_id, name=name, description=description, profile_name=profile_name,
            environment=environment, service_overrides=dict(service_overrides or {}),
            default_branch=default_branch, default_ttl_hours=default_ttl_hours,
            default_provider=default_provider,
            naming_strategy=dict(naming_strategy) if naming_strategy else None,
            created_by=created_by, created_at=now, last_used_at=None, use_count=0,
        )
        async with self._uow() as tx:
            if self._presets.get_by_name(tx, name) is not None:
                raise PresetNameExists(name)
            self._presets.insert(tx, row)
        return row

    async def update(
        self,
        preset_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        profile_name: str | None = None,
        service_overrides: Mapping[str, Mapping[str, str]] | None = None,
        default_branch: str | None = None,
        default_ttl_hours: int | None = None,
        default_provider: str | None = None,  # DR-0046
        naming_strategy: Mapping[str, Any] | None = None,
    ) -> PresetRow:
        sets: dict[str, Any] = {}
        if name is not None:
            sets["name"] = name
        if description is not None:
            sets["description"] = description
        if profile_name is not None:
            sets["profile_name"] = profile_name
            _, raw_profile = load_deployment_profile(self._config_dir, profile_name)
            sets["environment"] = _profile_environment(raw_profile)
        if service_overrides is not None:
            sets["service_overrides"] = dict(service_overrides)
        if default_branch is not None:
            sets["default_branch"] = default_branch
        if default_ttl_hours is not None:
            sets["default_ttl_hours"] = default_ttl_hours
        if default_provider is not None:
            sets["default_provider"] = default_provider
        if naming_strategy is not None:
            sets["naming_strategy"] = dict(naming_strategy)

        async with self._uow() as tx:
            if self._presets.get(tx, preset_id) is None:
                raise PresetNotFound(preset_id)
            if name is not None:
                existing = self._presets.get_by_name(tx, name)
                if existing is not None and existing.id != preset_id:
                    raise PresetNameExists(name)
            self._presets.update(tx, preset_id, **sets)
            row = self._presets.get(tx, preset_id)
        assert row is not None
        return row

    async def delete(self, preset_id: str) -> None:
        async with self._uow() as tx:
            deleted = self._presets.delete(tx, preset_id)
        if not deleted:
            raise PresetNotFound(preset_id)

    # -------------------------------------------------------------------
    # Deploy
    # -------------------------------------------------------------------

    async def deploy(
        self,
        preset_id: str,
        *,
        branch: str | None = None,
        service_overrides: Mapping[str, Mapping[str, str]] | None = None,
        provider_override: str | None = None,
        ttl_hours: float | None = None,
        cluster_name: str | None = None,  # noqa: ARG002 -- see module docstring's cluster-naming scope note
        data_initialization: Mapping[str, Any] | None = None,
        actor: str,
    ) -> DeploymentResponse:
        preset = await self.get(preset_id)
        _, raw_profile = load_deployment_profile(self._config_dir, preset.profile_name)

        discovery_branch = branch or preset.default_branch or "main"
        merged_overrides: dict[str, Mapping[str, str]] = dict(preset.service_overrides or {})
        merged_overrides.update(service_overrides or {})
        image_overrides = _build_image_overrides(raw_profile, merged_overrides)
        effective_ttl = ttl_hours or preset.default_ttl_hours or _DEFAULT_TTL_HOURS
        # DR-0046 decision 2a, most specific wins: call-time override, then the
        # preset, then (inside DeploymentService) the profile's `provider:` key, then
        # the global default. A preset BEATS a profile -- argued, not assumed: a
        # preset's provider is operator intent, the same kind of thing as the
        # call-time flag. The counter-argument is that a profile's `provider:` might
        # be a correctness constraint, so the tie is broken by observability rather
        # than by refusing: whoever deploys is told which provider was chosen.
        effective_provider = provider_override or preset.default_provider

        reason = f"Preset deployment: {preset.name} ({preset.profile_name})"
        if data_initialization:
            reason += f"; data_initialization requested: {dict(data_initialization)!r}"

        async with self._uow() as tx:
            self._presets.record_usage(tx, preset_id, clock=self._clock)

        return await self._deployments.deploy_direct(
            profile_name=preset.profile_name,
            environment=preset.environment,
            repo=f"preset:{preset.name}",
            branch=discovery_branch,
            image=f"{_GHCR_BASE}/preset-deploy:{discovery_branch.replace('/', '-')}",
            commit=preset_id[:8],
            ttl_hours=effective_ttl,
            provider_override=effective_provider,  # DR-0046
            image_overrides=image_overrides,
            reason=reason,
            data_initialization=data_initialization,
            actor=actor,
        )
