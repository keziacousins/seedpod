"""``seedpod/api/routers/presets.py`` -- Round 6, api-features component. Thin
HTTP over ``PresetService`` (``seedpod/app/services/preset_service.py``, this
same round's sibling component): every handler here does request -> service
call -> response shaping. ``POST /{id}/deploy`` is the one write that touches
cluster/deployment state -- it delegates entirely to ``DeploymentService.
deploy_direct`` (through ``PresetService.deploy``), never opens its own
``Dispatcher.apply()``/``uow`` write (DR-0008/CLAUDE.md).

Salvaged request/response shapes from ``reference-code/seedpod/seedpod/api/
presets.py`` (``create_preset`` :121, ``list_presets`` :186, ``update_preset``
:236, ``delete_preset`` :296, ``deploy_from_preset`` :461), adapted to v2:

- ``GET /api/presets`` returns ``{"presets": [...]}`` (DR-0017's uniform
  collection envelope; v1 returned a bare array).
- ``preset.environment`` is READ-ONLY over HTTP (derived from the target
  profile's ``environment_type`` at create/update time, ``PresetService``'s own
  docstring) -- never a request field, matching v1 (v1's own
  ``PresetCreateRequest``/``PresetUpdateRequest`` never accept it either).
- Preset CRUD + deploy reuse the existing ``deployments:read``/
  ``deployments:create`` scopes (v1 parity -- ``seedpod/api/permissions.py``'s
  own docstring: no dedicated ``presets:*`` scope in either tree).
- ``POST /{id}/deploy``'s response is ``DeploymentResponse`` shaped exactly like
  ``POST /api/version-update``'s (``seedpod/api/routers/deployments.py``'s
  ``_serialize_deployment_response``) -- the SPA's own obligation is just
  ``result.deployment_id`` (ui-contract row 50); reusing the identical shape
  means a preset-deploy and a rule-deploy render through the same
  ``DeploymentDetail.jsx`` navigation path v1 already relied on.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.app.services.deployment_service import DeploymentResponse
from seedpod.app.services.preset_service import PresetNameExists, PresetNotFound
from seedpod.core.errors import PermanentError
from seedpod.data.repositories import PresetRow

__all__ = ["router"]

router = APIRouter(prefix="/presets", tags=["presets"])


def _no_naming_strategy(value: dict[str, Any] | None) -> dict[str, Any] | None:
    """DR-0038 decision 2 -- reject rather than silently store.

    v2 replaced v1's naming-strategy engine with a deterministic slugifier
    (`app/services/deployment_service.py`'s `_slugify`), but the preset surface kept
    accepting, storing and echoing `naming_strategy`, so a user could set it, see it
    returned, and get a derived slug anyway. Failing where the user can see it is the
    same call DR-0037 made for an unhonoured `resolution_strategy`.

    Existing rows are untouched: the column stays and the GET serializer still returns
    whatever a row holds (decision 3). Only setting a non-null value is refused."""
    if value:
        raise ValueError(
            "naming_strategy is not supported: cluster slugs are derived deterministically from "
            "repository, branch and cluster id. The slug is also the DNS record name, so a fixed "
            "name would collide across clusters from the same preset (DR-0038)."
        )
    return value


class PresetCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: str | None = None
    profile_name: str
    service_overrides: dict[str, dict[str, str]] | None = None
    default_branch: str | None = None
    default_ttl_hours: int | None = None
    default_provider: str | None = None  # DR-0046: the provider this preset pins
    # DR-0038 decision 2: accepted only as `null`. v2 derives every cluster slug
    # deterministically (`_slugify`), so a stored naming_strategy did nothing but
    # echo back -- and the slug is now the DNS record name (DR-0034), where a
    # `fixed` strategy would give two clusters from one preset the same hostname.
    naming_strategy: dict[str, Any] | None = None

    _reject_naming_strategy = field_validator("naming_strategy")(_no_naming_strategy)


class PresetUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    profile_name: str | None = None
    service_overrides: dict[str, dict[str, str]] | None = None
    default_branch: str | None = None
    default_ttl_hours: int | None = None
    default_provider: str | None = None  # DR-0046
    naming_strategy: dict[str, Any] | None = None

    _reject_naming_strategy = field_validator("naming_strategy")(_no_naming_strategy)


class RestoreFromLatest(BaseModel):
    branch: str | None = None
    profile: str | None = None
    max_age_days: int | None = None


class DataInitialization(BaseModel):
    restore_from_snapshot: str | None = None
    restore_from_latest: RestoreFromLatest | None = None
    services: list[str] | None = None


class DeployFromPresetRequest(BaseModel):
    branch: str | None = None
    service_overrides: dict[str, dict[str, str]] | None = None
    provider_override: str | None = None
    ttl_hours: float | None = None
    cluster_name: str | None = None
    data_initialization: DataInitialization | None = None


def _serialize(row: PresetRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "name": row.name,
        "description": row.description,
        "profile_name": row.profile_name,
        "environment": row.environment,
        "service_overrides": dict(row.service_overrides or {}),
        "default_branch": row.default_branch,
        "default_ttl_hours": row.default_ttl_hours,
        "default_provider": row.default_provider,  # DR-0046
        "naming_strategy": dict(row.naming_strategy) if row.naming_strategy else None,
        "created_by": row.created_by,
        "created_at": row.created_at.isoformat(),
        "last_used_at": row.last_used_at.isoformat() if row.last_used_at else None,
        "use_count": row.use_count,
    }


def _serialize_deployment_response(result: DeploymentResponse) -> dict[str, Any]:
    return {
        "deployment_id": result.deployment_id,
        "cluster_id": result.cluster_id,
        "status": result.status,
        "message": result.message,
        "environment": result.environment,
        "provider": result.provider,  # DR-0046 decision 4
    }


@router.get("")
async def list_presets(
    request: Request,
    profile: str | None = None,
    _api_key=Depends(require_permission("deployments:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    rows = await app.services.presets.list(profile=profile)
    return {"presets": [_serialize(r) for r in rows]}


@router.get("/{preset_id}")
async def get_preset(
    preset_id: str, request: Request, _api_key=Depends(require_permission("deployments:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.presets.get(preset_id)
    except PresetNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return _serialize(row)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_preset(
    body: PresetCreateRequest, request: Request, api_key=Depends(require_permission("deployments:create"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.presets.create(
            name=body.name, description=body.description, profile_name=body.profile_name,
            service_overrides=body.service_overrides, default_branch=body.default_branch,
            default_ttl_hours=body.default_ttl_hours, default_provider=body.default_provider,
            naming_strategy=body.naming_strategy,
            created_by=api_key.username,
        )
    except PresetNameExists as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except PermanentError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return _serialize(row)


@router.put("/{preset_id}")
async def update_preset(
    preset_id: str, body: PresetUpdateRequest, request: Request,
    _api_key=Depends(require_permission("deployments:create")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.presets.update(
            preset_id, name=body.name, description=body.description, profile_name=body.profile_name,
            service_overrides=body.service_overrides, default_branch=body.default_branch,
            default_ttl_hours=body.default_ttl_hours, default_provider=body.default_provider,
            naming_strategy=body.naming_strategy,
        )
    except PresetNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except PresetNameExists as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except PermanentError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return _serialize(row)


@router.delete("/{preset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_preset(
    preset_id: str, request: Request, _api_key=Depends(require_permission("deployments:create"))  # noqa: B008
) -> None:
    app = get_app(request)
    try:
        await app.services.presets.delete(preset_id)
    except PresetNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/{preset_id}/deploy")
async def deploy_preset(
    preset_id: str,
    request: Request,
    body: DeployFromPresetRequest | None = None,
    api_key=Depends(require_permission("deployments:create")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    body = body or DeployFromPresetRequest()
    data_init = body.data_initialization.model_dump(exclude_none=True) if body.data_initialization else None
    try:
        result = await app.services.presets.deploy(
            preset_id, branch=body.branch, service_overrides=body.service_overrides,
            provider_override=body.provider_override, ttl_hours=body.ttl_hours,
            cluster_name=body.cluster_name, data_initialization=data_init,
            actor=f"api:{api_key.username}",
        )
    except PresetNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except PermanentError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return _serialize_deployment_response(result)
