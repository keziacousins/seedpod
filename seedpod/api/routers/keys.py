"""``seedpod/api/routers/keys.py`` -- Round 6, api-features component. Thin HTTP
over ``ApiKeyService`` (``seedpod/app/services/api_key_service.py``, an
already-committed/tested component -- ``create_api_key`` is the exact conftest-
pinned contract ``tests/conftest.py``'s ``make_auth_headers`` already exercises).

Salvaged request/response shapes from ``reference-code/seedpod/seedpod/api/
auth.py`` (``create_api_key`` :73, ``list_api_keys`` :143, ``get_api_key`` :185,
``revoke_api_key`` :222, ``update_api_key`` :257), adapted to v2:

- Every route here is gated behind the bare ``"*"`` super-wildcard (v1's
  ``admin:*`` on every ``/keys``/``/permissions`` route, translated per
  ``seedpod/api/permissions.py``'s own docstring -- this catalog has no
  dedicated ``keys:*`` scope, matching v1's own "admin only" design, not a v2
  gap).
- ``permissions`` is a JSON **list** of granted permission strings end to end
  (request AND response) -- v1's ``dict[str, bool]`` shape is NOT ported
  (``ApiKeyService``'s own docstring: "not a bug pin... per this round's brief").
- ``GET /api/keys`` returns ``{"keys": [...]}`` (DR-0017's uniform collection
  envelope; v1 returned a bare array).
- ``POST /api/keys`` returns the plaintext ``api_key`` FLAT alongside the key's
  own metadata fields (v1 nested metadata under a ``key_info`` sub-object) --
  matches this package's own established flattening precedent
  (``seedpod/api/routers/deployments.py``'s ``POST /api/deployment-preview``,
  "flat at the top level... no v1 wrapper to preserve"); ui-contract's own
  obligation is only ``response.api_key``, satisfied either way, and flat is
  simpler for the SPA to consume alongside the rest of this package's DTOs.
- ``permissions`` are NOT mutable via ``PATCH`` (v1's own module docstring:
  "Permissions are immutable. To change permissions, create a new key and
  revoke the old one.") -- carried forward verbatim, not a v2 narrowing.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.app.services.api_key_service import ApiKeyNotFound
from seedpod.data.repositories import ApiKeyRow

__all__ = ["router"]

router = APIRouter(prefix="/keys", tags=["keys"])


class CreateApiKeyRequest(BaseModel):
    username: str
    environment: str | None = None
    permissions: list[str] = Field(default_factory=list)
    expires_hours: float | None = None
    description: str | None = None


class UpdateApiKeyRequest(BaseModel):
    description: str | None = None
    expires_at: datetime | None = None


def _serialize(row: ApiKeyRow, *, now: datetime) -> dict[str, Any]:
    is_valid = row.is_active and (row.expires_at is None or row.expires_at > now)
    return {
        "id": row.id,
        "username": row.username,
        "environment": row.environment,
        "permissions": list(row.permissions),
        "is_active": row.is_active,
        "is_valid": is_valid,
        "description": row.description,
        "created_by": row.created_by,
        "created_at": row.created_at.isoformat(),
        "expires_at": row.expires_at.isoformat() if row.expires_at else None,
        "last_used_at": row.last_used_at.isoformat() if row.last_used_at else None,
    }


@router.get("")
async def list_keys(
    request: Request,
    active_only: bool = False,
    username: str | None = None,
    environment: str | None = None,
    _api_key=Depends(require_permission("*")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    rows = await app.services.api_keys.list(username=username, environment=environment, active_only=active_only)
    now = app.clock.now()
    return {"keys": [_serialize(r, now=now) for r in rows]}


@router.get("/{key_id}")
async def get_key(
    key_id: int, request: Request, _api_key=Depends(require_permission("*"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.api_keys.get(key_id)
    except ApiKeyNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return _serialize(row, now=app.clock.now())


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_key(
    body: CreateApiKeyRequest, request: Request, api_key=Depends(require_permission("*"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    row, plaintext = await app.services.api_keys.create_api_key(
        username=body.username, environment=body.environment, permissions=body.permissions,
        expires_hours=body.expires_hours, description=body.description, created_by=api_key.username,
    )
    return {"api_key": plaintext, **_serialize(row, now=app.clock.now())}


@router.patch("/{key_id}")
async def update_key(
    key_id: int, body: UpdateApiKeyRequest, request: Request, _api_key=Depends(require_permission("*"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.api_keys.update(key_id, description=body.description, expires_at=body.expires_at)
    except ApiKeyNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return _serialize(row, now=app.clock.now())


@router.delete("/{key_id}")
async def revoke_key(
    key_id: int, request: Request, _api_key=Depends(require_permission("*"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    revoked = await app.services.api_keys.revoke(key_id)
    if not revoked:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"api key {key_id} not found")
    return {"id": key_id, "status": "revoked"}
