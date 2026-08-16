"""``seedpod/api/routers/secrets.py`` -- Round 6, api-features component. Thin HTTP
over ``SecretService`` (``seedpod/app/services/secret_service.py``, an
already-committed/tested component): every handler here does request -> service
call -> response shaping, never its own ``uow``/crypto call of its own.

Salvaged request/response shapes from ``reference-code/seedpod/seedpod/api/
secrets.py`` (``list_secrets`` :64, ``create_secret`` :103, ``reveal_secret`` :150,
``delete_secret`` :230), adapted to v2:

- ``GET /api/secrets`` returns ``{"secrets": [...]}`` (DR-0017's uniform
  collection envelope; v1 returned a bare array) -- ``key_name``/``environment``/
  ``key_class``/``created_at``/``updated_at`` per this round's brief (v1's
  ``last_revealed_by``/``last_revealed_at`` per-row reveal-audit lookups are not
  ported -- an N+1 audit query per listed secret with no v2 caller asking for it;
  the reveal audit trail itself is fully preserved, just not surfaced on the list
  view).
- ``environment`` query param is OPTIONAL (v1 :66): defaults to the calling key's
  own scoped environment (falling back to ``"local"`` for an ``'all'``-scoped key,
  since ``'all'`` is the api_keys sentinel, never a real ``secrets.environment``
  value -- ``ApiKeyService``'s own docstring).
- Unknown environment RAISES (this round's brief; ``SecretService``'s own
  docstring, "Gotcha 8, closed at the crypto layer... unknown env RAISES, never
  DEV-defaults") -- every handler below maps the resulting ``PermanentError``
  (``ErrorCode.INVALID_INPUT``) to 400, never silently substituting DEV.
- v1's split ``secrets:read``/``secrets:write`` scopes narrow to this catalog's
  existing finer-grained ``secrets:read``/``secrets:create``/``secrets:delete``
  (``seedpod/api/permissions.py`` already carries all three -- no catalog gap to
  fill, see that module's own docstring).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.app.services.secret_service import SecretNotFound
from seedpod.core.errors import PermanentError
from seedpod.data.repositories import ApiKeyRow, SecretMetadataRow

__all__ = ["router"]

router = APIRouter(prefix="/secrets", tags=["secrets"])


class SecretUpsertRequest(BaseModel):
    environment: str
    key_name: str = Field(..., min_length=1, max_length=255)
    value: str = Field(..., min_length=1)


def _serialize(row: SecretMetadataRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "environment": row.environment,
        "key_name": row.key_name,
        "key_class": row.key_class,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def _default_environment(api_key: ApiKeyRow) -> str:
    if api_key.environment and api_key.environment != "all":
        return api_key.environment
    return "local"


@router.get("")
async def list_secrets(
    request: Request,
    environment: str | None = None,
    api_key=Depends(require_permission("secrets:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    env = environment or _default_environment(api_key)
    try:
        rows = await app.services.secrets.list_for_environment(env)
    except PermanentError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"secrets": [_serialize(r) for r in rows]}


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_secret(
    body: SecretUpsertRequest,
    request: Request,
    api_key=Depends(require_permission("secrets:create")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        await app.services.secrets.upsert(
            body.environment, body.key_name, body.value, actor=f"api:{api_key.username}"
        )
    except PermanentError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"environment": body.environment, "key_name": body.key_name, "status": "created"}


@router.get("/{environment}/{key_name}/reveal")
async def reveal_secret(
    environment: str,
    key_name: str,
    request: Request,
    api_key=Depends(require_permission("secrets:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        value = await app.services.secrets.reveal(environment, key_name, actor=f"api:{api_key.username}")
    except PermanentError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except SecretNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return {"environment": environment, "key_name": key_name, "value": value}


@router.delete("/{environment}/{key_name}")
async def delete_secret(
    environment: str,
    key_name: str,
    request: Request,
    api_key=Depends(require_permission("secrets:delete")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        deleted = await app.services.secrets.delete(environment, key_name, actor=f"api:{api_key.username}")
    except PermanentError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"secret {environment}/{key_name} not found")
    return {"environment": environment, "key_name": key_name, "status": "deleted"}
