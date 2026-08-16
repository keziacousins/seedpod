"""Bearer-token auth (ui-contract §5): ``get_current_api_key`` validates the
``Authorization: Bearer <key>`` header through ``ApiKeyService`` and
``require_permission(scope)`` layers a permission check on top -- the two
dependencies every non-public REST handler in this package depends on.

Salvaged from ``reference-code/seedpod/seedpod/api/dependencies.py``'s
``get_current_api_key``/``require_permission`` (401 on missing/invalid/expired key,
403 on insufficient scope), reworked onto v2's real collaborators: v1's
``APIKeyManager.validate_api_key``/``check_permission`` (module-global, late-import)
become ``app.services.api_keys.validate(...)`` (Round-6 ``ApiKeyService``, reached
via ``api.state.app`` -- ``seedpod/api/deps.py``) + this package's
``permissions.has_permission`` (the v2 list-shaped scope check). 401 clears nothing
server-side (ui-contract §5: "token cleared" is a client-side ``localStorage``
concern, not a server-side session to invalidate) -- ``validate()`` is read-only,
this module writes nothing.

v1's ``permission_check_performed`` bookkeeping (a ``ContextVar`` PLUS a
``request.state`` mirror, read post-hoc by a default-deny backstop middleware) is
NOT ported here -- ``seedpod/api/factory.py``'s own module docstring explains why
that backstop was tried and dropped (it deadlocks a long-lived
``StreamingResponse``). Every non-public route in this package names its own
required scope via ``require_permission(...)`` directly; there is no separate
post-hoc mechanism to feed."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from seedpod.api.deps import get_app
from seedpod.api.permissions import has_permission
from seedpod.data.repositories import ApiKeyRow

__all__ = ["get_current_api_key", "require_permission"]

_security = HTTPBearer(auto_error=False)


async def get_current_api_key(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_security),  # noqa: B008 -- FastAPI's own DI idiom
) -> ApiKeyRow:
    """Validate the ``Authorization: Bearer`` header against ``ApiKeyService``.
    401 for a missing header, an unknown key, or one that's inactive/expired
    (``ApiKeyService.validate`` collapses all three into ``None`` -- module
    docstring; this dependency never distinguishes them in the response, same as
    v1)."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    app = get_app(request)
    key = await app.services.api_keys.validate(credentials.credentials)
    if key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return key


def require_permission(scope: str):
    """Dependency factory: 401 (via ``get_current_api_key``) for a missing/invalid
    key, 403 for a valid key that lacks ``scope`` (``permissions.has_permission``)."""

    async def _dependency(api_key: ApiKeyRow = Depends(get_current_api_key)) -> ApiKeyRow:  # noqa: B008
        if not has_permission(api_key.permissions, scope):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions for operation: {scope}",
            )
        return api_key

    return _dependency
