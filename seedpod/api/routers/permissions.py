"""``GET /api/permissions`` -- ui-contract §1: "``permissions{}, categories{}``"
(CreateApiKey.jsx's scope picker when minting a new key). Gated behind the literal
``"*"`` scope (the v2 super-wildcard, ``seedpod/api/permissions.py``) -- the same
admin-only gate v1 put on this endpoint (``require_permission("admin:*")``,
``reference-code/seedpod/seedpod/api/auth.py``), translated to v2's bare-``"*"``
convention (that module's own docstring)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from seedpod.api.auth import require_permission
from seedpod.api.permissions import AVAILABLE_PERMISSIONS, PERMISSION_CATEGORIES

__all__ = ["router"]

router = APIRouter(tags=["permissions"])


@router.get("/permissions")
async def list_permissions(_api_key=Depends(require_permission("*"))) -> dict[str, Any]:  # noqa: B008
    return {"permissions": AVAILABLE_PERMISSIONS, "categories": PERMISSION_CATEGORIES}
