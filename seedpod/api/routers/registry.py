"""``seedpod/api/routers/registry.py`` -- Round 6, api-features component.
Read-only browse surface over on-disk deployment profiles / provider config and
the GHCR supporting service (DR-0015's ``app.ghcr``, credential-gated) -- used by
the SPA to populate dropdowns for preset creation and ad-hoc deployments
(ui-contract: "GET /api/registry/*" must survive).

Salvaged request/response shapes from ``reference-code/seedpod/seedpod/api/
registry.py`` (``list_profiles`` :98, ``get_profile`` :151, ``list_tags`` :243,
``list_providers`` :314), adapted to v2:

- Every route reuses the existing ``deployments:read`` scope (v1 parity --
  ``seedpod/api/permissions.py``'s own docstring: no dedicated ``registry:*``
  scope in either tree).
- ``GET /api/registry/tags/{repo}`` goes through ``app.ghcr`` (DR-0015's
  ``GhcrService``, constructed only when ``AppConfig.github_token`` is set) --
  ``503`` when unset, matching v1's own "GHCR service not available" gate.
  Tests inject a real ``httpx.MockTransport``-backed ``http_transport`` seam
  (``tests/conftest.py``'s own documented seam) with ``github_token`` set, never
  ``Mock``/``patch``.
- ``GET /api/registry/providers`` lists exactly the CURRENTLY ENABLED providers
  (``app.providers``'s own keys -- the composition root's already-resolved
  enabled-set, ``seedpod/app/factory.py``'s ``load_enabled_providers``), not
  every ``config/providers/*.yml`` file on disk regardless of ``enabled:``/
  ``SEEDPOD_ENABLED_PROVIDERS`` -- matching v1's own "Only returns providers
  that are currently enabled" docstring exactly, now the actually-correct
  v2 source of truth for "enabled" rather than a second disk read.
- ``GET /api/registry/repositories`` has NO ui-contract §1 row (grep turns up
  zero mentions outside this round's own build brief) and ``GhcrService`` --
  deliberately scope-narrowed, that module's own docstring -- carries no
  ``list_repositories``/org-package-listing method to wire (v1's ``GHCRClient.
  list_repositories`` was never salvaged; inventing one here would be new
  supporting-service surface, not wiring). This derives the same "what
  container repositories exist" answer from a source v2 genuinely already has:
  the deduplicated set of ``repository`` names declared across every on-disk
  deployment profile -- serving the identical SPA purpose (populate a
  repository-choice dropdown) without a live GHCR org-listing call.
"""

from __future__ import annotations

from typing import Any

import yaml
from fastapi import APIRouter, Depends, HTTPException, Request, status

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app

__all__ = ["router"]

router = APIRouter(prefix="/registry", tags=["registry"])


def _load_yaml(path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


def _profile_services(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "repository": cfg.get("repository", name),
            "port": cfg.get("port"),
            "external": bool(cfg.get("external", False)),
        }
        for name, cfg in (raw.get("services") or {}).items()
        if isinstance(cfg, dict)
    ]


def _serialize_profile(name: str, raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "provider": raw.get("provider", "unknown"),
        "environment": raw.get("environment_type", raw.get("environment", "unknown")),
        "services": _profile_services(raw),
        "description": raw.get("description"),
    }


@router.get("/profiles")
async def list_profiles(
    request: Request, _api_key=Depends(require_permission("deployments:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    profiles_dir = app.config.config_dir / "deployment-profiles"
    profiles = []
    if profiles_dir.exists():
        for path in sorted(profiles_dir.glob("*.yml")):
            raw = _load_yaml(path)
            if raw:
                profiles.append(_serialize_profile(path.stem, raw))
    return {"profiles": profiles}


@router.get("/profiles/{profile_name}")
async def get_profile(
    profile_name: str, request: Request, _api_key=Depends(require_permission("deployments:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    path = app.config.config_dir / "deployment-profiles" / f"{profile_name}.yml"
    if not path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"profile {profile_name!r} not found")
    return _serialize_profile(profile_name, _load_yaml(path))


@router.get("/providers")
async def list_providers(
    request: Request, _api_key=Depends(require_permission("deployments:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    providers = []
    for name in sorted(app.providers.keys()):
        raw = _load_yaml(app.config.config_dir / "providers" / f"{name}.yml")
        providers.append({"name": name, "display_name": raw.get("display_name", name)})
    return {"providers": providers}


@router.get("/repositories")
async def list_repositories(
    request: Request, _api_key=Depends(require_permission("deployments:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    profiles_dir = app.config.config_dir / "deployment-profiles"
    repos: set[str] = set()
    if profiles_dir.exists():
        for path in sorted(profiles_dir.glob("*.yml")):
            raw = _load_yaml(path)
            for svc in (raw.get("services") or {}).values():
                if isinstance(svc, dict) and svc.get("repository"):
                    repos.add(svc["repository"])
    return {"repositories": [{"name": r} for r in sorted(repos)]}


@router.get("/tags/{repository}")
async def list_tags(
    repository: str,
    request: Request,
    limit: int = 50,
    _api_key=Depends(require_permission("deployments:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    if app.ghcr is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="GHCR service not available -- GITHUB_TOKEN is not configured",
        )
    tags = await app.ghcr.list_tags(repository)
    tags = sorted(tags, key=lambda t: t.updated_at, reverse=True)[:limit]
    return {
        "repository": repository,
        "tags": [{"tag": t.name, "size_bytes": t.size_bytes, "pushed_at": t.updated_at.isoformat()} for t in tags],
    }
