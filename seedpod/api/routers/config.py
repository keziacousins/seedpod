"""``seedpod/api/routers/config.py`` -- Round 6, api-features component.
Read-only "configuration browser" over the loaded ``RuleEngine`` + on-disk
deployment-profile/resolution-strategy config (ui-contract: "GET /api/config/*"
must survive).

Salvaged request/response shapes from ``reference-code/seedpod/seedpod/api/
config.py`` (``get_config_overview`` :22, ``get_deployment_rules`` :69,
``list_deployment_profiles`` :123, ``get_deployment_profile`` :148,
``get_resolution_strategies`` :180, ``get_resolution_strategy`` :213,
``get_providers`` :263), adapted to v2:

- Every route reuses the existing ``config:read`` scope (v1 parity).
- ``GET /api/config/overview``'s ``rules{...}`` sub-object is
  ``rules_admin.rules_summary(app.rules)`` verbatim -- the SAME formatter
  ``POST /api/rules/reload``'s response already uses
  (``seedpod/app/services/rules_admin.py``'s own docstring: "reused verbatim...
  same concept, same caller-facing shape, one formatter"), not a second,
  drifting implementation of the same summary.
- ``GET /api/config/rules`` (the FULL rule configuration, every field --
  distinct from ``/overview``'s summary) reads ``app.rules.config`` directly
  (the frozen ``RuleConfig``/``Rule`` dataclasses ``seedpod/services/rules.py``
  already exposes as a public, live attribute -- ``rules_admin.py``'s own
  precedent for reading/mutating it from the outside).
- ``GET /api/config/deployment-profiles``/``/{name}``/``/resolution-strategies``/
  ``/{name}`` are plain disk reads over ``config_dir`` -- the same lightweight-
  read-directly-in-the-router precedent this package's own
  ``deployments.py::reload_deployment_profiles`` already set (that handler's own
  docstring: "no in-process cache to invalidate... this just confirms the
  directory is readable"), since ``ManifestResolver``/``load_deployment_profile``
  deliberately do not load ``resolution-strategies.yml`` at all (out of scope,
  ``seedpod/services/manifests.py``'s own docstring) and only load ONE profile at
  a time (no "list every profile" surface to reuse).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, Depends, HTTPException, Request, status

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.app.services import rules_admin
from seedpod.app.services.profiles import SUPPORTED_RESOLUTION_STRATEGIES

__all__ = ["router"]

router = APIRouter(prefix="/config", tags=["configuration"])


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


def _serialize_rule(rule) -> dict[str, Any]:
    return {
        "name": rule.name,
        "description": rule.description,
        "enabled": rule.enabled,
        "branch_patterns": list(rule.branch_patterns),
        "repo_patterns": list(rule.repo_patterns),
        "tag_pattern": rule.tag_pattern,
        "action": rule.action,
        "config": dict(rule.config),
    }


def _strategy_dict(name: str, raw: dict[str, Any]) -> dict[str, Any]:
    """``supported`` (DR-0037 decision 3) tells the caller whether the ENGINE will
    honour this strategy, as opposed to whether the file describes it.

    v2 implements exactly one. The others are documented intent -- and until this
    flag existed the API advertised all four identically, so a profile set to
    ``strict_branch`` ("no fallbacks, fail if not found") read as available and
    silently got full fallback behaviour instead. That is backlog #24's shape, and
    the pair of "declare it here / refuse to load it in `load_deployment_profile`"
    is what closes it: the list stays honest documentation, and a profile cannot
    quietly get the wrong behaviour from it.

    ``require_triggering_repo`` is echoed from the file for parity with v1's own
    response shape; v1 defined the field (``manifest_resolver.py``:176) and never
    read it anywhere, so it describes nothing in either system."""
    return {
        "name": name,
        "description": raw.get("description", ""),
        "explanation": raw.get("explanation", ""),
        "fallback_branches": list(raw.get("fallback_branches") or []),
        "require_triggering_repo": bool(raw.get("require_triggering_repo", True)),
        "allow_external_fallback": bool(raw.get("allow_external_fallback", True)),
        "supported": name in SUPPORTED_RESOLUTION_STRATEGIES,
    }


def _load_strategies(config_dir: Path) -> dict[str, dict[str, Any]]:
    raw = _load_yaml(config_dir / "resolution-strategies.yml")
    strategies = raw.get("strategies") or {}
    return {name: _strategy_dict(name, cfg) for name, cfg in strategies.items() if isinstance(cfg, dict)}


def _profile_names(config_dir: Path) -> list[str]:
    profiles_dir = config_dir / "deployment-profiles"
    if not profiles_dir.exists():
        return []
    return sorted(p.stem for p in profiles_dir.glob("*.yml"))


def _profile_summary(name: str, raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": raw.get("version", "1.0"),
        "description": raw.get("description", ""),
        "services": list((raw.get("services") or {}).keys()),
        "environment_type": raw.get("environment_type"),
        "resolution_strategy": raw.get("resolution_strategy"),
    }


def _load_profiles(config_dir: Path) -> dict[str, dict[str, Any]]:
    profiles_dir = config_dir / "deployment-profiles"
    return {
        name: _profile_summary(name, _load_yaml(profiles_dir / f"{name}.yml"))
        for name in _profile_names(config_dir)
    }


@router.get("/overview")
async def config_overview(
    request: Request, _api_key=Depends(require_permission("config:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    profiles = _profile_names(app.config.config_dir)
    strategies = _load_strategies(app.config.config_dir)
    return {
        "rules": rules_admin.rules_summary(app.rules),
        "deployment_profiles": {"total": len(profiles), "profiles": profiles},
        "resolution_strategies": {
            "total": len(strategies), "strategies": list(strategies.keys()),
            "default": "branch_discovery_with_fallback",
        },
    }


@router.get("/rules")
async def get_rules(
    request: Request, _api_key=Depends(require_permission("config:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    cfg = app.rules.config
    return {
        "status": "loaded",
        "version": cfg.version,
        "global_ephemeral_enabled": cfg.global_ephemeral_enabled,
        "default_ttl_hours": cfg.default_ttl_hours,
        "defaults": dict(cfg.defaults),
        "rules": [_serialize_rule(r) for r in cfg.rules],
        "valid_actions": list(cfg.valid_actions),
        "valid_environments": list(cfg.valid_environments),
    }


@router.get("/deployment-profiles")
async def list_deployment_profiles(
    request: Request, _api_key=Depends(require_permission("config:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    profiles = _load_profiles(app.config.config_dir)
    return {"status": "success", "count": len(profiles), "deployment_profiles": profiles}


@router.get("/deployment-profiles/{profile_name}")
async def get_deployment_profile(
    profile_name: str, request: Request, _api_key=Depends(require_permission("config:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    path = app.config.config_dir / "deployment-profiles" / f"{profile_name}.yml"
    if not path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"profile {profile_name!r} not found")
    return {"status": "success", "profile_name": profile_name, "config": _load_yaml(path)}


@router.get("/resolution-strategies")
async def list_resolution_strategies(
    request: Request, _api_key=Depends(require_permission("config:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    strategies = _load_strategies(app.config.config_dir)
    return {"status": "success", "count": len(strategies), "strategies": strategies}


@router.get("/resolution-strategies/{strategy_name}")
async def get_resolution_strategy(
    strategy_name: str, request: Request, _api_key=Depends(require_permission("config:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    strategies = _load_strategies(app.config.config_dir)
    if strategy_name not in strategies:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"resolution strategy {strategy_name!r} not found"
        )
    return {"status": "success", "strategy": strategies[strategy_name]}


@router.get("/providers")
async def get_providers(
    request: Request, _api_key=Depends(require_permission("config:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    known = sorted({p.stem for p in (app.config.config_dir / "providers").glob("*.yml")} | set(app.providers.keys()))
    return {
        "providers": {name: {"enabled": name in app.providers} for name in known},
        "enabled_providers": sorted(app.providers.keys()),
    }
