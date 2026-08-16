"""The deployment-facing routers -- the parity-gate spine (Round 6, api-deployments
component). Thin HTTP over ``DeploymentService`` (``seedpod/app/services/
deployment_service.py``, a previous, already-committed/tested component): every
handler below does request -> service call -> response shaping, never its own
Dispatcher.apply()/uow write. DR-0008 discipline: the only ``uow()`` opens here are
short, DB-only reads (``GET /api/deployments*``); every write and every provider/
GHCR/manifest-resolution IO path already lives inside ``DeploymentService`` itself,
never straddled by a transaction opened in this module.

Salvaged request/response shapes + decision->status mapping from
``reference-code/seedpod/seedpod/api/deployments.py`` (``version_update`` :237,
``list_deployments`` :441, ``get_deployment_details`` :527, ``deployment_preview``
:691, ``redeploy_deployment`` :910, ``cancel_deployment`` :992, ``retrigger_deployment``
:1182), adapted to v2 field names per ui-contract (``error_message`` ->
``failure_reason``, ``services`` -> ``resolved_images``, new ``spec_ref``/
``superseded_by``) and DR-0002 ("we own the UI, so the SPA adapts to the clean v2
contract -- no compatibility shims"):

- ``GET /api/deployments`` returns ``{"deployments": [...]}`` (v1 returned a bare
  array) -- matches this package's own committed convention
  (``seedpod/api/routers/workflows.py``'s ``{"workflows": [...]}}``,
  ``timers.py``'s ``{"timers": [...]}}``), not v1's shape.
- Per DR-0016, every deployment DTO here (list summary and detail) exposes the row's
  ``created_at`` column under the response KEY ``deployed_at`` (ui-contract's field
  name is unchanged; v1's own ``deployed_at`` was stamped at row creation, never at
  deploy-completion, so this is a faithful rename, not a semantic substitution).
- ``POST /api/deployment-preview`` returns the preview fields FLAT at the top level
  (v1 nested them under a ``"preview"`` key beside ``status``/``correlation_id``) --
  the parity gate's own assertion (``preview_data["status"] == "success"``) only
  needs ``status`` at the top, and ``DeploymentPreviewResponse`` is already a clean
  DTO with no v1 wrapper to preserve.
- ``POST /api/version-update/preview`` is wired as a plain ALIAS of
  ``POST /api/deployment-preview`` (same handler, same request shape --
  ``deployment_profile_name``/``triggering_repo``/``triggering_branch``/
  ``triggering_image``), not v1's DISTINCT webhook-shaped preview (rule evaluation +
  ``ClusterSpecification`` summary, reference-code :135-234). ``DeploymentService``
  exposes exactly one non-persisting preview method
  (``deployment_preview()`` -- its own docstring: "Mirrors ``version_update``'s
  manifest-resolution half without persisting anything"), and this round's brief
  itself names both routes together as "the non-persisting mirror" of the same
  parity assertion (``test_deployment_preview_to_actual_deployment`` only ever
  calls ``/api/deployment-preview``) -- v1's rule-evaluation-preview responsibility
  (``core/cluster_spec.py``'s ``ClusterSpecification`` synthesis) is out of scope
  per ``DeploymentService``'s own documented scope-narrowing, so there is no second
  service method to wrap a second shape around.
- Rule enable/disable/reload (``POST /api/rules/{name}/disable|enable|reload``) are
  genuinely new-in-v2 -- see ``seedpod/app/services/rules_admin.py``'s module
  docstring for why v1 never actually shipped a working disable/enable pair despite
  the parity gate exercising the route names.
- ``DeploymentPreviewRequest.environment`` (Round 9, **DR-0027**, docs/decisions/
  DR-0027-secret-scope-is-the-rule-derived-environment.md) is a new, OPTIONAL
  field threaded straight through to ``DeploymentService.deployment_preview``'s
  own optional ``environment=`` parameter -- purely additive, so it does not
  change this endpoint's existing request contract for any caller that omits
  it (that method's own docstring has the exact-vs-approximate reasoning).
  ``_deployment_preview``'s existing ``except ProviderError`` wrapper already
  maps the manifest-resolution ``PermanentError(INVALID_INPUT)`` this can
  raise to a 4xx (DR-0026 part 2) -- no new exception handling needed here.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.app.services import rules_admin
from seedpod.app.services.deployment_service import (
    DeploymentNotFound,
    DeploymentPreviewResponse,
    DeploymentResponse,
)
from seedpod.core.errors import ErrorCode, ProviderError
from seedpod.core.machine import InvalidTransition, StaleVersion
from seedpod.data.repositories import DeploymentAuditRepository, DeploymentAuditRow, DeploymentRow
from seedpod.services.rules import RuleValidationError

__all__ = ["router"]

router = APIRouter(tags=["deployments"])


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------


class VersionUpdateRequest(BaseModel):
    repo: str
    branch: str
    image: str
    commit: str
    tag: str | None = None


class DeploymentPreviewRequest(BaseModel):
    deployment_profile_name: str
    triggering_repo: str
    triggering_branch: str
    triggering_image: str
    commit_sha: str | None = None
    # DR-0027 (docs/decisions/DR-0027-secret-scope-is-the-rule-derived-
    # environment.md): optional -- omitting it keeps every existing caller's
    # behavior unchanged (falls back to the profile's own environment_type).
    # When supplied, secrets are scoped to it EXACTLY, same as a real
    # deployment recorded under that environment would be.
    environment: str | None = None


class CancelRequest(BaseModel):
    reason: str = ""


# ---------------------------------------------------------------------------
# Error mapping -- ProviderError (the one taxonomy home, CLAUDE.md) and the
# machine-layer siblings (seedpod/core/machine.py) that a Dispatcher.apply() call
# reachable from this module (redeploy/retrigger/cancel) can raise, mapped to
# the HTTP status their own docstrings already name.
# ---------------------------------------------------------------------------

_NOT_FOUND_CODES = frozenset({ErrorCode.NOT_FOUND})


def _http_error_for_provider_error(exc: ProviderError) -> HTTPException:
    if exc.code in _NOT_FOUND_CODES:
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if exc.code in (ErrorCode.AUTH,):
        return HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc))
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


def _serialize_deployment_response(result: DeploymentResponse) -> dict[str, Any]:
    return {
        "deployment_id": result.deployment_id,
        "cluster_id": result.cluster_id,
        "status": result.status,
        "message": result.message,
        "environment": result.environment,
        "provider": result.provider,  # DR-0046 decision 4
    }


def _serialize_preview_response(result: DeploymentPreviewResponse) -> dict[str, Any]:
    return {
        "status": result.status,
        "deployment_profile": result.deployment_profile,
        "triggering_repo": result.triggering_repo,
        "triggering_branch": result.triggering_branch,
        "triggering_image": result.triggering_image,
        "resolution_strategy": result.resolution_strategy,
        "resolved_images": dict(result.resolved_images),
        "registry_queries": [dict(q) for q in result.registry_queries],
        "template_files": list(result.template_files),
    }


def _serialize_deployment_summary(row: DeploymentRow) -> dict[str, Any]:
    """``GET /api/deployments`` list shape (ui-contract): ``deployment_id,
    cluster_id, manifest_version, status, deployed_by, deployed_at`` -- per
    DR-0016, the response KEY is ``deployed_at`` (ui-contract's unchanged field
    name), sourced verbatim from the row's ``created_at`` (v1's own
    ``deployed_at`` was stamped at row creation, never at deploy-completion)."""
    return {
        "deployment_id": row.id,
        "cluster_id": row.cluster_id,
        "environment": row.environment,
        "manifest_version": row.manifest_version,
        "status": row.status,
        "deployed_by": row.deployed_by,
        "deployed_at": row.created_at.isoformat(),
    }


def _serialize_audit(audit: DeploymentAuditRow) -> dict[str, Any]:
    return {
        "id": audit.id,
        "triggering_repo": audit.triggering_repo,
        "triggering_branch": audit.triggering_branch,
        "triggering_image": audit.triggering_image,
        "commit_sha": audit.commit_sha,
        "deployment_profile_name": audit.deployment_profile_name,
        "resolution_strategy": audit.resolution_strategy,
        "resolved_images": dict(audit.resolved_images),
        "created_at": audit.created_at.isoformat(),
    }


def _serialize_deployment_detail(row: DeploymentRow, audits: list[DeploymentAuditRow]) -> dict[str, Any]:
    """``GET /api/deployments/{id}`` -- the full row (ui-contract: ``failure_reason``
    not ``error_message``; ``resolved_images`` not ``services``; ``spec_ref``/
    ``superseded_by`` newly surfaced) plus ``audit_history`` (v1 :570-583, filtered
    by ``deployment_id`` there; v2's audit rows never carry a live
    ``deployment_id`` back-reference -- ``DeploymentService``'s own module
    docstring/``_audit_row`` -- so this resolves the SAME audit through the
    deployment's own ``spec_ref`` instead, a strictly equivalent 0-or-1-row
    "history" for a deployment that has never been redeployed)."""
    return {
        "deployment_id": row.id,
        "cluster_id": row.cluster_id,
        "environment": row.environment,
        "status": row.status,
        "manifest_version": row.manifest_version,
        "spec_ref": row.spec_ref,
        "resolved_images": dict(row.resolved_images),
        "superseded_by": row.superseded_by,
        "deployed_by": row.deployed_by,
        "failure_reason": row.failure_reason,
        "deployed_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "audit_history": [_serialize_audit(a) for a in audits],
    }


# ---------------------------------------------------------------------------
# The parity spine
# ---------------------------------------------------------------------------


@router.post("/version-update")
async def version_update(
    body: VersionUpdateRequest,
    request: Request,
    api_key=Depends(require_permission("deployments:trigger")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    result = await app.services.deployments.version_update(
        repo=body.repo, branch=body.branch, image=body.image, commit=body.commit,
        tag=body.tag, actor=f"api:{api_key.username}",
    )
    return _serialize_deployment_response(result)


async def _deployment_preview(body: DeploymentPreviewRequest, request: Request) -> dict[str, Any]:
    app = get_app(request)
    try:
        result = await app.services.deployments.deployment_preview(
            deployment_profile_name=body.deployment_profile_name,
            triggering_repo=body.triggering_repo,
            triggering_branch=body.triggering_branch,
            triggering_image=body.triggering_image,
            commit_sha=body.commit_sha,
            environment=body.environment,
        )
    except ProviderError as exc:
        raise _http_error_for_provider_error(exc) from exc
    return _serialize_preview_response(result)


@router.post("/deployment-preview")
async def deployment_preview(
    body: DeploymentPreviewRequest,
    request: Request,
    _api_key=Depends(require_permission("deployments:read")),  # noqa: B008
) -> dict[str, Any]:
    return await _deployment_preview(body, request)


@router.post("/version-update/preview")
async def version_update_preview(
    body: DeploymentPreviewRequest,
    request: Request,
    _api_key=Depends(require_permission("deployments:read")),  # noqa: B008
) -> dict[str, Any]:
    """Alias of ``POST /api/deployment-preview`` -- module docstring."""
    return await _deployment_preview(body, request)


# ---------------------------------------------------------------------------
# Rule admin -- seedpod/app/services/rules_admin.py
# ---------------------------------------------------------------------------


@router.post("/rules/{name}/disable")
async def disable_rule(
    name: str, request: Request, _api_key=Depends(require_permission("config:reload"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    if not rules_admin.set_rule_enabled(app.rules, name, enabled=False):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"rule {name!r} not found")
    return {"status": "disabled", "rule": name}


@router.post("/rules/{name}/enable")
async def enable_rule(
    name: str, request: Request, _api_key=Depends(require_permission("config:reload"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    if not rules_admin.set_rule_enabled(app.rules, name, enabled=True):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"rule {name!r} not found")
    return {"status": "enabled", "rule": name}


@router.post("/rules/reload")
async def reload_rules(
    request: Request, _api_key=Depends(require_permission("config:reload"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        rules_admin.reload_rules(app.rules, app.config.config_dir / "deployment-rules.yml")
    except RuleValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"status": "reloaded", "summary": rules_admin.rules_summary(app.rules)}


@router.post("/deployment-profiles/reload")
async def reload_deployment_profiles(
    request: Request, _api_key=Depends(require_permission("config:reload"))  # noqa: B008
) -> dict[str, Any]:
    """``ManifestResolver``/``load_deployment_profile`` read every profile fresh
    from disk on every call (no in-process cache to invalidate -- ``seedpod/app/
    services/profiles.py``'s own docstring), so there is nothing to actually
    reload; this just confirms the directory is readable and reports how many
    profiles are on disk, matching ui-contract's ``status, deployment_profiles_
    count`` shape."""
    app = get_app(request)
    profiles_dir = app.config.config_dir / "deployment-profiles"
    count = len(list(profiles_dir.glob("*.yml"))) if profiles_dir.exists() else 0
    return {"status": "reloaded", "deployment_profiles_count": count}


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


@router.get("/deployments")
async def list_deployments(
    request: Request,
    cluster_id: str | None = None,
    show_history: bool = False,
    _api_key=Depends(require_permission("deployments:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    rows = await app.services.deployments.list(cluster_id=cluster_id, show_history=show_history)
    return {"deployments": [_serialize_deployment_summary(r) for r in rows]}


@router.get("/deployments/{deployment_id}")
async def get_deployment(
    deployment_id: str, request: Request, _api_key=Depends(require_permission("deployments:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.deployments.get(deployment_id)
    except DeploymentNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    audits: list[DeploymentAuditRow] = []
    if row.spec_ref is not None:
        # DeploymentAuditRepository isn't in the Dispatcher-facing `Repositories`
        # bundle (that dataclass's own docstring); built ad hoc from `app.crypto`,
        # the same pattern `seedpod/api/routers/health.py` already uses for
        # `ApiKeyRepository()`. Short DB-only read, no provider/GHCR IO (DR-0008).
        deployment_audits = DeploymentAuditRepository(app.crypto)
        async with app.uow() as t:
            audit = deployment_audits.get(t, row.spec_ref)
        if audit is not None:
            audits = [audit]

    return _serialize_deployment_detail(row, audits)


@router.post("/deployments/{deployment_id}/redeploy")
async def redeploy_deployment(
    deployment_id: str, request: Request, api_key=Depends(require_permission("deployments:create"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        result = await app.services.deployments.redeploy(deployment_id, actor=f"api:{api_key.username}")
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except (InvalidTransition, StaleVersion) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return _serialize_deployment_response(result)


@router.post("/deployments/{deployment_id}/retrigger")
async def retrigger_deployment(
    deployment_id: str, request: Request, api_key=Depends(require_permission("deployments:create"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        result = await app.services.deployments.retrigger(deployment_id, actor=f"api:{api_key.username}")
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except (InvalidTransition, StaleVersion) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    body = _serialize_deployment_response(result)
    body["new_deployment_id"] = result.deployment_id  # ui-contract: DeploymentDetail.jsx:138
    body["original_deployment_id"] = deployment_id
    return body


@router.post("/deployments/{deployment_id}/cancel")
async def cancel_deployment(
    deployment_id: str,
    request: Request,
    body: CancelRequest | None = None,
    api_key=Depends(require_permission("deployments:update")),  # noqa: B008
) -> dict[str, Any]:
    """ui-contract: "Cancel no longer touches cluster state" -- a thin wrapper
    over ``DeploymentService.cancel``, which is itself already exactly that
    (its own docstring). ``body`` is optional (v1's ``DeploymentDetail.jsx:160``
    reads no response fields and sends no body) -- a bodyless ``POST`` must not
    422."""
    app = get_app(request)
    reason = body.reason if body is not None else ""
    try:
        row = await app.services.deployments.cancel(
            deployment_id, actor=f"api:{api_key.username}", reason=reason
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except InvalidTransition as exc:
        # seedpod/core/machine.py's own docstring: "From api:* actors this is the
        # caller's 409" -- e.g. cancelling an already-terminal deployment.
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return {"status": "cancelled", "deployment_id": row.id}
