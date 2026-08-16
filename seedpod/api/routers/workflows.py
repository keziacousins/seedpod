"""``GET /api/workflows`` -- the DR-0003 replacement for v1's retired ``GET
/api/jobs`` (ui-contract §4: "Entire surface is GONE -> rebuild on
``/api/workflows``"). Read-only: ``workflow_runs`` rows, newest first
(``WorkflowRunRepository.list_all``).

Field set is exactly ui-contract obligation 7 / DR-0003's inventory: ``id, workflow,
cluster_id, deployment_id, status, failed_step, error, undo_incomplete, created_at,
started_at, finished_at`` -- a clean DTO (DR-0002: "we own the UI, so the SPA adapts
to the clean v2 contract"), not the full ``WorkflowRunRow`` (which also carries
``workflow_version``/``dedupe_key``/``args``/``cancel_requested``/``initiated_by``,
none of which ui-contract's column map for the Workflows-page rewrite names)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.data.repositories import WorkflowRunRow

__all__ = ["router"]

router = APIRouter(tags=["workflows"])


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _serialize(row: WorkflowRunRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "workflow": row.workflow,
        "cluster_id": row.cluster_id,
        "deployment_id": row.deployment_id,
        "status": row.status,
        "failed_step": row.failed_step,
        "error": row.error,
        "undo_incomplete": row.undo_incomplete,
        "created_at": _iso(row.created_at),
        "started_at": _iso(row.started_at),
        "finished_at": _iso(row.finished_at),
    }


@router.get("/workflows")
async def list_workflows(
    request: Request, _api_key=Depends(require_permission("workflows:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    async with app.uow() as t:
        rows = app.repos.workflow_runs.list_all(t)
    return {"workflows": [_serialize(r) for r in rows]}
