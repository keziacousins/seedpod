"""``seedpod/api/routers/snapshots.py`` -- Round 6, api-features component. Thin
HTTP over ``SnapshotService`` (``seedpod/app/services/snapshot_service.py``,
this same round's sibling component -- see its own module docstring for the
dump/restore mechanism, storage layout, and the restore-history-via-
``workflow_runs`` design DR-0020/this round's brief both name).

Salvaged request/response shapes from ``reference-code/seedpod/seedpod/api/
snapshots.py`` (``list_snapshots`` :268, ``get_snapshot`` :303, ``create_snapshot``
:333, ``restore_snapshot`` (background-task variant) :420ish, ``delete_snapshot``
:551), adapted to v2:

- ``GET /api/snapshots`` returns ``{"snapshots": [...]}`` (DR-0017's uniform
  collection envelope; v1 returned a bare array).
- ``POST /api/snapshots`` runs SYNCHRONOUSLY and returns the finished
  ``SnapshotRow`` with ``201`` -- v1's ``BackgroundTasks``+
  ``SnapshotOperation``-polling flow has no v2 background-task infra to run on
  (``SnapshotService``'s own module docstring); this is the honest, narrowed
  v2 shape, not a silently-dropped feature.
- ``GET /api/snapshots/clusters/{id}/restore-history`` reads FROM
  ``workflow_runs`` (this round's brief, citing the standing decision --
  ``snapshot_operations`` is deliberately NOT recreated,
  ``seedpod/data/repositories.py``'s ``SnapshotRow`` docstring). Registered
  under this router's own ``/snapshots`` prefix (``/snapshots/clusters/{id}/
  restore-history``, matching v1's own route shape byte-for-byte) rather than
  under ``clusters.py`` -- the read is entirely ``SnapshotService``'s
  (``workflow_runs`` rows THIS service wrote), so it lives beside its writer.
- ``POST /api/snapshots/{id}/restore`` broadcasts ``snapshot_restore_completed``
  (ui-contract obligation 5: "refetch restore history") over the hub AFTER the
  restore call returns -- OUTSIDE any ``uow`` (DR-0008; ``SnapshotService.
  restore`` itself never opens a transaction around the broadcast either), scoped
  to the target cluster's own environment (DR-0010 convention every other
  broadcast site in this tree already follows).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.app.services.cluster_service import ClusterNotFound
from seedpod.app.services.snapshot_service import (
    SnapshotCreationFailed,
    SnapshotIncompatible,
    SnapshotNotFound,
)
from seedpod.core.errors import InfrastructureUnreachableError
from seedpod.data.repositories import SnapshotRow, WorkflowRunRow

__all__ = ["router"]

router = APIRouter(prefix="/snapshots", tags=["snapshots"])


class CreateSnapshotRequest(BaseModel):
    cluster_id: str
    name: str
    description: str | None = None


class RestoreSnapshotRequest(BaseModel):
    cluster_id: str
    services: list[str] | None = None
    run_migrations: bool = True


def _serialize(row: SnapshotRow, *, detail: bool = False) -> dict[str, Any]:
    body = {
        "id": row.id,
        "name": row.name,
        "description": row.description,
        "source_cluster_id": row.source_cluster_id,
        "source_cluster_slug": row.source_cluster_slug,
        "branch": row.branch,
        "deployment_profile": row.deployment_profile,
        "services": [dict(s) for s in row.services],
        "total_size_bytes": row.total_size_bytes,
        "is_auto": row.is_auto,
        "created_by": row.created_by,
        "created_at": row.created_at.isoformat(),
    }
    if detail:
        body["storage_path"] = row.storage_path
    return body


def _serialize_history(row: WorkflowRunRow) -> dict[str, Any]:
    args = row.args or {}
    return {
        "id": row.id,
        "snapshot_name": args.get("snapshot_name"),
        "snapshot_id": args.get("snapshot_id"),
        "snapshot_branch": args.get("snapshot_branch"),
        "status": row.status,
        "services_completed": args.get("services_completed"),
        "services_total": args.get("services_total"),
        "initiated_by": row.initiated_by,
        "started_at": row.started_at.isoformat() if row.started_at else None,
    }


@router.get("")
async def list_snapshots(
    request: Request,
    branch: str | None = None,
    profile: str | None = None,
    _api_key=Depends(require_permission("snapshots:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    rows = await app.services.snapshots.list(branch=branch, profile=profile)
    return {"snapshots": [_serialize(r) for r in rows]}


@router.get("/clusters/{cluster_id}/restore-history")
async def restore_history(
    cluster_id: str, request: Request, _api_key=Depends(require_permission("snapshots:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    rows = await app.services.snapshots.restore_history(cluster_id)
    return {"restore_history": [_serialize_history(r) for r in rows]}


@router.get("/{snapshot_id}")
async def get_snapshot(
    snapshot_id: str, request: Request, _api_key=Depends(require_permission("snapshots:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.snapshots.get(snapshot_id)
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return _serialize(row, detail=True)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_snapshot(
    body: CreateSnapshotRequest, request: Request, api_key=Depends(require_permission("snapshots:create"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        cluster = await app.services.clusters.get(body.cluster_id)
    except ClusterNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    if cluster.status != "active":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"cannot snapshot cluster in {cluster.status!r} state; cluster must be active",
        )
    try:
        row = await app.services.snapshots.create(
            cluster, name=body.name, description=body.description, created_by=api_key.username,
        )
    except SnapshotCreationFailed as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return _serialize(row, detail=True)


@router.post("/{snapshot_id}/restore")
async def restore_snapshot(
    snapshot_id: str, body: RestoreSnapshotRequest, request: Request,
    api_key=Depends(require_permission("snapshots:create")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        cluster = await app.services.clusters.get(body.cluster_id)
    except ClusterNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    try:
        result = await app.services.snapshots.restore(
            snapshot_id, cluster_id=body.cluster_id, services=body.services,
            run_migrations=body.run_migrations, actor=f"api:{api_key.username}",
        )
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except SnapshotIncompatible as exc:
        # DR-0030 fix 2: a pre-flight incompatibility is now a RAISED
        # PermanentError, not folded into `RestoreResult.error` -- this
        # router (the OTHER caller of `SnapshotService.restore`, besides
        # `deploy.restore_snapshot`) must handle it explicitly too, or it
        # would surface as an unstructured 500 instead of a real 400 naming
        # the mismatch.
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except InfrastructureUnreachableError as exc:
        # DR-0030 fix 1: a restore that could not DETERMINE whether it
        # succeeded is not a restore that failed (CLAUDE.md's hard rule) --
        # map it to a 503 naming the cluster rather than letting it fall
        # through to an unhandled 500 with no structured body and no
        # `snapshot_restore_completed` broadcast.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"cluster {body.cluster_id} unreachable: {exc}",
        ) from exc

    app.hub.broadcast(
        "snapshot_restore_completed",
        {
            "cluster_id": body.cluster_id,
            "snapshot_id": snapshot_id,
            "status": "completed" if result.success else "failed",
            "services_restored": result.services_restored,
            "error": result.error,
        },
        cluster.environment,
    )
    return {
        "success": result.success,
        "services_restored": result.services_restored,
        "services_failed": result.services_failed,
        "error": result.error,
    }


@router.delete("/{snapshot_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_snapshot(
    snapshot_id: str, request: Request, _api_key=Depends(require_permission("snapshots:delete"))  # noqa: B008
) -> None:
    app = get_app(request)
    try:
        await app.services.snapshots.delete(snapshot_id)
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
