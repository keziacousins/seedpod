"""``seedpod/api/routers/clusters.py`` -- the cluster-facing router (Round 6,
api-clusters component). Thin HTTP over ``ClusterService`` (``seedpod/app/
services/cluster_service.py``, a previous, already-committed/tested component):
every handler here does request -> service call -> response shaping, never its
own ``Dispatcher.apply()``/``uow`` write. DR-0008 discipline: the only ``uow()``
blocks opened directly in THIS module are short, DB-only reads (the
``GET .../deployments``/``GET .../audit`` list queries) -- every state change
(destroy/extend/rehabilitate) and every provider-plane read (pods/pod
details/logs/events) already lives inside ``ClusterService`` itself, which the
DR-0008 module docstring and ``tests/app/test_services_cluster.py`` both pin as
uow-closed-before-IO; this router adds no transaction of its own around those
calls.

Salvaged request/response shapes from ``reference-code/seedpod/seedpod/api/
clusters.py`` (``list_clusters`` :119, ``get_cluster`` :195, ``extend_cluster_ttl``
:343, ``destroy_cluster`` :422, ``rehabilitate_cluster`` :875, ``get_cluster_pods``
:1202, ``get_pod_details`` :1303, ``get_pod_logs`` :1401, ``get_cluster_events``
:1497, ``get_cluster_deployments`` :1600, ``get_cluster_audit_history`` :1128),
adapted to v2 field names per ui-contract and DR-0002 ("we own the UI, so the SPA
adapts to the clean v2 contract -- no compatibility shims"):

- ``GET /api/clusters`` returns ``{"clusters": [...]}`` (v1 returned a bare
  ``list[ClusterResponse]``) -- DR-0017's uniform collection-envelope rule, and
  every other collection response here (``pods``/``events``/``deployments``/
  ``audit``) follows the SAME convention, matching this package's own committed
  siblings (``deployments.py``'s ``{"deployments": [...]}}``, ``workflows.py``'s
  ``{"workflows": [...]}}``, ``timers.py``'s ``{"timers": [...]}}``).
- ``environment`` (v1's overloaded managed/discovered *and* deployment-env field)
  splits per DR-0013: ``ClusterRow.origin`` (managed|discovered, the DELETE force
  gate's field -- ui-contract's "environment -> origin" rename) is surfaced
  ALONGSIDE the now-genuinely-deployment-scoped ``ClusterRow.environment``
  (ephemeral/staging/production), on both the list and detail responses.
- ``cluster_url``/``reconciliation_stale`` are NOT stored columns in v2 (Seam D's
  DDL promotes ``last_reconciled_at`` to a real column but drops the
  ``provider_config['last_reconciled_at']`` grab-bag v1 read from) -- both are
  derived here exactly as v1's ``ClusterRecord.cluster_url``/
  ``._is_reconciliation_stale`` did (``reference-code/seedpod/seedpod/data/
  models.py:81-100``): ``cluster_url = f"https://{dns_hostname}"`` when a hostname
  is set, and "stale" iff a reconciliation HAS happened but was more than 1800s
  (30 minutes -- v1's own "3x the typical 10-minute reconciliation interval")
  before ``now`` (the injected ``Clock``, never a wall-clock call -- CLAUDE.md).
  Never-reconciled is NOT stale (v1: "no reconciliation data yet - not stale,
  just unknown").
- ``GET /api/clusters/{id}/audit`` rows expose ``actor`` (ui-contract worklist 12:
  v2's ``cluster_state_audits`` has no ``trigger``/``initiated_by`` columns at all
  -- coherence-review Conflict 11 -- so there is nothing to DTO-map away; ``actor``
  is passed straight through).
- ``error_message``/``services`` -> ``failure_reason``/``resolved_images`` on the
  cluster-scoped deployments list, matching the top-level ``GET /api/deployments``
  router's identical rename.
- ``DELETE ...?force`` composes TWO independent guards, both enforced inside
  ``ClusterService.destroy`` (not re-implemented here): the machine's
  discovered-origin gate (``core/machine.py``'s ``transition()``, mapped from the
  resulting ``InvalidTransition`` to 409) and DR-0018's production-destroy gate
  (a managed ``environment == 'production'`` cluster without ``force=true``
  raises ``PermanentError(INVALID_INPUT)``, mapped via ``_http_error_for_
  provider_error`` to 400). A single ``force=true`` satisfies both.
- ``DELETE ...?snapshot_before_destroy`` (DR-0020): the real fail-open
  pre-destroy snapshot is api-features' component (the ``SnapshotService``
  collaborator it injects into ``ClusterService``) -- no snapshot capability
  exists anywhere in this tree yet (the ``snapshots`` DDL + read-only repository
  are the only committed snapshot surface -- ``seedpod/data/repositories.py``'s
  own module docstring). Because the SPA never reads the destroy response body
  (ui-contract row 34), silently accepting the flag and skipping the snapshot is
  indistinguishable from silent data loss -- so THIS component's
  ``ClusterService.destroy`` raises ``PermanentError(UNSUPPORTED)`` for
  ``snapshot_before_destroy=true``, which this router maps to ``501 Not
  Implemented`` -- never a silent ``{"snapshot": "skipped"}`` no-op. api-features
  replaces that branch with the real fail-open snapshot-then-destroy call.
- Permission scopes are the v2 catalog's actual granted set (``seedpod/api/
  permissions.py``'s ``AVAILABLE_PERMISSIONS`` -- there is no ``clusters:destroy``/
  ``clusters:modify``/``clusters:admin``/``clusters:logs`` scope in v2, v1's
  names): reads use ``clusters:read``, destroy uses ``clusters:delete``, extend
  uses the dedicated ``clusters:extend``, and rehabilitate -- a state UPDATE, not
  a destroy/extend -- uses ``clusters:update``.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.app.services.cluster_service import ClusterNotFound
from seedpod.core.errors import ErrorCode, ProviderError
from seedpod.core.machine import InvalidTransition
from seedpod.data.repositories import ClusterRow, ClusterStateAuditRow, DeploymentRow
from seedpod.providers.contract import PodDetailsResult
from seedpod.providers.kube_types import EventInfo, PodDetails, PodInfo

__all__ = ["router"]

router = APIRouter(tags=["clusters"])

# v1's threshold, salvaged verbatim (reference-code/seedpod/seedpod/data/models.py:98
# "Stale if older than 30 minutes (3x typical 10-min reconciliation interval)").
_STALE_THRESHOLD_SECONDS = 1800


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------


class ExtendTtlRequest(BaseModel):
    ttl_hours: float = Field(..., gt=0)


# ---------------------------------------------------------------------------
# Error mapping -- ProviderError (the one taxonomy home, CLAUDE.md) + the
# machine-layer sibling a Dispatcher.apply() call reachable from this module
# (destroy/rehabilitate) can raise.
# ---------------------------------------------------------------------------


def _http_error_for_provider_error(exc: ProviderError) -> HTTPException:
    if exc.code == ErrorCode.NOT_FOUND:
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if exc.code in (ErrorCode.AUTH, ErrorCode.ENDPOINT_UNREACHABLE, ErrorCode.API_TIMEOUT, ErrorCode.DAEMON_UNREACHABLE):
        # "cannot determine state" (crown jewel #1) -- never conflated with an
        # ordinary 4xx caller error; surfaced as a gateway failure.
        return HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc))
    if exc.code == ErrorCode.UNSUPPORTED:
        # DR-0020: ClusterService.destroy's snapshot_before_destroy=true interim
        # guard -- the capability genuinely isn't wired yet in this component.
        return HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=str(exc))
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


async def _provider_read(coro: Any) -> Any:
    """Every provider-plane read (pods/pod details/logs/events) funnels its error
    mapping through here: ``ClusterService``'s own ``_kubeconfig_for`` raises
    ``ClusterNotFound``/``ClusterHasNoKubeconfig`` (a plain ``LookupError``
    subclass and a ``PermanentError`` respectively -- both must map to 404), and
    a live kubectl failure raises some other ``ProviderError``."""
    try:
        return await coro
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ProviderError as exc:
        raise _http_error_for_provider_error(exc) from exc


# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------


def _cluster_url(row: ClusterRow) -> str | None:
    return f"https://{row.dns_hostname}" if row.dns_hostname else None


def _reconciliation_stale(row: ClusterRow, *, now) -> bool:
    if row.last_reconciled_at is None:
        return False  # no reconciliation data yet -- not stale, just unknown (v1 parity)
    return (now - row.last_reconciled_at).total_seconds() > _STALE_THRESHOLD_SECONDS


def _serialize_cluster(row: ClusterRow, *, now) -> dict[str, Any]:
    return {
        "id": row.id,
        "slug": row.slug,
        "name": row.name,
        "origin": row.origin.value,
        "environment": row.environment,
        "repository": row.repository,
        "branch": row.branch,
        "status": row.status,
        "provider": row.provider,
        "provider_config": dict(row.provider_config),
        "node_count": row.node_count,
        "public_ip": row.public_ip,
        "dns_hostname": row.dns_hostname,
        "cluster_url": _cluster_url(row),
        "cost_per_hour": row.cost_per_hour,
        "total_cost": row.total_cost,
        "failure_reason": row.failure_reason,
        "last_reconciled_at": row.last_reconciled_at.isoformat() if row.last_reconciled_at else None,
        "reconciliation_stale": _reconciliation_stale(row, now=now),
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "expires_at": row.expires_at.isoformat() if row.expires_at else None,
    }


def _serialize_audit(row: ClusterStateAuditRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "from_state": row.from_state,
        "to_state": row.to_state,
        "event": row.event,
        "actor": row.actor,
        "reason": row.reason,
        "timestamp": row.created_at.isoformat(),
    }


def _serialize_cluster_deployment(row: DeploymentRow) -> dict[str, Any]:
    return {
        "deployment_id": row.id,
        "cluster_id": row.cluster_id,
        "environment": row.environment,
        "status": row.status,
        "manifest_version": row.manifest_version,
        "resolved_images": dict(row.resolved_images),
        "superseded_by": row.superseded_by,
        "deployed_by": row.deployed_by,
        "failure_reason": row.failure_reason,
        "deployed_at": row.created_at.isoformat(),
    }


def _serialize_pod(pod: PodInfo) -> dict[str, Any]:
    return {
        "name": pod.name,
        "namespace": pod.namespace,
        "status": pod.status,
        "ready": pod.ready,
        "restarts": pod.restarts,
        "age": pod.age,
        "created": pod.created,
        "node": pod.node,
        "ip": pod.ip,
        "image": pod.image,
    }


def _serialize_pod_details(pod: PodDetails) -> dict[str, Any]:
    return {
        "name": pod.name,
        "namespace": pod.namespace,
        "status": pod.status,
        "age": pod.age,
        "created": pod.created,
        "node": pod.node,
        "ip": pod.ip,
        "hostIP": pod.host_ip,
        "labels": pod.labels,
        "annotations": pod.annotations,
        "conditions": pod.conditions,
        "initContainers": pod.init_containers,
        "containers": pod.containers,
        "volumes": pod.volumes,
    }


def _serialize_event(event: EventInfo) -> dict[str, Any]:
    return {
        "namespace": event.namespace,
        "name": event.name,
        "type": event.type,
        "reason": event.reason,
        "message": event.message,
        "involved_object_kind": event.involved_object_kind,
        "involved_object_name": event.involved_object_name,
        "count": event.count,
        "first_timestamp": event.first_timestamp,
        "last_timestamp": event.last_timestamp,
        "source_component": event.source_component,
    }


# ---------------------------------------------------------------------------
# CRUD + lifecycle
# ---------------------------------------------------------------------------


@router.get("/clusters")
async def list_clusters(
    request: Request,
    show_destroyed: bool = False,
    status: str | None = None,  # noqa: A002 -- ui-contract's `?status=active`, not the fastapi module
    _api_key=Depends(require_permission("clusters:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    rows = await app.services.clusters.list(show_destroyed=show_destroyed, status=status)
    now = app.clock.now()
    return {"clusters": [_serialize_cluster(r, now=now) for r in rows]}


@router.get("/clusters/{cluster_id}")
async def get_cluster(
    cluster_id: str, request: Request, _api_key=Depends(require_permission("clusters:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.clusters.get(cluster_id)
    except ClusterNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return _serialize_cluster(row, now=app.clock.now())


@router.delete("/clusters/{cluster_id}")
async def destroy_cluster(
    cluster_id: str,
    request: Request,
    force: bool = False,
    snapshot_before_destroy: bool = False,
    api_key=Depends(require_permission("clusters:delete")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.clusters.destroy(
            cluster_id,
            actor=f"api:{api_key.username}",
            force=force,
            snapshot_before_destroy=snapshot_before_destroy,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except InvalidTransition as exc:
        # e.g. a discovered-origin cluster destroyed without force=true -- module
        # docstring: the machine's own guard, threaded through unmodified.
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ProviderError as exc:
        # DR-0018: a managed production cluster destroyed without force=true --
        # ClusterService.destroy's own PermanentError(INVALID_INPUT) -> 400.
        # DR-0020: snapshot_before_destroy=true with no snapshot capability --
        # PermanentError(UNSUPPORTED) -> 501.
        raise _http_error_for_provider_error(exc) from exc
    return {
        "cluster_id": row.id,
        "status": row.status,
        "message": "cluster destruction initiated",
    }


@router.post("/clusters/{cluster_id}/extend")
async def extend_cluster(
    cluster_id: str,
    body: ExtendTtlRequest,
    request: Request,
    api_key=Depends(require_permission("clusters:extend")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.clusters.extend(cluster_id, ttl_hours=body.ttl_hours, actor=f"api:{api_key.username}")
    except ClusterNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ProviderError as exc:
        raise _http_error_for_provider_error(exc) from exc
    return {
        "cluster_id": row.id,
        "status": row.status,
        "expires_at": row.expires_at.isoformat() if row.expires_at else None,
    }


@router.post("/clusters/{cluster_id}/rehabilitate")
async def rehabilitate_cluster(
    cluster_id: str, request: Request, api_key=Depends(require_permission("clusters:update"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        row = await app.services.clusters.rehabilitate(cluster_id, actor=f"api:{api_key.username}")
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except InvalidTransition as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return {"cluster_id": row.id, "status": row.status}


# ---------------------------------------------------------------------------
# Provider-plane reads -- OUTSIDE any uow (DR-0008; ClusterService's own job)
# ---------------------------------------------------------------------------


@router.get("/clusters/{cluster_id}/pods")
async def list_pods(
    cluster_id: str,
    request: Request,
    namespace: str | None = None,
    _api_key=Depends(require_permission("clusters:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    pods = await _provider_read(app.services.clusters.pods(cluster_id, namespace=namespace))
    return {"pods": [_serialize_pod(p) for p in pods]}


@router.get("/clusters/{cluster_id}/pods/{namespace}/{pod_name}")
async def get_pod(
    cluster_id: str,
    namespace: str,
    pod_name: str,
    request: Request,
    _api_key=Depends(require_permission("clusters:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    result: PodDetailsResult = await _provider_read(
        app.services.clusters.pod_details(cluster_id, namespace, pod_name)
    )
    if not result.found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"pod {pod_name!r} not found in namespace {namespace!r}",
        )
    assert result.details is not None
    return {"pod": _serialize_pod_details(result.details)}


@router.get("/clusters/{cluster_id}/pods/{namespace}/{pod_name}/logs")
async def get_pod_logs(
    cluster_id: str,
    namespace: str,
    pod_name: str,
    request: Request,
    container: str | None = None,
    tail_lines: int = 100,
    previous: bool = False,
    _api_key=Depends(require_permission("clusters:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    logs = await _provider_read(
        app.services.clusters.pod_logs(
            cluster_id, namespace, pod_name, container=container, tail_lines=tail_lines, previous=previous
        )
    )
    return {"logs": logs}


@router.get("/clusters/{cluster_id}/events")
async def list_cluster_events(
    cluster_id: str,
    request: Request,
    namespace: str | None = None,
    limit: int = 200,
    _api_key=Depends(require_permission("clusters:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    events = await _provider_read(app.services.clusters.events(cluster_id, namespace=namespace, limit=limit))
    return {"events": [_serialize_event(e) for e in events]}


# ---------------------------------------------------------------------------
# DB-only lists -- short uow, no provider IO (DR-0008)
# ---------------------------------------------------------------------------


@router.get("/clusters/{cluster_id}/deployments")
async def list_cluster_deployments(
    cluster_id: str, request: Request, _api_key=Depends(require_permission("clusters:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        await app.services.clusters.get(cluster_id)
    except ClusterNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    async with app.uow() as tx:
        rows = app.repos.deployments.list_for_cluster(tx, cluster_id)
    return {"deployments": [_serialize_cluster_deployment(r) for r in rows]}


@router.get("/clusters/{cluster_id}/audit")
async def cluster_audit(
    cluster_id: str,
    request: Request,
    limit: int = 50,
    _api_key=Depends(require_permission("clusters:read")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    try:
        await app.services.clusters.get(cluster_id)
    except ClusterNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    async with app.uow() as tx:
        rows = app.repos.cluster_state_audits.list_for_cluster(tx, cluster_id, limit=limit)
    return {"audit": [_serialize_audit(r) for r in rows]}
