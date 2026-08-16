"""``GET /health`` (basic) and ``GET /health/detailed`` (ui-contract obligation 7,
docs/decisions/DR-0003) -- both public: neither route below declares a
``require_permission(...)`` dependency, which is the whole story (no
``PermissionEnforcementMiddleware``/allowlist governs this -- ``seedpod/api/
factory.py``'s own docstring documents that backstop as tried and deliberately
dropped; public-ness here is purely the absence of an auth dependency on these
two routes). Mounted at the ROOT path (no ``/api`` prefix -- the acceptance gate
hits ``GET /health`` bare: ``tests/acceptance/test_deployment_flow.py``).

Salvaged from ``reference-code/seedpod/seedpod/api/health.py``'s ``health_check``/
``detailed_health_check`` (the basic-check shape; the detailed check's
``database``/counts pattern) -- v1's ``/health/ready``/``/health/live`` and its
Postgres-vs-SQLite branching are NOT ported (v2 is SQLite-only, Seam D; no
readiness/liveness split is named anywhere in ui-contract). v1's
``background_scheduler``/``reconciliation`` blocks are REPLACED wholesale (DR-0003:
"``/health/detailed`` replaces the ``scheduler`` block with three engine-truth
blocks, ``database``/``reconciler`` unchanged") by ``executor``/``timers``/``engine``,
read from the live Round-4/Round-5 runtime components (``app.executor``/
``app.timers``/``app.repos.workflow_runs``), never from a v1-shaped scheduler
introspection that no longer exists.

**DR-0008 discipline.** All six counts (``database``'s three, ``executor``'s two,
``engine``'s one -- ``ACTIVE_RUN_STATUSES`` rows) are read in ONE short DB-only
transaction, closed BEFORE this handler touches ``app.executor.running``/
``app.timers.running``/``app.timers.next_fire_at()``/
``app.services.reconciliation.running``/``.last_sync()`` -- none of which is a
database read (each is a live Python attribute/method on an already-running
in-process component); no provider IO, no broadcast, ever runs with that
transaction still open.

``timestamp`` on both endpoints comes from ``app.clock.now()`` (CLAUDE.md: "every
timestamp via the injected Clock, no now()") -- never ``datetime.now(UTC)``."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from seedpod.api.deps import get_app
from seedpod.data.repositories import ACTIVE_RUN_STATUSES, ApiKeyRepository

__all__ = ["router"]

router = APIRouter(tags=["health"])

_SERVICE_NAME = "seedpod"
_SERVICE_VERSION = "2.0.0"

# Stateless -- session-in, DTO-out, no constructor args (matches every other ad hoc
# repository instantiation already committed, e.g. `WorkflowStepRepository()` in
# `seedpod/app/factory.py`'s engine construction). `Repositories` (the
# Dispatcher-facing bundle) deliberately does not carry `api_keys` -- see that
# dataclass's own docstring -- so this handler builds its own accessor, the same
# way `seedpod/app/factory.py` does for `ApiKeyService`.
_api_keys_repo = ApiKeyRepository()


@router.get("/health")
async def health(request: Request) -> dict[str, Any]:
    app = get_app(request)
    return {
        "status": "healthy",
        "service": _SERVICE_NAME,
        "version": _SERVICE_VERSION,
        "timestamp": app.clock.now().isoformat(),
    }


@router.get("/health/detailed")
async def health_detailed(request: Request) -> dict[str, Any]:
    app = get_app(request)

    async with app.uow() as t:
        cluster_count = app.repos.clusters.count(t)
        deployment_count = app.repos.deployments.count(t)
        api_key_count = _api_keys_repo.count(t)
        pending_outbox = app.repos.outbox.count_by_status(t, "pending")
        dead_outbox = app.repos.outbox.count_by_status(t, "dead")
        active_runs = len(app.repos.workflow_runs.list_by_status(t, ACTIVE_RUN_STATUSES))

    next_fire_at = app.timers.next_fire_at()
    last_sync = app.services.reconciliation.last_sync()

    return {
        "status": "healthy",
        "service": _SERVICE_NAME,
        "version": _SERVICE_VERSION,
        "timestamp": app.clock.now().isoformat(),
        "database": {
            "connected": True,
            "cluster_count": cluster_count,
            "deployment_count": deployment_count,
            "api_key_count": api_key_count,
        },
        "executor": {
            "running": app.executor.running,
            "pending_outbox": pending_outbox,
            "dead_outbox": dead_outbox,
        },
        "timers": {
            "running": app.timers.running,
            "next_fire_at": next_fire_at.isoformat() if next_fire_at is not None else None,
        },
        "engine": {"active_runs": active_runs},
        "reconciler": {
            "running": app.services.reconciliation.running,
            "last_sync": last_sync.isoformat() if last_sync is not None else None,
        },
    }
