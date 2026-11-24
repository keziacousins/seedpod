"""``GET /api/timers`` -- docs/decisions/DR-0003: "Expose the ``timers`` table via
``GET /api/timers`` -> ``{timers: [{aggregate_type, aggregate_id, timer_key,
fire_at}]}``, ordered by ``fire_at``. ... Read-only in v2.0 (no create/cancel via
this endpoint -- timers are machine decisions; TTL changes go through ``POST
/api/clusters/{id}/extend``)." This replaces the scheduled-jobs tab's data source
(ui-contract §4: "schedules tab from ``GET /api/timers``").

Field set is the DR-0003 shape verbatim -- ``event``/``fire_at_text``/
``created_by_effect`` (``TimerRow``'s other columns) are storage/replay plumbing,
not UI-facing (DR-0002: clean contract, no internal machinery leaking through)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.core.environment_scope import ALL_ENVIRONMENTS, environment_admits
from seedpod.data.repositories import TimerRow

__all__ = ["router"]

router = APIRouter(tags=["timers"])


def _serialize(row: TimerRow) -> dict[str, Any]:
    return {
        "aggregate_type": row.aggregate_type,
        "aggregate_id": row.aggregate_id,
        "timer_key": row.timer_key,
        "fire_at": row.fire_at.isoformat(),
    }


@router.get("/timers")
async def list_timers(
    request: Request, api_key=Depends(require_permission("workflows:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    async with app.uow() as t:
        rows = app.repos.timers.list_all(t)
        if api_key.environment != ALL_ENVIRONMENTS:
            # DR-0054: a timer has the environment of its cluster or instance (DR-0055). A timer on
            # any other aggregate type maps to "", which no key admits.
            environments = {("cluster", c.id): c.environment for c in app.repos.clusters.list_all(t)}
            environments |= {("instance", i.id): i.environment for i in app.repos.instances.list_all(t)}
            rows = [
                r for r in rows
                if environment_admits(api_key.environment, environments.get((r.aggregate_type, r.aggregate_id), ""))
            ]
    return {"timers": [_serialize(r) for r in rows]}
