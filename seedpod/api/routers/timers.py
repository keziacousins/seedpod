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
    request: Request, _api_key=Depends(require_permission("workflows:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    async with app.uow() as t:
        rows = app.repos.timers.list_all(t)
    return {"timers": [_serialize(r) for r in rows]}
