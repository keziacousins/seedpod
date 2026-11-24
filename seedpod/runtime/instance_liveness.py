"""``InstanceLivenessSweep``: the one recovery mechanism for an instance (DR-0055).

An instance that may hold a resource always needs a way to destruction, without a restart. This
sweep reads the CURRENT state, on a timer, and repairs it. Nothing else recovers an instance.
For each instance that is not ``destroyed``, in its own try/except:

1. An active run with no live engine task: ``engine.resume_run`` adopts it.
2. An active run with a live task, or a pending outbox row that will start this state's workflow
   (``provision`` for ``provisioning``, ``destroy`` for ``destroying``): nothing. A dead row is
   not pending, so it falls through.
3. ``destroying`` with neither: ``InstanceDestroyFailed`` (the machine arms the retry timer).
   ``provisioning`` with neither: ``InstanceProvisionFailed``. This also reports a run that ended
   with no outcome (a missing definition).
4. ``active``, ``failed``, ``destroy-failed`` or ``zombie`` with no ``expiry`` timer:
   ``InstanceTimerLost`` (the machine re-arms it).

A healthy pass writes nothing. The read and the repair share one transaction; only the adoption
is outside it, and ``resume_run`` is idempotent.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import TYPE_CHECKING

from seedpod.core import events as ev
from seedpod.core.clock import Clock
from seedpod.core.codec import decode_effect
from seedpod.core.effects import RunWorkflow
from seedpod.core.records import InstanceRecord, InstanceStatus
from seedpod.data.repositories import Repositories
from seedpod.data.uow import UnitOfWork
from seedpod.runtime.dispatcher import Dispatcher

if TYPE_CHECKING:
    from seedpod.engine.engine import WorkflowEngine

__all__ = ["InstanceLivenessSweep"]

_log = logging.getLogger(__name__)
_ACTOR = "sweep:instance-liveness"
_S = InstanceStatus
_TIMER_STATES = frozenset({_S.ACTIVE, _S.FAILED, _S.DESTROY_FAILED, _S.ZOMBIE})
_START_WORKFLOW = {_S.PROVISIONING: "provision", _S.DESTROYING: "destroy"}  # the pending row that starts the run


class InstanceLivenessSweep:
    """``InstanceLivenessSweep(uow, repos, dispatcher, engine, clock, interval=30.0)``.
    ``run_once()`` is one pass, public so a test drives it with no sleeping."""

    def __init__(
        self, uow: UnitOfWork, repos: Repositories, dispatcher: Dispatcher, engine: WorkflowEngine, clock: Clock,
        interval: float = 30.0,
    ) -> None:
        self._uow, self._repos, self._dispatcher = uow, repos, dispatcher
        self._engine, self._clock, self._interval = engine, clock, interval
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Idempotent."""
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Idempotent. A pass is a few reads and one transaction per repair, so a cancel is safe."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _run(self) -> None:
        while True:
            try:
                await self.run_once()
            except Exception:
                _log.exception("instance liveness pass failed, will retry on the next pass")
            await asyncio.sleep(self._interval)

    async def run_once(self) -> list[str]:
        """One pass. Returns the ids it repaired. One instance's failure is logged and the pass goes on."""
        async with self._uow() as tx:
            ids = [r.id for r in self._repos.instances.list_all(tx) if r.status != _S.DESTROYED]
        repaired = []
        for instance_id in ids:
            try:
                if await self._check(instance_id):
                    repaired.append(instance_id)
            except Exception:  # noqa: BLE001 -- one instance must not stop the rest
                _log.exception("instance liveness: could not check or repair instance %s", instance_id)
        return repaired

    async def _check(self, instance_id: str) -> bool:
        async with self._uow() as tx:
            record = self._repos.instances.load(tx, instance_id)
            run = self._repos.workflow_runs.active_for_instance(tx, instance_id)
            if record is None:
                return False
            if run is None:
                event = None if self._has_start_row(tx, record) else self._repair(tx, record)
                if event is not None:
                    _log.warning("instance liveness: %s (%s): applying %s", instance_id, record.state, type(event).__name__)
                    await self._dispatcher.apply("instance", instance_id, event, tx=tx)
                return event is not None
            if self._engine.is_running(run.id):
                return False
        _log.warning("instance liveness: %s: run %s has no task; adopting it", instance_id, run.id)
        await self._engine.resume_run(run.id)
        return True

    def _has_start_row(self, tx, record: InstanceRecord) -> bool:
        wanted = _START_WORKFLOW.get(record.state)
        for row in self._repos.outbox.list_for_aggregate(tx, "instance", record.id):
            if wanted and row.kind == "run_workflow" and row.status == "pending":
                effect = decode_effect(json.loads(row.payload))
                if isinstance(effect, RunWorkflow) and effect.workflow == wanted:
                    return True
        return False

    def _repair(self, tx, record: InstanceRecord) -> ev.Event | None:
        now = self._clock.now()
        if record.state is _S.DESTROYING:
            return ev.InstanceDestroyFailed(at=now, actor=_ACTOR, reason="liveness sweep: destroying with no run and no destroy row")
        if record.state is _S.PROVISIONING:
            return ev.InstanceProvisionFailed(at=now, actor=_ACTOR, reason="liveness sweep: provisioning with no run and no provision row")
        if record.state in _TIMER_STATES and self._repos.timers.get(tx, "instance", record.id, "expiry") is None:
            return ev.InstanceTimerLost(at=now, actor=_ACTOR)
        return None
