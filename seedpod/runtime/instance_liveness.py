"""``InstanceLivenessSweep`` -- the level-triggered backstop for an instance's way to destruction
(DR-0055; the owner's design decision after four rounds of edge-triggered fixes).

**The invariant.** An instance that may hold a resource always has a WORKING path to destruction,
without a restart. Every edge-triggered repair (a dead outbox row reports, a run that cannot start
reports, a hard cancel finishes the run) can itself fail, so none of them is the guarantee. This
sweep is: it does not wait for an event, it reads the CURRENT state and repairs it, on a timer.

**A path.** For each instance that is not ``destroyed``, a path exists if ANY of these holds:

1. an active run (``pending``/``running``/``blocked``/``compensating``) with a LIVE engine task;
2. a ``pending`` outbox row that will START the workflow the state waits on: a ``provision`` row
   for ``provisioning``, a ``destroy`` row for ``destroying``. A pending row whose payload cannot
   be decoded is NOT a path (it will go ``dead`` after its backoff ladder and start nothing). A
   pending ``cancel_workflow`` row is not one either: it starts nothing, and the destroy row that
   follows it in the same transaction is what counts;
3. for a state that waits on a timer (``active``, ``failed``, ``destroy-failed``, ``zombie``), a
   VALID armed ``expiry`` timer, judged from its raw row (nothing is parsed by the repository): its
   ``fire_at`` parses to an aware datetime, its event decodes to ``InstanceExpired``, and its
   ``fire_at`` is not later than the latest time the machine can arm it for this instance
   (``hard_expires_at``; a ``destroy-failed`` retry or a ``zombie`` found after the hard limit is armed
   from the time of the event, so ``now`` plus ``INSTANCE_DESTROY_RETRY_DELAY`` / ``now`` extend it).
   A bad timer is not a path: it is replaced through ``InstanceTimerLost``, whose upsert overwrites
   it, and the replacement is valid, so the next pass writes nothing.

   A valid timer that is OVERDUE IS a path: firing it is the timer service's job. The sweep only
   logs a WARNING (once per instance per timer value) and writes NOTHING. A stopped timer service
   therefore causes no audit, outbox or version change, however many passes run.

**Repairs** (first match, one instance at a time, each in ONE transaction):

- an active run with NO live task AND not being ended by the engine's hard-cancel backstop
  (``engine.is_ending``: a run in that window has no task and is not adoptable): the engine adopts that one run (``engine.resume_run``, the
  single-run form of ``resume_inflight``). A run that already has a live task is never given a
  second one. No event is applied, because the run reports its own outcome when it ends;
- ``destroying`` with no path: ``InstanceDestroyFailed`` (the machine arms the retry timer);
- ``provisioning`` with no path: ``InstanceProvisionFailed`` (the expiry timer is still armed);
- a timer-waiting state with no armed timer: ``InstanceTimerLost``, which the machine turns into a
  re-armed ``expiry`` timer, never later than ``hard_expires_at`` and due at once if already past.

**Reads only the database and the engine's task table.** No provider call. Every state change goes
through ``Dispatcher.apply`` (in the sweep's transaction, so the read that found the missing path
and the write that restores it are one atomic step under the single-writer lock, DR-0008: a path
that appears in between cannot be missed, and cannot cause a double run or a wrong transition).
The one thing done outside the transaction is the adoption, which is idempotent by construction:
``resume_run`` re-reads the run and refuses one that is terminal or already has a task.

**One bad instance never stops the pass.** The pass lists instance ids WITHOUT decoding rows, then
loads and checks each id inside its own isolation. A failure on one instance (a row that cannot be
decoded included) is logged at ERROR with its id, on every pass, and the next one is examined. The
pass itself is isolated the same way inside ``_run``.

**No grace period.** A row, a timer and the state transition that needs them are written in ONE
transaction (``Dispatcher.apply``), so a just-created instance is never seen without its path. The
only gap is between the executor admitting a run and handing it to the engine, and that is a run
with no task, which is adopted idempotently (the executor's own ``engine.start`` then finds the task
and does nothing). So a false positive cannot arise from timing, and nothing here waits.

**Injectable interval.** ``interval`` is a constructor argument, and ``run_once()`` is the pass
itself, public, so a test drives it with a fake clock and no sleeping.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from seedpod.core.clock import Clock
from seedpod.core.codec import decode_effect, decode_event
from seedpod.core.effects import RunWorkflow
from seedpod.core.events import (
    Event,
    InstanceDestroyFailed,
    InstanceExpired,
    InstanceProvisionFailed,
    InstanceTimerLost,
)
from seedpod.core.machine import INSTANCE_DESTROY_RETRY_DELAY
from seedpod.core.records import InstanceRecord, InstanceStatus
from seedpod.data.repositories import Repositories, _iso
from seedpod.data.uow import UnitOfWork
from seedpod.runtime.dispatcher import Dispatcher

__all__ = ["InstanceLivenessSweep", "Repair", "inspect_instance", "report_if_no_path"]

_log = logging.getLogger(__name__)

_DEFAULT_INTERVAL = 30.0  # seconds. Bounds how long a lost path can last; a pass is a few reads.
# How far past its ``fire_at`` a valid timer may be before the sweep says so (a WARNING, nothing else).
# The timer service fires what is due within a poll, so a timer this late means it is not running.
_OVERDUE_WARNING_AFTER = timedelta(minutes=5)
_STOP_GRACE_SECONDS = 5.0  # as TimerService: cooperative stop, cancel only as a backstop
_ACTOR = "sweep:instance-liveness"

# The states that wait on the ``expiry`` timer: it is their way out.
_TIMER_STATES = frozenset(
    {InstanceStatus.ACTIVE, InstanceStatus.FAILED, InstanceStatus.DESTROY_FAILED, InstanceStatus.ZOMBIE}
)
# The states that wait on a run, and the workflow whose pending outbox row starts it.
_RUN_WORKFLOW_OF = {InstanceStatus.PROVISIONING: "provision", InstanceStatus.DESTROYING: "destroy"}


class EngineLike(Protocol):
    def is_running(self, run_id: str) -> bool: ...

    def is_ending(self, run_id: str) -> bool: ...

    async def resume_run(self, run_id: str) -> bool: ...


@dataclass(frozen=True, slots=True)
class Repair:
    """What one pass did for one instance (also what ``run_once()`` returns, for tests and logs)."""

    instance_id: str
    kind: str  # "adopted-run" | "destroy-failed" | "provision-failed" | "timer-restored"
    detail: str


@dataclass(frozen=True, slots=True)
class _Finding:
    adopt_run_id: str | None = None
    event: Event | None = None
    kind: str = ""
    detail: str = ""
    overdue_fire_at: str | None = None  # a VALID timer this far overdue: logged by the sweep, never written


_OK = _Finding()


def _has_pending_start_row(repos: Repositories, tx: Any, record: InstanceRecord) -> bool:
    wanted = _RUN_WORKFLOW_OF.get(record.state)
    if wanted is None:
        return False
    for row in repos.outbox.list_for_aggregate(tx, "instance", record.id):
        if row.kind != "run_workflow" or row.status != "pending":
            continue
        try:
            effect = decode_effect(json.loads(row.payload))
        except Exception:  # noqa: BLE001 -- an undecodable row starts nothing
            continue
        if (
            isinstance(effect, RunWorkflow)
            and effect.aggregate_type == "instance"
            and effect.aggregate_id == record.id
            and effect.workflow == wanted
        ):
            return True
    return False


def _latest_legitimate_fire_at(record: InstanceRecord, now: datetime) -> datetime:
    """The latest ``fire_at`` the machine can arm for this instance's ``expiry`` timer."""
    latest = record.hard_expires_at
    if record.state is InstanceStatus.DESTROY_FAILED:
        # The retry is armed one delay after the failure, which may be after the hard limit.
        return max(latest, now + INSTANCE_DESTROY_RETRY_DELAY)
    if record.state is InstanceStatus.ZOMBIE:
        # A zombie found after the hard limit is armed at the time it was found.
        return max(latest, now)
    return latest


def _expiry_timer_problem(raw: tuple[Any, Any], record: InstanceRecord, now: datetime) -> tuple[str | None, datetime | None]:
    """``(why this raw ``expiry`` row is not a path or None, its ``fire_at`` when it parsed)``."""
    fire_at_text, event_text = raw
    try:
        fire_at = datetime.fromisoformat(fire_at_text)
        if fire_at.tzinfo is None:
            raise ValueError("naive datetime")
    except Exception:  # noqa: BLE001 -- any failure to parse is the finding
        return f"its fire_at {fire_at_text!r} is not a valid aware datetime", None
    if fire_at_text != _iso(fire_at):
        # The timer scan compares stored TEXT, so another spelling of a valid time may never be due.
        return f"its fire_at {fire_at_text!r} is not in the stored form", fire_at
    try:
        event = decode_event(json.loads(event_text))
    except Exception as exc:  # noqa: BLE001 -- any failure to decode is the finding
        return f"its stored event cannot be decoded ({type(exc).__name__})", fire_at
    if not isinstance(event, InstanceExpired):
        return f"its stored event is a {type(event).__name__}, not an InstanceExpired", fire_at
    latest = _latest_legitimate_fire_at(record, now)
    if fire_at > latest:
        return f"its fire_at {fire_at.isoformat()} is later than the latest the machine arms ({latest.isoformat()})", fire_at
    return None, fire_at


def inspect_instance(
    repos: Repositories,
    tx: Any,
    record: InstanceRecord,
    *,
    task_alive: Callable[[str], bool],
    now: Any,
    reason: str | None = None,
    actor: str = _ACTOR,
) -> _Finding:
    """The pure-read half of a pass for one instance, inside the caller's transaction: does it have a
    path, and if not, what repairs it. ``task_alive(run_id)`` is whether the engine has a live task
    for that run."""
    if record.state in (InstanceStatus.DESTROYED, InstanceStatus.NEW):
        return _OK
    run = repos.workflow_runs.active_for_instance(tx, record.id)
    if run is not None:
        if task_alive(run.id):
            return _OK
        return _Finding(adopt_run_id=run.id, kind="adopted-run", detail=f"{run.workflow} run {run.id} had no task")
    if _has_pending_start_row(repos, tx, record):
        return _OK
    if record.state in _TIMER_STATES:
        raw = repos.timers.get_raw(tx, "instance", record.id, "expiry")
        fire_at = None
        if raw is None:
            problem = "it had no armed expiry timer"
        else:
            problem, fire_at = _expiry_timer_problem(raw, record, now)
        if raw is not None and problem is None:
            if fire_at is not None and now - fire_at > _OVERDUE_WARNING_AFTER:
                return _Finding(overdue_fire_at=raw[0])
            return _OK
        return _Finding(
            event=InstanceTimerLost(at=now, actor=actor),
            kind="timer-restored",
            detail=f"{record.state.value} instance: {problem}",
        )
    if record.state is InstanceStatus.DESTROYING:
        why = reason or "liveness sweep: destroying with no run, no pending destroy row and no live task"
        return _Finding(
            event=InstanceDestroyFailed(at=now, actor=actor, reason=why), kind="destroy-failed", detail=why
        )
    if record.state is InstanceStatus.PROVISIONING:
        why = reason or "liveness sweep: provisioning with no run, no pending provision row and no live task"
        return _Finding(
            event=InstanceProvisionFailed(at=now, actor=actor, reason=why), kind="provision-failed", detail=why
        )
    return _OK


async def report_if_no_path(
    repos: Repositories, dispatcher: Dispatcher, tx: Any, instance_id: str, *, clock: Clock, reason: str, actor: str
) -> Repair | None:
    """The immediate form of the same repair, for a caller that has just made a path impossible
    (the executor, when it marks an instance's outbox row ``dead``) and holds a transaction: apply
    the event for a ``destroying``/``provisioning`` instance that has no path, and nothing else.

    Every active run counts as a path here (its task may simply not have been adopted yet; the
    sweep adopts it), so this can never report an instance that a run is still working for. Not a
    substitute for the sweep: if this fails or is skipped, the sweep finds the same state."""
    record = repos.instances.load(tx, instance_id)
    if record is None or record.state not in _RUN_WORKFLOW_OF:
        return None
    finding = inspect_instance(
        repos, tx, record, task_alive=lambda _run_id: True, now=clock.now(), reason=reason, actor=actor
    )
    if finding.event is None:
        return None
    await dispatcher.apply("instance", instance_id, finding.event, tx=tx)
    return Repair(instance_id, finding.kind, finding.detail)


class InstanceLivenessSweep:
    """``InstanceLivenessSweep(uow, repos, dispatcher, engine, clock, interval=30.0)``."""

    def __init__(
        self,
        uow: UnitOfWork,
        repos: Repositories,
        dispatcher: Dispatcher,
        engine: EngineLike,
        clock: Clock,
        interval: float = _DEFAULT_INTERVAL,
    ) -> None:
        self._overdue_warned: dict[str, str] = {}  # instance id -> the timer value already warned about
        self._uow = uow
        self._repos = repos
        self._dispatcher = dispatcher
        self._engine = engine
        self._clock = clock
        self._interval = interval
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Idempotent."""
        if self._task is not None:
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Idempotent. Cooperative, with cancel as a backstop only (``TimerService.stop`` explains
        why a bare ``cancel()`` after a wake is unsound on Python 3.11)."""
        task, self._task = self._task, None
        if task is None:
            return
        self._stopping = True
        self._wake.set()
        done, _pending = await asyncio.wait({task}, timeout=_STOP_GRACE_SECONDS)
        if not done:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        while not self._stopping:
            self._wake.clear()
            if self._stopping:
                return
            try:
                await self.run_once()
            except Exception:
                _log.exception("instance liveness pass failed, will retry on the next pass")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._interval)

    async def run_once(self) -> list[Repair]:
        """One pass over every instance that is not ``destroyed``. Never raises for one instance's
        failure (a row that cannot be decoded included): it is logged at ERROR with the instance id,
        and the pass goes on to the next. Returns what was repaired."""
        async with self._uow() as tx:
            ids = self._repos.instances.list_live_ids(tx)
        repairs: list[Repair] = []
        self._overdue_warned = {i: v for i, v in self._overdue_warned.items() if i in set(ids)}
        for instance_id in ids:
            if self._stopping:
                break
            try:
                repair = await self._check(instance_id)
            except Exception:  # noqa: BLE001 -- one instance must not stop the rest; the next pass retries
                _log.exception("instance liveness: could not check or repair instance %s", instance_id)
                continue
            if repair is not None:
                repairs.append(repair)
        return repairs

    async def _check(self, instance_id: str) -> Repair | None:
        finding = _OK
        async with self._uow() as tx:
            record = self._repos.instances.load(tx, instance_id)
            if record is None:
                return None
            finding = inspect_instance(
                self._repos,
                tx,
                record,
                # A run the engine is ending has no task, and is not adoptable either: it is a path.
                task_alive=lambda run_id: self._engine.is_running(run_id) or self._engine.is_ending(run_id),
                now=self._clock.now(),
            )
            if finding.event is not None:
                await self._dispatcher.apply("instance", instance_id, finding.event, tx=tx)
        if finding.overdue_fire_at is not None:
            if self._overdue_warned.get(instance_id) != finding.overdue_fire_at:
                self._overdue_warned[instance_id] = finding.overdue_fire_at
                _log.warning(
                    "instance liveness: %s: its expiry timer is overdue (due %s) and has not fired; "
                    "is the timer service running?",
                    instance_id, finding.overdue_fire_at,
                )
            return None
        self._overdue_warned.pop(instance_id, None)
        if finding.event is not None:
            _log.warning("instance liveness: %s: %s", instance_id, finding.detail)
            return Repair(instance_id, finding.kind, finding.detail)
        if finding.adopt_run_id is not None:
            # Outside the transaction (the engine opens its own). Idempotent: `resume_run` re-reads the
            # run and does nothing for one that finished, or that the executor just handed over.
            if await self._engine.resume_run(finding.adopt_run_id):
                _log.warning("instance liveness: %s: %s; adopted", instance_id, finding.detail)
                return Repair(instance_id, finding.kind, finding.detail)
        return None
