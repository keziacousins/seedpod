"""``TimerService`` -- polls the ``timers`` table and fires due timers with
conditional consume, atomically: ``repos.timers.consume(t, ..., fire_at_text=snapshot)``
(a guarded ``DELETE ... AND fire_at = :snapshot``) + (rowcount 1 only)
``dispatcher.apply(decode_event(row.event), tx=t)`` in ONE transaction
(docs/design/seam-a-core.md §D, "Timer delivery", as amended by
docs/decisions/DR-0009-conditional-timer-consume.md, RATIFIED: *"per timer, one
transaction: conditional consume -- `DELETE ... WHERE (pk) AND fire_at = :snapshot`
(the fire_at this scan pass saw); rowcount 1 => apply(decode_event(...)) in the same
transaction + commit; rowcount 0 => a concurrent same-key re-arm/cancel won the
scan-to-fire window: skip the apply entirely"*). Constructor pinned by
docs/design/coherence-review.md Conflict 15's amended factory excerpt::

    timers = TimerService(uow=uow, repos=repos, dispatcher=dispatcher, clock=clock)

Fresh v2 component -- no v1 salvage source (v1 scheduled TTL/destroy sweeps via
APScheduler; DR-0003 retires it in favour of the durable ``timers`` table + this
poller, and exposes the table read-only via ``GET /api/timers``).

**Atomic consume.** The conditional DELETE and the ``apply()`` run inside the SAME
``UnitOfWork`` transaction (``seedpod/data/uow.py``: commit-on-clean-exit,
rollback-on-error). A crash/exception between the two is impossible to observe as
"only one happened": either both land (commit) or neither does (rollback, and the row
is still there to retry). A crash *before* a fire's transaction opens leaves the row
armed, so the timer is picked up again on the next poll -- at-least-once, never lost.
This guarantee is only as strong as the data layer's isolation: on the pinned
``StaticPool`` single shared SQLite connection (``seedpod/data/database.py``), a
concurrent transaction (the executor, the engine, an API handler) could otherwise
interleave mid-fire and corrupt this "both or neither" property. ``UnitOfWork`` now
serializes every transaction on one ``asyncio.Lock`` for its whole extent
(docs/decisions/DR-0008-uow-transaction-serialization.md, RATIFIED) precisely so this
module's atomicity claim is a true statement about the data layer, not an aspiration.
This also gives ``StaleVersion`` (docs/design/seam-a-core.md §D's general "caller
re-reads and re-decides" rule) a free ride here: a losing CAS rolls back the delete
along with the failed ``Persist``, so the row survives to be re-attempted on the very
next poll tick -- no bespoke retry-counter is needed (the ``timers`` table, unlike
``effects_outbox``, deliberately carries no ``attempts``/``status`` columns; the poll
loop itself IS the retry policy).

**Conditional consume (DR-0009).** The DR-0008 lock is released between the scan
transaction (``_fire_due``'s ``repos.timers.due(...)``) and each per-fire
transaction -- a window in which a concurrent same-key ``ScheduleTimer`` re-arm or
``CancelTimer`` can legitimately land. An unconditional PK delete would destroy a
just-re-armed row (losing its new deadline) and could apply a stale-but-still-valid
event against the current state (e.g. ``ACTIVE x TtlExpired`` after a TTL extend) --
the machine's Ignore rows cannot absorb this because the event is genuinely valid at
the *old* deadline. So ``_fire_one`` threads the exact ``fire_at`` TEXT its scan pass
saw (``row.fire_at_text`` -- the raw column value, not a re-serialized ``datetime``)
into ``repos.timers.consume(..., fire_at_text=row.fire_at_text)``: rowcount 1
means the row is unchanged since the scan and ``apply()`` proceeds in the same
transaction; rowcount 0 means a re-arm or cancel won the race, and this module skips
the apply entirely -- no state pre-check, no read-then-compare in Python, just the
conditioned DELETE's rowcount. A re-arm to the identical ``fire_at`` still fires
(semantically identical deadline, harmless); a re-arm to a NEW ``fire_at`` survives
to fire on a later poll pass.

**No state pre-checks here.** Every event a ``ScheduleTimer`` effect can store is a
``TimerFired`` (``TtlExpired`` | ``DestroyDue``); per the totality law
(docs/design/seam-a-core.md §F), ``transition()`` NEVER raises ``InvalidTransition``
for a ``TimerFired`` -- an unlisted ``(state, TimerFired-subclass)`` cell defaults to
Ignore (no write, no audit, no version bump). A raced/stale fire (the aggregate moved
on since the timer was armed) is therefore already handled correctly by just applying
the event verbatim and letting the machine decide; adding a staleness pre-check in
this module would duplicate, and risk drifting from, that law. (This is orthogonal to
-- and does not overlap with -- DR-0009's conditional consume above: machine Ignore
covers state that moved on; conditional consume covers a same-state re-arm the
machine cannot see.)

**Per-row isolation.** One due timer failing (e.g. a losing CAS) does not stop the
rest of the batch or kill the poll loop -- each row's fire is wrapped individually so
a single bad row is simply retried on a later poll while its siblings still fire this
tick. This is this module's own implementation choice (no spec text mandates it), made
because the alternative -- one bad row silently halting ALL future timer delivery --
is a correctness regression no spec would plausibly want.

**Scan-level isolation.** Per-row isolation alone is not enough: the scan itself
(``repos.timers.due(...)``) and the trailing ``next_fire_at()`` refresh are also
transactions against the same shared connection, and a transient failure there (e.g.
a busy/locked connection) is just as real as a bad row. ``_run``'s poll loop
therefore wraps the WHOLE poll pass (``_fire_due()``, scan included) in the same
per-tick isolation -- an exception anywhere in a pass is logged and the loop
continues to the next tick, exactly as a single bad row does not stop its siblings.
Without this, one transient error would permanently kill the background task: no
component supervises it, ``start()`` is a no-op once ``_task`` is set, and every
armed timer -- not just the failing row -- would silently stop being delivered for
the rest of the process's life. That would falsify this module's own "at-least-once,
never lost" claim above, so the guard lives at the same altitude as the claim it
protects.

**``running``.** ``self._task is not None`` alone cannot tell a healthy loop from a
crashed one -- a dead task stays non-``None`` forever (``stop()`` is the only thing
that clears it). ``running`` is therefore ``self._task is not None and not
self._task.done()``: truthful even if the scan-level guard above were ever bypassed
or the loop task died for some other reason (e.g. a bug in the loop's own control
flow, as opposed to a poll pass it isolates). Backs Round 6's ``/health/detailed``
``{"timers": {"running": ...}}`` field (docs/decisions/DR-0003).

**``poke()``.** A latency hint only (Conflict 15): wakes the poll loop early so a
freshly-armed timer with a near ``fire_at`` doesn't wait a full ``poll_interval``.
Correctness never depends on it -- the loop's own periodic poll is the backstop,
exactly as ``Dispatcher.attach_executor``/``attach_timers``' docstring already
describes of the collaborators IT pokes. ``_run`` clears the wake event BEFORE
running the poll pass (not after) so a ``poke()`` landing mid-pass -- e.g. the
Dispatcher commits a freshly-armed, immediately-due timer while this pass is already
scanning -- is not erased by a clear that comes later: the event survives to wake the
subsequent wait immediately instead of costing a full ``poll_interval`` of latency
for free.

**``poll_interval`` default.** No spec pins one -- Conflict 15's factory excerpt
constructs ``TimerService`` without the kwarg (i.e. "use the default"). This module
picks ``1.0`` second: timers are minute/hour-grained (TTL, destroy-due), so
sub-second latency has no product value, and a 1s floor keeps idle CPU negligible.
An implementation choice, not a spec gap.

**``next_fire_at()``.** Round 6's ``/health/detailed`` (docs/decisions/DR-0003)
reads this for its ``{"timers": {"running": ..., "next_fire_at": ...}}`` block.
Cached from the last SUCCESSFUL poll pass rather than re-querying the DB per health
check -- this module already computes the answer on every tick; a pass isolated by
the scan-level guard above simply leaves the previous value in place until a later
pass succeeds.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import datetime

from seedpod.core.clock import Clock
from seedpod.core.codec import decode_event
from seedpod.data.repositories import Repositories, TimerRow
from seedpod.data.uow import UnitOfWork
from seedpod.runtime.dispatcher import Dispatcher

__all__ = ["TimerService"]

_DEFAULT_POLL_INTERVAL = 1.0  # seconds -- see module docstring

# How long `stop()` lets the loop finish cooperatively before falling back to
# `task.cancel()`. Only a genuinely stuck pass should ever reach it: a loop parked
# on its wake-wait returns within one tick of `stop()`'s poke.
_STOP_GRACE_SECONDS = 5.0

_log = logging.getLogger(__name__)


class TimerService:
    """``TimerService(uow, repos, dispatcher, clock, poll_interval=1.0)`` -- polls
    ``timers``; atomic conditional-consume+apply per fire (coherence-review Conflicts
    1/15; seam-a-core.md §D as amended by DR-0009)."""

    def __init__(
        self,
        uow: UnitOfWork,
        repos: Repositories,
        dispatcher: Dispatcher,
        clock: Clock,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
    ) -> None:
        self._uow = uow
        self.repos = repos
        self._dispatcher = dispatcher
        self._clock = clock
        self._poll_interval = poll_interval
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._next_fire_at: datetime | None = None

    def poke(self) -> None:
        """Latency hint: wake the poll loop before its next timeout. Never
        load-bearing -- see module docstring."""
        self._wake.set()

    def next_fire_at(self) -> datetime | None:
        """The earliest armed ``fire_at`` as of the last SUCCESSFUL poll pass, or
        ``None`` if no timer is currently armed. Read by Round 6's
        ``/health/detailed`` (DR-0003)."""
        return self._next_fire_at

    @property
    def running(self) -> bool:
        """Truthful liveness: the loop task exists AND has not finished (module
        docstring, "``running``") -- ``self._task is not None`` alone would
        misreport a dead task as running, since only ``stop()`` clears it. Backs
        Round 6's ``/health/detailed`` ``{"timers": {"running": ...}}`` (DR-0003)."""
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Start the background poll loop. Idempotent -- a second ``start()`` while
        already running is a no-op."""
        if self._task is not None:
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Ask the poll loop to finish, and wait for it to actually do so.
        Idempotent.

        COOPERATIVE, with cancel only as a backstop. The obvious implementation --
        ``task.cancel()`` then ``await task`` -- is unsound on Python 3.11: this
        loop's wait is ``asyncio.wait_for(self._wake.wait(), ...)``, and 3.11's
        pre-3.12 ``wait_for`` has no ``uncancel`` bookkeeping, so a ``cancel()``
        arriving in the same window in which a ``poke()`` completes the inner
        ``wake.wait()`` is SWALLOWED. The task is then branded ``cancelling`` yet
        keeps running, and ``await task`` never returns -- a hang with an idle
        event loop (no fds, no timers) that no later cancel arrives to break. That
        is the root cause of an intermittent ``App.stop()`` teardown hang, and
        ``poke()`` immediately before ``stop()`` is exactly what ``App.stop()``
        does in practice. Reproduced deterministically: the cancel is lost 100% of
        the time when it follows a poke by <=1 event-loop tick.

        So the normal path never depends on cancellation at all: set ``_stopping``,
        then ``poke()`` to wake the wait at once, and the loop returns on its own.
        ``asyncio.wait`` (which, unlike ``wait_for``, cancels NOTHING on timeout)
        gives the loop a bounded grace to notice; only if it overruns -- a genuinely
        stuck pass, not this race -- does the cancel backstop fire."""
        task, self._task = self._task, None
        if task is None:
            return
        self._stopping = True
        self._wake.set()  # cooperative wake: ordered AFTER _stopping, see _run
        done, _pending = await asyncio.wait({task}, timeout=_STOP_GRACE_SECONDS)
        if not done:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        while not self._stopping:
            # Clear BEFORE the pass, not after (module docstring, "poke()") -- a
            # poke() landing mid-pass sets the event again and survives to wake the
            # wait below immediately, instead of being erased by a clear that comes
            # after the pass already missed it.
            self._wake.clear()
            # Re-check AFTER the clear: `stop()` sets `_stopping` BEFORE its
            # `_wake.set()`, so even if the clear above erased that wake, the flag
            # is already visible here and the loop still exits.
            if self._stopping:
                return
            try:
                await self._fire_due()
            except Exception:
                # Scan-level isolation (module docstring) -- a transient failure in
                # the scan or the next_fire_at refresh must not kill this task: no
                # supervisor restarts it, and start()/stop() cannot revive a dead
                # task. Log and retry on the next tick, exactly as a single bad row
                # does not stop its siblings.
                _log.exception("timer poll pass failed, will retry on a later poll")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval)

    async def _fire_due(self) -> None:
        """One poll pass: fire every timer due as of ``clock.now()``, then refresh
        ``next_fire_at()``. Each row is isolated (module docstring, "Per-row
        isolation") -- a failure fires no siblings' rows but does not stop them. The
        whole pass (this scan included) is itself isolated one level up, by ``_run``
        (module docstring, "Scan-level isolation")."""
        async with self._uow() as t:
            due = self.repos.timers.due(t, self._clock.now())
        for row in due:
            try:
                await self._fire_one(row)
            except Exception:
                _log.exception(
                    "timer fire failed, will retry on a later poll: %s/%s/%s",
                    row.aggregate_type,
                    row.aggregate_id,
                    row.timer_key,
                )
        async with self._uow() as t:
            self._next_fire_at = self.repos.timers.next_fire_at(t)

    async def _fire_one(self, row: TimerRow) -> None:
        """ONE transaction: conditionally consume the row (DR-0009's guarded
        ``DELETE ... AND fire_at = :snapshot``, keyed on the exact ``fire_at`` TEXT
        this scan pass saw -- ``row.fire_at_text``, not a re-serialized ``datetime``),
        then, ONLY if that consume actually deleted the row (rowcount 1), apply its
        stored event verbatim through the Dispatcher (its ``actor`` is already
        ``"timer:<key>"`` -- set when the ``ScheduleTimer`` effect was built, never
        synthesized here). rowcount 0 means a concurrent same-key re-arm or cancel won
        the scan-to-fire window (module docstring, "Conditional consume") -- skip the
        apply entirely; the row was already someone else's write, not this pass's to
        touch.
        Atomic: an exception anywhere in this block rolls back BOTH the consume and
        whatever ``apply()`` attempted, leaving the row armed for the next poll
        (module docstring, "Atomic consume")."""
        async with self._uow() as t:
            consumed = self.repos.timers.consume(
                t, row.aggregate_type, row.aggregate_id, row.timer_key, row.fire_at_text
            )
            if not consumed:
                return
            event = decode_event(json.loads(row.event))
            await self._dispatcher.apply(row.aggregate_type, row.aggregate_id, event, tx=t)
