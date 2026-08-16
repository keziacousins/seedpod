"""``HealthMonitor`` -- the ACTIVE-cluster kubectl health poll loop, a SEPARATE loop
from ``seedpod/runtime/reconciliation.py`` (docs/design/seam-b-engine.md: "The
ACTIVE-cluster monitoring loop's transient/permanent counter is *not* a Schedule; it
stays as salvaged logic in the monitoring component, outside this seam" -- THIS
module IS that component).

**Salvage source (hysteresis behavior, VERBATIM shape):**
``reference-code/seedpod/seedpod/core/job_manager.py:478-524`` (the inline
health-check-in-poll: classify -> reset-on-healthy / fail-on-permanent /
increment-and-threshold-check-on-transient) and ``:629-653``
(``_reset_health_failure_count``/``_increment_health_failure_count``, now
``ClusterRepository.set_health_failures`` -- the direct-ORM-write bypass v1 used
there is gone; this module calls the dedicated repo method instead, per that
method's own docstring). Decoupled from v1's status-polling (``_poll_cluster_status``
ran BOTH the health check AND provider-status-sync inline, in one job): this module
is its own standalone loop, with no status-sync responsibility at all (that stays
``ReconciliationService``'s job).

**DESIGN QUESTION resolved (build-agent judgment call, not a spec gap).** v1's
``core/health_check.py`` regex-classified an error STRING into
``ErrorType.TRANSIENT``/``PERMANENT`` because v1's provider returned
``(is_healthy: bool, error_reason: str)`` -- a stringly result with no typed
taxonomy underneath. v2's ``KubectlProvider`` (``seedpod/providers/kubectl.py``)
raises the REAL taxonomy directly: ``_get_cluster_info`` either yields
``Result(str)`` (success) or RAISES ``TransientError``/``PermanentError``/
``InfrastructureUnreachableError`` via ``_classify_failure`` -- there is no
"reachable but unhealthy, not an error" verdict ``KubeGetClusterInfo`` can express;
connectivity failure IS the only way this probe reports non-health, and it is
already typed. So this module classifies EXCLUSIVELY on the exception type the probe
raises (or its absence), never by re-importing v1's regex tables (which would be
classifying a string ``KubectlProvider`` never hands us -- the probe's caught
exception's ``str()`` only ever reaches a ``HealthCheckFailed.reason``/log line, never
a re-classification input). If a future probe ever needs a "reachable but degraded"
verdict that isn't an exception, that would be a real seam-c gap to raise then; it
does not exist today.

**Per-tick flow (DR-0008, BINDING).** ``tick()``: read every ACTIVE cluster's row in
ONE short DB-only transaction, close it -- then, for each row, probe with NO
transaction open (kubectl IO), then apply the verdict, each its own transaction. No
provider IO, no ``dispatcher.apply()`` await, ever runs with the scan's transaction
(or any transaction) still open.

**Counter-write seam** (docs/design/seam-d-foundation.md:100 "kept: durable
per-cluster counter for the health poll"; :399 "health counter column kept,
repo-method-only"). ``consecutive_health_failures`` is DB-only bookkeeping running
parallel to, not part of, the state machine (``ClusterRepository.set_health_failures``'s
own docstring: "this counter is health-poll bookkeeping ... Pillar 1 never reads or
writes it") -- reset/increment go straight through that repo method, in their own
short transaction, and NEVER ride the Dispatcher. Only the FAILED transition itself
(``HealthCheckFailed``) goes through ``Dispatcher.apply()`` -- the ONE write path for
cluster STATE (CLAUDE.md). These are two different columns with two different
write disciplines by design, not an inconsistency to paper over.

**StaleVersion / mid-redeploy safety (seam-a-core.md §D's general re-read/re-decide
rule; gotcha 16 names THIS module's ancestor -- "a health job's ``HealthCheckFailed``
built from a stale read loses the CAS and must re-decide against the fresh record" --
by name).** ``_apply_health_check_failed`` passes ``record=<the row this tick's scan,
or the most recent re-read, has in hand>`` to ``dispatcher.apply()`` rather than
letting it reload internally -- the SAME mechanism
``tests/runtime/test_dispatcher.py``'s ``test_stale_cas_raises_stale_version_and_leaves_db_untouched``
demonstrates (a ``HealthCheckFailed`` applied with a caller-supplied ``record=``
forces the CAS against that row's ``version``, not a freshly-reloaded one) --
because the ACTIVE-cluster row this module decides against is read at scan time,
across the probe's real IO gap (DR-0008: the probe runs with no transaction open),
during which another writer (an operator-triggered redeploy, a reconciler
Observation, ...) can legitimately move the SAME row on. A losing CAS
(``StaleVersion``) means exactly that happened: this module re-reads the row fresh
(its own short transaction) and re-decides --

- fresh row gone, or no longer ``ACTIVE`` -- the cluster left ACTIVE (a redeploy, a
  destroy, ...) in the gap; the correct outcome is "don't fail a cluster that isn't
  ACTIVE any more", so this module logs and drops, exactly the outcome the totality
  law's own Observation-default-Ignore rule (``seedpod/core/machine.py``'s
  ``_fill_defaults``: an unlisted ``(state, Observation)`` cell -> ``Ignore``, not
  ``InvalidTransition`` -- ``HealthCheckFailed`` is an ``Observation``) ALSO produces
  if the retried ``apply()`` call reaches ``transition()`` against a non-ACTIVE fresh
  record without ever raising: ``result.effects == ()`` is treated identically to a
  caught ``InvalidTransition`` below -- both are "logged and dropped, never failed",
  matching seam-a-core.md gotcha 3's outcome even though the CURRENT committed
  totality law reaches it via a silent no-op rather than an exception for this event
  class. ``InvalidTransition`` is still caught defensively (seam-a-core.md's own
  totality-law prose: "``InvalidTransition`` from ... ``reconciler``/``engine:*``/
  ``timer:*`` -> logged and dropped" -- ``health`` is the same actor family) even
  though it is unreachable for ``HealthCheckFailed`` against any REAL persisted row
  today, in case a future machine change ever makes a cell Invalid instead of Ignore.
- fresh row still ``ACTIVE`` -- genuinely raced on the version alone; retry the SAME
  verdict (the probe's classification does not change) against the fresh row.

Bounded to 3 attempts total (seam-a-core.md §D: "caller re-reads and re-decides,
bounded to 3 attempts"); exhausting all 3 logs an error and gives up for THIS tick --
the row is still ACTIVE and still unhealthy, so the NEXT tick's probe picks it right
back up (the same "per-row isolation, retried next pass" discipline
``TimerService``/``ReconciliationService`` already use for their own per-item
failures).

**Crown jewel #1's health analog.** ``InfrastructureUnreachableError`` means "cannot
determine health", never "unhealthy" -- this module does NOT increment the transient
counter and does NOT fail the cluster on it; it logs and skips the cluster for this
tick only. A network blip to the apiserver must never fail an ACTIVE cluster.

**kubeconfig resolution.** ``KubectlProvider`` is stateless (kubeconfig always passed
in, CLAUDE.md) -- this module decrypts ``ClusterRow.encrypted_kubeconfig`` via the
injected ``CryptoService`` (the taxonomy's one crypto site,
``seedpod/services/crypto.py``) using the row's own stamped ``kubeconfig_key_class``,
exactly the "stamp columns make decrypt independent of the mapping entirely"
discipline that module's docstring describes. No committed step/service resolves this
today (``seedpod/engine/steps/`` is still empty; ``cluster.load_kubeconfig`` is a
future workflow verb, not built) -- this is new orchestration-shell plumbing this
module owns, not a re-use of something already committed. An ACTIVE row with no
captured kubeconfig (should not occur in practice, but is not guaranteed unreachable
by the schema) is treated the same as "cannot determine health": logged and skipped,
never failed.

**``interval``/``max_transient_failures`` defaults.** No spec pins either. v1's
``cluster_status_poll_active`` (``reference-code/seedpod/seedpod/core/config.py:174-176``)
default was 60 seconds and ``health_check_max_transient_failures`` (:190-192) default
was 3 -- this module keeps both numbers as its own defaults, an implementation choice
like ``ReconciliationService.interval``'s, not a spec gap.

**Liveness.** ``running``/``last_sync()`` mirror ``ReconciliationService``'s exactly
(same discipline, same Round-6 ``/health/detailed`` consumer) -- no separate
"next-run" accessor: neither sibling salvaged-loop component
(``ReconciliationService``) exposes one beyond ``last_sync()`` + its own ``interval``,
so this module doesn't invent a new convention for itself alone.

**No new SSE topic.** A health failure surfaces via the FAILED cluster's normal
``cluster_state_changed`` ``Notify`` (emitted by ``transition()`` itself, drained by
the ``EffectExecutor`` exactly like every other cluster state change) -- this module
never touches the hub.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime

from seedpod.core.clock import Clock
from seedpod.core.errors import InfrastructureUnreachableError, PermanentError, TransientError
from seedpod.core.events import HealthCheckFailed
from seedpod.core.machine import InvalidTransition, StaleVersion
from seedpod.core.records import ClusterState
from seedpod.data.repositories import ClusterRow, Repositories
from seedpod.data.uow import UnitOfWork
from seedpod.providers.contract import KubeGetClusterInfo, Provider
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.services.crypto import CryptoService

__all__ = ["HealthMonitor"]

_log = logging.getLogger(__name__)

_DEFAULT_INTERVAL = 60.0  # seconds -- see module docstring's "defaults" note
_DEFAULT_MAX_TRANSIENT_FAILURES = 3  # ditto
_MAX_STALE_RETRIES = 3  # seam-a-core.md §D: "bounded to 3 attempts"


class HealthMonitor:
    """``HealthMonitor(kubectl, crypto, repos, dispatcher, uow, clock,
    interval=60.0, max_transient_failures=3)`` (module docstring). ``kubectl`` is any
    ``Provider``-shaped collaborator that accepts ``KubeGetClusterInfo`` (the real
    ``KubectlProvider`` in production; a hand-built fake in tests, CLAUDE.md)."""

    def __init__(
        self,
        kubectl: Provider,
        crypto: CryptoService,
        repos: Repositories,
        dispatcher: Dispatcher,
        uow: UnitOfWork,
        clock: Clock,
        interval: float = _DEFAULT_INTERVAL,
        max_transient_failures: int = _DEFAULT_MAX_TRANSIENT_FAILURES,
    ) -> None:
        self._kubectl = kubectl
        self._crypto = crypto
        self.repos = repos
        self._dispatcher = dispatcher
        self._uow = uow
        self._clock = clock
        self._interval = interval
        self._max_transient_failures = max_transient_failures
        self._task: asyncio.Task[None] | None = None
        self._last_sync: datetime | None = None

    def last_sync(self) -> datetime | None:
        """The clock-stamped time of the last COMPLETED tick. Backs Round 6's
        ``/health/detailed`` (module docstring, "Liveness")."""
        return self._last_sync

    @property
    def running(self) -> bool:
        """Truthful liveness, same discipline as ``TimerService.running``/
        ``ReconciliationService.running``."""
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Idempotent. Fires one immediate tick BEFORE returning (same discipline as
        ``ReconciliationService.start()``), then spawns the periodic loop."""
        if self._task is not None:
            return
        await self.tick()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Cancel the periodic loop and wait for it to actually finish. Idempotent."""
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self.tick()
            except Exception:
                # Scan-level isolation, same discipline as TimerService/
                # ReconciliationService: a transient failure in a whole pass must
                # not kill this task -- no supervisor restarts it.
                _log.exception("health poll pass failed, will retry next tick")

    async def tick(self) -> None:
        """One full pass (module docstring's DR-0008 read/IO/apply ordering): read
        every ACTIVE cluster (one short DB-only transaction, closed before any IO),
        then probe + act on each, one at a time, isolated per row."""
        async with self._uow() as t:
            rows = self.repos.clusters.list_by_status(t, (ClusterState.ACTIVE.value,))
        for row in rows:
            try:
                await self._check_one(row)
            except Exception:
                _log.exception(
                    "health check failed for cluster %s, will retry next tick", row.id
                )
        self._last_sync = self._clock.now()

    # -------------------------------------------------------------------------
    # Per-cluster: probe with NO transaction open (DR-0008), then act
    # -------------------------------------------------------------------------

    async def _check_one(self, row: ClusterRow) -> None:
        kubeconfig = self._resolve_kubeconfig(row)
        if kubeconfig is None:
            _log.warning(
                "health: cluster %s has no captured kubeconfig, skipping this tick", row.id
            )
            return
        try:
            await self._probe(kubeconfig)
        except InfrastructureUnreachableError as exc:
            # Crown jewel #1's health analog (module docstring): cannot determine
            # health != unhealthy. Never increments, never fails.
            _log.warning("health: cluster %s unreachable, skipping this tick: %s", row.id, exc)
            return
        except PermanentError as exc:
            await self._fail(row, reason=f"kubectl connectivity lost (permanent): {exc}")
            return
        except TransientError as exc:
            await self._transient(row, exc)
            return
        await self._healthy(row)

    def _resolve_kubeconfig(self, row: ClusterRow) -> str | None:
        if not row.encrypted_kubeconfig or not row.kubeconfig_key_class:
            return None
        return self._crypto.decrypt(row.encrypted_kubeconfig, row.kubeconfig_key_class)

    async def _probe(self, kubeconfig: str) -> None:
        """One bounded ``KubeGetClusterInfo`` -- no transaction open (DR-0008)."""
        async for _ev in self._kubectl.execute(KubeGetClusterInfo(kubeconfig=kubeconfig)):
            pass  # reaching the end of the stream without a raised error IS healthy

    # -------------------------------------------------------------------------
    # Verdicts -- each write below is its OWN transaction (DR-0008)
    # -------------------------------------------------------------------------

    async def _healthy(self, row: ClusterRow) -> None:
        if row.consecutive_health_failures > 0:
            async with self._uow() as t:
                self.repos.clusters.set_health_failures(t, row.id, 0, clock=self._clock)

    async def _transient(self, row: ClusterRow, exc: TransientError) -> None:
        new_count = row.consecutive_health_failures + 1
        if new_count >= self._max_transient_failures:
            reason = (
                f"kubectl connectivity lost after {new_count} consecutive "
                f"transient failures: {exc}"
            )
            await self._fail(row, reason=reason)
        else:
            _log.warning(
                "health: transient failure %d/%d for cluster %s: %s",
                new_count, self._max_transient_failures, row.id, exc,
            )
            async with self._uow() as t:
                self.repos.clusters.set_health_failures(t, row.id, new_count, clock=self._clock)

    async def _fail(self, row: ClusterRow, *, reason: str) -> None:
        """PERMANENT (immediate) or TRANSIENT-threshold-breach -> ``HealthCheckFailed``
        -> ACTIVE -> FAILED via ``Dispatcher.apply()`` -- the ONE write path for
        cluster state. See the module docstring's "StaleVersion / mid-redeploy
        safety" for the retry/re-decide loop below."""
        _log.error("health: cluster %s failing: %s", row.id, reason)
        current = row
        for attempt in range(1, _MAX_STALE_RETRIES + 1):
            event = HealthCheckFailed(at=self._clock.now(), actor="health", reason=reason)
            try:
                result = await self._dispatcher.apply(
                    "cluster", current.id, event, record=current
                )
            except StaleVersion:
                fresh = await self._reread(current.id)
                if fresh is None or fresh.status != ClusterState.ACTIVE.value:
                    _log.info(
                        "health: cluster %s left ACTIVE before HealthCheckFailed could "
                        "apply (attempt %d, stale re-read) -- dropped, not failed",
                        current.id, attempt,
                    )
                    return
                current = fresh
                continue
            except InvalidTransition:
                # Defensive (module docstring) -- unreachable for HealthCheckFailed
                # against any real persisted row today (Observations default to
                # Ignore, not Invalid, on out-of-table cells), kept for the same
                # "logged and dropped" outcome seam-a-core.md's totality law
                # prescribes for this actor family.
                _log.info(
                    "health: cluster %s HealthCheckFailed invalid (left ACTIVE) -- dropped",
                    current.id,
                )
                return
            if not result.effects:
                # Ignore: the fresh record transition() actually saw was no longer
                # ACTIVE -- same "dropped, not failed" outcome as InvalidTransition
                # above (module docstring).
                _log.info(
                    "health: cluster %s no longer ACTIVE by apply time -- "
                    "HealthCheckFailed ignored, dropped, not failed",
                    current.id,
                )
                return
            # The FAILED transition landed -- keep counter+state consistent.
            async with self._uow() as t:
                self.repos.clusters.set_health_failures(t, current.id, 0, clock=self._clock)
            return
        _log.error(
            "health: cluster %s HealthCheckFailed lost the CAS %d times in a row this "
            "tick -- giving up, will retry next tick",
            row.id, _MAX_STALE_RETRIES,
        )

    async def _reread(self, cluster_id: str) -> ClusterRow | None:
        async with self._uow() as t:
            return self.repos.clusters.get(t, cluster_id)
