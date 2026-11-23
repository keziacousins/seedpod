"""engine/engine.py — WorkflowEngine: the run executor (docs/design/seam-b-engine.md
§2.3.2-2.3.6, amended by docs/design/coherence-review.md Conflicts 2, 4-10, 12-14).

Ownership (per this task's brief): persistence points 2-9 (point 1, admission, is the
runtime spine's run-admitter per Conflict 2 -- this module only exposes ``start``/
``resume_inflight``/``cancel`` for it to drive), resume (§2.3.3), compensation
(§2.3.4), cancellation G1-G5 (§2.3.5), job_started/job_completed/job_failed +
workflow_progress Notify rows (§2.3.6), the engine-owned gate loop, and Conflict 5's
blocked-park law for ``InfrastructureUnreachableError`` in full.

The engine is a plain-asyncio executor: one task per active run, tracked in an
in-process registry (``run_id -> _RunHandle``). It never owns a TaskGroup itself here
(the composition root's lifetime owns process shutdown); each run's task is spawned
via ``asyncio.ensure_future`` and self-removes from the registry on completion.

``SecretStr`` Output/Params fields round-trip through persisted storage via a minimal
``seedpod.services.crypto.SecretManager`` (Fernet) -- NOT the full Pillar-4 crypto
service (key rotation / ``kubeconfig_key_class`` bookkeeping), just enough that a
crash/resume gets the real value back instead of pydantic's irreversible
``"**********"`` mask. Two distinct dumps of a step's Output/Params exist throughout
this module: the ENCRYPTED form (``_encrypt_secrets``) is what ever reaches
``workflow_steps.output``/``.params``; the PLAIN form (``_plain_dump``) is what ever
reaches in-memory ``scope`` (and therefore the next step's bound ``Params``). Neither
form is ever the masked sentinel. Events/outbox payloads never carry a live
``SecretStr`` value in the first place under the current, closed event union (no
shipped event has a ``SecretStr``-typed field; V8 would reject a mismatched Ref
binding at load time regardless).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, get_args
from uuid import UUID

from pydantic import BaseModel, SecretStr
from pydantic_core import PydanticUndefined

from seedpod.core.clock import Clock
from seedpod.core.errors import InfrastructureUnreachableError, PermanentError, TransientError
from seedpod.core.events import (
    EVENT_REGISTRY,
    Event,
    InstanceDestroyFailed,
    InstanceProvisionFailed,
)
from seedpod.data.repositories import (
    ACTIVE_RUN_STATUSES,
    OutboxRepository,
    WorkflowRunRepository,
    WorkflowRunRow,
    WorkflowStepRepository,
    WorkflowStepRow,
)
from seedpod.data.uow import UnitOfWork
from seedpod.engine.cancel import CancelToken
from seedpod.engine.config import ForeachDef, Ref, StepDef, WorkflowDefinition
from seedpod.engine.errors import StepCancelled
from seedpod.engine.registry import StepRegistry
from seedpod.engine.schedule import (
    NAMED_POLICIES,
    Outcome,
    Schedule,
    classify,
    delay_seconds,
    schedule_for_undo,
)
from seedpod.engine.step import Ready, Step, StepContext, StepServices
from seedpod.services.crypto import SecretManager

__all__ = ["Sleeper", "RealSleeper", "DispatcherLike", "WorkflowEngine"]

_log = logging.getLogger(__name__)


# ----------------------------------------------------------------------------------
# The injected sleeper seam (backoff / gate interval / blocked-park re-probe waits) --
# NOT used for ctx.sleep/ctx.run_subprocess, which are the STEP's own cancel-aware
# primitives (engine/step.py) and stay wall-clock-real; this seam is purely the
# ENGINE's own waits, so crash/cancel matrix tests can run fast and deterministic.
# ----------------------------------------------------------------------------------


class Sleeper(Protocol):
    async def sleep(self, seconds: float) -> None: ...


class RealSleeper:
    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))


class DispatcherLike(Protocol):
    """The Dispatcher-shaped dependency the engine needs (coherence-review Conflict 3):
    step-emit and run-terminal transactions call ``dispatcher.apply(aggregate,
    aggregate_id, event, tx=step_tx)`` -- the real ``seedpod/runtime/dispatcher.py``
    does not exist in this tree yet (spine work); tests use a typed ``FakeDispatcher``.
    """

    async def apply(self, aggregate: str, aggregate_id: str, event: Event, *, tx: object) -> object: ...


# ----------------------------------------------------------------------------------
# Internal control-flow signals -- never escape WorkflowEngine's public methods.
# ----------------------------------------------------------------------------------


class _RunCancelledSignal(Exception):
    def __init__(self, step_path: str | None) -> None:
        super().__init__(f"run cancelled (interrupted step: {step_path!r})")
        self.step_path = step_path


class _RunFailed(Exception):
    """Raised only for the ``on_failure: compensate`` path -- the failing step's row
    and the run's transition to 'compensating' already committed atomically
    (``_fail_and_signal``); this just carries what ``_handle_failure`` needs to run
    the undo pass and the eventual terminal write."""

    def __init__(self, step_path: str, kind: str, message: str) -> None:
        super().__init__(message)
        self.step_path = step_path
        self.kind = kind
        self.message = message


class _RunAlreadyFinalized(Exception):
    """Internal signal: the run already reached a terminal status AND fired its
    outcome event, atomically, in the same transaction that recorded the failing (or
    cancelled) step -- there is no compensation phase to run in between, so
    persistence points 7 and 9 (§2.3.2) collapse into one write. Caught in ``_run``
    as a pure no-op."""


class _RunFenced(Exception):
    """Internal signal: an INSTANCE run is already terminal, and a write from its own (straggling)
    task was refused. Raised inside a write transaction, before anything is written, so the
    transaction rolls back and the finished run keeps the status it was given. Caught at the
    task's boundary (``_run_guarded``) as a no-op, and by ``_fail_unexpected`` before its fallback
    write. Cluster runs are never fenced."""


class _UnreachableExhausted(Exception):
    """Internal signal from ``_park_and_wait``; always caught inside this module.

    ``reason`` carries the LAST ``InfrastructureUnreachableError``'s message text --
    backlog #18 again, one layer up from ``_failure_message``. Without it, a run that
    parked and re-probed for the full 15-minute budget failed with nothing but
    "unreachable_budget exhausted": the engine caught, classified and discarded every
    one of those exceptions, so the single question an operator has -- unreachable
    *how*? -- was the one thing the record could not answer.
    """

    def __init__(self, step_path: str, *, reason: str = "") -> None:
        super().__init__(step_path)
        self.reason = reason


_CONTINUED = object()  # sentinel: an on_failure:continue step recorded and the run proceeds


# A run in one of these is finished. `_run` returns before doing anything to it.
_TERMINAL_RUN_STATUSES = ("succeeded", "failed", "cancelled")

# DR-0024: how long `stop()` lets live runs reach a persistence point before the
# cancel backstop. Far below any step timeout on purpose -- see `stop()`.
_DEFAULT_STOP_GRACE_SECONDS = 5.0


@dataclass
class _RunState:
    run_id: str
    wf: WorkflowDefinition
    token: CancelToken
    row: WorkflowRunRow
    scope: dict[str, Any] = field(default_factory=dict)
    undo_incomplete: list[str] = field(default_factory=list)


@dataclass
class _RunHandle:
    task: asyncio.Task
    cancel_token: CancelToken
    # Set by `_run` once the row is read: the instance this run belongs to, or None for a cluster
    # (or deployment) run. Only the hard-cancel backstop reads it, to choose its path.
    instance_id: str | None = None
    # INSTANCE runs only (DR-0055): the one hard-cancel backstop this run has, and whether it has
    # claimed the run for ending. A repeated cancel makes no second backstop; a claimed run has none.
    backstop: asyncio.Future | None = None
    abandoned: bool = False


# ----------------------------------------------------------------------------------
# Failure text -- the operator-facing `message` persisted onto a failed step/run.
# ----------------------------------------------------------------------------------


# Cap on the provider stderr appended to a failure message (backlog #18). Real
# validation errors -- the case this exists for -- run to a few hundred characters,
# so this truncates essentially never; it is a bound on a pathological script, not a
# working limit. The HEAD is kept: kubectl/API validation errors lead with the reason
# ("The Ingress "mailhog" is invalid: ..."), which is the shape smoke 8 needed. A
# shell script whose reason is in its LAST line is the case this choice loses; flip it
# if that turns out to be the common one in practice.
_MAX_STDERR_CHARS = 2000

# Cap on the terminal-outcome ``reason`` (DR-0039). Tighter than _MAX_STDERR_CHARS
# because this string lands in ``clusters.failure_reason``/``deployments.failure_reason``
# and ``api/routers/clusters.py`` serialises it straight into the SPA, which renders it
# as a single line of error text. The UNTRUNCATED message always remains in
# ``workflow_runs.error`` -- this is the operator's first answer, not their only one.
_MAX_REASON_CHARS = 500


def _outcome_reason(kind: str | None, message: str | None, status: str) -> str:
    """The terminal event's ``reason`` (DR-0039).

    Was the bare error KIND -- 'permanent' -- which told an operator the taxonomy
    bucket and nothing about the failure. The 2026-08-12 tart run is the worked
    example: a cluster sat at ``failure_reason: "permanent"`` while the actual cause
    ("install_k3s: exited 1; stderr: ... Download failed") sat one table over in
    ``workflow_runs.error``, reachable only by opening sqlite. Sixth instance of this
    repo's recurring shape -- the reason is computed, then discarded before the
    operator sees it.

    ``cancelled`` and the other no-message paths still read exactly as they did
    (Conflict 8's mapping table: cancelled -> "cancelled"), because a kind with no
    message to add IS the whole reason there."""
    base = kind if kind is not None else status
    if not message:
        return base
    reason = f"{base}: {message}"
    if len(reason) > _MAX_REASON_CHARS:
        dropped = len(reason) - _MAX_REASON_CHARS
        reason = f"{reason[:_MAX_REASON_CHARS]}… (+{dropped} chars truncated)"
    return reason


def _stderr_suffix(exc: BaseException) -> str:
    """The ``; stderr: ...`` tail appended to a provider failure, or ``""``.

    Backlog #18, and the same move DR-0033 point 1 made for gate timeouts one layer
    up: ``ProviderError.detail`` is where raw stderr / exit codes live, and nothing
    downstream of the raise ever read it. Only ``detail["stderr"]`` is lifted --
    ``exit_code`` and ``status`` are already in the classifiers' own message text
    (``"exited 2"``, ``"auth failed (403)"``), so appending them would just repeat.
    """
    detail = getattr(exc, "detail", None)
    if not isinstance(detail, Mapping):
        return ""  # not a ProviderError -- nothing attached anything
    stderr = str(detail.get("stderr", "")).strip()
    if not stderr:
        return ""
    if len(stderr) > _MAX_STDERR_CHARS:
        dropped = len(stderr) - _MAX_STDERR_CHARS
        stderr = f"{stderr[:_MAX_STDERR_CHARS]}… (+{dropped} chars truncated)"
    return f"; stderr: {stderr}"


def _exhausted_message(phase: str, exhausted: _UnreachableExhausted) -> str:
    """``"unreachable_budget exhausted during <phase>"``, plus the last probe's reason.

    Same shape as DR-0033 point 1's ``"; last poll: ..."`` gate suffix, and omitted
    the same way when there is genuinely nothing to say -- the engine reports what it
    was told and never invents a reason.
    """
    text = f"unreachable_budget exhausted during {phase}"
    return f"{text}; last probe: {exhausted.reason}" if exhausted.reason else text


def _failure_message(exc: BaseException, *, timeout: float | None = None) -> str:
    """``str(exc)``, plus any provider stderr, EXCEPT when ``str(exc)`` is empty.

    A bare ``TimeoutError`` -- what ``_try_call``'s ``asyncio.timeout`` raises when a
    step's own ``timeout_seconds`` expires -- stringifies to ``""``. Persisting that
    verbatim lands the failure in ``workflow_steps.error`` as
    ``{"kind": "transient", "message": ""}``: an operator sees that a step failed and
    gets no reason at all, not even the exception's type.

    Found by smoke 4 (2026-08-08), during a real DigitalOcean droplet-create outage:
    ``infra.create_instance`` failed twice, once on a 504 and once on a create that
    hung past the step's 60s budget, and BOTH rows read ``"message": ""`` -- the run
    history could not distinguish an upstream outage from a v2 defect, which is
    exactly the question a smoke run exists to answer.

    The synthesis is deliberately narrow: a non-empty ``str(exc)`` is used verbatim,
    so every already-diagnostic provider message (``classify_http``'s
    ``"digitalocean.create_instance: server-side failure (504)"``) survives intact.
    Only the empty case invents text, and only from facts already in hand -- the
    exception's type, plus the step budget that produced it when the caller knows it.

    **Backlog #18 (2026-08-09), the fourth instance of one recurring shape**: v2
    computed the reason and discarded it before an operator saw it. ``kubectl``'s
    classifier raises ``PermanentError("kubectl.apply_manifest: invalid input",
    detail={"stderr": ...})`` -- the stderr naming WHICH document and WHY was attached
    and then never read, so ``workflow_steps.error`` said only ``"invalid input"``.
    Diagnosing smoke 8 meant decrypting the audit blob and re-running ``kubectl apply
    --dry-run=server`` against the live cluster to recover a string this process
    already held. The stderr is now appended to every failure that carries one,
    whatever the classifier's own message says.
    """
    text = str(exc)
    if not text:
        text = (
            f"{type(exc).__name__}: step timeout of {timeout:g}s expired"
            if isinstance(exc, TimeoutError) and timeout is not None
            else type(exc).__name__
        )
    return f"{text}{_stderr_suffix(exc)}"


# ----------------------------------------------------------------------------------
# Ref resolution -- a Ref is only ever the ENTIRE value of a param (grammar V-rules;
# engine/config.py's parser already rejects nested Refs), so no recursion is needed.
# ----------------------------------------------------------------------------------


def _resolve_value(value: Any, scope: Mapping[str, Any]) -> Any:
    if isinstance(value, Ref):
        return _resolve_ref(value, scope)
    return value


def _resolve_ref(ref: Ref, scope: Mapping[str, Any]) -> Any:
    head, *rest = ref.path.split(".")
    cur = scope[head]
    for seg in rest:
        cur = cur[seg] if isinstance(cur, Mapping) else getattr(cur, seg)
    return cur


# ----------------------------------------------------------------------------------
# SecretStr <-> storage helpers (module-level: no engine state needed for the parts
# that don't touch the SecretManager).
# ----------------------------------------------------------------------------------


def _is_secretstr_type(annotation: Any) -> bool:
    return annotation is SecretStr or SecretStr in get_args(annotation)


def _plain_dump(model: BaseModel) -> dict[str, Any]:
    """The LIVE, in-memory form (scope / next-step params): SecretStr fields resolve
    to their real value -- never pydantic's ``"**********"`` mask, never ciphertext."""
    dumped = model.model_dump(mode="json")
    for name in type(model).model_fields:
        value = getattr(model, name)
        if isinstance(value, SecretStr):
            dumped[name] = value.get_secret_value()
    return dumped


class WorkflowEngine:
    """The run executor. Constructed once at the composition root with real
    dependencies; tests build it via ``tests/engine/fakes.py``'s ``build_engine``."""

    def __init__(
        self,
        *,
        definitions: Mapping[str, WorkflowDefinition],
        steps: StepRegistry,
        uow: UnitOfWork,
        run_repo: WorkflowRunRepository,
        step_repo: WorkflowStepRepository,
        outbox_repo: OutboxRepository,
        dispatcher: DispatcherLike,
        clock: Clock,
        step_services: StepServices,
        sleeper: Sleeper | None = None,
        secret_manager: SecretManager | None = None,
        resume_replay_limit: int = 5,
        cancel_grace_seconds: float = 30.0,
        unreachable_budget_seconds: float = 900.0,
        unreachable_reprobe_schedule: Sequence[float] = (5.0, 15.0, 30.0, 60.0),
    ) -> None:
        self._definitions = dict(definitions)
        self._steps = steps
        self._uow = uow
        self._run_repo = run_repo
        self._step_repo = step_repo
        self._outbox_repo = outbox_repo
        self._dispatcher = dispatcher
        self._clock = clock
        self._step_services = step_services
        self._sleeper = sleeper or RealSleeper()
        self._secret_manager = secret_manager or SecretManager()
        self._resume_replay_limit = resume_replay_limit
        self._cancel_grace_seconds = cancel_grace_seconds
        self._unreachable_budget_seconds = unreachable_budget_seconds
        self._unreachable_reprobe_schedule = tuple(unreachable_reprobe_schedule)
        self._runs: dict[str, _RunHandle] = {}
        # INSTANCE runs the hard-cancel backstop is ending right now (DR-0055). From the moment it
        # claims the run to the moment its terminal write commits, the run may have no task and
        # still be active. This set says "the engine is finishing it": the liveness sweep counts it
        # as a path, and `_adopt` refuses it, so nothing adopts it and compensates behind the
        # backstop's back. In memory only: after a crash it is empty, and `resume_inflight` adopts
        # the run (its `cancel_requested` is durable) exactly as it adopts any other.
        self._ending: set[str] = set()
        self._stopping = False  # DR-0024: one-way; a stopped engine adopts nothing
        self._ordinal_counters: dict[str, int] = {}

    # -------------------------------------------------------------------------
    # Public surface (coherence-review Conflict 2): admission itself is the
    # runtime spine's job; the engine only starts/adopts an EXISTING row.
    # -------------------------------------------------------------------------

    async def start(self, run_id: str) -> None:
        """Admits an already-inserted ``workflow_runs`` row (any resumable status,
        typically 'pending') into in-process execution."""
        await self._adopt(run_id)

    async def resume_inflight(self) -> None:
        """Startup / periodic-pass recovery (§2.3.3): adopt every non-terminal run
        without a live task -- 'pending'/'running'/'blocked'/'compensating'."""
        async with self._uow() as tx:
            rows = self._run_repo.resumable(tx)
        for row in rows:
            await self._adopt(row.id)

    async def resume_run(self, run_id: str) -> bool:
        """``resume_inflight()`` for ONE run: adopt it if it is non-terminal and has no live task.
        Returns whether a task was started. A run that is terminal, missing, or already has a live
        task is left alone, so a caller that races the executor's own hand-off (or another sweep)
        can never cause a second task for the same run. Used by the instance liveness sweep."""
        async with self._uow() as tx:
            row = self._run_repo.get(tx, run_id)
        if row is None or row.status not in ACTIVE_RUN_STATUSES:
            return False
        if self._stopping or run_id in self._runs or run_id in self._ending:
            return False
        await self._adopt(run_id)
        return run_id in self._runs

    async def cancel(self, run_id: str) -> None:
        """``cancel(run_id)`` = commit ``cancel_requested=TRUE``, THEN trip the
        in-memory token (G1: durable before acknowledged)."""
        async with self._uow() as tx:
            self._run_repo.request_cancel(tx, run_id)
        handle = self._runs.get(run_id)
        if handle is None:
            return
        handle.cancel_token.trip()
        if handle.instance_id is not None and (
            handle.abandoned or (handle.backstop is not None and not handle.backstop.done())
        ):
            return  # an instance run has ONE backstop: a repeated cancel (the destroy row's retry) adds none
        handle.backstop = asyncio.ensure_future(self._hard_cancel_backstop(handle, run_id))

    async def stop(self, *, grace_seconds: float = _DEFAULT_STOP_GRACE_SECONDS) -> None:
        """DR-0024 (ratified 2026-08-03): quiesce the engine for shutdown. Idempotent.

        **Shutdown is an INTERRUPTION, not a cancellation.** This deliberately does
        NOT trip any ``CancelToken`` and does NOT write ``cancel_requested``: that is
        the "the user cancelled this run" signal, and every ``provision-*.yml``
        declares ``on_failure: compensate``, so a ``stop()`` built on ``cancel()``
        would DESTROY every cluster this process was mid-way through provisioning --
        on every restart. Instead a run interrupted here stays non-terminal and is
        re-adopted by ``resume_inflight()`` at the next boot, indistinguishably from
        one interrupted by ``kill -9`` (the resume path must never have to tell the
        two apart). ``interrupted_count``/``resume_replay_limit`` already bound the
        replay, and an interrupted non-idempotent step already fails permanently
        rather than acting twice.

        Sets ``_stopping`` first so ``_adopt()`` refuses new work -- neither
        ``start()``/``resume_inflight()`` nor an in-flight ``EffectExecutor`` drain
        pass can spawn a run task while shutdown is in progress.

        ``asyncio.wait``, never ``asyncio.wait_for``: ``wait_for``'s timeout path
        cancels the future it guards (see commit 3ea5c94 and
        ``TimerService.stop()``); ``asyncio.wait`` cancels nothing on timeout, so the
        grace is a grace and the cancel below is the only cancel.

        ``grace_seconds`` is deliberately far below step timeouts (a real
        DigitalOcean provision measures ~185s; gates allow 600s). It exists to let a
        step already AT a persistence point commit it -- not to let a step finish.
        """
        self._stopping = True
        tasks = {handle.task for handle in self._runs.values() if not handle.task.done()}
        if not tasks:
            return
        _done, pending = await asyncio.wait(tasks, timeout=grace_seconds)
        for task in pending:
            task.cancel()
        for task in pending:
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def wait_for(self, run_id: str) -> None:
        """Test convenience: await the run's in-process task if one is live."""
        handle = self._runs.get(run_id)
        if handle is not None:
            await handle.task

    def is_running(self, run_id: str) -> bool:
        return run_id in self._runs

    def is_ending(self, run_id: str) -> bool:
        """Whether the hard-cancel backstop is ending this INSTANCE run right now: it may have no
        task, and it is not adoptable (see ``_ending``)."""
        return run_id in self._ending

    @property
    def definitions(self) -> Mapping[str, WorkflowDefinition]:
        """Read-only view of the concrete workflow definitions this engine was
        constructed with, keyed by name. Added for the runtime spine's
        run-admitter (docs/design/coherence-review.md Conflict 2 --
        ``seedpod/runtime/effect_executor.py``): admission resolves an abstract
        verb to a concrete definition name via ``WorkflowDispatch`` (Conflict 13)
        and needs THAT definition's ``.version`` to pin
        ``workflow_runs.workflow_version`` at admission time (Conflict 4). The
        engine already owns the one canonical copy of every definition; Conflict
        15's factory excerpt constructs ``EffectExecutor`` with no ``definitions=``
        argument of its own, so the admitter reads it here rather than being
        handed a second, possibly-drifting copy of the same mapping."""
        return dict(self._definitions)

    # -------------------------------------------------------------------------
    # Task lifecycle
    # -------------------------------------------------------------------------

    async def _adopt(self, run_id: str) -> None:
        # DR-0024: a stopped engine adopts nothing, so a concurrent start()/
        # resume_inflight()/executor drain cannot spawn work into a shutting-down
        # process. One-way for this instance's lifetime -- App has no
        # restart-after-stop path (build_app() is the only constructor).
        if self._stopping or run_id in self._runs or run_id in self._ending:
            return
        token = CancelToken()
        task = asyncio.ensure_future(self._run_guarded(run_id, token))
        self._runs[run_id] = _RunHandle(task=task, cancel_token=token)
        task.add_done_callback(self._on_run_done)

    def _on_run_done(self, task: asyncio.Future) -> None:
        """Deregister the finished run, and RETRIEVE its exception (DR-0045 decision 3).

        This used to be a lambda that only popped the registry, which meant an
        exception escaping ``_run`` went nowhere but asyncio's "Task exception was
        never retrieved" warning -- the only trace that a run had died. Decision 1's
        boundary should make that unreachable; this is the backstop for the case where
        the boundary itself fails, so a dead run can never be silent."""
        for run_id, handle in list(self._runs.items()):
            if handle.task is task:
                self._runs.pop(run_id, None)
                break
        else:
            run_id = "<unregistered>"
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            _log.error(
                "run %s: task ended with an unretrieved exception -- the DR-0045 "
                "boundary did not catch it, so this run may be non-terminal in the DB",
                run_id,
                exc_info=exc,
            )

    async def _hard_cancel_backstop(self, handle: _RunHandle, run_id: str) -> None:
        """G3's backstop: hard-cancel the run's asyncio task if it hasn't wound down
        within ``cancel_grace_seconds`` of the trip.

        A CLUSTER (or deployment) run stops there, as it always has: the task is cancelled and
        the run keeps whatever status it had.

        An INSTANCE run is also ended (DR-0055). Cancelling the task alone leaves the run
        ``running`` with no task, and that run holds the instance's one-active-run slot, so the
        destroy that asked for the cancel would wait for it for ever. So, after the task is
        cancelled, the engine itself finishes the run (``_abandon_instance_run``)."""
        done, _pending = await asyncio.wait({handle.task}, timeout=self._cancel_grace_seconds)
        if handle.task in done or handle.task.done():
            return
        if handle.instance_id is None:
            handle.task.cancel()
            return
        # An INSTANCE run. The backstop acts only on the run it was made for, and only while that
        # run is still active and still owned by this task: checked BEFORE anything is cancelled
        # or killed, because the instance may be on its next run by now.
        try:
            if not await self._claim_for_ending(handle, run_id):
                return
        except Exception:  # noqa: BLE001 -- nothing was cancelled; the next cancel makes a new backstop
            _log.exception("run %s: could not read the run before its hard cancel", run_id)
            return
        try:
            handle.task.cancel()
            await self._abandon_instance_run(handle, run_id)
        except Exception:  # noqa: BLE001 -- the sweep adopts or repairs it on its next pass
            _log.exception("run %s: could not end the run after its hard cancel", run_id)
        finally:
            self._ending.discard(run_id)

    async def _claim_for_ending(self, handle: _RunHandle, run_id: str) -> bool:
        """Whether the backstop may end this instance run, and if so, mark it ``_ending`` before
        anything else can happen (nothing awaits between the final check and the mark)."""
        if self._runs.get(run_id) is not handle:
            return False
        async with self._uow() as tx:
            row = self._run_repo.get(tx, run_id)
        if row is None or row.status not in ACTIVE_RUN_STATUSES:
            return False
        if self._runs.get(run_id) is not handle or handle.task.done() or handle.abandoned or run_id in self._ending:
            return False
        handle.abandoned = True
        self._ending.add(run_id)
        return True

    async def _abandon_instance_run(self, handle: _RunHandle, run_id: str) -> None:
        """End an INSTANCE run whose step ignored its cancel past the grace period: ``cancelled``,
        with the workflow's own ``cancelled`` outcome event applied to the instance, in ONE
        transaction. Compensation is skipped (the step that would run it is the one that would not
        stop). Any VM the run made is found by the destroy that follows, by its recorded ids or its
        marker (DR-0055 decision 3).

        The run's subprocesses are killed FIRST, and the run is finished after. The other order
        would let the destroy run be admitted the moment the slot frees and have ITS subprocesses
        killed by this call.

        The write re-reads the run in its own transaction and does nothing if it is already
        terminal (the run's own task may have finished at the same moment: the single-writer
        lock makes one of the two win, never both). Once it commits, the run is terminal and
        every later write from a straggling task of this run is refused (``_assert_live``)."""
        instance_id = handle.instance_id
        assert instance_id is not None
        manager = self._step_services.subprocess_manager
        # `SubprocessManagerLike` only promises register/unregister (a test fake need not kill
        # anything), so the kill is used when the real manager offers it. It is scoped to THIS RUN,
        # never to the instance: the instance's next run (the destroy) has processes of its own.
        terminate = getattr(manager, "terminate_for_run", None)
        if terminate is not None:
            try:
                await terminate(run_id)
            except Exception:  # noqa: BLE001 -- a failed kill must not keep the run alive
                _log.exception("run %s: terminating the subprocesses of the run failed (instance %s)", run_id, instance_id)
        message = (
            f"cancelled: a step did not stop within {self._cancel_grace_seconds:g}s of the cancel; "
            "the engine ended the run"
        )
        error = {"kind": "cancelled", "step": None, "message": message}
        try:
            async with self._uow() as tx:
                row = self._run_repo.get(tx, run_id)
                if row is None or row.status not in ACTIVE_RUN_STATUSES:
                    return  # the run's own task finished it first
                wf = self._definitions.get(row.workflow)
                if wf is None:
                    raise LookupError(f"unknown workflow {row.workflow!r}")
                error = {"kind": "cancelled", "step": row.failed_step, "message": message}
                self._run_repo.update(tx, run_id, error=error)
                state = _RunState(
                    run_id=run_id, wf=wf, token=handle.cancel_token, row=row, scope={"run": dict(row.args)}
                )
                await self._apply_terminal(tx, state, "cancelled", "cancelled", message)
            return
        except Exception:  # noqa: BLE001 -- the workflow's own outcome could not be built or applied
            _log.exception("run %s: the cancelled outcome failed; the run is ended by its name instead", run_id)
        # A fresh transaction: the one above rolled back with its exception.
        async with self._uow() as tx:
            row = self._run_repo.get(tx, run_id)
            if row is None or row.status not in ACTIVE_RUN_STATUSES:
                return
            self._run_repo.update(tx, run_id, status="cancelled", error=error, finished_at=self._clock.now())
            await self._apply_instance_failure_in_tx(
                tx, row, run_id, _outcome_reason("cancelled", message, "cancelled")
            )

    # -------------------------------------------------------------------------
    # The run's task body -- the total function over crash states (§2.3.3)
    # -------------------------------------------------------------------------

    async def _run_guarded(self, run_id: str, token: CancelToken) -> None:
        """The run task's body: ``_run``, with a refused write from a FINISHED instance run ended
        quietly. (See ``_RunFenced``: only an instance run, whose engine-ended terminal status a
        straggling step must not overwrite, ever raises it.)"""
        try:
            await self._run(run_id, token)
        except _RunFenced:
            _log.warning("run %s: already finished by the engine; a later write from its task was refused", run_id)

    def _assert_live(self, tx: Any, state: _RunState) -> None:
        """INSTANCE runs only: refuse to write for a run that is already terminal. The check reads the
        run's row in the SAME transaction as the write that follows, and the single-writer lock makes
        the pair atomic against ``_abandon_instance_run``, the one place a run is ended behind its
        task's back. A cluster or deployment run never reads or raises here."""
        if state.row.instance_id is None:
            return
        current = self._run_repo.get(tx, state.run_id)
        if current is None or current.status in _TERMINAL_RUN_STATUSES:
            raise _RunFenced(state.run_id)

    @contextlib.asynccontextmanager
    async def _tx(self, state: _RunState):
        """``self._uow()``, for a write made on behalf of a run's own task: fenced for an instance run."""
        async with self._uow() as tx:
            self._assert_live(tx, state)
            yield tx

    async def _run(self, run_id: str, token: CancelToken) -> None:
        async with self._uow() as tx:
            row = self._run_repo.get(tx, run_id)
            if row is not None:
                # Seeds the Notify ordinal counter from what's already durable, so a
                # fresh engine instance (a real process restart) never reuses an
                # ordinal a pre-crash process already wrote (effects_outbox.effect_id
                # is UNIQUE) -- e.g. job_started pre-crash, job_completed post-resume.
                existing_notify_rows = self._outbox_repo.list_for_aggregate(tx, "run", run_id)
                self._ordinal_counters[run_id] = len(existing_notify_rows)
        if row is None:
            return
        handle = self._runs.get(run_id)
        if handle is not None:
            handle.instance_id = row.instance_id
        if row.instance_id is not None and row.status in _TERMINAL_RUN_STATUSES:
            # INSTANCE runs only. A terminal status is a final answer. Nothing below may touch a
            # finished run: not the unknown-workflow failure, not the missing-input failure (which
            # would apply the outcome event a second time), not a resume. `start()` reaches here on a
            # replayed admission, so a `succeeded` run is never rewritten to `failed`. (A cluster
            # run is unchanged: it takes the path it took before DR-0055.)
            return
        wf = self._definitions.get(row.workflow)
        if wf is None:
            message = f"unknown workflow {row.workflow!r}"
            async with self._uow() as tx:
                self._run_repo.update(
                    tx,
                    run_id,
                    status="failed",
                    error={"kind": "permanent", "step": None, "message": message},
                    finished_at=self._clock.now(),
                )
                if row.instance_id is not None:
                    # No definition means no `outcome:` block to read. But an instance in `destroying`
                    # (or `provisioning`) is waiting on this run's report, and nothing else will send
                    # it, so the report is built from the workflow's name. (A cluster run takes the
                    # path it always did.)
                    await self._apply_instance_failure_in_tx(tx, row, run_id, _outcome_reason("permanent", message, "failed"))
            return

        state = _RunState(run_id=run_id, wf=wf, token=token, row=row, scope={"run": dict(row.args)})

        # DR-0045 decision 4. A run's args are frozen at admission, so a definition
        # that has since gained a required input leaves already-admitted runs
        # unsatisfiable -- and every binding onto the missing name would raise a bare
        # `KeyError` deep in `_build_params`, which before decision 1 wedged the run
        # non-terminal forever. Checked HERE, before any step runs, so the failure
        # names the input instead of a traceback, and so a resume that cannot possibly
        # succeed says why on its first attempt rather than its hundredth.
        #
        # This is not hypothetical: DR-0043 erratum E3 shipped a `snapshot` input that
        # `DispatchTable.resolve` did not supply, and recovering the stranded run
        # needed a hand-written UPDATE against production.
        missing_inputs = sorted(set(wf.inputs) - set(row.args))
        if missing_inputs:
            await self._fail_unexpected(
                state,
                message=(
                    f"run args do not satisfy workflow {wf.workflow!r} v{wf.version}: "
                    f"missing declared input(s) {missing_inputs}. The args were frozen when "
                    f"this run was admitted; a definition change since then cannot be applied "
                    f"to it retroactively."
                ),
            )
            return
        # 'blocked' is ambiguous on its own (Conflict 5 rule 5 says "adopt like
        # running", but a run can also be parked mid-COMPENSATION -- Conflict 5 rules
        # 1-3 apply the same park law to undo). The schema (Conflict 4, final) has no
        # spare column to record which; `failed_step` is only ever set at the same
        # atomic moment the run leaves forward execution (§2.3.2 point 7), so its
        # presence is a reliable, DDL-compatible discriminator: blocked+failed_step
        # unset -> parked mid-forward; blocked+failed_step set -> parked mid-undo.
        compensating_resume = row.status == "compensating" or (row.status == "blocked" and row.failed_step is not None)
        forward_resume = row.status == "running" or (row.status == "blocked" and row.failed_step is None)
        try:
            if row.cancel_requested and row.status == "pending":
                raise _RunCancelledSignal(step_path=None)
            if row.cancel_requested and forward_resume:
                # Compensation is not cancellable (§2.3.4) -- a cancel_requested flag
                # observed while resuming into the compensating branch below must NOT
                # re-trigger G1 here; it is simply ignored (the compensate pass already
                # in flight finishes to a terminal status regardless).
                raise _RunCancelledSignal(step_path=None)
            if row.status == "pending":
                await self._mark_started(state)
                await self._forward(state)
            elif forward_resume:
                await self._rebuild_scope(state)
                await self._forward(state)
            elif compensating_resume:
                await self._rebuild_scope(state)
                await self._compensate(state)
                error_kind = (row.error or {}).get("kind") if row.error else None
                error_message = (row.error or {}).get("message") if row.error else None
                target = "cancelled" if error_kind == "cancelled" else "failed"
                await self._finalize(state, target, error_kind=error_kind, error_message=error_message)
                return
            else:
                return  # already terminal -- nothing to adopt (defensive)
        except _RunCancelledSignal as sig:
            await self._handle_cancel(state, sig.step_path)
            return
        except _RunAlreadyFinalized:
            return
        except _RunFenced:
            raise  # `_run_guarded` ends it quietly: an instance run the engine already finished
        except _RunFailed as fail:
            await self._handle_failure(state, fail)
            return
        except Exception as exc:  # noqa: BLE001 -- DR-0045 decision 1, see _fail_unexpected
            await self._fail_unexpected(state, exc=exc)
            return
        await self._succeed(state)

    async def _fail_unexpected(
        self, state: _RunState, *, exc: BaseException | None = None, message: str | None = None
    ) -> None:
        """DR-0045 decision 1: an exception the engine does not already classify still
        ends the run TERMINALLY, instead of leaving it at ``running`` forever.

        **It does not compensate (decision 2, ratified explicitly).** An unanticipated
        exception means the engine cannot know what state it is in, and running undo
        steps from there could do real harm to real infrastructure. The run fails
        loudly and a human decides. That is a deliberate asymmetry with the ordinary
        step-failure path, not an oversight.

        Decision 5 is why this routes through ``_apply_terminal`` rather than just
        writing a status: that is what emits the workflow's ``outcome.failed`` event
        through the Dispatcher, so the aggregate follows the run. A destroy that
        crashed leaves its cluster in DESTROY_FAILED -- which already exists and
        already has a retry route (``DESTROY_FAILED x DestroyRequested``). The point
        was never tidier bookkeeping; it is that the machine's existing recovery paths
        become reachable at all. Before this, nothing ever told the machine the run
        had died, so a live droplet sat in ``destroying`` and billed until a human
        noticed (2026-08-16).
        """
        detail = message if message is not None else f"{type(exc).__name__}: {exc}"
        _log.error("run %s failed unexpectedly: %s", state.run_id, detail, exc_info=exc)
        error = {"kind": "permanent", "step": None, "message": detail}
        try:
            async with self._tx(state) as tx:
                self._run_repo.update(tx, state.run_id, error=error)
                await self._apply_terminal(tx, state, "failed", "permanent", detail)
        except _RunFenced:
            raise  # an instance run the engine already ended: nothing may be written for it (`_run_guarded`)
        except Exception:  # noqa: BLE001 -- last-resort, see below
            # If the terminal write ITSELF fails (a malformed outcome block, a
            # scope-dependent payload the crash already poisoned, a DB error), fall
            # back to the smallest thing that still gets the run out of `running`.
            # No outcome event, so the aggregate does NOT follow -- that is worth
            # saying out loud rather than swallowing, because a terminal run whose
            # cluster is still mid-flight is exactly the inconsistency an operator
            # needs to know to look for.
            _log.exception(
                "run %s: terminal write failed after an unexpected error; falling back to a "
                "status-only update -- the aggregate will NOT have been advanced and may need "
                "manual reconciliation",
                state.run_id,
            )
            with contextlib.suppress(Exception):
                async with self._tx(state) as tx:
                    self._run_repo.update(
                        tx, state.run_id, status="failed", error=error, finished_at=self._clock.now()
                    )
                    if state.row.instance_id is not None:
                        # An instance run still reports, from its name, so the instance does not wait
                        # forever on a run that has ended (see `_apply_instance_failure_in_tx`).
                        await self._apply_instance_failure_in_tx(
                            tx, state.row, state.run_id, _outcome_reason("permanent", detail, "failed")
                        )

    async def _apply_instance_failure_in_tx(self, tx: Any, row: Any, run_id: str, reason: str) -> None:
        """Report a run that ended without a usable definition (or whose own terminal write failed)
        to its INSTANCE, in the caller's transaction: ``destroy*`` -> ``InstanceDestroyFailed``
        (``destroy-failed``, which arms the retry), ``provision*`` -> ``InstanceProvisionFailed``
        (``failed``, whose expiry is still armed). Any other name has no report to send, and is
        logged: an instance run is only ever one of these two."""
        at, actor = self._clock.now(), f"engine:run:{run_id}"
        if row.workflow.startswith("destroy"):
            event: Event = InstanceDestroyFailed(at=at, actor=actor, reason=reason)
        elif row.workflow.startswith("provision"):
            event = InstanceProvisionFailed(at=at, actor=actor, reason=reason)
        else:
            _log.error("run %s: workflow %r is neither a provision nor a destroy; no report for instance %s",
                       run_id, row.workflow, row.instance_id)
            return
        await self._dispatcher.apply("instance", row.instance_id, event, tx=tx)

    async def _mark_started(self, state: _RunState) -> None:
        async with self._tx(state) as tx:
            self._run_repo.update(tx, state.run_id, status="running", started_at=self._clock.now())
            await self._emit_job_notify_in_tx(tx, state, "job_started")

    def _scope_value_from_row(self, row: WorkflowStepRow) -> dict[str, Any]:
        """Rehydrates a persisted step's output into scope-ready PLAIN form (real
        secret values, never ciphertext) -- §2.3.3's "rebuild bindings from persisted
        output ... resume gets byte-identical inputs" means the same plain value a
        live run would have put in scope, not the ciphertext the DB stores at rest.

        A 'failed_continued' row (§2.3.2 point 7) may carry no recorded output at all
        -- its ``execute()`` raised before ever returning one -- yet Seam B V4 only
        allows a downstream Ref to bind one of its fields when that field's static
        type is itself ``Optional[T]`` (e.g. ``cluster.load_kubeconfig_optional``'s
        ``kubeconfig: SecretStr | None = None``, referenced by destroy-cloud.yml's
        ``tailscale`` step). For any field the row didn't record, fall back to the
        Output model's own declared default so that binding resolves to the same
        "no-op" value V4 already promises is safe -- never a synthesized value for a
        field with no default, which stays absent and still surfaces as a clear
        KeyError (a genuine workflow-authoring bug) rather than a silent guess."""
        step = self._steps.get(row.verb)
        if step is None:
            return dict(row.output or {})
        decrypted = self._decrypt_secrets_for(step.Output, row.output or {})
        for name, f in step.Output.model_fields.items():
            if name in decrypted:
                continue
            if f.default is not PydanticUndefined:
                decrypted[name] = f.default
            elif f.default_factory is not None:
                decrypted[name] = f.default_factory()
        return decrypted

    async def _reload_scope_value(self, state: _RunState, step_path: str) -> dict[str, Any]:
        """Re-reads a just-committed step row and rehydrates it via
        ``_scope_value_from_row`` -- used for the 'failed_continued' live-run flavor
        so a fresh (just-failed) step gets the SAME scope value a resumed run would
        derive for it (see the call sites in ``_advance_top_level_step`` /
        ``_advance_foreach``), instead of leaving no scope entry at all and crashing
        a downstream Ref's ``_build_params`` with a bare ``KeyError``."""
        async with self._uow() as tx:
            row = self._step_repo.get(tx, state.run_id, step_path)
        return self._scope_value_from_row(row) if row is not None else {}

    async def _rebuild_scope(self, state: _RunState) -> None:
        """§2.3.3: "rebuild bindings from persisted workflow_steps.output ... never
        in-memory state, so resume gets byte-identical inputs." Only TOP-LEVEL step
        outputs are scope-addressable (foreach body outputs are per-iteration-local;
        ``_advance_foreach`` re-derives them directly from the DB per iteration)."""
        async with self._uow() as tx:
            rows = self._step_repo.list_for_run(tx, state.run_id)
        for row in rows:
            if row.status in ("succeeded", "failed_continued") and row.output is not None and "[" not in row.step_path:
                state.scope[row.step_path] = self._scope_value_from_row(row)

    # -------------------------------------------------------------------------
    # Forward execution (§2.3.2 persistence points 2, 4, 5, 6; §2.2 foreach)
    # -------------------------------------------------------------------------

    async def _forward(self, state: _RunState) -> None:
        async with self._uow() as tx:
            existing = {s.step_path: s for s in self._step_repo.list_for_run(tx, state.run_id)}
        for entry in state.wf.steps:
            if isinstance(entry, StepDef):
                await self._advance_top_level_step(state, entry, existing)
            else:
                await self._advance_foreach(state, entry, existing)

    async def _advance_top_level_step(
        self, state: _RunState, step_def: StepDef, existing: Mapping[str, WorkflowStepRow]
    ) -> None:
        step_path = step_def.id
        row = existing.get(step_path)
        if row is not None and row.status in ("succeeded", "failed_continued"):
            state.scope[step_def.id] = self._scope_value_from_row(row)
            return
        result = await self._run_step(state, step_def, step_path, state.scope, row)
        if result is _CONTINUED:
            # §2.3.3 resume rule: a downstream Ref bound to a failed_continued step
            # must see byte-identical bindings whether it runs live or after a
            # resume. The resume path (the branch above, and _rebuild_scope) both
            # populate scope for 'failed_continued' rows via _scope_value_from_row;
            # the live path must do the same instead of leaving the key absent
            # (which used to crash the next step's _build_params with a bare
            # KeyError and permanently wedge the run non-terminal).
            state.scope[step_def.id] = await self._reload_scope_value(state, step_path)
        else:
            state.scope[step_def.id] = result

    async def _advance_foreach(
        self, state: _RunState, entry: ForeachDef, existing: Mapping[str, WorkflowStepRow]
    ) -> None:
        items = _resolve_ref(entry.items, state.scope)
        for i, item in enumerate(items):
            child_scope = dict(state.scope)
            child_scope[entry.as_] = item
            for body_step in entry.body:
                step_path = f"{entry.id}[{i}].{body_step.id}"
                row = existing.get(step_path)
                if row is not None and row.status in ("succeeded", "failed_continued"):
                    child_scope[body_step.id] = self._scope_value_from_row(row)
                    continue
                result = await self._run_step(state, body_step, step_path, child_scope, row)
                if result is _CONTINUED:
                    # Same fix as the top-level branch above, for foreach bodies.
                    child_scope[body_step.id] = await self._reload_scope_value(state, step_path)
                else:
                    child_scope[body_step.id] = result

    async def _run_step(
        self,
        state: _RunState,
        step_def: StepDef,
        step_path: str,
        scope: Mapping[str, Any],
        existing: WorkflowStepRow | None,
    ) -> Any:
        step = self._steps.get(step_def.uses)
        schedule = self._resolve_schedule(step_def.retry, step.default_retry)
        timeout = step_def.timeout_seconds or step.default_timeout_seconds

        if existing is None:
            params = self._build_params(step, step_def, scope)
            await self._insert_step_row(state, step_path, step.verb, params)
        elif existing.status == "gating":
            new_interrupted = existing.interrupted_count + 1
            if new_interrupted > self._resume_replay_limit:
                return await self._fail_and_signal(
                    state, step_def, step_path, kind="permanent",
                    message=f"resume_replay_limit={self._resume_replay_limit} exceeded", output=existing.output,
                )
            async with self._tx(state) as tx:
                self._step_repo.update(tx, state.run_id, step_path, interrupted_count=new_interrupted)
            params = step.Params(**self._decrypt_secrets_for(step.Params, existing.params))
            provisional = step.Output(**self._decrypt_secrets_for(step.Output, existing.output or {}))
            return await self._run_gate(state, step, step_def, step_path, params, provisional, scope, existing.attempt)
        else:  # "running": crash mid-execute, OR resumed-from-'blocked' (Conflict 5 rule 5)
            params = step.Params(**self._decrypt_secrets_for(step.Params, existing.params))
            if not step.idempotent:
                return await self._fail_and_signal(
                    state, step_def, step_path, kind="permanent", message="interrupted; non-idempotent", output=None
                )
            new_interrupted = existing.interrupted_count + 1
            if new_interrupted > self._resume_replay_limit:
                return await self._fail_and_signal(
                    state, step_def, step_path, kind="permanent",
                    message=f"resume_replay_limit={self._resume_replay_limit} exceeded", output=None,
                )
            async with self._tx(state) as tx:
                self._step_repo.update(tx, state.run_id, step_path, interrupted_count=new_interrupted)

        start_attempt = existing.attempt if existing is not None else 1
        provisional = await self._execute_with_retries(state, step, step_def, step_path, params, schedule, timeout, start_attempt)
        if provisional is _CONTINUED:
            return _CONTINUED
        encrypted = self._encrypt_secrets(provisional)
        plain = _plain_dump(provisional)
        if step_def.gate is not None:
            await self._persist_step_gating(state, step_path, encrypted)
            attempt = await self._current_attempt(state, step_path)
            return await self._run_gate(state, step, step_def, step_path, params, provisional, scope, attempt)
        await self._persist_step_succeeded(state, step_def, step_path, encrypted, {**scope, step_def.id: plain})
        return plain

    def _build_params(self, step: Step, step_def: StepDef, scope: Mapping[str, Any]) -> Any:
        merged = {k: _resolve_value(v, scope) for k, v in step_def.with_.items()}
        return step.Params(**merged)

    def _resolve_schedule(self, retry: Any, default: Schedule) -> Schedule:
        if retry is None:
            return default
        if isinstance(retry, str):
            return NAMED_POLICIES[retry]
        return Schedule(
            max_attempts=retry.max_attempts,
            base_delay_seconds=retry.base_delay_seconds,
            factor=retry.factor,
            max_delay_seconds=retry.max_delay_seconds,
        )

    async def _current_attempt(self, state: _RunState, step_path: str) -> int:
        async with self._uow() as tx:
            row = self._step_repo.get(tx, state.run_id, step_path)
        return row.attempt if row is not None else 1

    # -------------------------------------------------------------------------
    # SecretStr <-> storage (needs self._secret_manager; module-level helpers above
    # cover the parts that don't).
    # -------------------------------------------------------------------------

    def _encrypt_secrets(self, model: BaseModel) -> dict[str, Any]:
        """The DB-persisted form (``workflow_steps.output``/``.params``): SecretStr
        fields ciphertext via ``SecretManager`` -- never plaintext, never pydantic's
        irreversible ``"**********"`` mask."""
        dumped = model.model_dump(mode="json")
        for name in type(model).model_fields:
            value = getattr(model, name)
            if isinstance(value, SecretStr):
                dumped[name] = self._secret_manager.encrypt(value.get_secret_value())
        return dumped

    def _decrypt_secrets_for(self, model_cls: type[BaseModel], data: Mapping[str, Any]) -> dict[str, Any]:
        """Inverse of ``_encrypt_secrets``, keyed by ``model_cls``'s declared field
        types (an Output/Params class) since a bare dict carries no type info of its
        own. Used everywhere a persisted params/output row is rehydrated: resume,
        undo, and scope-rebuild."""
        out = dict(data)
        for name, f in model_cls.model_fields.items():
            if name in out and out[name] is not None and _is_secretstr_type(f.annotation):
                out[name] = self._secret_manager.decrypt(out[name])
        return out

    # -------------------------------------------------------------------------
    # Persistence helpers (points 2, 3, 5, 6, 7)
    # -------------------------------------------------------------------------

    async def _insert_step_row(self, state: _RunState, step_path: str, verb: str, params: Any) -> None:
        async with self._tx(state) as tx:
            run_row = self._run_repo.get(tx, state.run_id)
            if run_row is not None and run_row.cancel_requested:
                raise _RunCancelledSignal(step_path=None)  # G2: DB-serialized cancel-vs-step-start
            self._step_repo.insert(
                tx,
                WorkflowStepRow(
                    run_id=state.run_id, step_path=step_path, verb=verb, status="running",
                    attempt=1, interrupted_count=0, params=self._encrypt_secrets(params),
                    notes={}, output=None, undo_status=None, error=None,
                    started_at=self._clock.now(), finished_at=None,
                ),
            )

    async def _persist_attempt(self, state: _RunState, step_path: str, attempt: int) -> None:
        async with self._tx(state) as tx:
            self._step_repo.update(tx, state.run_id, step_path, attempt=attempt)

    async def _persist_step_gating(self, state: _RunState, step_path: str, output_dict: Mapping[str, Any]) -> None:
        async with self._tx(state) as tx:
            self._step_repo.update(tx, state.run_id, step_path, status="gating", output=output_dict)

    async def _persist_step_succeeded(
        self, state: _RunState, step_def: StepDef, step_path: str, output_dict: Mapping[str, Any], scope_with_self: Mapping[str, Any]
    ) -> None:
        async with self._tx(state) as tx:
            self._step_repo.update(tx, state.run_id, step_path, status="succeeded", output=output_dict, finished_at=self._clock.now())
            if step_def.emit is not None:
                await self._apply_event_in_tx(tx, state, step_def.emit.event, step_def.emit.payload, scope_with_self)

    async def _fail_and_signal(
        self, state: _RunState, step_def: StepDef, step_path: str, *, kind: str, message: str, output: Mapping[str, Any] | None
    ) -> Any:
        """Persistence point 7 (§2.3.2), made atomic: the failing step's row AND the
        run-level transition commit in ONE transaction (fixes resume re-executing a
        permanently-failed step after a crash landed between two separate writes).
        When the workflow has no compensation phase to run in between (on_failure:
        'report', or a step-level on_failure: 'continue' that never transitions the
        run at all), point 9 (terminal status + outcome event, §2.3.2) collapses into
        this SAME transaction too, since nothing legitimately intervenes -- a crash
        right after must never leave a terminal run with its outcome event lost.

        Returns ``_CONTINUED`` for the step-level continue flavor (caller keeps going
        forward); otherwise ALWAYS raises: ``_RunAlreadyFinalized`` when this call
        already reached and recorded a terminal run status, or ``_RunFailed`` for the
        on_failure='compensate' path (the only case with more work -- the undo pass --
        still to do)."""
        continued = step_def.on_failure == "continue"
        step_status = "failed_continued" if continued else "failed"
        run_status: str | None = None
        async with self._tx(state) as tx:
            self._step_repo.update(
                tx, state.run_id, step_path, status=step_status, error={"kind": kind, "message": message},
                output=output, finished_at=self._clock.now(),
            )
            if not continued:
                run_status = "compensating" if state.wf.on_failure == "compensate" else "failed"
                self._run_repo.update(
                    tx, state.run_id, status=run_status, failed_step=step_path,
                    error={"kind": kind, "step": step_path, "message": message},
                )
                if run_status != "compensating":
                    await self._apply_terminal(tx, state, "failed", kind, message)
        if continued:
            return _CONTINUED
        if run_status != "compensating":
            raise _RunAlreadyFinalized()
        raise _RunFailed(step_path, kind, message)

    async def _fail_unreachable_and_finalize(self, state: _RunState, step_path: str, *, message: str, output: Mapping[str, Any] | None) -> None:
        """Conflict 5 rule 3, forward-step branch, made atomic: the failing step's
        row, every step's ``undo_status='skipped'`` (compensation entirely bypassed --
        this NEVER runs the normal compensate pass, even for an on_failure:compensate
        workflow), the run's terminal 'failed' status, and the outcome event all
        commit in ONE transaction. A crash here must never resume into re-executing
        the permanently failed step, nor lose the outcome event."""
        async with self._tx(state) as tx:
            self._step_repo.update(
                tx, state.run_id, step_path, status="failed", error={"kind": "unreachable", "message": message},
                output=output, finished_at=self._clock.now(),
            )
            for s in self._step_repo.list_for_run(tx, state.run_id):
                if s.undo_status is None:
                    self._step_repo.update(tx, state.run_id, s.step_path, undo_status="skipped")
            self._run_repo.update(
                tx, state.run_id, status="failed", failed_step=step_path,
                error={"kind": "unreachable", "step": step_path, "message": "unreachable_budget exhausted"},
            )
            # `message` (from _exhausted_message) carries the last probe's reason --
            # backlog #18's fifth instance. The run row's own error text stays the
            # terse "unreachable_budget exhausted"; the operator-facing reason gets
            # the richer one.
            await self._apply_terminal(tx, state, "failed", "unreachable", message)

    # -------------------------------------------------------------------------
    # Execute + retry loop (§2.3.1 Schedule; Conflict 5 blocked-park via
    # _probe_with_park; Conflict 6 retry_after override)
    # -------------------------------------------------------------------------

    async def _execute_with_retries(
        self, state: _RunState, step: Step, step_def: StepDef, step_path: str, params: Any,
        schedule: Schedule, timeout: float, start_attempt: int,
    ) -> Any:
        ctx_attempt = start_attempt
        try:
            while True:
                state.token.raise_if_cancelled()
                ctx = self._make_ctx(state, step_path, ctx_attempt, state.token)
                try:
                    provisional = await self._probe_with_park(
                        state, step_path, token=state.token,
                        probe=lambda ctx=ctx: self._try_call(step.execute(params, ctx), timeout),
                    )
                except _UnreachableExhausted as exhausted:
                    await self._fail_unreachable_and_finalize(
                        state, step_path,
                        message=_exhausted_message("execute", exhausted), output=None,
                    )
                    raise _RunAlreadyFinalized() from None
                except StepCancelled:
                    raise  # caught by the outer try below, wherever it originated
                except Exception as exc:  # noqa: BLE001 -- classify() is the fixed taxonomy
                    outcome = classify(exc)
                    if outcome is Outcome.RETRY and ctx_attempt < schedule.max_attempts:
                        ctx_attempt += 1
                        await self._persist_attempt(state, step_path, ctx_attempt)
                        retry_after = getattr(exc, "retry_after", None)
                        await self._cancel_aware_wait(delay_seconds(schedule, ctx_attempt, retry_after=retry_after), state.token)
                        continue
                    kind = "transient" if outcome is Outcome.RETRY else "permanent"
                    return await self._fail_and_signal(
                        state, step_def, step_path, kind=kind,
                        message=_failure_message(exc, timeout=timeout), output=None,
                    )
                else:
                    return provisional
        except StepCancelled as exc:
            # G2/G4: wherever cancellation landed inside this loop -- the top-of-loop
            # check, mid-execute (via _probe_with_park), or the backoff wait itself --
            # it converts here, uniformly, into the run-cancellation signal.
            raise _RunCancelledSignal(step_path) from exc

    async def _try_call(self, coro: Any, timeout: float) -> Any:
        async with asyncio.timeout(timeout):
            return await coro

    async def _probe_with_park(self, state: _RunState, step_path: str, probe: Any, token: CancelToken) -> Any:
        try:
            return await probe()
        except InfrastructureUnreachableError:
            return await self._park_and_wait(state, step_path, probe, token)

    async def _park_and_wait(self, state: _RunState, step_path: str, probe: Any, token: CancelToken) -> Any:
        """Conflict 5's blocked-park law: parks the run (status='blocked'), re-probes
        on the 5s/15s/30s/60s-cap schedule up to ``unreachable_budget_seconds``
        (default 15 min), and restores the prior status once a non-Unreachable
        outcome is produced. Raises ``_UnreachableExhausted`` once the budget runs
        out. Never consumes the caller's Schedule budget.

        ``token`` is whichever token the CALLER is operating under -- the run's own
        token for execute/gate, or a FRESH non-tripped one for undo (§2.3.4:
        compensation is not cancellable; using the run's token here for undo would
        let a cancel-triggered compensation's own Unreachable retries be short-
        circuited by the very token that triggered them, skipping the re-probe
        schedule entirely)."""
        async with self._tx(state) as tx:
            current = self._run_repo.get(tx, state.run_id)
            prior_status = current.status if current is not None else "running"
            self._run_repo.update(tx, state.run_id, status="blocked")
        remaining = self._unreachable_budget_seconds
        idx = 0
        try:
            while True:
                token.raise_if_cancelled()
                delay = min(self._unreachable_reprobe_schedule[min(idx, len(self._unreachable_reprobe_schedule) - 1)], remaining)
                if delay > 0:
                    await self._cancel_aware_wait(delay, token)
                remaining -= delay
                idx += 1
                try:
                    return await probe()
                except InfrastructureUnreachableError as exc:
                    if remaining <= 0:
                        raise _UnreachableExhausted(step_path, reason=_failure_message(exc)) from None
                    continue
        finally:
            async with self._tx(state) as tx:
                row = self._run_repo.get(tx, state.run_id)
                if row is not None and row.status == "blocked":
                    self._run_repo.update(tx, state.run_id, status=prior_status)

    async def _cancel_aware_wait(self, seconds: float, token: CancelToken) -> None:
        """Engine-owned wait (backoff / gate interval / park re-probe): races the
        injected ``Sleeper`` against the token so a cancel lands promptly (§2.3.2
        point 3: "cancel-aware backoff sleep"; G2/G3)."""
        token.raise_if_cancelled()
        if seconds <= 0:
            return
        sleep_task = asyncio.ensure_future(self._sleeper.sleep(seconds))
        cancel_task = asyncio.ensure_future(token.wait())
        done, pending = await asyncio.wait({sleep_task, cancel_task}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
        if cancel_task in done:
            raise StepCancelled("cancelled during engine-owned wait")

    def _make_ctx(self, state: _RunState, step_path: str, attempt: int, token: CancelToken) -> StepContext:
        # A run belongs to exactly one aggregate (WorkflowRunRow's invariant, the table's CHECK).
        # The context carries that one id under its own name: an instance run never presents
        # its id as a cluster's (DR-0055).
        return StepContext(
            run_id=UUID(state.run_id), cluster_id=state.row.cluster_id, instance_id=state.row.instance_id,
            workflow=state.wf.workflow,
            step_path=step_path, attempt=attempt, cancel=token, services=self._step_services,
            note_sink=self._note_sink, progress_sink=self._progress_sink,
        )

    # -------------------------------------------------------------------------
    # Gate loop (engine-owned interval/timeout/hysteresis/cancel-between-polls;
    # Conflict 5: Unreachable polls do NOT feed the hysteresis counter and the
    # overall timeout clock is SUSPENDED while blocked)
    # -------------------------------------------------------------------------

    async def _run_gate(
        self, state: _RunState, step: Step, step_def: StepDef, step_path: str, params: Any,
        provisional: Any, scope: Mapping[str, Any], attempt: int,
    ) -> Any:
        gate = step_def.gate
        assert gate is not None
        gate_timeout = float(_resolve_value(gate.timeout_seconds, scope))
        interval = float(gate.interval_seconds)
        max_failures = gate.max_consecutive_poll_failures
        consecutive_failures = 0
        elapsed = 0.0
        current = provisional
        # DR-0033 point 1: the most recent non-empty `NotReady.detail`, so a gate that
        # gives up can say WHY it never became ready. `NotReady` has carried `detail`
        # since Pillar 2 and five step sites populate it (`deploy.await_wave` computes
        # the exact list of services that never came up) -- the gate simply threw it
        # away, which is how smoke 5's `k3s.await_ssh` failure reported a bare timeout
        # while every poll underneath it was getting EHOSTUNREACH from the kernel.
        last_detail = ""
        try:
            while True:
                state.token.raise_if_cancelled()
                ctx = self._make_ctx(state, step_path, attempt, state.token)
                try:
                    result = await self._probe_with_park(
                        state, step_path, token=state.token,
                        probe=lambda ctx=ctx, current=current: step.poll_ready(params, current, ctx),
                    )
                except _UnreachableExhausted as exhausted:
                    await self._fail_unreachable_and_finalize(
                        state, step_path, message=_exhausted_message("gate poll", exhausted),
                        output=self._encrypt_secrets(current),
                    )
                    raise _RunAlreadyFinalized() from None
                except StepCancelled:
                    raise  # caught by the outer try below
                except PermanentError as exc:
                    return await self._fail_and_signal(
                        state, step_def, step_path, kind="permanent",
                        message=_failure_message(exc), output=self._encrypt_secrets(current),
                    )
                except TransientError as exc:
                    consecutive_failures += 1
                    if consecutive_failures >= max_failures:
                        return await self._fail_and_signal(
                            state, step_def, step_path, kind="transient",
                            message=(f"gate exceeded max_consecutive_poll_failures={max_failures}: "
                                     f"{_failure_message(exc)}"),
                            output=self._encrypt_secrets(current),
                        )
                except Exception as exc:  # noqa: BLE001 -- §2.3.1's fixed classification for
                    # gate polls: anything that isn't Transient/Permanent/StepCancelled/
                    # Unreachable is ≡ Permanent (e.g. a bare TimeoutError from
                    # ctx.run_subprocess inside poll_ready) -- fails immediately, no
                    # hysteresis, rather than crashing the run task uncaught.
                    return await self._fail_and_signal(
                        state, step_def, step_path, kind="permanent",
                        message=_failure_message(exc), output=self._encrypt_secrets(current),
                    )
                else:
                    if isinstance(result, Ready):
                        if gate.settle_seconds > 0:
                            # DR-0022 Erratum E2: a post-Ready grace, honored ONLY once the
                            # gate has actually succeeded (never on NotReady/failure) --
                            # v1's "give Tailscale a few extra seconds to send disconnect to
                            # the control plane" (destruction_job.py:164-181), preserved as
                            # gate data rather than a step-internal sleep. Cancel-aware like
                            # every other engine-owned wait; a trip here converts to
                            # _RunCancelledSignal via this method's own outer except below.
                            await self._cancel_aware_wait(float(gate.settle_seconds), state.token)
                        final = result.outputs if result.outputs is not None else current
                        final_encrypted = self._encrypt_secrets(final)
                        final_plain = _plain_dump(final)
                        await self._persist_step_succeeded(state, step_def, step_path, final_encrypted, {**scope, step_def.id: final_plain})
                        return final_plain
                    consecutive_failures = 0  # NotReady with no exception resets hysteresis
                    # Only overwrite on a detail we can actually use: a step that returns a
                    # bare NotReady() must not erase the last informative one it gave us.
                    if result.detail:
                        last_detail = result.detail
                await self._cancel_aware_wait(interval, state.token)
                elapsed += interval
                if elapsed >= gate_timeout:
                    return await self._fail_and_signal(
                        state, step_def, step_path, kind="permanent",
                        message=(f"gate timed out after {gate_timeout}s"
                                 + (f"; last poll: {last_detail}" if last_detail else "")),
                        output=self._encrypt_secrets(current),
                    )
        except StepCancelled as exc:
            raise _RunCancelledSignal(step_path) from exc

    # -------------------------------------------------------------------------
    # note()/progress() sinks (§2.1, Conflict 3)
    # -------------------------------------------------------------------------

    async def _note_sink(self, run_id: UUID, step_path: str, facts: Mapping[str, str]) -> None:
        async with self._uow() as tx:
            if self._is_finished_instance_run(tx, str(run_id)):
                return  # a straggler of an instance run the engine ended: dropped, not written
            existing = self._step_repo.get(tx, str(run_id), step_path)
            merged = dict(existing.notes) if existing is not None else {}
            merged.update(facts)
            self._step_repo.update(tx, str(run_id), step_path, notes=merged)

    async def _progress_sink(
        self, run_id: UUID, cluster_id: str | None, workflow: str, step_path: str, attempt: int, message: str,
        fields: Mapping[str, Any], *, instance_id: str | None = None,
    ) -> None:
        # The payload names the run's aggregate under its own key: `cluster_id` for a cluster
        # run (unchanged), `instance_id` for an instance run -- never one id under the other's name.
        owner = {"cluster_id": cluster_id} if instance_id is None else {"instance_id": instance_id}
        async with self._uow() as tx:
            if instance_id is not None and self._is_finished_instance_run(tx, str(run_id)):
                return  # as `_note_sink`
            ordinal = self._next_ordinal(str(run_id))
            self._outbox_repo.insert_run_notify(
                tx, run_id=str(run_id), step_path=step_path, ordinal=ordinal, topic="workflow_progress",
                payload={"run_id": str(run_id), **owner, "workflow": workflow, "step_path": step_path, "attempt": attempt, "message": message, **fields},
                clock=self._clock,
            )

    def _is_finished_instance_run(self, tx: Any, run_id: str) -> bool:
        row = self._run_repo.get(tx, run_id)
        return row is not None and row.instance_id is not None and row.status in _TERMINAL_RUN_STATUSES

    def _next_ordinal(self, run_id: str) -> int:
        n = self._ordinal_counters.get(run_id, 0)
        self._ordinal_counters[run_id] = n + 1
        return n

    async def _emit_job_notify_in_tx(self, tx: Any, state: _RunState, topic: str) -> None:
        ordinal = self._next_ordinal(state.run_id)
        self._outbox_repo.insert_run_notify(
            tx, run_id=state.run_id, step_path="_run", ordinal=ordinal, topic=topic,
            payload={
                "run_id": state.run_id, "workflow": state.wf.workflow,
                **({"cluster_id": state.row.cluster_id} if state.row.instance_id is None else {"instance_id": state.row.instance_id}),
            },
            clock=self._clock,
        )

    # -------------------------------------------------------------------------
    # Event targeting + emit/outcome application (Conflict 3, Conflict 8)
    # -------------------------------------------------------------------------

    def _target_for(self, state: _RunState) -> tuple[str, str]:
        """Conflict 8's targeting rule: deploy/rollback events -> ('deployment',
        run.deployment_id); every other run -> the aggregate it belongs to (DR-0055 decision 4):
        ('cluster', run.cluster_id) or ('instance', run.instance_id).
        ``workflow_runs.workflow`` is the CONCRETE definition name (Conflict 13);
        every deploy/rollback definition is named with a 'deploy' prefix
        (deploy-waves, deploy-rollback), so the prefix check is exact and total
        over the closed set of definitions this engine will ever be handed.

        A run with neither id, or a deploy run with no ``deployment_id``, raises rather than
        guessing a target (the table's CHECK makes the first impossible for a stored row)."""
        row = state.row
        if state.wf.workflow.startswith("deploy"):
            if row.deployment_id is None:
                raise ValueError(f"run {state.run_id}: workflow {state.wf.workflow!r} has no deployment_id")
            return ("deployment", row.deployment_id)
        if row.instance_id is not None:
            return ("instance", row.instance_id)
        if row.cluster_id is not None:
            return ("cluster", row.cluster_id)
        raise ValueError(f"run {state.run_id} has neither cluster_id nor instance_id")

    def _build_event(self, event_name: str, payload_def: Mapping[str, Any], scope: Mapping[str, Any], state: _RunState) -> Event:
        """Step-level ``emit:`` events only -- no auto-attach (see
        ``_build_outcome_event`` for the terminal-outcome variant)."""
        cls = EVENT_REGISTRY[event_name]
        payload = {k: _resolve_value(v, scope) for k, v in payload_def.items()}
        return cls(at=self._clock.now(), actor=f"engine:run:{state.run_id}", **payload)

    def _build_outcome_event(
        self, event_name: str, payload_def: Mapping[str, Any], scope: Mapping[str, Any], state: _RunState,
        status: str, error_kind: str | None, error_message: str | None = None,
    ) -> Event:
        """Terminal outcome events only. Seam B §2.2's grammar comment ("engine auto-
        attaches error, failed_step, undo_incomplete") describes the PRE-Conflict-8
        event shape -- none of those fields exist on any Report class Conflict 8
        actually shipped (``ProvisionFailed``/``DeployFailed``/``DestroyFailed`` have
        only ``reason: str``; ``RollbackFinished`` has only ``ok: bool``, both
        required, no default). The coherence-review event redesign OVERRIDES that
        sentence; this is its equivalent auto-attach: ``reason`` <- the run's error
        kind ('permanent'|'transient'|'unreachable'|'cancelled') when the event class
        declares that field and the workflow's YAML payload didn't already supply one
        (Conflict 8's mapping table: cancelled -> reason="cancelled", unreachable-
        exhausted destroy -> reason="unreachable" -- both exactly the error kind);
        ``ok`` <- ``status == "succeeded"`` under the same condition. Without this,
        every failed/cancelled/rollback terminal of every shipped workflow TypeErrors
        inside the terminal transaction (the required kwarg is simply missing)."""
        cls = EVENT_REGISTRY[event_name]
        payload = {k: _resolve_value(v, scope) for k, v in payload_def.items()}
        field_names = {f.name for f in dataclasses.fields(cls)}
        if "reason" in field_names and "reason" not in payload:
            payload["reason"] = _outcome_reason(error_kind, error_message, status)
        if "ok" in field_names and "ok" not in payload:
            payload["ok"] = status == "succeeded"
        return cls(at=self._clock.now(), actor=f"engine:run:{state.run_id}", **payload)

    async def _apply_event_in_tx(self, tx: Any, state: _RunState, event_name: str, payload_def: Mapping[str, Any], scope: Mapping[str, Any]) -> None:
        aggregate, aggregate_id = self._target_for(state)
        event = self._build_event(event_name, payload_def, scope, state)
        await self._dispatcher.apply(aggregate, aggregate_id, event, tx=tx)

    # -------------------------------------------------------------------------
    # Compensation (§2.3.4): strict LIFO, failed/interrupted/cancelled step FIRST,
    # record-and-continue, fresh non-tripped token per undo.
    # -------------------------------------------------------------------------

    @staticmethod
    def _stepdef_for_path(wf: WorkflowDefinition, step_path: str) -> StepDef | None:
        if "[" in step_path.split(".", 1)[0]:
            foreach_part, _, body_id = step_path.partition(".")
            foreach_id = foreach_part.split("[", 1)[0]
            for entry in wf.steps:
                if isinstance(entry, ForeachDef) and entry.id == foreach_id:
                    for body_step in entry.body:
                        if body_step.id == body_id:
                            return body_step
            return None
        for entry in wf.steps:
            if isinstance(entry, StepDef) and entry.id == step_path:
                return entry
        return None

    @staticmethod
    def _lifo_order(all_steps: list[WorkflowStepRow], failed_step_path: str | None) -> list[WorkflowStepRow]:
        failed_row = None
        others: list[WorkflowStepRow] = []
        for s in all_steps:
            if failed_step_path is not None and s.step_path == failed_step_path:
                failed_row = s
            else:
                others.append(s)
        return ([failed_row] if failed_row is not None else []) + list(reversed(others))

    async def _compensate(self, state: _RunState) -> None:
        async with self._uow() as tx:
            run_row = self._run_repo.get(tx, state.run_id)
            all_steps = self._step_repo.list_for_run(tx, state.run_id)
        failed_step_path = run_row.failed_step if run_row is not None else None
        ordered = self._lifo_order(all_steps, failed_step_path)
        undo_incomplete: list[str] = list((run_row.undo_incomplete if run_row is not None else None) or [])
        for row in ordered:
            if row.undo_status is not None:
                continue  # already compensated -- resume case
            step_def = self._stepdef_for_path(state.wf, row.step_path)
            step = self._steps.get(row.verb) if step_def is not None else None
            if step_def is None or step is None or not step.undoable or row.status == "failed_continued":
                await self._mark_undo(state, row.step_path, "skipped")
                continue
            ok = await self._undo_one(state, step, step_def, row)
            if not ok:
                undo_incomplete.append(row.step_path)
                async with self._tx(state) as tx:
                    self._run_repo.update(tx, state.run_id, undo_incomplete=undo_incomplete)
        state.undo_incomplete = undo_incomplete

    async def _mark_undo(self, state: _RunState, step_path: str, status: str) -> None:
        async with self._tx(state) as tx:
            self._step_repo.update(tx, state.run_id, step_path, undo_status=status)

    async def _undo_one(self, state: _RunState, step: Step, step_def: StepDef, row: WorkflowStepRow) -> bool:
        fresh_token = CancelToken()  # undo runs on a FRESH non-tripped token (§2.3.4)
        params = step.Params(**self._decrypt_secrets_for(step.Params, row.params))
        output = step.Output(**self._decrypt_secrets_for(step.Output, row.output)) if row.output else None
        schedule = schedule_for_undo(self._resolve_schedule(step_def.retry, step.default_retry))
        timeout = step_def.timeout_seconds or step.default_timeout_seconds
        attempt = 1
        while True:
            ctx = self._make_ctx(state, row.step_path, attempt, fresh_token)
            try:
                await self._probe_with_park(
                    state, row.step_path, token=fresh_token,
                    probe=lambda ctx=ctx: self._try_call(step.undo(params, output, row.notes, ctx), timeout),
                )
            except _UnreachableExhausted:
                await self._mark_undo(state, row.step_path, "failed")
                return False
            except StepCancelled:
                await self._mark_undo(state, row.step_path, "failed")
                return False
            except Exception as exc:  # noqa: BLE001
                outcome = classify(exc)
                if outcome is Outcome.RETRY and attempt < schedule.max_attempts:
                    attempt += 1
                    retry_after = getattr(exc, "retry_after", None)
                    await self._cancel_aware_wait(delay_seconds(schedule, attempt, retry_after=retry_after), fresh_token)
                    continue
                await self._mark_undo(state, row.step_path, "failed")
                return False
            else:
                await self._mark_undo(state, row.step_path, "done")
                return True

    # -------------------------------------------------------------------------
    # Terminal handlers (cancel G4, failure, success)
    # -------------------------------------------------------------------------

    async def _handle_cancel(self, state: _RunState, interrupted_step_path: str | None) -> None:
        compensate = state.wf.on_failure == "compensate"
        async with self._tx(state) as tx:
            if interrupted_step_path is not None:
                row = self._step_repo.get(tx, state.run_id, interrupted_step_path)
                if row is not None and row.status in ("running", "gating"):
                    self._step_repo.update(tx, state.run_id, interrupted_step_path, status="cancelled", finished_at=self._clock.now())
            self._run_repo.update(
                tx, state.run_id,
                status="compensating" if compensate else "cancelled",
                failed_step=interrupted_step_path,
                error={"kind": "cancelled", "step": interrupted_step_path, "message": "cancelled"},
            )
            if not compensate:
                # No compensation phase intervenes: points 7 and 9 collapse into this
                # one transaction, exactly like `_fail_and_signal`'s report-mode branch.
                await self._apply_terminal(tx, state, "cancelled", "cancelled")
        if compensate:
            await self._compensate(state)
            await self._finalize(state, "cancelled", error_kind="cancelled")

    async def _handle_failure(self, state: _RunState, fail: _RunFailed) -> None:
        """Only reached for ``on_failure: compensate`` workflows -- the failing step's
        row and the run's transition to 'compensating' (point 7) already committed
        atomically in ``_fail_and_signal``; report-mode workflows finalize there
        directly and raise ``_RunAlreadyFinalized`` instead, never reaching here."""
        await self._compensate(state)
        await self._finalize(state, "failed", error_kind=fail.kind, error_message=fail.message)

    async def _succeed(self, state: _RunState) -> None:
        if state.row.instance_id is not None:
            # An instance run's success event can be refused where it is BUILT (``InstanceProvisioned``
            # needs an address and resource ids, and a step that returned an empty one is a defect in
            # that step). Left to ``_finalize``, the ValueError would escape ``_run`` with the run still
            # `running` and no task: nothing would ever report the instance. So the event is built first,
            # and a refusal is an ordinary permanent step failure: the run goes `compensating`, undoes
            # what it did, and finishes `failed` with the workflow's own `failed` event applied. Any
            # resource id the run already recorded stays on the instance, which stays destroyable.
            outcome_def = state.wf.outcome.succeeded
            try:
                self._build_outcome_event(
                    outcome_def.event, outcome_def.payload, state.scope, state, "succeeded", None
                )
            except Exception as exc:  # noqa: BLE001 -- any refusal of the event is a failed run
                message = f"invalid {outcome_def.event}: {type(exc).__name__}: {exc}"
                _log.error("run %s: %s", state.run_id, message)
                async with self._tx(state) as tx:
                    self._run_repo.update(
                        tx, state.run_id, status="compensating",
                        error={"kind": "permanent", "step": None, "message": message},
                    )
                await self._compensate(state)
                await self._finalize(state, "failed", error_kind="permanent", error_message=message)
                return
        await self._finalize(state, "succeeded")

    async def _apply_terminal(
        self, tx: Any, state: _RunState, status: str, error_kind: str | None,
        error_message: str | None = None,
    ) -> None:
        """Persistence point 9 (§2.3.2): run terminal status + finished_at + the
        outcome event, in the transaction the CALLER already has open. Callers with no
        compensation phase between the failing write and this one fold it directly in
        (``_fail_and_signal``'s report-mode branch, ``_fail_unreachable_and_finalize``,
        ``_handle_cancel``'s report-mode branch); ``_finalize`` is the standalone entry
        point for callers that DO have a phase in between (compensation, plain
        success)."""
        outcome_def = getattr(state.wf.outcome, status)
        self._run_repo.update(tx, state.run_id, status=status, undo_incomplete=state.undo_incomplete or None, finished_at=self._clock.now())
        event = self._build_outcome_event(outcome_def.event, outcome_def.payload, state.scope, state, status, error_kind, error_message)
        aggregate, aggregate_id = self._target_for(state)
        await self._dispatcher.apply(aggregate, aggregate_id, event, tx=tx)
        await self._emit_job_notify_in_tx(tx, state, "job_completed" if status == "succeeded" else "job_failed")

    async def _finalize(
        self, state: _RunState, status: str, *, error_kind: str | None = None,
        error_message: str | None = None,
    ) -> None:
        async with self._tx(state) as tx:
            await self._apply_terminal(tx, state, status, error_kind, error_message)
