"""Typed test doubles for engine primitive tests. No Mock/patch anywhere (CLAUDE.md
testing posture) — fakes are plain classes implementing the same protocols as the real
thing.

The second half of this module (from "Engine-level fake verbs" onward) is the SHARED
HARNESS other agents' engine test suites (resume/compensation/cancel/gate/blocked-park
matrices) build on: deterministic fake verbs, ``FakeDispatcher``, the controllable
``InstantSleeper`` seam, and ``build_engine`` wiring a real ``WorkflowEngine`` against
real SQLite (migrate + Database + UnitOfWork + repos) — no Mock/patch anywhere.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from seedpod.core.clock import Clock
from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    TransientError,
)
from seedpod.core.events import Event
from seedpod.engine.errors import StepCancelled
from seedpod.engine.registry import resolve_type_expr
from seedpod.engine.schedule import NAMED_POLICIES, Schedule
from seedpod.engine.step import JsonValue, NotReady, Ready, Step, StepContext, StepServices


class FakeSubprocessManager:
    """Records register/unregister calls; asserts nothing on its own."""

    def __init__(self) -> None:
        self.registered: list[asyncio.subprocess.Process] = []
        self.unregistered: list[asyncio.subprocess.Process] = []

    def register(self, process: asyncio.subprocess.Process, *, cluster_id: str | None = None) -> None:
        self.registered.append(process)

    def unregister(self, process: asyncio.subprocess.Process) -> None:
        self.unregistered.append(process)


@dataclass
class RecordingNoteSink:
    calls: list[tuple[uuid.UUID, str, Mapping[str, str]]] = field(default_factory=list)

    async def __call__(self, run_id: uuid.UUID, step_path: str, facts: Mapping[str, str]) -> None:
        self.calls.append((run_id, step_path, dict(facts)))


@dataclass
class RecordingProgressSink:
    calls: list[tuple] = field(default_factory=list)
    raise_on_call: bool = False

    async def __call__(
        self,
        run_id: uuid.UUID,
        cluster_id: str,
        workflow: str,
        step_path: str,
        attempt: int,
        message: str,
        fields: Mapping[str, JsonValue],
    ) -> None:
        self.calls.append((run_id, cluster_id, workflow, step_path, attempt, message, dict(fields)))
        if self.raise_on_call:
            raise RuntimeError("progress sink exploded")


def make_step_context(
    *,
    cancel=None,
    services: StepServices | None = None,
    note_sink: RecordingNoteSink | None = None,
    progress_sink: RecordingProgressSink | None = None,
    run_id: uuid.UUID | None = None,
    cluster_id: str = "cluster-1",
    workflow: str = "provision-digitalocean",
    step_path: str = "create",
    attempt: int = 1,
) -> StepContext:
    from seedpod.engine.cancel import CancelToken

    return StepContext(
        run_id=run_id or uuid.uuid4(),
        cluster_id=cluster_id,
        workflow=workflow,
        step_path=step_path,
        attempt=attempt,
        cancel=cancel or CancelToken(),
        services=services or StepServices(subprocess_manager=FakeSubprocessManager()),
        note_sink=note_sink or RecordingNoteSink(),
        progress_sink=progress_sink or RecordingProgressSink(),
    )


# --------------------------------------------------------------------------------------
# Fake verbs for registry tests
# --------------------------------------------------------------------------------------


class EchoParams(BaseModel):
    message: str = "hi"


class EchoOutput(BaseModel):
    echoed: str = ""


class FakeEchoStep(Step[EchoParams, EchoOutput]):
    verb = "test.echo"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    gateable = False
    undoable = False
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        return EchoOutput(echoed=params.message)


class FakeGateStep(Step[EchoParams, EchoOutput]):
    verb = "test.gate"
    Params = EchoParams
    Output = EchoOutput
    gateable = True
    default_retry = NAMED_POLICIES["kubectl_default"]

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        return EchoOutput(echoed=params.message)

    async def poll_ready(self, params, provisional, ctx):
        raise NotImplementedError


class FakeUndoableStep(Step[EchoParams, EchoOutput]):
    verb = "test.undoable"
    Params = EchoParams
    Output = EchoOutput
    undoable = True

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        return EchoOutput(echoed=params.message)

    async def undo(self, params, output, notes, ctx):
        return None


# ========================================================================================
# Engine-level fake verbs — the shared harness for tests/engine/test_engine_*.py
# ========================================================================================


class InstantStep(Step[EchoParams, EchoOutput]):
    """Instant, always-successful, idempotent verb."""

    verb = "fake.instant"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self) -> None:
        self.calls: list[EchoParams] = []

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls.append(params)
        return EchoOutput(echoed=params.message)


# A fast, jitter-free named-style retry policy for deterministic retry-matrix tests
# (real NAMED_POLICIES carry v1's tuned multi-second delays -- fine for production,
# noisy for assertions on exact retry counts under an InstantSleeper).
FAST_RETRY = Schedule(max_attempts=10, base_delay_seconds=0.01, factor=1.0, max_delay_seconds=0.01, jitter=0.0)


class TransientNTimesStep(Step[EchoParams, EchoOutput]):
    """Raises TransientError for its first ``fail_times`` calls, then succeeds."""

    verb = "fake.transient_n_times"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = FAST_RETRY
    default_timeout_seconds = 30

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise TransientError(f"transient failure #{self.calls}", code=ErrorCode.API_TIMEOUT)
        return EchoOutput(echoed=params.message)


class PermanentStep(Step[EchoParams, EchoOutput]):
    """Always fails with PermanentError."""

    verb = "fake.permanent"
    Params = EchoParams
    Output = EchoOutput
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls += 1
        raise PermanentError("permanent failure", code=ErrorCode.INVALID_INPUT)


class DetailBearingPermanentStep(Step[EchoParams, EchoOutput]):
    """Fails with a ``PermanentError`` whose ``detail`` carries raw stderr.

    Backlog #18's exact shape, copied from ``providers/kubectl.py``'s
    ``_classify_failure``: a terse, uninformative ``message`` beside a ``detail``
    holding the stderr that actually says what went wrong.
    """

    verb = "fake.permanent_with_detail"
    Params = EchoParams
    Output = EchoOutput
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, stderr: str) -> None:
        self.stderr = stderr
        self.calls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls += 1
        raise PermanentError(
            "kubectl.apply_manifest: invalid input",
            code=ErrorCode.INVALID_INPUT,
            provider="kubectl",
            command="apply_manifest",
            detail={"stderr": self.stderr},
        )


class UnreachableNTimesStep(Step[EchoParams, EchoOutput]):
    """Raises InfrastructureUnreachableError for its first ``unreachable_times``
    calls (i.e. blocked-park re-probes), then succeeds."""

    verb = "fake.unreachable_n_times"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, unreachable_times: int) -> None:
        self.unreachable_times = unreachable_times
        self.calls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls += 1
        if self.calls <= self.unreachable_times:
            raise InfrastructureUnreachableError("still unreachable", code=ErrorCode.ENDPOINT_UNREACHABLE)
        return EchoOutput(echoed=params.message)


class GateReadyAfterKStep(Step[EchoParams, EchoOutput]):
    """A gateable verb whose ``poll_ready`` returns NotReady for the first
    ``ready_after - 1`` polls, then Ready (enriching the output) on poll K."""

    verb = "fake.gate_ready_after_k"
    Params = EchoParams
    Output = EchoOutput
    gateable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, ready_after: int) -> None:
        self.ready_after = ready_after
        self.polls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        return EchoOutput(echoed=params.message)

    async def poll_ready(self, params: EchoParams, provisional: EchoOutput, ctx: StepContext):
        self.polls += 1
        await ctx.progress(f"poll {self.polls}")
        if self.polls >= self.ready_after:
            return Ready(outputs=EchoOutput(echoed=f"{provisional.echoed}-ready"))
        return NotReady(detail=f"poll {self.polls}")


class GateTransientNTimesStep(Step[EchoParams, EchoOutput]):
    """Gateable verb whose poll_ready raises TransientError for its first
    ``fail_times`` polls (hysteresis-counter matrix), then returns Ready."""

    verb = "fake.gate_transient_n_times"
    Params = EchoParams
    Output = EchoOutput
    gateable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.polls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        return EchoOutput(echoed=params.message)

    async def poll_ready(self, params: EchoParams, provisional: EchoOutput, ctx: StepContext):
        self.polls += 1
        if self.polls <= self.fail_times:
            raise TransientError(f"poll failure #{self.polls}", code=ErrorCode.API_TIMEOUT)
        return Ready(outputs=provisional)


class SleeperStep(Step[EchoParams, EchoOutput]):
    """Sleeps (via ``ctx.sleep``, cancellation-aware) for cancel-matrix tests."""

    verb = "fake.sleeper"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 300

    def __init__(self, seconds: float = 60.0) -> None:
        self.seconds = seconds
        self.started = 0
        self.cancelled = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.started += 1
        try:
            await ctx.sleep(self.seconds)
        except StepCancelled:
            self.cancelled += 1
            raise
        return EchoOutput(echoed=params.message)


class NoteParams(BaseModel):
    resource_id: str = "r-1"


class NoteOutput(BaseModel):
    resource_id: str = "r-1"


class NoteWritingStep(Step[NoteParams, NoteOutput]):
    """Writes ctx.note() before returning (C1-closing write-ahead scratchpad) and is
    undoable, recording every undo() call for LIFO-ordering assertions."""

    verb = "fake.note_writer"
    Params = NoteParams
    Output = NoteOutput
    idempotent = False
    undoable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self) -> None:
        self.undo_calls: list[tuple[NoteParams, NoteOutput | None, dict[str, str]]] = []

    async def execute(self, params: NoteParams, ctx: StepContext) -> NoteOutput:
        await ctx.note(resource_id=params.resource_id)
        return NoteOutput(resource_id=params.resource_id)

    async def undo(self, params: NoteParams, output: NoteOutput | None, notes: Mapping[str, str], ctx: StepContext) -> None:
        self.undo_calls.append((params, output, dict(notes)))


class RetryAfterNTimesStep(Step[EchoParams, EchoOutput]):
    """Raises ``TransientError(retry_after=...)`` for its first ``fail_times``
    calls, then succeeds -- Conflict 6's "retry_after overrides the computed
    delay" matrix (test_gates_schedule_park.py). ``default_retry`` is
    ``FAST_RETRY`` (base_delay_seconds=0.01) deliberately, so any test asserting
    the injected sleeper actually recorded ``retry_after`` (not the tiny computed
    backoff) is unambiguous."""

    verb = "fake.retry_after_n_times"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = FAST_RETRY
    default_timeout_seconds = 30

    def __init__(self, fail_times: int, retry_after: float) -> None:
        self.fail_times = fail_times
        self.retry_after = retry_after
        self.calls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise TransientError(
                f"transient failure #{self.calls}", code=ErrorCode.API_TIMEOUT, retry_after=self.retry_after
            )
        return EchoOutput(echoed=params.message)


class TimeoutNTimesStep(Step[EchoParams, EchoOutput]):
    """Sleeps (a bare ``asyncio.sleep`` -- deliberately NOT ``ctx.sleep``, so this
    is a genuine per-attempt timeout expiry, not cooperative cancellation) past
    its own ``default_timeout_seconds`` for the first ``slow_times`` calls, then
    returns instantly -- proves ``engine/schedule.py``'s ``classify()`` treats a
    ``TimeoutError`` from ``asyncio.timeout`` exactly like ``TransientError``
    (docs/design/seam-b-engine.md §2.3.1: "per-attempt timeout expiry ->
    retry")."""

    verb = "fake.timeout_n_times"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = FAST_RETRY
    default_timeout_seconds = 0.05

    def __init__(self, slow_times: int, slow_seconds: float = 0.2) -> None:
        self.slow_times = slow_times
        self.slow_seconds = slow_seconds
        self.calls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls += 1
        if self.calls <= self.slow_times:
            await asyncio.sleep(self.slow_seconds)
        return EchoOutput(echoed=params.message)


class ScriptedGateStep(Step[EchoParams, EchoOutput]):
    """A gateable verb whose ``poll_ready`` follows an explicit script of
    outcomes, one per call (the last entry repeats forever once the script is
    exhausted): "transient" (raise ``TransientError``), "permanent" (raise
    ``PermanentError``), "unreachable" (raise ``InfrastructureUnreachableError``),
    "notready" (``NotReady`` WITH a detail), "notready_bare" (``NotReady`` with no
    detail -- DR-0033's "gate must not invent one" case), "ready" (``Ready``,
    enriching the provisional output) -- the one flexible fake verb the gate-
    hysteresis / Unreachable-interaction matrix (test_gates_schedule_park.py) needs."""

    verb = "fake.gate_scripted"
    Params = EchoParams
    Output = EchoOutput
    gateable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, script: Sequence[str]) -> None:
        self.script = list(script)
        self.polls = 0
        self.calls: list[str] = []

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        return EchoOutput(echoed=params.message)

    async def poll_ready(self, params: EchoParams, provisional: EchoOutput, ctx: StepContext):
        self.polls += 1
        outcome = self.script[min(self.polls - 1, len(self.script) - 1)]
        self.calls.append(outcome)
        if outcome == "transient":
            raise TransientError(f"poll transient #{self.polls}", code=ErrorCode.API_TIMEOUT)
        if outcome == "permanent":
            raise PermanentError(f"poll permanent #{self.polls}", code=ErrorCode.INVALID_INPUT)
        if outcome == "unreachable":
            raise InfrastructureUnreachableError(f"poll unreachable #{self.polls}", code=ErrorCode.ENDPOINT_UNREACHABLE)
        if outcome == "notready":
            return NotReady(detail=f"poll {self.polls}")
        if outcome == "notready_bare":
            return NotReady()  # DR-0033: no detail -- the gate must not invent one
        if outcome == "ready":
            return Ready(outputs=EchoOutput(echoed=f"{provisional.echoed}-ready"))
        raise AssertionError(f"unknown ScriptedGateStep outcome {outcome!r}")


class UnreachableUndoStep(Step[EchoParams, EchoOutput]):
    """Undoable verb whose ``execute`` always succeeds but whose ``undo`` always
    raises ``InfrastructureUnreachableError`` -- Conflict 5's undo-exhaustion
    branch: that undo's OWN ``undo_status`` becomes 'failed' (appended to
    ``run.undo_incomplete``) while the LIFO compensation loop continues to the
    remaining undos (test_gates_schedule_park.py)."""

    verb = "fake.unreachable_undo"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    undoable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self) -> None:
        self.undo_calls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        return EchoOutput(echoed=params.message)

    async def undo(self, params: EchoParams, output: EchoOutput | None, notes: Mapping[str, str], ctx: StepContext) -> None:
        self.undo_calls += 1
        raise InfrastructureUnreachableError("undo target unreachable", code=ErrorCode.ENDPOINT_UNREACHABLE)


class NonIdempotentStep(Step[EchoParams, EchoOutput]):
    """idempotent=False -- crash-mid-execute resume must fail it immediately rather
    than re-entering execute()."""

    verb = "fake.non_idempotent"
    Params = EchoParams
    Output = EchoOutput
    idempotent = False
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls += 1
        return EchoOutput(echoed=params.message)


class UndoableEchoStep(Step[EchoParams, EchoOutput]):
    """Plain undoable, idempotent, always-successful verb -- for compensation
    LIFO-ordering tests over several distinct steps."""

    verb = "test.undoable_echo"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    undoable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self) -> None:
        self.undo_calls: list[EchoParams] = []

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        return EchoOutput(echoed=params.message)

    async def undo(self, params: EchoParams, output: EchoOutput | None, notes: Mapping[str, str], ctx: StepContext) -> None:
        self.undo_calls.append(params)


class GateSleeperStep(Step[EchoParams, EchoOutput]):
    """Gateable verb whose ``poll_ready`` sleeps (via ``ctx.sleep``, cancellation-
    aware) -- the cancel-matrix's probe for G3 landing WHILE a poll is in flight,
    as distinct from the engine-owned interval wait BETWEEN polls (which races the
    injected ``Sleeper`` seam instead, see ``GatedSleeper``)."""

    verb = "fake.gate_sleeper"
    Params = EchoParams
    Output = EchoOutput
    gateable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, seconds: float = 60.0) -> None:
        self.seconds = seconds
        self.polls = 0
        self.cancelled = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        return EchoOutput(echoed=params.message)

    async def poll_ready(self, params: EchoParams, provisional: EchoOutput, ctx: StepContext):
        self.polls += 1
        try:
            await ctx.sleep(self.seconds)
        except StepCancelled:
            self.cancelled += 1
            raise
        return Ready(outputs=provisional)


class SubprocessSleeperStep(Step[EchoParams, EchoOutput]):
    """Runs a real short-lived-but-slow child process via ``ctx.run_subprocess`` --
    the cancel-matrix's probe for a genuine process-GROUP kill (G3), as distinct
    from the ``ctx.sleep``-based cancellation ``SleeperStep`` exercises."""

    verb = "fake.subprocess_sleeper"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 60

    def __init__(self) -> None:
        self.started = 0
        self.cancelled = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.started += 1
        try:
            await ctx.run_subprocess(["sleep", "30"])
        except StepCancelled:
            self.cancelled += 1
            raise
        return EchoOutput(echoed=params.message)  # pragma: no cover -- "sleep 30" never finishes in a cancel test


class PauseAfterExecuteStep(Step[EchoParams, EchoOutput]):
    """Computes its output immediately then pauses on a bare ``asyncio.Event``
    (``entered``) until ``release`` is set -- the cancel-matrix's probe for a
    cancel landing in the GAP between two units of work (a foreach-iteration
    boundary, or the boundary between two top-level steps), as distinct from
    mid-``execute`` (``SleeperStep``, cancellation-aware via ``ctx.sleep``): this
    step's own pause deliberately does NOT observe cancellation (a bare
    ``asyncio.Event.wait()``, never ``ctx.sleep``), so it always finishes on its
    own -- proving whatever blocks the NEXT unit from starting is the engine's
    G2 DB-serialized re-read, not this step noticing anything."""

    verb = "fake.pause_after_execute"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls.append(params.message)
        self.entered.set()
        await self.release.wait()
        return EchoOutput(echoed=params.message)


class SleepingUndoStep(Step[NoteParams, NoteOutput]):
    """Undoable verb whose ``undo()`` sleeps (via ``ctx.sleep``, on whatever token
    the engine hands it) -- the cancel-matrix's probe for G4/§2.3.4's "compensation
    runs on a FRESH, non-tripped token and cannot be cancelled": undo must run to
    completion even though the run's own CancelToken was tripped before
    compensation started."""

    verb = "fake.sleeping_undo"
    Params = NoteParams
    Output = NoteOutput
    idempotent = True
    undoable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, undo_seconds: float = 0.05) -> None:
        self.undo_seconds = undo_seconds
        self.undo_calls: list[tuple[NoteParams, NoteOutput | None, dict[str, str]]] = []

    async def execute(self, params: NoteParams, ctx: StepContext) -> NoteOutput:
        return NoteOutput(resource_id=params.resource_id)

    async def undo(
        self, params: NoteParams, output: NoteOutput | None, notes: Mapping[str, str], ctx: StepContext
    ) -> None:
        await ctx.sleep(self.undo_seconds)  # must complete: undo's token is fresh & never tripped
        self.undo_calls.append((params, output, dict(notes)))


class RecordingStep(Step[EchoParams, EchoOutput]):
    """Records every ``params.message`` it receives, in call order -- the shared
    primitive for integration tests proving (a) a downstream step's bound param
    equals a specific upstream value, and (b) foreach iterations run strictly
    sequentially in list order (each iteration's body step appends to the SAME
    ``received`` list exactly once, in order). If given a ``pause`` (a
    ``PauseGate``, see below -- typed loosely here as ``Any`` since this class is
    defined before ``PauseGate`` in this module), waits on it AFTER recording but
    BEFORE returning -- lets a test inspect the persisted, already-resolved
    ``workflow_steps.params`` row for the step that just started before it
    finishes, then mutate whatever upstream fake produced its binding and prove
    that mutation has no effect (Seam B §2.3.3's "rebuild bindings from persisted
    ... never in-memory state" holds just as much mid-run as across a crash)."""

    verb = "fake.recording"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, *, pause: Any = None) -> None:
        self.received: list[str] = []
        self.pause = pause

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.received.append(params.message)
        if self.pause is not None:
            await self.pause.wait()
        return EchoOutput(echoed=params.message)


class MintOnceStep(Step[EchoParams, EchoOutput]):
    """Always returns ``echoed=self.value`` regardless of ``params`` -- ``self.value``
    is deliberately public and mutable so a test can flip it AFTER this step has
    already succeeded and been persisted, then assert a later step's Ref-bound
    param still reflects the ORIGINAL, already-persisted value (proving the
    engine's scope is a snapshot, never a live read of this object's state)."""

    verb = "fake.mint_once"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, value: str) -> None:
        self.value = value
        self.calls = 0

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls += 1
        return EchoOutput(echoed=self.value)


class FailOnValueStep(Step[EchoParams, EchoOutput]):
    """Succeeds for every ``message`` except ``fail_on``, which it always fails
    PERMANENTLY on -- lets a foreach/wave-shaped integration test target exactly
    ONE iteration's failure by content rather than by call count (a call-count
    fake like ``PausableStep``/``TransientNTimesStep`` can't cleanly express
    "wave[0] succeeds, wave[1] fails" when the same verb instance serves every
    iteration)."""

    verb = "fake.fail_on_value"
    Params = EchoParams
    Output = EchoOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, fail_on: str) -> None:
        self.fail_on = fail_on
        self.calls: list[str] = []

    async def execute(self, params: EchoParams, ctx: StepContext) -> EchoOutput:
        self.calls.append(params.message)
        if params.message == self.fail_on:
            raise PermanentError(f"forced failure on {params.message!r}", code=ErrorCode.INVALID_INPUT)
        return EchoOutput(echoed=params.message)


# ----------------------------------------------------------------------------
# FakeDispatcher -- records (aggregate, aggregate_id, event, tx) applications
# (coherence-review Conflict 3: the engine takes a Dispatcher-SHAPED dependency)
# ----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RecordedApply:
    aggregate: str
    aggregate_id: str
    event: Event
    tx: Any


@dataclass
class FakeDispatcher:
    calls: list[RecordedApply] = field(default_factory=list)

    async def apply(self, aggregate: str, aggregate_id: str, event: Event, *, tx: Any) -> None:
        self.calls.append(RecordedApply(aggregate=aggregate, aggregate_id=aggregate_id, event=event, tx=tx))


# ----------------------------------------------------------------------------
# InstantSleeper -- the controllable async sleeper seam (engine/engine.py's
# `Sleeper` protocol): records every requested duration, advances zero real time,
# so backoff/gate/park schedules stay fast and deterministic in matrix tests.
# ----------------------------------------------------------------------------


@dataclass
class InstantSleeper:
    requested: list[float] = field(default_factory=list)

    async def sleep(self, seconds: float) -> None:
        self.requested.append(seconds)
        await asyncio.sleep(0)  # yield control so concurrently-scheduled tasks progress


# ----------------------------------------------------------------------------
# A minimal RegistryView + type resolver for engine/config.py's load_workflow(),
# self-contained (no import from tests/engine/test_validator.py -- a separate,
# independently-evolving test module).
# ----------------------------------------------------------------------------

_BUILTIN_SCALARS: dict[str, type] = {"str": str, "int": int, "float": float, "bool": bool}


def _parse_type_expr(expr: str, named: Mapping[str, type]) -> type | None:
    """Delegates the grammar to production's own ``resolve_type_expr``
    (``engine/registry.py``) so a fixture can never resolve a type expression
    differently from the real registry -- gate finding M-1's second half. The only
    fixture-local part is the NAME table: test workflows may declare types no
    production ``NAMED_TYPES`` entry covers yet."""
    return resolve_type_expr(expr, {**_BUILTIN_SCALARS, **named})


@dataclass
class _RegistryViewAdapter:
    """Structurally satisfies engine/config.py's RegistryView using a real
    StepRegistry for `.verb()` plus a small local type-name resolver for
    `.resolve_type()` (workflow `inputs:` types)."""

    registry: Any
    named_types: Mapping[str, type] = field(default_factory=dict)

    def verb(self, name: str):
        return self.registry.verb(name)

    def resolve_type(self, type_expr: str) -> type | None:
        return _parse_type_expr(type_expr, self.named_types)


# ----------------------------------------------------------------------------
# build_engine -- wires a real WorkflowEngine against real SQLite
# (migrate + Database + UnitOfWork + repos), a StepRegistry.for_tests(*fake_steps),
# and the given workflow definitions (name -> YAML text).
# ----------------------------------------------------------------------------


@dataclass
class EngineHarness:
    engine: Any
    db: Any
    uow: Any
    run_repo: Any
    step_repo: Any
    outbox_repo: Any
    dispatcher: FakeDispatcher
    sleeper: InstantSleeper
    registry: Any
    definitions: Mapping[str, Any]


def build_engine(
    tmp_path: Path,
    definitions_yaml: Mapping[str, str],
    fake_steps: Sequence[Step],
    clock: Clock,
    *,
    named_types: Mapping[str, type] | None = None,
    dispatcher: FakeDispatcher | None = None,
    sleeper: InstantSleeper | None = None,
    subprocess_manager: FakeSubprocessManager | None = None,
    **engine_overrides: Any,
) -> EngineHarness:
    """The shared test harness other engine test suites build on. `definitions_yaml`
    maps CONCRETE workflow definition name (`workflow_runs.workflow`, Conflict 13) ->
    YAML text; each is parsed+validated via `engine.config.load_workflow` against a
    `StepRegistry.for_tests(*fake_steps)`. No Mock/patch anywhere."""
    from seedpod.data.database import Database
    from seedpod.data.migrate import migrate
    from seedpod.data.repositories import (
        OutboxRepository,
        WorkflowRunRepository,
        WorkflowStepRepository,
    )
    from seedpod.data.uow import UnitOfWork
    from seedpod.engine.config import load_workflow
    from seedpod.engine.engine import WorkflowEngine
    from seedpod.engine.registry import StepRegistry

    db = Database(f"sqlite:///{tmp_path / 'engine.db'}")
    migrate(db.engine)
    uow = UnitOfWork(db)
    run_repo = WorkflowRunRepository()
    step_repo = WorkflowStepRepository()
    outbox_repo = OutboxRepository()

    registry = StepRegistry.for_tests(*fake_steps)
    adapter = _RegistryViewAdapter(registry=registry, named_types=dict(named_types or {}))
    definitions = {name: load_workflow(text, adapter) for name, text in definitions_yaml.items()}

    dispatcher = dispatcher if dispatcher is not None else FakeDispatcher()
    sleeper = sleeper if sleeper is not None else InstantSleeper()
    services = StepServices(subprocess_manager=subprocess_manager or FakeSubprocessManager())

    engine = WorkflowEngine(
        definitions=definitions,
        steps=registry,
        uow=uow,
        run_repo=run_repo,
        step_repo=step_repo,
        outbox_repo=outbox_repo,
        dispatcher=dispatcher,
        clock=clock,
        step_services=services,
        sleeper=sleeper,
        **engine_overrides,
    )
    return EngineHarness(
        engine=engine,
        db=db,
        uow=uow,
        run_repo=run_repo,
        step_repo=step_repo,
        outbox_repo=outbox_repo,
        dispatcher=dispatcher,
        sleeper=sleeper,
        registry=registry,
        definitions=definitions,
    )


# ========================================================================================
# Crash-injection seam — the shared harness for tests/engine/test_crash_matrix.py
# (docs/design/seam-b-engine.md §2.3.2's persistence-point table, §2.3.3 resume).
#
# ``PauseGate`` is a bare ``asyncio.Event`` a fake verb (or ``GatedSleeper``, engine.py's
# injected ``Sleeper`` seam) parks on at a chosen sub-step moment. ``crash_run`` waits for
# the run's task to actually park there, then hard-cancels that ``asyncio.Task`` and awaits
# its demise. This is a FAITHFUL process-crash simulation, not just "cancel and hope",
# because every park point this module offers is a plain ``await event.wait()`` with no
# ``UnitOfWork`` transaction open and — critically — no ``try/finally`` anywhere on the
# call stack between the park point and ``WorkflowEngine._run``'s outermost frame, so
# ``asyncio.CancelledError`` propagates straight out with zero engine-side cleanup code
# executing (verified against seedpod/engine/engine.py: its ONLY ``finally`` block is
# ``_park_and_wait``'s blocked-status restore — deliberately NOT a park point this module
# offers; see tests/engine/test_crash_matrix.py's module docstring for why the blocked/
# cancel-requested-pre-crash scenarios instead craft the post-crash DB row directly, per
# coherence-review Conflict 2's "engine tests insert run rows directly" convention, rather
# than live-interrupting a task parked inside that one guarded region).
# ========================================================================================


@dataclass
class PauseGate:
    """``wait()`` marks ``entered`` (signalling "the run's task has parked exactly
    here") then blocks until the test calls ``release()``. ``rearm()`` lets ONE
    PauseGate be reused across repeated crash/resume cycles (the resume_replay_limit
    crash-loop test) without constructing a fresh gate + fake-verb instance each time."""

    _event: asyncio.Event = field(default_factory=asyncio.Event)
    entered: asyncio.Event = field(default_factory=asyncio.Event)

    async def wait(self) -> None:
        self.entered.set()
        await self._event.wait()

    def release(self) -> None:
        self._event.set()

    def rearm(self) -> None:
        self._event = asyncio.Event()
        self.entered = asyncio.Event()


@dataclass
class GatedSleeper:
    """A ``Sleeper`` (engine.py's injected backoff/gate-interval/park-reprobe waiter)
    that parks on a ``PauseGate`` instead of resolving — lets a test crash the run's
    task while it is genuinely mid-backoff-sleep (persistence point 3) or mid-gate-
    interval-wait, with no surrounding ``try/finally`` in the engine at that point
    (unlike the blocked-park reprobe wait — this seam is deliberately not used for
    that scenario; see the crash-injection-seam docstring above)."""

    gate: PauseGate
    requested: list[float] = field(default_factory=list)

    async def sleep(self, seconds: float) -> None:
        self.requested.append(seconds)
        await self.gate.wait()


class PausableStep(Step[NoteParams, NoteOutput]):
    """The crash-injection matrix's one flexible fake verb: scriptable failures
    (``fail_times`` calls raising ``TransientError``/``PermanentError`` per
    ``fail_kind``), an optional ``ctx.note()`` write, and independently pausable
    ``execute``/``poll_ready``/``undo`` bodies (each takes its own optional
    ``PauseGate``, or ``None`` to run straight through) — every persistence-point
    2-8 crash scenario in test_crash_matrix.py is one of these, configured
    differently. ``idempotent``/``undoable``/``gateable``/``verb`` are INSTANCE
    attributes (shadowing the inherited ``ClassVar`` defaults) so one class covers
    every combination the matrix needs, including multiple differently-behaving
    instances registered under distinct verb strings in the same ``StepRegistry``."""

    Params = NoteParams
    Output = NoteOutput
    default_retry = FAST_RETRY
    default_timeout_seconds = 30

    def __init__(
        self,
        *,
        verb: str = "fake.pausable",
        idempotent: bool = True,
        undoable: bool = False,
        gateable: bool = False,
        fail_times: int = 0,
        fail_kind: str = "transient",  # "transient" | "permanent"
        note: Mapping[str, str] | None = None,
        pause_execute: PauseGate | None = None,
        pause_undo: PauseGate | None = None,
        pause_poll: PauseGate | None = None,
        ready_after: int = 1,
    ) -> None:
        self.verb = verb
        self.idempotent = idempotent
        self.undoable = undoable
        self.gateable = gateable
        self.fail_times = fail_times
        self.fail_kind = fail_kind
        self.note = note
        self.pause_execute = pause_execute
        self.pause_undo = pause_undo
        self.pause_poll = pause_poll
        self.ready_after = ready_after
        self.calls = 0
        self.polls = 0
        self.undo_calls: list[tuple[NoteParams, NoteOutput | None, dict[str, str]]] = []

    async def execute(self, params: NoteParams, ctx: StepContext) -> NoteOutput:
        self.calls += 1
        if self.calls <= self.fail_times:
            if self.fail_kind == "permanent":
                raise PermanentError(f"permanent failure #{self.calls}", code=ErrorCode.INVALID_INPUT)
            raise TransientError(f"transient failure #{self.calls}", code=ErrorCode.API_TIMEOUT)
        if self.note is not None:
            await ctx.note(**self.note)
        if self.pause_execute is not None:
            await self.pause_execute.wait()
        return NoteOutput(resource_id=params.resource_id)

    async def poll_ready(self, params: NoteParams, provisional: NoteOutput, ctx: StepContext):
        self.polls += 1
        if self.pause_poll is not None:
            await self.pause_poll.wait()
        if self.polls >= self.ready_after:
            return Ready(outputs=provisional)
        return NotReady(detail=f"poll {self.polls}")

    async def undo(
        self, params: NoteParams, output: NoteOutput | None, notes: Mapping[str, str], ctx: StepContext
    ) -> None:
        if self.pause_undo is not None:
            await self.pause_undo.wait()
        self.undo_calls.append((params, output, dict(notes)))


async def crash_run(harness: EngineHarness, run_id: str, gate: PauseGate, *, timeout: float = 5.0) -> None:
    """Waits for the run's in-process task to park on ``gate``, then hard-cancels
    that task and awaits its demise — the crash-injection primitive every
    persistence-point test in test_crash_matrix.py builds on. Reaches into
    ``harness.engine``'s private task registry deliberately: Conflict 2 scopes the
    engine's PUBLIC surface to start/resume_inflight/cancel/wait_for/is_running,
    none of which expose the raw ``asyncio.Task`` a white-box crash test needs to
    interrupt, and adding one purely for this test file is not this task's call to
    make on engine.py."""
    await asyncio.wait_for(gate.entered.wait(), timeout=timeout)
    handle = harness.engine._runs.get(run_id)  # noqa: SLF001 -- deliberate white-box access, see docstring
    assert handle is not None, f"run {run_id} has no live task to crash (already finished?)"
    handle.task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(handle.task, timeout=timeout)
    # `add_done_callback`'s registry-eviction runs one loop-tick after the task
    # transitions to done (asyncio schedules callbacks via call_soon) — a bare
    # `await task` above can race it, so poll briefly rather than assume it's gone.
    for _ in range(10):
        if run_id not in harness.engine._runs:  # noqa: SLF001
            return
        await asyncio.sleep(0)
    raise AssertionError(f"run {run_id} still registered as live after crash_run")
