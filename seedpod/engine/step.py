"""engine/step.py — Seam B §2.1: the Step / StepContext / StepServices / Ready / NotReady
contract. Docstrings on StepContext's methods and Step's abstract methods are copied
verbatim from docs/design/seam-b-engine.md §2.1 — they are NORMATIVE BEHAVIOR, not
paraphrase.

Two step families share this one contract (Seam B §2.0): provider steps (wrap Pillar-3
``Provider`` IO; stateless, all context in params) and domain steps (engine-side, may
use injected repositories). Steps are constructed with explicit DI at registry build
time (engine/registry.py); this module never reaches for a global.

``StepServices``' repository / secret-manager / provider-registry fields point at
Pillar-3/4 and spine types that do not exist in this tree yet
(``seedpod/providers/contract.py``, ``seedpod/services/crypto.py``,
``seedpod/data/repositories.py``, ``seedpod/runtime/subprocess_manager.py``). Per the
coherence-review Conflict 7 convention ("declare the minimal types locally behind a
TODO(Pillar-N) marker"), they are declared here as light Protocols / ``Any`` and must be
narrowed, not widened, once those pillars land. This module must not import
``seedpod.providers``, ``seedpod.data``, or ``seedpod.services``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, ClassVar, Generic, Literal, Protocol, TypeVar
from uuid import UUID

from pydantic import BaseModel

from seedpod.engine.cancel import CancelToken
from seedpod.engine.errors import StepCancelled
from seedpod.engine.schedule import NAMED_POLICIES, Schedule

__all__ = [
    "JsonValue",
    "ExecResult",
    "SubprocessManagerLike",
    "NoteSink",
    "ProgressSink",
    "StepServices",
    "StepContext",
    "NotReady",
    "Ready",
    "EmptyParams",
    "EmptyOutput",
    "Step",
]

# Recursive JSON-safe value. TODO(core): promote to seedpod/core if another pillar needs
# the same alias — not owned by this module, declared locally to avoid a premature
# cross-pillar dependency.
JsonValue = str | int | float | bool | None | Mapping[str, "JsonValue"] | Sequence["JsonValue"]

P = TypeVar("P", bound=BaseModel)
O = TypeVar("O", bound=BaseModel)  # noqa: E741 -- name pinned verbatim by Seam B §2.1's code block


# --------------------------------------------------------------------------------------
# run_subprocess support
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExecResult:
    returncode: int
    stdout: bytes
    stderr: bytes


class SubprocessManagerLike(Protocol):
    """Minimal surface ``StepContext.run_subprocess`` needs from
    ``seedpod/runtime/subprocess_manager.py`` (not yet built — TODO(spine): satisfy this
    protocol structurally; the real implementation salvages
    ``reference-code/seedpod/seedpod/core/subprocess_manager.py``'s tracked-process /
    graceful-shutdown pattern so ``App.stop()`` can sweep every live subprocess, not just
    the one a cancelled step is watching). This module's own cancel-triggered kill logic
    does NOT depend on the manager being wired to anything — it holds the process handle
    directly — so a no-op fake fully satisfies tests.
    """

    def register(self, process: asyncio.subprocess.Process, *, cluster_id: str | None = None) -> None: ...

    def unregister(self, process: asyncio.subprocess.Process) -> None: ...


class NoteSink(Protocol):
    """Persists ``ctx.note()`` facts. The engine passes in an implementation bound to
    the run's UnitOfWork/repositories (workflow_steps.notes, persistence point 4,
    Seam B §2.3.2) — this module only defines the seam."""

    async def __call__(self, run_id: UUID, step_path: str, facts: Mapping[str, str]) -> None: ...


class ProgressSink(Protocol):
    """Writes ``ctx.progress()`` as an outbox ``Notify(topic='workflow_progress')`` row
    (coherence-review Conflict 3: drain-lane Notify rows written directly, effect_id
    ``"run/{run_id}@{step_path}#{n}"``). This module only defines the seam."""

    async def __call__(
        self,
        run_id: UUID,
        cluster_id: str,
        workflow: str,
        step_path: str,
        attempt: int,
        message: str,
        fields: Mapping[str, JsonValue],
    ) -> None: ...


@dataclass
class StepServices:
    """Injected: repositories, SecretManager, provider registry, salvaged
    SubprocessManager — never globals (Seam B §2.1)."""

    subprocess_manager: SubprocessManagerLike
    providers: Mapping[str, Any] = field(default_factory=dict)  # TODO(Pillar-3): Mapping[str, Provider]
    repositories: Any = None  # TODO(spine): Repositories (data/repositories.py)
    secret_manager: Any = None  # TODO(spine): SecretManager (services/crypto.py, Fernet DEV/PROD)


class StepContext:
    """Seam B §2.1 verbatim contract. Constructed per attempt by the engine; ``attempt``
    is 1-based and read from the DB, so it survives restart."""

    def __init__(
        self,
        *,
        run_id: UUID,
        cluster_id: str,
        workflow: str,
        step_path: str,
        attempt: int,
        cancel: CancelToken,
        services: StepServices,
        note_sink: NoteSink,
        progress_sink: ProgressSink,
    ) -> None:
        self.run_id = run_id
        self.cluster_id = cluster_id
        self.workflow = workflow
        self.step_path = step_path  # materialized cursor path: 'create' | 'wave[1].apply'
        self.attempt = attempt
        self.cancel = cancel
        self.services = services
        self._note_sink = note_sink
        self._progress_sink = progress_sink

    async def note(self, **facts: str) -> None:
        """Durable write-ahead scratchpad, committed to workflow_steps.notes BEFORE
        returning. e.g. provider.create_server notes server_id the instant the API
        responds, so undo/resume can find the resource even if execute never
        completes (closes the C1 window structurally)."""
        await self._note_sink(self.run_id, self.step_path, facts)

    async def progress(self, message: str, /, **fields: JsonValue) -> None:
        """Writes Notify(topic='workflow_progress') to the effects outbox with payload
        {run_id, cluster_id, workflow, step_path, attempt, message, **fields}.
        Never raises to the step; never touches the cursor. Replaces per-job SSE
        and _job_wrapper's 36-char-arg scanning (gotcha 15)."""
        try:
            await self._progress_sink(
                self.run_id, self.cluster_id, self.workflow, self.step_path, self.attempt, message, fields
            )
        except Exception:
            return  # never raises to the step

    async def sleep(self, seconds: float) -> None:
        """Cancellation-aware sleep: raises StepCancelled if the token trips.
        All in-step waits MUST use this, never asyncio.sleep."""
        self.cancel.raise_if_cancelled()
        try:
            await asyncio.wait_for(self.cancel.wait(), timeout=seconds)
        except TimeoutError:
            return  # the sleep elapsed without the token tripping
        raise StepCancelled(f"sleep({seconds}) cancelled")

    async def run_subprocess(
        self,
        argv: list[str],
        *,
        stdin: bytes | None = None,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        """THE only way steps spawn kubectl/ssh. Runs argv in its own process group,
        registered with the salvaged subprocess_manager. If the token trips:
        SIGTERM the group within ~1s, SIGKILL after a 10s grace, raise StepCancelled.
        This is the structural H16 fix: a step cannot spawn an uninterruptible
        subprocess because this is the only subprocess API it has.

        ``env``, if given, is merged OVER the current process environment (never
        replaces it wholesale) — steps only ever need to add/override a few vars
        (KUBECONFIG, SSH_AUTH_SOCK). ``timeout`` bounds this one subprocess call; on
        expiry the process group is killed the same way as on cancel, and a
        ``TimeoutError`` is raised (distinct from StepCancelled — Schedule classifies
        TimeoutError as retryable, §2.3.1)."""
        self.cancel.raise_if_cancelled()
        merged_env = {**os.environ, **env} if env else None
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=merged_env,
            start_new_session=True,  # own process group; pgid == pid (salvaged shutdown
            # pattern from reference-code/seedpod/seedpod/core/subprocess_manager.py,
            # generalized from per-process SIGTERM/SIGKILL to process-GROUP kill so a
            # spawned kubectl/ssh's own children die too)
        )
        self.services.subprocess_manager.register(process, cluster_id=self.cluster_id)
        try:
            communicate_task = asyncio.ensure_future(process.communicate(stdin))
            cancel_task = asyncio.ensure_future(self.cancel.wait())
            done, _pending = await asyncio.wait(
                {communicate_task, cancel_task}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if communicate_task in done:
                cancel_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await cancel_task
                stdout, stderr = communicate_task.result()
                assert process.returncode is not None
                return ExecResult(returncode=process.returncode, stdout=stdout, stderr=stderr)

            # Neither the process finished nor did we hit an unrelated third state:
            # either the token tripped or `timeout` elapsed first. Either way the
            # process must die, group-wide, before we raise.
            was_cancelled = cancel_task in done
            communicate_task.cancel()
            cancel_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await communicate_task
            with contextlib.suppress(asyncio.CancelledError):
                await cancel_task
            await self._kill_group(process)
            if was_cancelled:
                raise StepCancelled(f"run_subprocess({argv[0]!r}) cancelled")
            raise TimeoutError(f"run_subprocess({argv[0]!r}) exceeded timeout={timeout}s")
        finally:
            self.services.subprocess_manager.unregister(process)

    async def _kill_group(self, process: asyncio.subprocess.Process) -> None:
        pgid = process.pid
        assert pgid is not None
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=10.0)
            return
        except TimeoutError:
            pass
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
        await process.wait()


# --------------------------------------------------------------------------------------
# The Step contract itself
# --------------------------------------------------------------------------------------


class NotReady(BaseModel):
    detail: str = ""


class Ready(BaseModel, Generic[O]):
    outputs: O | None = None  # if set, REPLACES the step's persisted outputs
    # (e.g. droplet gate enriches with public_ip)


class EmptyParams(BaseModel):
    """The canonical ``Params`` for verbs whose YAML ``with:`` block is empty or
    absent (``cluster.load_kubeconfig``, ``cluster.load_kubeconfig_optional``) --
    the step reads ``ctx.cluster_id``/``ctx.run_id`` implicitly rather than through
    a binding. Mirrors ``tests/engine/declared_verbs.py``'s fixture of the same
    name, which has documented this shape as the canonical one since Pillar 2."""


class EmptyOutput(BaseModel):
    """The canonical ``Output`` for steps that produce nothing worth persisting."""


class Step(ABC, Generic[P, O]):
    verb: ClassVar[str]  # registry key, e.g. "kube.apply_docs" (DR-0022 vocabulary)
    Params: ClassVar[type[BaseModel]]  # validates YAML with:-bindings at load & run time
    Output: ClassVar[type[BaseModel]]  # EmptyOutput if none; SecretStr fields encrypted
    idempotent: ClassVar[bool] = True  # governs crash-mid-step resume (§2.3.4)
    gateable: ClassVar[bool] = False  # may carry a gate: block in YAML
    undoable: ClassVar[bool] = False  # participates in compensation
    default_retry: ClassVar[Schedule] = NAMED_POLICIES["none"]
    default_timeout_seconds: ClassVar[int] = 300
    # DR-0022 P2 — layer is a typed registry property, not a name prefix. `plane`
    # names which of the three families this verb belongs to (`provider`: wraps a
    # Seam C ProviderCommand, conformance-covered; `service`: a supporting service
    # invoked through the engine's step machinery, e.g. DnsService; `domain`:
    # engine-side, repository/crypto access, no Seam C command at all). `thin`
    # is True iff the step is exactly one Seam C command with no extra domain
    # logic (DR-0022's own definition: "thin ⇒ exactly one Seam C command").
    # Defaulted to the domain/non-thin case so no already-committed subclass
    # breaks; every real Step this catalog adds must set both truthfully
    # (enforced by tests/engine/test_verb_conventions.py).
    plane: ClassVar[Literal["provider", "service", "domain"]] = "domain"
    thin: ClassVar[bool] = False

    @abstractmethod
    async def execute(self, params: P, ctx: StepContext) -> O:
        """One attempt. Engine enforces the per-attempt timeout (asyncio.timeout) and
        the Schedule — do not self-timeout, do not self-retry. Raise TransientError
        to request retry, PermanentError to fail; any other exception ≡ Permanent."""
        raise NotImplementedError

    async def poll_ready(self, params: P, provisional: O, ctx: StepContext) -> Ready[O] | NotReady:
        """gateable verbs only. ONE cheap idempotent probe; the engine owns the loop,
        interval, overall timeout, transient-failure hysteresis, and cancel checks
        between polls. May raise PermanentError for definitive failure (a K8s Job
        with condition=Failed). May call ctx.progress per poll."""
        raise NotImplementedError(f"{type(self).__name__} (verb={self.verb!r}) is not gateable")

    async def undo(self, params: P, output: O | None, notes: Mapping[str, str], ctx: StepContext) -> None:
        """undoable verbs only. output is None if execute never succeeded (partial
        external effect possible) — undo must then work from notes and/or the
        cluster-uuid tag. MUST be idempotent, tolerate 'already gone', use
        check_enabled=False semantics on provider calls (gotcha 1), and never
        enqueue new runs. Runs on a FRESH non-tripped token."""
        raise NotImplementedError(f"{type(self).__name__} (verb={self.verb!r}) is not undoable")
