"""engine/provider_step.py — ProviderStep: the ONE bridge between Seam B's ``Step`` and
Seam C's ``Provider`` contract (docs/design/coherence-review.md Conflict 7, verbatim).

``Progress(phase=RESOURCE_ALLOCATED)`` events are folded through ``ctx.note()`` (durable,
committed BEFORE ``execute`` returns — persistence point 4), so ``Observed`` is
rehydratable from ``workflow_steps.notes`` after a crash, not just from an in-memory
stream. ``Step.undo`` is *implemented as* ``undo_for(self.command(params),
Observed(data=notes, value=output))`` — both C1 windows (mid-stream death and process
crash) close through this one path (Conflict 7's resolution paragraph).

Pillar 3 has landed: the provider contract types and ``undo_for`` below are real
imports from ``seedpod.providers.contract`` / ``seedpod.providers.compensation``, not
local stand-ins.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar, Literal, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from seedpod.engine.step import JsonValue, Step, StepContext
from seedpod.providers.compensation import undo_for
from seedpod.providers.contract import (
    RESOURCE_ALLOCATED,
    Observed,
    Progress,
    ProviderCommand,
    ProviderEvent,
    Result,
)
from seedpod.providers.contract import jsonable as _jsonable

__all__ = [
    "RESOURCE_ALLOCATED",
    "ProviderCommand",
    "Progress",
    "Result",
    "ProviderEvent",
    "Observed",
    "Provider",
    "ProviderStep",
]


@runtime_checkable
class Provider(Protocol):
    """Minimal surface ``ProviderStep`` needs out of ``seedpod.providers.contract.Provider``:
    one async-generator ``execute(command)`` yielding ``ProviderEvent``s, keyed into
    ``ctx.services.providers`` by ``provider_name``. (The full ``Provider`` protocol also
    carries ``name``/``supported``/``check_ready``; this module only ever calls
    ``execute``, so it depends on that narrower surface.)
    """

    def execute(self, command: ProviderCommand): ...  # -> AsyncIterator[ProviderEvent]


def jsonable(data: Mapping[str, object]) -> Mapping[str, JsonValue]:
    """Re-exported for callers of this module; delegates to
    ``providers.contract.jsonable`` (coerces a provider event's free-form ``data`` into
    ``ctx.progress``'s JSON-safe kwargs)."""
    return _jsonable(data)


# --------------------------------------------------------------------------------------
# ProviderStep — Conflict 7's code block, verbatim
# --------------------------------------------------------------------------------------

P = TypeVar("P", bound=BaseModel)
O = TypeVar("O", bound=BaseModel)  # noqa: E741 -- name pinned verbatim by Seam B's code block


class ProviderStep(Step[P, O]):
    provider_name: ClassVar[str]  # registry key into ctx.services.providers
    undoable = True  # iff undo_for(command) can be non-None
    # DR-0022 Erratum E12: every ProviderStep (and, by inheritance, every
    # LateBoundProviderStep) wraps a Seam C ProviderCommand, so `plane` is
    # truthfully `"provider"` for the whole family -- one sanctioned default
    # here instead of ~15 verb classes each having to remember to declare it.
    # `thin` stays explicit per verb: composites like `kube.wipe_namespace`/
    # `deploy.await_wave` are ProviderSteps issuing N commands, so no default
    # is truthful for it.
    plane: ClassVar[Literal["provider"]] = "provider"

    def command(self, params: P) -> ProviderCommand:
        """Pure param -> command mapping. Subclasses implement."""
        raise NotImplementedError

    def output_from(self, value: object) -> O:
        """Result.value -> Output. Subclasses implement."""
        raise NotImplementedError

    def result_value_from(self, output: O) -> object:
        """The inverse of ``output_from`` — recovers the ``Result.value`` shape from a
        persisted ``Output`` for undo's ``Observed.value``. Default: the output itself
        (subclasses whose ``output_from`` reshapes the value should override this too)."""
        return output

    async def execute(self, params: P, ctx: StepContext) -> O:
        provider = ctx.services.providers[self.provider_name]
        value = None
        async for ev in provider.execute(self.command(params)):
            match ev:
                case Progress(phase=phase, data=d) if phase == RESOURCE_ALLOCATED:
                    await ctx.note(**{k: str(v) for k, v in d.get("resource_ids", {}).items()})
                    await ctx.progress(ev.message or ev.phase, **jsonable(ev.data))
                case Progress():
                    await ctx.progress(ev.message or ev.phase, **jsonable(ev.data))
                case Result():
                    value = ev.value
        return self.output_from(value)

    async def poll_ready(self, params, provisional, ctx):  # gateable subclasses: one probe command
        raise NotImplementedError(f"{type(self).__name__} (verb={self.verb!r}) is not gateable")

    async def undo(self, params: P, output: O | None, notes: Mapping[str, str], ctx: StepContext) -> None:
        observed = Observed(
            data=dict(notes),
            value=self.result_value_from(output) if output is not None else None,
        )
        inverse = undo_for(self.command(params), observed)  # seedpod/providers/compensation.py
        if inverse is None:
            return
        async for _ in ctx.services.providers[self.provider_name].execute(inverse):
            pass  # idempotent, absence-tolerant by C-10/C-23
