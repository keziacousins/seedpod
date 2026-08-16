"""engine/steps/late_bound.py — ``LateBoundProviderStep`` (DR-0022 ruling 1).

Provider identity flows as typed data from ``cluster.load_spec`` (provision) /
``cluster.load_infra`` (destroy), V4-checked at config-load time, instead of a
fixed ``provider_name`` ClassVar. The adapter is still resolved by a **dict
lookup**, never a branch — DR-0004's stated fear ("a generic verb would
reintroduce provider branching inside step implementations") answered
structurally rather than by per-provider verb names.

``execute``/``undo`` are overridden to resolve
``ctx.services.providers[params.provider]`` instead of
``ctx.services.providers[self.provider_name]``. Everything else — the
``command()``/``output_from()`` subclass contract, the ``RESOURCE_ALLOCATED``
note-fold, the ``undo_for()`` compensation bridge — is the same behaviour as
``ProviderStep`` (Conflict 7), duplicated here rather than factored out
because ``engine/provider_step.py`` is committed code this round may not edit
(subclass it, don't touch it). ``command(self, params)`` MUST stay pure (no
``ctx``) on every concrete subclass — the provider identity is read only at
this dict-lookup call site, never threaded into the param -> command mapping
itself.

``poll_ready`` is ALSO overridden (DR-0022 Erratum E4a: "late-bound provider
resolution for GATEABLE verbs lives in ``LateBoundProviderStep`` as a template
method covering ``poll_ready`` as well as ``execute``/``undo``, so the
dict-lookup rule exists in exactly one place") — as a template method: it
resolves the adapter once via the same ``params.provider`` dict lookup, then
delegates to ``probe()``, which gateable concrete subclasses
(``infra.await_instance``, ``infra.destroy_instance``) implement instead of
``poll_ready`` itself. This is the ONE place any ``infra.*`` Step ever writes
``ctx.services.providers[params.provider]`` for a gate poll — no concrete
subclass repeats the lookup.

A ``LateBoundProviderStep``'s ``Params`` model MUST declare a ``provider: str``
field (P8: every fact a provider step needs comes from a typed ``cluster.load_*``
head, never an implicit ClassVar or an ``EmptyParams`` shortcut).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TypeVar

from pydantic import BaseModel

from seedpod.engine.provider_step import (
    RESOURCE_ALLOCATED,
    Observed,
    Progress,
    Provider,
    ProviderStep,
    Result,
    jsonable,
    undo_for,
)
from seedpod.engine.step import NotReady, Ready, StepContext

__all__ = ["LateBoundProviderStep"]

P = TypeVar("P", bound=BaseModel)
O = TypeVar("O", bound=BaseModel)  # noqa: E741 -- name pinned verbatim by Seam B's code block


class LateBoundProviderStep(ProviderStep[P, O]):
    """``ProviderStep`` whose adapter is resolved per-call from
    ``params.provider`` instead of the ``provider_name`` ClassVar. Used by every
    ``infra.*`` verb (``infra.create_instance``, ``infra.await_instance``,
    ``infra.fetch_kubeconfig``, ``infra.destroy_instance``) — the late-bound
    replacement for DR-0004's per-provider verb families
    (``do.*``/``kind.*``/``tart.*``/``orbstack.*``)."""

    async def execute(self, params: P, ctx: StepContext) -> O:
        provider = ctx.services.providers[params.provider]  # type: ignore[attr-defined]
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

    async def undo(self, params: P, output: O | None, notes: Mapping[str, str], ctx: StepContext) -> None:
        observed = Observed(
            data=dict(notes),
            value=self.result_value_from(output) if output is not None else None,
        )
        inverse = undo_for(self.command(params), observed)
        if inverse is None:
            return
        provider = ctx.services.providers[params.provider]  # type: ignore[attr-defined]
        async for _ in provider.execute(inverse):
            pass  # idempotent, absence-tolerant by C-10/C-23

    async def poll_ready(self, params: P, provisional: O, ctx: StepContext) -> Ready[O] | NotReady:
        """Template method (Erratum E4a): the ONE dict-lookup site for a
        late-bound gate poll. Concrete gateable subclasses (``infra.await_instance``,
        ``infra.destroy_instance``) implement ``probe()``, never this method."""
        provider = ctx.services.providers[params.provider]  # type: ignore[attr-defined]
        return await self.probe(params, provisional, provider, ctx)

    async def probe(self, params: P, provisional: O, provider: Provider, ctx: StepContext) -> Ready[O] | NotReady:
        """gateable ``LateBoundProviderStep`` subclasses implement this instead
        of ``poll_ready`` — ``provider`` is already resolved (the dict lookup
        this class exists to centralize), so subclasses only ever build a probe
        command (e.g. ``ProbeInstance``/``ProbeDestruction``) and interpret its
        result. Non-gateable ``infra.*`` verbs never override this: the default
        below raises the same "not gateable" ``NotImplementedError`` shape as
        ``Step.poll_ready``/``ProviderStep.poll_ready``, so the failure mode is
        identical to any other non-gateable Step -- just reached one dict
        lookup later, via this template method rather than directly."""
        raise NotImplementedError(f"{type(self).__name__} (verb={self.verb!r}) is not gateable")
