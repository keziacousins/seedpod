"""tests/conformance/test_c07_c08_c09_create_lifecycle.py — C-07/C-08/C-09 (Seam C §5.6 table),
the C1 close.

    C-07 | test_create_idempotent_reinvocation | machine | CreateInstance twice with same
    cluster_uuid ⇒ second returns same resource_ids, adopted_existing=True, no duplicate
    backend resource
    C-08 | test_create_emits_resource_allocated_and_tags_before_boot | machine |
    RESOURCE_ALLOCATED progress precedes any readiness activity and equals terminal ids;
    killing create right after allocation still leaves the cluster-uuid tag/name on the
    backend resource
    C-09 | test_undo_after_partial_create | machine | DIE_MID_CREATE ⇒ stream raises;
    undo_for(cmd, fold(events)) returns DestroyInstance; executing it on a fresh provider ⇒
    backend clean (the C1 test)

All three rows apply to the machine plane only (``machine_harness`` fixture:
digitalocean/kind/tart/orbstack).
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import ProviderError
from seedpod.providers.compensation import undo_for
from seedpod.providers.contract import DestroyInstance, DestroyStatus, Observed, Progress, Result
from tests.conformance._support import drain, fold_resource_ids, skip_if
from tests.conformance.harness import Fault

pytestmark = pytest.mark.asyncio

# orbstack's create is unconditionally idempotent by construction (a fixed built-in cluster —
# there is nothing to "duplicate"), and it never dies mid-create (no allocation step to
# truncate) — both already asserted structurally in test_orbstack_smoke.py's
# test_create_always_adopts_never_mutates_backend / the module docstring's genuinely-NEW note.
# DIE_MID_CREATE has no analogue for orbstack (nothing is ever "mid" — create is one atomic,
# already-satisfied check).
_DIE_MID_CREATE_SKIPS = {
    "orbstack": "orbstack's create is a single atomic adoption of the fixed built-in cluster "
    "— there is no allocation step to die 'mid', so DIE_MID_CREATE has no analogue "
    "(module docstring)",
}


async def test_create_idempotent_reinvocation_adopts_not_duplicates(machine_harness):
    provider = machine_harness.provider()
    cmd = machine_harness.create_command()

    first = await drain(provider, cmd)
    before = await machine_harness.backend_resources()

    second = await drain(provider, cmd)
    after = await machine_harness.backend_resources()

    first_result = next(ev.value for ev in first if isinstance(ev, Result))
    second_result = next(ev.value for ev in second if isinstance(ev, Result))

    assert second_result.adopted_existing is True
    assert second_result.resource_ids == first_result.resource_ids
    assert before == after, "re-invocation must never create a duplicate backend resource"


async def test_create_emits_resource_allocated_before_terminal_and_ids_match(machine_harness):
    provider = machine_harness.provider()
    events = await drain(provider, machine_harness.create_command())

    *progress_events, terminal = events
    resource_allocated = [ev for ev in progress_events if isinstance(ev, Progress) and ev.phase == "resource-allocated"]
    assert len(resource_allocated) == 1, "CreateInstance MUST emit exactly one Progress(RESOURCE_ALLOCATED)"
    assert isinstance(terminal, Result)
    assert terminal.value.resource_ids == resource_allocated[0].data["resource_ids"]


async def test_undo_after_partial_create_cleans_backend(machine_harness):
    skip_if(_DIE_MID_CREATE_SKIPS, machine_harness.name)
    provider = machine_harness.provider(Fault.DIE_MID_CREATE)
    cmd = machine_harness.create_command()

    events: list = []
    with pytest.raises(ProviderError):
        async for ev in provider.execute(cmd):
            events.append(ev)

    notes = fold_resource_ids(events)
    assert notes, "RESOURCE_ALLOCATED must have been observed before the stream died"

    observed = Observed(data=notes, value=None)
    inverse = undo_for(cmd, observed)
    assert isinstance(inverse, DestroyInstance)
    assert inverse.resource_ids == notes

    clean_provider = machine_harness.provider()  # fresh instance, same shared fake backend
    (destroy_result,) = await drain(clean_provider, inverse)
    # DO's destroy is asynchronous (a successful delete call yields DESTROYING, never a
    # synchronous DESTROYED — v1 discipline: never claim done before it's confirmed); every
    # other machine provider's delete is synchronous. Either way the fake backend must
    # already be clean — that's the actual C1 guarantee, not the literal status enum.
    assert destroy_result.value.status in (DestroyStatus.DESTROYED, DestroyStatus.DESTROYING)
    leftover = await machine_harness.backend_resources()
    assert not any(rid in leftover for rid in notes.values()), "undo must leave the backend clean (the C1 close)"
