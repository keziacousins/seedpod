"""tests/conformance/_support.py — shared plumbing for the C-01..C-24 conformance suite
(docs/design/seam-c-provider.md §5.6), amended by docs/design/coherence-review.md.

Not a ``conftest.py`` on purpose: fixtures live in ``conftest.py``; this module holds plain,
directly-importable helpers and constants so individual ``test_cNN_*.py`` files can pick exactly
what they need without going through pytest's fixture-injection machinery for things that are
just functions (``_drain``, ``_fold_resource_ids``) or data (the harness class list, capability
skip lists).

No ``Mock``/``patch`` anywhere in this module or anything it supports (CLAUDE.md).
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from seedpod.providers.contract import Progress, ProviderEvent
from tests.conformance.digitalocean_harness import DigitalOceanHarness
from tests.conformance.harness import Harness
from tests.conformance.kind_harness import KindHarness
from tests.conformance.kubectl_harness import KubectlHarness
from tests.conformance.orbstack_harness import OrbstackHarness
from tests.conformance.ssh_k3s_harness import SshK3sHarness
from tests.conformance.tart_harness import TartHarness

__all__ = [
    "HARNESS_CLASSES",
    "HARNESS_IDS",
    "MACHINE_NAMES",
    "MACHINE_HARNESS_CLASSES",
    "drain",
    "fold_resource_ids",
    "skip_if",
    "classification_rows",
]

# The six harnesses left by the provider agents (Seam C §5.6's "parametrized over all six
# providers via a per-provider Harness"). Order is stable so parametrize ids stay stable.
HARNESS_CLASSES: tuple[type[Harness], ...] = (
    DigitalOceanHarness,
    KindHarness,
    TartHarness,
    OrbstackHarness,
    SshK3sHarness,
    KubectlHarness,
)
HARNESS_IDS: tuple[str, ...] = tuple(cls.name for cls in HARNESS_CLASSES)

# "Machine" plane per §5.4's plane matrix: digitalocean | kind | tart | orbstack.
MACHINE_NAMES: frozenset[str] = frozenset({"digitalocean", "kind", "tart", "orbstack"})
MACHINE_HARNESS_CLASSES: tuple[type[Harness], ...] = tuple(
    cls for cls in HARNESS_CLASSES if cls.name in MACHINE_NAMES
)


async def drain(provider, cmd) -> list[ProviderEvent]:
    """Consume a ``Provider.execute()`` stream to completion (or until it raises)."""
    events: list[ProviderEvent] = []
    async for ev in provider.execute(cmd):
        events.append(ev)
    return events


def fold_resource_ids(events: list[ProviderEvent]) -> dict[str, str]:
    """Mirrors ``engine/provider_step.py``'s ``ctx.note(**{k: str(v) for k, v in
    d.get("resource_ids", {}).items()})`` fold (Conflict 7) without importing the engine —
    the same helper every provider smoke test defines locally, centralized here."""
    notes: dict[str, str] = {}
    for ev in events:
        if isinstance(ev, Progress) and ev.phase == "resource-allocated":
            notes.update({str(k): str(v) for k, v in ev.data.get("resource_ids", {}).items()})
    return notes


def skip_if(skips: Mapping[str, str], name: str) -> None:
    """Capability skip list lookup (Seam C §5.6: "each provider registers a capability skip
    list for structurally inapplicable cases ... the skip list is reviewed like a verb
    addition"). ``skips`` maps harness name -> a reason string; every skip in this suite goes
    through this one function so `git grep skip_if` finds the whole reviewable list."""
    if name in skips:
        pytest.skip(skips[name])


def classification_rows() -> list:
    """Flattens every harness's ``classification_cases()`` into one parametrize table:
    ``(harness_cls, fault, expected_cls, expected_code)``, id'd ``{provider}-{fault}-{code}``.
    Shared by C-04 (Unreachable-only subset) and C-17 (the full table)."""
    rows = []
    for cls in HARNESS_CLASSES:
        h = cls()
        for fault, expected_cls, expected_code in h.classification_cases():
            rows.append(
                pytest.param(
                    cls, fault, expected_cls, expected_code, id=f"{cls.name}-{fault.value}-{expected_code.value}"
                )
            )
    return rows
