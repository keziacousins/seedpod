"""tests/conformance/harness.py — the shared conformance ``Harness`` protocol, Seam C §5.6
(``docs/design/seam-c-provider.md``), materialized in code.

Every provider under ``tests/conformance/`` implements this ``Protocol`` so the shared,
parametrized C-01..C-24 suite (written by a later agent, per the Pillar-3 task split) can run
identically across all six providers. Nothing here talks to a real backend: each concrete
``Harness.provider()`` returns a ``Provider`` backed by a typed FAKE TRANSPORT (an
``httpx.AsyncBaseTransport`` for HTTP-speaking providers, a ``SubprocessRunner`` implementation
for CLI-speaking ones) with canned frames mined from v1 behavior. Fault injection happens at
that transport seam only — never ``Mock``/``patch`` (CLAUDE.md, Seam C §5.4).

``ReconcileCase`` is referenced by name in the seam spec's ``Harness`` code block but never
defined there; it is defined here (first Pillar-3 provider to need it) so every subsequent
harness imports the same shape instead of re-inventing it.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from seedpod.core.errors import ErrorCode, ProviderError
from seedpod.providers.contract import CreateInstance, FetchKubeconfig, Provider, ProviderCommand

__all__ = ["Fault", "ReconcileCase", "Harness"]


class Fault(StrEnum):
    """Injected failure modes a ``Harness.provider()`` can compose (Seam C §5.6). Not every
    provider has an analogue for every member (e.g. ``MISSING_SOURCE`` is a tart/kind "source
    image absent" concept); a harness maps each fault it doesn't have a literal match for onto
    its closest structural equivalent (documented in that harness's ``classification_cases()``)
    rather than silently ignoring it.
    """

    UNREACHABLE = "unreachable"
    TRANSIENT_ONCE = "transient-once"
    AUTH = "auth"
    MISSING_SOURCE = "missing-source"
    RATE_LIMIT = "rate-limit"
    DIE_MID_CREATE = "die-mid-create"


@dataclass(frozen=True)
class ReconcileCase:
    """One row of a provider's C-13 reconcile truth table: a synthetic backend reality
    (``backend_present``/``backend_tagged``) crossed with a DB-side ``ClusterSnapshot.status``
    (``db_status``; ``None`` means "no DB row at all" — an untracked backend resource), and the
    ``IntentType`` value the ``Reconcile`` command is expected to produce for that combination
    (``None`` means "no intent" — e.g. an untagged/unmanaged backend resource is skipped).
    """

    name: str
    db_status: str | None
    backend_present: bool
    backend_tagged: bool = True
    expected_intent: str | None = None


class Harness(Protocol):
    name: str

    def provider(self, *faults: Fault) -> Provider:
        """A fresh ``Provider`` instance backed by this harness's fake transport, composed
        with zero or more injected ``Fault``s."""
        ...

    def broken_environment(self) -> AbstractContextManager:
        """Context manager yielding a ``Provider`` whose ``check_ready()`` must fail
        (C-01) — missing binary / base image / token, depending on the provider."""
        ...

    async def backend_resources(self) -> frozenset[str]:
        """Raw backend truth (resource ids currently "alive" in the fake backend) — the leak
        check C-09/C-10/C-21 assert against."""
        ...

    def backend_attempts(self) -> int:
        """Cumulative transport call counter, for C-15's single-attempt assertion and C-24's
        zero-backend-traffic assertion."""
        ...

    def create_command(self) -> CreateInstance:
        """A ready-to-execute ``CreateInstance`` for this provider (machine providers only)."""
        ...

    def observe_command(self) -> ProviderCommand:
        """The cheapest state-read command this provider supports, targeting a resource the
        harness has already seeded in its fake backend."""
        ...

    def reconcile_truth_table(self) -> Sequence[ReconcileCase]:
        """C-13's parametrization source (machine providers only)."""
        ...

    def rewrite_cases(self) -> Sequence[tuple[str, FetchKubeconfig, str]]:
        """C-19 golden kubeconfig-rewrite cases: (case name, command, expected substring/regex
        match against the rewritten server URL). Empty for providers that don't implement
        ``FetchKubeconfig``."""
        ...

    def classification_cases(self) -> Sequence[tuple[Fault, type[ProviderError], ErrorCode]]:
        """C-17's parametrization source: for each fault this provider honors, the
        (exception class, ``ErrorCode``) a representative command must raise."""
        ...
