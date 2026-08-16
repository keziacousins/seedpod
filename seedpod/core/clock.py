"""Injected time source. ``now()`` lives here and NOWHERE else in ``seedpod/core/``.

Fresh v2 code (no v1 salvage — v1 called ``datetime.utcnow()``/``datetime.now()``
directly all over ``core/`` and ``jobs/``, which is exactly the naive-UTC-timestamp
gotcha this module exists to retire; see docs/design/seam-a-core.md §D and the
coherence review's "aware UTC everywhere" rule).

``Clock`` is a ``Protocol`` so ``core/`` stays pure (no IO) while still allowing the
composition root to inject a real clock and tests to inject a deterministic one.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

__all__ = ["Clock", "SystemClock", "FrozenClock"]


@runtime_checkable
class Clock(Protocol):
    """The only time source `core/` (and the rest of v2) may depend on."""

    def now(self) -> datetime:
        """Return the current instant as an aware UTC ``datetime``."""
        ...


class SystemClock:
    """Real wall-clock time, always aware UTC. Used only at the composition root."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class FrozenClock:
    """Deterministic clock for tests: settable and advanceable, never wall-clock.

    Always holds an aware UTC ``datetime``. Naive datetimes are rejected, matching
    the core-wide ban (docs/design/seam-a-core.md §B).
    """

    def __init__(self, at: datetime) -> None:
        self._at = _require_aware(at)

    def now(self) -> datetime:
        return self._at

    def set(self, at: datetime) -> None:
        self._at = _require_aware(at)

    def advance(self, delta: timedelta) -> None:
        self._at = self._at + delta


def _require_aware(at: datetime) -> datetime:
    if at.tzinfo is None:
        raise ValueError("FrozenClock requires an aware datetime; naive datetimes are banned")
    return at
