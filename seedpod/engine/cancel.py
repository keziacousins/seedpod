"""engine/cancel.py — CancelToken: cooperative cancellation (Seam B §2.1, §2.3.5 G1-G5).

A CancelToken is per-run (forward execution) or per-undo-pass (compensation runs on a
FRESH, non-tripped token — §2.3.4). It carries no policy of its own; it is a level-
triggered flag plus the primitives steps need to observe it promptly:

- ``trip()`` is one-way and idempotent: once tripped, a token stays tripped forever;
  calling ``trip()`` again is a no-op. There is no ``reset()`` — a fresh CancelToken is
  a new object.
- ``tripped`` is level-triggered: it reflects "has trip() ever been called", not an
  edge. Any number of readers can observe it at any time.
- ``raise_if_cancelled()`` is the synchronous check steps/engine code call at unit
  boundaries (before starting a step, a foreach iteration, a retry attempt, a gate
  poll — G2).
- ``wait()`` resolves the instant the token trips (or immediately if already tripped),
  for ``asyncio.wait``-style races against subprocess completion / backoff sleeps (G3).
"""

from __future__ import annotations

import asyncio

from seedpod.engine.errors import StepCancelled

__all__ = ["CancelToken"]


class CancelToken:
    """Cooperative cancellation. Trip is one-way, idempotent, level-triggered."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def trip(self) -> None:
        """Idempotent: tripping an already-tripped token is a no-op."""
        self._event.set()

    @property
    def tripped(self) -> bool:
        return self._event.is_set()

    def raise_if_cancelled(self) -> None:
        """Raises StepCancelled if the token is tripped; a no-op otherwise."""
        if self._event.is_set():
            raise StepCancelled("cancel token tripped")

    async def wait(self) -> None:
        """Resolves when tripped; for select-style races. Returns immediately if the
        token is already tripped (asyncio.Event semantics)."""
        await self._event.wait()
