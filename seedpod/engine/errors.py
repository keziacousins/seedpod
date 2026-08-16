"""engine/errors.py — re-exports the core error taxonomy for engine consumers.

docs/design/coherence-review.md Conflict 6: the error taxonomy has ONE home,
``seedpod/core/errors.py``. This module only RE-EXPORTS ``TransientError`` /
``PermanentError`` / ``InfrastructureUnreachableError`` (and, for convenience,
``ErrorCode``/``ProviderError``) from there — it must never define a sibling leaf.

``StepCancelled`` is engine-owned and defined here: it is raised by ``StepContext``
primitives (``sleep``, ``run_subprocess``) and ``CancelToken.raise_if_cancelled`` when
the token trips (docs/design/seam-b-engine.md §2.1). It is explicitly NOT a
``ProviderError`` — Schedule classification (engine/schedule.py) never retries it, and
it is never conflated with a step failure (Conflict 6's final comment line).
"""

from __future__ import annotations

from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
    TransientError,
)

__all__ = [
    "ErrorCode",
    "ProviderError",
    "TransientError",
    "PermanentError",
    "InfrastructureUnreachableError",
    "StepCancelled",
]


class StepCancelled(Exception):
    """Raised when a CancelToken trips while a step is inside a cancel-aware ctx
    primitive (``ctx.sleep``, ``ctx.run_subprocess``) or checks
    ``ctx.cancel.raise_if_cancelled()`` directly.

    Never retried by Schedule (docs/design/seam-b-engine.md §2.3.1: "StepCancelled ->
    never retried"). Deliberately NOT a ``ProviderError`` subclass — it is cooperative
    cancellation, not a step failure, and must never be classified as Transient or
    Permanent by anything that pattern-matches on the ``ProviderError`` hierarchy.
    """
