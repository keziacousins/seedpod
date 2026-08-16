"""engine/schedule.py — Seam B §2.3.1: Schedule (retry policy) + NAMED_POLICIES, verbatim.

``NAMED_POLICIES`` and each verb's ``default_retry`` ClassVar are the SOLE retry
authority (docs/design/coherence-review.md Conflict 6 deletes Seam C's inline retry
default). Classification is fixed, not configurable:

- ``TransientError`` and per-attempt timeout expiry -> retry until ``max_attempts``,
  then the step fails with the last error.
- ``PermanentError`` and any other exception -> fail immediately.
- ``StepCancelled`` -> never retried.
- ``InfrastructureUnreachableError`` -> NEVER consumes Schedule budget and never
  triggers/continues compensation (coherence-review Conflict 5, replacing Seam B's
  "treated as Transient" sentence wholesale). It is classified here as its own
  ``Outcome.UNREACHABLE`` precisely so callers cannot accidentally fold it into RETRY
  or FAIL — the blocked-park law (engine.py, not this module) is the only legal
  response.

Gate polls are explicitly NOT Schedule-retried: a poll raising TransientError
increments a consecutive-failure counter (reset on any successful poll) and fails the
gate only at ``max_consecutive_poll_failures`` — that hysteresis loop is engine-owned
(engine/engine.py, out of this file's scope) and must never reach for a Schedule.

The same Schedule machinery wraps ``undo`` calls, but never below ``max_attempts=2``
(a transient API blip during compensation must not leak a droplet) — see
``schedule_for_undo`` below.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import StrEnum

from seedpod.engine.errors import (
    InfrastructureUnreachableError,
    PermanentError,
    StepCancelled,
    TransientError,
)

__all__ = [
    "Schedule",
    "NAMED_POLICIES",
    "Outcome",
    "classify",
    "delay_seconds",
    "schedule_for_undo",
]


@dataclass(frozen=True)
class Schedule:
    max_attempts: int = 1  # total attempts, not retries
    base_delay_seconds: float = 5.0
    factor: float = 2.0  # delay_n = min(max_delay, base*factor**(n-1)) * uniform(1±jitter)
    max_delay_seconds: float = 60.0
    jitter: float = 0.1


NAMED_POLICIES: dict[str, Schedule] = {
    "none": Schedule(),
    "api_default": Schedule(3, 2.0, 2.0, 30.0),  # closes H4/H5 (GHCR/CF now engine-retried)
    "ssh_default": Schedule(3, 5.0, 2.0, 60.0),  # replicates _ssh_k3s_installer 3x/5s/exp exactly
    "kubectl_default": Schedule(3, 2.0, 2.0, 15.0),  # closes H6
}


class Outcome(StrEnum):
    """The fixed classification of an exception raised from a step attempt."""

    RETRY = "retry"  # TransientError / per-attempt timeout expiry
    FAIL = "fail"  # PermanentError / any other exception
    CANCELLED = "cancelled"  # StepCancelled — never retried
    UNREACHABLE = "unreachable"  # InfrastructureUnreachableError — never Schedule-classified


def classify(exc: BaseException) -> Outcome:
    """The fixed exception -> Outcome mapping (docs/design/seam-b-engine.md §2.3.1,
    amended by coherence-review Conflict 5 for InfrastructureUnreachableError).

    Order matters: StepCancelled and InfrastructureUnreachableError are checked before
    TransientError/PermanentError because both taxonomies are exception hierarchies a
    naive `except TransientError` could otherwise shadow incorrectly — here they are
    siblings, not subclasses, so order is a defensive clarity choice, not a correctness
    requirement of the current class hierarchy.
    """
    if isinstance(exc, StepCancelled):
        return Outcome.CANCELLED
    if isinstance(exc, InfrastructureUnreachableError):
        return Outcome.UNREACHABLE
    if isinstance(exc, TimeoutError):  # per-attempt timeout expiry (asyncio.timeout / TimeoutError)
        return Outcome.RETRY
    if isinstance(exc, TransientError):
        return Outcome.RETRY
    if isinstance(exc, PermanentError):
        return Outcome.FAIL
    return Outcome.FAIL  # any other exception ≡ Permanent


def delay_seconds(
    schedule: Schedule,
    attempt: int,
    *,
    retry_after: float | None = None,
    rng: Callable[[], float] = random.random,
) -> float:
    """The backoff delay before starting ``attempt`` (the upcoming attempt number,
    2-based: attempt=2 is the first retry after attempt=1 failed).

    ``retry_after`` (from ``TransientError.retry_after``) overrides the computed delay
    entirely when given (coherence-review Conflict 6). Otherwise:

        n = attempt - 1                                    # 1-based retry index
        delay = min(max_delay_seconds, base_delay_seconds * factor ** (n - 1))
        delay *= uniform(1 - jitter, 1 + jitter)

    ``rng`` is injected (defaults to ``random.random``) so tests can pin the jitter
    multiplier deterministically without patching the stdlib ``random`` module.
    """
    if attempt < 2:
        raise ValueError(f"delay_seconds is only defined for retry attempts (attempt >= 2), got {attempt}")
    if retry_after is not None:
        return max(0.0, retry_after)
    n = attempt - 1
    base = min(schedule.max_delay_seconds, schedule.base_delay_seconds * schedule.factor ** (n - 1))
    low, high = 1 - schedule.jitter, 1 + schedule.jitter
    multiplier = low + rng() * (high - low)
    return base * multiplier


def schedule_for_undo(retry: Schedule) -> Schedule:
    """The same Schedule machinery wraps undo calls (verb default), but never below
    ``max_attempts=2`` — a transient API blip during compensation must not leak a
    droplet (docs/design/seam-b-engine.md §2.3.1)."""
    if retry.max_attempts >= 2:
        return retry
    return replace(retry, max_attempts=2)
