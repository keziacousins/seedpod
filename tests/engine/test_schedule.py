"""tests/engine/test_schedule.py — Schedule delay math (incl. jitter bounds),
retry_after override, classification (docs/design/seam-b-engine.md §2.3.1). No
Mock/patch: jitter is pinned via an injected `rng` callable, never `random.random`
patching.
"""

from __future__ import annotations

import pytest

from seedpod.engine.errors import (
    InfrastructureUnreachableError,
    PermanentError,
    StepCancelled,
    TransientError,
)
from seedpod.engine.schedule import (
    NAMED_POLICIES,
    Outcome,
    Schedule,
    classify,
    delay_seconds,
    schedule_for_undo,
)

# ----------------------------------------------------------------------------
# NAMED_POLICIES verbatim (Seam B §2.3.1)
# ----------------------------------------------------------------------------


def test_named_policies_verbatim_values():
    assert NAMED_POLICIES["none"] == Schedule()
    assert NAMED_POLICIES["api_default"] == Schedule(3, 2.0, 2.0, 30.0)
    assert NAMED_POLICIES["ssh_default"] == Schedule(3, 5.0, 2.0, 60.0)
    assert NAMED_POLICIES["kubectl_default"] == Schedule(3, 2.0, 2.0, 15.0)


def test_schedule_default_is_single_attempt_no_retry():
    s = Schedule()
    assert s.max_attempts == 1
    assert s.base_delay_seconds == 5.0
    assert s.factor == 2.0
    assert s.max_delay_seconds == 60.0
    assert s.jitter == 0.1


def test_schedule_is_frozen():
    import dataclasses

    s = Schedule()
    with pytest.raises(dataclasses.FrozenInstanceError):
        s.max_attempts = 5  # type: ignore[misc]


# ----------------------------------------------------------------------------
# delay_seconds: math, jitter bounds, retry_after override
# ----------------------------------------------------------------------------


def const_rng(value: float):
    return lambda: value


def test_delay_seconds_rejects_first_attempt():
    with pytest.raises(ValueError):
        delay_seconds(Schedule(), attempt=1)


def test_delay_seconds_base_case_no_jitter():
    schedule = Schedule(max_attempts=3, base_delay_seconds=2.0, factor=2.0, max_delay_seconds=30.0, jitter=0.0)
    # attempt=2 -> n=1 -> base * factor**0 == base
    assert delay_seconds(schedule, attempt=2, rng=const_rng(0.5)) == pytest.approx(2.0)


def test_delay_seconds_exponential_growth_no_jitter():
    schedule = Schedule(max_attempts=5, base_delay_seconds=2.0, factor=2.0, max_delay_seconds=1000.0, jitter=0.0)
    assert delay_seconds(schedule, attempt=2, rng=const_rng(0.5)) == pytest.approx(2.0)  # base
    assert delay_seconds(schedule, attempt=3, rng=const_rng(0.5)) == pytest.approx(4.0)  # base*factor
    assert delay_seconds(schedule, attempt=4, rng=const_rng(0.5)) == pytest.approx(8.0)  # base*factor**2


def test_delay_seconds_caps_at_max_delay():
    schedule = Schedule(max_attempts=10, base_delay_seconds=2.0, factor=2.0, max_delay_seconds=15.0, jitter=0.0)
    # attempt=6 -> n=5 -> base*factor**4 = 2*16 = 32, capped to 15
    assert delay_seconds(schedule, attempt=6, rng=const_rng(0.5)) == pytest.approx(15.0)


def test_delay_seconds_jitter_lower_bound():
    schedule = NAMED_POLICIES["kubectl_default"]  # base=2.0, jitter=0.1
    delay = delay_seconds(schedule, attempt=2, rng=const_rng(0.0))
    assert delay == pytest.approx(2.0 * 0.9)


def test_delay_seconds_jitter_upper_bound():
    schedule = NAMED_POLICIES["kubectl_default"]
    delay = delay_seconds(schedule, attempt=2, rng=const_rng(1.0))
    assert delay == pytest.approx(2.0 * 1.1)


def test_delay_seconds_jitter_stays_within_bounds_for_full_rng_range():
    schedule = NAMED_POLICIES["ssh_default"]  # base=5.0, jitter=0.1
    low = schedule.base_delay_seconds * (1 - schedule.jitter)
    high = schedule.base_delay_seconds * (1 + schedule.jitter)
    for r in (0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0):
        delay = delay_seconds(schedule, attempt=2, rng=const_rng(r))
        assert low - 1e-9 <= delay <= high + 1e-9


def test_delay_seconds_retry_after_overrides_computed_delay():
    schedule = NAMED_POLICIES["api_default"]
    delay = delay_seconds(schedule, attempt=2, retry_after=99.5, rng=const_rng(0.5))
    assert delay == 99.5


def test_delay_seconds_retry_after_negative_clamped_to_zero():
    schedule = NAMED_POLICIES["api_default"]
    delay = delay_seconds(schedule, attempt=2, retry_after=-5.0, rng=const_rng(0.5))
    assert delay == 0.0


# ----------------------------------------------------------------------------
# classification (fixed, not configurable)
# ----------------------------------------------------------------------------


def test_classify_transient_error_with_code():
    from seedpod.core.errors import ErrorCode

    assert classify(TransientError("boom", code=ErrorCode.API_TIMEOUT)) == Outcome.RETRY


def test_classify_permanent_is_fail():
    from seedpod.core.errors import ErrorCode

    assert classify(PermanentError("nope", code=ErrorCode.INVALID_INPUT)) == Outcome.FAIL


def test_classify_step_cancelled_is_cancelled():
    assert classify(StepCancelled("cancelled")) == Outcome.CANCELLED


def test_classify_unreachable_is_its_own_outcome_not_transient_not_fail():
    from seedpod.core.errors import ErrorCode

    exc = InfrastructureUnreachableError("can't tell", code=ErrorCode.ENDPOINT_UNREACHABLE)
    outcome = classify(exc)
    assert outcome == Outcome.UNREACHABLE
    assert outcome not in (Outcome.RETRY, Outcome.FAIL)


def test_classify_timeout_error_is_retry():
    assert classify(TimeoutError("attempt timed out")) == Outcome.RETRY


def test_classify_arbitrary_exception_is_fail():
    assert classify(ValueError("weird")) == Outcome.FAIL
    assert classify(RuntimeError("weird")) == Outcome.FAIL
    assert classify(KeyError("weird")) == Outcome.FAIL


# ----------------------------------------------------------------------------
# schedule_for_undo: min max_attempts=2
# ----------------------------------------------------------------------------


def test_schedule_for_undo_bumps_single_attempt_policy_to_two():
    undo_schedule = schedule_for_undo(NAMED_POLICIES["none"])
    assert undo_schedule.max_attempts == 2
    # everything else about "none" is preserved
    assert undo_schedule.base_delay_seconds == NAMED_POLICIES["none"].base_delay_seconds


def test_schedule_for_undo_leaves_already_retrying_policy_alone():
    undo_schedule = schedule_for_undo(NAMED_POLICIES["kubectl_default"])
    assert undo_schedule == NAMED_POLICIES["kubectl_default"]


def test_schedule_for_undo_never_lowers_max_attempts():
    custom = Schedule(max_attempts=5)
    assert schedule_for_undo(custom).max_attempts == 5
