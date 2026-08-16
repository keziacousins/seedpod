"""tests/engine/test_errors.py — engine/errors.py re-exports the core taxonomy verbatim
and StepCancelled is engine-owned, NOT a ProviderError (coherence-review Conflict 6).
"""

from __future__ import annotations

from seedpod.core import errors as core_errors
from seedpod.engine import errors as engine_errors


def test_reexports_are_identical_objects_not_copies():
    assert engine_errors.TransientError is core_errors.TransientError
    assert engine_errors.PermanentError is core_errors.PermanentError
    assert engine_errors.InfrastructureUnreachableError is core_errors.InfrastructureUnreachableError
    assert engine_errors.ProviderError is core_errors.ProviderError
    assert engine_errors.ErrorCode is core_errors.ErrorCode


def test_step_cancelled_is_not_a_provider_error():
    assert not issubclass(engine_errors.StepCancelled, engine_errors.ProviderError)
    assert not issubclass(engine_errors.StepCancelled, engine_errors.TransientError)
    assert not issubclass(engine_errors.StepCancelled, engine_errors.PermanentError)


def test_step_cancelled_is_a_plain_exception():
    exc = engine_errors.StepCancelled("cancelled")
    assert isinstance(exc, Exception)
    assert str(exc) == "cancelled"
