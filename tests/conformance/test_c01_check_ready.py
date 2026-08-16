"""tests/conformance/test_c01_check_ready.py — C-01 (Seam C §5.6 table).

    C-01 | test_check_ready_fails_fast | all + services (⊂) | broken environment (missing
    binary / base image / token) ⇒ check_ready raises Permanent or Unreachable before any
    command runs

Parametrized over all six providers via ``tests.conformance.conftest.harness``. No
``Mock``/``patch`` — every harness's ``broken_environment()`` is backed by its own fake
transport (CLAUDE.md).
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import InfrastructureUnreachableError, PermanentError

pytestmark = pytest.mark.asyncio


async def test_check_ready_succeeds_against_healthy_backend(harness):
    provider = harness.provider()
    await provider.check_ready()  # must not raise


async def test_check_ready_fails_fast_on_broken_environment(harness):
    with harness.broken_environment() as provider:
        with pytest.raises((PermanentError, InfrastructureUnreachableError)) as excinfo:
            await provider.check_ready()
        # Exactly one of the two taxonomy leaves check_ready is allowed to raise (§5.4's
        # Provider protocol docstring) — never a bare Exception, never a silent pass.
        assert isinstance(excinfo.value, (PermanentError, InfrastructureUnreachableError))
