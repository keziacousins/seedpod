"""tests/conformance/test_c17_classification.py — C-17 (Seam C §5.6 table).

    C-17 | test_error_classification_table | all + services (⊂) | each harness
    classification_cases() row: injected fault ⇒ expected (class, code); envelope complete
    (code/provider/command set, raw stderr/status only in detail)

One parametrized test over the flattened ``classification_rows()`` from every harness (36 rows
across all six providers as of this writing) — consolidating what was six separate per-provider
``test_classification_table`` loops in the smoke suites into the shared table the seam
mandates.
"""

from __future__ import annotations

import pytest

from tests.conformance._support import classification_rows, drain

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("harness_cls,fault,expected_cls,expected_code", classification_rows())
async def test_classification_table(harness_cls, fault, expected_cls, expected_code):
    harness = harness_cls()
    provider = harness.provider(fault)
    cmd = harness.classification_command(fault)

    with pytest.raises(expected_cls) as excinfo:
        await drain(provider, cmd)

    err = excinfo.value
    assert err.code == expected_code
    # Envelope completeness (§5.1's ProviderError base): code/provider/command always set;
    # raw stderr/http-status live ONLY in `detail`, never smuggled into the top-level message
    # as the sole carrier (detail may legitimately be empty for e.g. a synthesized error, but
    # provider/command must always identify the call site).
    assert err.provider
    assert err.command
    assert isinstance(err.detail, dict)
