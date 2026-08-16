"""tests/conformance/test_c02_stream_shape.py — C-02 (Seam C §5.6 table).

    C-02 | test_stream_shape | all | every supported command: >=0 Progress then exactly one
    Result, nothing after; errors raised never yielded

Parametrized over all six providers via ``tests.conformance.conftest.harness``. Each
provider's ``create_command()``/``observe_command()`` are the two representative commands
every ``Harness`` implementation is guaranteed to supply (``create_command()`` is structurally
inapplicable to the k3s/kubernetes-plane providers — see their harnesses' module docstrings —
so it's skipped there via the documented ``NotImplementedError``, not weakened silently).
Provider-local smoke tests (``test_*_smoke.py``) additionally cover stream shape for every
other individual command each provider supports (``InstallK3s``, ``KubeApplyManifest``,
``KubeWatchPods``, ...).
"""

from __future__ import annotations

import pytest

from seedpod.providers.contract import Progress, Result
from tests.conformance._support import drain

pytestmark = pytest.mark.asyncio


async def test_create_stream_shape_progress_then_result(harness):
    try:
        cmd = harness.create_command()
    except NotImplementedError:
        pytest.skip(f"{harness.name} has no CreateInstance concept (k3s/kubernetes plane only)")

    events = await drain(harness.provider(), cmd)

    assert events, "create must yield at least the terminal Result"
    *progress_events, terminal = events
    assert all(isinstance(ev, Progress) for ev in progress_events), "only Progress may precede the terminal Result"
    assert isinstance(terminal, Result), "the stream's last event must be the terminal Result"


async def test_observe_stream_shape_result_only(harness):
    events = await drain(harness.provider(), harness.observe_command())
    assert len(events) == 1, "the cheapest state-read command must yield exactly one event"
    assert isinstance(events[0], Result)


async def test_error_raised_never_yielded_as_an_event(harness):
    """A faulted execute() may yield zero or more Progress events before dying, but it must
    never yield a Result and then also raise, and it must never yield something that *looks*
    like an error — the taxonomy error is RAISED, not smuggled into the stream as data."""
    cases = harness.classification_cases()
    if not cases:
        pytest.skip(f"{harness.name} declares no classification_cases()")
    fault, expected_cls, _ = cases[0]
    provider = harness.provider(fault)
    cmd = harness.classification_command(fault)

    events: list = []
    with pytest.raises(expected_cls):
        async for ev in provider.execute(cmd):
            events.append(ev)

    assert not any(isinstance(ev, Result) for ev in events), "no Result may precede a raised error"
