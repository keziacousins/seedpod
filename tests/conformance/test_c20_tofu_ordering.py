"""tests/conformance/test_c20_tofu_ordering.py — C-20 (Seam C §5.6 table), crown jewel #2.

    C-20 | test_tofu_ordering | ssh-k3s | fake sshd asserts cloud-init status --wait (the sole
    non-strict call) precedes keyscan; InstallK3s(known_hosts="") ⇒ Permanent(INVALID_INPUT);
    all post-capture SSH uses strict checking

ssh-k3s-only by the table's own "Applies" column — no generic ``harness`` fixture needed.
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import PermanentError
from tests.conformance._support import drain
from tests.conformance.ssh_k3s_harness import SshK3sHarness

pytestmark = pytest.mark.asyncio


async def test_cloud_init_precedes_keyscan_as_the_sole_insecure_call():
    harness = SshK3sHarness()
    provider = harness.provider()
    await drain(provider, harness.capture_host_keys_command())

    calls = harness.backend.call_log
    cloud_init_idx = next(i for i, c in enumerate(calls) if "cloud-init status --wait" in c[-1])
    keyscan_idx = next(i for i, c in enumerate(calls) if c[0] == "ssh-keyscan")
    assert cloud_init_idx < keyscan_idx

    insecure_calls = [c for c in calls if "StrictHostKeyChecking=no" in c]
    assert len(insecure_calls) == 1, "cloud-init status --wait must be the ONLY insecure call"
    assert insecure_calls[0] == calls[cloud_init_idx]


async def test_install_before_keys_is_rejected_zero_backend_traffic():
    harness = SshK3sHarness()
    provider = harness.provider()
    before = harness.backend_attempts()

    with pytest.raises(PermanentError) as excinfo:
        await drain(provider, harness.install_k3s_command(known_hosts=""))
    assert excinfo.value.code == "invalid_input"
    assert harness.backend_attempts() == before


async def test_every_post_capture_ssh_call_uses_strict_checking():
    harness = SshK3sHarness()
    provider = harness.provider()
    await drain(provider, harness.install_k3s_command())

    strict_calls = [c for c in harness.backend.call_log if c[0] == "ssh" and len(c) > 2 and "-i" in c]
    assert strict_calls, "expected at least one strict ssh invocation"
    for call in strict_calls:
        assert "StrictHostKeyChecking=yes" in call
        assert "StrictHostKeyChecking=no" not in call
