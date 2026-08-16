"""tests/conformance/test_c23_c24_undo_and_unsupported.py — C-23/C-24 (Seam C §5.6 table).

    C-23 | test_undo_mapping_total_and_idempotent | all | undo_for returns None or an
    in-union command for every supported command; every returned undo executed twice
    succeeds twice
    C-24 | test_unsupported_command_rejected | all | command outside supported ⇒
    Permanent(UNSUPPORTED) with zero backend traffic

C-23's *totality* half (every command in ``ProviderCommand`` maps to ``None`` or an in-union
command, deterministically) is a pure-function property tested exhaustively — no fake, no
harness — in ``tests/providers/test_compensation.py``. This file covers the half that needs a
real (fake) transport: an undo command, once produced, is safe to *execute* twice.

C-24 picks, for each harness, the first of a small candidate list that isn't in
``provider.supported`` — genuinely generic across all six providers' differing plane matrices,
rather than a hand-picked command per provider.
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import PermanentError
from seedpod.providers.compensation import undo_for
from seedpod.providers.contract import (
    DestroyStatus,
    FetchKubeconfig,
    KubeApplyManifest,
    KubeDeleteManifest,
    ListInstances,
    Observed,
    ProbeSshPort,
    Result,
)
from tests.conformance._support import drain
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG, KubectlHarness

pytestmark = pytest.mark.asyncio

_MANIFEST = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: demo
  namespace: default
spec:
  replicas: 1
"""

# ---------------------------------------------------------------------------
# C-23 — undo executed twice succeeds twice
# ---------------------------------------------------------------------------


async def test_create_instance_undo_executed_twice_succeeds_twice(machine_harness):
    provider = machine_harness.provider()
    cmd = machine_harness.create_command()
    (result,) = [ev for ev in await drain(provider, cmd) if isinstance(ev, Result)]

    inverse = undo_for(cmd, Observed(data={}, value=result.value))
    assert inverse is not None

    for _ in range(2):
        (outcome,) = await drain(provider, inverse)
        # DO's destroy is asynchronous (a successful call ⇒ DESTROYING, confirmed later via
        # ProbeDestruction) — every other machine provider's delete is synchronous DESTROYED.
        # Both are "success executed twice"; neither is DESTROY_FAILED.
        assert outcome.value.status in (DestroyStatus.DESTROYED, DestroyStatus.DESTROYING)


async def test_kube_apply_manifest_undo_executed_twice_succeeds_twice():
    harness = KubectlHarness()
    provider = harness.provider()
    apply_cmd = KubeApplyManifest(kubeconfig=FAKE_KUBECONFIG, manifest_yaml=_MANIFEST)
    await drain(provider, apply_cmd)

    inverse = undo_for(apply_cmd, Observed(data={}, value=None))
    assert isinstance(inverse, KubeDeleteManifest)

    for _ in range(2):
        await drain(provider, inverse)  # ignore_not_found=True: idempotent both times
    assert await harness.backend_resources() == frozenset()


# ---------------------------------------------------------------------------
# C-24 — unsupported command rejected, zero backend traffic
# ---------------------------------------------------------------------------

_UNSUPPORTED_CANDIDATES = (
    ProbeSshPort(host="unsupported-probe"),
    ListInstances(),
    FetchKubeconfig(rewrite_server_to="https://example.invalid:6443"),
)


async def test_unsupported_command_rejected_with_zero_backend_traffic(harness):
    provider = harness.provider()
    cmd = next((c for c in _UNSUPPORTED_CANDIDATES if type(c) not in provider.supported), None)
    if cmd is None:
        pytest.skip(f"{harness.name} supports every C-24 candidate probe command")

    before = harness.backend_attempts()
    with pytest.raises(PermanentError) as excinfo:
        provider.execute(cmd)
    assert excinfo.value.code == "unsupported"
    assert harness.backend_attempts() == before
