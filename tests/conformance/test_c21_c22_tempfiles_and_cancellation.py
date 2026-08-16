"""tests/conformance/test_c21_c22_tempfiles_and_cancellation.py — C-21/C-22 (Seam C §5.6
table), H17.

    C-21 | test_tempfile_hygiene | all that touch disk | temp files live under the registry
    dir, mode 0600; after completion the dir is empty; simulated hard-kill + startup sweep
    removes strays (H17)
    C-22 | test_cancellation_cleanup | all | cancelling mid-execute (KubeWatchPods, InstallK3s)
    ⇒ tracked subprocess terminated, temp files unlinked, CancelledError re-raised

"All that touch disk" (C-21) is ``kind``/``ssh-k3s``/``kubectl`` — the only three providers
that import ``seedpod.core.tempfiles.TempFileRegistry`` at all (``digitalocean``/``tart`` speak
HTTP/a CLI with no file arguments; ``orbstack`` shells straight through ``kubectl`` without a
kubeconfig file of its own). "All" (C-22) is narrowed by the table's own worked examples to the
two natively cancellable long-running commands, ``KubeWatchPods`` and ``InstallK3s`` — no other
command in the whole contract has anything to cancel mid-flight (every other call is one bounded
request/response against a fake transport that returns synchronously).
"""

from __future__ import annotations

import asyncio
import stat

import pytest

from seedpod.core.tempfiles import TempFileRegistry
from seedpod.providers.contract import KubeWatchPods
from tests.conformance._support import drain, skip_if
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG, NAMESPACE, KubectlHarness
from tests.conformance.ssh_k3s_harness import SshK3sHarness

# No blanket `pytestmark = pytest.mark.asyncio`: this module mixes sync (TempFileRegistry
# unit tests) and async tests; `asyncio_mode = "auto"` (pyproject.toml) already async-wraps
# the latter without it.

_C21_SKIPS = dict.fromkeys(("digitalocean", "tart", "orbstack"), "no TempFileRegistry usage — speaks HTTP/a CLI with no file arguments (see module docstring)")

# ---------------------------------------------------------------------------
# C-21 — TempFileRegistry unit-level guarantees (provider-agnostic)
# ---------------------------------------------------------------------------


def test_temp_file_registry_creates_0600_files(tmp_path):
    registry = TempFileRegistry(root=tmp_path)
    with registry.file("secret content", suffix=".yml") as path:
        assert path.parent == tmp_path
        mode = stat.S_IMODE(path.stat().st_mode)
        assert mode == 0o600
    assert not path.exists(), "the file must be unlinked on context-manager exit"


def test_temp_file_registry_sweep_removes_stale_entries(tmp_path):
    registry = TempFileRegistry(root=tmp_path)
    stale = registry.create("orphaned by a hard kill", suffix=".yml")
    assert stale.exists()

    removed = TempFileRegistry.sweep(root=tmp_path)
    assert stale.name in removed
    assert not stale.exists()


# ---------------------------------------------------------------------------
# C-21 — no residue left behind by an actual provider call
# ---------------------------------------------------------------------------


async def test_provider_leaves_no_tempfile_residue(harness):
    skip_if(_C21_SKIPS, harness.name)
    provider = harness.provider()

    if harness.name == "kind":
        cmd = harness.create_command()
    elif harness.name == "ssh-k3s":
        cmd = harness.install_k3s_command()
    else:  # kubectl
        cmd = harness.observe_command()

    await drain(provider, cmd)

    registry_root = provider._tempfiles.root
    if registry_root.exists():
        assert list(registry_root.iterdir()) == [], f"{harness.name}: temp files leaked after execute()"


# ---------------------------------------------------------------------------
# C-22 — cancellation cleanup for the two natively cancellable commands
# ---------------------------------------------------------------------------


async def test_kube_watch_pods_cancellation_unlinks_tempfile_and_reraises():
    harness = KubectlHarness()
    harness.backend.watch_lines = [b'{"type": "ADDED", "object": {}}'] * 1000
    harness.backend.watch_line_delay_s = 0.05
    provider = harness.provider()

    async def _consume() -> None:
        async for _ in provider.execute(KubeWatchPods(kubeconfig=FAKE_KUBECONFIG, namespace=NAMESPACE, timeout_s=60)):
            pass

    task = asyncio.ensure_future(_consume())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    registry_root = provider._tempfiles.root
    if registry_root.exists():
        assert list(registry_root.iterdir()) == [], "kubectl: kubeconfig temp file leaked on cancellation"


async def test_install_k3s_cancellation_unlinks_known_hosts_and_reraises():
    harness = SshK3sHarness()
    harness.backend.install_delay_s = 0.2  # additive fake hook — see fake_sshd.py
    provider = harness.provider()
    cmd = harness.install_k3s_command()

    async def _consume() -> None:
        async for _ in provider.execute(cmd):
            pass

    task = asyncio.ensure_future(_consume())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    registry_root = provider._tempfiles.root
    if registry_root.exists():
        assert list(registry_root.iterdir()) == [], "ssh-k3s: known_hosts temp file leaked on cancellation"
