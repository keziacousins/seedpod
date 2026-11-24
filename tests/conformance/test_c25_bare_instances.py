"""tests/conformance/test_c25_bare_instances.py -- the bare-instance commands (DR-0055 decision 3):
CreateBareInstance, DestroyBareInstance and ListBareInstances (tart only) and BootInstance (ssh-k3s).
Fault injection is at the transport seam; no Mock, no patch."""

from __future__ import annotations

import pytest

from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    TransientError,
)
from seedpod.core.instance_template import BOOT_PAYLOAD_PATH, build_boot_payload
from seedpod.providers.contract import (
    RESOURCE_ALLOCATED,
    BootInstance,
    ClusterSnapshot,
    CreateBareInstance,
    DestroyBareInstance,
    DestroyStatus,
    ListBareInstances,
    ListInstances,
    Reconcile,
    Result,
    SSHTarget,
    SubprocessResult,
)
from seedpod.providers.ssh_k3s import SshK3sConfig, SshK3sProvider
from seedpod.providers.tart import TartConfig, TartProvider
from tests.conformance._support import HARNESS_CLASSES, drain
from tests.conformance.fake_sshd import FakeSshdBackend, FakeSshTransport
from tests.conformance.fake_tart import FakeTartBackend, FakeTartTransport
from tests.conformance.harness import Fault

_BASE = "local-dev-base-rosetta"
_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA user@host"


class _Scripted(FakeTartTransport):
    """The fake tart transport, with fixed answers for chosen subcommands (a killed or failing command)."""

    def __init__(self, backend, answers=None):
        super().__init__(backend, frozenset())
        self.answers = answers or {}

    async def run(self, argv, **kw):
        if len(argv) > 1 and argv[1] in self.answers:
            self.backend.attempt_count += 1
            return self.answers[argv[1]]
        return await super().run(argv, **kw)


def _tart(backend, **kw) -> TartProvider:
    return TartProvider(TartConfig(base_image_name=_BASE), _Scripted(backend, **kw))


def _create(slug="box"):
    return CreateBareInstance(instance_id="i-1", slug=slug, image=_BASE, vcpu=2, memory_mb=2048, tags=("seedpod-instance",))


def _destroy(slug="box", **ids):
    return DestroyBareInstance(instance_id="i-1", slug=slug, resource_ids=ids)


async def _value(provider, cmd):
    return next(e.value for e in await drain(provider, cmd) if isinstance(e, Result))


async def test_create_names_the_vm_spi_slug_and_records_it_before_any_tart_call():
    backend = FakeTartBackend()
    stream = _tart(backend).execute(_create())
    first = await stream.__anext__()
    assert first.phase == RESOURCE_ALLOCATED and first.data["resource_ids"] == {"tart_vm_name": "spi-box"}
    assert backend.attempt_count == 0
    rest = [e async for e in stream]
    assert rest[-1].value.resource_ids == {"tart_vm_name": "spi-box"}
    assert backend.present_names() == {"spi-box"}
    assert ("tart", "clone", _BASE, "spi-box") in backend.call_log
    assert ("tart", "set", "spi-box", "--memory", "2048", "--cpu", "2") in backend.call_log  # no disk named: none set


async def test_creating_twice_adopts_the_vm_and_never_duplicates_it():
    backend = FakeTartBackend()
    provider = _tart(backend)
    first, second = await _value(provider, _create()), await _value(provider, _create())
    assert (first.adopted_existing, second.adopted_existing) == (False, True)
    assert backend.present_names() == {"spi-box"}
    assert [c for c in backend.call_log if c[1] == "run"] == [("tart", "run", "--no-graphics", "--rosetta=rosetta", "spi-box")]


@pytest.mark.parametrize("ids", [{"tart_vm_name": "spi-box"}, {}], ids=["recorded-id", "name-only"])
async def test_destroy_finds_the_vm_by_its_recorded_id_or_by_spi_slug(ids):
    """`name-only` is a late VM: a create that finished after the run ended, so no id was ever recorded."""
    backend = FakeTartBackend()
    backend.seed_vm("spi-box")
    backend.seed_vm("seedpod-box")  # a cluster with the same slug is never touched
    assert (await _value(_tart(backend), _destroy(**ids))).status is DestroyStatus.DESTROYED
    assert backend.present_names() == {"seedpod-box"}


async def test_destroy_of_an_absent_vm_succeeds_only_because_the_list_succeeded():
    backend = FakeTartBackend()
    assert (await _value(_tart(backend), _destroy())).status is DestroyStatus.DESTROYED
    assert backend.call_log == [("tart", "list", "--format", "json")]


async def test_destroy_with_tart_unreachable_raises_and_is_never_absent():
    backend = FakeTartBackend()
    backend.seed_vm("spi-box")
    with pytest.raises(InfrastructureUnreachableError):
        await drain(TartProvider(TartConfig(), FakeTartTransport(backend, frozenset({Fault.UNREACHABLE}))), _destroy())
    assert backend.present_names() == {"spi-box"}


@pytest.mark.parametrize("name", ["seedpod-box", "other", "spi-another-instance"])
async def test_destroy_refuses_a_recorded_name_that_is_not_its_own_vm_before_any_tart_call(name):
    backend = FakeTartBackend()
    backend.seed_vm("seedpod-box")
    with pytest.raises(PermanentError) as err:
        await drain(_tart(backend), _destroy(tart_vm_name=name))
    assert err.value.code == ErrorCode.INVALID_INPUT and backend.attempt_count == 0
    assert backend.present_names() == {"seedpod-box"}


@pytest.mark.parametrize(
    ("stage", "answer", "error"),
    [
        ("stop", SubprocessResult(1, b"", b"", timed_out=True), InfrastructureUnreachableError),  # killed
        ("stop", SubprocessResult(-9, b"", b"killed"), PermanentError),  # non-zero
        ("delete", SubprocessResult(1, b"", b"disk busy"), TransientError),
    ],
)
async def test_a_killed_or_failing_stop_or_delete_is_never_destroyed(stage, answer, error):
    backend = FakeTartBackend()
    backend.seed_vm("spi-box")
    with pytest.raises(error):
        await drain(_tart(backend, answers={stage: answer}), _destroy())
    assert backend.present_names() == {"spi-box"}


async def test_cluster_listing_and_reconcile_never_see_an_spi_vm():
    backend = FakeTartBackend()
    backend.seed_vm("spi-box")
    backend.seed_vm("seedpod-c1")
    provider = _tart(backend)
    assert [s.name for s in await _value(provider, ListInstances())] == ["seedpod-c1"]
    assert [s.name for s in await _value(provider, ListBareInstances())] == ["spi-box"]
    snapshot = ClusterSnapshot(cluster_uuid="c1", slug="c1", status="active", resource_ids={})
    assert await _value(provider, Reconcile(clusters=(snapshot,))) == ()  # the cluster's own VM is running


@pytest.mark.parametrize("harness_cls", [c for c in HARNESS_CLASSES if c.name != "tart"], ids=lambda c: c.name)
@pytest.mark.parametrize("cmd", [_create(), _destroy(), ListBareInstances()], ids=lambda c: type(c).__name__)
def test_every_other_provider_refuses_the_bare_commands_with_no_backend_traffic(harness_cls, cmd):
    harness = harness_cls()
    with pytest.raises(PermanentError) as err:
        harness.provider().execute(cmd)
    assert err.value.code == ErrorCode.UNSUPPORTED and harness.backend_attempts() == 0


# --- boot (ssh-k3s) -------------------------------------------------------------------------

_SSH = SSHTarget(host="10.0.0.9", user="admin", private_key_path="/k")
_HOSTS = "10.0.0.9 ssh-ed25519 AAAAC3"


def _boot(sshd, **kw):
    fields = {"ssh": _SSH, "known_hosts": _HOSTS, "payload": build_boot_payload(ssh_public_key=_KEY), "entrypoint": "/usr/local/sbin/boot"}
    return SshK3sProvider(SshK3sConfig(), FakeSshTransport(sshd, frozenset())), BootInstance(**{**fields, **kw})


async def test_boot_is_strict_sends_the_payload_on_stdin_and_never_puts_it_in_the_command():
    sshd = FakeSshdBackend()
    marker = "ssh-ed25519 AAAA $(touch /pwned) `id`"  # a comment is printable text, and still only data
    provider, cmd = _boot(sshd, payload=build_boot_payload(ssh_public_key=marker))
    await drain(provider, cmd)
    argv, stdin = sshd.boot
    assert "StrictHostKeyChecking=yes" in argv and not any("StrictHostKeyChecking=no" in a for a in argv)
    assert stdin == cmd.payload and b"$(touch /pwned)" in stdin
    command = argv[-1]
    assert "$(touch" not in command and "pwned" not in command and "id`" not in command
    assert f"chmod 0600 {BOOT_PAYLOAD_PATH}" in command and command.endswith(f"/usr/local/sbin/boot {BOOT_PAYLOAD_PATH}")


@pytest.mark.parametrize("bad", ["", "boot", "/a b", "/a;rm -rf /", "/a/../b", "/a/$(x)", "/a\nb", "/" + "a" * 300])
async def test_boot_rejects_an_unsafe_entrypoint_before_any_ssh_call(bad):
    sshd = FakeSshdBackend()
    provider, cmd = _boot(sshd, entrypoint=bad)
    with pytest.raises(PermanentError) as err:
        await drain(provider, cmd)
    assert err.value.code == ErrorCode.INVALID_INPUT and sshd.attempt_count == 0


async def test_boot_before_the_host_key_is_pinned_is_refused_and_a_failing_entrypoint_fails():
    sshd = FakeSshdBackend()
    provider, cmd = _boot(sshd, known_hosts="")
    with pytest.raises(PermanentError):
        await drain(provider, cmd)
    assert sshd.attempt_count == 0
    sshd.boot_exit_code = 1
    provider, cmd = _boot(sshd)
    with pytest.raises(PermanentError):
        await drain(provider, cmd)


@pytest.mark.parametrize("bad", ["", "x", f"{_KEY}\nssh-ed25519 BBBB", 'command="x" ' + _KEY, _KEY + "\r"])
def test_the_payload_refuses_anything_but_one_openssh_key_line(bad):
    with pytest.raises(ValueError):
        build_boot_payload(ssh_public_key=bad)
