"""tests/conformance/test_c25_bare_instances.py -- the bare-instance commands (DR-0055 decision 3):
CreateBareInstance, DestroyBareInstance and ListBareInstances (tart and digitalocean) and BootInstance (ssh-k3s).
Fault injection is at the transport seam; no Mock, no patch."""

from __future__ import annotations

import httpx
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
    ProbeInstance,
    Reconcile,
    Result,
    SSHTarget,
    SubprocessResult,
)
from seedpod.providers.digitalocean import DigitalOceanConfig, DigitalOceanProvider
from seedpod.providers.ssh_k3s import SshK3sConfig, SshK3sProvider
from seedpod.providers.tart import TartConfig, TartProvider
from tests.conformance._support import HARNESS_CLASSES, drain
from tests.conformance.fake_digitalocean import FakeDigitalOceanBackend, FakeDigitalOceanTransport
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


@pytest.mark.parametrize("harness_cls", [c for c in HARNESS_CLASSES if c.name not in ("tart", "digitalocean")], ids=lambda c: c.name)
@pytest.mark.parametrize("cmd", [_create(), _destroy(), ListBareInstances()], ids=lambda c: type(c).__name__)
def test_every_other_provider_refuses_the_bare_commands_with_no_backend_traffic(harness_cls, cmd):
    harness = harness_cls()
    with pytest.raises(PermanentError) as err:
        harness.provider().execute(cmd)
    assert err.value.code == ErrorCode.UNSUPPORTED and harness.backend_attempts() == 0


# --- digitalocean (DR-0056) -----------------------------------------------------------------

_SSH_SOURCES = ("198.51.100.0/24", "203.0.113.0/24")
_TAGS = ["seedpod-instance", "instance-uuid:i-1"]


class _Wire(FakeDigitalOceanTransport):
    """The fake DigitalOcean transport, recording `(method, path)` and failing chosen routes with a 500."""

    def __init__(self, backend, fail=()):
        super().__init__(backend, frozenset())
        self.calls, self.fail = [], fail

    async def handle_async_request(self, request):
        key = (request.method, request.url.path.removeprefix("/v2"))
        self.calls.append(key)
        if key in self.fail:
            return httpx.Response(500, json={"id": "server_error", "message": "boom"})
        return await super().handle_async_request(request)


def _do(backend, *faults, **kw):
    wire = _Wire(backend, **kw)
    wire.faults = frozenset(faults)
    config = DigitalOceanConfig(api_token="t", ssh_allowed_sources=_SSH_SOURCES, default_region="ams3")  # pragma: allowlist secret
    return DigitalOceanProvider(config, httpx.AsyncClient(transport=wire)), wire


def _do_create(**kw):
    fields = {"instance_id": "i-1", "slug": "box", "image": "ubuntu-22-04-x64", "vcpu": 2, "memory_mb": 4096, "provider_size": "s-2vcpu-4gb"}
    return CreateBareInstance(**{**fields, **kw})


def _rules(addresses):
    return [{"protocol": "tcp", "ports": "22", "sources": {"addresses": list(addresses)}}]


async def test_do_create_ensures_the_firewall_first_then_makes_one_droplet_with_exactly_the_two_instance_tags():
    backend = FakeDigitalOceanBackend()
    provider, wire = _do(backend)
    stream = provider.execute(_do_create(region="nyc1"))
    events = [e async for e in stream]
    (droplet,) = backend.droplets.values()
    (firewall,) = backend.firewalls.values()
    assert firewall["name"] == "seedpod-instances" and firewall["tags"] == ["seedpod-instance"]
    assert firewall["inbound_rules"] == _rules(_SSH_SOURCES)
    assert {(r["protocol"], tuple(r["destinations"]["addresses"])) for r in firewall["outbound_rules"]} == {
        (p, ("0.0.0.0/0", "::/0")) for p in ("tcp", "udp", "icmp")
    }
    assert wire.calls.index(("POST", "/firewalls")) < wire.calls.index(("POST", "/droplets"))
    assert (droplet["name"], droplet["tags"], droplet["size_slug"], droplet["region"]["slug"], droplet["image"]["slug"]) == (
        "spi-box", _TAGS, "s-2vcpu-4gb", "nyc1", "ubuntu-22-04-x64",
    )
    assert droplet["vpc_uuid"] is None and backend.vpcs == {} and ("POST", "/vpcs") not in wire.calls
    progress, result = events
    assert progress.phase == RESOURCE_ALLOCATED and progress.data["resource_ids"] == {"droplet_id": str(droplet["id"])}
    assert result.value.resource_ids == progress.data["resource_ids"] and not result.value.adopted_existing


async def test_do_create_without_a_region_uses_the_provider_default():
    backend = FakeDigitalOceanBackend()
    await drain(_do(backend)[0], _do_create())
    assert next(iter(backend.droplets.values()))["region"]["slug"] == "ams3"


@pytest.mark.parametrize(
    ("seeded", "writes"),
    [
        (None, [("POST", "/firewalls")]),
        (_rules(["0.0.0.0/0"]), [("PUT", "/firewalls/30001")]),
        (_rules(_SSH_SOURCES), []),
    ],
    ids=["missing", "drifted", "current"],
)
async def test_do_create_creates_a_missing_firewall_replaces_a_drifted_one_and_leaves_a_current_one(seeded, writes):
    backend = FakeDigitalOceanBackend()
    if seeded is not None:
        backend.create_firewall({"name": "seedpod-instances", "inbound_rules": seeded, "tags": ["seedpod-instance"], "outbound_rules": []})
        backend.firewalls["30001"]["outbound_rules"] = _fw_outbound()
    provider, wire = _do(backend)
    await drain(provider, _do_create())
    assert [c for c in wire.calls if c[0] in ("POST", "PUT") and "firewalls" in c[1]] == writes
    (firewall,) = backend.firewalls.values()
    assert firewall["inbound_rules"] == _rules(_SSH_SOURCES) and len(firewall["outbound_rules"]) == 3


def _fw_outbound():
    everywhere = {"addresses": ["0.0.0.0/0", "::/0"]}
    return [{"protocol": p, "ports": "all", "destinations": everywhere} for p in ("tcp", "udp")] + [
        {"protocol": "icmp", "destinations": everywhere}
    ]


@pytest.mark.parametrize("route", [("GET", "/firewalls"), ("POST", "/firewalls")])
async def test_do_create_fails_with_no_droplet_when_the_firewall_cannot_be_made(route):
    backend = FakeDigitalOceanBackend()
    provider, wire = _do(backend, fail={route})
    with pytest.raises(TransientError):
        await drain(provider, _do_create())
    assert backend.droplets == {} and ("POST", "/droplets") not in wire.calls


async def test_do_create_without_a_provider_size_is_refused_before_any_call():
    backend = FakeDigitalOceanBackend()
    with pytest.raises(PermanentError) as err:
        await drain(_do(backend)[0], CreateBareInstance(instance_id="i-1", slug="box", image="x", vcpu=1, memory_mb=1))
    assert err.value.code == ErrorCode.INVALID_INPUT and backend.call_count == 0


async def test_do_creating_twice_adopts_the_droplet_by_its_tag_and_never_posts_twice():
    backend = FakeDigitalOceanBackend()
    provider, wire = _do(backend)
    first, second = await _value(provider, _do_create()), await _value(provider, _do_create())
    assert (first.adopted_existing, second.adopted_existing) == (False, True) and first.resource_ids == second.resource_ids
    assert len(backend.droplets) == 1 and wire.calls.count(("POST", "/droplets")) == 1


@pytest.mark.parametrize("recorded", [True, False], ids=["recorded-id", "by-tag"])
async def test_do_destroy_finds_the_droplet_by_its_recorded_id_or_by_its_tag(recorded):
    backend = FakeDigitalOceanBackend()
    mine = backend.seed_droplet(tags=_TAGS)
    other = backend.seed_droplet(tags=["seedpod-instance", "instance-uuid:i-2"])
    cluster = backend.seed_droplet(tags=["seedpod-managed", "cluster-uuid:c1", "cluster-box"])
    assert (await _value(_do(backend)[0], _destroy(**({"droplet_id": mine} if recorded else {})))).status is DestroyStatus.DESTROYED
    assert set(backend.droplets) == {other, cluster}


@pytest.mark.parametrize("recorded", [True, False], ids=["recorded-id", "by-tag"])
async def test_do_destroy_of_an_absent_droplet_succeeds_only_because_the_read_succeeded(recorded):
    backend = FakeDigitalOceanBackend()
    provider, wire = _do(backend)
    assert (await _value(provider, _destroy(**({"droplet_id": "404"} if recorded else {})))).status is DestroyStatus.DESTROYED
    assert [m for m, _ in wire.calls] == ["GET"]


async def test_do_destroy_with_digitalocean_unreachable_raises_and_is_never_absent():
    backend = FakeDigitalOceanBackend()
    mine = backend.seed_droplet(tags=_TAGS)
    for ids in ({"droplet_id": mine}, {}):
        with pytest.raises(InfrastructureUnreachableError):
            await drain(_do(backend, Fault.UNREACHABLE)[0], _destroy(**ids))
    with pytest.raises(TransientError):  # a failed DELETE is not DESTROYED either
        await drain(_do(backend, fail={("DELETE", f"/droplets/{mine}")})[0], _destroy())
    assert set(backend.droplets) == {mine}


@pytest.mark.parametrize("tags", [["seedpod-managed", "cluster-uuid:c1"], ["seedpod-instance", "instance-uuid:i-2"], []])
async def test_do_destroy_refuses_a_recorded_droplet_without_this_instances_tag(tags):
    backend = FakeDigitalOceanBackend()
    other = backend.seed_droplet(tags=tags)
    provider, wire = _do(backend)
    with pytest.raises(PermanentError) as err:
        await drain(provider, _destroy(droplet_id=other))
    assert err.value.code == ErrorCode.INVALID_INPUT and ("DELETE", f"/droplets/{other}") not in wire.calls
    assert set(backend.droplets) == {other}


async def test_do_listing_returns_only_instance_droplets_and_cluster_commands_never_see_them():
    backend = FakeDigitalOceanBackend()
    mine = backend.seed_droplet(tags=_TAGS)
    cluster = backend.seed_droplet(tags=["seedpod-managed", "cluster-uuid:c1", "cluster-c1"])
    provider, _ = _do(backend)
    listed = await _value(provider, ListBareInstances())
    assert [(s.name, s.resource_ids["droplet_id"]) for s in listed] == [("seed", mine)]
    assert {s.resource_ids["droplet_id"] for s in await _value(provider, ListInstances())} == {cluster}
    snapshot = ClusterSnapshot(cluster_uuid="c1", slug="c1", status="active", resource_ids={})
    assert await _value(provider, Reconcile(clusters=(snapshot,))) == ()  # no intent for either instance droplet


@pytest.mark.parametrize(("status", "phase"), [("active", "running"), ("off", "stopped"), ("gone", "absent")])
async def test_do_probe_reads_an_instance_droplet(status, phase):
    backend = FakeDigitalOceanBackend()
    droplet_id = backend.seed_droplet(tags=_TAGS, status="off" if status == "off" else "active")
    if status == "gone":
        backend.destroy_droplet(droplet_id)
    state = await _value(_do(backend)[0], ProbeInstance(resource_ids={"droplet_id": droplet_id}))
    assert state.phase == phase


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


async def test_do_first_create_on_an_account_makes_the_tag_the_firewall_names_and_a_snapshot_id_is_a_number():
    backend = FakeDigitalOceanBackend()  # no `seedpod-instance` tag yet: the fake refuses a firewall that names it
    await drain(_do(backend)[0], _do_create(image="123456789"))
    assert "seedpod-instance" in backend.tags and len(backend.firewalls) == 1
    assert next(iter(backend.droplets.values()))["image"]["slug"] == 123456789
