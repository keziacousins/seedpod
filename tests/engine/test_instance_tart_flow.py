"""A bare VM on tart, provisioned and destroyed by the SHIPPED workflows through the real engine,
dispatcher and executor, over a fake tart and a fake sshd at the transport seam (DR-0055). No Mock.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from seedpod.app.factory import load_instance_templates
from seedpod.core.clock import FrozenClock
from seedpod.core.events import InstanceCreateRequested, InstanceDestroyRequested
from seedpod.core.instance_template import InstanceSize, InstanceTemplate
from seedpod.engine.dispatch_table import WorkflowDispatch
from seedpod.engine.steps.cluster import SshIdentity
from seedpod.engine.steps.infra import InfraAwaitInstance
from seedpod.engine.steps.instance import (
    InstanceBoot,
    InstanceCreate,
    InstanceDestroy,
    InstanceLoadInfra,
    InstanceLoadSpec,
)
from seedpod.engine.steps.k3s import K3sAwaitSsh, K3sTrustHostKeys
from seedpod.providers.contract import Result, SshPortState
from seedpod.providers.ssh_k3s import SshK3sConfig, SshK3sProvider
from seedpod.providers.tart import TartConfig, TartProvider
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.runtime.effect_executor import EffectExecutor
from tests.conformance.fake_sshd import FakeSshdBackend, FakeSshTransport
from tests.conformance.fake_tart import FakeTartBackend, FakeTartTransport
from tests.conformance.harness import Fault
from tests.engine.fakes import build_engine
from tests.engine.test_instance_lifecycle import Stack, _repos, instance, settle
from tests.runtime.conftest import FakeHub, make_instance_row, make_owner_key

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)
CONFIG = Path(__file__).resolve().parents[2] / "config"
KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA requester@host"
TEMPLATE = InstanceTemplate(
    provider="tart", image="local-dev-base-rosetta", ssh_user="admin", entrypoint="/usr/local/sbin/seedpod-boot",
    sizes={"small": InstanceSize(vcpu=2, memory_mb=2048)}, lease_seconds=3600, max_lifetime_seconds=86400,
)


class _Ssh(SshK3sProvider):
    """The real ssh-k3s provider. ``ProbeSshPort`` is a TCP dial, not a subprocess, so it is answered here."""

    async def _probe_ssh_port(self, cmd):
        yield Result(SshPortState(open=True))


def _stack(tmp_path, *faults: Fault):
    clock, repos = FrozenClock(NOW), _repos()
    tart, sshd = FakeTartBackend(ip_on_run="192.168.64.9"), FakeSshdBackend()
    box: list = []
    uow = lambda: box[0].uow()  # noqa: E731 -- the steps need the engine harness's own unit of work
    steps = [
        InstanceLoadSpec(
            uow=uow, instances=repos.instances, templates={"ubuntu": TEMPLATE},
            ssh_identities={"tart": SshIdentity(private_key_path="/tmp/key")},
        ),
        InstanceCreate(), InfraAwaitInstance(), K3sAwaitSsh(), K3sTrustHostKeys(), InstanceBoot(),
        InstanceLoadInfra(uow=uow, instances=repos.instances), InstanceDestroy(),
    ]
    definitions = {n: (CONFIG / "workflows" / f"{n}.yml").read_text() for n in ("provision-instance-tart", "destroy-instance")}
    h = build_engine(tmp_path, definitions, steps, clock, dispatcher_factory=lambda u: Dispatcher(u, repos, clock))
    box.append(h)
    h.engine._step_services.providers.update(  # noqa: SLF001
        {
            "tart": TartProvider(TartConfig(), FakeTartTransport(tart, frozenset(faults))),
            "ssh-k3s": _Ssh(SshK3sConfig(), FakeSshTransport(sshd, frozenset())),
        }
    )
    executor = EffectExecutor(h.uow, repos, FakeHub(), h.engine, WorkflowDispatch(destroy_by_provider={}), clock, poll_interval=10.0)
    return Stack(h, repos, h.dispatcher, executor, None, clock, None, None, None, None, None), tart, sshd


async def _birth(stack: Stack) -> None:
    async with stack.uow() as tx:
        owner = make_owner_key(tx)
    row = make_instance_row(
        "i1", "box", owner, provider="tart", created_at=NOW, updated_at=NOW,
        expires_at=NOW + timedelta(hours=1), hard_expires_at=NOW + timedelta(hours=24), ssh_public_key=KEY,
    )
    await stack.dispatcher.apply("instance", "i1", InstanceCreateRequested(at=NOW, actor="api:alice"), record=row)


async def test_provision_then_destroy_through_the_shipped_workflows(tmp_path):
    stack, tart, sshd = _stack(tmp_path)
    await _birth(stack)
    await settle(stack)

    row = await instance(stack)
    assert (row.status, row.address, dict(row.resource_ids)) == ("active", "192.168.64.9", {"tart_vm_name": "spi-box"})
    assert tart.present_names() == {"spi-box"}
    argv, stdin = sshd.boot
    assert "admin@192.168.64.9" in argv and b"requester@host" in stdin  # booted over the pinned host key

    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=NOW, actor="api:alice"))
    await settle(stack)
    assert (await instance(stack)).status == "destroyed" and tart.present_names() == frozenset()


@pytest.mark.parametrize("failure", ["boot", "run"])
async def test_a_provision_that_fails_after_create_leaves_no_vm(tmp_path, failure):
    """`boot`: the entrypoint exits 1, after the VM is up. `run`: `tart run` fails, after clone and set."""
    stack, tart, sshd = _stack(tmp_path, *([Fault.DIE_MID_CREATE] if failure == "run" else []))
    sshd.boot_exit_code = 1
    await _birth(stack)
    await settle(stack)

    assert (await instance(stack)).status == "failed"
    assert tart.present_names() == frozenset()
    assert any(c[:2] == ("tart", "clone") for c in tart.call_log)  # a VM did exist


# --- templates ------------------------------------------------------------------------------


_TEMPLATE = "provider: tart\nimage: img\nssh_user: admin\nentrypoint: /opt/boot\nsizes: {small: {vcpu: 2, memory_mb: 2048}}\nlease_seconds: 60\nmax_lifetime_seconds: 600\n"


@pytest.mark.parametrize(
    "edit",
    [
        ("/opt/boot", "/opt/boot; id"),
        ("admin", "admin\nport: 22"),
        ("image: img\n", ""),
        ("admin", "-oProxyCommand=x"),
        ("image: img", "image: --evil-flag"),
        ("max_lifetime_seconds: 600", "max_lifetime_seconds: 59"),
        ("memory_mb: 2048}", "memory_mb: 2048, hourly_price: -1}"),
    ],
)
def test_templates_load_from_yaml_and_a_bad_one_is_refused(tmp_path, edit):
    assert load_instance_templates(tmp_path / "absent") == {}
    (tmp_path / "ubuntu.yml").write_text(_TEMPLATE)
    assert load_instance_templates(tmp_path)["ubuntu"].sizes["small"].disk_gb is None
    (tmp_path / "ubuntu.yml").write_text(_TEMPLATE.replace(*edit))
    with pytest.raises(ValueError):
        load_instance_templates(tmp_path)
