"""The instance reconciler compares tart's ``spi-`` VMs with the instance rows (DR-0055).

The real TartProvider over the fake tart transport, the shipped workflows, a real dispatcher, timer
service and executor over tmp SQLite. No Mock/patch. Every tart call asserts that no transaction is open.
"""

from __future__ import annotations

import logging

import pytest

from seedpod.core.events import InstanceDestroyRequested
from seedpod.runtime.instance_reconcile import InstanceReconciler
from seedpod.runtime.timers import TimerService
from tests.conformance.fake_tart import FakeTartTransport
from tests.conformance.harness import Fault
from tests.engine.test_instance_lifecycle import (
    audits,
    fire_due_timers,
    instance,
    settle,
    timer_keys,
)
from tests.engine.test_instance_tart_flow import _birth, _stack


class _NoTxTransport(FakeTartTransport):
    def __init__(self, inner: FakeTartTransport, uow) -> None:
        super().__init__(inner.backend, inner.faults)
        self._uow = uow

    async def run(self, argv, **kwargs):
        assert not self._uow._lock.locked(), "DR-0008: a tart call ran with a transaction open"  # noqa: SLF001
        return await super().run(argv, **kwargs)


async def _world(tmp_path):
    """An active instance ``i1`` whose VM ``spi-box`` is running, and a reconciler over its provider."""
    stack, tart, _ = _stack(tmp_path)
    provider = stack.h.engine._step_services.providers["tart"]  # noqa: SLF001
    provider.transport = _NoTxTransport(provider.transport, stack.uow)
    stack.timers = TimerService(stack.uow, stack.repos, stack.dispatcher, stack.clock, poll_interval=0.01)
    await _birth(stack)
    await settle(stack)
    assert (await instance(stack)).status == "active"
    reconciler = InstanceReconciler(stack.uow, stack.repos, stack.dispatcher, {"tart": provider}, stack.clock)
    return stack, tart, provider, reconciler


@pytest.mark.parametrize("running", [True, False])
async def test_a_leftover_vm_of_a_destroyed_instance_is_destroyed_again(tmp_path, running):
    stack, tart, _, reconciler = await _world(tmp_path)
    await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=stack.clock.now(), actor="api:alice"))
    await settle(stack)
    assert (await instance(stack)).status == "destroyed" and not tart.present_names()
    tart.seed_vm("spi-box", running=running)

    await reconciler.tick()
    assert (await instance(stack)).status == "zombie"
    await fire_due_timers(stack)
    await settle(stack)

    assert (await instance(stack)).status == "destroyed" and not tart.present_names()


async def test_an_active_instance_whose_vm_stopped_fails_and_keeps_its_expiry(tmp_path):
    stack, tart, _, reconciler = await _world(tmp_path)
    tart.vms["spi-box"]["running"] = False

    await reconciler.tick()

    assert (await instance(stack)).status == "failed"
    assert ("instance", "i1", "expiry") in await timer_keys(stack)


async def test_an_active_instance_with_a_running_vm_is_left_alone(tmp_path):
    stack, tart, _, reconciler = await _world(tmp_path)
    before = await audits(stack)

    await reconciler.tick()

    assert await audits(stack) == before and tart.present_names() == {"spi-box"}


@pytest.mark.parametrize(("vm", "warned"), [("spi-ghost", True), ("seedpod-c1", False)])
async def test_a_vm_with_no_row_is_never_touched(tmp_path, caplog, vm, warned):
    stack, tart, _, reconciler = await _world(tmp_path)
    tart.seed_vm(vm)
    calls = len(tart.call_log)

    with caplog.at_level(logging.WARNING):
        await reconciler.tick()

    assert (vm in caplog.text) is warned
    assert tart.present_names() == {"spi-box", vm}
    assert not [c for c in tart.call_log[calls:] if c[1] in ("stop", "delete", "clone", "run")]
    async with stack.uow() as tx:
        assert [r.id for r in stack.repos.instances.list_all(tx)] == ["i1"]


async def test_an_unreachable_tart_changes_nothing(tmp_path):
    stack, tart, provider, reconciler = await _world(tmp_path)
    tart.vms["spi-box"]["running"] = False
    before = await audits(stack)
    provider.transport.faults = frozenset({Fault.UNREACHABLE})

    await reconciler.tick()

    assert (await instance(stack)).status == "active" and await audits(stack) == before


class _DestroyDuringList:
    """The tart provider, with a destroy run that finishes while the list result is in flight."""

    def __init__(self, inner, destroy):
        self.inner, self.destroy = inner, destroy
        self.supported, self.name = inner.supported, inner.name

    async def execute(self, cmd):
        events = [e async for e in self.inner.execute(cmd)]
        if type(cmd).__name__ == "ListBareInstances" and self.destroy is not None:
            destroy, self.destroy = self.destroy, None
            await destroy()
        for e in events:
            yield e


async def test_a_destroy_that_ends_during_the_list_is_not_a_leftover(tmp_path):
    stack, tart, provider, _ = await _world(tmp_path)

    async def destroy():
        await stack.dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=stack.clock.now(), actor="api:alice"))
        await settle(stack)

    racing = _DestroyDuringList(provider, destroy)
    reconciler = InstanceReconciler(stack.uow, stack.repos, stack.dispatcher, {"tart": racing}, stack.clock)
    await reconciler.tick()
    assert (await instance(stack)).status == "destroyed" and not tart.present_names()
    tart.seed_vm("spi-box")  # a real leftover is still found on the next pass
    await reconciler.tick()
    assert (await instance(stack)).status == "zombie"
