"""``InstanceReconciler``: compare the bare VMs a provider holds with the instance rows (DR-0055).

One tick, for each provider that supports ``ListBareInstances``:

1. List its ``spi-`` VMs, with no transaction open. Unreachable (or any provider error): skip the
   provider this pass. A failed list is never read as "no VMs".
2. A VM whose row is ``destroyed`` is a leftover. It gets ``InstanceRunningObserved``, whether it is
   running or stopped, because a stopped VM still costs money. The machine makes the row a zombie
   with an ``expiry`` timer due now, and the normal expiry path destroys it.
3. A VM with no row is logged and left alone. DR-0055 does not say what to do with it.
4. An ``active`` row is probed. A stopped VM gets ``InstanceStoppedObserved`` (``failed``, with the
   expiry timer still armed). An absent VM is logged only: its expiry timer destroys the row, and
   destroying an absent VM succeeds.

Nothing else. Runs and timers are the liveness sweep's job. Clusters and ``seedpod-`` VMs are not
touched. Each read is its own short transaction (DR-0008), each provider call has none open, and
each ``Dispatcher.apply`` has its own.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Mapping

from seedpod.core import events as ev
from seedpod.core.clock import Clock
from seedpod.core.errors import ProviderError
from seedpod.core.records import InstanceStatus
from seedpod.data.repositories import Repositories
from seedpod.data.uow import UnitOfWork
from seedpod.providers.contract import ListBareInstances, ProbeInstance, Provider, Result
from seedpod.providers.tart import INSTANCE_VM_NAME_PREFIX
from seedpod.runtime.dispatcher import Dispatcher

__all__ = ["InstanceReconciler"]

_log = logging.getLogger(__name__)
_ACTOR = "reconciler"


class InstanceReconciler:
    """``InstanceReconciler(uow, repos, dispatcher, providers, clock, interval=60.0)``.
    ``tick()`` is one pass, public so a test drives it with no sleeping."""

    def __init__(
        self, uow: UnitOfWork, repos: Repositories, dispatcher: Dispatcher, providers: Mapping[str, Provider],
        clock: Clock, interval: float = 60.0,
    ) -> None:
        self._uow, self._repos, self._dispatcher = uow, repos, dispatcher
        self._providers, self._clock, self._interval = providers, clock, interval
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Idempotent."""
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Idempotent. Each repair is its own transaction, so a cancel is safe."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _run(self) -> None:
        while True:
            try:
                await self.tick()
            except Exception:
                _log.exception("instance reconcile pass failed, will retry on the next pass")
            await asyncio.sleep(self._interval)

    async def tick(self) -> None:
        for name, provider in self._providers.items():
            if ListBareInstances in provider.supported:
                await self._reconcile(name, provider)

    async def _call(self, provider: Provider, cmd: ListBareInstances | ProbeInstance):
        """The provider's ``Result`` value, or ``None`` when it cannot answer (logged)."""
        try:
            values = [e.value async for e in provider.execute(cmd) if isinstance(e, Result)]
        except ProviderError as exc:
            _log.warning("instance reconcile: %s: %s failed, skipping: %s", provider.name, type(cmd).__name__, exc)
            return None
        return values[-1] if values else None

    async def _observe(self, instance_id: str, event: ev.Event) -> None:
        try:
            await self._dispatcher.apply("instance", instance_id, event)
        except Exception:
            _log.exception("instance reconcile: %s: applying %s failed, will retry", instance_id, type(event).__name__)

    async def _reconcile(self, name: str, provider: Provider) -> None:
        # Rows first. A row read as ``destroyed`` stays so, so a VM listed after the read is a
        # real leftover. Listed first, a destroy run that ends between the two would look like one.
        async with self._uow() as tx:
            rows = [r for r in self._repos.instances.list_all(tx) if r.provider == name]
        listed = await self._call(provider, ListBareInstances())
        if listed is None:
            return
        by_vm = {f"{INSTANCE_VM_NAME_PREFIX}{r.slug}": r for r in rows}  # a slug is never reused
        now = self._clock.now()
        for vm in listed:
            row = by_vm.get(vm.name)
            if row is None:
                _log.warning("instance reconcile: %s holds VM %r with no instance row; leaving it", name, vm.name)
            elif row.status == InstanceStatus.DESTROYED:
                _log.warning("instance reconcile: VM %r of destroyed instance %s is still there", vm.name, row.id)
                await self._observe(row.id, ev.InstanceRunningObserved(at=now, actor=_ACTOR))
        for row in rows:
            if row.status != InstanceStatus.ACTIVE:
                continue
            state = await self._call(provider, ProbeInstance(resource_ids=row.resource_ids))
            if state is not None and state.phase == "stopped":
                await self._observe(row.id, ev.InstanceStoppedObserved(at=now, actor=_ACTOR))
            elif state is not None and state.phase == "absent":
                _log.warning("instance reconcile: VM of active instance %s is gone; its expiry will destroy it", row.id)
