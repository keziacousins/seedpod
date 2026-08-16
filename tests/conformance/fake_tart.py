"""tests/conformance/fake_tart.py — a typed FAKE TRANSPORT simulating enough of the local
``tart`` CLI (VM lifecycle: clone/set/run/ip/stop/delete/list) for
``seedpod.providers.tart.TartProvider`` conformance testing (Seam C §5.6).

``FakeTartBackend`` is the in-memory "Tart daemon": a plain mutable store of VMs (name ->
source/running/ip) plus a set of available base images. ``FakeTartTransport`` implements the
``SubprocessRunner`` protocol (``seedpod.providers.contract.SubprocessRunner``) — installed
directly as the provider's ``transport`` — so fault injection happens at the actual transport
seam the provider talks to, never ``Mock``/``patch`` (CLAUDE.md).

Routing mirrors how ``tart.py`` actually invokes the binary: ``argv[0] == "tart"`` dispatches on
``argv[1]`` (``list`` / ``clone`` / ``set`` / ``run`` / ``ip`` / ``stop`` / ``delete``), matching
the exact stderr phrasing ``_tart_cli``/``tart.py`` classify on (salvaged from
``reference-code/seedpod/tests/unit/test_tart_cli.py``'s own fixture strings, e.g. "already
exists", "no such virtual machine", "VM doesn't have IP assigned yet", "VM is not running").
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field

from seedpod.providers.contract import SubprocessResult
from tests.conformance.harness import Fault

__all__ = ["FakeTartBackend", "FakeTartTransport"]


@dataclass
class FakeTartBackend:
    """The in-memory Tart daemon. Every "VM" is ``{"source": str, "running": bool, "ip": str |
    None}``; ``images`` is the set of names ``tart list`` would report with ``Source: "local"``
    that ``tart clone`` may use as a source (real base images and any VM created via
    ``seed_vm``/``_clone`` both count as valid clone sources, mirroring the real CLI where any
    existing local VM can itself be a clone source)."""

    images: set[str] = field(default_factory=lambda: {"local-dev-base-rosetta"})
    vms: dict[str, dict[str, object]] = field(default_factory=dict)
    call_log: list[tuple[str, ...]] = field(default_factory=list)
    attempt_count: int = 0
    delete_fails_for: set[str] = field(default_factory=set)  # row 8: delete-after-stop failure

    def seed_vm(self, name: str, *, running: bool = True, ip: str | None = "192.168.64.10") -> None:
        """Test setup helper (not part of the tart CLI surface): directly inserts a VM, bypassing
        a real ``tart clone``/``run`` round-trip, for harness pre-seeding."""
        self.vms[name] = {"source": "local", "running": running, "ip": ip}

    def force_delete_failure(self, name: str) -> None:
        """Test setup helper: the next ``tart delete <name>`` (after a successful stop) fails
        with a non-not-found error — row 8's ``Transient(RESOURCE_BUSY)`` ⇒ ``DESTROYING``
        vocabulary path."""
        self.delete_fails_for.add(name)

    def present_names(self) -> frozenset[str]:
        return frozenset(self.vms.keys())


class FakeTartTransport:
    """Implements ``seedpod.providers.contract.SubprocessRunner`` against a ``FakeTartBackend``.
    """

    def __init__(self, backend: FakeTartBackend, faults: frozenset[Fault]) -> None:
        self.backend = backend
        self.faults = faults
        self._transient_once_consumed = False

    async def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        cluster_id: str | None = None,
    ) -> SubprocessResult:
        self.backend.attempt_count += 1
        self.backend.call_log.append(tuple(argv))

        if Fault.UNREACHABLE in self.faults:
            # Row 1: a hung/unresponsive tart daemon — v1's TartDaemonUnreachable only ever
            # comes from a subprocess timeout or a missing binary, never a plain non-zero exit.
            return SubprocessResult(returncode=1, stdout=b"", stderr=b"", timed_out=True)

        if Fault.TRANSIENT_ONCE in self.faults and not self._transient_once_consumed:
            self._transient_once_consumed = True
            return SubprocessResult(returncode=1, stdout=b"", stderr=b"", timed_out=True)

        if argv[0] != "tart":
            return SubprocessResult(returncode=127, stdout=b"", stderr=f"command not found: {argv[0]}".encode())

        return self._handle_tart(list(argv[1:]))

    def _handle_tart(self, args: list[str]) -> SubprocessResult:
        sub = args[0] if args else ""

        if sub == "list":
            entries = [
                {"Name": image, "Source": "local", "State": "stopped", "Running": False, "Disk": 20, "Size": 3}
                for image in sorted(self.backend.images)
                # MISSING_SOURCE (row 2/4's closest literal match): the configured base image is
                # simply absent from `tart list` — both check_ready's scan and clone's "source
                # not found" symptom fall out of this one omission, no separate branch needed.
                if Fault.MISSING_SOURCE not in self.faults
            ]
            for name, vm in self.backend.vms.items():
                entries.append(
                    {
                        "Name": name,
                        "Source": vm["source"],
                        "State": "running" if vm["running"] else "stopped",
                        "Running": bool(vm["running"]),
                        "Disk": 20,
                        "Size": 10,
                    }
                )
            return SubprocessResult(returncode=0, stdout=json.dumps(entries).encode(), stderr=b"")

        if sub == "clone":
            source, name = args[1], args[2]
            if name in self.backend.vms:
                return SubprocessResult(returncode=1, stdout=b"", stderr=f"VM '{name}' already exists".encode())
            source_known = source in self.backend.images or source in self.backend.vms
            if Fault.MISSING_SOURCE in self.faults or not source_known:
                return SubprocessResult(returncode=1, stdout=b"", stderr=f"source image {source!r} does not exist".encode())
            self.backend.vms[name] = {"source": "local", "running": False, "ip": None}
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"")

        if sub == "set":
            name = args[1]
            if name not in self.backend.vms:
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"no such virtual machine")
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"")

        if sub == "run":
            name = args[-1]
            if name not in self.backend.vms:
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"no such virtual machine")
            if Fault.DIE_MID_CREATE in self.faults:
                # Simulates a launch failure AFTER `clone`/`set` already left a resource in the
                # backend (the C1 window this provider's undo_for(CreateInstance) must close) —
                # a plain script failure, not a connectivity symptom.
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"tart run failed: virtualization framework error")
            # Simulates a successful detached launch: the VM starts booting (running=True) but
            # has no IP yet — mirrors what a correctly-detached spawn would report without
            # waiting for the child to exit (see tart.py's `_run_detached` docstring).
            self.backend.vms[name]["running"] = True
            self.backend.vms[name]["ip"] = None
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"")

        if sub == "ip":
            name = args[1]
            vm = self.backend.vms.get(name)
            if vm is None:
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"VM not found")
            ip = vm.get("ip")
            if ip:
                return SubprocessResult(returncode=0, stdout=f"{ip}\n".encode(), stderr=b"")
            return SubprocessResult(returncode=1, stdout=b"", stderr=b"VM doesn't have IP assigned yet")

        if sub == "stop":
            name = args[1]
            vm = self.backend.vms.get(name)
            if vm is None:
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"VM not found")
            if not vm["running"]:
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"VM is not running")
            vm["running"] = False
            vm["ip"] = None
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"")

        if sub == "delete":
            name = args[1]
            if name not in self.backend.vms:
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"VM not found")
            if name in self.backend.delete_fails_for:
                # Row 8: a genuine failure AFTER a successful stop (disk busy, etc.) — driven
                # directly via `FakeTartBackend.force_delete_failure`, not a Fault member (tart
                # has no rate-limit/auth concept to repurpose for this; see
                # tests/conformance/test_tart_smoke.py's destroy-vocabulary coverage).
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"disk busy, try again")
            del self.backend.vms[name]
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"")

        return SubprocessResult(returncode=127, stdout=b"", stderr=f"fake tart: no route for {args!r}".encode())

    def stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cluster_id: str | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]:
        """tart never streams (only ``KubeWatchPods`` does, on the ``kubectl`` provider) —
        present only to satisfy the ``SubprocessRunner`` protocol shape."""
        raise NotImplementedError("tart never calls SubprocessRunner.stream()")
