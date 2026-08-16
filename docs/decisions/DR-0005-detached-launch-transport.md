---
title: DR-0005 — Detached-launch subprocess transport (tart run)
type: decision
status: active
created: 2026-07-15
updated: 2026-07-15
---

# DR-0005: Detached-launch subprocess transport (`tart run`)

**Status: ACTIVE — ratified by Kezia, 2026-07-15. The Round-4 runtime-spine build of the
concrete `SubprocessRunner`s and `SubprocessManager` may proceed against this decision.**

## Problem

`tart run --no-graphics <name>` is the VM's hypervisor process: it blocks in the foreground for
the VM's entire lifetime and must **outlive the seedpod process** (v1 spawned it with
`start_new_session=True`, all stdio → DEVNULL, and never awaited it —
`reference-code/seedpod/seedpod/providers/_tart_cli.py:220-253`; it was never registered with
v1's `SubprocessManager`, so shutdown never touched it).

Pillar 3 deliberately pushed this out of the provider: `tart.py` issues one ordinary
`self.transport.run(...)` call and pins that "it is the concrete `SubprocessRunner` backing this
Provider's job to recognize this specific invocation shape and realize v1's detached-launch
semantics" (`seedpod/providers/tart.py` module docstring). But no spec says **how** the concrete
runner recognizes it, **where** that runner lives (the coherence-review glossary names no
concrete `SubprocessRunner` at all — only the protocol in `providers/contract.py` and
`SubprocessManager` in `runtime/subprocess_manager.py`), or how a detached child interacts with
the two lifecycle laws the spine must implement:

- **H16 / Seam B**: `ctx.run_subprocess` children get process-group SIGTERM→SIGKILL on
  cancellation.
- **Conflict 15**: `App.stop()` calls `subprocesses.shutdown()` — every *tracked* child is
  terminated at exit.

A detached VM process fits neither the bounded request/response shape nor the
cancellation-managed stream shape. Building the spine without deciding this means a build agent
invents the seam silently — the stop-signal condition in CLAUDE.md.

## Proposal

Two concrete transports, both living in `seedpod/runtime/subprocess_manager.py` beside
`SubprocessManager` (one module owns all spawn code; no package-layout change):

1. **`TrackedSubprocessRunner`** — the default transport behind every provider. Bounded
   run-to-completion per the `SubprocessRunner` protocol: registers the child with
   `SubprocessManager` for its duration, process-group terminate→kill on cancellation (H16),
   reports timeout / binary-missing via `SubprocessResult` flags, never raises on non-zero exit.

2. **`DetachedLaunchRunner`** — a wrapper constructed with `inner: SubprocessRunner` and
   `launch_prefixes: tuple[tuple[str, ...], ...]`. A call whose argv matches a prefix
   (`basename(argv[0])` + following tokens, so `("tart", "run")` matches regardless of binary
   path) is spawned with v1's detached semantics **verbatim**: stdio → DEVNULL ×3,
   `start_new_session=True`, **never awaited, never registered with `SubprocessManager`**, and
   the call returns `SubprocessResult(returncode=0, stdout=b"", stderr=b"")` immediately after a
   successful spawn. `FileNotFoundError` at spawn → `binary_missing=True` result (the shared
   classifier then maps it to `InfrastructureUnreachableError`, decision-table row 1 — same as
   every other tart command). Every non-matching argv delegates to `inner` untouched.

**Wiring (composition root, factory step 5):** only the tart provider's transport is wrapped —
`DetachedLaunchRunner(tracked, launch_prefixes=(("tart", "run"),))`. Every other provider gets
`TrackedSubprocessRunner` directly. The provider neither knows nor cares (its committed
docstring stance is preserved; the Seam C protocol is untouched).

**Blast radius (binding — unrelated tart processes are unreachable by construction):** the
prefix match applies only to argv this transport is *about to spawn* for the tart provider —
it is a spawn-mode selector, never a process-table scan. Nothing in this design signals,
enumerates, or matches *existing* processes by name: `SubprocessManager.shutdown()` and
cluster-scoped termination operate solely on the registry of handles we spawned, and the
wrapper is wired per-transport, so no other provider's argv can ever reach it. Operator-owned
`tart run` sessions and VMs left by a prior seedpod incarnation are therefore invisible to
shutdown, cancellation, and this runner alike. VM-*identity* protection is a separate,
already-committed layer (the `seedpod-` name prefix filter in `_list_instances`/`_reconcile`,
Pillar 3; multiple seedpod instances excluded by the salvaged PID-file singleton in
`start.py`). A build must not add any kill-by-name or process-scan "cleanup" path — that would
be the first mechanism able to touch a foreign VM.

**Lifecycle law (the actual decision):** detached children are *invisible* to
`SubprocessManager`. `App.stop()` never touches them; a seedpod restart neither adopts nor
re-tracks them; cancellation cannot reach them (there is nothing to interrupt — the launch call
has already returned). The VM's lifetime ends only via `DestroyInstance` (`tart stop` /
`tart delete` in the destroy workflow), and its liveness is *observed*, never managed — the
provision workflow's `tart.await_vm` gate (repeated `ProbeInstance`) catches a VM that dies
right after launch, and reconciliation catches everything later. Exactly v1's model.

**Reaping note (accepted cost, v1 parity):** the detached child stays parented to seedpod until
one of them exits — one process-table entry per live VM. asyncio's child watcher reaps it if the
VM stops while seedpod runs; if seedpod exits first, init inherits it. No double-fork — v1
didn't, and we don't invent daemonization.

**Tests (Round 4, transport level — no `Mock`/`patch`):** `tests/conformance/fake_tart.py`
already encodes the observable provider-side contract (launch returns immediately, VM `running`
with no IP). The spine adds real-process tests on `DetachedLaunchRunner` using a short script:
returns immediately while the child still runs; child is in a new session (its pgid ≠ ours);
child survives `SubprocessManager.shutdown()`; non-matching argv is delegated to `inner`;
missing binary yields `binary_missing=True`.

## Consequences

- The coherence-review type glossary gains two rows (`TrackedSubprocessRunner`,
  `DetachedLaunchRunner` — owner `runtime/subprocess_manager.py`) and the factory step-5 comment
  gains the tart wrapping, once ratified.
- The Seam C `SubprocessRunner` protocol, the `Provider` contract, and all committed Pillar-3
  code are unchanged.
- Any future provider needing a detached launch declares another prefix at the composition
  root — data, not a new mechanism.

## Alternatives considered

- **`detach: bool` parameter on `SubprocessRunner.run()`** (rejected: threads a
  hypervisor-specific concern through the frozen, provider-neutral Seam C protocol; contradicts
  the already-committed `tart.py` docstring and `fake_tart.py` conformance behavior; re-opens
  C-suite call signatures for one caller).
- **A second "detached" registry on `SubprocessManager`** (rejected: nothing would ever read
  it — shutdown must skip it and a restart loses it anyway; a registry with no consumer is
  drift bait masquerading as bookkeeping).
- **Double-fork / daemonize the VM process** (rejected: invention beyond v1 with
  macOS-Virtualization-framework risk; v1's spawn shape is field-proven and the zombie-window
  cost is one process-table entry per VM).
- **Argv recognition inside `tart.py` itself** (rejected: providers must not spawn — the whole
  point of the transport seam is that fault injection and process mechanics live behind it).
