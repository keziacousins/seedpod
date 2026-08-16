---
title: DR-0033 — a gate that gives up must say why, and a refused TCP dial must say which error
type: decision
status: active
created: 2026-08-09
updated: 2026-08-09
---

# DR-0033: a gate that gives up must say why

**Status: ACTIVE — authorized by Kezia 2026-08-09**, as the diagnosability half of backlog #15.

## Context

Backlog #15 was opened after smoke 5 lost hours to a `k3s.await_ssh` gate that failed with
`gate timed out after 180.0s` and nothing else. The underlying cause was a host permission problem
(macOS Local Network Privacy denying the vmnet), and the process *knew* the answer at the moment it
failed — every one of the ~60 polls got `EHOSTUNREACH` from the kernel — but nothing carried it out.

Investigating that backlog item surfaced **two independent defects**, at two different layers. They
are recorded together because fixing either alone leaves the reported symptom unchanged.

### Defect 1 — the engine discards `NotReady.detail` (systemic)

`NotReady` has carried a `detail: str` field since Pillar 2 (`engine/step.py:269`), and **five step
sites populate it with genuinely useful text**:

| site | detail |
|---|---|
| `steps/infra.py:319` | `phase={state.phase}` |
| `steps/kube.py:304` | the rollout message, or `rollout not complete` |
| `steps/deploy_apply.py:536` | `"; ".join(not_ready)` — the services still not ready |
| `steps/k3s.py:250` | `ssh port not open yet` |
| `steps/k3s.py:367` | `readiness.detail` from `ProbeK3s` |

The gate loop reads `result` **only** to test `isinstance(result, Ready)` and then drops it
(`engine/engine.py:949-965`). On timeout it reports `f"gate timed out after {gate_timeout}s"`
(`:970`). So the detail is computed, persisted nowhere, and thrown away on the one path where it is
most wanted.

This is not a tart or SSH problem. A `deploy.await_wave` timeout — the gate most likely to fire on a
real deployment — already computes the exact list of services that never became ready and then
reports a bare timeout instead. **Every gate in v2 is undiagnosable on timeout**, and always has
been.

### Defect 2 — `SshPortState` cannot carry the error (local to the k3s plane)

`ProbeSshPort` is specified as a raw TCP `connect_ex` that "never raises": any connect failure
collapses to `SshPortState(open=False)` (`providers/ssh_k3s.py:241-247`, seam-c §5.1's decision
row). That collapse is **correct** — "not booted yet" is the overwhelmingly common case and must not
fail a run — but `SshPortState` carries only `open: bool`
(`providers/contract.py:272`, pinned by `docs/design/seam-c-provider.md:312`), so the errno has
nowhere to go. `k3s.await_ssh` can therefore only ever say the constant `"ssh port not open yet"`.

Fixing Defect 1 alone would surface that constant — true, and still useless: a connection refused by
a booting VM and a connection denied by the OS produce identical text.

## Decision

**1. A gate that times out reports the last `NotReady.detail` it saw.** The gate loop remembers the
most recent non-empty detail and appends it to the timeout message:
`gate timed out after 180.0s; last poll: <detail>`. When no detail was ever produced the message is
unchanged, so gates whose steps return a bare `NotReady()` read exactly as they do today.

**2. `SshPortState` gains `detail: str = ""`, carrying the failed dial's error.** `_probe_ssh_port`
formats the caught `OSError`/`TimeoutError` into it. The real strings, measured rather than guessed
(2026-08-09, macOS 15.7.2): `[Errno 65] No route to host` for the Local Network denial,
`[Errno 61] Connect call failed ('127.0.0.1', 1)` for a refused dial, and
`connect timed out after 3.0s` for `asyncio.wait_for`'s message-less `TimeoutError`.
`k3s.await_ssh` appends it to its `NotReady` detail.

Note that `asyncio` renders these inconsistently — it wraps the refusal in "Connect call failed" but
passes `strerror` straight through for errno 65, and it never emits the constant names
`ECONNREFUSED`/`EHOSTUNREACH` at all. **The errno number is the stable, identifying part**, which is
what the conformance test asserts on.

**`detail` is diagnostic-only and MUST NEVER be read for control flow.** `open: bool` remains the
single decision input — the property that makes `ProbeSshPort` unable to classify-fail, which is
deliberate and unchanged. A test pins this rather than leaving it to convention.

## Why this shape

- **`str`, not `errno: int`.** The probe's failure modes are not all errno-bearing: `TimeoutError`
  from `asyncio.wait_for` has no errno at all, and DNS failures surface as `socket.gaierror` with an
  errno from a different namespace. One preformatted string covers all three; an `int | None` would
  need a parallel field for the timeout case and would push formatting into every consumer.
- **A default of `""`, so nothing else changes.** Every existing construction site
  (`SshPortState(open=True)`, the conformance fakes, `tests/engine/steps/test_k3s_steps.py`'s
  `_FixedProvider`) keeps compiling and keeps meaning what it meant.
- **The engine change carries no new field anywhere.** It reads a field the contract has had since
  Pillar 2. That is why point 1 needs no seam-b amendment — the gate is starting to honor
  `NotReady.detail`'s existing documented purpose, not acquiring a new obligation.

## Consequences

- `docs/design/seam-c-provider.md:312`'s `# Result: SshPortState(open: bool)` is amended in place to
  name the new field. Seam specs are normative "what is" and edited in place (DR-0001).
- **Backlog #15's second task is closed by this DR; its first task (documentation) is not**, and its
  *diagnosis* was wrong — see the same-day rewrite of #15 and `docs/guides/tart-local-dev.md`. The
  denial is **per-binary**, not a function of session parentage as smoke 5's write-up inferred.
- Failure messages get longer. That is the point, but it does mean `workflow_steps.error` rows now
  carry step-supplied text; nothing truncates it today and nothing needs to — the five detail sites
  are all short and bounded, and `deploy_apply.py:536`'s join is bounded by the wave's service count.
- **No provider other than `ssh-k3s` is affected.** `SshPortState` is produced by exactly one
  command in one provider; the conformance suite's other backends never construct it.

## Alternatives rejected

- **Make `ProbeSshPort` raise on a permission denial** (classify `EHOSTUNREACH` as Permanent).
  Rejected: it re-litigates a settled seam-c decision row, and it is wrong on the merits —
  `EHOSTUNREACH` is also what a genuinely-not-yet-routable VM returns during boot, so this would
  convert the common case into a hard failure to improve the rare one.
- **A vmnet pre-flight in tart's `check_ready`** (the backlog's own suggestion). Rejected as the
  primary fix: `check_ready`'s IO goes through the injected transport, and conformance forbids
  `Mock`/`patch` with fault injection living at the transport seam, so a raw socket there is
  untestable by the suite's own rules. It is also strictly narrower than fixing the gate — it would
  help tart and no other provider, and would not have helped a `deploy.await_wave` timeout at all.
- **Log the detail instead of putting it in the failure message.** Rejected: the failure message is
  what `_failure_message()` persists to `workflow_steps.error` and what the SPA and CLI surface. A
  log line requires already knowing to go looking, which is precisely the position smoke 5 was in.
