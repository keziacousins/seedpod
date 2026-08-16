---
title: Running seedpod against tart on macOS — setup and the Local Network trap
type: guide
status: active
created: 2026-08-09
updated: 2026-08-09
---

# Running seedpod against `tart` on macOS

`tart` is one of v2's two production providers: DigitalOcean for shared infra, `tart` for a
developer running a full stack locally. It provisions ~9x faster than DO (26.9s vs 242.3s in
smokes 5 and 4), which is the inner-loop case it exists for.

This guide is the setup notes backlog #15 asked for. **Read the Local Network section before your
first run** — it is the one thing that will silently break a working setup, and it costs hours if
you meet it without knowing.

## Prerequisites

- The `tart` binary on `PATH`.
- The `local-dev-base-rosetta` base image present in `tart list`. The build recipe lives in
  `~/vm-infra` on the machine that hosts the VMs, not in this repo. `TartProvider.check_ready`
  fails loudly at startup if it is missing (`PermanentError(NOT_FOUND)`), so you find out before a
  provision rather than during one.
- Rosetta, if you run amd64 images. Proven in smoke 5: an amd64 `exampleco-web-2` image runs `1/1`
  inside an Apple-Silicon VM.

## The Local Network trap

**Symptom.** Provisioning dies at the `k3s.await_ssh` gate after its full budget. Every tart VM is
unreachable, and it looks exactly like a VM that never booted. Since DR-0033 the failure message
names the actual error, so you should now see something like:

```
gate timed out after 180.0s; last poll: ssh port not open yet: [Errno 65] No route to host
```

**Errno 65 (`EHOSTUNREACH`) against a `192.168.65.x` address is this problem, not a slow boot.**
Match on the *number*, not the words: `asyncio` renders errno 65 as "No route to host" and does not
use the constant's name anywhere.
A booting VM refuses the connection (errno 61, rendered `[Errno 61] Connect call failed (...)`) or
times out; it does not report the host as unreachable while the vmnet interface is up.

**Cause.** macOS 15's Local Network Privacy denies the process access to the vmnet subnet the tart
VMs live on. The permission is evaluated per *binary*, and a denied binary gets `EHOSTUNREACH` on
every connect with no prompt and no log entry.

**What is actually load-bearing — measured, not inferred (2026-08-09, macOS 15.7.2).** Smoke 5's
original write-up blamed *session parentage* — that a process reparented away from a live login
session (`nohup`, `ssh -f`, `launchd`) is denied. That is **not** the rule. In one detached wrapper
script, so every probe below had the identical parent process and ran within a second of the
others, against four live VMs:

| binary | reparented result |
|---|---|
| `/opt/homebrew/opt/python@3.11/bin/python3.11` (what `.venv/bin/python3.11` resolves to) | `EHOSTUNREACH` ×4 |
| `/usr/bin/python3` | open ×4 |
| `/opt/homebrew/bin/python3.13` | open ×4 |
| `/usr/bin/nc` | open |

Two processes with the same parent got opposite answers, so parentage cannot be the discriminator.
And since python3.13 is *also* Homebrew, it is not a Homebrew-vs-Apple or signed-vs-unsigned story
either. **The grant is per-binary**, and on this host `python@3.11` specifically is in a denied
state.

Parentage still *appears* to matter, which is why the original diagnosis was plausible: run the
same binary **in-session** and it works. In-session the responsible process is the terminal or SSH
session, which already holds a grant, so the binary's own state is never consulted. Detach, and the
process becomes its own responsible process and is judged on its own record.

**Fixes, in preference order.**

1. **Grant Local Network to the exact interpreter binary.** System Settings → Privacy & Security →
   Local Network. Note it is the *resolved* binary that matters — for a venv, follow the symlink:
   `readlink -f .venv/bin/python3.11` (here, `/opt/homebrew/opt/python@3.11/bin/python3.11`).
   Recreating the venv does **not** help; the venv path is not what is judged.
2. **Run seedpod as a child of one live session** — a single
   `ssh <host> '/path/to/run-everything.sh'` that starts the server *and* drives the work. This is
   what made smoke 5 pass. It is a workaround, not a fix: it borrows the session's grant.
3. Use a different interpreter that already holds the grant. Works, but it is luck rather than
   configuration, and it will surprise the next person.

**Checking it yourself.** There is no clean read path — Local Network grants live in the system TCC
store, which needs sudo/Full Disk Access to enumerate, and they are *not* in the per-user `TCC.db`.
So test behaviourally: from a detached process (one that outlives the shell that spawned it),
`connect_ex()` to a running VM's port 22 and look at the errno. In-session tests will pass
regardless and tell you nothing.

## Why this was expensive, and what changed

The run had the answer at every one of ~60 polls — the kernel returned `EHOSTUNREACH` each time —
and threw it away twice over: `ProbeSshPort` collapsed every error into `open=False`, and the engine
gate discarded `NotReady.detail` on timeout. The failure surfaced as `gate timed out after 180.0s`.

DR-0033 fixed both layers, so the message now names the errno. The general lesson is worth keeping:
a probe that "never raises" is right about control flow and still owes the operator the reason.
