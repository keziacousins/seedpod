---
title: DR-0048 — a kubectl that cannot connect is unreachable, in every sentence it uses to say so
type: decision
status: proposal
created: 2026-10-06
updated: 2026-10-06
---

# DR-0048: a kubectl that cannot connect is unreachable, in every sentence it uses to say so

**Status: PROPOSAL — not yet ratified.** Found on 2026-10-06: seedpod marked a healthy cluster
`failed`. The node was `Ready` for the whole period and no pod had restarted in 47 hours.

## Context

The cluster was a dev stack on tart. Times are CEST.

1. **03:56. Seedpod lost the VM.** The SSH login that had started the server ended, and macOS
   stopped the process from reaching the VM subnet (`docs/guides/tart-local-dev.md`). The health
   monitor did the correct thing for 18 ticks: `unreachable, skipping this tick`.
2. **04:14:36. kubectl's discovery cache expired.** The cache has a 6 hour life and was last
   written at 22:14:36. From this point kubectl runs API discovery before each command.
3. **04:14:58. One tick failed the cluster.** Discovery could not connect, so stderr gained the line
   `couldn't get current server API group list: … connect: no route to host`. That phrase was in
   `_INVALID_INPUT_STDERR_PHRASES`, and that list is read before the connectivity rules. The probe
   raised `PermanentError(INVALID_INPUT)`. The health monitor fails a cluster at once on a
   permanent error: `kubectl connectivity lost (permanent): kubectl.get_cluster_info: invalid input`.

The line is kubectl's wrapper for a failed discovery request. It does not say why the request
failed. The reason comes after the colon. The line is also present or absent with the age of a
cache file, so the same outage had two classifications.

The reproduction found three more sentences with the same result. Each is what kubectl v1.33.9
printed for a server it could not reach, and no phrase in `TRANSIENT_STDERR_PHRASES` matched:

| kubectl printed | When |
|---|---|
| `The connection to the server … was refused - did you specify the right host or port?` | k3s is restarting |
| `connect: host is down` | macOS, a local address where nothing answers |
| `context deadline exceeded (Client.Timeout exceeded while awaiting headers)` | `--request-timeout` ran out |

All four break row 27 of the classification table (`seam-c-provider.md` §5.1): kubectl, any command,
no connection to the apiserver ⇒ `Unreachable / ENDPOINT_UNREACHABLE`. Row 29 gives `INVALID_INPUT`
to `apply` only. This is the hard rule that "cannot determine state" is never an answer about state.

## Decision

**1. The discovery line is not a verdict.** It leaves `_INVALID_INPUT_STDERR_PHRASES`. The reason it
wraps is classified as before: a rejected credential still matches `_AUTH_STDERR_PHRASES` (row 28),
and a connection failure goes to `classify_subprocess` (row 27).

**2. The three sentences join `TRANSIENT_STDERR_PHRASES`.** `host is down` and
`client.timeout exceeded` are general. The `was refused` sentence is kubectl's own, and it is in the
shared list because that list is the one home for connectivity phrases.

**3. The tests replay real stderr at the transport seam.** `FakeKubectlBackend` gains
`unreachable_stderr_override`. The fake had one tidy `connection refused` line for "unreachable",
and real kubectl does not print that line.

## Consequences

- An apiserver that cannot be reached is `Unreachable` for every kubectl command, and stays so when
  the discovery cache expires. The health monitor skips the tick. A deploy step parks.
- A kubeconfig that points at the wrong address is now `Unreachable` too, not `INVALID_INPUT`. This
  is correct: seedpod cannot tell the two apart from one failed connection.
- The spec does not change. The code now does what row 27 says.

## Not decided here

- **The health monitor fails a cluster on the first `PermanentError` of any code**, and
  `SCRIPT_FAILED` is the classification of every sentence the classifier does not know. The next
  unknown sentence does the same damage. One answer is to fail at once on `AUTH` only and to count
  the other codes like transient failures. Fail-on-permanent is salvaged v1 behaviour
  (`runtime/health.py`), so a change to it needs its own decision.
- **The Local Network grant** that started the outage. See the guide.
- **Recovery. There is none.** The cluster stayed `failed` after it was reachable again, and the
  state machine has no transition from `FAILED` to `ACTIVE`. `clusters rehabilitate` is for
  destroyed, zombie and unmanaged clusters and is rejected here. `RetryRequested` runs the provision
  workflow again, and nothing emits it. So a cluster that the health monitor fails in error can only
  be destroyed and deployed again. This makes each wrong `PermanentError` expensive.

## What pins it

- `tests/conformance/test_kubectl_smoke.py::test_row27_real_kubectl_unreachable_stderr_is_never_permanent`
  — five captured outputs. Without the fix, all five are `PermanentError`.
- `…::test_discovery_failure_keeps_its_cause_when_the_cause_is_auth` — the wrapped reason still counts.
- `tests/runtime/test_health.py::test_unreachable_apiserver_with_expired_discovery_cache_stays_active`
  — the real `KubectlProvider` under the health monitor. Without the fix: `active` → `failed`.
