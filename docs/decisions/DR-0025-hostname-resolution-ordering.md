---
title: DR-0025 — a hostname that cannot be resolved yet fails loudly; it is never rendered empty
type: decision
status: active
created: 2026-08-04
updated: 2026-08-04
amended-by: DR-0025 Erratum E1 (self, 2026-08-04); DR-0025 Erratum E2 (self, 2026-08-06)
---

# DR-0025: post-provision hostnames — fail loudly now, re-resolve in the deploy verbs

**Status: ACTIVE — ratified by Kezia, 2026-08-04.** Raised by Round 9's second adversarial judge on the
`resolved-config` component, which caught the build papering over the gap with an empty-string default.

## Problem

**v1 resolved manifests *after* provisioning; v2 resolves *before* the cluster row is even born.**

That ordering difference is deliberate and load-bearing in v2 — manifest resolution is part of the
deployment *decision* flow (webhook → rule → resolve → birth → response), which is exactly what the
acceptance parity gate pins, and what makes a resolution failure degrade to a recorded, rejected
deployment instead of a 500. But it means one class of value v1 always had is structurally unknowable
at v2's resolution time: anything derived from the provisioned infrastructure.

`hostname.strategy: provider_host` is that class. v1's `_resolve_hostname`
(`reference-code/seedpod/seedpod/orchestrator/manifest_resolver.py:694-765`) reads a `provider_host`
that, in v1, was always available because provisioning had already happened. In v2, for a **new**
cluster, there is no droplet, no VM, and no IP at the moment `resolve()` runs.

Three shipped profiles declare it — `exampleco-dev-stack-nodns.yml`, `exampleco-staging-stack-nodns.yml`,
`exampleco-web-2-kind.yml` — and this is not a cosmetic label. **Twenty environment variables across the
two `-nodns` profiles interpolate the hostname into URLs**:

```yaml
KEYCLOAK_PUBLIC_URL: "https://{{ cluster_hostname }}/auth"
KC_HOSTNAME:         "https://{{ cluster_hostname }}/auth"
FRONTEND_URL:        "https://{{ cluster_hostname }}"
KEYCLOAK_REDIRECT_URIS: "https://{{ cluster_hostname }}/*"
```

The Round 9 build resolved this correctly in `_build_resolved_config` — it omits `cluster_hostname`
entirely when unresolvable, faithfully matching v1's `if cluster_hostname:` guard — and then
reintroduced the problem one layer up, in the context handed to environment-variable resolution:

```python
"cluster_hostname": resolved_config.get("cluster_hostname", ""),   # defeats StrictUndefined
```

That empty-string default is the whole defect. `EnvironmentVariables.resolve_for_service` renders with
`StrictUndefined` precisely so a missing value raises; supplying `""` makes the name *defined*, so
Jinja renders it happily and every URL above becomes `https:///auth`. Keycloak, the frontend, and the
redirect URIs are then configured with malformed URLs — in manifests that apply cleanly and report
green. This is the silent-edge-regression failure mode `CLAUDE.md` names as the one that matters, and
it would not have been caught by any test asserting "resolution succeeded".

## Decision

**Two parts. The first binds Round 9; the second binds Round 10.**

### 1. A value that cannot be resolved is ABSENT, never empty (Round 9, immediate)

No placeholder, no `""` default, no `or ""`, anywhere on the resolution path. If a profile's hostname
strategy cannot be satisfied, `cluster_hostname` is simply **not in the context**, and `StrictUndefined`
raises on first use. The resulting `PermanentError(ErrorCode.INVALID_INPUT)` names the offending
service and key, and flows into the *existing, already-pinned* degradation contract: HTTP 200,
`status=manifest_resolution_failed`, deployment `rejected`, cluster still born, never a 500.

The consequence is accepted deliberately: **a `provider_host` profile cannot complete a first
deployment until part 2 lands.** A loud, recorded rejection naming the reason is strictly better than
a green deployment wired to `https:///auth`. This is the same reasoning as DR-0022's `kube.apply_docs`
ruling and the crown-jewel-#1 posture generally — absence and emptiness are not the same fact.

This generalizes beyond hostnames. Any future value derived from provisioned infrastructure obeys the
same rule: absent, loud, and recorded — never a plausible-looking placeholder.

### 2. The deploy verbs re-resolve with the known host (Round 10)

Round 10 builds the seven deploy-path verbs. By the time they run, the cluster is `ACTIVE` and its
address is known. Hostname-dependent resolution is therefore **re-run at deploy time**, against the
real provisioned host, before the manifests are applied.

Round 10 owes an explicit answer to one question this raises, recorded in its own DR or in this one by
amendment: `deployment_audits` currently stores the manifest resolved at decision time, and that row is
the reproducibility record. If the applied manifest is re-resolved, the audit must either store the
re-resolved manifest too, or record clearly that it holds the pre-provision resolution. **The audit
must not silently diverge from what was applied** — that would trade one silent inconsistency for
another.

## Consequences

- **Round 9** must contain no empty-string or placeholder default for `cluster_hostname` on any path,
  and must carry a test proving that a `provider_host` profile resolved with no known host raises a
  typed error naming the variable — rather than emitting a `https:///` URL. That test is the
  regression pin for this DR.
- **`exampleco-dev-stack-nodns` / `exampleco-staging-stack-nodns` / `exampleco-web-2-kind` cannot complete a first
  deployment between Round 9 and Round 10.** `exampleco-web-2.yml` — the profile all three smokes used —
  declares neither `hostname:` nor `dns:`, resolves to no hostname at all, and is unaffected.
- **Smoke 4 (a real deployment on `tart`) is gated on part 2.** The documented way to run a stack
  against tart is a preset with `provider_override: tart` pointed at `exampleco-dev-stack-nodns`, which is
  a `provider_host` profile. Sequence Round 10 before smoke 4, or smoke against a profile with no
  hostname strategy.
- **`PARITY-BACKLOG` item #2 is narrower than written but not empty.** Hostname synthesis is real work;
  what it is *not* is "DNS/SSL/ingress synthesis" as a block — the shipped templates read five config
  keys, none of them the hostname. The hostname's only consumers are environment-variable values.

## Alternatives rejected

- **Thread `existing.public_ip` on the redeploy path only.** Makes redeploys to an existing cluster
  work and first deploys still fail. Rejected as a half-measure that leaves the same hole open on the
  path that matters, while adding a branch whose two sides behave differently for no stated reason.
- **Defer to Round 10 without the loud failure.** Rejected because it leaves the empty-string default
  in the tree for a whole round — precisely the "we'll fix it later" state in which
  `https:///auth` ships.
- **Keep lenient rendering and validate the rendered output for `https:///`.** Rejected: string-matching
  rendered YAML for malformed URLs is a detector for one symptom of a general problem, and it would pass
  any profile that interpolated the empty hostname somewhere a regex did not anticipate.

## Erratum E1 — "absent" was two different facts; the decision splits them

**Ratified by Kezia, 2026-08-04**, on the second `resolved-config` halt. **This erratum is binding and
refines the Decision above wherever they differ.**

The Decision as first written said no empty default "on any path", and treated *absence* as a single
outcome. That conflated two genuinely different facts, and taken literally it would have broken the
shipped templates.

`config/manifest-templates/exampleco-stack/` uses `cluster_hostname` as a **feature gate**, not just as an
interpolated value — `frontend-server.yaml`, `mailhog.yaml`, `mailpit.yaml` and `exampleco-api.yaml` all
carry `{% if cluster_hostname %}` and `{% if cluster_hostname and ssl_enabled %}`. Under
`StrictUndefined`, an *omitted* name makes those gates **raise** rather than evaluate false. So a
profile that legitimately has no hostname (`hostname.strategy: "none"` — the correct, intended state
for `exampleco-web-2.yml` and anything without ingress) would fail to render at all.

The fix is not a carve-out. It is this DR's own principle, stated properly — the same
absence-vs-unreachability distinction `CLAUDE.md` calls the one that matters:

| Fact | Representation | Behaviour under `StrictUndefined` |
|---|---|---|
| The empty string, in any form | **BANNED, always** | — (never produced) |
| "This profile deliberately has no hostname" (strategy `none`, or no strategy resolvable to one) | `cluster_hostname = None` | Feature gates evaluate **false**, cleanly; direct interpolation yields `None`, not a plausible-looking `""` |
| "A strategy wanted a host and could not produce one" (`provider_host` before provisioning) | **key omitted from the context entirely** | **Raises**, loudly, naming the variable |

The empty-string ban is unchanged and absolute. What changes is that omission is now reserved for the
*unknowable* case, and `None` carries the *legitimately-nothing* case — so a feature gate keeps
working while an unknowable host still fails loudly.

**Consequence for the mandated test:** it must now pin BOTH halves, or it does not pin this DR — a
`provider_host` profile with no known host raises, AND a `strategy: "none"` profile renders cleanly
with its `{% if cluster_hostname %}` blocks omitted. A test covering only the raising half would let
the template-breaking regression back in.

## Erratum E2 — parts 1 and 2 contradicted each other; rendering DEFERS rather than fails

**Ratified by Kezia, 2026-08-06.** Raised by Round 10's second adversarial judge on `load-and-plan`.
**This erratum is binding and supersedes the Decision above wherever they differ.**

### The contradiction

Part 1 makes a `provider_host` profile **fail** resolution at decision time. `_deploy`
(`seedpod/app/services/deployment_service.py:568-580`) responds to a resolution error by birthing the
deployment as `DeployRejected` — and **inserting no `deployment_audits` row at all** (the audit insert
lives in the `else` branch at `:581`).

Part 2 says the deploy verbs re-resolve **from that audit**.

So part 2 could never execute: part 1 rejects the deployment before an audit, a `spec_ref`, or a deploy
run exists. The DR specified a dead code path and then made implementing it Round 10's obligation.

The mechanical root is that rendering is all-or-nothing per template: with `cluster_hostname` absent
and `StrictUndefined` in force, `_render_templates` raises. There is no "render what you can".

### The decision

**A fourth state: DEFERRED. "Not known yet, and will be known after provisioning" is distinct from
"unknowable".**

| Fact | Representation |
|---|---|
| The empty string, in any form | **BANNED, always** (unchanged) |
| Deliberately no hostname (`strategy: "none"`) | `cluster_hostname = None`; renders now, gates evaluate false (E1, unchanged) |
| A strategy wanted a host and it can **never** be produced (bad config) | key omitted; **raises now**, deployment rejected (unchanged) |
| A strategy wanted a host that **will exist after provisioning** (`provider_host`, pre-provision) | **DEFERRED** — see below |

For the deferred case, scoped to profiles that need it and to nothing else:

1. **At decision time**, image/secret/config resolution proceeds normally, and the `deployment_audits`
   row IS written — carrying the resolution inputs it already stores (`triggering_repo`,
   `triggering_branch`, `triggering_image`, `commit_sha`, `deployment_profile_name`, `environment`,
   `resolved_config`, `resolved_images`, `resolved_secrets`) — but with **no rendered manifests**, and
   marked explicitly as pending deploy-time rendering. The deployment is **not** rejected.
2. **At deploy time**, once the cluster is `ACTIVE` and its address is known, the deploy path renders
   against the real host and **rewrites the same audit row in place**, recording that it was rehydrated.

Deferral does not weaken part 1. Part 1 forbids *silently rendering a plausible-looking empty value*;
deferral renders nothing, says so, and renders correctly later. The failure mode part 1 exists to stop —
`https:///auth` reaching an applied manifest — remains impossible.

### Why rewrite in place rather than append

One row, one truth. DR-0025's own Consequences require that the audit not silently diverge from what
was applied; a second row would satisfy history at the cost of making every reader decide which row to
trust. The rehydration marker preserves the fact that two resolutions happened without splitting the
record.

### Consequences

- Everything outside the deferred case is untouched: `exampleco-web-2.yml` (no hostname strategy, all three
  smokes' profile) and the acceptance parity gate behave exactly as before. **Confirm that by test.**
- A test must pin the deferral end to end: a `provider_host` profile deploys, its audit is written with
  no manifests and marked pending, and after deploy-time rendering the same row carries manifests
  containing the real host and **no `https:///`**.
- "Pending" must be a real, queryable fact on the row, not an inference from an empty string. An
  audit whose manifests are empty for any *other* reason must remain distinguishable from a deferred one.
- The rejected-deployment path stays exactly as it is for genuine config errors. Do not let deferral
  swallow a profile whose hostname strategy is simply wrong.
