---
title: DR-0034 — DNS records, both directions: two new verbs, a persisted record, and a create that is not best-effort
type: decision
status: active
created: 2026-08-10
updated: 2026-08-10
amends: DR-0022-step-verb-vocabulary.md
---

# DR-0034: DNS records, both directions

**Status: ACTIVE — ratified by Kezia, 2026-08-10**, including decision 7 (a DNS failure fails the
run) which was put to her explicitly as the one point a reasonable person could take the other way.
Opened for backlog **#22** (v2 never creates DNS
records) and **#6** (DNS-on-destroy), which smoke 10 established are one item with two halves.
Amends **DR-0022**'s ratified verb catalog (30 → 32) and therefore must land together with
`_build_step_registry`, `DECLARED_VERBS` and `DR_0022_VERBS` (DR-0022 Erratum E11's completeness
gate is a hard assertion).

## Context

Smoke 10 deployed `exampleco-staging-stack` — the DNS profile — on DigitalOcean. Every Ingress `host`
field and every app URL rendered `preset-…-e35dbd4d.cluster.example.com`. `dig` returned
nothing. The stack was reachable only by IP with a `Host:` header.

Three facts, one root cause:

- **There is no `dns.create_record` verb.** `services/dns.py:107` has `upsert_record`, salvaged from
  v1's `CloudflareDNSProvider`, and it is called by nothing. `engine/steps/dns.py` defines
  `dns.delete_record` alone, and DR-0022's catalog authorizes exactly that one. No shipped workflow
  references DNS creation. This never regressed; it was never in the vocabulary.
- **`clusters.dns_hostname` has no writer.** It is `None` at birth
  (`app/services/deployment_service.py:1368`) and `None` for discovered clusters
  (`runtime/reconciliation.py:426`). `api/routers/clusters.py:153` already derives
  `cluster_url = f"https://{dns_hostname}"`; the column, the API and the SPA are all ready for a
  value nobody writes. `clusters.dns_zone` is in the same state.
- **The destroy half already exists and is confused about it.** `destroy-cloud.yml:63` binds
  `dns.delete_record` to `infra.dns_record`, and `cluster.load_infra` builds that ref via
  `DnsRecordRef.from_provider_config` — reading `dns_record_id`/`dns_zone`/`dns_hostname` out of the
  cluster's `provider_config` blob, v1's storage shape. Nothing in v2 ever writes those keys, so
  `load_infra` always yields `None`, `dns.delete_record` always no-ops, and the destroy reports
  `succeeded` having deleted a record that was never created. **Backlog #6 has it backwards: destroy
  is the half that exists.**

This is also the **sixth instance** of this repo's recurring shape (see backlog #18): the hostname is
computed at deploy time, rendered into every manifest, and dropped before the record the API serves
ever sees it.

### What v1 actually did — read from source, not from the backlog prose

Per CLAUDE.md's salvage discipline and the standing lesson that four of six spec errors last session
came from drafting off summaries, every claim below cites v1:

| v1 site | behaviour |
|---|---|
| `orchestrator/cluster_manager.py:318-321` | copies the deployment profile's whole `dns:` block into `provider_config["dns_config"]`, **only when `enabled` is true** |
| `core/state_manager.py:451` | dispatches `_create_dns_record_if_configured` on the **`DEPLOYING`** transition — after K3s is installed and the public IP is known |
| `core/state_manager.py:937-1010` | reads `provider_config["dns_config"]`; returns early (debug) when not `enabled`; **returns `False` when `public_ip` is missing**; calls `create_cluster_dns_record`; writes `dns_record_id`/`dns_hostname`/`dns_zone` back into `provider_config` |
| `core/state_manager.py:1004-1010` | wraps the whole thing in `except Exception` and returns `False`: *"Don't fail the deployment if DNS creation fails - it's not critical"* |
| `providers/cloudflare_dns.py:308-354` | `subdomain_pattern.format(cluster_slug=…)`, default `"{cluster_slug}"`, `ttl` default 300, `proxied` default `False`; raises `ValueError` if `zone` is absent |
| `jobs/state/destruction_job.py:199-236` | deletes by **id**, skipping when `dns_record_id` or `dns_zone` is absent; best-effort, warning on failure |

So v1's create was: **profile-driven** (never provider-driven), **at DEPLOYING**, **persisted in the
`provider_config` blob**, and **non-fatal**.

## Decisions

### 1. Two new verbs; the catalog is 32

| verb | plane | thin | idempotent | undoable | Params → Output |
|---|---|---|---|---|---|
| `dns.create_record` | `service` | false | true | **true** | `{intent: DnsIntent \| None, slug: str, address: str}` → `{record: DnsRecordRef \| None, created: bool}` |
| `cluster.store_dns_record` | `domain` | false | true | false | `{cluster_id: str, record: DnsRecordRef \| None}` → `EmptyOutput` |

`dns.create_record` is a `Step`, not a `ProviderStep`, for exactly the reasons `dns.delete_record`
already is (DR-0022 P1: a DNS record is a supporting *service*; `DnsService` has no Seam C command and
no conformance suite). It takes `dns: DnsService | None` as a required-but-nullable constructor
keyword, identically to `DnsDeleteRecord`, and raises the same loud `PermanentError` when an intent
exists but no service is configured.

**Why a second verb rather than one that also persists.** `plane="service"` steps do not touch
repositories; `plane="domain"` steps do. Splitting the Cloudflare call from the row write is the
same shape v2 already ships for the kubeconfig — `k3s.fetch_kubeconfig` (IO, produces the fact) →
`cluster.store_kubeconfig` (domain, persists it) — and it keeps the engine's step-output persistence
as the crash-recovery boundary: the record id is durable in the step row before anything tries to
write the cluster row.

Consequences that must land in the same commit (DR-0022 E11): `DR_0022_VERBS` and `DECLARED_VERBS`
go to 32, `test_verb_catalog_size`'s two `== 30` assertions become `== 32`, and
`_build_step_registry` gains both entries. `FULLY_REGISTERED_WORKFLOWS` will need widening — that
test failing is the signal, not a nuisance.

### 2. `DnsIntent` — the create side's input, read from the cluster row

`core/dns_record.py` gains a sibling to `DnsRecordRef`:

```python
class DnsIntent(BaseModel):
    zone: str
    subdomain_pattern: str = "{cluster_slug}"
    ttl: int = 300
    proxied: bool = False

    @classmethod
    def from_provider_config(cls, provider_config) -> DnsIntent | None: ...
```

The defaults are v1's, verbatim (`cloudflare_dns.py:335-339`). `from_provider_config` returns `None`
unless `dns_config.enabled` is true — v1's own guard (`state_manager.py:958`).

**One deliberate divergence:** an `enabled: true` block with no `zone` raises `PermanentError` rather
than proceeding. v1 raised `ValueError` here (`cloudflare_dns.py:333`) inside the blanket
`except Exception`, i.e. it degraded to a silent skip. A profile that asks for DNS and names no zone
is a malformed profile, and the resolved hostname it would produce (`slug.cluster.`) is nonsense in
the manifests too. No shipped profile does this (grep-verified across `config/deployment-profiles/`).

### 3. The intent reaches the workflow through `cluster.load_spec`

`LoadSpecOutput` gains `dns_intent: DnsIntent | None`. This is an Output extension to an existing
catalog verb, of the same kind DR-0022 Erratum E13 recorded for `slug`, and it is authorized here
explicitly. `cluster.load_spec` is already the head of all four provision workflows, already reads
the cluster row, and already exists to answer "what does this cluster's provisioning need?".

It also gives the two directions a pleasing symmetry: `cluster.load_spec` yields `dns_intent`
(provision), `cluster.load_infra` yields `dns_record` (destroy).

**`_provider_config_from` carries the profile's `dns:` block** into
`clusters.provider_config["dns_config"]` when — and only when — `enabled` is true. This is v1
verbatim (`cluster_manager.py:318-321`); the "only when enabled" half matters, because it is what
makes absence in the blob mean absence of intent. `_cluster_specification_from` reads only named
keys, so the extra key is inert for `ClusterSpecification` construction.

### 4. The record is persisted in columns, not in the `provider_config` blob

Migration `0002_cluster_dns_record_id.sql` adds `dns_record_id TEXT` to `clusters`.
`cluster.store_dns_record` writes all three of `dns_hostname`, `dns_zone`, `dns_record_id` through a
new `ClusterRepository.set_dns_record(...)` — a plain UPDATE, no CAS, no `version` bump, returning
`False` if the row vanished, exactly the discipline `set_kubeconfig` documents for row-only columns
the pure machine never reads.

`DnsRecordRef.from_provider_config` is **replaced** by `DnsRecordRef.from_cluster_row`, reading the
three columns. `cluster.load_infra` changes one line.

Three alternatives rejected:

- **The `provider_config` blob (v1's shape).** Two of the three fields already have dedicated
  columns; v2's schema deliberately split v1's one blob into inputs (`provider_config`), provider
  outputs (`provider_resources`) and first-class columns. Writing the record back into the inputs
  blob re-conflates what the schema separated, and needs a read-modify-write of a JSON column.
- **`provider_resources`.** Tempting — it is the provisioning-*outputs* map, and `load_infra`'s own
  comment says so. But that whole map is bound wholesale into `infra.destroy_instance`'s
  `resource_ids`, so a `dns_record_id` key would be handed to the machine provider as one of its own
  resources.
- **A new machine event.** `emit:` would route the fact through `Dispatcher.apply`, but a new cluster
  event means spelling out every `(state, event)` cell (DR-0031 Erratum E1's three invariants, and
  `_fill_defaults`' raise-on-undefined). The kubeconfig precedent already establishes that a row-only
  column is a domain step's job, not the machine's.

### 5. Placement: after the address gate, in all four provision workflows

```yaml
  - id: dns                            # no-op unless the profile enabled DNS
    uses: dns.create_record
    with: {intent: {from: spec.dns_intent}, slug: {from: spec.slug},
           address: {from: droplet.address}}
    retry: api_default
    timeout_seconds: 30
  - id: dns_store
    uses: cluster.store_dns_record
    with: {cluster_id: {from: run.cluster_id}, record: {from: dns.record}}
    timeout_seconds: 30
```

Immediately after the `infra.await_instance` gate — the earliest point the address exists — and
before the k3s steps, rather than at the tail. Three reasons: it is the point where v1's own
precondition (`public_ip` present) first holds; the remaining two to three minutes of k3s install
become free DNS propagation time before the first deploy renders that hostname into an Ingress; and
placing it early is what makes the undo in decision 6 a real path rather than a formality.

**In all four provision workflows**, not just the cloud ones. v1 gated on `dns_config.enabled`
alone and never on provider, and the step is a no-op without an intent; no shipped local profile
enables DNS (`exampleco-*-nodns.yml` set `enabled: false`), so kind/orbstack pay nothing. A per-provider
allowlist here would be a second, silent policy about which profiles are honoured where.

### 6. `dns.create_record` is undoable; it deletes iff it created

`undo` deletes the record iff `output.created` is true. This is the entire reason
`DnsRecordUpserted.created` exists — Seam C §5.5's "P2 graft" over v1, so a rollback never destroys a
pre-existing record that the run merely re-pointed.

When `output is None` (execute never returned), undo does **nothing**. v2 cannot know whether a POST
landed, and guessing is exactly what `created` was introduced to avoid. The residual risk is a
create that succeeded at Cloudflare with a lost response: the record exists, no id is stored, and
destroy will not remove it. The next provision for the same slug upserts the same name, so it does
not multiply. Accepted and recorded, not fixed.

### 7. A create that fails, fails the run — a deliberate divergence from v1

`dns.create_record` and `cluster.store_dns_record` carry **no `on_failure: continue`**. The default
`fail` applies, and every `provision-*.yml` is `on_failure: compensate`, so a DNS failure destroys
what the run made — including, via decision 6, the DNS record if it got as far as creating one.

v1 was explicitly best-effort: *"Don't fail the deployment if DNS creation fails - it's not
critical"*. That was true in v1's world and is not true in v2's. `exampleco-staging-stack` renders
`cluster_hostname` into all four Ingress `host` fields and into every app URL — `KEYCLOAK_PUBLIC_URL`,
`FRONTEND_URL`, the five `window.APP_CONFIG` blocks. A cluster that reaches ACTIVE while its
advertised name does not resolve is precisely the smoke-10 defect, and it is silent: the SPA shows a
green cluster with a `cluster_url` that 404s. A profile that sets `dns.enabled: true` has declared
the record load-bearing.

The cost of transient failure is already covered: `retry: api_default` is 3 attempts with
exponential backoff (`Schedule(3, 2.0, 2.0, 30.0)`), and `DnsService` classifies Cloudflare
connectivity failures as `TransientError` by decision-table rows 36-38, never as
`InfrastructureUnreachableError`. What is left to fail the run is a *permanent* failure — a bad
token, a zone that is not in the account, a Cloudflare `success: false` envelope — every one of
which is an operator configuration error that must surface rather than be logged and forgotten.

This is the one decision in this DR that a reasonable person could take the other way. The
alternative — `on_failure: continue`, v1's policy — leaves a provisioned cluster advertising an
unresolvable name and costs nothing at provision time. **Put to Kezia explicitly and ratified as written, 2026-08-10.**

### 8. The FQDN invariant, and what this does NOT change

The record created at provision time and the hostname rendered into manifests at deploy time must be
the same string. They are computed by two different functions from the same two inputs:

- `app/services/deployment_service.py:951` — `f"{subdomain_pattern.format(cluster_slug=slug)}.{zone}"`
- `services/dns.py:120-122` — `_full_name(subdomain_pattern.format(cluster_slug=slug), zone)`, which
  is the same concatenation with a don't-double-suffix guard.

Equal by construction, given the same `dns_config` and slug — which decision 3 guarantees, because
both read the profile's `dns:` block, one live and one via `provider_config`. **Pinned by a test that
runs both paths off one shipped profile.**

**DR-0025 is untouched.** Manifest hostname resolution still happens at deploy time from the profile;
`clusters.dns_hostname` is written for the API/UI (`cluster_url`) and for the destroy path, and is
**never read back** by manifest resolution. Adding a rehydrate-from-column path would be a DR-0025
change and is deliberately not made here — the same restraint #17 exercised when the tempting
one-place hostname fix would have re-created the `https:///auth` bug.

Also explicitly out of scope: v1's `ssl_config` / Traefik ACME configuration
(`state_manager.py:1012-1090`). v2 uses Traefik's self-signed certificate; a real Let's Encrypt
certificate needs the DNS record this DR creates, so it becomes *possible* here, but it is a separate
item.

### 9. Backlog #6 closes as a consequence

No new work. Once decision 4 lands, `cluster.load_infra` yields a real `DnsRecordRef` and the
already-shipped `dns.delete_record` step in `destroy-cloud.yml` / `destroy-shared.yml` starts
deleting real records. #6's own text ("`cluster.load_infra` … still-unimplemented") is stale — it has
been implemented since Round 8b; what was missing was anything to load.

## Consequences

**Files that must move together** (E11's gate makes the first three atomic):

- `docs/decisions/DR-0022-step-verb-vocabulary.md` — frontmatter `amended-by: DR-0034`
- `tests/engine/declared_verbs.py`, `tests/engine/test_verb_conventions.py` — 30 → 32
- `seedpod/app/factory.py` — two registry entries
- `seedpod/core/dns_record.py` — `DnsIntent`; `from_provider_config` → `from_cluster_row`
- `seedpod/engine/steps/dns.py` — `DnsCreateRecord`
- `seedpod/engine/steps/cluster.py` — `StoreDnsRecord`; `LoadSpecOutput.dns_intent`; `LoadInfra`'s ref build
- `seedpod/data/migrations/0002_cluster_dns_record_id.sql`, `seedpod/data/repositories.py` — column + `set_dns_record`
- `seedpod/app/services/deployment_service.py` — `_provider_config_from` carries `dns_config`
- `seedpod/runtime/reconciliation.py` — `dns_record_id=None` on the discovered-cluster row
- `config/workflows/provision-{digitalocean,tart,kind,orbstack}.yml` — the two steps

**Verification.** The suite cannot prove this one: the record either resolves or it does not. The
closing evidence is a smoke on `exampleco-staging-stack` (the DNS profile) on DigitalOcean that
**checks the advertised hostname resolves and serves** — `dig +short <hostname>` returning the
droplet IP, and an HTTPS request with no `Host:` override — and then confirms after destroy that
`dig` returns nothing and the Cloudflare zone has no leftover record. Nine smokes stopped at
"pods are Running"; that is the gap this item exists to close.
