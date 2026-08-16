---
title: DR-0004 — Workflow verb families for kind / tart / orbstack provisioning
type: decision
status: superseded
created: 2026-07-14
updated: 2026-07-20
superseded-by: DR-0022-step-verb-vocabulary.md
---

# DR-0004: Workflow verb families for kind / tart / orbstack provisioning

**Status: SUPERSEDED by DR-0022 (2026-07-20)** — the per-provider `kind.*`/`tart.*`/`orbstack.*`
create/await families this DR introduced are replaced by late-bound `infra.*` verbs. DR-0022 retains
this DR's correct instinct (a vendor prefix for genuinely provider-unique capabilities, e.g.
`do.apply_firewalls`) and overturns its "a generic verb would reintroduce provider branching"
rationale on the record. Historical text below, unedited.

**Status: ACTIVE — ratified by Kezia, 2026-07-14. The three provision YAMLs may now be written.**

## Problem

The Pillar-2 build could not ship `provision-kind.yml`, `provision-tart.yml`, or
`provision-orbstack.yml`: no spec names a single concrete workflow-YAML verb string for these
providers. Seam B's `do.*` / `ssh.*` / `k3s.*` names are DigitalOcean-proof-only; Seam C defines
the underlying `ProviderCommand` dataclasses (`CreateInstance` / `ProbeInstance` /
`FetchKubeconfig` / …) but no YAML registry-key naming convention. Writing the files would have
meant inventing the verb surface without review — the stop-signal condition in CLAUDE.md. The
build agent correctly refused and flagged it.

## Proposal

One verb family per provider, mirroring `do.*`, each verb a `ProviderStep` over the Seam C
command it names:

- **kind**: `kind.create_cluster` (CreateInstance), `kind.await_ready` (ProbeInstance, gate),
  `kind.fetch_kubeconfig` (FetchKubeconfig — kind is in the FetchKubeconfig plane).
  No `ssh.*`/`k3s.*` steps — per Seam C §5.4 the kind/orbstack provision workflows simply omit
  them. Traefik shim = `kubectl.apply` of `config/manifest-templates/infrastructure/traefik-kind.yaml`
  with a non-fatal rollout gate (crown jewel #10 as workflow policy).
- **tart**: `tart.create_vm` (CreateInstance), `tart.await_vm` (ProbeInstance, gate — IP
  acquisition), then the **existing** `ssh.trust_host_keys` / `ssh.install_k3s` /
  `k3s.await_ready` / `ssh.fetch_kubeconfig` steps verbatim (tart is a real-VM provider using the
  shared ssh-k3s plane, exactly like DO from `trust_host` onward), then `kubeconfig.store`.
- **orbstack**: `orbstack.adopt_cluster` (CreateInstance — OrbStack has one persistent built-in
  cluster; "create" is adoption, and the verb name says so honestly), `orbstack.fetch_kubeconfig`,
  then `kubeconfig.store`, plus the Traefik shim via `kubectl.apply` of `traefik-orbstack.yaml`.

All three tails end with `kubeconfig.store` per coherence Conflict 9, and every file takes
`inputs: {cluster_id}` with a `cluster.load_spec` head per Conflict 10.

## Consequences

- Verbs grow the registry only (grammar untouched) — consistent with the freeze.
- `tests/engine/declared_verbs.py` (the load-time fixture that is the Pillar-3 interface
  contract) gains these verbs when the YAMLs are written; conformance reconciles fixture vs real
  steps.
- Timeout/interval literals come from `config/providers/{kind,tart,orbstack}.yml` — imported,
  not re-guessed.

## Alternatives considered

- Generic `provider.create_instance` verb parameterized by cluster provider (rejected: dispatch
  already selects the concrete workflow file per provider — Conflict 13; a generic verb would
  reintroduce provider branching inside step implementations).
- Reusing `do.*` names for all machine providers (rejected: the names would lie, and per-provider
  files exist precisely so each provider's flow is explicit data).
