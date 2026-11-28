---
title: DR-0055 — a bare VM is an instance, its own aggregate, and never a cluster without k3s
type: decision
status: active
created: 2026-11-23
updated: 2026-11-28
amended-by: DR-0056
---

# DR-0055: a bare VM is an instance, not a cluster

**Status: ACTIVE — ratified by Kezia, 2026-11-23.**

## Context

Agents need to request a plain VM for resourcing, locally or in a cloud, from a template. crew
needs the same thing for worker nodes: its ADR-0019 and `docs/notes/worker-nodes.md` leave open who
creates and retires them. Seedpod already has the hard parts: retry-safe creation, compensation,
durable timers, reconciliation, audit and scoped keys.

A cluster that skips k3s does not work:

- `ProvisionSucceeded` requires a `kubeconfig_ref`. `CreateInstance` requires pod and service CIDRs.
- `HealthMonitor` skips a cluster with no kubeconfig, so a bare VM would be `active` and unchecked.
- Tart's `Reconcile` turns a stopped VM into an `OrphanIntent`.
- Cluster routes, deployments and snapshots would all be offered on something with no Kubernetes.

Seedpod also has no quotas, no idle handling beyond a fixed TTL, and no boot payload.

## Decision

**1. A new aggregate, `Instance`.** It has an `instances` table and its own pure transition table.
The name follows the provider contract (`CreateInstance`, `InstanceState`). "Machine" already means
the state machine in `core/machine.py`. States: `new`, `provisioning`, `active`, `failed`,
`destroying`, `destroyed`, `destroy-failed`, `zombie`. There is no `unmanaged`: seedpod never adopts
a VM as an instance.

**2. Instances are invisible to cluster discovery.** DigitalOcean reconcile births an `UNMANAGED`
cluster, in `production` (DR-0013), for any `seedpod-managed` droplet with a `cluster-uuid:` tag and
no row. So an instance:

- on DigitalOcean carries `seedpod-instance` and `instance-uuid:{id}`, and neither cluster tag;
- on tart is named `spi-{slug}`, which does not match the cluster prefix `seedpod-`.

Instance reconcile lists only its own marker and never touches anything else.

**3. New provider commands; cluster commands do not change.**

- `CreateBareInstance` takes the instance id (the idempotency key), slug, template image, size and
  tags.
- `DestroyBareInstance` takes the instance id, slug and recorded resource ids. With no recorded id,
  it finds the VM by its instance marker only: `instance-uuid:{id}` on DigitalOcean, `spi-{slug}` on
  tart.
- `ListBareInstances` returns only VMs that carry the instance marker.
- `ProbeInstance` and `ProbeDestruction` are reused. They read only resource ids.

An instance must not use `DestroyInstance`. With no recorded id, that command falls back to
`cluster-{slug}` on DigitalOcean and `seedpod-{slug}` on tart, so it can destroy a live cluster with
the same slug. `ListInstances` does not fit either. It filters on `seedpod-managed` on DigitalOcean
and on `seedpod-` on tart, and an instance has neither.

An instance slug is unique among instances that are not `destroyed`, because tart finds a VM by
that name.

New step verbs live under `instance.*`. Workflows are `provision-instance-{provider}.yml` and
`destroy-instance.yml`. The grammar does not grow.

**4. The engine runs workflows for either aggregate.** `RunWorkflow` and `CancelWorkflow` take
`aggregate_type` and `aggregate_id`, as `ScheduleTimer` already does. The codec must still decode
outbox rows written with `cluster_id`.

- `workflow_runs` gains a nullable `instance_id`. Its `cluster_id` becomes nullable. A CHECK
  requires exactly one.
- `ux_wr_one_active` does not limit NULL values. It becomes partial on `cluster_id IS NOT NULL`, and
  a second unique index applies the same limit to `instance_id`.
- The dispatcher and the engine select the aggregate explicitly. The dispatcher now treats every
  non-cluster as a deployment, and the engine treats every non-deploy run as a cluster. An unknown
  aggregate type is an error.
- The migration turns `foreign_keys` off before its own `BEGIN`, rebuilds the table, and commits.
  A plain rebuild fails on the `workflow_steps` foreign key.
- The migration test asserts that `foreign_key_check` returns no rows. A SQL script cannot act on
  that result.
- The migration runner restores `foreign_keys` when a migration fails, because the connection is
  shared.

**5. Templates are the only source of images.** `config/instance-templates/*.yml` names a provider,
image, allowed sizes, default lease and maximum lifetime. "From scratch" is a template on a stock
image. A request names a template, and cannot supply an image.

**6. Limits are checked at admission.** `config/instance-limits.yml` caps count, vCPU, memory and
hourly spend, per key and per environment, plus capacity per local provider host.

- The check runs in the transaction that writes the birth row, so concurrent requests cannot both
  take the last slot.
- Every state that may still hold a resource counts: all but `destroyed`.
- Spend uses the hourly price in the template's size list. A size with no price is refused.
- A refusal is 429, names the limit, and leaves no row.

**7. Every instance has a lease and a hard limit.** A request sets `lease_seconds`, within the
template's bounds. `hard_expires_at` is fixed at creation and never moves.

- `POST /api/instances/{id}/renew` sets the expiry to `min(now + lease, hard_expires_at)`.
- One durable timer, key `expiry`, fires at that time and destroys the instance.
- The holder renews while work is active. Seedpod does not inspect the guest to infer idleness.
- Renewal after expiry is 409. There is no instance without an expiry.
- Stop and resume are not built. A VM found stopped becomes `failed`, and its timer still runs.

**8. Boot data goes over SSH, after the host key is captured.** Tart has no user-data channel, and
any process on a droplet can read its metadata. One step, for both providers, writes the payload to
a root-only file and runs the template's fixed entrypoint. The payload holds the requester's SSH
public key.

The payload holds no boot secrets until run inputs are encrypted. The `secret: true` flag is parsed,
but nothing acts on it, so a secret input would be stored as plaintext in `effects_outbox` and
`workflow_runs.args`. A request that supplies a boot secret is refused.

**9. A key sees only its own instances.** New permissions: `instances:create`, `instances:read`,
`instances:renew`, `instances:delete`. An instance records `owner_key_id`. Only a `*` key acts on
another key's instance. DR-0054's environment rule applies on top, including to `*`.

**10. crew is a client.** crew or its hub requests and renews instances through this API. What runs
on the host stays crew's: identity, runs, workers and transcripts.

## Consequences

- One provisioning path serves agents, crew and the local and cloud providers.
- **Decision 4 is the riskiest change.** SQLite needs a table rebuild to relax `NOT NULL`, on a table
  every workflow uses. It needs a migration test against a populated database, including a failed
  migration. The acceptance suite must pass unchanged.
- DR-0054's table gains a row: an instance's environment is `instances.environment`. Its timers and
  runs follow it. The timer and workflow lists currently hide these rows from a scoped key, and must
  change. Decision 9 depends on DR-0054.
- `docs/design/` and crew's `DESIGN.md`, which plans its own VM providers, need updating on
  ratification.
- A holder that keeps renewing while stuck costs money until `hard_expires_at`. That is the bound.
- Destroy on expiry loses anything unsaved on the VM. Holders must collect their work first.

## Not decided here

- **Boot secret delivery.** It needs encrypted run inputs first.
- **A marked VM with no row.** This can follow database loss. Decision 1 does not adopt it, and it
  has no expiry timer, so it costs money until someone acts. Instance reconcile must not destroy it
  until this is decided.
- **Network egress.** crew's sandbox contract needs an allowlist enforced outside the VM. It needs a
  proxy for each provider, and gets its own DR. Until then, do not treat an instance as a containment
  boundary for untrusted work.
- **DigitalOcean inbound firewall.** The cluster firewall opens Kubernetes ports. Instances need
  their own rule, and the allowed SSH sources depend on how crew hosts join the network.
- **Warm pools, stop and resume, and building or refreshing template images.**
- **How a crew host gets its identity** (crew's `docs/notes/identity-beyond-tailscale.md`). Decision 8
  is the delivery path, not the identity scheme.
- **UI.** This decision covers the API and `seedpodctl` only.
