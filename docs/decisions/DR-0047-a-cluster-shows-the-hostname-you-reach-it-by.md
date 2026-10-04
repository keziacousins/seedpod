---
title: DR-0047 — a cluster shows the hostname you reach it by, not only the DNS record seedpod made
type: decision
status: active
created: 2026-10-04
updated: 2026-10-04
---

# DR-0047: a cluster shows the hostname you reach it by

**Status: ACTIVE — ratified by Kezia, 2026-10-04.** Found on 2026-10-04: a cluster on tart was
reachable by name, and its page in the SPA showed only an IP address that no browser outside the
host could reach.

## Context

A cluster has two different facts that can be called "its hostname".

1. **The DNS record seedpod made.** `clusters.dns_hostname`, with `dns_zone` and `dns_record_id`.
   One step writes them: `cluster.store_dns_record` (DR-0034), and only for a profile with
   `dns.enabled: true`. The destroy path reads the same three columns to delete the record.
2. **The name the stack was rendered for.** The profile's `hostname.strategy` resolves
   `cluster_hostname`. The Ingress host rules and the application URLs use it. It is stored in
   `deployment_audits.resolved_config`, and nowhere on the cluster row.

For a profile with `strategy: dns`, the two are the same string. For a profile with
`dns.enabled: false` and `strategy: custom`, only the second exists. The measured case: the
pattern `{cluster_slug}.<tailnet>.ts.net`, which is the Tailscale MagicDNS name that the in-cluster
tailscale DaemonSet registers.

The cluster API derived `cluster_url` from `dns_hostname` alone, as v1 did. The SPA shows its "URL"
row only when `cluster_url` is set. So for the second kind of cluster the API and the SPA showed
nothing, although seedpod had resolved the name and every Ingress in the cluster used it.

## Decision

**1. `dns_hostname` keeps its meaning.** It is the DNS record that seedpod owns and deletes. A name
that seedpod did not create is never written there.

**2. The cluster API reports how to reach the cluster.** `GET /api/clusters` and
`GET /api/clusters/{id}` gain two fields:

| Field | Value |
|---|---|
| `hostname` | The name to reach the cluster by, or `null` |
| `hostname_source` | `dns_record`, `profile`, or `null` |

`cluster_url` is now `https://{hostname}`. It was `https://{dns_hostname}`.

**3. The rule is pure and lives in `seedpod/core/cluster_access.py`.**

- If `dns_hostname` is set, the hostname is that name and the source is `dns_record`.
- Otherwise, if the newest deployment audit of the cluster resolved a `cluster_hostname`, the
  hostname is that name and the source is `profile`.
- Otherwise there is no hostname.

**4. An IP address is not a hostname.** `strategy: provider_host` resolves to the address the
provider reports. On DigitalOcean and tart that is an IP, and the API already returns it as
`public_ip`. The test is the same narrow one as the `is dns_name` template test: reject IP
literals and nothing else.

**5. The value is derived when the API reads it.** `DeploymentAuditRepository.
latest_cluster_hostnames` reads `resolved_config` for the clusters that have no `dns_hostname`. It
decrypts nothing. There is no schema change, no new step, and no state change, so the Dispatcher
is not involved.

**6. The SPA shows the name, and says when seedpod did not make it.** The cluster page and the
cluster list use `hostname`. For `hostname_source: profile` the cluster page adds one line: seedpod
made no DNS record for this name, so it resolves only where the operator's network knows it.

## Why not the alternatives

- **Write the profile name into `dns_hostname`.** The destroy path uses that column to delete a
  DNS record. A name seedpod did not create must not look like a record it owns.
- **Add an "access hostname" column to `clusters`.** This is the cleaner model, and it removes the
  extra read. It also changes the ratified schema and adds a write on the state path. The fact
  already exists in the audit, so the read is enough for now. If a second reader needs the name
  (the reconciler, a webhook payload), add the column then.
- **Say "Tailscale" in the SPA.** seedpod does not know why a profile name resolves. It knows only
  that the profile supplied it. The note says that, and no more.

## Consequences

- `cluster_url` is no longer v1's exact derivation. A cluster that had `cluster_url: null` can now
  have a value. No caller is known to depend on the `null`.
- A cluster list makes one more query when at least one listed cluster has no `dns_hostname`.
- The scheme is always `https`, as before. A profile with `ssl.enabled: false` still gets an
  `https` URL. Not decided here.
- The hostname comes from the **newest** audit. If a later deployment to the same cluster resolves
  no hostname, the API shows none; it does not fall back to an older audit.

## Changed

- `seedpod/core/cluster_access.py` (new), `tests/core/test_cluster_access.py` (new).
- `seedpod/data/repositories.py`: `DeploymentAuditRepository.latest_cluster_hostnames`.
- `seedpod/api/routers/clusters.py`: `hostname`, `hostname_source`, and the `cluster_url` rule.
- `ui/src/pages/ClusterDetail.jsx`, `ui/src/pages/ClusterList.jsx`.
- `docs/design/ui-contract.md`: the two cluster rows.
