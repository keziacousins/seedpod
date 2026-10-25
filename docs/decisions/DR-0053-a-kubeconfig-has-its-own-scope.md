---
title: DR-0053 — a cluster's kubeconfig is readable through the API, behind a scope of its own
type: decision
status: proposal
created: 2026-10-25
updated: 2026-10-25
amended-by: DR-0054
---

# DR-0053: a kubeconfig has its own scope

**Status: PROPOSAL — not yet ratified.**

## Context

Seedpod stores each cluster's kubeconfig, encrypted, in `clusters.encrypted_kubeconfig`. Before
this decision, only seedpod read it: the pods, logs and events routes decrypt it and run kubectl.
No route returned it.

A machine that deploys through seedpod also needs to run `kubectl` against the cluster it just
deployed. It had no way to get the kubeconfig except a copy by hand from the seedpod host. That
copy goes stale each time a deploy makes a new cluster.

A kubeconfig is admin access to the cluster. With it, a caller can read every Kubernetes secret
in the cluster. No existing scope gives that access: `secrets:read` shows metadata only, and
`clusters:*` can destroy a cluster but cannot look inside it.

## Decision

**1. `GET /api/clusters/{id}/kubeconfig` returns the decrypted kubeconfig.** The response is
JSON, `{cluster_id, slug, status, kubeconfig}`, like `GET /api/secrets/{env}/{key}/reveal`.
`{id}` is an id or a slug, like `GET /api/clusters/{id}`. A caller can then do this after each
deploy:

```sh
seedpodctl clusters kubeconfig "$SLUG" -o ~/.kube/config
```

A cluster with no kubeconfig yet (still provisioning, or failed first) is 404. An unknown cluster
is 404.

**2. The scope is `kubeconfig:read`, in a category of its own.** It is not
`clusters:kubeconfig`. The permission check admits `<category>:*` for every scope in that
category, so `clusters:kubeconfig` would go to every key that holds `clusters:*`. Those keys
would get admin access to clusters without anyone asking for it. With its own category, only
`kubeconfig:read`, `kubeconfig:*` and `*` admit the route.

**3. The key's environment must admit the cluster's.** A key scoped to `'all'` can read any
cluster's kubeconfig. A key scoped to an environment can read only the kubeconfig of a cluster in
that same environment. Any other key gets 403, and the message names both environments. This
holds for a key with `*` too: `*` grants every scope, not every environment. The rule is
DR-0010's rule for SSE, applied to one REST route. DR-0054 applies it to every REST route and
moves it to `seedpod/core/environment_scope.py`.

**4. `seedpodctl clusters kubeconfig <id-or-slug>` prints the file itself**, not the JSON
around it. With `-o FILE` it writes the file with mode 0600 and replaces any old file in one
rename. A shell redirect would make the file 0644 on most systems.

**5. Each read writes one INFO log line** with the cluster id, the slug and the key's username.
No database row records it. `cluster_state_audits` records state changes, and a read is not one.

## Consequences

- To give an existing key this scope, make a new key and revoke the old one. Key permissions do
  not change after a key is made (`seedpod/api/routers/keys.py`). The new key's environment
  must be `'all'` or the environment of the clusters it will read.
- The new scope shows in `GET /api/permissions`, so the SPA's key form offers it with no SPA
  change.
- The server address in the kubeconfig is the one seedpod uses. Each provider rewrites it when it
  stores the file. A caller on another network may not reach that address.

## Not decided here

- **Environment scope on every other REST route, and key environment values.** DR-0054 decides
  both.
- **A durable audit row.** If a read must be on record after the logs rotate, it needs a table.
