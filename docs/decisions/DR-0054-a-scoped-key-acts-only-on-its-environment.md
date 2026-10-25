---
title: DR-0054 — an API key scoped to one environment acts only on that environment, on every REST route
type: decision
status: proposal
created: 2026-10-25
updated: 2026-10-25
---

# DR-0054: a scoped key acts only on its environment

**Status: PROPOSAL — not yet ratified.**

## Context

An API key has an `environment`. `'all'` means the key is not scoped. Any other value is meant to
limit the key to one environment. The SPA's key form offers `ephemeral`, `staging` and
`production`.

Two ratified decisions assume REST routes enforce that limit:

- DR-0010: "REST responses are filtered by the API key's environment scope". It kept the SSE
  filter so that a scoped key could not watch what it "cannot GET".
- DR-0013: a non-production key "can neither watch nor GET" a discovered cluster.

v2 never built the REST half. Before this decision, only SSE and
`GET /api/clusters/{id}/kubeconfig` (DR-0053) read a key's environment. On every other route, a
key scoped to `staging` with the right permission could:

- reveal, change or delete a production secret. The environment is in the URL or the body, and
  no route compared it with the key's.
- read a production pod's logs, its environment variables and its events;
- destroy, extend or rehabilitate a production cluster;
- start, cancel, redeploy or retrigger a production deployment;
- restore production data into a cluster, or delete a production snapshot;
- make a new key scoped to `'all'`, if it held `*`.

`POST /api/keys` also accepted any string as an environment. A misspelt key (`prod`) matched
nothing, and nothing said so.

## Decision

**1. One rule.** A key may act on a resource if the key's environment is `'all'`, or is equal to
the resource's environment. The rule is DR-0010's SSE rule. It lives in
`seedpod/core/environment_scope.py` (`environment_admits`, `check_environment`). It is pure, so
the API and the services can both call it.

**2. The rule is about environments, not permissions.** `*` grants every permission and no
environment. A `*` key scoped to `staging` cannot touch `production`.

**3. A resource's environment:**

| Resource | Its environment |
|---|---|
| cluster | `clusters.environment` |
| deployment | `deployments.environment` |
| preset | `presets.environment` (from the profile's `environment_type`) |
| secret | the environment in the request (path, body or query) |
| snapshot | its source cluster's. Snapshots have no environment column. |
| workflow run | its cluster's |
| timer | its cluster's. A timer on any other aggregate type is hidden from a scoped key. |

**4. A list shows only what the key admits. A request for one resource the key does not admit is
403.** This covers clusters, deployments, presets, snapshots, workflow runs and timers. The SPA
then works with a scoped key, and still shows a clear error for a direct link. The 403 message
names the key's environment and the resource's.

A 403 says that the resource exists. It does not say what is in it. Ids are random, and slugs
come from branch names, which are not secret. A 404 would hide little, and would make the
refusal harder to understand.

**5. Where the rules or a profile choose the environment, the check comes after the choice and
before the first write.** A refused request leaves no row behind.

- `POST /api/version-update` and `POST /api/presets/{id}/deploy` both reach
  `DeploymentService._deploy`. Its first statement is the check.
- `POST /api/deployments/{id}/retrigger` is checked twice: against the original deployment, and
  in `_deploy` against the new one. The rules run again, so the two can differ.
- Preset deploy is also checked before `record_usage`, so a refused deploy is not counted as a
  use.
- Preset create and update check the profile's environment. Update also checks the preset's
  current environment.
- Preview checks the environment it would use, because a preview lists that environment's secret
  names.

The services take the key's environment as `key_environment`, a keyword argument with **no
default**. A call site that does not pass it fails at once. It does not fail open.

**6. A snapshot restore needs both ends.** The key must admit the target cluster's environment
and the snapshot's source cluster's environment. Without the second check, a staging key could
pour production data into a staging cluster.

**7. A change that reaches every environment needs an `'all'` key.** That means the key routes,
`GET /api/permissions`, rule enable, disable and reload, and profile reload
(`require_permission(..., all_environments=True)`). Reads of configuration and the registry stay
open to scoped keys, because they change nothing.

**8. `POST /api/keys` refuses an environment name that secrets and deployments would not
accept.** The names are `CryptoService`'s list: `local`, `development`, `ephemeral`, `staging`,
`production`. `'all'` and no value are still accepted. The match is exact, so `Staging` is
refused.

**9. One 403 handler.** `EnvironmentNotAdmitted` is mapped to 403 once, in
`seedpod/api/factory.py`. A service can raise it at the point where it learns the environment.

## Consequences

- An `'all'` key sees and does everything it did before. The bootstrap admin key is `'all'`, so
  is every key made with no environment, and so is every key in the test suite's fixtures.
- A scoped key now sees less, and some requests it made before now get 403. A scoped key that
  was used for key management, rule changes or cross-environment work stops working for those
  things. That is the purpose of this decision.
- **The SPA's secrets page opens on `ephemeral`.** A key scoped to another environment gets a
  403 there until the user picks its own environment. The page does not know the key's
  environment.
- A list for a scoped key on snapshots, workflow runs or timers reads the cluster table as well.
  An `'all'` key skips that read.

## Not decided here

- **SSE progress events.** Engine notices (`workflow_progress`, `job_*`) have no environment, so
  DR-0010's rule sends them to every subscriber. A scoped key sees progress for runs in other
  environments. DR-0010 chose this, and it stays.
- **`GET /api/health/detailed` has no authentication.** It returns counts of clusters,
  deployments and API keys across all environments. This is older than this decision.
- **Two lists of environment names.** `config/deployment-rules.yml` lists `dev` in
  `valid_environments`. `CryptoService` does not know `dev`, so a rule that deploys to `dev`
  fails when it loads secrets. Decision 8 uses `CryptoService`'s list.
- **A scoped key cannot make keys, even for its own environment.**
