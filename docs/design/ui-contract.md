---
title: UI contract — v1 SPA consumption audit & v2 migration worklist
type: design
status: active
created: 2026-07-12
updated: 2026-08-09
---

# UI contract — SPA consumption audit & v2 migration worklist

Direction (DR-0002): **we own the UI, so the SPA adapts to the clean v2 contract — no compatibility shims, no server-side `display_status` synthesis.** This doc is (a) the exhaustive inventory of what the v1 SPA (`reference-code/seedpod/seedpod-ui/`, Preact + preact-router) actually consumes, (b) the binding obligations that inventory places on the v2 server, and (c) the SPA migration worklist. Produced by a full-source audit on 2026-07-12; all `file:line` references are into `seedpod-ui/src/`.

## Verdict up front

- **The design's re-fetch-on-reconnect assumption is VERIFIED.** The SPA never trusts the SSE stream as state: every data page re-fetches its REST endpoints on `reconnected` (ClusterList.jsx:71-76, ClusterDetail.jsx:240-246, DeploymentList.jsx:59-61, DeploymentDetail.jsx:91-93, JobsList.jsx:44-46, PodDetail.jsx:60-62, ContainerDetail.jsx:95-97). Domain events are used purely as refetch triggers; REST is always the store of record. Duplicate SSE on outbox crash-replay is therefore harmless, as the design assumed.
- **The migration is modest**: mostly S-sized field renames and gate rewrites, two M items (composite status display, `workflow_progress` adoption), one L (the Jobs page, whose backing endpoint disappears wholesale).

## Binding server obligations (Pillar builders: these are requirements, not suggestions)

1. **`deployment_status_changed` payloads MUST carry `deployment_id`** (plus `cluster_id`, `old_status`, `new_status`). DeploymentDetail.jsx:76 filters on it; if absent the page silently stops live-updating. The Seam A "v1-shaped payload" rule already implies this — hereby pinned explicitly (DR-0002).
2. **SSE keepalives ≤ 120s apart AND observable to the client.** The client's heartbeat monitor force-reconnects after 120s of silence (sse-client.js:157-175). Keep the `server_shutdown` message (client switches to a 15s reconnect delay). **The keepalive must be a `data:` frame, not an SSE comment** — amended 2026-08-09 after smoke 6. v1 (and v2 until then) sent the comment line `: keepalive`, which satisfies the cadence half of this obligation and defeats its purpose: `EventSource` discards comment frames before `onmessage`, where the client's `updateHeartbeat()` lives, so an idle connection force-reconnected every ~2 minutes (`[SSE] Heartbeat timeout (120002ms)`, observed live). v2 now sends a `keepalive` envelope via `SSEHub.envelope`, identical in shape to every broadcast (obligation 4). It is deliberately unsubscribed on the client — see `event-store.js`'s default topic list — so it refreshes liveness without entering the event buffer or the HUD.
3. **SSE auth stays query-param**: `GET /api/events/stream?token=<key>` — EventSource cannot set headers (sse-client.js:30). REST stays `Authorization: Bearer`.
4. **SSE envelope**: `{type, data:{...}, timestamp}` — pages unwrap `event.data || event` (ClusterDetail.jsx:167).
5. **Topics that must survive** (consumed by the v1 SPA, not in the design's kept-list — port or consciously drop with a DR): `pod_status_changed`, `snapshot_restore_completed`, `reconciliation_skipped`. **DISCHARGED 2026-08-10 by [DR-0035](../decisions/DR-0035-ui-contract-obligation-5.md)**, and the "UNMET" note this line carried until then was two-thirds wrong — a correction worth keeping, since it cost a scoping pass. The actual state, checked against source:
   - `reconciliation_skipped` — **was already emitted and tested** (`runtime/reconciliation.py:497`, environment-scoped per DR-0010; `tests/runtime/test_reconciliation.py:137`). Payload `{cluster_id, provider, reason}` matches what the SPA reads. No work needed; **kept**.
   - `snapshot_restore_completed` — **was already emitted and tested on the REST path** (`api/routers/snapshots.py:197`; `tests/api/test_features.py:333`). **Kept** for that path. The gap was the *workflow* path (`deploy.restore_snapshot` inside `deploy-waves.yml`), which emitted nothing; it now emits `ctx.progress` per attempt instead of a second terminal-sounding topic, because that step retries up to 19 times by design. `ClusterDetail` refetches restore history on `workflow_progress`, so the page behaves identically however the restore was triggered.
   - `pod_status_changed` — **consciously DROPPED** (DR-0035 decision 1). v2 deliberately replaced v1's per-deployment `watch_pods` SSE task with `deploy.await_wave`'s per-poll `ctx.progress` → `workflow_progress` (`deploy_apply.py:533`); the SPA was never told, so its pod pages had dead listeners and never refreshed. They now listen for `workflow_progress`, filtered on `cluster_id`. **Accepted limitation**: progress flows only during a workflow run, so pod churn on an idle ACTIVE cluster is not live. `KubeWatchPods` stays built and unused — it is what a future `runtime/` watcher would be built on if that limitation ever bites.

   Two defects found while discharging this, both now fixed: the HUD's `pod_status_changed` formatter read `data.status`, a field **neither** v1 nor v2 ever sent (v1's payload had `phase`) — the same conflation class as the `|| "updated"` bug; and `event-store.js` never subscribed to `workflow_progress` at all, though the HUD had a formatter case and listed it as verbose, so it could never enter the buffer.
6. **Endpoints/params that must survive at parity**: `POST /api/clusters/{id}/rehabilitate`; query params `show_destroyed`, `show_history`, `status=active`, `active_only`; `GET /api/registry/*`; `GET /api/config/*`; the presets/snapshots/secrets/keys surfaces as inventoried below.
7. **New endpoints/shapes (DR-0003)**: `GET /api/workflows` (runs, per the workflow_runs fields); `GET /api/timers` → `{timers: [{aggregate_type, aggregate_id, timer_key, fire_at}]}` ordered by `fire_at`, read-only; `/api/health/detailed` (moved from `/health` by DR-0042) replaces the `scheduler` block with `executor{running, pending_outbox, dead_outbox}`, `timers{running, next_fire_at}`, `engine{active_runs}` — `database`/`reconciler` blocks unchanged.

## 1 REST inventory

| Endpoint | Fields read (response) / sent (request) | v2 status | Required change |
|---|---|---|---|
| `GET /api/clusters` (`?show_destroyed=true` ClusterList.jsx:32; `?status=active` CreateSnapshotModal.jsx:28, RestoreSnapshotModal.jsx:38; bare, Login.jsx:24) | reads: `id, repository, branch, status, reconciliation_stale, dns_hostname, cluster_url, created_at, expires_at, slug`; CreateSnapshotModal.jsx:108 also `provider_config.deployment_profile` | CHANGED | `status` gets new value set (no creating/deploying); `?status=active` filter still valid. `provider_config.deployment_profile` is an undocumented nested read — verify it survives (at risk). |
| `GET /api/clusters/{id}` (ClusterDetail.jsx:48) | `id, status, expires_at, repository, branch, created_at, environment, last_reconciled_at, reconciliation_stale, public_ip, cluster_url, dns_hostname, slug` | CHANGED | `environment` → `origin` (managed\|discovered) for provenance (ClusterDetail.jsx:655-657); status set changes drive all gating below. |
| `DELETE /api/clusters/{id}?force&snapshot_before_destroy` (DestroyClusterModal.jsx:18-29) | sends query flags only | CHANGED | `force` gate keyed off `cluster.environment === "discovered"` (line 10) → `cluster.origin === "discovered"`. |
| `POST /api/clusters/{id}/extend` `{ttl_hours}` (ClusterDetail.jsx:310) | none read | UNCHANGED | none |
| `POST /api/clusters/{id}/rehabilitate` (ClusterDetail.jsx:557) | none read | UNCHANGED (obligation 6) | none |
| `GET /api/clusters/{id}/pods` (ClusterDetail.jsx:64) | `pods[]: name, status, ready ("2/2"), restarts, age, ip, image, namespace` | UNCHANGED | none |
| `GET /api/clusters/{id}/pods/{ns}/{pod}` (PodDetail.jsx:23, ContainerDetail.jsx:33) | `pod: status, namespace, age, node, ip, hostIP, labels, conditions[].{type,status}, containers[]/initContainers[]: {name, ready, state.{running,waiting.reason,terminated.{reason,exitCode}}, restarts, image, ports, env}` | UNCHANGED | none |
| `GET .../pods/{ns}/{pod}/logs?tail_lines&container&previous` (ContainerDetail.jsx:119) | `logs` | UNCHANGED | none |
| `GET /api/clusters/{id}/deployments` (ClusterDetail.jsx:76) | `deployments[]: deployment_id, status, manifest_version, deployed_by, deployed_at, error_message, services{}` | CHANGED | `error_message`→`failure_reason` (:765-768); `services`→`resolved_images` (:784-789); order-dependence: `deployments[0]` assumed latest — with `superseded_by` available, prefer explicit latest selection. |
| `GET /api/clusters/{id}/audit` (ClusterDetail.jsx:89) | `from_state, to_state, trigger, initiated_by, reason, timestamp, id` | CHANGED (values) | State names in audit rows will be the new set; StatusBadge renders them (:446, :451). NB v2 audits carry `actor`, not `trigger`/`initiated_by` — the API DTO must map or the SPA adapts (see worklist 12). |
| `GET /api/clusters/{id}/events?limit=200` (ClusterDetail.jsx:110) | `events[]: last_timestamp, type, reason, involved_object_kind, involved_object_name, namespace, message, count` | UNCHANGED | none |
| `GET /api/deployments?show_history=true` (DeploymentList.jsx:30) | `deployment_id, cluster_id, manifest_version, status, deployed_by, deployed_at` | CHANGED (values) | deployment status set gains `new, rejected`; keep `show_history` filter at parity. |
| `GET /api/deployments/{id}` (DeploymentDetail.jsx:36) | `deployment_id, status, cluster_id, manifest_version, deployed_at, deployed_by, error_message, services{svc:image}, audit_history[]: {triggering_repo, triggering_branch, commit_sha, deployment_profile_name, resolution_strategy, created_at}` | CHANGED | `error_message`→`failure_reason` (:353-355); `services`→`resolved_images` (:367-393); new fields `spec_ref`, `superseded_by` available (not yet displayed — add). |
| `POST /api/deployments/{id}/redeploy` (DeploymentDetail.jsx:116) | reads `result.deployment_id` | verify parity | none if kept |
| `POST /api/deployments/{id}/retrigger` (DeploymentDetail.jsx:138) | reads `result.new_deployment_id` | verify parity | none if kept |
| `POST /api/deployments/{id}/cancel` (DeploymentDetail.jsx:160) | none read | CHANGED (semantics) | Cancel no longer touches cluster state — update modal copy at :440 ("return the cluster to its previous state") and remove any cluster-state expectations. |
| `GET /api/presets`, `GET /api/presets/{id}` (PresetList.jsx:43, PresetDetail.jsx:28) | `id, name, description, profile_name, environment, default_branch, default_ttl_hours, service_overrides{svc:{tag}}, naming_strategy{type,name,pattern}, use_count, last_used_at, created_by, created_at` | parity intended, EXCEPT `naming_strategy` | Flag: `preset.environment` badge (PresetList.jsx:177, PresetDetail.jsx:149) — presets keep `environment` (deployment env, not the retired cluster sentinel); confirm. **`naming_strategy` is WITHDRAWN in v2 (DR-0038)**: slugs are derived deterministically and the slug is now the DNS record name, so a fixed name would collide across clusters from one preset. The column and this response field remain for existing rows; setting a non-null value is refused. v2's SPA never rendered it, so this row records v1's consumption, not an obligation. |
| `POST /api/presets`, `PUT /api/presets/{id}`, `DELETE /api/presets/{id}` (PresetList.jsx:99,134; PresetEditModal.jsx:49; PresetDetail.jsx:71) | sends name/description/profile_name/default_branch/default_ttl_hours/service_overrides/naming_strategy | parity intended, EXCEPT `naming_strategy` | A non-null `naming_strategy` is now a 422 (DR-0038). `null` is still accepted, so any client sending the key unset is unaffected. |
| `POST /api/presets/{id}/deploy` (PresetDeployModal.jsx:137) | sends `branch, provider_override, ttl_hours, cluster_name, data_initialization{restore_from_snapshot \| restore_from_latest{branch,profile,max_age_days}, services}`; reads `result.deployment_id` | parity intended | none; navigation to `/deployments/{deployment_id}` relies on an immediate deployment record (v2 `new` state fits). |
| `GET /api/registry/profiles`, `/{name}` (PresetList.jsx:57, PresetDetail.jsx:34,52) | `profiles[]: name, provider, services[]:{name, repository, port, external}` | parity intended | none |
| `GET /api/registry/providers` (PresetList.jsx:66, PresetDetail.jsx:44) | `providers[]: name, display_name` | parity intended | none |
| `GET /api/registry/tags/{repo}?limit=50` (TagPicker.jsx:28) | `tags[]: tag, size_bytes, pushed_at` | parity intended | none |
| `GET /api/snapshots` (`?branch&profile` SnapshotList.jsx:41-48; bare PresetDeployModal.jsx:71) | `id, name, is_auto, source_cluster_id, source_cluster_slug, branch, deployment_profile, total_size_bytes, created_by, created_at, services[]` | parity intended | none |
| `GET /api/snapshots/{id}` (SnapshotDetail.jsx:33) | plus `description, services[]: service_name, persistence_type, database, size_bytes` | parity intended | none |
| `POST /api/snapshots` (CreateSnapshotModal.jsx:59); `POST /api/snapshots/{id}/restore` `{cluster_id, services, run_migrations}` (RestoreSnapshotModal.jsx:101); `DELETE /api/snapshots/{id}` | — | parity intended | none |
| `GET /api/snapshots/clusters/{id}/restore-history` (ClusterDetail.jsx:99) | `id, snapshot_name, snapshot_id, snapshot_branch, status, services_completed, services_total, initiated_by, started_at` | parity intended | none |
| `GET /api/secrets?environment=`, `POST /api/secrets`, `GET /api/secrets/{env}/{key}/reveal` (reads `value`), `DELETE /api/secrets/{env}/{key}` (SecretList.jsx:34,55,70,107,125) | list reads `key_name, environment, key_class, created_at, updated_at` | parity intended | Secret env tabs `local/ephemeral/staging/production` (SecretList.jsx:212-217) are deployment envs, not cluster sentinels — safe. |
| `GET /api/keys?active_only=false`, `GET /api/keys/{id}`, `POST /api/keys` (reads `response.api_key`), `PATCH /api/keys/{id}` `{description,expires_at}`, `DELETE /api/keys/{id}` (ApiKeyList.jsx:31,59; ApiKeyDetail.jsx:32,49,60; CreateApiKey.jsx:69) | `id, username, environment, is_active, is_valid, last_used_at, expires_at, created_at, description, permissions{}` | parity intended | none |
| `GET /api/permissions` (CreateApiKey.jsx:29) | `permissions{}, categories{}` | parity intended | none |
| `GET /api/jobs` (JobsList.jsx:21) | `scheduled_jobs[]: name, id, next_run, trigger{type,interval_seconds}, metadata{cluster_id,correlation_id}`; `recent_executions[]: job_id, job_name, status, started_at, duration_ms, cluster_id, error`; `summary` | **GONE** | Rewrite against `GET /api/workflows` (workflow_runs: `id, workflow, cluster_id, deployment_id, status, failed_step, error, undo_incomplete, created_at/started_at/finished_at`). Scheduled-jobs half → `GET /api/timers` (DR-0003). |
| `GET /api/config/overview` (ConfigOverview.jsx:18) | `rules{version,total,enabled,disabled,global_ephemeral_enabled,enabled_rules[],disabled_rules[]}, deployment_profiles{total,profiles[]}, resolution_strategies{total,strategies[],default}` | parity intended | none |
| `GET /api/config/rules` (DeploymentRulesList.jsx:16, RuleDetail.jsx:21) | `status==="loaded", rules[]:{name,description,enabled,action,repo_patterns,branch_patterns,tag_pattern,config{ttl_hours,cluster_size,environment,deployment_profile,require_manual_approval}}, version, global_ephemeral_enabled, default_ttl_hours, error` | parity intended | none |
| `GET /api/config/deployment-profiles`, `/{name}` (DeploymentProfilesList.jsx:14, ProfileDetail.jsx:18) | `status==="success"`; list: map of `{version, environment_type, services[names], resolution_strategy}`; detail: `config{version, environment_type, resolution_strategy, services[]:{name,repository,port,replicas,image}, cluster{cpu,memory,region}}` | parity intended | `environment_type` is profile config, untouched by the origin refactor. |
| `GET /api/config/resolution-strategies`, `/{name}` (ResolutionStrategiesList.jsx:14, StrategyDetail.jsx:17) | `status, strategies{}: name, description, fallback_branches, require_triggering_repo, allow_external_fallback, explanation` | parity intended | none |
| `POST /api/rules/reload`, `POST /api/deployment-profiles/reload` (ConfigOverview.jsx:37,55,78-79) | `status, summary{...}`; `status, deployment_profiles_count` | parity intended | none |
| `GET /api/health/detailed` (Health.jsx:35; moved from `/health` by DR-0042) — **polled every 5s** (Health.jsx:15; the only polling loop in the app) | `status==="healthy", service, version, timestamp, database{connected,cluster_count,deployment_count,api_key_count}, scheduler{running,job_count}, reconciler{running,last_sync}` | CHANGED (shape) | `scheduler` block replaced per DR-0003 (obligation 7); Health.jsx:138-155 adapts. |
| `GET /api/events/stream?token=` (sse-client.js:30, EventSource) | see §2 | CHANGED (new topic) | see §2 |

## 2 SSE pipeline

**Mechanics.** `sse-client.js` is a singleton wrapping one `EventSource` on `/api/events/stream?token=<apikey>`. All messages arrive on the default `onmessage` channel as JSON with a `type` field; the client re-emits by `data.type` to listeners (sse-client.js:63-80). Reconnect: exponential backoff 1s→30s cap, infinite attempts (:83-105); a `server_shutdown` message switches to a fixed 15s delay (:9, :72-75, :92-93). A heartbeat monitor forces close+reconnect after 120s of silence, checked every 30s (:157-175). Synthetic client-side events: `connected`, `disconnected`, `error`, `reconnected` (:41-44). `event-store.js` is a bounded (100) newest-first buffer feeding only the MiniEventHud and ConnectionStatus pulse — **no state reconstruction anywhere**.

| Event type | Where subscribed | Payload fields read | Effect | v2 status |
|---|---|---|---|---|
| `cluster_state_changed` | ClusterList.jsx:85 (ignores payload), ClusterDetail.jsx:258 (`cluster_id` match :173), event-store, MiniEventHud.jsx:130 (`cluster_id, old_status, new_status`) | cluster_id, old_status, new_status | refetch clusters / all cluster-detail data | UNCHANGED topic; new status values flow through display only |
| `deployment_status_changed` | ClusterDetail.jsx:259 (`cluster_id` :199), DeploymentList.jsx:69, DeploymentDetail.jsx:101 (`deployment_id` :76), MiniEventHud.jsx:133 (`deployment_id, old_status, new_status`) | cluster_id, deployment_id, old_status, new_status | refetch deployments (+cluster, pods, events on ClusterDetail) | UNCHANGED topic; **`deployment_id` in payload is a pinned server obligation (above)**. This row used to say the hud read a bare `status` — it did, and that was the bug: the payload has only `old_status`/`new_status` (obligation 1), so the hud's `\|\| "updated"` fallback fired on every event. Corrected against the wire during smoke 6 (2026-08-09). |
| `pod_status_changed` | ClusterDetail.jsx:260, PodDetail.jsx:64, ContainerDetail.jsx:99, MiniEventHud.jsx:151 | cluster_id, pod_name, status | refetch pods / pod detail | obligation 5 — must survive |
| `snapshot_restore_completed` | ClusterDetail.jsx:261 (`cluster_id` :232), MiniEventHud.jsx:154 (`cluster_id, status, services_restored[]`) | as listed | refetch restore history | obligation 5 |
| `job_started` / `job_completed` / `job_failed` | JobsList.jsx:55-57 (ignores payload), event-store, MiniEventHud.jsx:136-142 (`job_type \|\| job_name`, `cluster_id`, `error`) | job_type/job_name, cluster_id, error | refetch /api/jobs; HUD line | CHANGED payload: now `{workflow, cluster_id, run_id}` — HUD reads switch to `workflow`; JobsList refetch target becomes /api/workflows |
| `workflow_progress` | — (does not exist in v1) | — | — | NEW — no v1 consumer; adopt per worklist 9 |
| `server_shutdown` | sse-client.js:72, event-store, MiniEventHud.jsx:144 (`message`) | message | 15s reconnect delay | keep at parity |
| `reconciliation_skipped` | event-store, MiniEventHud.jsx:157 (`cluster_id, provider`) | as listed | HUD only | obligation 5 |
| `connected`/`disconnected`/`reconnected` | all pages + Health.jsx:22-23, ConnectionStatus.jsx:40-41 | (synthetic) | refetch / indicator | client-side, unchanged |

## 3 Status-literal coupling

| file:line | Literal(s) | v2 disposition |
|---|---|---|
| StatusBadge.jsx:8 | `PENDING` | keep (deployment `pending`, workflow `pending`) |
| StatusBadge.jsx:9 | `PROVISIONING` | keep (cluster) |
| StatusBadge.jsx:10 | `CREATING` | **GONE** — delete |
| StatusBadge.jsx:11 | `READY` | GONE as cluster state — delete (unless pod use) |
| StatusBadge.jsx:12 | `DEPLOYING` | keep, deployment-only now |
| StatusBadge.jsx:13 | `CANCELLING` | GONE (deployment set has `cancelled`, not `cancelling`) — delete |
| StatusBadge.jsx:14-16 | `ACTIVE, RUNNING, SUCCESS` | ACTIVE keep; RUNNING keep (workflow/pod); SUCCESS → `SUCCEEDED` (workflow) |
| StatusBadge.jsx:17-18 | `DESTROYING, DESTROY_SCHEDULED` | keep; **pre-existing bug**: `"destroy-scheduled".toUpperCase()` = `DESTROY-SCHEDULED` (hyphen) never matches the underscore keys at :18/:23 — falls to muted. Fix normalization in v2. |
| StatusBadge.jsx:21-24 | `FAILED, DEPLOY_FAILED, DESTROY_FAILED, CANCELLED` | FAILED/CANCELLED keep; DEPLOY_FAILED gone (delete); DESTROY_FAILED keep (fix hyphen bug) |
| StatusBadge.jsx:27-30 | `SUPERSEDED, DESTROYED, ZOMBIE, UNMANAGED` | keep; add `NEW`, `REJECTED`, `BLOCKED`, `COMPENSATING` |
| ClusterDetail.jsx:284, 295, 699, 823, 839 | `["active","deploying"].includes(cluster.status)` / `=== "deploying"` | **GONE**: cluster never `deploying`. Reduce to `active`; compose with latest deployment.status for "deploy in progress" affordances |
| ClusterDetail.jsx:540-544 | cluster `destroying, destroyed, zombie, unmanaged` (action disable) | values keep; consider adding `destroy-scheduled`, `destroy-failed`, `failed` to the gate |
| ClusterDetail.jsx:547 | cluster `active` (canSnapshot) | UNCHANGED |
| ClusterDetail.jsx:550 | cluster `destroyed, destroy-failed, zombie` (canRehabilitate) | UNCHANGED |
| ClusterDetail.jsx:717 | deployment `["pending","deploying","active"]` (current-deployment card) | add `new` |
| DeploymentDetail.jsx:242-248 | deployment `active` / `failed` / `deploying` (border color) | UNCHANGED |
| DeploymentDetail.jsx:253-254 | deployment `pending \|\| deploying` (Cancel button) | add `new`; update copy per new cancel semantics |
| DestroyClusterModal.jsx:10 | `cluster.environment === "discovered"` | **CHANGED** → `cluster.origin === "discovered"` |
| JobsList.jsx:215, 247-248, 256, 281, 326 | job execution `running` | endpoint GONE; workflow status `running` exists in replacement |
| ApiKeyList.jsx:95-97, ApiKeyDetail.jsx:99-101; RuleDetail.jsx:110, DeploymentRulesList.jsx:180 | cosmetic badge labels | unchanged |
| MiniEventHud.jsx:34-39, 47-48 | verbose filter on `job_*`, `pod_status_changed` type names | topic names kept; add `workflow_progress` to the verbose list |

## 4 Jobs/progress

- Sole consumer: `pages/JobsList.jsx` — `GET /api/jobs` (:21) reading `scheduled_jobs`, `recent_executions`, `summary`; SSE `job_*` (:55-57) used only to refetch. Entire surface is **GONE** → rebuild on `/api/workflows`. Column map: `job_name`→`workflow`, `duration_ms`→derive from started/finished timestamps, `error`→`error`; new columns available: `failed_step`, `undo_incomplete`, `deployment_id`.
- Per-job progress: **no v1 consumer exists** — nothing subscribes to bespoke progress events. `workflow_progress` is net-new capability; natural insertion points are the static in-progress banners (DeploymentDetail.jsx:358-364, ClusterDetail.jsx:772-782) and MiniEventHud.
- Health page reads `health.scheduler{running, job_count}` (Health.jsx:138-155) — adapts to the DR-0003 blocks (obligation 7).

## 5 Auth

- REST: `Authorization: Bearer` on every call (api-client.js:34-36); token in `localStorage["auth_token"]` (:10-24). 401 → token cleared, error thrown; no redirect (:43-47).
- SSE: query-param token (obligation 3).
- No dedicated auth endpoints: Login "validates" by calling `GET /api/clusters` (Login.jsx:24); no user-info endpoint (app.jsx:63 hardcodes the username).

## 6 Migration worklist (ordered)

1. **(M) Composite cluster×deployment status display** — replace every `cluster.status === "deploying"` / `["active","deploying"]` gate (ClusterDetail.jsx:284, 295, 699, 823, 839) with `cluster.status === "active"` plus a derived "deploy in progress" flag from the latest deployment (`new|pending|deploying`); add a composed status line to ClusterDetail header and the ClusterList status column. ⚠ **Riskiest #1** — every action-gate and tab-gate must be re-derived.
2. **(L) Rewrite JobsList → Workflows page** — runs tab from `GET /api/workflows` (new columns: `workflow, status, failed_step, error, undo_incomplete`, duration from timestamps); schedules tab from `GET /api/timers` (DR-0003); keep `job_*` SSE as refetch triggers. ⚠ **Riskiest #2** — only page whose backing endpoint disappears wholesale.
3. **(S) StatusBadge map overhaul** (StatusBadge.jsx:6-35) — drop `CREATING, READY, CANCELLING, DEPLOY_FAILED`; add `NEW, REJECTED, BLOCKED, COMPENSATING, SUCCEEDED`; **normalize hyphens** (fixes the pre-existing destroy-scheduled/destroy-failed styling bug).
4. **(S) `error_message` → `failure_reason`** — DeploymentDetail.jsx:353-355; ClusterDetail.jsx:765-768.
5. **(S) `deployment.services` → `resolved_images`** — DeploymentDetail.jsx:367-393; ClusterDetail.jsx:784-799.
6. **(S) `environment` → `origin`** — DestroyClusterModal.jsx:10 gate; ClusterDetail.jsx:655-657 info row.
7. **(S) Deployment cancel semantics** — DeploymentDetail.jsx:269, :440 copy no longer claims cluster rollback; extend Cancel gate to `new` (:253-254).
8. **(–) `deployment_status_changed` carries `deployment_id`** — resolved as server obligation 1 (was riskiest #3; now pinned server-side, no SPA change).
9. **(M) Consume `workflow_progress`** — replace static banners (DeploymentDetail.jsx:358-364, ClusterDetail.jsx:772-782) with live `message`/`step_path`/`attempt`; add HUD case (MiniEventHud.jsx:128-179) and verbose-filter entry (:34-39).
10. **(S) MiniEventHud job payload reads** — `job_type||job_name` → `workflow` (:136-142).
11. **(S) Surface new deployment fields** — `spec_ref`, `superseded_by` on DeploymentDetail; use `superseded_by` instead of positional `deployments[0]` for "current" on ClusterDetail (:716-719).
12. **(S) API DTO decisions during Pillar build** — audit rows expose `actor` (v2) where the SPA reads `trigger`/`initiated_by`: either DTO-map or update ClusterDetail.jsx:446-451; verify `cluster.provider_config.deployment_profile` (CreateSnapshotModal.jsx:108) and the redeploy/retrigger endpoints port.
13. **(–) Reconnect handling: no change** — re-fetch-on-reconnect is already universal (§2); keep server keepalives per obligation 2.
14. **(S) List-endpoint envelope (DR-0017)** — every v2 collection endpoint returns `{<resource>: [...]}`, never a bare top-level array (the only envelope consistent with the necessarily-wrapped multi-field responses and the DR-0003 endpoints). Every SPA list consumer that read a bare array adapts `data` → `data.<resource>`: `GET /api/deployments` (DeploymentList.jsx:31,146 → `data.deployments`), `GET /api/clusters` (ClusterList.jsx → `data.clusters`), and any other list read bare in the §1 inventory.

## Open questions — RESOLVED (DR-0003, 2026-07-12)

1. **Scheduled-jobs tab** → the `timers` table is exposed via read-only `GET /api/timers` (obligation 7); the tab becomes a schedules view (`fire_at`, `timer_key`, `aggregate_id`). Periodic loops (reconciler, health poll) are not timers; their liveness lives in `/api/health/detailed`.
2. **`/api/health/detailed`** → the proposed shape adopted, plus `dead_outbox` in the executor block (the one number that says "reconciliation has inherited work"). Full shape in obligation 7 and DR-0003.
