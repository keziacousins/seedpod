---
title: Parity backlog — from parity-gate-green to a smoke-tested, runnable v2
type: guide
status: active
created: 2026-07-18
updated: 2026-08-16
---

# Parity backlog — closing the gap to a runnable, smoke-tested v2

Six rounds plus the SPA migration are committed (`d014a29`) and the acceptance parity gate is
**green**. But the gate proves only the **in-process deployment *decision* flow** (webhook → rule →
manifest resolve → cluster/deployment birth → response contract) against `FakeProvider` — it does
not prove that v2 (a) can be *started* at all, or (b) provisions real infrastructure end-to-end.
This doc is the complete outstanding tracker to close both gaps.

**On the citations in here.** This repository is published as a single commit, so the short
SHAs below (`d014a29`, `b565f4c`, …) do not resolve in it. They are provenance from the
development history, like the `reference-code/…` paths — see the README. Deployment profiles,
manifest templates and hostnames are the generic `exampleco` examples that ship in `config/`;
the real ones are private, and IP addresses are documentation-range placeholders.

**Not debt:** the deliberate v1-bug-not-pinned improvements (providers §5.7.4, `rules` config
swallow, `kubectl` blocking-wait removal, crypto). **Already done:** SSE keepalives (obligation 2,
events router), real snapshots (kubectl `pg_dump -Fc`), the full SPA §6 migration (14 items,
build-verified). Sizes are rough.

---

## §0 — Runtime entry point & CLI (DR-0021) — **DONE** (committed `b565f4c`, 2026-07-20)

All three entry points are built, adversarially reviewed, and green (full suite passes, ruff clean;
three console scripts resolve). **v2 is now runnable.** Built via the `round7-entrypoint-cli`
workflow (build → mechanical check → spec-fidelity judge + a trust-boundary judge on the two CLIs →
fix); the judges caught a real graceful-shutdown regression and added PID-file coverage. Detail of
each surface below is retained for reference. Per **DR-0021**, three entry points by trust model:

0a. **Server runner** — `python -m seedpod` (`__main__.py`): `build_app(AppConfig.from_env())` +
    `uvicorn.run(app.api)`, with `App.start()/stop()` wired into the ASGI lifespan; `start.py`
    keeps `load_dotenv`/PID-file/log-rotation. **Already specified** (seam-d Decision 8) — build
    it. *Size: S.*
0b. **Bootstrap CLI** — `seedpod-bootstrap`, offline/on-disk only: `generate-keys`, `migrate`,
    `create-admin <username>` (mint the first API key directly). The only direct-DB tool; solves
    cold-start. *Size: S–M.* — `migrate` = apply v2's schema to a **cold** DB; there is **no**
    v1→v2 data-migration path and none is wanted (Kezia, 2026-07-19 — v2 cold-starts fresh; DR-0021 §0b).
0c. **User CLI** — `seedpodctl`, an authenticated HTTP client over the same API the SPA uses
    (keys/secrets/clusters/deployments/deploy/snapshots/presets/workflows/timers/health/config).
    No direct DB. *Size: M.*

**This is the prerequisite for everything downstream — the smoke test cannot run until v2 starts.**

## P0 — provisioning critical path (blocks a real end-to-end deploy)

> **First real smoke run, 2026-07-20 (DigitalOcean, `exampleco-web-2` @ `feature/testing-deploys`).**
> v2 booted, matched the rule, resolved `environment=ephemeral`, and **birthed a real cluster +
> deployment** — then stopped dead. Findings, in priority order: **#0 (the verb catalog) is the
> real blocker and it dwarfs the rest.** Nothing was provisioned; **0 droplets created, no spend.**

> **Second real smoke run, 2026-08-02 (same repo/branch, cold DB, after Round 8a + its gate fixes).**
> **PROVISIONING WORKS END TO END.** All 11 `provision-digitalocean.yml` steps succeeded on the
> first attempt — cluster `ACTIVE` in **185s**, real droplet `589450319` @ `203.0.113.12`,
> k3s serving, `coredns` Running, kubeconfig Fernet-encrypted at rest (4024B, key class `DEV`) and
> decryptable by the API (`clusters pods` returned real pods). Verified live: TOFU captured three
> real host keys and threaded them structurally (crown jewel #2); DR-0023's SSH identity resolved
> `root` / `id_exampleco_testing` from provider config; the droplet gate polled at **20.8s / 20.7s**
> (the m-3 fix) with no 30s settle (m-4); per-cluster CIDR allocation visible in pod IP `10.42.213.4`.
> **P0 #1 (`provider_config` synthesis) is resolved** — `cluster.load_spec` builds the spec at
> provision time. **Still open:** #0c/#3 (`'environment_variables' is undefined` — deployment
> `rejected`, cluster provisioned anyway, exactly the designed degradation), and #0's sub-finding,
> which **recurred on the destroy path**: `destroy-cloud.yml` needs 5 unbuilt verbs, the engine task
> died on `UnknownVerbError('cluster.load_infra')`, and the cluster stranded in `destroying` with no
> API path out. **The droplet had to be deleted via DigitalOcean's own API.** Cost: one 1vCPU/2GB
> droplet for ~7 minutes. Two region-scoped `seedpod-{mgmt,apps}-ams3` firewalls remain by design
> (v1's per-region model, reused across clusters, no cost).

> **Third real smoke run, 2026-08-03 (after Round 8b's destroy half).** **THE FULL
> LIFECYCLE WORKS.** provision -> ACTIVE -> destroy -> DESTROYED, end to end through
> seedpod with no manual intervention: provision 11/11 steps in 174.7s, destroy 5/5 in
> 21.1s, 0 droplets left, cluster row `destroyed`. This closes the 2026-08-02 smoke's
> worst finding (a destroy stranded the cluster in `destroying` and the droplet had to
> be deleted through DigitalOcean's own API).
>
> Two things the run found that tests had not:
>
> 1. **A real bug in `infra.destroy_instance`, fixed (`74d5a28`).** DO's delete is
>    ASYNCHRONOUS -- the droplet keeps reporting `status: active` for seconds after a
>    successful delete, and DO's probe maps active -> DESTROY_FAILED. The step treated
>    that as terminal and raised 2.2s into a destroy that then completed normally,
>    marking the cluster DESTROY_FAILED (the droplet did die; no billing leak). The
>    same status means different things on the two paths: terminal from the INITIATE
>    call, "still in flight" from a PROBE, where the gate's own `timeout_seconds: 900`
>    is what adjudicates. A regression test names the incident.
> 2. **Resume works on real infrastructure.** Booting against the previous smoke's DB
>    re-adopted its stranded `destroy-cloud` run and carried it three steps further --
>    the verbs simply had not existed before. Reconciliation also logged
>    `suppressing OrphanIntent -- run in flight`, correctly deferring to the live run.
>    All three clusters from all three smokes now read `destroyed`.
>
> Still open, unchanged by this run: #0c/#3 (`'environment_variables' is undefined`,
> so the deployment is still `rejected` while the cluster provisions fine) and the
> 7 deploy-path verbs. Cost: two droplets for ~10 minutes total.

> **Fourth real smoke run, 2026-08-08/09 (after Rounds 9 + 10).** **A REAL DEPLOYMENT
> LANDED ON A REAL CLUSTER.** provision -> ACTIVE (242.3s) -> `deploy-waves` (57.7s) ->
> deployment `active` -> destroy -> `destroyed`, **0 droplets left**. Cost: one
> 1vCPU/2GB droplet for ~8 minutes, plus two failed creates the night before.
>
> **The 2026-08-08 attempt was blocked by an upstream DigitalOcean outage**, not by v2
> (status incident `2wql4f4sb13r`, from 18:57 UTC: "customers are unable to create
> Droplets"). Two runs died at `infra.create_instance` -- one on a 504, one on a create
> that hung past the step's 60s budget. **0 droplets leaked both times**: the C1
> "tolerant of never created" undo did its cluster-uuid TAG lookup and correctly
> concluded nothing existed. Both clusters went to `failed` cleanly, with no strand --
> the failure mode smokes 1 and 2 hit. Re-run 2026-08-09 once DO was operational again.
>
> The three first-time exercises, and what each actually showed:
>
> 1. **Wave ordering (DR-0029) -- CORRECT, verified from the step history by
>    timestamp, not from a green result.** `deploy.plan_waves` produced wave **0** =
>    `Secret/ghcr-secret` alone (the unmatched doc, exactly where DR-0029 sends it) and
>    wave **3** = all 8 application-tier docs. `wave[0].apply` finished 07:16:33.934;
>    `wave[1].apply` started 07:16:35.342 -- the pull secret existed **before** the
>    Deployment that needs it. Also established: **wave indices are NOT compacted** --
>    the foreach step path is `wave[1]` (loop position) while the `Wave.index` stays
>    `3` (declared rank). LOUD CAVEAT: this profile (`exampleco-misc`) has **zero** init
>    containers, so nothing could mask the result -- but equally, **the #0d masking
>    risk lives entirely in `exampleco-stack`, which this smoke never touched**. Wave 0 vs
>    application tier is proven; the full datastore -> migration -> app ordering is NOT.
> 2. **The GHCR pull secret (Round 9, #4) -- CONFIRMED on real infrastructure.**
>    `exampleco-web-2` reached **Running 1/1, 0 restarts** pulling the private
>    `ghcr.io/exampleco/exampleco-web-2:feature-testing-deploys-5926924`. This is
>    precisely the ImagePullBackOff that used to kill this path.
> 3. **`ensure_rollouts`' restart rule -- STILL UNPROVEN**, blocked by the defect
>    below. Both waves reported every resource `created`, never `unchanged`, so the
>    restart branch never ran.
>
> Two things the run found that the 2192-test suite could not:
>
> 1. **A REDEPLOY TO AN ALREADY-ACTIVE CLUSTER STRANDS IN `pending` FOREVER. NOT
>    FIXED -- needs a DR (see #13 below).** The redeploy created deployment
>    `64db05b5` and **zero workflow runs**. `version_update` correctly reuses the
>    ACTIVE cluster, but `PENDING -> DEPLOYING` is driven ONLY by `ClusterReady`,
>    emitted from exactly one place (`core/machine.py:314`, the
>    `provisioning x ProvisionSucceeded` cascade). An already-ACTIVE cluster never
>    re-emits it. `runtime/dispatcher.py`'s own docstring names the intended mechanism
>    -- "API `DeployRequested`+`ClusterReady` chains" -- and `apply()` carries the
>    optional `tx=` for exactly that chaining, but `deployment_service` never does it.
>    **This blocks `ensure_rollouts` from ever being provable**, and it breaks the
>    endpoint's own headline use case (it is called `version-update`).
>    *Why the suite is green:* `test_reuses_existing_active_cluster_for_same_repo_
>    branch_environment` asserts the cluster is reused and the deployment id differs --
>    **never that the second deployment deploys**. It pins the routing decision, not
>    its consequence.
> 2. **A failed step could persist an EMPTY error message, fixed.** The engine
>    recorded `str(exc)` verbatim, and a bare `TimeoutError` (what `asyncio.timeout`
>    raises on a step's own `timeout_seconds` expiry) stringifies to `""` -- so both
>    DO-outage failures landed as `{"kind": "transient", "message": ""}`, and the run
>    history could not distinguish an upstream outage from a v2 defect, which is
>    exactly the question a smoke exists to answer. `_failure_message()` in
>    `seedpod/engine/engine.py` (4 call sites) now never persists empty text; a
>    regression test names the incident and was verified failing pre-fix.
>
> **Two non-defects confirmed**, both worth not re-investigating: (a) reconciliation
> **auto-reaps stale `failed` clusters on boot** -- it destroyed both DO-outage
> clusters, and the `ClusterGone` cascade correctly moved their `pending` deployments
> to `destroyed`, so "a provision failure leaves the deployment pending" is a correct
> waiting state, not a leak; (b) a cold DB needs a **`tailscale_auth_key` secret**
> seeded or manifest resolution raises -- that raise is DESIGNED (v1 silently rendered
> an empty string; `services/manifests.py:103` plus a test pin it). See the cold-start
> note under §12.

> **Fifth real smoke run, 2026-08-09 — `tart` (local VM-k3s), on `minimax`.** **THE FULL
> LIFECYCLE WORKS ON TART, AND IT IS FAST.** provision **26.9s** -> `deploy-waves` **47.6s** ->
> deployment `active` -> **redeploy** -> destroy -> `destroyed`, **0 VMs left**. For comparison,
> the same profile on DigitalOcean takes **242.3s** to provision — tart is ~9x faster, which is
> exactly the developer-inner-loop case it exists for. Both providers are production targets (DO
> for shared infra, tart for developers running a full stack locally).
>
> Run on `minimax` (seedpod must run ON the VM host), from a checkout at `1933b46` with a
> tart-only `.env`: **fresh Fernet keys generated on the host** so no key material crossed
> machines, and `GITHUB_TOKEN` as the only secret copied (no DO or Cloudflare credentials).
>
> **Three things proven for the first time:**
>
> 1. **The tart provider works end to end.** `check_ready()` correctly gates startup on the binary
>    plus the `local-dev-base-rosetta` base image; VM clone is copy-on-write (~1s vs DO's ~40s
>    droplet create) with a NAT IP in ~4s; `destroy` honours `cleanup.delete_on_destroy` and left
>    nothing behind.
> 2. **Rosetta works.** `exampleco-web-2` — an **amd64/linux** image — reached `Running 1/1, 0 restarts`
>    inside an Apple-Silicon VM, which is the entire reason the `-rosetta` base image exists.
>    Traefik also came up (it never got that far inside DO's 8-minute window).
> 3. **`deploy.ensure_rollouts`' RESTART branch finally fired** — the last unverified behavior from
>    Round 10. Evidence, not inference: `kubectl.kubernetes.io/restartedAt: 2026-08-09T11:29:04`
>    (an annotation only `kubectl rollout restart` writes), deployment `generation: 2`, and the old
>    ReplicaSet scaled to 0 while a new one took over. **This also validates #14's fix on real
>    infrastructure**: wave 1 reported **all 8 resources `unchanged`**, including the two
>    (`secret/tailscale-auth`, `daemonset.apps/tailscale`) that previously reported `configured`
>    forever. Note the dependency chain — **DR-0031** made a redeploy possible at all, **#14** made
>    all-`unchanged` achievable, and only then could the restart branch be reached.
>
> DR-0031's supersession also works here (`773580b5` -> superseded by `e2b4fc94`).
>
> **The one real finding is environmental, and it is a trap worth knowing — see #15.** The first
> attempt failed at the `k3s.await_ssh` gate (180s timeout) and cost a long investigation. It was
> **not** the timeout, the SSH key, or the probe: macOS 15's **Local Network Privacy** denies
> `192.168.65.x` access (errno 65, `EHOSTUNREACH`) to a seedpod process that has been reparented
> away from a live login session. Launching seedpod with `nohup`/`ssh -f` so the session ends is
> enough to trigger it. Proven by a side-by-side A/B at the same second, same host, same IP:
> in-session `connect_ex=0 (OPEN)`, reparented `connect_ex=65 (BLOCKED)`. Re-running with seedpod
> as a child of a live session made the identical 180s gate pass first time.

0. **THE STEP VERB CATALOG — DONE, ALL 30 VERBS** (Round 10, committed `099fdcd`, 2026-08-08).
   *Size: was L.* **All 8 shipped workflows now validate against the REAL composition-root registry**,
   and DR-0022 Erratum E11's completeness gate has been flipped from `xfail` to a **hard assertion**
   (`test_registry_verb_set_is_exactly_the_dr_0022_catalog`) — it is now the test that notices if a
   verb silently drops out of `_build_step_registry`, which the subset check above it would not.

   Round 10 delivered the 7 deploy-path verbs plus the 5 DTOs, and grew twice beyond that scope,
   deliberately and with a DR each time — read `DR-0029` and `DR-0030` before assuming this round was
   only "the deploy half":
   - **DR-0028** — the five DTOs. A diagnose-first audit before any agent ran found **four of the five
     stand-ins were wrong**, one worse than Round 8b's `DnsRecordRef`.
   - **DR-0029** — **wave orchestration is BUILT**, realising `reference-code/PLAN-wave-orchestration.md`,
     a v1 plan that was never implemented. `seam-b-engine.md:214-218` had specified it as though v1 had
     it; `deploy_wave` had zero occurrences in v1's code or config. Profiles gained `deploy_wave`.
   - **DR-0030** — `SnapshotService.restore` stops conflating `InfrastructureUnreachableError` with a
     definitive failure, and regains v1's dropped pre-flight compatibility check.
   - **DR-0025 Erratum E2** — deferred manifest rendering, closing a contradiction between DR-0025's
     own parts 1 and 2 (part 1 rejected a `provider_host` deployment before any audit existed, so
     part 2 could never run).

   Historical detail of the catalog's origin retained below.

   **Vocabulary ratified 2026-07-20 by DR-0022** (supersedes DR-0004): 35 organically-grown
   verbs re-normalized to **30** (Erratum E1 — the DR's prose said 31; its table and the shipped YAMLs
   both say 30), with late-bound `infra.*` verbs, two correctness fixes (`kube.apply_docs` is
   `undoable=False`, closing a latent deploy-time data-loss path; `kube.delete_daemonset` becomes a
   gate, restoring Seam C's "no command waits" law), and `cluster.load_infra` superseding the dispatch
   table's DNS hook.

   **23 of 30 verbs are registered** (`app/factory.py:_build_step_registry()`), and **5 of the 8
   shipped workflows now validate against the REAL registry**, asserted rather than assumed by
   `tests/engine/test_verb_conventions.py`'s coverage-boundary test:
   `provision-{digitalocean,tart,kind,orbstack}.yml`, `destroy-cloud.yml`, `destroy-shared.yml`,
   `deploy-rollback.yml`. Round 8a built the 14 provision-path verbs (smoke 2 provisioned for real);
   Round 8b built the 9 destroy-path verbs (smoke 3 ran the **full lifecycle** — see above).

   **What remains: the 7 deploy-path verbs** — `deploy.load_audit`, `deploy.plan_waves`,
   `deploy.prepare_wave`, `deploy.restore_snapshot`, `deploy.ensure_rollouts`, `deploy.await_wave`,
   `kube.apply_docs`. They were deliberately deferred on two fronts, the second of which Round 9
   (below) has now closed:

   - **Five DTOs they bind exist only as fixture stand-ins** in `tests/engine/declared_verbs.py`:
     `ManifestDoc`, `DeploymentProfile`, `Wave`, `ApplyChangeSummary`, `SnapshotRestoreSpec`. Each
     must land as a real type (and be registered in `engine/registry.py`'s `NAMED_TYPES`, which the
     real-registry validation enforces). Round 8b's `DnsRecordRef` is the worked example — and a
     cautionary one: the stand-in's shape was WRONG (missing the `record_id` the DNS service is keyed
     on), so treat each stand-in as a hypothesis to check against v1, never as a spec. **Still open —
     this is the whole of Round 10.**
   - **Items #0c/#2/#3/#4 below** — **CLOSED, Round 9.** Manifest resolution used to fail at
     `'environment_variables' is undefined`; it no longer does (real `exampleco-web-2`/`tailscale`
     profiles render to valid Kubernetes YAML end to end — see those items). The deploy catalog can
     now be built against real resolved manifests instead of invented shapes once Round 10 lands the
     DTOs above.

   **Sub-finding, still open:** when an engine task dies on an unregistered verb it is never marked
   failed (`Task exception was never retrieved`), stranding the cluster with **no API path out**.
   Seen twice (smoke 1 stranded `provisioning`, smoke 2 stranded `destroying`). Narrower now that the
   catalog is mostly built, but not fixed — a dying run must fail its cluster.

0a-i. **GOTCHA 1 IS NOT PRESERVED — a cluster whose provider was disabled cannot be destroyed.**
   *Size: S, needs a DR.* v1 resolved the destroy provider with `check_enabled=False` precisely
   "because we need to destroy clusters even if provider is now disabled"
   (`reference-code/.../jobs/state/destruction_job.py`), and both destroy workflows still carry that
   comment on the `infra.destroy_instance` step. v2 cannot honour it: `load_enabled_providers` makes a
   disabled provider **absent** from the mapping ("disabled = absent … no `ProviderDisabledError` type
   exists in v2", Decision 8 step 5), and `ctx.services.providers` is the only route a late-bound verb
   has. Closing it is a design change — destroy resolving against an all-providers mapping, or
   providers carrying an enabled flag instead of being omitted — so it needs a DR, not a patch.
   Meanwhile `InfraDestroyInstance._resolve_provider` fails with a message naming the actual cause
   rather than a bare `KeyError`, and a test pins that.

0b. **v2 never reads `config/org.yml`** — **CLOSED, Round 9 (the org-and-ghcr component).**
   `app/factory.py`'s `_resolve_github_organization` now reads `organization.github_organization`
   from `config/org.yml` as the default, with `GITHUB_ORGANIZATION` still winning when set (12-factor
   precedence). Did BOTH halves the backlog offered as alternatives: ported the file read, AND made a
   still-empty org (after checking both sources) a loud `MissingGithubOrganization` failure at
   `build_app()` composition-root time — gated strictly inside `if config.github_token:`, so the
   existing no-token degradation path (DR-0015, an acceptance test depends on it) is untouched.
   Proven at the actual downstream symptom: a real GHCR image-URL lookup for `exampleco-web-2` now
   produces `ghcr.io/exampleco/exampleco-web-2:...`, never the double-slash `ghcr.io//exampleco-web-2:...`
   (`tests/app/test_factory.py`).

0c. **Manifest templates need config v2 does not build** — **CLOSED, Round 9 (the resolved-config
   component).** With the org set (#0b), resolution's first concrete error was `manifest resolution
   failed: 'environment_variables' is undefined` — that was items 2 and 3 below, now BOTH closed;
   see those entries for the mechanism. The degradation path itself worked correctly throughout, per
   DR/Round 6 and unchanged here: HTTP 200 with a `deployment_id`, `environment` set,
   `status=manifest_resolution_failed`, deployment `rejected` — never a 500
   (`tests/api/test_version_update.py::test_manifest_resolution_failed_is_200_with_deployment_id`
   pins the contract against the real resolver, not a stand-in failure).

1. **`cluster_spec` → `provider_config` synthesis** — `deployment_service.py` (~:39-41). Birth
   starts `provider_config={}`; v1 synthesized CIDR allocation + cluster spec (CPU/mem/region/pod-
   service CIDRs) from the profile's `cluster_spec` block (v1 `core/cluster_spec.py`). The
   provisioning workflow reads `provider_config` — empty very likely breaks real provisioning.
   Decide the owner (birth vs a provisioning verb-catalog step); needs a DR + build. *Size: M–L.*
2. **Hostname / DNS / SSL / ingress config synthesis** — **CLOSED, Round 9 (the resolved-config
   component).** v1's `_resolve_hostname`/`_build_resolved_config`
   (`reference-code/.../manifest_resolver.py:766-836`) are salvaged into
   `deployment_service.py`'s module-level `_build_resolved_config`/`_hostname_strategy`/
   `_resolve_hostname` (~:793-1065), called from BOTH `DeploymentService` sites that reach the
   resolver (`_deploy` and `deployment_preview`) — no more passing a finished `config` through
   untouched. Built: `cluster_id`/`environment`/`cluster_slug`; `pod_cidr`/`service_cidr` via the
   already-committed, pure `core/cluster_spec.allocate_cluster_cidrs(cluster_id)` (item 1's function,
   a different consumer — no CIDR sequencing problem, contrary to how this item used to read, since
   `cluster_id` is assigned before `resolve()` is called); `cluster_hostname` with a deliberate
   None-vs-omitted split that is NOT a straight v1 port (`docs/decisions/DR-0025-hostname-resolution-
   ordering.md`, raised by this round's own adversarial judge); `ssl_enabled`/`dns_enabled`;
   `ingress_strategy` reading both shipped shapes (sibling-of-`cluster_config` wins, nested is the
   fallback), reusing the same sibling-overlay rule `cluster.load_spec` already committed rather than
   re-deriving a second normalization that could drift. `tests/app/
   test_deployment_service_resolved_config.py` and the real-profile render in `tests/app/
   test_manifest_resolution_end_to_end.py` cover it.
3. **Cross-service environment-variable resolution** — `services/manifests.py` (~:30) — **CLOSED,
   Round 9.** This item previously claimed v1's `core/environment_config.py` provides
   `${SERVICE.INTERNAL_URL}`-style cross-service resolution; that feature does not exist anywhere
   in v1 (zero hits across `reference-code/seedpod/seedpod/` and `reference-code/seedpod/config/`
   — the claim was simply wrong, corrected in place per CLAUDE.md's "don't pin v1 bugs" discipline,
   which applies equally to not pinning a false claim about v1). What v1's `resolve_all_services`
   actually does: merge a `shared` dict with a per-service dict (service wins), then render each
   VALUE through Jinja2 with `StrictUndefined` if it contains `{{`/`}}` —
   `config/deployment-profiles/exampleco-web-2.yml`'s own `CLUSTER_ID: "{{ cluster_id }}"` confirms the
   real syntax. Ported verbatim to `seedpod/core/environment_config.py` and wired into
   `ManifestResolver._render_templates`; see this round's `docs/decisions/DR-0025-hostname-
   resolution-ordering.md` for the closely related hostname-resolution work also closed here.
4. **GHCR pull-secret auto-generation** — **CLOSED, Round 9 (the org-and-ghcr component).** Ported
   v1's `_add_ghcr_auth_if_needed` (`ManifestResolver._add_ghcr_auth_if_needed`, called from
   `resolve()` between image resolution and rendering) and the `infrastructure_templates`
   conditional-render hook (`_INFRASTRUCTURE_TEMPLATES`, a module-level constant now — `_render_templates`
   renders `ghcr-secret.yaml` in its own pass after the per-service loop, since that file's stem is
   never a declared service name). Condition unchanged from v1: some resolved, non-external image
   references `ghcr.io` AND a GHCR token is configured. The username is the org this same component's
   #0b closes (`self.ghcr_service.config.organization`) — no ambient/global settings read, unlike v1.
   Genuine correctness fix, not a v1 bug pin: v1's `except Exception: logger.warning(...); continue`
   around this whole step is NOT carried forward — a missing token stays a silent, legitimate no-op,
   but a render failure once the condition is met now raises `PermanentError` like every other
   template, rather than silently shipping a `Deployment` whose `imagePullSecrets` names a `Secret`
   that was never rendered. `tests/services/test_manifests.py` + the real `exampleco-web-2` profile in
   `tests/app/test_manifest_resolution_end_to_end.py` cover it, including a `.dockerconfigjson`
   decode assertion.

0d. **Init-container removal — DONE** (2026-08-09, smoke 8; `af866e9`). **21 `until nc -z …` init
   containers removed across 11 templates**, taking every `busybox` reference in
   `config/manifest-templates/exampleco-stack/` with them — so the plan's step 5 (drop the busybox
   image pulls) fell out for free. *Was: M, needs its own smoke.*

   **Proven by running it both ways on real infrastructure.** With the init containers still in
   place, then again from **cold on a fresh cluster** with them gone:

   | transition | with waits | without waits |
   |---|---|---|
   | wave 1 → 2 (datastores) | 2m38s | **2m45s** |
   | wave 2 → 3 (migration Job) | 25s | **26s** |

   Near-identical, which is the finding: the **waves** were doing the work and each `nc -z` had
   been passing instantly all along. All 13 pods `Running 1/1`, `exampleco-atlas-migrations`
   `Completed`, and pod ages corroborate the ordering independently (datastores 7m42s, migration
   4m56s, apps 4m30s).

   **A cold start was required; a redeploy would not have done.** Redeploying onto the live cluster
   (also run, green) leaves postgres et al already running, so nothing stresses the ordering even
   if it were broken. Only the fresh provision actually tests the claim.

   **THE TRAP, for anyone doing this to another template set:** 14 files have `initContainers`, not
   11. **Three do real work and must be kept** — `frontend-server.yaml`'s
   `copy-{supplier,buyer,admin}-assets` (`cp -r /assets/. /target/…` into the shared volume;
   removing them serves an empty frontend), `rabbitmq-dev.yaml`'s `install-delayed-message-plugin`,
   and `tigerbeetle.yaml`'s `format-data`. A blanket "delete `initContainers`" breaks all three
   silently. The backlog's "20+ waits across 11 files" below was exactly right; the other three
   were never in scope.

   ---
   *Original entry, kept for the reasoning:*

0d-orig. **Init-container removal — DR-0029's tracked follow-up.** *Size: M, needs its own smoke.*
   `config/manifest-templates/exampleco-stack/` carries **20+ busybox `until nc -z …` dependency waits
   across 11 files**. They are exactly what `reference-code/PLAN-wave-orchestration.md` was written to
   replace: "Current approach uses busybox init containers in each pod to poll dependencies … This
   works for first deploy but breaks on redeploy — when all pods restart simultaneously, circular
   waits and race conditions occur."

   Round 10 built the wave orchestration that supersedes them (DR-0029) but **deliberately did not
   remove them** — the plan sequences removal as its own migration step 4, after wave values are added
   and the grouping is proven, and DR-0029 point 6 records that decision. They are harmless meanwhile:
   once waves order correctly each `nc -z` passes immediately, because the dependency is already up.

   Removing them is a real change to 11 shipped templates whose failure mode is a deployment that
   half-starts, so it wants a smoke of its own rather than riding along with another round's. The
   plan's own step 5 (dropping the busybox image pulls) folds in with it. **Do this only after a
   deployment smoke has proven the wave ordering on real infrastructure** — otherwise the init
   containers are silently doing the work and their removal is what reveals it.

## P1 — fidelity (parity, not smoke-blocking)

5. **Slug naming-strategy engine — CLOSED, not ported** (2026-08-11,
   [DR-0038](decisions/DR-0038-slug-naming-is-deterministic.md)). The item's framing was backwards:
   v2 did not fail to port v1's engine, it **deliberately replaced** it and said so
   (`deployment_service.py:57`). So this was "reverse a documented decision", not "finish the work".
   The decision stands, and it is now stronger than when it was made: **the slug is the DNS record
   name** (DR-0034), so v1's `fixed` strategy would give two clusters from one preset the same
   hostname and the second provision would repoint the first's record. What was genuinely wrong is
   fixed — `naming_strategy` was accepted, stored and echoed while doing nothing; a non-null value
   is now a 422 and the two `seedpodctl --naming-strategy` flags are gone. Column and serializer
   kept, so existing rows read back unchanged. v2's SPA never rendered it.
6. **DNS-on-destroy — DONE** (2026-08-10, DR-0034, as a consequence of #22 rather than as separate
   work). This item's own text was stale twice over: the `_no_dns_record_ref`/`dispatch_table.py`
   "TODO(spine)" hook was deleted by DR-0022 ruling 2, and `cluster.load_infra` +
   `dns.delete_record` were both **built in Round 8b** — what was missing was anything to load.
   `DnsRecordRef.from_provider_config` read three keys out of v1's `provider_config` blob that v2
   never wrote, so the destroy path always no-opped while reporting `succeeded`. It now reads the
   three columns `cluster.store_dns_record` writes, and a test drives store → `load_infra` to pin
   the join. **Destroy was the half that already existed; #22 was the half that did not.**
   **PROVEN by smoke 11**: the destroy run's `dns` step emitted `dns record deleted | existed=True`
   for record `e16122840b…` — the first time that step has ever deleted anything. Every previous
   destroy ran with `record=None` and reported `succeeded` having done nothing.
7. **`resolution-strategies.yml` — CLOSED** (2026-08-11,
   [DR-0037](decisions/DR-0037-resolution-strategy-source-of-truth.md)). Half stale, half the wrong
   problem. **The endpoint already had a real source** (`config.py:83` reads the file). The real
   finding: **v1 and v2 are exactly inverted** — v1 resolves fallbacks from the named strategy and
   leaves the profile's own `fallback_branches` dead-but-echoed (`manifest_resolver.py:1037`); v2
   reads the profile and ignores the strategy entirely. For `exampleco-staging-stack` that means v1 falls
   back `dev → main` and v2 falls back `staging → dev` — **different images** when a service has no
   build for the branch. The profile now formally owns it (a recorded divergence, not an accident),
   an unsupported `resolution_strategy` **fails at profile load** instead of silently degrading (a
   profile asking for `strict_branch`'s "no fallbacks" was getting full fallback), and each strategy
   the API serves carries `supported: bool` so the list stops advertising what the engine ignores —
   backlog #24's shape. `require_triggering_repo` is defined in v1 and read nowhere; deliberately
   not carried.

## P2 — minor

8. **Snapshot storage gzip — DONE** (2026-08-11). v2 now writes `<service>.dump.gz` as v1 did.
   Deferred originally as "an orthogonal storage-format choice", which was true until snapshots
   started accumulating for real. The **read** side already sniffed the gzip magic (smoke 10's
   restore fix), so both forms restore and every snapshot taken before this keeps working unchanged;
   `mtime=0` keeps the bytes deterministic for identical input so `total_size_bytes` does not flap.
9. **`GET /api/registry/repositories`** — `api/routers/registry.py:29`. Derived from profile
   `repository` names rather than a real GHCR org listing (no ui-contract row). Revisit only if a
   true registry listing is wanted. *Size: S.*
10. **Python 3.14 upgrade — deferred, deliberately** (Kezia, 2026-08-03: "put it on the roadmap and
    not do it now"). Currently 3.11.12; `requires-python` is already `>=3.11`, and a survey during
    the 2026-08-02 deadlock investigation found **no blockers**. What it buys: 3.12's `wait_for`
    rewrite (which added `uncancel` bookkeeping) **deletes bug-class (2) at the root** — the
    swallowed-cancel race behind the `App.stop()` teardown deadlock — and 3.14's
    `python -m asyncio ps <pid>` replaces the hand-rolled task-dump watchdog that investigation
    needed. **Not urgent, because the bug class is already handled in code**: `3ea5c94`'s cooperative
    `TimerService`/`EffectExecutor` stops and DR-0024's `WorkflowEngine.stop()` never depend on
    cancellation on the normal path, so the upgrade would demote them from load-bearing to
    belt-and-braces rather than fixing anything still broken. Keep
    `tests/runtime/test_shutdown_races.py` after the bump — it pins the cooperative behaviour, which
    stays correct on any version. One known modernization to land with it: `providers/kubectl.py`'s
    `get_event_loop()` → `get_running_loop()`. *Size: S–M (mostly re-verification: full suite, the
    conformance suites, and one real provider smoke).*

11. **Deploying config, once config lives in its own repo** (Kezia, 2026-08-16). The engine is
    going public under MIT; the real deployment config — profiles, manifest templates, provider
    settings, org identity — moved to a private per-project repo (`seedpod-config`, one per
    client). That breaks a working assumption: **DR-0041 decision 2 bakes `config/` into the
    release artifact**, and `scripts/build_release.py` copies it verbatim. An artifact cannot
    carry a private repository, so the appliance needs another way to get config.
    **The current thinking is to push config through `seedpodctl`** rather than couple the
    release build to a second checkout — the CLI is already the authenticated HTTP surface
    (DR-0021 §0c), and a config push is exactly the kind of operator action it exists for. That
    would also make config a *deployable, versioned thing* rather than something rsynced by
    hand, which is the same argument DR-0041 made about code. Open questions worth a DR: whether
    the server validates a pushed config before accepting it (it already fails loudly on an
    unresolvable profile, so a dry-run render is within reach); whether config is versioned
    alongside releases or independently; and what happens to a running server when config
    changes underneath it — today `config_dir` is read at boot, so nothing re-reads it.
    Until this exists, point `SEEDPOD_CONFIG_DIR` at a clone of the config repo and update it by
    hand. *Size: M, and it needs a DR before code.*

## SPA — done, and now PROVEN against a live v2 (smoke 6, 2026-08-09)

The §6 migration is complete, build-verified (`npm run build`, 64 modules), and — as of
**smoke 6** — driven end to end against a live server on real DigitalOcean infrastructure.
**The whole lifecycle was performed through the UI**, not the CLI: create preset -> deploy
(provider picked in `PresetDeployModal`) -> watch provision -> ACTIVE -> live pod list ->
redeploy -> destroy. `provision-digitalocean` and both `deploy-waves` runs succeeded, the
supersession chain populated, and 0 droplets were left behind.

**What the live run proved that the build could not:**

- **SSE genuinely works.** `cluster_state_changed`, `deployment_status_changed` and
  `workflow_progress` all arrive and drive live refetches. DR-0031's escalation is *visible* in the
  MiniEventHud — an `e9f7cebf | active → active` cluster line immediately followed by
  `01bb08da | pending → deploying` on the new deployment.
- **Live pod visibility works** — `exampleco-web-2-…  RUNNING  1/1` with the resolved GHCR image,
  alongside the k3s system pods.
- The registry-backed "Service Tag Overrides" panel in the create-preset modal, the
  enabled-providers dropdown, and the deployment audit table all populate from real data.

**Three findings; two fixed in the same sitting.**

- **FIXED — every `expires_at` rendered `NaNh NaNm`.** Five sites did `new Date(s + "Z")` on a
  timestamp v2 already serializes with a `+00:00` offset (aware datetimes; naive ones are banned in
  `core/`), yielding `...+00:00Z` = Invalid Date. v1 emitted them bare, so the idiom was correct
  *there* and the §6 migration carried it forward unexamined. The repo's own `time-utils.js`
  `parseUTC` already handled both shapes — it just wasn't exported. Now exported and used at all
  five sites. `ApiKeyDetail.jsx:202` was the worst of them: `new Date(Invalid).toISOString()`
  **throws**, so the key-edit form would have crashed on open.
- **FIXED — MiniEventHud showed `updated` for every deployment event.** It read `data.status`;
  v2 sends `old_status`/`new_status` (obligation 1) and never a bare `status`, so the `|| "updated"`
  fallback fired every single time and the actual status was never visible. Note the ui-contract is
  internally inconsistent here — obligation 1 pins `old_status`/`new_status`, while §2's table lists
  the read field as `status`. The server is right; the SPA and the §2 table were wrong.
- **FIXED — an idle SSE connection tore down and reconnected every ~2 minutes.** The server's
  keepalive was an SSE *comment* (`: keepalive`), which `EventSource` does not deliver to
  `onmessage` — so `sse-client.js`'s `updateHeartbeat()` never ran on an idle connection,
  `lastHeartbeatTime` never advanced, and the 120s monitor force-reconnected. **Confirmed live**, not
  inferred: `[SSE] Heartbeat timeout (120002ms), forcing reconnect`. Obligation 2 was satisfied on the
  wire and still failed at its purpose. **v1-carried, not a v2 regression** — v1's
  `api/events.py:78` yields the same comment line — so this was a "don't pin v1's bugs" call.
  **The keepalive is now a real `data:` frame** carrying a `keepalive` envelope built by
  `SSEHub.envelope` (promoted from `_envelope`), so its shape is identical to every broadcast
  (obligation 4). **No DR needed** — `runtime/sse.py`'s own docstring says the topic set is
  "documentation of what callers are expected to send, not something this module enforces", and
  obligation 2 pinned only a timing ceiling, never the wire shape. **No client change was needed
  either**: `onmessage` already refreshes the heartbeat before dispatching, nothing listens for
  `keepalive`, and `event-store.js`'s default topic list excludes it — a comment there now says why
  adding it would flood the HUD. Obligation 2 in `ui-contract.md` is amended to require the
  keepalive be *observable to the client*, which is what it was always trying to buy.
  **The test that could not previously fail now can.** `test_events_sse.py`'s `_stream` helper
  filters on `data: `, so a comment keepalive was invisible to every HTTP-level test — nothing would
  have failed if the keepalive broke outright, which is exactly how this survived to smoke 6. The
  renamed `test_event_stream_yields_keepalive_data_frame_when_idle` now asserts the `data:` framing
  itself. A full HTTP round trip would need either a 30s wait or the `AppConfig` interval seam
  `events.py` deliberately refuses, so it is not worth it for a property the generator-level test
  already pins.

**Seventh real smoke run, 2026-08-09 — the verification run for smoke 6's four fixes (DigitalOcean).**
Same shape as smoke 6, driven entirely through the UI: `ui-smoke` preset → deploy (DO, TTL 1h) →
provision → ACTIVE → live pods → redeploy → destroy → `destroyed`, **0 droplets left** (checked
against the DO API, not inferred). Both `provision-digitalocean` and both `deploy-waves` runs
succeeded with no failed steps. `POST /api/deployment-preview` was run first as the free pre-flight
and came back `success`. **0 console errors and 0 warnings across the whole run**, and no
`Heartbeat timeout` line — the keepalive fix holding under a real workload, not just an idle one.

What it proved that smoke 6 could not:

- **DR-0032 is live and uniform.** `Deployed By: api:kezia` renders at **all four SPA sites**
  (DeploymentList, DeploymentDetail, ClusterDetail's current-deployment panel and its
  Deployment History table). More than that, the value **survived every transition**:
  `pending → deploying → active → superseded → destroyed`, still `api:kezia` at the end — the
  row-only-column property the DR reasons from, now observed rather than argued.
  **All three birth sites are covered**: `_deploy`'s success branch (the first deploy) and
  `redeploy` (the second) live here; the manifest-resolution-failure branch stays covered by the
  service tests only, because reaching it on real infra still births a cluster row.
- **The MiniEventHud fix is right on the wire.** The HUD rendered
  `624cd096 | pending → deploying` — the actual statuses, where before the fix every line read
  `updated`.
- **DR-0031's escalation is visible again**, in the same three-line signature as smoke 6:
  `624cd096 | ? → pending`, `6194051f | active → active`, `624cd096 | pending → deploying`.
- **The `parseUTC` fix is confirmed against a live TTL.** ClusterDetail showed `TTL: 0h 55m` and
  counted down to `0h 54m` — `formatTimeRemaining` is precisely the function that rendered
  `NaNh NaNm` before, so this is the fix's own regression case passing on real data.

**One new (cosmetic) finding, not fixed.** The HUD renders a deployment's *birth* event as
`? → pending`. That is two deliberate decisions meeting badly: `core/machine.py` passes
`notify_old_status=""` at all four birth transitions (`:287,:299,:750,:761`, commented as
"preserves v1's UI-visible birth broadcast shape"), and the HUD's `data.old_status || "?"` renders
empty-string as `?`. So a **deliberately** empty old-status is displayed identically to a
**missing** field — the same conflation class as the `|| "updated"` bug smoke 6 found, though here
the server is right and only the display is misleading. Left as a residual: it is cosmetic, and the
honest fix (render birth as `→ pending`, distinguishing `""` from `undefined`) is a UI decision, not
a contract one.

Remaining residual:

- **Item 9 partial (intentional):** MiniEventHud consumes `workflow_progress`, but the inline
  "deploy in progress" banners in ClusterDetail/DeploymentDetail are left as static fallbacks — a
  live-progress enhancement, not a contract regression.
- **Still no SPA test suite.** Smoke 6 was driven by hand (a browser-automation walk of every
  route); nothing pins these behaviours. The two fixes above are exactly the class of regression a
  handful of contract tests over `time-utils` and `formatEventData` would have caught at build time.

## Verify flags

10. **DR-0013** — discovered-cluster `environment="production"` default; ratified but unproven.
    Revisit against real fleet behavior once discovery runs under test (a discovered *local*
    kind/tart VM labelled `production` is the wart).
11. **Restore-history `initiated_by` — VERIFIED, already correct** (2026-08-11). Checked rather
    than assumed: `api/routers/snapshots.py:174` passes `actor=f"api:{api_key.username}"`,
    `SnapshotService.restore` writes it onto the `workflow_runs` row (`:497`), and the router
    serializes it (`:100`). It already follows DR-0032's actor convention, which is exactly what
    DR-0032 asked for. The workflow path records `system:deploy` by the same mechanism. No work.
16. **`deployed_by` is never populated — DONE** (DR-0032, drafted + implemented 2026-08-09; the DR
    is `status: proposal` pending ratification). It now records the **actor string**
    (`api:<user>`) at all three birth sites — the same value the cluster state audit records for the
    identical request. Deliberately *not* a literal v1 port: v1 stored the triggering **repo** for
    webhook deploys and the **username** for preset deploys, so one column held two different kinds
    of thing (evidence table in the DR). Four lines in `deployment_service.py` — `actor` was already
    a parameter of both `_deploy` and `redeploy`. **No `core/`, machine-table or `Dispatcher`
    change**: `deployed_by` is a row-only column and `DeploymentRepository.persist` CAS-updates only
    the columns `DeploymentRecord` carries, so the birth value survives every later transition
    (now pinned by `test_deployed_by_survives_every_later_transition`). Original finding, for the
    record: `_birth_deployment_row` hardcoded `deployed_by=None` and took no parameter for it, with
    **no other writer anywhere in `seedpod/`** — while three API responses serialized the field
    (`api/routers/deployments.py:171,208`, `api/routers/clusters.py:210`) and four SPA sites rendered
    it, so "Deployed By" was *structurally* always empty. Needed a DR on two independent grounds:
    `DeploymentService` is a committed frozen service (DR-0030's "the next such request needs its
    own answer"), and this is a deliberate divergence from salvaged v1 behavior (DR-0001).
    **Verify flag 11 (restore-history `initiated_by`) is the same family and was NOT in scope** —
    DR-0032 records that it should adopt the same actor semantics when addressed, rather than
    inventing a third convention.

## §0 follow-ups (found dogfooding the CLIs, 2026-07-20)

13. **`seedpod-bootstrap` does not load `.env` — DONE** (2026-08-11). `main()` now loads it (not
    at import time — the module's zero-side-effects contract is intact) and a missing required
    variable exits 2 with `error: ...` plus a hint naming `generate-keys`, instead of a raw
    `MissingEnvironmentVariable` traceback. **A bug in the first cut, caught by its own test and
    worth recording**: bare `load_dotenv()` resolves its path by walking up from the *calling
    module's file*, so it found the checkout's `.env` regardless of where the operator ran the
    command — pointing a cold start at the developer's database. It uses
    `find_dotenv(usecwd=True)`. Every command in smokes 11 and 12 needed `set -a; . ./.env` first;
    that is what prompted this. (Unrelated and still true: a DB path that cannot be opened still
    tracebacks, and `SEEDPOD_DATABASE_URL` in `.env` is repo-root-relative, so running from a
    subdirectory fails either way.)

## Known accepted issues (won't-fix now)

- **`kind` provider — deprecated; do not invest.** VM-k3s (the `tart` provider) is significantly
  more performant than kind, so kind is being wound down and gets no further work now (Kezia,
  2026-07-20). Concretely: `providers/kind.py:_resolve_host_ip` uses a **blocking**
  `socket.gethostbyname` (real IO *outside* the transport seam) so the resolved IP can be baked into
  the kubeconfig `server:` URL + `public_ip` (`config/workflows/provision-kind.yml:37/62/68`) — v1's
  workaround for runtime `.local`/mDNS resolution. That IP-baking is a hack that doesn't earn its
  place, but the only real fix (inject an async `HostResolver` into `KindProvider`) isn't worth it
  given the deprecation. **Consequence:** the conformance test
  `test_probe_instance_or_probe_k3s_is_one_iteration[kind]` is macOS-mDNS-sensitive — it resolves
  `minimax.local` (~5s on Bonjour) and trips its 2s budget **locally on macOS**, but passes in the
  network-less CI/sandbox. A green local full-suite run may show this one failure; it predates §0
  and is unrelated to the app/CLI work.

## The validation that ties it together

12. **Real-cutover smoke** — **RUN TWELVE TIMES** (2026-07-20, 08-02, 08-03, 08-08/09, 08-09,
    smoke 6 on 08-09 through the SPA, smoke 7 on 08-09 verifying smoke 6's fixes, **smoke 8**
    on 08-09 — the first `exampleco-stack` deployment, which proved multi-tier wave ordering and then
    closed #0d — and **smoke 9** on 08-09, the first `provider_host` deploy anywhere, which proved
    #17 and verify (f) together; see the blocks under P0 and the SPA section).

    > **TWELFTH real smoke run, 2026-08-10 — the ACME proof, and it took three attempts.**
    > `exampleco-staging-stack` (DNS) on DigitalOcean. **PASSED on the third**, and the first two
    > failures are the report.
    >
    > **12a — the fix did not work, and every layer looked right.** Cluster ACTIVE 3m19s, deploy
    > 5m40s, hostname resolved, app served — and the certificate was still
    > `CN=TRAEFIK DEFAULT CERT`. The diagnosis chain, offline first then on the live cluster:
    > the profile carried `ssl_config`; `provider_config` persisted it; `cluster.load_spec` output
    > the `AcmeConfig`; `k3s.install`'s persisted step params carried it **with the staging
    > directory** — and the `HelmChartConfig` on the cluster had the ports block and **no
    > `certificatesResolvers`**. **`_ingress_for` had gained an `acme` parameter and never passed it
    > into the `IngressConfig` it returns.** One line.
    >
    > **Why 2404 green tests missed it.** Every DR-0036 test constructed
    > `IngressConfig(acme=...)` **by hand** and drove the provider, so they proved the manifest
    > renders and never that anything reaches it. The step → provider translation — the one thing
    > that was wrong — had no test. **This is backlog #13's shape exactly** ("a test can pin a
    > decision and miss its consequence"), and it is now
    > `test_install_threads_acme_into_the_ingress_config`, verified failing against the pre-fix
    > code and passing after.
    >
    > **12b — a wasted droplet, and the operational lesson.** The re-deploy was fired against a
    > server started BEFORE the fix. **`start.py` does not run uvicorn reload-mode** (deliberately
    > not ported), so a running seedpod serves the code it booted with. **Restart the server after
    > every code change, before any smoke.** Also learned trying to abort it: `DELETE /api/clusters/{id}`
    > is **rejected while `provisioning`** ("DestroyRequested is not valid ... in state
    > 'provisioning'") — a deliberate machine rule, but it means a run started by mistake must be
    > allowed to reach ACTIVE before it can be destroyed.
    >
    > **12c — PASSED.** Provision 2m58s → deploy 5m41s → destroy 30s, 0 droplets, 0 DNS records.
    > Evidence, in the order it becomes real:
    > 1. `HelmChartConfig` on the cluster carries `certificatesResolvers.letsencrypt.acme` with the
    >    staging `caServer`, `email`, `storage`, `httpChallenge.entryPoint: web`.
    > 2. The **running** Traefik deployment carries all four
    >    `--certificatesresolvers.letsencrypt.acme.*` args.
    > 3. The served certificate: `issuer=C=US, O=Let's Encrypt, CN=(STAGING) Dastardly Durum YR1`,
    >    `SAN: DNS:preset-stack-smoke8-dns-staging-869908bd.cluster.example.com`, valid
    >    Aug 10 → Nov 8 (LE's real 90-day lifetime).
    > 4. Plain `curl` with no `-k` **rejects** it, verify result 20 — correct for the staging CA,
    >    and the cleanest proof it is no longer a self-signed Traefik cert.
    > 5. `/auth/realms/master` → 200 over the name.
    >
    > **Smoke streak: 11 for 12.** The DNS path (#22/#6) was re-exercised end to end on all three
    > runs for free.

    > **ELEVENTH real smoke run, 2026-08-10 — the DNS profile, and the first run that asked
    > whether the advertised hostname RESOLVES.** `exampleco-staging-stack` (DNS) @ `ephemeral` on
    > DigitalOcean, via the `stack-smoke8-dns` preset. **PASSED.** Provision → ACTIVE **2m56s**
    > (17:54:49 → 17:57:45Z), deploy-waves → active **5m40s** (→ 18:03:25Z), destroy **24s**
    > (18:07:01 → 18:07:25Z). 17/20 pods Running (the other three `Succeeded` Jobs — migrations and
    > the two traefik helm installs). 0 droplets left, 0 DNS records left.
    >
    > **The chain #22 existed to close, end to end:**
    > 1. `clusters.dns_hostname` populated for the **first time ever** —
    >    `preset-stack-smoke8-dns-staging-de2e27d3.cluster.example.com` — and `cluster_url`
    >    derived from it by an API that had been ready for a value nobody wrote.
    > 2. `dig +short` → `203.0.113.10` from **1.1.1.1, 8.8.8.8 and the system resolver**.
    > 3. The persisted `dns_record_id` (`e16122840bddaf19798f62ef59d3cc18`) is **byte-identical** to
    >    the record id in the Cloudflare zone.
    > 4. `https://<hostname>/` → **HTTP 200**, `<title>Exampleco Supplier</title>`, curl resolving the
    >    name itself with **no `Host:` override**; `/auth/` → 302; `/auth/realms/master` returns real
    >    Keycloak JSON.
    > 5. **The negative control is what makes it conclusive**: `--resolve wrong.example.com:443:<IP>`
    >    → **404**, the real hostname at the same IP → **200**. Name-based routing, not a catch-all
    >    answering to anything.
    > 6. Destroy emitted `dns record deleted | existed=True` — **the first time that step has ever
    >    deleted anything**; every previous destroy ran with `record=None` and reported success. Then
    >    0 cluster records in the zone, authoritative `dig` (@kristin.ns.cloudflare.com) empty.
    >
    > **#23's server half, observed live:** 21 `workflow_progress` events during the deploy, each
    > carrying the `cluster_id` the new pod-page listeners filter on. Kezia confirmed the SPA pod
    > pages refreshing themselves.
    >
    > **The one finding — backlog #24: Let's Encrypt is asked for and never configured.** All four
    > Ingresses rendered `router.tls.certresolver: letsencrypt` (confirmed in the decrypted audit),
    > but nothing in v2 defines a resolver by that name — v1's `_apply_traefik_config` HelmChartConfig
    > was never ported — so Traefik served `CN=TRAEFIK DEFAULT CERT`. **It could not have been found
    > before this run**: ACME HTTP-01 needs the hostname to resolve, which nothing made happen until
    > #22 landed the same day.
    >
    > **Two things caught OFFLINE, before a droplet existed.** `CLOUDFLARE_API_TOKEN` in `.env` was
    > **empty** — under DR-0034 decision 7 that now fails the provision and destroys the droplet, so
    > it would have burned a run; and the token was then verified for **edit** scope (not just read)
    > with a throwaway record created and deleted in the zone, since a read-only token fails at
    > exactly the point that compensates. **The habit paid for itself for the third smoke running.**
    >
    > **Smoke streak: 10 for 11.** This was an exploratory run against genuinely new surface and it
    > found something, exactly as the "exploratory smokes find things" rule predicts.

    > **Eighth real smoke run, 2026-08-09 — `exampleco-staging-stack` @ `staging` on DigitalOcean.**
    > **THE FIRST MULTI-TIER STACK v2 HAS EVER DEPLOYED** (~15 services against exampleco-web-2's 2),
    > and the run that closed **P0 #0d**. Four waves, gates that genuinely held (2m38s on the
    > datastores, 25s on the migration Job), 13 pods `Running 1/1`, the migration Job `Completed`,
    > 0 droplets left, firewalls detached and `dns.delete_record` succeeded.
    >
    > **Four findings, three fixed the same day.**
    > 1. **A booting sshd could fail a whole provision permanently** (`769573d`). Attempt 1 died at
    >    `trust_host` (`cloud_init_wait: exited 255`) and compensation destroyed the droplet;
    >    `retry: ssh_default` was declared and never used, because the error classified Permanent.
    >    An immediate re-run succeeded, which is what proved it transient. The sshd-mid-restart
    >    stderr family (`kex_exchange_identification`, `Connection closed/reset by`) is now in
    >    `TRANSIENT_STDERR_PHRASES`, and `_run_insecure` treats ssh's own exit 255 as a transport
    >    failure directly — its command is `... || true`, so a non-zero rc can only be ssh failing.
    >    **Not** a blanket "255 is transient": ssh uses 255 for a rejected key too, and
    >    `Fault.AUTH => PermanentError` stays pinned.
    > 2. **Preview could never pre-flight a `provider_host` profile** (`9f5d333`) — found trying to
    >    pre-flight this run.
    > 3. **`provider_host` + Ingress is invalid on an IP-host provider** — NOT fixed, now **#17**.
    > 4. **The discarded-`detail` gap, twice more.** Both failures persisted a message with no
    >    detail: `ProviderError.detail` carries `exit_code`/`stderr`, `_failure_message` stores only
    >    `str(exc)`. Diagnosing "kubectl.apply_manifest: invalid input" required decrypting the
    >    audit blob and server-dry-running against the live cluster. **DR-0033 fixed this shape for
    >    gate timeouts; the step-failure half is still open** and is the same one-line-ish change.
    >
    > **Ninth real smoke run, 2026-08-09 — `exampleco-staging-stack-nodns` @ `staging` on
    > DigitalOcean. PASSED.** The first `provider_host` profile v2 has ever deployed *anywhere*,
    > and the run that turned #17 from code-proven into infrastructure-proven. Cluster ACTIVE in
    > **2m18s** with public IP `203.0.113.11` (an IP host — the #17 premise), deploy-waves
    > **6m07s**, 13 app pods `Running 1/1`, the migration Job `Succeeded`, destroy clean in 31s,
    > **0 droplets left**.
    >
    > **#17 PROVEN.** All four Ingress objects applied and every one is a catch-all — no
    > `spec.rules[].host`, no `spec.tls` block. **Smoke 8 died on exactly this document set**, after
    > wave 1's ten other documents had applied.
    >
    > **Verify (f) PROVEN in the same run** — DR-0025 E2's deferred hostname rendering. The
    > deployment was born with no cluster, so the hostname was DEFERRED, then rehydrated to the
    > droplet IP at deploy time and rendered everywhere: `KEYCLOAK_PUBLIC_URL =
    > https://203.0.113.11/auth`, `FRONTEND_URL`, `KEYCLOAK_REDIRECT_URIS`, the shared
    > `BUYER_UI_PAGE_URL`/`SUPPLIER_UI_PAGE_URL`, and `frontend-nginx-config`'s five
    > `window.APP_CONFIG` blocks. **Zero occurrences of `https:///` cluster-wide.**
    >
    > **The two together are the real result, and they are the design decision validated.** On one
    > cluster, from one variable: the IP is **absent** from every Ingress host and **present** in
    > every URL. That is precisely what the "resolve an IP hostname to `None`, in one place" fix —
    > the obvious reading of #17's original write-up — would have broken, and it would have broken
    > it into `https:///auth`, the exact rendering DR-0025 exists to prevent.
    >
    > **Wave ordering re-proven from cold** (the only way it counts — a redeploy leaves the
    > datastores up): wave 0 instant, **wave 1 165s** (five datastores), **wave 2 15s**
    > (`exampleco-atlas-migrations` Job), **wave 3 180s** (app services). Same shape as smoke 8's
    > post-#0d run, on a different profile.
    >
    > **This smoke found nothing; the streak stood at 8 for 9 after it** (9 for 10 after smoke 10). Worth recording rather than
    > glossing: it was a *verification* run against a defect a previous smoke had already found and
    > that had been fixed offline with a targeted regression test — the narrowest kind of smoke
    > there is. The eight that found something were all exploring surface no run had touched.
    > **#18 got no exercise at all** — nothing failed, so no failure message was persisted; its
    > value is still only argued, not observed.
    >
    > One non-finding worth not re-investigating: immediately after destroy, `seedpod-apps-ams3`
    > and `seedpod-mgmt-ams3` still listed the deleted droplet's id (which 404s). **DigitalOcean
    > cleared both itself within ~45s** — its own eventual consistency, not a v2 leak. All four
    > firewalls are stable and reused (`_ensure_firewall_exists`); 0 attached droplets is their
    > correct steady state.
    >
    > **Tenth real smoke run, 2026-08-10 — `exampleco-staging-stack` (DNS) on DigitalOcean, with a
    > REAL v1 SNAPSHOT RESTORED AND REAL v1 SECRETS. PASSED.** The run that closed **#19** and
    > **verify (c)** together, and **the first time v2 has been shown to *work* rather than merely
    > deploy.** Provision 2m18s → deploy 6m07s → destroy 20s, 0 droplets left.
    >
    > **verify (c) PROVEN.** `wave[1].restore` took **39s** (0s in smoke 9 — nothing to restore).
    > In the live cluster: **7 organizations, 8 users, 6 invoices, 12 agreements**, matching the
    > counts read out of the dump file offline beforehand; Keycloak came back with all four realms
    > (`master`/`admin-account`/`buyer-account`/`supplier-account`) and 12 users enabled.
    >
    > **#19 PROVEN, all four legs**, using v1's real credentials:
    > 1. **Keycloak login** — HTTP 200 + a valid JWT for `admin@example.com` via `admin-cli`
    >    direct grant, issued by the *restored* `admin-account` realm.
    > 2. **postgres round-trip** — `GET /api/organizations/` and `/api/invoices/` returned 200 with
    >    restored data (6 orgs incl. "CleanTech Services"; 6 invoices totalling 21,905,739),
    >    JWT-authenticated end to end.
    > 3. **minio** — a real S3 round-trip (`mb` → `cp` up → `cp` down, byte-identical → `rb`) with
    >    the real `s3_access_key`/`s3_secret_key`.
    > 4. **rabbitmq** — 3 live AMQP connections from app pods, authenticated with the real
    >    `rabbitmq_default_pass`.
    >
    > **Two new findings, both deferred to the next session: #22 (v2 never creates DNS records —
    > the stack was unreachable at its own advertised hostname, and had to be exercised by IP with
    > a `Host:` header) and #23 (ui-contract obligation 5 never discharged — the SPA listens for
    > `pod_status_changed`, which v2 never emits, so live pod pages never refresh).**
    >
    > **A third finding was caught OFFLINE, before a droplet existed, and it is the one that made
    > this run possible:** `SnapshotService.restore` never sent the dump to the pod — it checked
    > `dump_path.exists()` then exec'd `pg_restore` with no file argument and no stdin, and
    > `KubeRun` had no field to carry the bytes. **verify (c) could not have passed.** Fixed in
    > `8c1c7e6` (`kubectl exec -i` + stdin, a deliberate divergence from v1's `kubectl cp`), with
    > gzip sniffing on read so v1's `.dump.gz` files restore as-is. See [[offline-repro-before-workflow]]:
    > reading the code first cost an hour and saved a wasted droplet plus an ambiguous failure.
    >
    > **Wave timings answer a standing question** (Kezia, 2026-08-10: "are we just waiting for time
    > periods?"). **No.** `interval_seconds: 5`, no `settle_seconds`, gate advances on the first
    > successful poll — worst-case padding is <5s per wave, ~20s across all four. Measured: wave 1
    > **125s** (five datastores), wave 2 **15s** (migration Job), wave 3 **113s** (app services).
    > That is genuine container-start and readiness-probe time. Also worth not re-litigating:
    > `restore` runs BEFORE the readiness gate (prep → apply → restore → restart → ready) and that
    > is deliberate — the step carries `retry: {max_attempts: 19, base_delay_seconds: 10,
    > factor: 1.0}`, a ~180s budget explicitly replicating v1's `_wait_for_database_pods_ready(180)`,
    > with the engine's Schedule owning the wait rather than a step-internal poll loop.
    >
    > **Runbook — using v1's real secrets and snapshots.** Both artefacts are split across machines:
    > v1's DB (`reference-code/seedpod/db/seedpod.db`, 20 real `ephemeral` secrets) and its six
    > snapshots (`reference-code/seedpod/data/snapshots/`) are **here**; the Fernet key
    > (`SEEDPOD_SECRET_KEY_DEV`) is **only inside `~/Backups/seedpod-20260305.tar.gz` on minimax**.
    > **v1 stores `base64(fernet_token)`** (`core/auth.py:58`) where v2 stores the token directly —
    > decrypting v1 needs the extra `base64.b64decode`, and without it all 20 fail `InvalidToken`
    > in a way that looks like a wrong key. v1's 20 secrets cover 100% of what the staging stack
    > needs: `ghcr_dockerconfig_json` is auto-generated, `rabbitmq_password` is referenced only by
    > `exampleco-web.yaml` (a service neither staging profile declares), and `security_api_key` is a v1
    > extra v2 never reads. v1's real `s3_access_key` **is** `dev-minio-access-key`, which is where
    > smoke 8's minio rule came from. **Importing a v1 snapshot row**: v2's `snapshots` table is
    > column-identical, but `source_cluster_id` is `NOT NULL REFERENCES clusters(id)` (insert the
    > source cluster as `destroyed` — inert to reconciliation, since DO Phase A only zombies a
    > destroyed cluster when a live droplet carries its uuid tag and Phase B skips `destroyed` via
    > `_ORPHAN_EXCLUDED_STATES`), and **v1's naive timestamps must be converted** or the server
    > refuses to start (`repositories._parse`: "read a naive datetime back from the database" —
    > v2's own rule, correctly enforced). The compatibility pre-flight compares **service names**,
    > not profile names, so the `exampleco-stack` vs `exampleco-staging-stack` rename is a non-issue.
    >
    > **Runbook:** the `-nodns` preset (`stack-smoke8`, profile `exampleco-staging-stack-nodns`) and
    > all 20 `ephemeral` secrets from smoke 8 were still in the DB and worked unchanged. Preview
    > wants `deployment_profile_name`/`triggering_repo`/`triggering_branch`/`triggering_image`
    > (not `profile_name`/`branch`). `seedpodctl workflows list` and `GET /api/workflow-runs`
    > both return empty — step timings came from `sqlite3 db/seedpod.db` directly.

    > **Runbook:** exampleco-stack needs **19 secrets** in `ephemeral`/DEV (only `tailscale_auth_key`
    > existed). One shared placeholder works for all but one, because server and client always read
    > the same key — **except `s3_access_key`, which must equal the `MINIO_ROOT_USER` literal
    > hardcoded in the profile** (`dev-minio-access-key`). `ghcr_dockerconfig_json` is generated
    > from org identity, never seeded. On DigitalOcean use `exampleco-staging-stack.yml`, not the
    > `-nodns` variant (#17).
    **BOTH production providers are now proven end to end**: DigitalOcean
    (shared infra) and `tart` (developers running a full stack locally), each provision -> ACTIVE ->
    a real deployment -> redeploy -> destroy -> DESTROYED with no manual intervention. Every run has
    found something the test suite could not: the empty verb catalog, `kube.apply_file`'s cwd
    dependence, DO's asynchronous delete, the redeploy strand (#13), the empty-error-message bug,
    macOS Local Network Privacy (#15), the HUD's birth-event `? → pending` display (smoke 7), and
    — smoke 8 — a transient sshd blip that failed a whole provision permanently, plus
    `provider_host` + Ingress (#17), and — smoke 10 — **v2 never creating DNS records (#22)** plus
    **ui-contract obligation 5 never discharged (#23)**. **Keep running it after each round — it is
    9 for 10**, smoke 9 being the one that found nothing: a narrow verification run against an
    already-diagnosed defect that already had a regression test. That is the shape of smoke that
    *should* come up empty; smoke 10, which exercised genuinely new surface, found two things.

    **Cold-start runbook gap found by smoke 4** — a fresh DB needs a `tailscale_auth_key` secret
    (`seedpodctl secrets create <env> tailscale_auth_key <value>`) or manifest resolution raises
    for every secret-bearing profile. A placeholder value is enough for a smoke: tailscale is
    `required: false` and `plan_waves` only puts `Deployment`/`Job` kinds in a wave's gate lists
    (`engine/steps/deploy.py:669-670`), so a non-authenticating tailscale DaemonSet cannot hang a
    run. **`POST /api/deployment-preview` is a free pre-flight** for all of this — it exercises the
    whole resolution path, real GHCR lookups included, without provisioning anything.

    ~~(a) the `tart` provider~~ **PROVEN, smoke 5** — including Rosetta running an amd64 image on
    Apple Silicon. ~~(d) `ensure_rollouts`' restart rule~~ **PROVEN, smoke 5** — both branches now:
    the NON-restart branch on DO (mixed `configured`/`unchanged` correctly does not restart, twice)
    and the RESTART branch on tart, evidenced by the `kubectl.kubernetes.io/restartedAt` annotation,
    `generation: 2`, and the ReplicaSet rotation.

    ~~(b) the SPA pointed at a live v2~~ **PROVEN, smoke 6** (2026-08-09) — the full
    preset -> deploy -> ACTIVE -> live pods -> redeploy -> destroy lifecycle driven entirely
    through the UI on real DigitalOcean, 0 droplets left. Found three things (two fixed): see
    "SPA — done, and now PROVEN against a live v2" above. **Smoke 7 (same day) re-ran the same
    lifecycle to verify all four of smoke 6's fixes on real infrastructure** — all four confirmed,
    0 droplets left, 0 console errors, and DR-0032's `deployed_by` proven to survive
    `pending → deploying → active → superseded → destroyed`. **It is now 7 for 7.**

    ~~(e) the full multi-tier wave ordering~~ **PROVEN, smoke 8** (2026-08-09) — `exampleco-staging-stack`
    on DigitalOcean, the first `exampleco-stack` deployment v2 has ever done. `plan_waves` grouped
    exactly as DR-0029 declares (wave 0 unmatched docs; wave 1 the five datastores; wave 2 the
    `exampleco-atlas-migrations` **Job**; wave 3 the seven app services), and the gates **held**:
    2m38s on wave 1, 25s on wave 2. Re-run from cold with the init containers removed: 2m45s and
    26s — see #0d.

    ~~(f) **deferred hostname rendering** (DR-0025 E2)~~ — **PROVEN, smoke 9** (2026-08-09). Smoke 8
    had tried the `provider_host` route and found it could not work on DigitalOcean at all (#17);
    once #17 was fixed, E2 proved out on DigitalOcean after all, with no `tart` dependency: the
    hostname was deferred at birth, rehydrated to the droplet IP at deploy time, and rendered into
    every URL with zero `https:///`.

    ~~(c) a snapshot restore~~ — **PROVEN, smoke 10** (2026-08-10): a real v1 snapshot restored
    onto a live cluster via a preset's `--data-initialization`. **Every §12 verify item is now
    proven.** (It could not have passed before `8c1c7e6` — `SnapshotService.restore` never sent the
    dump to the pod.)

13. **A redeploy to an already-ACTIVE cluster never starts — DONE** (DR-0031 + Erratum E1, ratified
    and implemented 2026-08-09, **proven on real DigitalOcean the same day**). *Was: S-M.*
    Found by smoke 4. `DeploymentService.version_update` deliberately reuses the one ACTIVE cluster
    for the same repo/branch/environment (its own module docstring, "narrows here to one rule"), and
    births the deployment in PENDING. But `PENDING -> DEPLOYING` has exactly one driver,
    `ClusterReady`, and that event has exactly one emitter: `core/machine.py:314`, the
    `provisioning x ProvisionSucceeded` cascade. A cluster that is ALREADY ACTIVE never re-emits it,
    so the deployment sits in `pending` with **zero** workflow runs, forever.

    It needs a DR rather than a patch because the fix is a placement decision with two defensible
    homes, and the wrong one duplicates state-machine authority:
    - **the API service chains it** — `runtime/dispatcher.py`'s docstring already names this
      ("API `DeployRequested`+`ClusterReady` chains") and `Dispatcher.apply()` carries the optional
      `tx=` for precisely this same-transaction chaining; or
    - **the cluster machine emits it** on a deploy request against an ACTIVE cluster, keeping every
      `ClusterReady` inside `core/machine.py` where the other one already lives.

    **Ratified analysis lives in `docs/decisions/DR-0031-deploying-to-an-already-active-cluster.md`**
    (status: PROPOSED as of 2026-08-09). Two corrections that DR's primary-source read produced,
    against an earlier draft of this entry:
    - **Superseding is NOT part of the problem.** `core/machine.py:701` already cascades
      `SupersededBy` to the cluster's ACTIVE deployments on `DeploySucceeded`, which is exactly v1's
      rule (`jobs/state/deployment_job.py:655-664`). `superseded_by` is unset only because the
      redeploy never runs — nothing to decide, nothing to build.
    - **It is not a `version_update` special case.** TWO dispatch sites birth a deployment with
      `DeployRequested` and both strand: `deployment_service.py:640` (`version_update`) and `:800`
      (`redeploy`). `retrigger` (`:806`) delegates to `version_update`, so three user-facing entry
      points are affected. **`redeploy` is broken unconditionally** — it exists only to redeploy onto
      the original cluster, so it fails in its entire intended use.

    v1 had no such branch to fall into: `cluster_manager._schedule_deployment_work` (:1443)
    **unconditionally** scheduled the deployment workflow, and "does this need provisioning first?"
    was a branch INSIDE it (`_ensure_target_cluster`). v2's event cascade is the better design but
    dropped the already-ready case — the exact "silently regressing edge behavior v1 already got
    right" failure `CLAUDE.md` names.

    **Resolution (DR-0031 Erratum E1).** The escalation lives in `Dispatcher.apply`, which is the
    only component that sees both aggregates, so ONE fix covers both birth sites. It **translates**
    rather than forwards: a deployment's `DeployRequested` becomes the cluster's new
    `DeploymentPending`, because `test_event_type_unions_partition_an_event_exactly` pins
    ClusterEvent/DeploymentEvent as disjoint and the ratified text's "add a `DeployRequested` row"
    would have broken that. `(ACTIVE, DeploymentPending)` cascades `ClusterReady` back to the
    cluster's PENDING deployments; the other nine states are explicit and `_ignore`/`_invalid`.

    Two further wrong implementations were caught by existing tests, both recorded in the DR: a
    no-`Persist` version collided on `effects_outbox.effect_id` (identity is
    `{aggregate}/{id}@{to_version}#{ordinal}`, so effect-producing transitions MUST bump the
    version), and a no-`Notify` version broke this table's one-Notify-per-Persist law.

    **Verified on real infrastructure 2026-08-09**, immediately after: deploy → redeploy → redeploy
    on one live cluster, giving a clean supersession chain `e7a568b7 → a00ec9cd → 8ef3726c` with a
    `deploy-waves` run for each. `superseded_by` populated for the first time ever, confirming the
    DR's read that superseding was already correct and merely unreachable.

    **It only half-unblocks item 12's `ensure_rollouts` proof** — see §12, which now records why the
    restart branch is unreachable on `exampleco-web-2` specifically.

14. **Two manifest fields kubectl can never report `unchanged` — DONE** (2026-08-09, same day).
    *Was: S.* Found while proving #13 on real infrastructure, then root-caused on a throwaway `kind`
    cluster by reading kubectl's literal PATCH bodies.

    **Symptom.** Two consecutive redeploys of an untouched `exampleco-web-2` stack both reported
    `secret/tailscale-auth` and `daemonset.apps/tailscale` as **`configured`** while the other six
    resources were `unchanged`, so `ApplyChangeSummary.all_unchanged` could never be true and
    `deploy.ensure_rollouts`' restart branch was **unreachable on any wave containing tailscale**.

    **Root cause — two DIFFERENT bugs in the same template, both "the field never round-trips":**

    | Resource | Template said | Live object stores | Why |
    |---|---|---|---|
    | `secret/tailscale-auth` | `stringData:` | `data:` only | `stringData` is write-only; the server converts it and never echoes it back, so kubectl re-sends it every apply |
    | `daemonset.apps/tailscale` | `TS_KUBE_SECRET: value: ""` | `{name: TS_KUBE_SECRET}`, no `value` key | `""` is the zero value and the server drops it, so kubectl re-patches it every apply |

    Reproduced on **kind v1.35.0**, a different distribution from the k3s where it was found.
    `secret/ghcr-secret` stayed `unchanged` throughout — it uses `data:` — which is what isolated
    the cause to the field, not to Secrets in general.

    **It was never dangerous, only silent.** `kubectl diff` reports no difference and the DaemonSet's
    `generation` stayed at **1** across three applies: nothing restarts, nothing churns. The entire
    impact was the false `configured` poisoning the one signal `ensure_rollouts` reads.

    **Fix.** `data:` + a new `| b64encode` Jinja filter (`seedpod/services/manifests.py`), and drop
    `value: ""` (a valueless env var already means `""`). Verified on a live cluster: repeat applies
    now report `unchanged`, and the stored objects are byte-identical to before — same env list, same
    decoded secret, same `Opaque` type, `generation` still 1, so the fix does not even cause a
    rollout. A parametrized guard over every shipped template
    (`test_shipped_templates_avoid_fields_kubectl_can_never_report_unchanged`) makes both patterns
    impossible to reintroduce; it was verified to flag the pre-fix templates.

    **v1 ships the identical two occurrences**, so v1's own rollout-restart rule was equally poisoned
    for tailscale waves and nobody noticed — an inherited defect deliberately not ported
    (CLAUDE.md: don't pin v1 bugs).

    **Both branches are now PROVEN** (smoke 5, 2026-08-09): the non-restart branch on DO (mixed
    changes → no restart, twice) and the restart branch on tart, where wave 1 reported all 8
    resources `unchanged` — including the two this item fixed — and `kubectl rollout restart` fired,
    evidenced by the `kubectl.kubernetes.io/restartedAt` annotation, `generation: 2` and a
    ReplicaSet rotation.

15. **macOS Local Network Privacy silently breaks tart — DONE** (2026-08-09; diagnosis corrected,
    documented in `docs/guides/tart-local-dev.md`, made diagnosable by DR-0033). *Was: S. Found by
    smoke 5.*

    On macOS 15, a seedpod process can be denied access to the `192.168.65.x` vmnet where tart VMs
    live. Every connection returns **errno 65 (`EHOSTUNREACH`)**, so *no* tart VM is reachable and
    the run dies at the `k3s.await_ssh` gate looking exactly like a VM that never booted. **This is
    a host permission issue, not a v2 defect** — but it will bite any developer who daemonises
    seedpod on the machine tart runs on, which is the entire local-dev deployment model.

    **The original diagnosis was wrong, and it is worth recording why.** Smoke 5 concluded that a
    process *reparented away from a live login session* (`nohup`, `ssh -f`, `launchd`) is blocked.
    The A/B behind that was real but under-controlled: it varied parentage and nothing else, so it
    could not see the actual variable. Re-measured 2026-08-09 on minimax (macOS 15.7.2) inside a
    **single detached wrapper** — identical parent process, same second, four live VMs:

    | binary | reparented result |
    |---|---|
    | `/opt/homebrew/opt/python@3.11/bin/python3.11` (what `.venv/bin/python3.11` resolves to) | `EHOSTUNREACH` ×4 |
    | `/usr/bin/python3` | OPEN ×4 |
    | `/opt/homebrew/bin/python3.13` | OPEN ×4 |
    | `/usr/bin/nc` | OPEN |

    Two processes with the **same parent** got opposite answers, so parentage cannot be the
    discriminator; and python3.13 is *also* Homebrew, so it is not a Homebrew-vs-Apple or
    signed-vs-unsigned story. **The grant is per-binary**, and `python@3.11` on that host is in a
    denied state. Parentage only *masks* it: in-session the responsible process is the terminal/SSH
    session, which already holds a grant, so the binary's own record is never consulted — which is
    exactly why the original A/B looked conclusive.

    This also corrects the fix. **Granting Local Network to the resolved interpreter binary** is the
    remedy (`readlink -f .venv/bin/python3.11`); recreating the venv does nothing, because the venv
    path is not what is judged. Running inside one live session still works, but it borrows the
    session's grant rather than fixing anything.

    Two caveats left open: the system TCC store needs sudo/Full Disk Access to enumerate, so
    **which** binaries are listed was never read directly — the result is behavioural. And there is
    no v1 checkout on minimax any more, so "v1 never hit this on the same machine" could not be
    re-verified; under the per-binary rule it needs no special explanation anyway.

    ~~**Document it**~~ **DONE** — `docs/guides/tart-local-dev.md`, the first doc under
    `docs/guides/` (DR-0001 reserved it; nothing had needed it until now).

    ~~**Make it diagnosable**~~ **DONE, and the fix was bigger than this item** — see **DR-0033**.
    Investigating this surfaced that the engine gate discards `NotReady.detail` on timeout, so
    *every* gate in v2 was undiagnosable, not just this one: `deploy.await_wave` already computed
    the exact list of services that never came up and reported a bare timeout instead. Both layers
    are fixed, and `k3s.await_ssh` now fails with
    `gate timed out after 180.0s; last poll: ssh port not open yet: [Errno 65] No route to host`
    (that string measured, not guessed — `asyncio` never emits the name `EHOSTUNREACH`, so the
    errno number is the part to match on).
    The backlog's other suggestion — a vmnet probe in tart's `check_ready` — was rejected with
    reasons recorded in the DR: `check_ready`'s IO goes through the injected transport and
    conformance forbids `Mock`/`patch`, so a raw socket there is untestable by the suite's own
    rules, and it would have helped tart only.

17. **`provider_host` + Ingress is invalid on any IP-host provider — DONE and PROVEN**
    (2026-08-09; found by smoke 8, fixed the same day, **proven on real DigitalOcean by smoke 9**
    the same day). *Size: S–M, as estimated.*

    A `hostname.strategy: provider_host` profile resolves `cluster_hostname` to whatever address
    the provider reports. On DigitalOcean that is an **IP**, and the exampleco-stack Ingress templates
    put it straight into `spec.rules[0].host`, which Kubernetes rejects:

    ```
    The Ingress "mailhog" is invalid: spec.rules[0].host:
      Invalid value: "203.0.113.13": must be a DNS name, not an IP address
    ```

    So **`exampleco-staging-stack-nodns` and `exampleco-dev-stack-nodns` could not deploy to DigitalOcean at
    all** — smoke 8's first attempt died on exactly this, after wave 1's ten other documents had
    already applied. It was latent on `tart`/`kind`, whose provider host is a DNS name
    (`minimax.local`), which is why no earlier smoke saw it. The templates already guarded the
    *absent* case (`{% if cluster_hostname %}`, written for DR-0025's `None`), but **an IP is
    truthy**, so the guard passed and emitted an invalid host.

    **The fix**: an `is dns_name` Jinja **test**, `_is_dns_name` in `services/manifests.py`,
    registered on the manifest `Environment` beside `| b64encode` (the same seam #14 used). Applied
    at the 10 sites Kubernetes actually validates — `spec.rules[].host` and `spec.tls[].hosts`, in
    all five Ingress-bearing templates.

    Three decisions worth carrying:

    - **It is a template test, not a hostname-resolution change, and that is the whole point.** An
      IP is perfectly valid in the URLs `frontend-server.yaml` builds from the same variable
      (`https://{{ cluster_hostname }}/api` works fine against an IP), so resolving an IP hostname
      to `None` — the tempting "fix it in one place" — would have re-introduced the `https:///auth`
      rendering DR-0025 exists to prevent. **DR-0025 is untouched**; its three states still mean
      what they meant, and two new tests pin that `None` still gates false and an omitted key
      still raises through `StrictUndefined` (had `_is_dns_name` swallowed `Undefined` into a tidy
      `False`, this fix would have converted a loud DR-0025 failure into a silently host-less
      Ingress).
    - **The predicate rejects IP literals and nothing else** — deliberately narrower than real
      RFC1123 validation, because the two errors are not symmetric: too permissive and kubectl
      rejects the apply loudly, exactly as today; too restrictive and the host silently vanishes
      into a catch-all Ingress that looks fine and routes wrong.
    - **A catch-all Ingress is the right shape for an IP-addressed cluster.** You reach it at the
      IP on Traefik's default self-signed cert, which is exactly what these profiles' `dns.enabled:
      false` + `ssl.enabled: true` asks for. (`use_acme_certs` is `ssl_enabled and dns_enabled`, so
      it was already false here — no ACME-for-an-IP hole.)

    Tests, each **verified failing against the pre-fix tree**: both halves against the REAL shipped
    `exampleco-api.yaml` (an inline fixture would have passed before the fix as easily as after it,
    since the defect was that an IP is truthy), 9 parametrized predicate cases including bracketed
    IPv6 and whitespace-padded IPs, and a mechanical per-template gate so a sixth Ingress template
    cannot reintroduce this on a profile nothing routinely smokes.

    **PROVEN by smoke 9** (2026-08-09, full report under §12): `exampleco-staging-stack-nodns` on
    DigitalOcean, droplet IP `203.0.113.11`, all four Ingress objects applied as catch-alls —
    no `host`, no `tls` — where smoke 8 had died on exactly that document set. **The first
    `provider_host` profile v2 has ever deployed anywhere.**

    **It also closed verify item (f)** — deferred hostname rendering (DR-0025 E2) — in the same run,
    and more cheaply than expected. The backlog had said proving E2 needed `tart`, because the
    rehydrated droplet IP could not render into an Ingress. Once it could, E2 proved out on the
    provider four smokes already knew well, with no `tart` and no macOS Local-Network setup.

    **The strongest evidence is the two halves coexisting.** On one live cluster, from one variable:
    the IP absent from every Ingress host, and present in every URL
    (`KEYCLOAK_PUBLIC_URL = https://203.0.113.11/auth`, the five `window.APP_CONFIG` blocks),
    with zero `https:///` anywhere. That is the "template test, not a resolution change" decision
    validated on real infrastructure rather than argued.

18. **Step failures discard `ProviderError.detail` — DONE** (2026-08-09; found by smoke 8 the same
    day, fixed before the next smoke as the item asked). *Size: S, as estimated.*

    `classify_subprocess` builds `detail = {"exit_code": rc, "stderr": stderr}` and every provider
    error carries it. `engine._failure_message` persisted only `str(exc)`, so `workflow_steps.error`
    ended up as `{"kind": "permanent", "message": "kubectl.apply_manifest: invalid input"}` — the
    stderr that says *which document and why* was computed, attached, and then dropped. Diagnosing
    smoke 8 required decrypting the audit blob and running `kubectl apply --dry-run=server` against
    the live cluster to recover a message the process already had.

    **The fix** (`engine/engine.py`), the same shape as DR-0033 point 1 and needing no DR:
    `_failure_message` now appends `_stderr_suffix(exc)` — `detail["stderr"]` only, since
    `exit_code`/`status` are already in the classifiers' own message text (`"exited 2"`,
    `"auth failed (403)"`). The classifier's message is **preserved and appended to**, never
    substituted. Capped at `_MAX_STDERR_CHARS = 2000` keeping the HEAD, because kubectl/API
    validation errors lead with the reason; the truncation says how much it dropped, since nothing
    else persists the remainder. Real errors run to a few hundred chars, so it essentially never
    bites. **Kezia, 2026-08-09: no special redaction handling for now** — stderr lands in the
    plaintext `message` column, which is also what the SSE/UI surface renders.

    **A second instance was found and fixed in the same pass**, one layer up: `_park_and_wait`
    caught, classified and discarded *every* `InfrastructureUnreachableError`, so a run that parked
    and re-probed for the full 15-minute `unreachable_budget` failed with nothing but
    `"unreachable_budget exhausted"` — unreachable *how* was the one question the record could not
    answer. `_UnreachableExhausted` now carries the last probe's `reason` and `_exhausted_message`
    appends it (`"; last probe: ..."`), omitted when there is genuinely nothing to say, exactly as
    DR-0033's gate suffix is. **The run-level row was deliberately left alone** on this path: it
    still reads the generic `"unreachable_budget exhausted"` and points at `failed_step`, while the
    step row carries the detail. (The `_fail_and_signal` paths — every non-unreachable failure —
    already mirror the step's full text onto the run row.)

    Three tests, each **verified failing against the pre-fix engine**, in
    `tests/engine/test_gates_schedule_park.py`: the stderr is carried, an error with no `detail`
    reads exactly as before (no dangling `"; stderr: "`), and a pathological stderr truncates
    visibly. Plus the `_UnreachableExhausted` assertion added to
    `test_unreachable_budget_exhausted_forward_fails_and_skips_compensation_entirely`.

    **This was the fourth and fifth instances of one recurring shape** — v2 computes the reason and
    discards it before an operator sees it. The others: gate timeouts and `ProbeSshPort` (both fixed
    by DR-0033), and smoke 4's empty-error-message bug (fixed). **Finding a fifth while fixing the
    fourth is the point**: when something in v2 is hard to diagnose, look for the drop, not for
    missing instrumentation.

19. **The stack was proven to START, never to FUNCTION.** *Size: M; raised by smoke 8, 2026-08-09.*

    Smoke 8's 19 secrets are **placeholders** (one shared value, plus `s3_access_key` matching the
    profile's `MINIO_ROOT_USER` literal). Every pod reached `Running 1/1` and the migration Job
    `Completed`, which proves manifests render, images pull, waves order and readiness probes pass.
    It proves **nothing about the application working**: Keycloak auth, S3/minio access, RabbitMQ
    publish/consume and the GitHub-token paths were never exercised, and `tailscale` was in
    `CrashLoopBackOff` throughout (expected — placeholder key, `required: false`).

    A readiness probe answering on a port is not evidence the service is correct. **If v1 is to be
    deprecated, something has to exercise the app itself** — at minimum a login through Keycloak and
    one round-trip that touches postgres, minio and rabbitmq. Until then "the stack deploys" is the
    honest claim, and "the stack works" is not.

20. **No SPA test suite, no CI — PARTLY DONE** (2026-08-11, Kezia chose GitHub Actions +
    the first SPA tests). `.github/workflows/ci.yml` is **the first CI this repo has ever had**:
    `uv sync --locked` → `ruff check` → `pytest` on one job, `npm ci` → `npm test` → `npm run build`
    on another. Deliberately the **floor**: it holds no provider credentials, runs no smoke, and
    would have caught none of the last three sessions' defects — those came from real infrastructure.
    What it catches is the class nobody should spend a droplet on. First SPA tests landed with it
    (vitest, 12 tests): `time-utils` — which is where smoke 6's `NaNh NaNm` bug lived, now pinned
    directly — and `MiniEventHud`'s `transition` (#21). **Still open:** the rest of the SPA is
    untested, and `formatEventData` is still a non-exported closure, so the HUD's other formatters
    are not yet reachable from a test.

    The original write-up follows.

    **No SPA test suite — nothing pins the UI at all.** *Size: M; standing since §6, sharpened by
    smokes 6-8.*

    Promoted from prose to an item because it is a confidence gap, not a nicety: **four UI defects
    were found by hand** across smokes 6-7 (three `Invalid Date` families, the HUD's `status`
    misread, and the SSE keepalive) and **none of them could fail a test**, because there are no
    tests. `ui/src/lib/time-utils.js` is pure and testable with zero setup and vitest reuses the
    existing `vite.config.js`; `formatEventData` is a non-exported closure inside `MiniEventHud.jsx`
    and needs extracting first. **There is no CI in this repo at all**, so any suite is local-only —
    which is itself worth deciding about before cutover.

21. **The MiniEventHud renders a deployment's birth as `? → pending` — DONE** (2026-08-11). The
    formatter is now an exported pure `transition(data)` that distinguishes a deliberately empty
    `old_status` (birth → `→ pending`) from an absent one (`? → …`), and it is **unit-tested** — the
    first component test in the repo. Made a named function rather than another inline `||` chain
    because this is the **third** defect of one class in this component: `|| "updated"` on
    `deployment_status_changed` (smoke 6), `data.status` on `pod_status_changed` — a field neither
    v1 nor v2 ever sent (DR-0035) — and this one. The common shape is a falsy-but-meaningful value
    treated as absent.

    Two deliberate decisions meeting badly: `core/machine.py` sends `notify_old_status=""` at all
    four birth transitions (`:287, :299, :750, :761` — "preserves v1's UI-visible birth broadcast
    shape"), and the HUD's `data.old_status || "?"` renders empty-string as `?`. A **deliberately
    empty** value therefore displays identically to a **missing** one — the same conflation class as
    the `|| "updated"` bug, except here the server is right and only the display misleads. The
    honest fix distinguishes `""` from `undefined` and renders birth as `→ pending`.

22. **v2 NEVER CREATES DNS RECORDS — a DNS-profile cluster is unreachable at the hostname it
    advertises.** *Size: M. Found by smoke 10, 2026-08-10.* **DONE AND PROVEN** — built under
    [DR-0034](decisions/DR-0034-dns-records-both-directions.md) together with #6, and proven on real
    infrastructure by **smoke 11** (2026-08-10): `dig` returns the droplet IP from three resolvers,
    the stack serves HTTPS at its own name with no `Host:` override, and destroy removes the record
    from the Cloudflare zone.

    **What landed.** The catalog goes 30 → 32: `dns.create_record` (service plane, undoable —
    `DnsRecordUpserted.created` is what makes the undo delete only what the run itself created) and
    `cluster.store_dns_record` (domain plane). `cluster.load_spec`'s Output gains
    `dns_intent: DnsIntent | None`, read from `provider_config["dns_config"]` — the profile's `dns:`
    block, which `_provider_config_from` now carries at birth when and only when it is enabled
    (v1's own rule, `cluster_manager.py:318-321`). Migration `0002` adds `clusters.dns_record_id`
    beside the two DNS columns that already existed and were never written; `DnsRecordRef` reads the
    three **columns** now, not v1's `provider_config` blob. Both steps sit in all four
    `provision-*.yml` right after the address gate.

    **Two decisions worth carrying forward.** (1) A DNS failure now **fails the run** rather than
    being best-effort as v1 was — v1's "not critical" was true in v1's world, and is not true when
    the hostname renders into every Ingress `host` and every app URL (DR-0034 decision 7, put to
    Kezia explicitly and ratified). (2) The record's FQDN and the manifests' `cluster_hostname` are
    computed by two different functions and **must** agree; a test drives both ends off the shipped
    `exampleco-staging-stack.yml` to pin it. **DR-0025 is untouched** — nothing rehydrates the hostname
    from the column.

    **The proof, delivered by smoke 11** (full report below): `dig` → `203.0.113.10` from
    1.1.1.1, 8.8.8.8 and the system resolver; `https://<hostname>/` → **HTTP 200** serving
    `<title>Exampleco Supplier</title>` with curl resolving the name itself; and the negative control
    that makes it conclusive — **the same IP returns 404 for a wrong hostname**, so this is genuine
    name-based Ingress routing, not a catch-all. After destroy: 0 records in the zone, authoritative
    `dig` empty, 0 droplets.

    The original write-up follows.

    `exampleco-staging-stack` (the DNS profile) resolved `cluster_hostname` to
    `preset-…-e35dbd4d.cluster.example.com`, rendered it into all four Ingress `host` fields
    and into every app URL (`KEYCLOAK_PUBLIC_URL`, `FRONTEND_URL`, the `window.APP_CONFIG` blocks).
    **`dig` returns nothing.** The stack was only reachable by IP with a `Host:` header, which is
    how smoke 10 exercised it.

    Three findings, one root cause:

    - **There is no `dns.create_record` step verb.** `services/dns.py:118` *has* `create_record`
      (salvaged from v1's `CloudflareDNSProvider`), but `engine/steps/dns.py` defines only
      `dns.delete_record`, and **DR-0022's ratified 30-verb catalog has no create counterpart** —
      the table at DR-0022:146 lists `dns.delete_record` alone. So this is not a verb that regressed;
      it was never in the vocabulary. `provision-digitalocean.yml` ends at `assign_project` with no
      DNS step, and no shipped workflow references DNS creation.
    - **`clusters.dns_hostname` has no writer at all.** It is set to `None` at birth
      (`deployment_service.py:1368`) and by reconciliation (`:426`), and nothing ever sets it to a
      real value. **The API is not at fault**: `api/routers/clusters.py:38` already derives
      `cluster_url = f"https://{dns_hostname}"` when a hostname exists, so the SPA would render it
      the moment it is populated. The column, the API and the UI are all ready for a value nobody
      writes.
    - **The hostname IS computed and then discarded** — resolved at deploy time, rendered into every
      manifest, never persisted to the record the API serves. **A sixth instance of this repo's
      recurring shape** (see #18): v2 works out the answer and drops it before an operator sees it.

    **Why nine smokes missed it.** Every previous run verified pods reached `Running` and stopped
    there. Nothing ever asked whether the advertised hostname resolved — and on the `-nodns`
    profiles (smoke 9) there is no hostname to resolve, so the question never arose. It took the
    first run that actually *used* the app to notice.

    **The destroy side is the mirror image and is equally confused.** `destroy-cloud.yml:63` calls
    `dns.delete_record` and it reports `succeeded` — deleting a record that was never created.
    Backlog **#6** tracks DNS-on-destroy as the gap; in fact destroy is the half that exists.
    **#6 and #22 should be scoped together**: `cluster.load_infra`'s domain-step Output (#6's
    dependency) is the same fact a create verb would have to persist.

    *Scoping note for the fix:* adding a verb requires a DR (DR-0022's catalog is ratified; its
    Erratum E11 completeness gate is a hard assertion, so `_build_step_registry` and the catalog
    must move together). Decide at the same time where `dns_hostname` gets written — almost
    certainly the same domain step — so the API stops serving `null` for a cluster that has a name.

23. **`ui-contract.md` obligation 5 — DONE** (2026-08-10,
    [DR-0035](decisions/DR-0035-ui-contract-obligation-5.md)). *Found by smoke 10.*

    **This item's own premise was two-thirds wrong, and that is the finding worth keeping.** It
    claimed none of the three topics was emitted by v2. Checked against source:

    - **`reconciliation_skipped` was already emitted and tested** — `runtime/reconciliation.py:497`,
      environment-scoped per DR-0010, `tests/runtime/test_reconciliation.py:137`, with a payload
      matching what the HUD reads. Nothing to do.
    - **`snapshot_restore_completed` was already emitted and tested on the REST path** —
      `api/routers/snapshots.py:197`, `tests/api/test_features.py:333`. The real gap was the
      *workflow* path (`deploy.restore_snapshot`), which is the path smoke 10's restore took.
    - **`pod_status_changed` was genuinely absent** — the one whole missing topic.

    So this was one topic and one path, not three topics. **Third instance of the standing lesson**
    (design docs describing intent as though it were built) — this time in the opposite direction,
    with prose *under*-describing what exists. Grep before scoping, in both directions.

    **What landed.** `pod_status_changed` is **consciously dropped**: v2 deliberately replaced v1's
    `watch_pods` SSE task with `deploy.await_wave`'s per-poll `ctx.progress` → `workflow_progress`,
    and the SPA was never told, so `PodDetail`/`ContainerDetail`/`ClusterDetail` now listen for
    `workflow_progress` filtered on `cluster_id`. `deploy.restore_snapshot` emits `ctx.progress` on
    success **and on each failed attempt with the reason** (DR-0033's lesson: a 19-attempt restore
    was silent for its whole ~180s budget; smoke 10's 39s restore emitted nothing at all), and
    `ClusterDetail` refetches restore history on it — so the page behaves identically however the
    restore was triggered. Option (b), a `runtime/` watcher over the already-complete
    `KubeWatchPods`, was weighed and declined: a new long-lived task per cluster needs a lifecycle
    owner and `HealthMonitor` is explicitly not it. `KubeWatchPods` stays built and unused, which is
    what a future watcher would use.

    **Accepted limitation, stated rather than hidden:** progress flows only *during a workflow run*,
    so pod churn on an idle ACTIVE cluster is not live. v1 was no better (it watched only during a
    rollout).

    **Two defects found while discharging it, both fixed.** The HUD's `pod_status_changed` formatter
    read `data.status` — a field **neither v1 nor v2 ever sent** (v1's payload had `phase`), so that
    line rendered `?` even against v1: the same conflation class as the `|| "updated"` bug smoke 6
    fixed. And `event-store.js` never subscribed to `workflow_progress` at all, though the HUD had a
    formatter case for it and listed it as verbose — so the live signal could never reach the
    buffer.

    **Not provable by the suite** (there is still no SPA test suite — #20). The evidence is the next
    smoke: watch a pod page refresh by itself during a deploy, and the HUD show restore progress
    where smoke 10 saw 39 seconds of silence.

24. **Let's Encrypt certificates are asked for and never configured — DONE AND PROVEN** (2026-08-10,
    [DR-0036](decisions/DR-0036-acme-certificate-resolver.md)). *Found by smoke 11, proven by
    smoke 12:* the served certificate is issued by `C=US, O=Let's Encrypt, CN=(STAGING) Dastardly
    Durum YR1`, SAN exactly the cluster hostname, 90-day validity — and plain `curl` correctly
    REJECTS it (verify result 20), which is what an untrusted staging CA should do.

    **The fix was smaller than porting v1, because reading both v1 sources changed the shape.**
    v1 had **two competing `HelmChartConfig`s for the same object**: the installer wrote one into
    `/var/lib/rancher/k3s/server/manifests/` before k3s started (ports/hostPort), and
    `_apply_traefik_config` `kubectl apply`-ed a second at DEPLOYING carrying a *poorer* ports block
    **plus** ACME — silently overwriting the first. v2 had salvaged the better one
    (`providers/ssh_k3s.py`) and lost ACME with the other. So the resolver was **folded into the
    config v2 already writes** rather than porting the second writer: one manifest, landing before
    Traefik's initial install, no reconfigure-and-restart, **no new step verb** (DR-0022 stays 32).

    `AcmeConfig` (`core/acme.py`) mirrors `DnsIntent` exactly — read from
    `provider_config["ssl_config"]`, which `_provider_config_from` now carries when enabled (v1
    verbatim), gated on **ssl.enabled AND dns.enabled** (v1's `use_acme_certs`). **A test pins the
    client and server halves to agree** across all three shipped profiles: an annotation naming a
    resolver nobody configures is precisely what this item was.

    Also fixed while there (DR-0036 decision 4): the config was written **only** on the `hostport`
    path, so a `loadbalancer` profile with ssl+dns would have rendered the annotations and got no
    resolver — the same defect one branch over. The writer now fires on "hostport OR acme", with the
    ports/service block still gated on hostport.

    **`exampleco-staging-stack.yml` moves to the LE STAGING directory** (DR-0036 decision 5, Kezia's
    call): production allows 50 certs/week per *registered domain* and every ephemeral cluster burns
    one. Consequence to know: **no shipped profile now targets LE production**, so ephemeral certs
    are untrusted-by-design and browsers will warn. A future non-ephemeral profile sets production —
    and should enable Traefik persistence at the same time, since `/data/acme.json` is an `emptyDir`
    today (decision 7).

    **Smoke 12 delivered the proof** (full report above), and found the fix was initially broken by a
    one-line omission its own tests could not see — see the smoke-12 report for why, it is worth
    reading before writing the next round's tests. There is deliberately no issuance gate: Traefik
    only requests a cert once a router exists, i.e. after the deploy, so a provisioning gate would
    have nothing to wait on (decision 6). The smoke check replaces it.

    The original write-up follows.

    `exampleco-staging-stack.yml` is fully configured for ACME — `ssl.enabled: true`, `acme_server`
    (production Let's Encrypt), `acme_email: kezia@example.com`, `challenge_type: httpChallenge` —
    and v2 renders the client half correctly: `use_acme_certs = ssl_enabled and dns_enabled`
    (`services/manifests.py:1078`, v1 verbatim) evaluates true, so **all four Ingresses carry
    `traefik.ingress.kubernetes.io/router.tls.certresolver: letsencrypt`** (verified in smoke 11's
    decrypted deployment audit).

    **Nothing in v2 ever defines a certresolver by that name.** v1 applied a Traefik
    `HelmChartConfig` carrying `certificatesResolvers.letsencrypt.acme` (email, caServer, and the
    httpChallenge/tlsChallenge entrypoint) from `_apply_traefik_config`
    (`reference-code/seedpod/seedpod/core/state_manager.py:1012-1090`), dispatched at the DEPLOYING
    transition alongside `_create_dns_record_if_configured`. **`provision-digitalocean.yml` has no
    Traefik step at all** — only `provision-kind.yml`/`provision-orbstack.yml` apply a Traefik
    manifest, and those are the hostPort/ingress-controller shim, not ACME. So Traefik receives a
    router asking for an unknown resolver and falls back to `CN=TRAEFIK DEFAULT CERT`.

    **It was unreachable until 2026-08-10, which is why no earlier smoke could have found it.**
    HTTP-01 validation requires the advertised hostname to resolve to the cluster; until #22 nothing
    created the DNS record, so ACME could never have succeeded even had the resolver been configured.
    #22 is what makes this item actionable.

    *Scoping notes:* the natural home is a step in `provision-digitalocean.yml` (and `tart`)
    mirroring the existing `traefik_apply` shim — `kube.apply_file` cannot take a rendered template,
    so this likely needs the config rendered from `ssl_config` rather than a static file, which is a
    real design question, not a copy. Note v1's own version was **best-effort** (it logged and
    continued on failure); given DR-0034 decision 7's reasoning, whether a cert failure should fail a
    run is a genuine question and not a foregone conclusion — a self-signed cert still serves, unlike
    a hostname that does not resolve. Check the `-nodns` profiles keep working: their own comment
    ("no certresolver annotation = Traefik uses auto-generated self-signed cert") is the intended
    behaviour there, and `use_acme_certs` is already false for them.

---

## Operational readiness — v1 is off; what would hurt now?

Added 2026-08-09 as "Cutover readiness — can v1 be deprecated?", **rewritten 2026-08-10 (Kezia:
"v1 is no longer live so not worried about actual deploy/rollback")**. The original framing asked
what stood between v2 and switching v1 off. That question is answered by events: v1 is
decommissioned and v2 is the control plane. The section is kept — with a different question — because
the risks changed shape rather than disappearing.

### What is proven on real infrastructure

- **Both production providers**, full lifecycle, nothing left behind — DigitalOcean (smokes 4, 6, 7,
  8, 9, 10, 11, 12) and `tart` (smoke 5).
- **A real multi-tier deployment** — `exampleco-stack`, 4 waves, gates that held, on **both hostname
  strategies**: DNS (smokes 8, 10, 11, 12) and `provider_host`/IP (smoke 9).
- **The stack WORKS, not merely deploys** (smoke 10): Keycloak login against a restored realm,
  authenticated reads of restored data, S3 round-trip, live AMQP.
- **DNS both directions** (smoke 11): the advertised hostname resolves and serves, and destroy
  deletes the real record.
- **Real ACME certificates** (smoke 12): Let's Encrypt staging, SAN = the cluster hostname.
- **Redeploy and supersession**, **rollout restart** both branches, **the GHCR pull secret**,
  **snapshot restore**, **resume across a restart**, **the SPA driven end to end through the UI**.

### The three things that would actually hurt

The cutover-era list is retired. What replaces it is narrower, and none of it is about v1.

1. **Fernet key custody — WRITTEN UP as operator policy** (2026-08-11, Kezia: *"I guess this is
   operator policy; part of the runbook"*). `docs/guides/operations.md` §1 states what the keys
   encrypt, that loss means reprovisioning rather than recovery, and the one requirement: a second
   copy that does not share a failure mode with the laptop. **One line is deliberately left blank
   for Kezia** — where that copy lives. Everything else in the section is fact; that line is policy,
   and the runbook says so rather than inventing an answer.

2. **DB loss — MEASURED** (2026-08-11, `tests/runtime/test_db_loss_recovery.py`). It was untested;
   it is not any more, and no droplet was needed. A rebuilt DB recovers **inventory and the ability
   to destroy** — reconciliation births an `UNMANAGED` row from the `seedpod-managed` tag, and
   `cluster.load_infra` gets everything `infra.destroy_instance` needs from it — and **nothing
   else**: kubeconfigs, secrets, deployment history and DNS record ids were all *in* the database.
   **It will not destroy anything on its own**: rediscovery lands in `UNMANAGED` and the zombie
   sweep only reads rows the DB believes are `DESTROYED`, so a rebuilt DB (which believes nothing)
   sweeps nothing. That one is pinned by a test because the cost of being wrong is destroyed
   production infrastructure. Two consequences documented in the runbook: an adopted cluster's DNS
   record is orphaned on destroy (delete by hand), and every adopted cluster is labelled
   `environment="production"` (verify flag 10 / DR-0013, still ratified-but-unobserved on real
   infrastructure — though its consequence is now bounded).

3. **Migrations are forward-only — FLAGGED where the work happens** (2026-08-11, Kezia: *"so long
   as we flagged this we can solve it when we do a destructive migration"*).
   `seedpod/data/migrations/README.md` sits beside the files it governs and states the rule, what
   the database uniquely holds, and the three things to do before a destructive migration. Still
   true and deliberately unsolved: there is no reverse path and no automatic backup.

**Not blockers, on the evidence:** the step-verb catalog, wave orchestration, both providers, the
SPA's feature set, DNS, ACME, and the parity gate — all done and exercised on real infrastructure.

**Still open, but ordinary work rather than risk:** #20 (no SPA tests, no CI — every guarantee
outside `pytest` rests on someone running a smoke by hand and reading it carefully), and the P1/P2
fidelity items.

---

## Suggested order

~~§0a + §0b~~ → ~~§12 smokes 1-3~~ → ~~P0 #0's provision half (Round 8a)~~ →
~~P0 #0's destroy half (Round 8b)~~ → ~~P0 #0b/#0c/#2–#4 (the manifest-resolution gaps, Round 9)~~ →
~~P0 #0's deploy half (Round 10)~~ → ~~smoke 4 — a real deployment, on DigitalOcean~~ →
~~#13 (the redeploy strand)~~ → ~~#14 (the two never-round-trip manifest fields)~~ →
~~smoke 5 — the same on `tart`~~ (PASSED, and the restart branch fired) →
~~#15 (document + make diagnosable the macOS Local Network trap)~~ (DONE 2026-08-09 — diagnosis
corrected to per-binary, `docs/guides/tart-local-dev.md` written, DR-0033 made every gate in v2
diagnosable, not just this one) → ~~P0 #0d (init-container removal)~~ (DONE 2026-08-09, smoke 8 —
multi-tier wave ordering proven first, then the 21 waits removed and re-proven from cold) →
~~#18 (step-failure detail — do it first, it makes everything after it cheaper to debug)~~
(DONE 2026-08-09; it paid for itself immediately by exposing a second, worse instance in the
unreachable-budget path) → ~~#17 (`provider_host` + Ingress)~~ (DONE + **PROVEN, smoke 9**) →
~~smoke 9~~ (PASSED 2026-08-09 — closed #17's evidence **and** verify (f) in one run) →
~~#19~~ + ~~verify (c)~~ (BOTH closed by **smoke 10**, 2026-08-10, on real v1 secrets and a real v1
snapshot) → ~~#22 + #6 (DNS, both directions)~~ (**BUILT 2026-08-10 under DR-0034 — awaiting the
smoke that resolves the hostname**) → **a DNS-profile smoke that checks `dig` and an HTTPS request
with no `Host:` override~~ (**PASSED, smoke 11**) → ~~#23~~ (**DONE, DR-0035**) → ~~#24 (Let's
Encrypt)~~ (**DONE + PROVEN, DR-0036 + smoke 12**) → **#20 SPA tests + a CI decision → #21 → the
rest of P1/P2 → a rollback plan**.

**"The stack works" is now an evidenced claim, not an aspiration** (smoke 10, 2026-08-10). A
Keycloak login against a restored realm, an authenticated API read of restored invoices and
organizations, a real S3 round-trip and live AMQP connections — all on v1's real credentials. That
retires the largest confidence gap in this document.

**#22 and #6 are BUILT** (2026-08-10, DR-0034): the vocabulary gained `dns.create_record` and
`cluster.store_dns_record`, the record is persisted in columns, and the destroy side now deletes a
real record. **It is not yet PROVEN** — no smoke has ever asked whether the advertised hostname
resolves, which is exactly why the feature was missing for ten runs. After that the remaining
blockers are a regression net that is not a person (#20 + a CI decision) and a rollback plan, which
nothing in this repo still describes. **#24 is closed and proven** (DR-0036, smoke 12). **#22, #6 and #23 are all closed** — and #23's premise turned out
two-thirds wrong, which is worth remembering the next time an item is scoped from prose.

**Everything in P0 is now closed.** The ordering above is deliberately *cutover-shaped* rather than
priority-shaped: #18 first because it is small and every subsequent investigation pays for it, then
the two items that block proving something (#17 unblocks verify (f); #19 is the difference between
"deploys" and "works"). See **Cutover readiness** above for what still stands between here and
deprecating v1 — the honest blockers are confidence-shaped, not feature-shaped.

**Both production providers are now proven**, which retires the largest remaining unknown. What is
left is fidelity work (#0d, P1, P2) plus the paths §12 still lists as unproven. **The SPA against a
live v2 is no longer one of them** — smoke 6 (2026-08-09) drove the whole lifecycle through the UI
on real DigitalOcean. That leaves a snapshot restore and multi-tier wave ordering on `exampleco-stack`
(the only place the #0d init-container masking risk actually lives).

**#13 was placed before smoke 5 deliberately, and that ordering held.** It was the one defect smoke 4
left unfixed; sending smoke 5 to `tart` first would have carried a known-broken redeploy path onto an
unexercised provider, re-creating the two-variables-at-once problem that moved smoke 4 to
DigitalOcean. It was ratified, implemented and proven on real infrastructure on 2026-08-09, so
**smoke 5 now inherits a deploy path proven for both first-deploy and redeploy.**

**Smoke 4 went to DigitalOcean, not `tart` — the ordering changed deliberately, and it paid off**
(Kezia, 2026-08-08). The earlier plan sent smoke 4 straight to `tart` because tart is the provider
being standardised on. But Round 10 landed a large amount of new, never-executed surface — wave
ordering, deferred rendering, restore, the seven deploy verbs — and `tart` would simultaneously
introduce a provider no smoke has ever exercised *and* a different operational shape (seedpod must
run ON the VM host, not off-host). Running the new deploy path first against infrastructure already
proven end-to-end three times isolated one variable instead of two.

**In the event this reasoning was vindicated twice over.** Smoke 4 found something (the previous
three were 3-for-3; it is now **4 for 4**), and because the provider was the known quantity, both
diagnoses were unambiguous: #13 is a deploy-path defect, while the 2026-08-08 create failures were an
upstream DigitalOcean outage. On an unexercised provider, either could plausibly have been blamed on
`tart`. `tart` is now smoke 5, against a deploy path already proven — after #13.

**The ordering flipped, deliberately.** The deploy verbs were the obvious next build, but they bind
five DTOs that do not exist and could not be smoke-tested while manifest resolution failed at
`'environment_variables' is undefined`. Doing #0b/#0c/#2–#4 first (now done, Round 9) means the
deploy verbs get built against real resolved manifests instead of invented shapes — and the
`DnsRecordRef` lesson (the fixture stand-in was wrong in a way that would have made the verb unable
to call its own service) says inventing shapes ahead of their consumers is how this goes wrong.

Unordered: P0 #0a-i (gotcha 1 — needs a DR, blocks nothing today) and P2 #10 (Python 3.14).

**§0 is DONE** (`b565f4c`) and **the first smoke has run** (2026-07-20, DigitalOcean): v2 boots,
authenticates, matches rules, and births clusters/deployments correctly — then stopped at an **empty
step-verb registry** (P0 #0). Round 8a built the 14 provision-path verbs and the **second smoke
(2026-08-02) provisioned a real cluster end to end**, which retired P0 #1. The critical path is now
the OTHER half of P0 #0 — the ~16 destroy/deploy verbs — and the second smoke showed why it is
urgent rather than merely next: with `destroy-cloud.yml`'s verbs missing, a destroy strands the
cluster in `destroying` with no API path out, and the droplet had to be deleted through
DigitalOcean's own API. Everything downstream — including the tart smoke — is blocked behind it.
