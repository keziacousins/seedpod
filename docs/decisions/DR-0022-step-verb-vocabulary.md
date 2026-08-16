---
title: DR-0022 — Step-verb vocabulary: re-normalized namespaces, late-bound infra verbs, and two correctness fixes
type: decision
status: active
created: 2026-07-20
updated: 2026-07-20
supersedes: DR-0004-thin-provider-workflow-verbs.md
amended-by: DR-0034-dns-records-both-directions.md
---

# DR-0022: The step-verb vocabulary, re-normalized before it is built

**Status: ACTIVE — ratified by Kezia, 2026-07-20. The 31-verb catalog may now be built.**
Supersedes DR-0004 (which named the
`kind.*`/`tart.*`/`orbstack.*` families). Direction set by Kezia, 2026-07-20: *"I'm not at all
convinced that the vocabulary doesn't need a redesign — it grew organically,"* and v1
incompatibility is explicitly acceptable.

## Problem

The first real cutover smoke (2026-07-20, commit `f792129`) stopped at
`UnknownVerbError('cluster.load_spec')`: `app/factory.py:_build_step_registry()` returns
`StepRegistry({})` and `seedpod/engine/steps/` contains only an empty `__init__.py`. **No step verb
has ever been built.** The 35 verb names exist only as strings in `config/workflows/*.yml` and in
the `tests/engine/declared_verbs.py` fixture.

An audit of those 35 verbs (spec coverage, v1 salvage, and vocabulary design) found the *backbone*
sound — verbs are a near-1:1 transcription of Seam C's `ProviderCommand` union — but the naming and
layering grown on top of it incoherent, plus **two defects that are correctness, not taste**.

This is the cheapest possible moment to fix it: **no shipped production code depends on a verb
string.** Verb names appear in roughly seven docstrings/comments only — `core/cluster_spec.py:24`,
`engine/step.py:281` (an illustrative comment), `engine/engine.py:394`, `runtime/health.py:107`,
`services/manifests.py:183`, plus hyphenated `kubectl-apply` prose in `providers/{compensation,
kind,orbstack}.py` — and **zero of them are functional references**. Everything else is YAML, one
fixture, and docs. After a build round, the same change costs ~35 step classes plus their tests.

### The defects

**D1 (correctness — latent data loss).** `kubectl.apply` is declared `undoable=True`
(`tests/engine/declared_verbs.py:413`) and `undo_for(KubeApplyManifest)` returns
`KubeDeleteManifest`. The only thing preventing a failed deploy from **deleting the application's
manifests** is that `config/workflows/deploy-waves.yml:18` says `on_failure: report`, so no undo
scope is ever pushed. `seedpod/providers/compensation.py`'s own docstring states the inverse is
"never for application deploy waves" — but nothing structurally enforces it. A one-word YAML edit
(`report` → `compensate`) turns a documented never-do into production behaviour. Seam C §5.5 names
this exact outcome as a regression.

**D2 (seam-law violation).** `kubectl.delete_daemonset` takes `wait`, `wait_timeout_seconds` and
`settle_seconds` as *step params* (`declared_verbs.py:314-322`, bound in `destroy-cloud.yml:32-33`).
Seam C's taste call 2 pins "no command waits, all waiting is an engine gate", and §5.4 says these
constants "become named `interval`/`settle_seconds` parameters on the workflow's **gates** —
preserved as data, deleted as sleeps (crown jewel #17)". This verb reintroduces an in-step sleep.

### The structural incoherences

- **The namespace prefix carries four unrelated meanings**: vendor (`do.`/`kind.`/`tart.`/
  `orbstack.`), a technology split *inside one provider* (`ssh.`/`k3s.` are both the `ssh-k3s`
  adapter, `providers/ssh_k3s.py:173`), domain/repository (`cluster.`/`deploy.`/`kubeconfig.`),
  supporting service (`dns.`), and late binding (`provider.`). Nothing in a name says whether a verb
  is a conformance-covered, compensated `ProviderStep` or a domain step with DB access.
- **Create is per-provider; destroy is generic.** `do.create_droplet`, `kind.create_cluster`,
  `tart.create_vm`, `orbstack.adopt_cluster` all map to `CreateInstance` and **literally share one
  Params class**, whose own docstring says "the shape doesn't change just because the provider name
  does" (`declared_verbs.py:246-254`). Meanwhile `provider.destroy_server` is a single late-bound
  verb. DR-0004 rejected a generic create because "a generic verb would reintroduce provider
  branching inside step implementations" — but `ProviderStep` selects its adapter by **dict lookup**
  (`provider_step.py:93`), not a branch, and the vendor information is already in the filename
  (Conflict 13 dispatches `provision-{cluster.provider}`).
- **Three identical probes wear three names** (`do.await_droplet`/`kind.await_ready`/`tart.await_vm`
  → all `ProbeInstance`, sharing one Params class, differing only in output field name `ip` vs
  `address`), while **two genuinely different probes share one suffix** (`ssh.await_ready` →
  `ProbeSshPort`, `k3s.await_ready` → `ProbeK3s`). Precisely inverted.
- **`provider.destroy_server` has `EmptyParams`** (`declared_verbs.py:447`) but `DestroyInstance`
  requires `slug` and `resource_ids`, and `ProviderStep.command(self, params)` is documented as a
  "pure param → command mapping" with no `ctx` (`provider_step.py:78-80`). The verb as declared
  cannot be a pure `ProviderStep`. Root cause: provision has a `cluster.load_spec` head (Conflict
  10) but destroy has **no load head** — its one needed cluster fact (`dns_record`) is smuggled in
  via the dispatch table instead. Two mechanisms for one job.
- **Conditionals and conjunctions in names**: `kubectl.rollout_restart_if_unchanged`,
  `kubectl.delete_jobs_and_stuck_pods`. The internal counter-precedent is decisive —
  `deploy.restore_snapshot` takes `spec: SnapshotRestoreSpec | None` and is a typed no-op when
  `None`, and is *not* named `restore_snapshot_if_declared`.
- **`kubectl.` straddles thin/composite.** Five verbs are one Seam C command; five are `KubeRun`
  composites with no Seam C command at all. DR-0004's title is "thin provider workflow verbs"; half
  the family isn't one. `kubectl.wave_ready` is the sharpest case — it consumes a deploy-domain
  `Wave` type and issues N commands per poll, yet wears a provider prefix.

## Decision

### Naming principles

- **P1 — The prefix names the step's SUBJECT, not its vendor and not its layer.** Six namespaces
  with fixed meaning: `infra.` (the machine/instance), `k3s.` (the node's k3s — the `ssh-k3s`
  provider), `kube.` (arbitrary cluster resources — the `kubectl` provider), `deploy.` (a
  deployment's manifests/waves), `cluster.` (the cluster record in *our* DB), `dns.` (a DNS record
  via `DnsService`).
- **P2 — Layer is a typed registry property, not a prefix.** `Step` gains
  `plane: Literal["provider","service","domain"]` and `thin: bool` (thin ⇒ exactly one Seam C
  command), enforced by a registry test. This makes "thin provider verb" checkable for the first
  time.
- **P3 — `await_` prefix ⟺ pure gate.** A verb is named `<ns>.await_x` **iff** it is `gateable` and
  its `execute()` is a no-op. Verbs that both actuate and gate keep the actuator name
  (`infra.destroy_instance`, `kube.delete_daemonset`). Machine-checkable.
- **P4 — No conditionals, conjunctions, or heuristics in names.** No `_if_*`, no `_and_`. A
  condition rides in a typed param. Precedent: `deploy.restore_snapshot`.
- **P5 — Optionality is a type; `_optional` is the ONE sanctioned suffix, on loader verbs only.**
- **P6 — Names use the glossary's nouns**: *instance* (never droplet/vm/server), *address* (never
  ip), *resource_ids*. Extends Conflict 16 rule 10 from fields to verbs.
- **P7 — A vendor prefix is permitted ONLY for a capability no other provider has**, and requires a
  Seam C command plus `supported`-set gating. (DR-0004's correct instinct, confined to the axis
  where it is true: `do.apply_firewalls`, `do.assign_project` survive unchanged.)
- **P8 — No `EmptyParams` provider verb.** Every fact a provider step needs is produced by a
  `cluster.load_*` head and bound in YAML, so V4 type-checks it and `command(params)` stays pure.

### The verb table (31 verbs)

| New verb | Replaces | Seam C command | Note |
|---|---|---|---|
| `infra.create_instance` | `do.create_droplet`, `kind.create_cluster`, `tart.create_vm`, `orbstack.adopt_cluster` | `CreateInstance` | 4→1, late-bound. Adoption honesty moves to output `adopted_existing: bool` (already `InstanceCreated`'s field) — honest for *every* provider, not just orbstack |
| `infra.await_instance` | `do.await_droplet`, `kind.await_ready`, `tart.await_vm` | `ProbeInstance` | 3→1. Output `address: str`, one name |
| `infra.fetch_kubeconfig` | `kind.fetch_kubeconfig`, `orbstack.fetch_kubeconfig` | `FetchKubeconfig` (resource_ids variant) | 2→1 |
| `infra.destroy_instance` | `provider.destroy_server` | `DestroyInstance` + `ProbeDestruction` | gains typed Params (P8) |
| `k3s.await_ssh` | `ssh.await_ready` | `ProbeSshPort` | |
| `k3s.trust_host_keys` | `ssh.trust_host_keys` | `CaptureHostKeys` | |
| `k3s.install` | `ssh.install_k3s` | `InstallK3s` | |
| `k3s.await_api` | `k3s.await_ready` | `ProbeK3s` | now distinguishable from `await_ssh` |
| `k3s.fetch_kubeconfig` | `ssh.fetch_kubeconfig` | `FetchKubeconfig` (ssh variant) | |
| `kube.cluster_info` | `kubectl.cluster_info` | `KubeGetClusterInfo` | |
| `kube.apply_docs` | `kubectl.apply` | `KubeApplyManifest` | **`undoable=False`** — fixes D1 |
| `kube.apply_file` | `kubectl.apply_manifest` | `KubeApplyManifest` | `undoable=True`; the infra shim |
| `kube.await_rollout` | `kubectl.probe_rollout` | `KubeProbeRollout` | |
| `kube.rollout_undo` | `kubectl.rollout_undo` | `KubeRolloutUndo` | |
| `kube.delete_daemonset` | `kubectl.delete_daemonset` | delete + absence probe | **now gateable**; wait/settle params move to `gate:` — fixes D2 |
| `kube.wipe_namespace` | `kubectl.wipe_namespace` | composite (`KubeRun`) | |
| `deploy.load_audit` | same | domain | |
| `deploy.plan_waves` | same | domain (pure) | |
| `deploy.prepare_wave` | `kubectl.delete_jobs_and_stuck_pods` | composite | no `_and_` (P4) |
| `deploy.restore_snapshot` | same | domain | the conditional-as-data exemplar |
| `deploy.ensure_rollouts` | `kubectl.rollout_restart_if_unchanged` | composite | condition lives in `changes:` (P4) |
| `deploy.await_wave` | `kubectl.wave_ready` | composite gate | leaves the provider namespace |
| `cluster.load_spec` | same | domain | output gains `provider: str` |
| `cluster.load_infra` | **new** | domain | `{provider, slug, resource_ids, dns_record: DnsRecordRef \| None}` — the destroy head |
| `cluster.load_kubeconfig` | same | domain | |
| `cluster.load_kubeconfig_optional` | same | domain | **kept and ratified** (P5) |
| `cluster.store_kubeconfig` | `kubeconfig.store` | domain | kills the namespace-of-one |
| `dns.delete_record` | same | `DnsService` | |
| `do.apply_firewalls` | same | `ApplyFirewalls` | vendor prefix survives under P7 |
| `do.assign_project` | same | `AssignToProject` | vendor prefix survives under P7 |

### Additional rulings

1. **Late-bound `infra.*`.** Provider identity flows as typed data from `cluster.load_spec`
   (provision) / `cluster.load_infra` (destroy), V4-checked at load time. Mechanism: a
   `LateBoundProviderStep(ProviderStep)` in `seedpod/engine/steps/` overriding `execute`/`undo` to
   read `params.provider` instead of the `provider_name` ClassVar. The adapter remains a **dict
   lookup, never a branch** — DR-0004's stated fear, answered structurally rather than by naming.
   *Ratified by Kezia, 2026-07-20: the loss of `orbstack.adopt_cluster`'s vividness is an accepted
   trade, since `adopted_existing` is honest for all providers.*
2. **`cluster.load_infra` SUPERSEDES the dispatch table's `dns_record_ref(cluster)`.** *Ratified by
   Kezia, 2026-07-20.* `WorkflowDispatch.resolve()`'s destroy arm becomes `{"cluster_id": ...}`,
   matching provision; the `DnsRecordRefResolver` Protocol (`engine/dispatch_table.py:23-33`) is
   deleted. Rationale: the dispatch-table value is a **snapshot taken at dispatch time**, so it is
   stale on any retry or crash-resumed run, whereas a load step reads fresh at run time; it is one
   mechanism instead of two for "get cluster facts into a workflow"; it deletes a Protocol +
   composition-root hook that exists for a single field; and it is what makes P8 (and therefore the
   `infra.destroy_instance` purity fix) possible at all. Conflict 13 is amended accordingly.
3. **`kube.apply_docs` is `undoable=False`** (D1), recorded in `providers/compensation.py`'s
   `KubeApplyManifest` arm and Seam C §5.5. Makes the regression **unrepresentable** rather than
   commented-against.
4. **`kube.delete_daemonset` becomes gateable** (D2); `wait`/`wait_timeout_seconds`/
   `settle_seconds` leave Params for `gate: {timeout_seconds: 45, interval_seconds: 3}`. The v1
   edge behaviour it protects (gotcha 10 — the 48-hour lingering Tailscale node) is **preserved as
   gate data**, not deleted.
5. **The `_optional` loader-pair convention is RATIFIED, not collapsed** (P5). `engine/config.py:537-545`
   (`_is_assignable`) implements V4's rule that an `Optional[T]` source binds only an `Optional[T]`
   param. A single loader returning `SecretStr | None` would force every strict-plane consumer to
   accept `None`. Recorded here so it stops being re-litigated as an inconsistency.
6. **`k3s.await_ssh` is added to `provision-tart.yml`**, resolving the asymmetry whereby DO and tart
   — the same ssh-k3s plane — had different readiness semantics (gate `READINESS_TIMEOUT` vs
   `RETRY_EXHAUSTED`) because of what DR-0004 happened to enumerate.

### Doc debts settled here (owed regardless of this DR)

7. **Seam C §5.3/§5.5 gain `ApplyFirewalls` and `AssignToProject`** — both are real, implemented
   (`providers/contract.py:414,437`; `providers/digitalocean.py:127-128`) and referenced by shipped
   verbs, but appear **nowhere under `docs/`**. DO-only, `supported`-gated, no inverse.
8. **Seam B §2.2 Proofs 1–3 are amended in place** to the ratified verb names. They are *already*
   superseded by Conflicts 8/9/10/14 (Proof 1 still names the deleted `DeployCancelled`; Proof 2
   predates `cluster.load_spec`), so this is an amendment already owed. **The GRAMMAR fence and
   validators V1–V10 are untouched — only verbs move.**

## Consequences

- **Production code: ~zero churn.** ~7 docstring/comment mentions, none functional. New work: one
  `LateBoundProviderStep`, two `Step` ClassVars (`plane`, `thin`), and the deletion of
  `DnsRecordRefResolver`.
- **Config: 8 YAMLs, mostly mechanical.** 52 `uses:` lines, 92 `{from: …}` refs; ~10 refs change
  because output field names normalize to `address`. Structural edits: both destroy files gain a
  `cluster.load_infra` head; `infra.destroy_instance` gains bound params; the daemonset wait params
  move into `gate:`; `provision-tart.yml` gains `k3s.await_ssh`.
- **Tests: one fixture plus three touch-ups.** `declared_verbs.py` rekeyed (and *shrinks* — ~6
  Params/Output classes merge away); `test_shipped_workflows.py` (7 strings);
  `test_validator.py` (~30, a fake registry — cosmetic); `test_workflow_repos.py` (3 column values).
- **The grammar freeze is untouched.** This DR moves only the registry's verb set — precisely the
  extension point CLAUDE.md designates ("a new need becomes a new step verb, never grammar").
- **New-provider cost drops from 2–3 verbs to zero.** A hypothetical `hetzner` needs a config file,
  an adapter, a workflow YAML, a dispatch row and a conformance harness — no new verbs. A new verb
  is required only for a genuinely provider-unique capability, which is where the DR-shaped review
  cost belongs (P7).
- **the parity backlog (not published) P0 #0 updates** from "35 distinct verbs" to this table, and gains the
  build round as its successor.
- `DR-0004` gets `superseded-by: DR-0022` (the one edit DR-0001 permits on an active DR).

## Erratum (2026-07-20, ratified by Kezia)

Appended rather than edited in place, per DR-0001's append-only rule. Three corrections found by the
Round-8a adversarial judges before any verb was implemented; the ratified body above stands except as
noted here.

**E1 — the catalog is 30 verbs, not 31.** The verb table above contains exactly 30 rows, and the 8
shipped `config/workflows/*.yml` use exactly 30 distinct verbs. The figure "31" in this DR's prose
(and in the parity backlog (not published)) is an arithmetic slip. **30 is authoritative**; the table is
correct and complete as written. No verb is missing.

**E2 — ruling 4 requires `GateDef` to gain a `settle_seconds` field.** Ruling 4 directs the
daemonset's `45`/`3` literals into a `gate:` block, but `seedpod/engine/config.py`'s `GateDef` has
only `timeout_seconds`/`interval_seconds`/`max_consecutive_poll_failures` — there is nowhere for the
settle to go, and folding it into `interval_seconds` does **not** reproduce it (the gate polls at
t=0). This is an **implementation gap against existing spec, not a grammar change**:
`docs/design/seam-c-provider.md:445` already specifies that these physics constants "become named
`interval`/`settle_seconds` parameters on the workflow's `wait-for-readiness` gates — preserved as
data, deleted as sleeps (crown jewel #17)". `GateDef` therefore gains `settle_seconds: int = 0`, and
ruling 4 stands as written. The semantic to preserve is v1's exactly
(`reference-code/.../jobs/state/destruction_job.py:164-181`): delete with
`--grace-period=30 --wait=true --timeout=45s`, then — **only on a successful delete** — a 3-second
grace so Tailscale can send its disconnect to the control plane. The settle is a post-termination
grace, not a poll interval.

**E3 — the Tailscale DaemonSet namespace is `default`, not `kube-system`.** `docs/design/seam-b-engine.md`
Proof 3 pinned `namespace: kube-system`, and both destroy YAMLs inherited it. v1 is authoritative and
says otherwise: its DaemonSet manifest is `namespace: default` throughout
(`reference-code/seedpod/config/manifest-templates/exampleco-stack/tailscale.yaml`) and its cleanup ran
`kubectl delete daemonset tailscale -n default`. Deleting from `kube-system` would find nothing,
"succeed" as NotFound, and silently reintroduce the 48-hour lingering-Tailscale-node bug this step
exists to prevent (v1 gotcha 10). Seam B's Proof 3 is amended in place accordingly (a normative
"what is" doc, edited in place per DR-0001); both destroy YAMLs use `namespace: default`.

**E4 — three implementation questions the judges raised, settled here.** (a) Late-bound provider
resolution for *gateable* verbs lives in `LateBoundProviderStep` as a **template method covering
`poll_ready` as well as `execute`/`undo`**, so the dict-lookup rule exists in exactly one place.
(b) P3's "`execute()` is a no-op" is defined machine-checkably as **"`execute` emits no Seam C
command"** — it returns a provisional Output and invokes no provider. (c) Because `ProviderStep`
hard-defaults `undoable = True`, ruling 3's data-loss fix is made load-bearing by a **registry ↔
`declared_verbs` reconciliation test** asserting each verb's `undoable`/`gateable` flags match the
fixture.

## Erratum, second batch (2026-07-20, ratified by Kezia)

Five further items raised by the Round-8a judges, all cases where the build made a sound choice that
no ratified text authorized. Ratified as follows.

**E5 — `provision-orbstack.yml` gains an `infra.await_instance` gate.** Ratified. Seam C's
`InstanceCreated.address` is genuinely `str | None` (`providers/contract.py:249`), so this DR's 4→1
create merge necessarily yields an Optional address, and ruling 5's Optional-binds-Optional rule then
forbids binding it to `infra.fetch_kubeconfig`'s `rewrite_server_to: str` or to
`ProvisionSucceeded.public_ip`. A gate step resolves it and — more importantly — **makes orbstack
uniform with DO/kind/tart**, which is precisely what this DR's own defect list asked for (it faulted
`orbstack.adopt_cluster`'s Output for "conflating two concerns that every other provider splits
across create+gate"). OrbStack's cluster is always already running, so the probe succeeds on its
first poll. The Consequences list's "four structural YAML edits" is extended to five, and the
`uses:` count from 52 to 56. The rejected alternative — declaring the merged Output's `address`
non-Optional — would make the verb lie about Seam C's actual type.

**E6 — the daemonset gate's poll interval is `interval_seconds: 5`.** Ratified as a deliberate v2
value. Ruling 4 wrote `interval_seconds: 3`; Erratum E2 reassigned that `3` to `settle_seconds` and
left the interval unstated. There is **no v1 cadence to salvage** — v1 used `kubectl --wait`, which
polls internally — so any value is new. `5` (GateDef's default) gives up to nine absence probes
inside the 45-second timeout, which is ample.

**E7 — `dns.delete_record` is `undoable=False` (a third correctness fix, D3).** Ratified; this is
**spec-mandated, not a new call**. `docs/design/seam-c-provider.md:475` states plainly that
`DestroyInstance` / `KubeDeleteManifest` / DNS delete have no inverse — "destruction IS compensation;
never auto-undone". The pre-existing fixture's `undoable=True` contradicted it. This DR's body
enumerated two correctness defects (D1, D2); this is the third, found while implementing.

**E8 — the gate's settle fires on any `Ready`, not only on a successful delete.** Accepted
divergence. v1 slept 3 seconds only when `returncode == 0`, explicitly not on the NotFound branch
(`reference-code/.../jobs/state/destruction_job.py:177-181`). The engine's gate settles on any
`Ready`, so a never-deployed DaemonSet — or a `None` kubeconfig typed no-op — costs 3 idle seconds on
a teardown path. It regresses nothing and special-casing the gate loop to inspect *why* a probe
reported Ready would push step semantics into the engine. Accepted as-is.

**E9 — Seam B §2.2 Proofs 1-2 are amended in place, not annotated.** Ruling 8 directed the Proofs to
be "amended in place to the ratified verb names"; the first implementation instead added a
frontmatter note and left Proofs 1-2 reading `do.create_droplet` / `ssh.install_k3s` / `kubectl.apply`
/ `provider.destroy_server`. Since `docs/design/` is normative "what is" per CLAUDE.md's authority
chain, an annotation leaves the normative spec and the shipped YAML in textual contradiction. The
verb names in Proofs 1-2 are therefore updated in place (a mechanical rename; the Proofs remain
otherwise superseded on their own terms by Conflicts 8/9/10/14, as this DR's body already notes).

## Erratum, third batch (2026-07-20, ratified by Kezia)

**E10 — `docs/design/coherence-review.md`'s YAML snippets are amended in place too.** Ruling 8 and
Erratum E9 named Seam B and Seam C but not coherence-review, which CLAUDE.md's authority chain places
**above** the seam specs and which carries eight `uses:` lines in the superseded vocabulary
(Conflicts 9/10/12/14). E9's rationale applies *a fortiori*: leaving the highest-precedence normative
doc contradicting the shipped YAML is worse than leaving Seam B stale was. Its verb names are renamed
in place; its adjudications are otherwise untouched.

**E11 — the catalog's completeness is machine-checked, not assumed.** The conventions test's
registry-side assertions iterate zero times while the registry is empty, so nothing would notice
Round 8b landing 12 of 30 verbs. A gate asserting `set(registry.verbs()) == DR_0022_VERBS` is added,
marked xfail/skip while the catalog is partial and flipped to a hard assertion when Round 8b
completes. Until then `DECLARED_VERBS` is the only non-vacuous check and it cannot detect a missing
*implementation*.

**E12 — `ProviderStep` declares `plane = 'provider'`.** P2 says `plane`/`thin` are "enforced by a
registry test" but not where the truthful default lives; `Step` defaults to `plane='domain'` and
`ProviderStep` overrode neither, so each of Round 8b's ~15 provider verbs would have had to remember
to declare it, with nothing catching an omission. `ProviderStep` (and by inheritance
`LateBoundProviderStep`) now sets `plane = 'provider'`. **`thin` stays explicit per verb** — composites
like `kube.wipe_namespace` and `deploy.await_wave` are ProviderSteps that issue N commands, so no
default is truthful for them.

## Erratum, fourth batch (2026-08-02, ratified by Kezia)

One item raised by the Round-8a "infra-and-do" component's adversarial judges: a sound,
well-reasoned Round-8a extension of a ratified shape that, unlike E2/E5/E12, was left recorded only
in code docstrings rather than here.

**E13 — `cluster.load_spec`'s Output gains `slug: str` (P8).** The verb table row above authorizes
exactly one shape change to this verb ("output gains `provider: str`"). The late-bound
`infra.create_instance`'s Seam C `CreateInstance` also requires `slug` (conformance C-07's
idempotency key's sibling fact, and the DO/kind/tart naming convention — DO's `k3s-{slug}` droplet
name and its `cluster-{slug}` legacy tag fallback; kind's cluster name is `_cluster_name(cmd.slug)`
verbatim) — a fact `command(self, params)` cannot conjure from `spec: ClusterSpecification` alone,
since `command()` must stay pure (no `ctx`, `engine/provider_step.py`/`engine/steps/late_bound.py`).
P8 is exactly the mechanism this DR already names for such facts: producer is a `cluster.load_*`
head, bound in YAML, V4-checked at load time. `LoadSpecOutput.slug` mirrors `cluster.load_infra`'s
own `LoadInfraOutput.slug` on the destroy side (ruling 2), which already carries the identical fact
for `infra.destroy_instance`'s typed Params. Ratified here (rather than left doc-drifted) for the
same reason E12 gives: leaving the highest-authority artifact describing a two-field Output while
five files (`engine/steps/cluster.py`, `config/workflows/provision-*.yml` ×4) depend on three is
worse than the one-paragraph cost of recording it.

## Alternatives considered

- **Build the catalog as-is, redesign later.** Rejected: the change is ~free now (two docstrings)
  and costs ~35 step classes plus their tests after a build round. The audit's defects would be
  baked in, including D1.
- **Keep per-provider create verbs (DR-0004's position).** Rejected: its rationale ("a generic verb
  would reintroduce provider branching") does not survive contact with `ProviderStep`, which
  resolves adapters by dict lookup; `provider.destroy_server` is a live counterexample in the same
  vocabulary; and the vendor information is already in the workflow filename. Retained only where it
  is *true* — genuinely provider-unique capabilities (P7).
- **Collapse `cluster.load_kubeconfig_optional` into one loader.** Rejected — see ruling 5; it would
  destroy the strict plane's type guarantee.
- **A full reinvention of the vocabulary.** Rejected: the Seam C command → verb backbone is sound
  and most verbs are honest transcriptions. This is a re-normalization (35 → 31), not a rewrite.
