---
title: DR-0023 — SSH identity reaches the k3s.* verbs through cluster.load_spec
type: decision
status: active
created: 2026-07-20
updated: 2026-07-20
---

# DR-0023: SSH identity is provider config, threaded as typed data through `cluster.load_spec`

**Status: ACTIVE — ratified by Kezia, 2026-07-20.** Raised by the Round-8a `k3s-family` build agent,
which correctly refused to invent a mechanism and shipped a loudly-flagged placeholder instead.

## Problem

Every Seam C command in the k3s plane except `ProbeSshPort` needs an `SSHTarget`
(`providers/contract.py:203-209`), whose `user` and `private_key_path` fields are **required — no
defaults**. Nothing currently carries them to the verbs:

- DR-0022 fixes `k3s.*` as ONE shared, non-late-bound `ssh-k3s` provider used identically by
  `provision-digitalocean.yml` and `provision-tart.yml`.
- The migrated Params for the five k3s verbs carry only `host`/`known_hosts`/`spec`/`extra_tls_san`/
  `rewrite_server_to` — no SSH identity.
- `ClusterSpecification`/`ClusterConfiguration` have no ssh fields; the committed `SshK3sConfig`
  carries only two timeouts; `AppConfig`'s Decision-8 field list has none.

Meanwhile **v1 used a genuinely different identity per provider**, constructing a fresh
`SSHBasedK3sInstaller` per call from that provider's own config section
(`reference-code/.../providers/digitalocean.py:682-683`, `tart.py:409`):

| Provider | `ssh_user` | `ssh_private_key_path` |
|---|---|---|
| `digitalocean` | `root` | `~/.ssh/id_exampleco_testing` |
| `tart` | `admin` | `~/.ssh/id_ed25519` |

Both are still present in `config/providers/{digitalocean,tart}.yml` and are load-bearing: each
provisioned machine's `authorized_keys` trusts only the key its own provider injected at build time.
A single shared identity would break one plane or the other on real infrastructure. This is a real
design question the specs do not answer, so the build agent halted rather than deciding it.

## Decision

**SSH identity is provider configuration, and it reaches the verbs as typed data through
`cluster.load_spec` — DR-0022's P8 applied verbatim** ("Every fact a provider step needs is produced
by a `cluster.load_*` head step and bound in YAML, so V4 type-checks it and `command(params)` stays
pure").

1. `cluster.load_spec`'s Output gains `ssh_user: str | None` and `ssh_private_key_path: str | None`.
   They are `None` for providers with no SSH plane (`kind`, `orbstack`), which — per DR-0022 ruling 5
   / V4's Optional-binds-Optional rule — makes it a type error for a kind/orbstack workflow to bind
   them into an SSH verb. The plane matrix stays enforced by types, not convention.
2. The values are read from the **provider's own `config/providers/<provider>.yml` section**, the
   same file v1 read and which the composition root already parses. There is no second source of
   truth and no duplication into workflow YAML.
3. `provision-digitalocean.yml` and `provision-tart.yml` bind them into the k3s verbs exactly as they
   already bind `provider` from the same head step.
4. `~` is expanded by the config loader, matching `SSHTarget.private_key_path`'s own documented
   contract ("`~` pre-expanded by the config loader").
5. The Round-8a placeholder (`_SSH_USER = "seedpod"`, `_SSH_PRIVATE_KEY_PATH =
   "~/.ssh/id_seedpod_k3s"` in `seedpod/engine/steps/k3s.py`) is **deleted**, not defaulted — no
   fallback identity may survive, because a silently-wrong SSH identity fails at k3s-install time on
   real infrastructure with a confusing error.

## Consequences

- v1's per-provider identity is preserved exactly; no behaviour change, no fleet reconfiguration.
- `command(params)` stays pure and the `ssh-k3s` provider stays a single shared, stateless adapter —
  no per-provider branching inside a step, and no edit to the committed `SshK3sConfig`.
- The identity is V4 type-checked at workflow load time, so a missing binding is a load error rather
  than a runtime SSH failure.
- `cluster.load_spec` gains a dependency on the parsed provider configs (a composition-root wiring
  change, not new domain logic).
- Round 8a's `k3s-family` component is rebuilt against this mechanism before the round completes.

## Erratum (2026-07-20)

**E1 — decision point 1's "type error" rationale was wrong; the k3s Params are `str | None` with a
loud runtime backstop.** This DR claimed that `None` for kind/orbstack "makes it a type error for a
kind/orbstack workflow to bind them into an SSH verb", so "the plane matrix stays enforced by types,
not convention". Verified empirically against `validate_workflow` during implementation, that is
incorrect on both halves:

- `cluster.load_spec`'s Output type is a **single global** `ssh_user: str | None` /
  `ssh_private_key_path: str | None` — it is not narrowed per provider. So under V4's
  Optional-binds-Optional rule, declaring the k3s verbs' Params as required `str` would reject
  **DigitalOcean's and tart's own legitimate bindings too**, breaking the shipped workflows.
- The protection was unreachable anyway: **`provision-kind.yml` and `provision-orbstack.yml` contain
  no `k3s.*` step at all** (both say so in their own headers, per Seam C §5.4's plane matrix). The
  plane matrix is enforced by *workflow composition* — which verbs a provider's file contains — not
  by the optionality of these two fields.

The implemented mechanism therefore types the k3s Params as `str | None` and enforces identity
presence with a **loud `PermanentError` in `_target()`** when either is `None`, which preserves this
DR's decision point 5 ("no fallback identity may survive") exactly: a missing identity fails
immediately and legibly rather than constructing a wrong `SSHTarget`. Everything else in this DR —
identity as provider config, threaded from `cluster.load_spec`, bound in YAML, `command(params)`
pure, per-provider values preserved — stands unchanged and is implemented as written.

## Alternatives considered

- **Bind SSH literals directly in each provision YAML.** Rejected: it duplicates
  `config/providers/*.yml`, and the two copies can silently drift — precisely the class of
  divergence that produced this project's `namespace: kube-system` bug.
- **One shared operator identity for all providers.** Rejected: it is a real, unrequested v1
  behaviour change that would require every DO droplet and tart VM to inject the same key at build
  time. The current fleet does not work that way, so it would break provisioning on first contact.
- **A per-provider identity map inside the `ssh-k3s` provider.** Rejected: it would push
  provider-name branching into a provider that DR-0022 deliberately kept shared and stateless, and it
  would require editing the committed `SshK3sConfig`.
- **Late-binding `k3s.*` like `infra.*`.** Rejected: the k3s plane genuinely is one adapter
  (`ssh-k3s`) used by two machine providers; what varies is the credential, not the implementation,
  so threading the credential is the smaller and more honest change.
