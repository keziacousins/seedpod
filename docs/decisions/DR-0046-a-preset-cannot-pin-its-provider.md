---
title: DR-0046 — a preset cannot pin its provider, so the safe-looking action is the one that bills you
type: decision
status: active
created: 2026-08-16
updated: 2026-08-16
---

# DR-0046: a preset cannot pin its provider

**Status: ACTIVE — ratified by Kezia, 2026-08-16.** Found on 2026-08-16 by deploying the preset
named `exampleco-dev-tart` and getting a DigitalOcean droplet.

## Context

Three facts that are individually reasonable and jointly a trap:

1. **The profile deliberately does not pin a provider.** `exampleco-dev-stack-nodns` has no
   `provider:` key, with a comment saying so explicitly — it used to pin the deprecated
   `orbstack`, and the fix was to make it provider-agnostic and "reach tart via
   `--provider-override tart`".
2. **The default is DigitalOcean.** `deployment_service.py:598` is
   `provider_override or raw_profile.get("provider", self._default_provider)`, and
   `default_provider: str = "digitalocean"`.
3. **A preset has nowhere to store a provider.** `deployment_presets` (0001_initial.sql) has
   `default_branch` and `default_ttl_hours` but no provider column, and `PresetRow` matches.
   `PresetService.deploy` accepts `provider_override` only as a **call-time** argument.

So the preset named `exampleco-dev-tart` cannot pin tart. Deploying from it without remembering
`--provider-override tart` silently provisions a billing cloud droplet where a free local VM was
intended. **The name is the only record of the intent, and names do not execute.**

The contradiction is already written down. `PresetRow`'s own docstring says:

> `/api/presets` is the only Tart provider-override deploy path (Decision 6).

Presets are documented as *the* route to a provider override, and the schema gives them nowhere to
put one. This is the repo's "described as real, actually absent" pattern (PARITY-BACKLOG's table of
three, cited by DR-0040) — with money attached.

## Why it matters more than it looks

The failure is **silent, and biased toward cost**:

- The safe-looking action — reuse the preset someone already configured — is the one that bills.
- Nothing in the response says which provider was chosen; you find out by inspecting the cluster
  row afterwards, or by seeing the invoice.
- The two providers are not interchangeable in consequence: tart is a free local VM, DigitalOcean
  is a billed droplet with a public IP.
- It defeats its own guard. A preset exists precisely so an operator does not have to remember a
  pile of flags — and this is the one flag that costs money to forget.

Observed 2026-08-16: `seedpodctl presets deploy <exampleco-dev-tart> --branch dev` produced droplet
`100000000`. The night before, the identical preset produced a tart VM, because that invocation
passed the flag. Same preset, same branch, same release — different bill.

## Decision

**1. `deployment_presets` gains a provider column** (migration `0003`), named `default_provider`
to match the existing `default_branch`/`default_ttl_hours` convention rather than inventing a
third naming style. Nullable: a preset that genuinely does not care keeps today's behaviour.

**2. `PresetService.deploy` resolves it exactly like TTL already does.** The existing line is
`effective_ttl = ttl_hours or preset.default_ttl_hours or _DEFAULT_TTL_HOURS`; the provider gets
the same shape. Call-time override still wins, so nothing that works today changes.

**2a. The precedence, stated rather than implied (ratified 2026-08-16).** Most specific wins:

1. call-time `--provider-override`
2. the preset's `default_provider`
3. the profile's `provider:` key
4. the global default (`digitalocean`)

So **a preset beats a profile** — and that ordering was argued rather than assumed, because it is
not obvious. A preset's provider is *operator intent*, which is the same kind of thing as the
call-time flag and belongs above the profile. But a profile's `provider:` could instead be a
*correctness constraint* (manifests that only work on that provider), and silently overriding one
would reintroduce exactly the family of silent-wrongness this DR exists to remove.

Resolved by observability rather than by refusal: the preset wins, **and** the effective provider
is named in the deploy response (decision 4) and logged when it overrode a profile's pin. Refusing
on disagreement was considered and rejected — it would make a preset unable to do the one thing
presets exist for, to spare an operator remembering a flag.

The conflict is nearly unreachable today in any case: `exampleco-web-2-kind.yml` is the only shipped
profile that pins a provider, it pins the deprecated `kind`, and it is already superseded by the
provider-agnostic `exampleco-web-2`. That makes this a cheap decision to get right now and an
expensive one to discover later.

**3. The API and CLI expose it** on preset create/update, and `GET /api/presets` returns it —
otherwise the column exists and no operator can set it, which is how this class of gap starts.

**4. The deploy response names the provider it chose.** Independent of the column, and arguably
the higher-value half: a caller should never have to query afterwards to learn whether they just
created a billing resource. `DeploymentResponse` already carries `environment`; `provider` belongs
beside it.

**5. Backfill `exampleco-dev-tart` to `tart`** as part of the migration's rollout on the appliance —
the preset whose name has been promising this since it was created.

## Consequences

- Presets mean what their names say, and the documented "only Tart provider-override deploy path"
  becomes true.
- One migration (`user_version` 2 → 3). DR-0040's Erratum E1 withdrew a column for being a second
  source of truth for something already derivable; this is the opposite case — the value is
  derivable from nothing, and is currently held only in a preset's *name*.
- Decision 4 changes a response shape. It is additive.
- Does not address the global default itself. `default_provider="digitalocean"` remains the
  fallback when neither preset nor profile says. Whether an ambiguous provider should instead
  **refuse** is a separate question, deliberately not settled here — but worth asking, since every
  argument above applies to it too.

## What would pin it

1. A preset with `default_provider="tart"` deploys to tart with no `--provider-override`.
2. `--provider-override` still beats the preset's value (decision 2's precedence).
3. A preset with `default_provider=NULL` behaves exactly as today — profile, then global default.
4. The deploy response names the provider (decision 4), asserted at the API layer where an
   operator would actually read it.
5. Migration `0003` applies cleanly to a database at `user_version=2` and is a no-op at 3.

---

## What actually landed

All five, in `tests/app/test_preset_provider.py` and the migrate/repo suites. Three notes:

**Decision 2a got a better test than it asked for.** Every fixture profile pins
`provider: "fake"`, so the preset-beats-profile assertion is not a contrived scenario — the
profile is genuinely asking for something else and losing. That is the contentious rung proven
directly rather than by construction.

**Rung 4 is deliberately not re-asserted through the preset path.** Since every fixture profile
pins a provider, reaching the global default from here would mean inventing a profile purely to
test a fallback that `tests/app/test_services_deployment.py` already covers at its own layer.
Left uncovered here and said so, rather than contrived into reach.

**Backfill is NOT in the migration** (decision 5 stands, but as a rollout step). A migration that
guessed which existing presets "meant" tart from their names would be doing precisely what this DR
says software must not do — infer intent from a string. `exampleco-dev-tart` gets set explicitly, by
someone who knows, when the release is installed:

```sql
UPDATE deployment_presets SET default_provider = 'tart' WHERE name = 'exampleco-dev-tart';
```

Suite: 2520 passed, 44 skipped.
