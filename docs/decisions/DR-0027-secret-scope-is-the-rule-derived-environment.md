---
title: DR-0027 — a deployment renders against the rule-derived environment's secrets, not the profile's environment_type
type: decision
status: active
created: 2026-08-06
updated: 2026-08-06
---

# DR-0027: secret scope is the environment the deployment is *recorded under*

**Status: ACTIVE — ratified by Kezia, 2026-08-06.** Raised by Round 9's second adversarial judge on the
`resolved-config` component (its third halt), which caught the build answering the question two
different ways in two methods without a ruling.

## Problem

v2 has **two** environment strings in scope during manifest resolution, and nothing said which one
scopes secrets:

- **The rule-derived `environment`** — produced by `RuleEngine.evaluate()`, passed into `_deploy`, and
  stamped on the cluster and deployment rows.
- **The profile's `environment_type`** — a field of `config/deployment-profiles/*.yml`.

Round 9 wired real secrets in for the first time (previously both `resolve()` call sites silently
passed no secrets at all, which is what let a `tailscale-auth` Secret ship empty). The build then used
`environment` in `_deploy` and `environment_type` in `deployment_preview` — a defensible local choice
in each place, but an inconsistency, and DR-0026's whole premise is that preview predicts deployment.

**v1 unambiguously used the profile.**
`reference-code/seedpod/seedpod/orchestrator/cluster_manager.py:1651-1655`:

```python
manifest_config = manifest_resolver.get_manifest_config(deployment_profile_name)
deployment_environment = manifest_config.get('environment_type', 'ephemeral')
secret_manager = create_secret_manager(deployment_environment)
```

**The two genuinely diverge on shipped config**, which the halting judge believed they did not.
`config/deployment-rules.yml` carries `action: staging_then_manual` (a rule-derived `staging`
environment), while **all five** shipped profiles declare `environment_type: "ephemeral"`. So the first
time that rule fires, v1 semantics render a *staging* deployment against *ephemeral* secrets.

## Decision

**The rule-derived `environment` is canonical. Secrets are loaded for the environment the deployment is
recorded under.**

Rationale, in the order that decided it:

1. **Coherence with the record.** `environment` is what lands on the cluster and deployment rows, what
   `key_class_for_environment` uses to encrypt the audit's resolved manifests, and what scopes SSE
   visibility (DR-0010) and REST-GET filtering. A deployment recorded as `staging` that was rendered
   with `ephemeral` secrets is a row that misdescribes its own contents.
2. **v1's behaviour here is a bug, not an edge to preserve.** `environment_type` selects which manifest
   templates and profile config to use; it is not a statement about which secret set a *particular
   deployment* should draw on. Giving a staging deployment ephemeral secrets is the kind of quiet
   mismatch that surfaces as a mysterious runtime misconfiguration. Per `CLAUDE.md`, this goes on the
   not-ported list rather than being pinned — recorded here, loudly, rather than dropped silently.
3. It is what `_deploy` already does, so the deploy path is unchanged.

### `deployment_preview`

Preview evaluates no rules — its signature takes a profile name plus triggering repo/branch/image — so
it has no rule-derived environment to use. It therefore:

- gains an **optional `environment` parameter**; when the caller supplies one, preview uses it and is
  exact;
- **falls back to the profile's `environment_type`** otherwise, with that approximation stated plainly
  in its docstring and in the response's own semantics.

This keeps DR-0026's "preview predicts deployment" premise honest: exact when the caller knows the
environment, explicitly approximate when nobody does. It is not an API break — the parameter is
optional and existing callers keep working.

## Consequences

- A test must pin the divergence directly: a profile whose `environment_type` differs from the
  environment passed to `_deploy` renders against the **passed** environment's secrets. No shipped
  profile distinguishes the two today, so without a purpose-built fixture the suite stays green under
  either behaviour — which is exactly how this went unnoticed into a third halt.
- The not-ported v1 behaviour must be called out in `deployment_service.py`'s docstring in the
  established LOUD style, citing `cluster_manager.py:1651-1655`, so a future reader sees a decision
  rather than a discrepancy.
- Preview's optional `environment` parameter should be threaded from the API layer where a caller can
  supply it; wiring it into the router is optional in Round 9 and must not change the endpoint's
  existing request contract.

## Alternatives rejected

- **Profile `environment_type` everywhere (v1 verbatim).** Trivially consistent across both paths, and
  rejected because it preserves the mismatch described above. "v1 did it" is not sufficient when v1's
  own behaviour is the defect.
- **Rule-derived everywhere, with preview *requiring* an environment.** The most coherent option, and
  rejected as disproportionate: it is a breaking contract change to a live endpoint the SPA and
  `seedpodctl` both call, to fix an approximation that an optional parameter already resolves for any
  caller who cares.
