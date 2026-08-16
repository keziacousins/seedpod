---
title: DR-0026 — deployment preview renders against redacted secrets; resolution failure is a 4xx, never a 500
type: decision
status: active
created: 2026-08-04
updated: 2026-08-04
---

# DR-0026: the manifest-resolution API edge — preview's render context, and error mapping

**Status: ACTIVE — ratified by Kezia, 2026-08-04.** Raised by Round 9's second adversarial judge on the
`resolved-config` component, alongside [DR-0025](DR-0025-hostname-resolution-ordering.md).

## Problem

DR-0025 makes manifest rendering strict: a value that cannot be resolved is absent, and rendering
raises rather than emitting a plausible-looking empty string. That is right for the deploy path, which
has a carefully-designed degradation contract — HTTP 200, `status=manifest_resolution_failed`,
deployment `rejected`, cluster still born, never a 500.

**The preview path has neither.** Two gaps fall out, and both are invisible to the test suite:

1. **`DeploymentService.deployment_preview` passes no secrets.** It mirrors `version_update`'s
   resolution half without persisting anything. Under lenient rendering that was survivable —
   `{{ secrets.DATABASE_URL }}` quietly became `""`. Under strict rendering it raises, so **every
   secret-bearing profile now previews as a failure**. `config/manifest-templates/` carries 98
   `secrets.*` references, so this is most shipped profiles.

2. **There is no `PermanentError` handler anywhere in `seedpod/api/`.** `deployment_preview` has no
   `try`/`except` wrapper (deliberately — it is the non-persisting mirror), and the router does not map
   the error, so a resolution failure surfaces as an unhandled **500**.

Neither is caught by tests. The acceptance parity gate exercises preview
(`test_deployment_preview_to_actual_deployment`) against a *fixture* profile that carries no secrets,
so the suite stays green while a real API surface — one the SPA and `seedpodctl` both use — is broken.
That "green tests, broken surface" shape is exactly what the smoke runs keep catching and what this
project's review posture exists to prevent.

A third option was live and is rejected below: passing **real decrypted secrets** into preview. It is
the most faithful rendering, and it is a permission-boundary regression — see Alternatives.

## Decision

### 1. Preview renders against REDACTED placeholders, not real secrets

`deployment_preview` builds its `secrets` mapping from `SecretRepository.list_for_environment`
**metadata** — key names only, never ciphertext, never a decrypt call — and supplies a redaction marker
as each value.

This satisfies three constraints at once:

- **Rendering succeeds.** Every referenced key is *defined*, so `StrictUndefined` does not fire for a
  secret that genuinely exists in the environment. A key referenced by a template but **absent** from
  the environment still raises — which is correct, and is a genuinely useful thing for a preview to
  tell you.
- **No plaintext leaves the process.** Preview returns `rendered_manifests` to any caller holding
  `deployments:read`. Decrypting into that response would hand full secret material to a permission
  level whose whole point is that it is not `secrets:read`. The metadata call is the same one the
  secrets API already uses to list without revealing.
- **Preview stays honest about its own limits.** A redaction marker in the output is visibly not the
  deployed value. An empty string is not.

**DR-0008 compliance:** this costs preview a short **read-only** transaction. That is permitted and is
not in tension with `deployment_preview`'s "no uow at all" docstring, which is about **not
persisting** — DR-0008's binding law is that *a transaction encloses only database statements*, and a
metadata read is exactly that. No provider IO, no decryption reaching outside the DB, nothing else in
the block. Update the docstring to say what it now does.

### 2. Manifest-resolution failure at the API edge is a 4xx, never a 500

The deployments router maps `PermanentError(ErrorCode.INVALID_INPUT)` arising from manifest resolution
to a **4xx** carrying the resolution error in the body.

A profile whose templates reference an undefined variable, or whose hostname strategy cannot be
satisfied, is **bad configuration** — a client-visible, actionable condition, not a server fault. This
is needed regardless of decision 1: a `provider_host` profile will still legitimately fail to resolve
until Round 10 (DR-0025), and that must read as "your profile cannot be resolved yet", not as a crash.

This does **not** change the deploy path. `_deploy`'s existing degrade-to-recorded-rejection contract
is unchanged and still returns 200 — it is pinned by the acceptance parity gate and must stay pinned.
This decision governs only the paths that have no such wrapper.

## Consequences

- `DeploymentService` gains a metadata-read dependency for preview. It already takes
  `SecretRepository` directly as of Round 9 — the established idiom it uses for
  `DeploymentAuditRepository` — so this is a method change, not new wiring.
- The redaction marker is a visible contract. Pick one obvious sentinel, use it consistently, and test
  that it appears rather than a real value. A preview response must never be mistakable for the real
  rendered manifest.
- **A test must pin the permission boundary**, not just the happy path: preview of a secret-bearing
  profile returns the marker and **no plaintext secret value**. That is the assertion that stops a
  future "make preview more useful" change from quietly becoming a leak.
- The acceptance parity gate's preview assertion (`preview_data["status"] == "success"`) must stay
  green. It uses a fixture profile with no secrets, so it does not exercise this path — which is
  precisely why the new tests above are required rather than optional.

## Alternatives rejected

- **Real decrypted secrets in preview.** The most faithful rendering, and rejected on the permission
  boundary: it returns full plaintext secret material in `rendered_manifests` to a caller holding only
  `deployments:read`, collapsing the distinction that `secrets:read` exists to draw. "v1 may have done
  it" is not sufficient — v1's own `CLAUDE.md` flags `deployment_audits` plaintext as restricted-access
  precisely because this boundary is easy to erode.
- **Leave preview broken and document it.** What the Round 9 build chose unilaterally, in a docstring.
  Rejected: it is a real regression on a live API surface, introduced by this round, and it is
  invisible to the suite. A docstring is not a decision record, and "documented" is not "ratified".
- **Make rendering lenient again for preview only.** Rejected: two rendering modes means preview stops
  predicting what deployment will do, which is preview's entire purpose. It would also reintroduce the
  DR-0025 empty-string hole through a side door.
