---
title: DR-0051 — secrets reach a pod by reference, through a Secret that seedpod writes for each service
type: decision
status: proposal
created: 2026-10-10
updated: 2026-10-11
---

# DR-0051: secrets reach a pod by reference

**Status: PROPOSAL — the design and decision 3 (the key suffix) were agreed with Kezia on
2026-10-09, the build on 2026-10-10, and one Secret for each service (decision 2) and the hard cut
(decision 4) on 2026-10-11. Not yet ratified.** Issue #22.

## Context

The manifest templates rendered each secret into the pod spec:

```yaml
- name: DATABASE_PASSWORD
  value: "{{ secrets.database_password }}"
```

Kubernetes then shows the value in plain text to anyone who can read a pod: `kubectl describe pod`,
`kubectl get pod -o yaml`, the Deployment, each old ReplicaSet, and the
`last-applied-configuration` annotation. One template also passed a password in `args`.

The usual repair is a `Secret` and a `secretKeyRef`. `exampleco-misc/tailscale.yaml` already does
this by hand. Two things made it more than a template edit:

- **Each template would carry its own `Secret` document.** That is the same YAML many times, and a
  template author must get the base64 and the `data`/`stringData` rule right each time (backlog
  #14).
- **A `secretKeyRef` stops a rotated secret from rolling its workloads.** With an inline value, a
  new value changes the pod template and the Deployment rolls. With a reference to a fixed key, the
  `Secret` reports `configured` and the Deployment reports `unchanged`. `all_unchanged` is then
  false, so `deploy.ensure_rollouts` forces no restart, and kubectl starts none. The pods keep the
  old value.

v1 rendered inline values too. This is not a v1 behaviour to keep.

## Decision

**1. `secrets.<name>` gains `.ref`.** In a template:

```yaml
- name: DATABASE_PASSWORD
  valueFrom: {{ secrets.database_password.ref }}
```

`.ref` renders a `secretKeyRef` as a one-line flow mapping, so the template has no indentation to
manage. The value is not rendered. The text `secrets.<name>` stays in the template, so
`required_secrets` finds the same names as before.

**2. The resolver writes one `Secret` for each service.** `ManifestResolver._render_templates`
records each key that a template asked for through `.ref`. For a template that asked for a key, it
writes a `Secret` named `<service>-secrets`, for example `exampleco-api-secrets`. That `Secret`
holds the keys that this template asked for, and no others.

- It uses `data` with base64 values, never `stringData` (backlog #14).
- The keys are sorted, so the same secrets always give the same text.
- It carries `app: <service>` and `app.kubernetes.io/managed-by: seedpod`, and the `environment` and
  `cluster-id` labels when the resolved config has them.
- Its name starts with the service name, so `deploy.plan_waves` puts it in the wave of that service.
  In the rendered text it is in front of the documents of its template.
- It is not in `template_files`. No template file made it.

A template that never uses `.ref` gets no generated `Secret`.

**Why not one `Secret` for the cluster.** That was the first design. A `Secret` is the only document
the resolver writes whose apply removes something: a key that the render does not ask for is deleted.
Each other document is additive, because the deploy path prunes nothing (issue #24). So a workload
that the current render does not mention keeps running: its service left the profile, or a different
profile was deployed to the same cluster. A cluster is found by repository, branch and environment,
and not by profile, so the second case is possible. With one shared `Secret`, such a workload loses
its keys and fails at its next pod restart, which can be days later. With a `Secret` of its own, the
workload is not touched, as its Deployment is not.

**3. Each key has a suffix that comes from its value.** The key is
`<name>-<8 hex characters>`, for example `database_password-3fa9c1d2`. A new value gives a new key,
so the reference changes, the pod template changes, and the Deployment rolls. Only the workloads
that refer to that secret roll. This is the behaviour that inline values had.

The suffix is an HMAC-SHA256 of the name and the value. It is not a plain hash, because a reader of
pod specs could test a guessed password against a plain hash. The name is in the message so that two
secrets with one value do not show the same suffix.

The HMAC key is not a new setting. The composition root derives it from `SEEDPOD_SECRET_KEY_DEV`
with HKDF (`services/crypto.py`, `derive_key`) and passes it to the resolver as `secret_ref_key`. It
is the same bytes on each start. A template that uses `.ref` in a resolver that has no key raises.

**4. A bare `{{ secrets.x }}` is an error.** A template cannot render the value of a secret. The
render fails with `PermanentError(INVALID_INPUT)`. The message names the template and the secret,
and says what to write. It does not contain the value.

A secret reaches a cluster in two ways only:

- `{{ secrets.x.ref }}`, for the env of a pod.
- `{{ secrets.x | b64encode }}`, for the `data` of a `Secret` that a template writes itself.

To a template, a secret is not a string. So no string operation gives the value back: `~`, `+`, a
slice, and each string filter fail too. `{% if secrets.x %}` still works.

One name is exempt: `ghcr_dockerconfig_json`. Seedpod builds it, and it is already the base64 text
of a `Secret`'s `data` when it reaches `ghcr-secret.yaml`.

This is a hard cut, by decision. A template that is not converted stops its deployment at render
time, before anything is applied. That is the purpose: it forces the change.

**5. Each shipped template uses `.ref`.** `exampleco-api`, `postgres`, `cache` and `minio` change
one line for each secret. `exampleco-migrations` puts the password in an env var with `.ref` and
passes `$(DATABASE_PASSWORD)` in `args`; Kubernetes expands it.

`exampleco-api` and `minio` put their secrets before the variables of the profile. Kubernetes
expands `$(NAME)` in an env value only for a variable that is earlier in the list, so a profile
variable can then hold a connection string that refers to a secret.

`tailscale.yaml` no longer writes a `tailscale-auth` Secret. It had the fault that the Context
describes: a fixed key, so a new auth key did not restart the DaemonSet. It now uses `.ref`.

`ghcr-secret.yaml` does not change. A `dockerconfigjson` Secret must be its own object.

`docs/guides/manifest-templates.md` is the guide for a template author, and for the conversion of
an older template.

## Consequences

- No template renders a secret value as plain text. A shipped template cannot, and a template
  outside this repository fails if it tries.
- **Each set of templates outside this repository must be converted before it is used with this
  release.** Until then, each deployment that uses it is rejected with
  `manifest resolution failed`, and names the first template and secret to change. A deployment
  whose rendering is deferred to deploy time fails there, in `deploy.load_audit`.
- A secret in a place that is not an env var goes through one. Put it in an env var with `.ref`,
  and write `$(NAME)` in `args`, in `command`, or in the value of a later env var (a connection
  string). A ConfigMap cannot hold a secret.
- A deployment preview shows the generated `Secret` documents, with the placeholder of DR-0026 as
  each value. The key suffixes in a preview are not those of a real deployment.
- A cluster that has the old `tailscale-auth` Secret keeps it. Nothing refers to it (issue #24).
- The first deployment after this change rolls each converted workload once, datastores included.
  Their pod templates change from `value` to `valueFrom`.
- A rotated secret rolls the workloads that use it, and no others.
- A pod from an old ReplicaSet cannot start after a rotation, because its key is gone from its
  service's `Secret`. This applies to an old pod that restarts during the rollout, and to
  `kubectl rollout undo`. With inline values both worked. To go back, deploy again.
- The rendered output is no longer the same bytes as v1's for a template that uses `.ref`. This is
  deliberate.
- The rendered manifest still holds each value, now as base64 in the `Secret` document. A stored or
  displayed manifest is as sensitive as it was.
- A new `SEEDPOD_SECRET_KEY_DEV` changes each suffix. The next deployment to each cluster then rolls
  each workload that uses a secret.
- The suffix key comes from the DEV key for each environment. A person who has the DEV key and can
  read production pod specs can test a guess against a production suffix. `SEEDPOD_SECRET_KEY_PROD`
  is optional, so it cannot be the source.
- A secret that two services use is in two `Secret` objects, under the same key name.
- A service that leaves a profile leaves its `Secret` on the cluster, with its values, until the
  cluster is destroyed. Its Deployment stays too. Issue #24 is about both.
- Two profiles that deploy to one cluster affect each other only where they declare a service of the
  same name. They already did, in that service's Deployment.
- A `Secret` is in the same wave as its workload, not in an earlier one. If the workload is created
  first, its pods wait for the `Secret` and then start.
- Only a string secret has `.ref`. On any other type the render fails.
- `| b64encode` writes the value wherever the template puts it. The resolver does not check that
  the place is the `data` of a `Secret`.
- A template that reads `secrets.x._value` gets the value. The cut stops an accident and a habit,
  not an intent: a template is trusted configuration.
- A reader who can `get secrets`, or can `exec` into a pod, still reads the values. This change
  does not address that.

## Not decided here

- **Encryption at rest.** K3s `--secrets-encryption` is a separate change to `k3s.install`.
- **A verb that sends rotated secrets to a cluster without a deployment.** The generated `Secret`
  documents make it possible: it would apply only those.
- **Removing what a deployment no longer declares.** Issue #24.

## What pins it

- `tests/services/test_manifests.py` —
  `test_secret_ref_renders_a_secret_key_ref_and_the_generated_secret_holds_the_value`,
  `test_template_without_a_ref_gets_no_generated_secret`,
  `test_bare_secret_raises_naming_the_template_and_the_secret_and_not_the_value`,
  `test_no_template_expression_renders_a_secret_value`,
  `test_secret_truthiness_still_works_for_an_optional_secret`,
  `test_secret_ref_on_a_value_that_is_not_a_string_raises`,
  `test_changed_secret_changes_only_the_workloads_that_refer_to_it`,
  `test_each_service_gets_a_secret_of_its_own_with_only_its_own_keys`,
  `test_a_service_secret_does_not_depend_on_the_other_services_in_the_render`,
  `test_secret_ref_keys_are_stable_across_renders_and_between_resolve_and_render_only`,
  `test_secret_ref_suffix_does_not_reveal_the_value`, `test_secret_ref_without_a_key_raises`,
  `test_secret_ref_to_a_missing_secret_raises_naming_the_template` and
  `test_secret_ref_with_a_name_kubernetes_cannot_use_as_a_key_raises`.
- `tests/app/test_manifest_resolution_end_to_end.py`, on the shipped stack —
  `test_shipped_stack_renders_no_secret_value_and_each_secret_is_in_its_services_wave`,
  `test_shipped_stack_service_secret_is_the_same_when_the_service_is_rendered_alone`,
  `test_shipped_stack_rotated_secret_changes_exactly_the_workloads_that_use_it`,
  `test_shipped_stack_secrets_come_before_the_profiles_variables` and
  `test_shipped_templates_never_render_a_secret_as_plain_text`.
- `tests/cli/test_bootstrap.py::test_required_secrets_derives_the_real_exampleco_dev_stack_list` did
  not change: the same names are required.

Not tested on a live cluster.
