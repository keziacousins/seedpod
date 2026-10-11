---
title: Manifest templates — how a template uses a secret
type: guide
status: active
created: 2026-10-11
updated: 2026-10-11
---

# How a manifest template uses a secret

A manifest template is a Jinja file in a profile's `manifests_dir`. It gets the secrets of the
deployment's environment as `secrets.<name>`.

**A template cannot render the value of a secret.** `{{ secrets.database_password }}` fails the
render. This is deliberate (DR-0051): a value in a pod spec is readable by anyone who can read the
pod.

There are two forms, and no others:

| You want | Write |
|---|---|
| an env var in a pod | `valueFrom: {{ secrets.<name>.ref }}` |
| a value in the `data` of a `Secret` that the template writes | `{{ secrets.<name> \| b64encode }}` |

The examples are in `config/manifest-templates/`.

## 1. An env var

```yaml
env:
  - name: DATABASE_PASSWORD
    valueFrom: {{ secrets.database_password.ref }}
```

Do not put quotes around `{{ … }}`. `.ref` renders a mapping, not a string:

```yaml
    valueFrom: {"secretKeyRef": {"name": "exampleco-api-secrets", "key": "database_password-5b96677c"}}
```

That is all the template writes. Seedpod writes the `Secret`:

- Its name is `<service>-secrets`, where the service is the name of the template file.
- It holds only the secrets that this template asked for with `.ref`.
- It is applied in the same wave as the service.

The key has a suffix that changes when the value changes. So when you change a secret and deploy,
each workload that uses the secret restarts, and no other workload does.

## 2. `args` and `command`

Kubernetes expands `$(NAME)` in `args` and `command` from the env vars of the container. Put the
secret in an env var, and refer to it:

```yaml
env:
  - name: DATABASE_PASSWORD
    valueFrom: {{ secrets.database_password.ref }}
args:
  - "migrate"
  - "--database-password"
  - "$(DATABASE_PASSWORD)"
```

`exampleco-stack/exampleco-migrations.yaml` and `exampleco-stack/cache.yaml` do this.

## 3. A value that contains a secret

A connection string is the usual case. Kubernetes also expands `$(NAME)` in the `value` of an env
var, for a variable that is **earlier in the same list**:

```yaml
env:
  - name: DATABASE_PASSWORD
    valueFrom: {{ secrets.database_password.ref }}
  - name: DATABASE_URL
    value: "postgres://app:$(DATABASE_PASSWORD)@postgres:5432/app"
```

The pod spec holds the text `$(DATABASE_PASSWORD)`, not the password.

A profile's `environment_variables` can use the same form, if the template puts its secrets before
the profile's variables. `exampleco-stack/exampleco-api.yaml` does:

```yaml
# in the profile
environment_variables:
  services:
    exampleco-api:
      DATABASE_URL: "postgres://app:$(DATABASE_PASSWORD)@postgres:5432/app"
```

If the variable is later in the list, or does not exist, Kubernetes does not expand it. The
program then gets the text `$(DATABASE_PASSWORD)`.

## 4. A `Secret` that the template writes itself

Use this when something needs a `Secret` with a name or a type of its own: a file to mount, a TLS
certificate, a `Secret` that another chart reads.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: signing-key
type: Opaque
data:
  key.pem: "{{ secrets.signing_key | b64encode }}"
```

- Use `data`, not `stringData`. With `stringData`, kubectl reports the `Secret` as `configured` on
  each apply, and that stops the rollout restart of its whole wave.
- **Seedpod does not restart a workload when this `Secret` changes.** The key has no suffix, so the
  pod spec does not change. Restart the workload yourself, or use `.ref` if an env var is enough.
- Use `| b64encode` only in the `data` of a `Secret`. The filter gives the value as base64, and
  base64 is not protection. Seedpod does not check where you put it.

## 5. A secret that is optional

A secret that is not stored in the environment is undefined, and a reference to it fails the
render. To make a secret optional, test for it:

```yaml
{% if secrets.mail_password is defined %}
  - name: MAIL_PASSWORD
    valueFrom: {{ secrets.mail_password.ref }}
{% endif %}
```

`{% if secrets.mail_password %}` is true for a stored secret that is not empty. It fails for a
secret that is not stored.

## 6. The image pull secret

`ghcr-secret.yaml` writes `{{ secrets.ghcr_dockerconfig_json }}` with no filter. This one name is
permitted. Seedpod builds the value from the GitHub token, and it is already the base64 text that
the `data` of a `dockerconfigjson` Secret needs.

## What does not work

To a template, a secret is not a string. Each of these fails the render:

```jinja
{{ secrets.database_password }}
"postgres://app:{{ secrets.database_password }}@postgres/app"
{{ secrets.database_password ~ "-suffix" }}
{{ secrets.database_password | trim }}
{{ secrets.database_password[0:8] }}
```

The error names the template and the secret, and says what to write:

```
manifests.resolve: template 'exampleco-api.yaml' renders secrets.database_password as plain text.
For a pod env var write `valueFrom: {{ secrets.database_password.ref }}`; for the data of a Secret
write `{{ secrets.database_password | b64encode }}` (DR-0051)
```

A deployment that hits this error is rejected with `manifest resolution failed`. Nothing is applied
to the cluster.

A ConfigMap cannot hold a secret. If a config file needs one, let the program read it from an env
var, or put the secret in a file of its own and mount it (section 4).

## Converting an older template

A template from before DR-0051 renders its secrets directly. Each deployment that uses it now
fails. Change each place:

| Before | After |
|---|---|
| `value: "{{ secrets.x }}"` in `env` | `valueFrom: {{ secrets.x.ref }}` |
| `"{{ secrets.x }}"` in `args` or `command` | an env var `X` with `.ref`, and `"$(X)"` |
| `value: "scheme://user:{{ secrets.x }}@host"` | an env var `X` with `.ref` before it, and `value: "scheme://user:$(X)@host"` |
| a `Secret` with `stringData: {K: "{{ secrets.x }}"}` | `data: {K: "{{ secrets.x \| b64encode }}"}` |
| a `Secret` that only feeds a `secretKeyRef` | delete the `Secret`, and use `.ref` in the env var |
| `{{ secrets.ghcr_dockerconfig_json }}` | no change |

Then check it before you deploy:

- `seedpod-bootstrap seed-secrets <environment> --profile <profile>` lists the secrets that the
  templates name. The list must be the same as before the conversion.
- `POST /api/deployment-preview` renders the templates and applies nothing. A template that is not
  converted gives the error above.

The preview renders with a placeholder in place of each value. It shows the generated `Secret`
documents. The key suffixes in a preview are not the ones of a real deployment.

## What to expect on the cluster

- **The first deployment after a conversion restarts each converted workload**, datastores
  included. Its pod spec changed.
- **`kubectl rollout undo` does not work across a changed secret.** The old ReplicaSet refers to a
  key that is gone. Deploy the earlier version again.
- **A reader who can `get secrets`, or can `exec` into a pod, can read the values.** `.ref` keeps a
  value out of the pod spec. It does not hide it from the cluster's administrators.
- **A service that leaves a profile leaves its `Secret` and its workloads on the cluster.** Seedpod
  does not remove them (issue #24).
