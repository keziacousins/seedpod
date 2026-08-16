---
title: DR-0036 — the ACME certresolver joins the HelmChartConfig v2 already writes, rather than porting v1's second one
type: decision
status: active
created: 2026-08-10
updated: 2026-08-10
---

# DR-0036: Let's Encrypt, configured where Traefik is already configured

**Status: ACTIVE — ratified by Kezia, 2026-08-10** (LE staging for ephemeral profiles; no issuance
gate, verified in the smoke instead). Closes backlog **#24**, found by smoke 11.

## Context

Smoke 11 served `https://<hostname>/` with `CN=TRAEFIK DEFAULT CERT`. The cause is a half-ported
feature: v2 renders the *client* half of ACME correctly and never configures the *server* half.

- `use_acme_certs = ssl_enabled and dns_enabled` is ported verbatim from v1
  (`services/manifests.py:1078` ← `manifest_resolver.py:886`) and evaluates **true** for
  `exampleco-staging-stack`, so all four Ingresses render
  `traefik.ingress.kubernetes.io/router.tls.certresolver: letsencrypt` (confirmed in smoke 11's
  decrypted deployment audit). The templates also render `spec.tls.hosts` with no `secretName`,
  which is correct for a Traefik certresolver.
- **Nothing in v2 defines a resolver named `letsencrypt`.** Traefik receives a router referencing an
  unknown resolver and falls back to its default self-signed certificate.

The profile is not at fault: it sets `ssl.enabled`, `acme_server`, `acme_email` and
`challenge_type` correctly.

### The finding that shapes the fix: v1 had TWO competing Traefik configs

Reading both v1 sources rather than the one the backlog named:

| v1 site | what it wrote | when |
|---|---|---|
| `_ssh_k3s_installer.py:548-573` (`create_traefik_hostport_config`) | `HelmChartConfig` traefik/kube-system: ports web/websecure with `hostPort`, `service.type: ClusterIP` | written to `/var/lib/rancher/k3s/server/manifests/` **before k3s starts** |
| `core/state_manager.py:1012-1090` (`_apply_traefik_config`) | `HelmChartConfig` traefik/kube-system **again**: a *simpler* ports block, **plus** `certificatesResolvers.letsencrypt.acme` | `kubectl apply` at the DEPLOYING transition, DigitalOcean only, best-effort |

Same name, same namespace, two writers — the second silently overwrote the first's richer ports
block. **v2 salvaged the first** (`providers/ssh_k3s.py:158-188`, and it is the better one: it lands
before Traefik's initial install, so there is no reconfigure-and-restart) **and lost ACME with the
second.**

So the fix is *not* to port `_apply_traefik_config`. Porting it would re-create v1's race, add a
second mechanism for one object, and require a new verb to apply a rendered manifest.

## Decisions

### 1. The ACME block is appended to the HelmChartConfig v2 already writes

`providers/ssh_k3s.py`'s traefik manifest gains an optional `certificatesResolvers.letsencrypt.acme`
section — `email`, `storage: /data/acme.json`, `caServer`, and either `httpChallenge.entryPoint: web`
or `tlsChallenge: {}` — transcribed from v1's `_apply_traefik_config`, which is the only place in v1
that ever wrote one.

One manifest, one writer, applied before Traefik's first start. **No new step verb, so DR-0022's
catalog is untouched** (still 32).

### 2. The gate is `ssl.enabled AND dns.enabled` — the same rule the templates use

`AcmeConfig.from_provider_config` (`seedpod/core/acme.py`) yields `None` unless both blocks are
present and enabled. That is exactly v1's `use_acme_certs` and exactly the condition under which the
Ingress annotation is rendered, which is what makes the two halves agree.

**The halves are pinned to agree by a test**, the same way DR-0034 decision 8 pins the FQDN: for a
shipped profile, `AcmeConfig.from_provider_config(...) is not None` ⟺ `use_acme_certs`. Without that,
a future edit could render annotations naming a resolver that no longer gets configured — which is
precisely the bug this DR closes.

v1 gated its ACME block on `dns_hostname` being present rather than on `dns.enabled`. Same intent
(don't ask a CA for a certificate for a name that does not resolve), evaluated earlier. The resolver
config itself contains no hostname — Traefik requests certificates for whatever `Host` rules its
routers carry — so nothing is lost by deciding at install time.

### 3. `ssl_config` reaches the cluster row the way `dns_config` does

`_provider_config_from` carries the profile's `ssl:` block into
`clusters.provider_config["ssl_config"]` when, and only when, it is enabled — v1 verbatim
(`orchestrator/cluster_manager.py:330-332`), and the exact precedent DR-0034 decision 3 set for
`dns_config`. `cluster.load_spec`'s Output gains `acme: AcmeConfig | None`, and `k3s.install` binds
it in the two workflows that have a k3s plane (`provision-digitalocean.yml`, `provision-tart.yml`).

`IngressConfig` (Seam C, `providers/contract.py:213`) gains `acme: AcmeConfig | None = None`. It is
an all-defaults frozen dataclass, so every existing construction — including the conformance
harness's — is unaffected.

**One deliberate divergence, following DR-0034 decision 2**: an enabled `ssl:` block that names no
`acme_email` raises rather than falling back to v1's `admin@example.com` default. A profile asking a
real CA for real certificates with no contact address is malformed, and `admin@example.com` is worse
than a loud failure. Unreachable for every shipped profile (the two `-nodns` ones have `ssl.enabled`
but `dns.enabled: false`, so the gate is false and the email is never read).

### 4. The config is written whenever Traefik needs it, not only on the hostport path

Today `ssh_k3s.py` writes the HelmChartConfig **only** when `expose_method == "hostport"`. A profile
with `loadbalancer` + ssl + dns would render certresolver annotations and get no resolver — the same
silent half-configuration this DR exists to remove, one branch over.

The writer's condition becomes "traefik enabled AND (hostport OR acme)", with the ports/service
block still gated strictly on hostport so a loadbalancer profile's service type is untouched. No
shipped profile exercises the new branch; it exists so the failure mode is unrepresentable rather
than merely absent today.

### 5. Ephemeral profiles use Let's Encrypt STAGING

`exampleco-staging-stack.yml` (`environment_type: ephemeral`) moves from the production ACME directory to
`https://acme-staging-v02.api.letsencrypt.org/directory`.

LE production limits are **50 certificates per week per registered domain** — `example.com`,
not per subdomain — and every ephemeral cluster gets a unique slug, so every smoke and every dev
cluster consumes one. Failed validations carry their own limit (5 per hostname per hour), which
debugging burns fastest. Sharing that budget between throwaway clusters and real deployments is the
wrong trade.

**Consequence, stated so it is not a surprise: no shipped profile now targets LE production**, since
`exampleco-staging-stack` is the only profile with both blocks enabled and it is ephemeral. `acme_server`
is a per-profile field, so a future non-ephemeral profile sets the production directory. Until then,
certificates on ephemeral clusters are issued by LE's staging CA and browsers will not trust them —
the machinery is identical, the issuer is not.

### 6. No issuance gate; the smoke verifies

v2 configures the resolver and moves on, as v1 did. Not because v1 did, but because **there is
nothing for a provisioning gate to wait on**: Traefik requests a certificate only once a router
referencing the resolver exists, which happens when the *deploy* applies the Ingresses — minutes
after `k3s.install`. A gate at the only place it could observe issuance (post-deploy) would block a
deployment on an external CA that can be slow or rate-limit, and would need a failure policy of its
own.

**What replaces it is a checked claim in the smoke**, not an assumption: read the served
certificate's issuer and assert it is not `CN=TRAEFIK DEFAULT CERT`. Configuring something and never
looking is exactly how #22 survived ten smoke runs; the check is the cheap half of that lesson.

### 7. Known limitation, recorded not fixed: `/data/acme.json` is not persisted

`storage: /data/acme.json` is v1's literal path, and k3s's bundled Traefik chart has
`persistence` **disabled** by default — so `/data` is an `emptyDir` and the issued certificate is
lost when the Traefik pod restarts, causing a fresh ACME request.

For 2-hour ephemeral clusters on LE staging this is close to free. It would matter for a long-lived
cluster on LE **production**, where duplicate-certificate limits (5 per identical hostname set per
week) mean a crash-looping Traefik could exhaust the budget for that name. Whoever adds the first
non-ephemeral ACME profile should enable Traefik persistence at the same time — noted here so that
is a decision rather than a surprise. Carried verbatim from v1, which had the same exposure.

## Consequences

- `seedpod/core/acme.py` — new: `AcmeConfig` + `from_provider_config`
- `seedpod/providers/contract.py` — `IngressConfig.acme`
- `seedpod/providers/ssh_k3s.py` — the resolver block, and decision 4's writer condition
- `seedpod/engine/steps/cluster.py` / `k3s.py` — `LoadSpecOutput.acme`, `k3s.install` Params
- `seedpod/app/services/deployment_service.py` — `_provider_config_from` carries `ssl_config`
- `config/workflows/provision-{digitalocean,tart}.yml` — bind `acme`
- `config/deployment-profiles/exampleco-staging-stack.yml` — staging ACME directory

**Verification.** Unit tests for the gate, the rendered block (both challenge types), and the
halves-agree equivalence. The real evidence is smoke 12: a `exampleco-staging-stack` deploy whose served
certificate is issued by Let's Encrypt's staging CA rather than Traefik's default — checked, not
assumed.
