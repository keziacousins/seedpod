---
title: DR-0052 — k3s.install pins the k3s version, so a provision does not depend on update.k3s.io
type: decision
status: active
created: 2026-10-11
updated: 2026-10-11
---

# DR-0052: `k3s.install` pins the k3s version

**Status: ACTIVE — ratified by Kezia, 2026-10-11.**

## Context

`k3s.install` ran `curl -sfL https://get.k3s.io | sudo INSTALL_K3S_EXEC='…' sh -` with no version.
The installer then asks a second host, `update.k3s.io`, which release the `stable` channel names.
That lookup is the only use of `update.k3s.io`. The binary comes from a GitHub release.

`update.k3s.io` went down on 2026-10-10. It first served a self-signed Traefik certificate. On
2026-10-11 it refused connections on ports 443 and 80. `get.k3s.io` and GitHub were not affected.
The upstream report is [k3s-io/k3s#14783](https://github.com/k3s-io/k3s/issues/14783). Users
reported certificate errors from the same host on 2026-07-01, 2026-07-12 and 2026-09-10
([#14315](https://github.com/k3s-io/k3s/issues/14315),
[#14395](https://github.com/k3s-io/k3s/issues/14395)).

While that host is down, no SSH-plane provision can succeed. The installer cannot resolve the
channel, so it tries to download a release named `stable`, and that download fails.

Two more facts:

- With `INSTALL_K3S_VERSION` set, the installer skips the channel lookup.
- `config/providers/tart.yml` and `config/providers/digitalocean.yml` each had a
  `k3s_config.version` key. No code read it. The DigitalOcean value was `v1.28.5+k3s1`, and v2
  never installed that release.
- v1 ran the same command, also with no version (`_ssh_k3s_installer.py`). This is not a v2
  regression.

An unpinned install also means two clusters built a week apart can run different k3s releases.

## Decision

**1. `k3s.install` sends `INSTALL_K3S_VERSION`.** Seam C's `InstallK3s` gains
`k3s_version: str | None = None`. The `ssh-k3s` provider puts the value before `INSTALL_K3S_EXEC`
in the same `sudo` command, and quotes it. `None` leaves the command as it was.

**2. The version is provider configuration, and it takes the DR-0023 route.** It is read from
`k3s_config.version` in the provider's own `config/providers/<provider>.yml`. The composition root
builds a `provider → version` mapping (`_k3s_versions()`). `cluster.load_spec` yields
`k3s_version`. The two provision workflows bind it into `k3s.install`. This is DR-0022's P8, and it
is how `ssh_user` already reaches the same verb. `ssh-k3s` is one provider for DigitalOcean and
tart, so the value cannot live on `SshK3sConfig` and still come from two files.

**3. The binding has no default.** `InstallK3sParams.k3s_version` is `str | None` with no default.
`None` is a legal value, but a workflow must bind it. A workflow file that does not is rejected by
validator rule V2, and the step fails when the engine builds its params. A default of `None` would
install an unpinned k3s and say nothing.

**4. A bad version fails at boot.** `_k3s_versions()` accepts a k3s release tag
(`v1.36.5+k3s1`, `v1.37.0-rc1+k3s1`) and raises `InvalidK3sVersion` for all other values. `stable`
and `latest` are channel names, not versions, and are rejected. The other outcome is a failed
download after the VM exists.

Both shipped provider files now pin `v1.36.5+k3s1`. That was the `stable` release on 2026-10-11.

## Consequences

- A provision uses `get.k3s.io` and GitHub. It does not use `update.k3s.io`.
- All clusters from one provider run the same k3s release until the file changes.
- A k3s upgrade for new clusters is now a one-line config change. Nobody gets a new release by
  accident, and nobody gets one automatically.
- `version: null` is still legal. It restores the old behaviour and the dependency.
- **A config directory outside this repository must change in two places.** A deployment that sets
  `SEEDPOD_CONFIG_DIR` has its own copy of `config/`. It needs `k3s_config.version` in each
  provider file, and the `k3s_version` binding in `provision-tart.yml` and
  `provision-digitalocean.yml`. Without the version, the install is unpinned. Without the binding,
  the `k3s` step fails, and the workflow destroys the instance it made.
- `cluster.load_spec`'s Output gains a field. This is the same kind of extension as `dns_intent`
  (DR-0034) and `acme` (DR-0036).

## Not decided here

- **The install script still comes from `get.k3s.io` at each provision.** That host is a separate
  dependency. A copy of the script in this repository would remove it.
- **Workflow validation does not run at boot.** `validate_workflow` runs in the tests against the
  shipped files. A workflow file in an outside config directory is checked only when a step runs.
  This is older than this decision (`app/factory.py` records it), and decision 3 is weaker because
  of it.
- **Existing clusters.** This decision is about installs. It does not upgrade a running cluster.
- **`k3s_config.channel` and `k3s_config.install_url`** are still not read.
