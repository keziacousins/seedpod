---
title: Operations runbook — running seedpod v2, and what to do when something is lost
type: guide
status: active
created: 2026-08-11
updated: 2026-08-11
---

# Operations runbook

v1 is decommissioned. **v2 is the only system that knows your clusters exist**, which changes what
"operations" means here: the risks are no longer about cutover, they are about custody. This file is
the operator-facing counterpart to the parity backlog (not published)'s "Operational readiness" section.

## 1. Key custody — the highest-consequence thing in this document

Two Fernet keys live in `.env`:

| variable | encrypts | used for environments |
|---|---|---|
| `SEEDPOD_SECRET_KEY_DEV` | kubeconfigs, secrets, resolved manifests | `local`, `ephemeral`, `staging` |
| `SEEDPOD_SECRET_KEY_PROD` | the same, for production | `production` |

Every stored kubeconfig (`clusters.encrypted_kubeconfig`), every deployment secret, and every audited
manifest is encrypted with one of them, and the ciphertext records which (`kubeconfig_key_class`), so
a row written under DEV still decrypts under DEV after an environment is remapped.

**If a key is lost, everything it encrypted is unrecoverable.** Not degraded — gone. Live clusters
stay running and stay destroyable (see §3), but their kubeconfigs cannot be decrypted, so they can
never be deployed to again. The practical recovery is: destroy and reprovision everything.

This is not a technical gap to be closed in code — the encryption is doing exactly what it should.
It is an **operator policy** question, and it has one requirement:

> **There must be a second copy of both keys, somewhere that does not share a failure mode with this
> laptop.** A password manager entry, a sealed envelope, another machine — the mechanism matters far
> less than that it exists and that someone other than the author knows where it is.

<!-- OPERATOR DECISION — fill this in. Everything above is fact; this line is policy, and the
     runbook is incomplete without it. -->
**Where the second copy lives:** _to be recorded by Kezia._

Two related facts worth knowing:

- `.env` is gitignored and holds these keys plus `DIGITALOCEAN_TOKEN`, `GITHUB_TOKEN` and
  `CLOUDFLARE_API_TOKEN`. A backup of `.env` is a backup of everything.
- v1's DEV key is a separate artifact, needed only to decrypt v1's old secrets and snapshots. It
  exists solely inside a single backup archive held offline. If v1 data is ever wanted
  again, that tarball is the whole story.

## 2. Cold start

```bash
seedpod-bootstrap generate-keys          # prints the two SEEDPOD_SECRET_KEY_* lines
#   ... paste them into .env, along with DIGITALOCEAN_TOKEN / GITHUB_TOKEN / CLOUDFLARE_API_TOKEN
seedpod-bootstrap migrate                # applies v2's schema to a cold DB (idempotent)
seedpod-bootstrap create-admin <user>    # prints the API key ONCE -> admin-api-key.txt
.venv/bin/python start.py                # the server; loads .env itself
```

`seedpod-bootstrap` reads `.env` from the working directory (walking up), so none of this needs
`set -a; . ./.env` any more. It is never exposed over HTTP; local filesystem access is its trust
boundary (DR-0021).

**`python -m seedpod` does NOT load `.env`** — only `start.py` does. Use `start.py` unless the
environment is already exported.

## 3. If the database is lost

**What you get back, measured rather than assumed** (`tests/runtime/test_db_loss_recovery.py`):

- **Inventory.** Reconciliation discovers running infrastructure by the `seedpod-managed` tag and
  births an `UNMANAGED` row per cluster, carrying provider, slug and `resource_ids`.
- **The ability to destroy.** `cluster.load_infra` gets everything `infra.destroy_instance` needs
  from that row, so a destroy issued after a rebuild reaches the real droplet.
- **Nothing else.** Kubeconfigs, secrets, deployment history and DNS record ids were all *in* the
  database. An adopted cluster can be inventoried and destroyed; it cannot be deployed to.

**It will not destroy anything on its own.** A rediscovered cluster is `UNMANAGED`; the zombie sweep
only ever touches rows the database believes are `DESTROYED`, and a rebuilt database believes
nothing. This is pinned by a test because the cost of being wrong is destroyed production infra.

**Two consequences to plan around:** a DNS record created before the loss is *not* recovered, so
destroying an adopted cluster leaves the record behind — delete it by hand in Cloudflare. And every
adopted cluster is labelled `environment="production"` (DR-0013), including local ones, so check
before acting on that field.

**There is no automatic database backup.** The convention is a manual copy, `db/seedpod.db.bak-<reason>`,
before anything risky. Two exist from earlier rounds. If regular backups are wanted, that is an
unclaimed decision.

## 4. Migrations are forward-only

See `seedpod/data/migrations/README.md`, which is deliberately next to the files it governs. Short
version: the runner is `PRAGMA user_version`-keyed with no reverse path, everything so far is
additive, and the first destructive migration needs a database copy taken first.

## 5. Everyday operations

```bash
# preview a deployment without provisioning anything -- free, runs the whole
# resolution path including real GHCR lookups. Use it before spending a droplet.
curl -X POST localhost:8000/api/deployment-preview -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"deployment_profile_name":"...","triggering_repo":"...","triggering_branch":"...","triggering_image":"..."}'

seedpodctl deploy --repo <r> --branch <b> --image <img> --commit <sha>
curl -X DELETE localhost:8000/api/clusters/<id> -H "Authorization: Bearer $KEY"
```

**Always check the provider for leftovers afterwards** rather than trusting a green destroy:
`GET https://api.digitalocean.com/v2/droplets`. The four `seedpod-*`/`exampleco-*-ams3` firewalls are
stable and reused, so seeing them with zero attached droplets is correct, not a leak.

For a DNS-enabled profile, also check the Cloudflare zone is clean:
`GET https://api.cloudflare.com/client/v4/zones/<zone_id>/dns_records?type=A`.

## 6. Traps that have each cost real time

- **Restart the server after any code change.** `start.py` does not run uvicorn reload-mode, so a
  running seedpod serves the code it booted with. A stale process makes a working fix look broken —
  this cost a droplet during smoke 12. Prove it is live before spending anything:
  `python -c "import inspect; from seedpod.<mod> import <fn>; print(inspect.getsource(<fn>))"`.
- **Stop the server before `pytest`.** A running seedpod holds `seedpod.pid`, which fails
  `test_importing_start_module_has_zero_side_effects`. Environmental, not a regression.
- **A cluster cannot be destroyed while `provisioning`.** `DELETE` is rejected by the state machine;
  a run started by mistake must be allowed to reach ACTIVE first (~4 minutes).
- **`CLOUDFLARE_API_TOKEN` must have DNS *edit* scope**, not just read. Since DR-0034 a DNS failure
  fails the provision and compensates, so a blank or read-only token burns a droplet. Verify with a
  throwaway record before a DNS smoke.
- **Check the provider's status page before diagnosing a provisioning failure.**
  `curl -s https://status.digitalocean.com/api/v2/status.json` settles in one call what can look
  exactly like a v2 regression.
- **A cold database needs a `tailscale_auth_key` secret** or manifest resolution raises for every
  secret-bearing profile. The raise is designed; a placeholder is fine.
- **Ephemeral clusters use Let's Encrypt STAGING** (DR-0036), so browsers will not trust their
  certificates. That is correct, not a misconfiguration.
