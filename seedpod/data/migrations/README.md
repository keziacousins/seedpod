# Migrations

Numbered SQL files applied in order, keyed on `PRAGMA user_version`. The whole system is
`seedpod/data/migrate.py` — about thirty lines. There is no `create_all()` anywhere in v2 and no
alembic (`docs/design/seam-d-foundation.md`, Decision 6).

To add one: `NNNN_short_description.sql`, ending with `PRAGMA user_version = NNNN;`. The runner
asserts the last file stamped its own number, so a forgotten `PRAGMA` fails loudly on the next
`seedpod-bootstrap migrate` rather than silently leaving the schema half-applied.

## Migrations are FORWARD-ONLY — read this before writing a destructive one

**No migration ships a down, and the runner has no reverse path.** It compares `user_version` to
each file's number and skips what is already applied; there is nothing to un-apply.

Everything so far is additive (`0002` is a single `ADD COLUMN`), so the exposure is theoretical
today. It stops being theoretical the first time a migration **drops or rewrites** a column, because
of what the database holds:

- `clusters.encrypted_kubeconfig` — the ONLY copy of every live cluster's kubeconfig. It cannot be
  re-fetched for a cluster seedpod has lost track of.
- `secrets` — every deployment secret, Fernet-encrypted.
- `deployment_audits.encrypted_resolved_manifests` — the reproducibility record.

A rebuilt database recovers **inventory and the ability to destroy**, and nothing else: reconciliation
rediscovers running infrastructure by the `seedpod-managed` tag and births an UNMANAGED row carrying
provider, slug and `resource_ids`. It does not recover kubeconfigs, secrets, deployment history, or
DNS record ids. That is measured, not assumed — `tests/runtime/test_db_loss_recovery.py`.

So, before a destructive migration:

1. **Copy the database first.** `db/*.db.bak-<reason>` is the existing convention (there are two,
   from before earlier risky rounds). It is manual and gitignored; nothing automates it.
2. **Prefer additive.** A new nullable column plus a backfill costs nothing and is reversible by
   ignoring it. Renaming or dropping is not.
3. **If you must be destructive, write the reverse SQL in a comment in the same file**, even though
   the runner cannot execute it. The next person's problem is knowing what the inverse *was*, and
   ten minutes of thinking at authoring time is worth more than a reconstruction under pressure.

Kezia, 2026-08-11: *"so long as we flagged this we can solve it when we do a destructive migration."*
This file is the flag, placed where the work happens rather than in a backlog nobody reads at that
moment.
