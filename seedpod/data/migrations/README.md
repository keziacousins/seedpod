# Migrations

Numbered SQL files applied in order, keyed on `PRAGMA user_version`. The whole system is
`seedpod/data/migrate.py` — about thirty lines. There is no `create_all()` anywhere in v2 and no
alembic (`docs/design/seam-d-foundation.md`, Decision 6).

To add one: `NNNN_short_description.sql`, ending with `PRAGMA user_version = NNNN;`. The runner
asserts the last file stamped its own number, so a forgotten `PRAGMA` fails loudly on the next
`seedpod-bootstrap migrate` rather than silently leaving the schema half-applied.

**A script with more than one statement runs outside any transaction** unless it opens its own
(`executescript` commits first, and SQLite autocommits each statement). Wrap a multi-statement
migration in an explicit `BEGIN; ... COMMIT;`. A table rebuild must also run `PRAGMA foreign_keys = OFF;`
BEFORE the `BEGIN` (the PRAGMA does nothing inside a transaction) and `PRAGMA foreign_keys = ON;` after
the `COMMIT`. When a script fails, the runner rolls back and turns `foreign_keys` back on, because the
connection is shared. Test a rebuild against a populated database, and run `PRAGMA foreign_key_check`
afterwards: a SQL script cannot act on its result (`tests/data/test_migrate_0005.py`).

## Migrations are FORWARD-ONLY — read this before writing a destructive one

**No migration ships a down, and the runner has no reverse path.** It compares `user_version` to
each file's number and skips what is already applied; there is nothing to un-apply.

`0002` to `0004` are additive (one `ADD COLUMN` each). `0006` adds no column and rebuilds nothing (DR-0055): one new table and a swap of the instance slug index from partial to a full `UNIQUE` (an instance slug is never reused). `0005` is the first table rebuild (DR-0055).
It rewrites `workflow_runs` to relax `cluster_id` NOT NULL. It rewrites `effects_outbox` to allow the
`instance` aggregate. Its reverse SQL is in the file. The exposure is real the first time a migration **drops or rewrites** a column, because of what
the database holds:

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
