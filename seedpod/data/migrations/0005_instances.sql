-- 0005_instances.sql — DR-0055 decisions 1 and 4.
--
-- A bare VM is an `Instance`, its own aggregate (DR-0055). This migration does the three schema
-- changes that decision needs, and nothing else:
--
--   1. `instances` — the whole table, complete now, so no later phase rebuilds it.
--   2. `workflow_runs` — rebuilt, so a run can belong to a cluster OR an instance.
--   3. `effects_outbox` — rebuilt, so its `aggregate_type` CHECK admits 'instance' (the
--      Dispatcher writes one outbox row per effect, under the aggregate it transitions).
--
-- DESTRUCTIVE (a table rebuild), so READ migrations/README.md first. The reverse SQL is at the
-- bottom of this file. Back up the database file before running it on real data
-- (`db/*.db.bak-<reason>`).
--
-- WHY A REBUILD. SQLite cannot relax a NOT NULL, or add a CHECK, with ALTER TABLE.
-- `workflow_runs.cluster_id` is NOT NULL, and an instance run has no cluster.
--
-- WHY `foreign_keys` GOES OFF FIRST. `workflow_steps.run_id` references `workflow_runs(id)`.
-- With foreign keys on, DROP TABLE on the old `workflow_runs` fails (or does an implicit
-- DELETE that violates the reference). The PRAGMA is a no-op inside a transaction, so it must run
-- BEFORE the explicit BEGIN. The runner (seedpod/data/migrate.py) restores it if this script
-- fails part-way. The tests in tests/data/test_migrate_0005.py run `PRAGMA foreign_key_check`
-- afterwards, because a SQL script cannot act on its result.
--
-- Conventions follow `clusters` (0001): TEXT ISO-8601 UTC timestamps written by the injected
-- clock, JSON as TEXT, `version` for the compare-and-swap Persist. Unlike `clusters`, `status`
-- has a CHECK (DR-0055 decision 1 fixes the state set; the pure transition table stays the
-- authority for which moves are legal).

PRAGMA foreign_keys = OFF;
BEGIN;

------------------------------------------------------------------
-- instances
------------------------------------------------------------------
CREATE TABLE instances (
    id               TEXT PRIMARY KEY,               -- uuid4; routes accept id-or-slug
    slug             TEXT NOT NULL,                  -- tart finds a VM by `spi-{slug}`
    provider         TEXT NOT NULL,
    environment      TEXT NOT NULL,                  -- DR-0054 scope
    template         TEXT NOT NULL,                  -- config/instance-templates/*.yml name
    size             TEXT NOT NULL,                  -- a size from that template's list
    status           TEXT NOT NULL CHECK (status IN
                       ('new','provisioning','active','failed','destroying','destroyed',
                        'destroy-failed','zombie')),
    version          INTEGER NOT NULL DEFAULT 0,
    -- The owner, DR-0055 decision 9. A key is revoked by `is_active = 0` (ApiKeyRepository.revoke),
    -- and no code deletes an api_keys row. So a revoked key keeps its row and this reference stays
    -- valid. NO ACTION (the default) is deliberate: CASCADE would delete the row of an instance
    -- that may still hold a VM, and SET NULL would lose who owns it.
    owner_key_id     INTEGER NOT NULL REFERENCES api_keys(id),
    resource_ids     TEXT NOT NULL DEFAULT '{}',     -- JSON; provider resource ids, fed by InfraAllocated
    address          TEXT,                           -- where SSH reaches the VM; NULL until known
    known_hosts      TEXT,                           -- host key captured before any boot data (decision 8)
    lease_seconds    INTEGER NOT NULL,               -- the length of one lease (decision 7)
    expires_at       TEXT NOT NULL,                  -- the `expiry` timer's fire time; there is no instance without one
    hard_expires_at  TEXT NOT NULL,                  -- fixed at creation, never moves
    -- Recorded at admission from the template's size list, so limits (decision 6) count them.
    -- No DEFAULT: a missing value must fail, not count as free.
    hourly_price     REAL NOT NULL,
    vcpu             INTEGER NOT NULL,
    memory_mb        INTEGER NOT NULL,
    failure_reason   TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    destroyed_at     TEXT
);
-- Unique among instances that are not destroyed (decision 3). The slug is reusable after destroy.
CREATE UNIQUE INDEX ux_instances_slug_live ON instances(slug) WHERE status != 'destroyed';
CREATE INDEX ix_instances_status ON instances(status);
CREATE INDEX ix_instances_owner  ON instances(owner_key_id, status);   -- per-key limit counts (decision 6)

------------------------------------------------------------------
-- workflow_runs — rebuilt. Every column, CHECK and row is kept, except:
--   cluster_id   NOT NULL -> nullable
--   instance_id  new, nullable, references instances(id)
--   a CHECK that exactly one of the two is set
-- Column order is not significant: every reader selects by name.
------------------------------------------------------------------
CREATE TABLE workflow_runs_new (
    id                TEXT PRIMARY KEY,                 -- uuid4
    workflow          TEXT NOT NULL,                    -- CONCRETE definition name (Conflict 13)
    workflow_version  INTEGER NOT NULL,                 -- pins the YAML version at admission
    cluster_id        TEXT REFERENCES clusters(id),
    instance_id       TEXT REFERENCES instances(id),
    deployment_id     TEXT REFERENCES deployments(id),
    dedupe_key        TEXT UNIQUE,                      -- RunWorkflow effect_id (exactly-once admission)
    args              TEXT NOT NULL DEFAULT '{}',       -- JSON; secret:true inputs Fernet-encrypted
    status            TEXT NOT NULL CHECK (status IN
                        ('pending','running','blocked','compensating',
                         'succeeded','failed','cancelled')),
    cancel_requested  INTEGER NOT NULL DEFAULT 0,
    failed_step       TEXT,                             -- step_path
    error             TEXT,                             -- JSON {kind:'transient'|'permanent'|'unreachable', step, message}
    undo_incomplete   TEXT,                             -- JSON [step_path]; non-empty == v1 "failed dirty"
    initiated_by      TEXT,
    created_at        TEXT NOT NULL,
    started_at        TEXT,
    finished_at       TEXT,
    CHECK ((cluster_id IS NULL) != (instance_id IS NULL))   -- exactly one aggregate
);

INSERT INTO workflow_runs_new
    (id, workflow, workflow_version, cluster_id, instance_id, deployment_id, dedupe_key, args,
     status, cancel_requested, failed_step, error, undo_incomplete, initiated_by,
     created_at, started_at, finished_at)
SELECT
     id, workflow, workflow_version, cluster_id, NULL,        deployment_id, dedupe_key, args,
     status, cancel_requested, failed_step, error, undo_incomplete, initiated_by,
     created_at, started_at, finished_at
FROM workflow_runs;

DROP TABLE workflow_runs;   -- also drops ux_wr_one_active and ix_wr_cluster
ALTER TABLE workflow_runs_new RENAME TO workflow_runs;

-- One live run per aggregate (H14). Each index is partial on its own column. The statuses are the
-- ones ACTIVE_RUN_STATUSES holds (data/repositories.py).
CREATE UNIQUE INDEX ux_wr_one_active ON workflow_runs (cluster_id)
    WHERE cluster_id IS NOT NULL AND status IN ('pending','running','blocked','compensating');
CREATE UNIQUE INDEX ux_wr_one_active_instance ON workflow_runs (instance_id)
    WHERE instance_id IS NOT NULL AND status IN ('pending','running','blocked','compensating');
CREATE INDEX ix_wr_cluster ON workflow_runs (cluster_id, created_at DESC);

------------------------------------------------------------------
-- effects_outbox — rebuilt. Only the aggregate_type CHECK changes: it gains 'instance'.
-- Every other column, default, CHECK and the UNIQUE on effect_id are as in 0001.
-- `seq` is AUTOINCREMENT and must never be reused (it is the drain order and the replay
-- key), so the rebuild keeps both the row values and the high-water mark:
--   * rows are copied WITH their seq;
--   * SQLite keeps the high-water mark in `sqlite_sequence`, one row per AUTOINCREMENT table.
--     Copying rows with explicit seq values makes the new table's entry equal to MAX(seq) of the
--     copy. That is LOWER than the old entry when the highest-seq row was deleted (prune_done
--     does that), so the old entry is carried over explicitly below. DROP TABLE removes the old
--     entry, and RENAME moves the new table's entry to the new name.
------------------------------------------------------------------
CREATE TABLE effects_outbox_new (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    effect_id      TEXT    NOT NULL UNIQUE,      -- "{aggregate_type}/{aggregate_id}@{to_version}#{ordinal}"
                                                 -- engine-origin rows: "run/{run_id}@{step_path}#{n}"
    aggregate_type TEXT    NOT NULL CHECK (aggregate_type IN ('cluster','deployment','run','instance')),
    aggregate_id   TEXT    NOT NULL,
    to_version     INTEGER NOT NULL,             -- 0 for engine-origin rows
    ordinal        INTEGER NOT NULL,
    kind           TEXT    NOT NULL CHECK (kind IN ('persist','schedule_timer','cancel_timer',
                                                    'run_workflow','cancel_workflow','cascade','notify')),
    payload        TEXT    NOT NULL,             -- canonical JSON from core/codec.encode()
    lane           TEXT    NOT NULL CHECK (lane IN ('tx','drain')),
    status         TEXT    NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','done','dead')),
    attempts       INTEGER NOT NULL DEFAULT 0,
    available_at   TEXT    NOT NULL,
    created_at     TEXT    NOT NULL,
    done_at        TEXT,
    last_error     TEXT
);

INSERT INTO effects_outbox_new
    (seq, effect_id, aggregate_type, aggregate_id, to_version, ordinal, kind, payload, lane,
     status, attempts, available_at, created_at, done_at, last_error)
SELECT
     seq, effect_id, aggregate_type, aggregate_id, to_version, ordinal, kind, payload, lane,
     status, attempts, available_at, created_at, done_at, last_error
FROM effects_outbox;

-- Carry the old high-water mark over. An empty copy has no entry yet: create it. A copy with
-- rows has one (= MAX(seq)): raise it if the old mark was higher.
INSERT INTO sqlite_sequence (name, seq)
    SELECT 'effects_outbox_new', seq FROM sqlite_sequence
    WHERE name = 'effects_outbox'
      AND NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'effects_outbox_new');
UPDATE sqlite_sequence
    SET seq = (SELECT seq FROM sqlite_sequence WHERE name = 'effects_outbox')
    WHERE name = 'effects_outbox_new'
      AND seq < (SELECT seq FROM sqlite_sequence WHERE name = 'effects_outbox');

DROP TABLE effects_outbox;   -- also drops idx_outbox_drain and idx_outbox_aggregate
ALTER TABLE effects_outbox_new RENAME TO effects_outbox;

CREATE INDEX idx_outbox_drain     ON effects_outbox (status, available_at, seq);
CREATE INDEX idx_outbox_aggregate ON effects_outbox (aggregate_type, aggregate_id, seq);

PRAGMA user_version = 5;
COMMIT;
PRAGMA foreign_keys = ON;

------------------------------------------------------------------
-- REVERSE (README rule 3). The runner cannot run this. It is the inverse, written while the
-- change is fresh. Back up the database first. Run it with `PRAGMA foreign_keys = OFF` before
-- the BEGIN, as above.
--
-- It is only valid after the instance rows are gone. Any run with `instance_id` set has no
-- `cluster_id` to go back to, so delete those runs and their steps first:
--
--   PRAGMA foreign_keys = OFF;
--   BEGIN;
--   DELETE FROM workflow_steps WHERE run_id IN (SELECT id FROM workflow_runs WHERE instance_id IS NOT NULL);
--   DELETE FROM workflow_runs  WHERE instance_id IS NOT NULL;
--   CREATE TABLE workflow_runs_old (
--       id TEXT PRIMARY KEY, workflow TEXT NOT NULL, workflow_version INTEGER NOT NULL,
--       cluster_id TEXT NOT NULL REFERENCES clusters(id),
--       deployment_id TEXT REFERENCES deployments(id),
--       dedupe_key TEXT UNIQUE, args TEXT NOT NULL DEFAULT '{}',
--       status TEXT NOT NULL CHECK (status IN
--           ('pending','running','blocked','compensating','succeeded','failed','cancelled')),
--       cancel_requested INTEGER NOT NULL DEFAULT 0, failed_step TEXT, error TEXT,
--       undo_incomplete TEXT, initiated_by TEXT, created_at TEXT NOT NULL,
--       started_at TEXT, finished_at TEXT);
--   INSERT INTO workflow_runs_old
--       (id, workflow, workflow_version, cluster_id, deployment_id, dedupe_key, args, status,
--        cancel_requested, failed_step, error, undo_incomplete, initiated_by,
--        created_at, started_at, finished_at)
--   SELECT id, workflow, workflow_version, cluster_id, deployment_id, dedupe_key, args, status,
--        cancel_requested, failed_step, error, undo_incomplete, initiated_by,
--        created_at, started_at, finished_at
--   FROM workflow_runs;
--   DROP TABLE workflow_runs;   -- drops ux_wr_one_active, ux_wr_one_active_instance, ix_wr_cluster
--   ALTER TABLE workflow_runs_old RENAME TO workflow_runs;
--   CREATE UNIQUE INDEX ux_wr_one_active ON workflow_runs (cluster_id)
--       WHERE status IN ('pending','running','blocked','compensating');
--   CREATE INDEX ix_wr_cluster ON workflow_runs (cluster_id, created_at DESC);
--   -- effects_outbox: back to the 0001 CHECK. Rows with aggregate_type 'instance' cannot be kept;
--   -- delete them first (they belong to instances being dropped below).
--   DELETE FROM effects_outbox WHERE aggregate_type = 'instance';
--   CREATE TABLE effects_outbox_old (
--       seq INTEGER PRIMARY KEY AUTOINCREMENT, effect_id TEXT NOT NULL UNIQUE,
--       aggregate_type TEXT NOT NULL CHECK (aggregate_type IN ('cluster','deployment','run')),
--       aggregate_id TEXT NOT NULL, to_version INTEGER NOT NULL, ordinal INTEGER NOT NULL,
--       kind TEXT NOT NULL CHECK (kind IN ('persist','schedule_timer','cancel_timer',
--           'run_workflow','cancel_workflow','cascade','notify')),
--       payload TEXT NOT NULL, lane TEXT NOT NULL CHECK (lane IN ('tx','drain')),
--       status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','done','dead')),
--       attempts INTEGER NOT NULL DEFAULT 0, available_at TEXT NOT NULL, created_at TEXT NOT NULL,
--       done_at TEXT, last_error TEXT);
--   INSERT INTO effects_outbox_old SELECT * FROM effects_outbox;   -- same column order
--   -- keep the high-water mark, as 0005 did:
--   INSERT INTO sqlite_sequence (name, seq)
--       SELECT 'effects_outbox_old', seq FROM sqlite_sequence
--       WHERE name = 'effects_outbox' AND NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'effects_outbox_old');
--   UPDATE sqlite_sequence SET seq = (SELECT seq FROM sqlite_sequence WHERE name = 'effects_outbox')
--       WHERE name = 'effects_outbox_old' AND seq < (SELECT seq FROM sqlite_sequence WHERE name = 'effects_outbox');
--   DROP TABLE effects_outbox;
--   ALTER TABLE effects_outbox_old RENAME TO effects_outbox;
--   CREATE INDEX idx_outbox_drain     ON effects_outbox (status, available_at, seq);
--   CREATE INDEX idx_outbox_aggregate ON effects_outbox (aggregate_type, aggregate_id, seq);
--   DROP TABLE instances;       -- drops its indexes; destroys the instance records
--   PRAGMA user_version = 4;
--   COMMIT;
--   PRAGMA foreign_keys = ON;
--   PRAGMA foreign_key_check;   -- must return no rows
--
-- Dropping `instances` loses the only record of any VM it names. Destroy or reconcile those VMs first.
