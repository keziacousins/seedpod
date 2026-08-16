-- 0001_initial.sql — the entire v2 schema, assembled from the ratified design:
--   clusters/deployments/audits ......... docs/design/coherence-review.md Conflict 11
--   workflow_runs/workflow_steps ........ docs/design/coherence-review.md Conflict 4
--   effects_outbox/timers ............... docs/design/coherence-review.md Conflict 1
--   deployment_audits, api_keys, secrets, secret_audits, deployment_presets,
--   snapshots ........................... docs/design/seam-d-foundation.md (unamended)
-- Conventions (Seam D): timestamps TEXT ISO-8601 UTC written by the injected clock
-- (no DB-side defaults); JSON as TEXT; no CHECK on machine-owned status columns —
-- the Pillar-1 transition table is the sole authority for those state sets.

------------------------------------------------------------------
-- clusters — the Pillar-1 aggregate. Coarse lifecycle states only.
------------------------------------------------------------------
CREATE TABLE clusters (
    id                   TEXT PRIMARY KEY,          -- uuid4 always; routes accept id-or-slug
    name                 TEXT NOT NULL,
    slug                 TEXT NOT NULL,
    origin               TEXT NOT NULL DEFAULT 'managed'
                         CHECK (origin IN ('managed','discovered')),   -- Seam A Origin; UNMANAGED is a STATUS
    environment          TEXT NOT NULL,
    repository           TEXT,
    branch               TEXT,
    status               TEXT NOT NULL,             -- NO CHECK; owned by Pillar 1. The set is Seam A's ten:
                                                    -- new/provisioning/active/destroy-scheduled/destroying/
                                                    -- destroyed/destroy-failed/failed/zombie/unmanaged
    pre_destroy_state    TEXT,                      -- set on entry to destroy-scheduled; cancel returns here
    version              INTEGER NOT NULL DEFAULT 0,
    provider             TEXT NOT NULL,
    provider_config      TEXT NOT NULL DEFAULT '{}',-- provisioning INPUTS (JSON)
    provider_resources   TEXT NOT NULL DEFAULT '{}',-- provisioning OUTPUTS (JSON); fed by InfraAllocated
    dns_hostname         TEXT,
    dns_zone             TEXT,
    public_ip            TEXT,
    node_count           INTEGER NOT NULL DEFAULT 1,
    encrypted_kubeconfig TEXT,
    kubeconfig_key_class TEXT CHECK (kubeconfig_key_class IN ('DEV','PROD')),
    kubeconfig_ref       TEXT,                      -- opaque handle from cluster.store_kubeconfig (DR-0022; Conflict 9)
    cost_per_hour        REAL NOT NULL DEFAULT 0,
    total_cost           REAL NOT NULL DEFAULT 0,
    consecutive_health_failures INTEGER NOT NULL DEFAULT 0,
    failure_reason       TEXT,                      -- was error_message; one name system-wide
    last_reconciled_at   TEXT,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL,
    expires_at           TEXT
);
CREATE INDEX ix_clusters_slug   ON clusters(slug);
CREATE INDEX ix_clusters_status ON clusters(status);
CREATE INDEX ix_clusters_expires_at ON clusters(expires_at);
CREATE INDEX ix_clusters_repo_branch_env ON clusters(repository, branch, environment);
    -- restores the index v1 migration c9cc7e35e6ea created and db620eadda40 silently dropped;
    -- backs find_active_cluster_by_branch (the version-update hot path)

-- Slug reusable after destroy, unique while live. TERMINAL_STATES = ('destroyed','failed')
-- exported by Pillar 1; 'destroy-failed' and 'zombie' stay live (they own real infra):
CREATE UNIQUE INDEX ux_clusters_slug_live ON clusters(slug)
    WHERE status NOT IN ('destroyed','failed');

------------------------------------------------------------------
-- deployments — the second, small machine. Same Persist discipline.
------------------------------------------------------------------
CREATE TABLE deployments (
    id               TEXT PRIMARY KEY,
    cluster_id       TEXT NOT NULL REFERENCES clusters(id),
    environment      TEXT NOT NULL,
    status           TEXT NOT NULL,                 -- NO CHECK; Seam A's nine deployment states
    version          INTEGER NOT NULL DEFAULT 0,
    manifest_version TEXT NOT NULL,
    spec_ref         TEXT REFERENCES deployment_audits(id),  -- DeployRequested.spec_ref = the audit row
    resolved_images  TEXT NOT NULL DEFAULT '{}',    -- was `services`; set by DeploySucceeded
    superseded_by    TEXT REFERENCES deployments(id),
    deployed_by      TEXT,
    failure_reason   TEXT,                          -- was error_message
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX ix_deployments_cluster ON deployments(cluster_id, created_at DESC);

------------------------------------------------------------------
-- deployment_audits — immutable, with ONE deliberate exception (DR-0025
-- Erratum E2, docs/decisions/DR-0025-hostname-resolution-ordering.md): a row
-- born DEFERRED (resolved_config carries `manifest_rendering_deferred: true`,
-- resolved_manifests empty -- a provider_host profile whose host was unknowable
-- at decision time) is rewritten IN PLACE, once, at deploy time, once the
-- cluster is ACTIVE and its real host is known
-- (DeploymentAuditRepository.update_rendered_manifests, seedpod/data/
-- repositories.py). "One row, one truth" — never a second row for the same
-- deployment. Every other write path still only ever INSERTs. Fixes the
-- INTEGER-vs-VARCHAR FK (v1 gotcha 3) and the missing cluster_id FK.
------------------------------------------------------------------
CREATE TABLE deployment_audits (
    id                           TEXT PRIMARY KEY,
    deployment_id                TEXT REFERENCES deployments(id),
    cluster_id                   TEXT NOT NULL REFERENCES clusters(id),
    environment                  TEXT NOT NULL,
    triggering_repo              TEXT NOT NULL,
    triggering_branch            TEXT NOT NULL,
    triggering_image             TEXT NOT NULL,
    commit_sha                   TEXT,
    deployment_profile_name      TEXT NOT NULL,
    resolution_strategy          TEXT NOT NULL,
    registry_queries             TEXT NOT NULL DEFAULT '[]',        -- JSON
    resolved_images              TEXT NOT NULL DEFAULT '{}',        -- JSON
    resolved_config              TEXT NOT NULL DEFAULT '{}',        -- JSON
    encrypted_resolved_manifests TEXT NOT NULL,                     -- via CryptoService
    encrypted_resolved_secrets   TEXT NOT NULL,                     -- via CryptoService
    key_class                    TEXT NOT NULL CHECK (key_class IN ('DEV','PROD')),
    template_files_used          TEXT NOT NULL DEFAULT '[]',        -- JSON
    created_at                   TEXT NOT NULL
);
CREATE INDEX ix_deployment_audits_cluster    ON deployment_audits(cluster_id, created_at DESC);
CREATE INDEX ix_deployment_audits_deployment ON deployment_audits(deployment_id);

------------------------------------------------------------------
-- State audits — one shape for both machines, written by the
-- Dispatcher in the SAME transaction as the Persist. actor replaces
-- v1's trigger/initiated_by (Seam A actor grammar: 'api:<user>' |
-- 'reconciler' | 'health' | 'engine:run:<id>' | 'timer:<key>' |
-- 'cluster-machine'); created_at = event.at (aware UTC 'Z').
------------------------------------------------------------------
CREATE TABLE cluster_state_audits (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_id   TEXT NOT NULL REFERENCES clusters(id),
    from_state   TEXT NOT NULL,
    to_state     TEXT NOT NULL,
    event        TEXT NOT NULL,
    actor        TEXT NOT NULL,
    reason       TEXT,
    context      TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX ix_csa_cluster_time ON cluster_state_audits(cluster_id, created_at DESC);

CREATE TABLE deployment_state_audits (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id TEXT NOT NULL REFERENCES deployments(id),
    cluster_id    TEXT NOT NULL REFERENCES clusters(id),
    from_state    TEXT NOT NULL,
    to_state      TEXT NOT NULL,
    event         TEXT NOT NULL,
    actor         TEXT NOT NULL,
    reason        TEXT,
    context       TEXT,
    created_at    TEXT NOT NULL
);
CREATE INDEX ix_dsa_deployment_time ON deployment_state_audits(deployment_id, created_at DESC);

------------------------------------------------------------------
-- api_keys / secrets — near-verbatim salvage, constraints made real.
------------------------------------------------------------------
CREATE TABLE api_keys (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    key_hash     TEXT NOT NULL UNIQUE,
    username     TEXT NOT NULL,
    environment  TEXT NOT NULL DEFAULT 'all',       -- 'all' sentinel kept verbatim (auth helpers + e2e)
    permissions  TEXT NOT NULL DEFAULT '[]',        -- JSON
    is_active    INTEGER NOT NULL DEFAULT 1,
    description  TEXT,
    created_by   TEXT,
    created_at   TEXT NOT NULL,
    expires_at   TEXT,
    last_used_at TEXT
);

CREATE TABLE secrets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    environment     TEXT NOT NULL,
    key_name        TEXT NOT NULL,
    encrypted_value TEXT NOT NULL,
    key_class       TEXT NOT NULL CHECK (key_class IN ('DEV','PROD')),
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE (environment, key_name)                  -- v1 gotcha 4 closed; repo upsert is
                                                    -- INSERT .. ON CONFLICT(environment,key_name) DO UPDATE
);

CREATE TABLE secret_audits (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    environment  TEXT NOT NULL,
    key_name     TEXT NOT NULL,
    action       TEXT NOT NULL CHECK (action IN ('create','update','delete','reveal')),
    performed_by TEXT NOT NULL,
    key_class    TEXT NOT NULL,
    context      TEXT,                              -- JSON
    created_at   TEXT NOT NULL
);
CREATE INDEX ix_secret_audits_env_key ON secret_audits(environment, key_name, created_at DESC);

------------------------------------------------------------------
-- Feature tables kept from v1 (presets are the only Tart
-- provider-override deploy path; snapshot rows index on-disk data).
------------------------------------------------------------------
CREATE TABLE deployment_presets (
    id                TEXT PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,
    description       TEXT,
    profile_name      TEXT NOT NULL,
    environment       TEXT NOT NULL,
    service_overrides TEXT,                         -- JSON
    default_branch    TEXT,
    default_ttl_hours INTEGER,
    naming_strategy   TEXT,                         -- JSON
    created_by        TEXT NOT NULL,
    created_at        TEXT NOT NULL,
    last_used_at      TEXT,
    use_count         INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE snapshots (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    description         TEXT,
    source_cluster_id   TEXT NOT NULL REFERENCES clusters(id),
    source_cluster_slug TEXT NOT NULL,
    branch              TEXT,
    deployment_profile  TEXT NOT NULL,
    services            TEXT NOT NULL,              -- JSON
    storage_path        TEXT NOT NULL,
    total_size_bytes    INTEGER NOT NULL DEFAULT 0,
    is_auto             INTEGER NOT NULL DEFAULT 0,
    created_by          TEXT NOT NULL,
    created_at          TEXT NOT NULL
);

------------------------------------------------------------------
-- workflow_runs / workflow_steps — the engine cursor (Conflict 4:
-- Seam B's structure, Seam D's SQLite conventions). One row per
-- step INSTANCE keyed by materialized step_path; no heartbeats.
------------------------------------------------------------------
CREATE TABLE workflow_runs (
    id                TEXT PRIMARY KEY,                 -- uuid4
    workflow          TEXT NOT NULL,                    -- CONCRETE definition name (Conflict 13)
    workflow_version  INTEGER NOT NULL,                 -- pins the YAML version at admission
    cluster_id        TEXT NOT NULL REFERENCES clusters(id),
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
    finished_at       TEXT
);
CREATE UNIQUE INDEX ux_wr_one_active ON workflow_runs (cluster_id)
    WHERE status IN ('pending','running','blocked','compensating');       -- H14
CREATE INDEX ix_wr_cluster ON workflow_runs (cluster_id, created_at DESC);

CREATE TABLE workflow_steps (
    run_id            TEXT NOT NULL REFERENCES workflow_runs(id),
    step_path         TEXT NOT NULL,                    -- 'create' | 'wave[1].apply'
    verb              TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN
                        ('running','gating','succeeded','failed','failed_continued','cancelled')),
    attempt           INTEGER NOT NULL DEFAULT 1,
    interrupted_count INTEGER NOT NULL DEFAULT 0,
    params            TEXT NOT NULL,                    -- resolved bindings (secrets encrypted)
    notes             TEXT NOT NULL DEFAULT '{}',       -- ctx.note() write-ahead facts
    output            TEXT,                             -- Output.model_dump(); SecretStr fields encrypted
    undo_status       TEXT CHECK (undo_status IN ('done','failed','skipped')),
    error             TEXT,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    PRIMARY KEY (run_id, step_path)
);

------------------------------------------------------------------
-- effects_outbox / timers — durable effects (Conflict 1: Seam A's
-- two-table design, amended). Written in the same transaction as
-- the Persist + audit row; drained by the EffectExecutor.
------------------------------------------------------------------
CREATE TABLE effects_outbox (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    effect_id      TEXT    NOT NULL UNIQUE,      -- "{aggregate_type}/{aggregate_id}@{to_version}#{ordinal}"
                                                 -- engine-origin rows: "run/{run_id}@{step_path}#{n}"
    aggregate_type TEXT    NOT NULL CHECK (aggregate_type IN ('cluster','deployment','run')),
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
CREATE INDEX idx_outbox_drain     ON effects_outbox (status, available_at, seq);
CREATE INDEX idx_outbox_aggregate ON effects_outbox (aggregate_type, aggregate_id, seq);

CREATE TABLE timers (
    aggregate_type    TEXT NOT NULL,
    aggregate_id      TEXT NOT NULL,
    timer_key         TEXT NOT NULL,             -- 'ttl' | 'destroy'
    fire_at           TEXT NOT NULL,
    event             TEXT NOT NULL,             -- codec.encode(event), applied verbatim on fire
    created_by_effect TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, timer_key)
);
CREATE INDEX idx_timers_fire ON timers (fire_at);

PRAGMA user_version = 1;
