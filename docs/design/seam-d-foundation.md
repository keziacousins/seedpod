---
title: Seam D — Persistence schema & composition root
type: design
status: active
created: 2026-07-12
updated: 2026-07-12
amended-by: coherence-review.md   # Conflicts 1–4, 11, 13, 15 REPLACE this spec's outbox/timers, workflow_runs, and clusters/deployments/audits DDL
---

# Seam D — FINAL SPEC: Persistence schema & composition root

## Verdicts

**Proposal 1 (clean-slate)** has the right structural skeleton and wins most of the mechanism decisions: the numbered-migration single authority (`PRAGMA user_version` runner, no `create_all` shadow), the input/output split of `provider_config`, `key_class` stamped beside every ciphertext, `CancelTimer` as an inline transactional `DELETE` (not an outbox row), the `dedupe_key` partial-unique for timers, `workflow_version` pinning, the one-active-run partial unique index, and the acyclic `Dispatcher` that lets the engine be constructed *after* the transition-applier with no post-hoc binding. Its fatal flaws are regression-shaped: its status `CHECK` constraints hardcode the **wrong spellings** (v1's `ClusterStatus` values are hyphenated — `destroy-scheduled`, `destroy-failed` — verified in `reference-code/seedpod/seedpod/core/cluster_spec.py:21-33`; the proposal wrote underscores) while also freezing the Pillar-1 state set into DDL; and it silently drops `deployment_presets`, `snapshots`, `consecutive_health_failures`, and `total_cost` — live features (`/api/presets` is the only Tart provider-override deploy path; snapshot rows index on-disk storage) discarded without a plan mandate.

**Proposal 2 (grounded-in-callers)** wins on regression safety and inventory completeness: it caught exactly the tables and columns a clean model loses, its v1→v2 column mapping and global→injection table are the best audit artifacts in either proposal, `background_jobs_enabled` is the right test lever, and fail-fast `RuleEngine` construction fixes a real v1 swallow. Its fatal flaws are mechanical: the idempotent `CREATE TABLE IF NOT EXISTS` schema **re-creates gotcha 1** (once a table exists, `IF NOT EXISTS` silently ignores column changes — the exact drift disease of v1's `create_all`-plus-empty-migrations); `cancel_timer` as an outbox row has a fire-before-cancel ordering race (a due timer drains ahead of the cancel row queued behind it); the `bind_event_sink` post-hoc setter papers over a constructor cycle Proposal 1 dissolves; and DB-side `strftime` defaults break `FrozenClock` determinism.

The final spec is Proposal 1's skeleton with Proposal 2's inventory grafted in.

---

# THE FINAL SPEC

## Decision 6 — The fresh schema

### Authority rule (closes v1 gotchas 1–2 structurally)

The schema exists in **exactly one place**: numbered SQL files under `seedpod/data/migrations/NNNN_*.sql`, applied in order by a ~30-line runner keyed on `PRAGMA user_version`. There is **no** `Base.metadata.create_all()` anywhere in v2 and no alembic. Repositories use SQLAlchemy Core over these tables and map rows to the salvaged DTO dataclasses (session-in, DTO-out, ABC + impl — the shape from `reference-code/seedpod/seedpod/data/repositories.py`, with two changes: no `session.commit()` inside repos, and no audit-writing/ID-derivation inside `create_cluster`).

```python
# seedpod/data/migrate.py — the entire migration system
def migrate(engine: Engine, migrations_dir: Path) -> None:
    files = sorted(migrations_dir.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    with engine.begin() as conn:
        current = conn.exec_driver_sql("PRAGMA user_version").scalar()
    for f in files:
        n = int(f.name[:4])
        if n <= current:
            continue
        with engine.begin() as conn:                       # one txn per migration
            conn.connection.executescript(f.read_text())   # file ends with PRAGMA user_version = N
    with engine.begin() as conn:
        if conn.exec_driver_sql("PRAGMA user_version").scalar() != int(files[-1].name[:4]):
            raise MigrationError("migration file did not stamp user_version")
```

**Conventions:**
- All timestamps `TEXT` ISO-8601 UTC (`2026-07-12T09:00:00.000Z`), **written by the application via the injected clock** — no DB-side `strftime` defaults, so `FrozenClock` tests are deterministic.
- All JSON as `TEXT`; repos own (de)serialization.
- Per-connection pragmas via the engine's connect listener (v1 parity): `PRAGMA foreign_keys=ON`, `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=30000`. SQLite engine uses `StaticPool` + `check_same_thread=False` as v1 does.
- **`CHECK` policy:** the DB constrains only enums the persistence/engine layer owns (`kind`, `key_class`, audit `action`, outbox `effect`, `workflow_runs.status`). It does **not** constrain `clusters.status` or `deployments.status` — the Pillar-1 transition table is the sole authority for machine state sets, and v1's hyphenated status strings (`destroy-scheduled`, `destroy-failed`) are salvaged verbatim so the ported acceptance spec and UI see identical values.

```sql
-- seedpod/data/migrations/0001_initial.sql

------------------------------------------------------------------
-- clusters — the Pillar-1 aggregate. Coarse lifecycle states only.
------------------------------------------------------------------
CREATE TABLE clusters (
    id                   TEXT PRIMARY KEY,          -- uuid4, ALWAYS. v1's slug-as-PK and
                                                    -- provider_config['cluster_uuid'] overrides are dropped;
                                                    -- discovered clusters get a fresh uuid, the provider's
                                                    -- own id lives in provider_resources. API routes accept
                                                    -- id-or-slug for lookups (v1 UX preserved).
    name                 TEXT NOT NULL,
    slug                 TEXT NOT NULL,             -- DNS-safe; globally NON-unique (reuse after destroy,
                                                    -- per v1 migration db620eadda40) — see partial index below
    kind                 TEXT NOT NULL DEFAULT 'managed'
                         CHECK (kind IN ('managed','discovered','unmanaged')),
                                                    -- un-smuggles v1's environment sentinels: 'discovered'/
                                                    -- 'unmanaged' were cluster KIND riding in environment
    environment          TEXT NOT NULL,             -- real env only: local/development/ephemeral/staging/production
    repository           TEXT,
    branch               TEXT,
    status               TEXT NOT NULL,             -- NO CHECK: owned by Pillar 1's transition table.
                                                    -- Values are v1's hyphenated strings verbatim:
                                                    -- creating/provisioning/deploying/active/destroy-scheduled/
                                                    -- destroying/destroyed/destroy-failed/failed/zombie
    version              INTEGER NOT NULL DEFAULT 0,-- optimistic concurrency; retires the asyncio.Lock dict (H8/H9)
    provider             TEXT NOT NULL,
    provider_config      TEXT NOT NULL DEFAULT '{}',-- JSON, provisioning INPUTS only (size, region, image)
    provider_resources   TEXT NOT NULL DEFAULT '{}',-- JSON, provisioning OUTPUTS written by workflow steps
                                                    -- (droplet_id, provider_resource_ids, VM names).
                                                    -- The v1 grab-bag, split by data direction; the keys
                                                    -- business logic read by string are real columns below.
                                                    -- v1's provider_config.created_by/.repo: DROPPED (dupes).
    dns_hostname         TEXT,                      -- promoted out of provider_config (DTO property + UI read it)
    dns_zone             TEXT,                      -- promoted out of provider_config
    public_ip            TEXT,
    node_count           INTEGER NOT NULL DEFAULT 1,
    encrypted_kubeconfig TEXT,                      -- Fernet+b64 ciphertext. Crypto lives ONLY in CryptoService,
                                                    -- never on a row object — kills the re-fetch-ORM-to-encrypt
                                                    -- motive behind most H10 sites and H18.
    kubeconfig_key_class TEXT CHECK (kubeconfig_key_class IN ('DEV','PROD')),
                                                    -- stamped at encrypt time; decrypt reads the stamp and
                                                    -- never re-derives key class from environment (gotcha 8)
    cost_per_hour        REAL NOT NULL DEFAULT 0,
    total_cost           REAL NOT NULL DEFAULT 0,   -- kept (UI cost display; frozen at destroy)
    consecutive_health_failures INTEGER NOT NULL DEFAULT 0,
                                                    -- kept: durable per-cluster counter for the health poll
                                                    -- (v1 job_manager.py:634,647); mutated ONLY via a
                                                    -- dedicated repo method — the bypass is gone, not the column
    error_message        TEXT,
    last_reconciled_at   TEXT,                      -- promoted out of provider_config (staleness check reads it)
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL,
    expires_at           TEXT                       -- TTL deadline; enforcement is a ScheduleTimer outbox row
);
CREATE INDEX ix_clusters_slug   ON clusters(slug);
CREATE INDEX ix_clusters_status ON clusters(status);
CREATE INDEX ix_clusters_expires_at ON clusters(expires_at);
CREATE INDEX ix_clusters_repo_branch_env ON clusters(repository, branch, environment);
    -- restores the index v1 migration c9cc7e35e6ea created and db620eadda40 silently dropped;
    -- backs find_active_cluster_by_branch (the version-update hot path)

-- v1 wanted "slug reusable after destroy, unique while live" and enforced it only in app code
-- (with terminal states hardcoded as ['destroyed','failed']). The schema now says it:
CREATE UNIQUE INDEX ux_clusters_slug_live ON clusters(slug)
    WHERE status NOT IN ('destroyed','failed');
    -- NOTE: 'destroy-failed' is deliberately treated as LIVE — such a cluster still owns real
    -- infrastructure, so its slug (and DNS hostname) stays reserved until destroy succeeds.
    -- This matches v1 active_only lookup behavior exactly; Pillar 1 exports TERMINAL_STATES
    -- = ('destroyed','failed') and repos filter with that constant, never string literals.

------------------------------------------------------------------
-- deployments — the second, small machine. Same Persist discipline.
------------------------------------------------------------------
CREATE TABLE deployments (
    id               TEXT PRIMARY KEY,              -- uuid4
    cluster_id       TEXT NOT NULL REFERENCES clusters(id),
    status           TEXT NOT NULL,                 -- NO CHECK: owned by the deployment machine
    version          INTEGER NOT NULL DEFAULT 0,
    manifest_version TEXT NOT NULL,
    services         TEXT NOT NULL DEFAULT '{}',    -- JSON {service: image}
    deployed_by      TEXT,
    rollback_target  TEXT REFERENCES deployments(id),
    error_message    TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX ix_deployments_cluster ON deployments(cluster_id, created_at DESC);

------------------------------------------------------------------
-- deployment_audits — immutable. Fixes the INTEGER-vs-VARCHAR FK
-- (v1 gotcha 3) and the missing cluster_id FK.
------------------------------------------------------------------
CREATE TABLE deployment_audits (
    id                           TEXT PRIMARY KEY,
    deployment_id                TEXT REFERENCES deployments(id),   -- TEXT, real FK; DTO retyped str|None
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
-- cluster_state_audits — written by the Dispatcher in the SAME
-- transaction as the Persist (not by the repo, unlike v1).
------------------------------------------------------------------
CREATE TABLE cluster_state_audits (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_id   TEXT NOT NULL REFERENCES clusters(id),
    from_state   TEXT NOT NULL,
    to_state     TEXT NOT NULL,
    event        TEXT NOT NULL,                     -- the Pillar-1 Event name (new precision)
    trigger      TEXT NOT NULL,                     -- source: api_request/reconciliation/timer/workflow (v1 semantic)
    initiated_by TEXT,
    reason       TEXT,
    context      TEXT,                              -- JSON
    created_at   TEXT NOT NULL
);
CREATE INDEX ix_csa_cluster_time ON cluster_state_audits(cluster_id, created_at DESC);
    -- v1 migration 26a277317e66 declared these indexes; a later table rebuild lost them. Here they exist.

------------------------------------------------------------------
-- api_keys / secrets — near-verbatim salvage, constraints made real.
------------------------------------------------------------------
CREATE TABLE api_keys (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    key_hash     TEXT NOT NULL UNIQUE,
    username     TEXT NOT NULL,
    environment  TEXT NOT NULL DEFAULT 'all',       -- 'all' sentinel kept verbatim: three auth helpers and
                                                    -- the acceptance e2e authenticate through this semantic
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
    UNIQUE (environment, key_name)                  -- v1 gotcha 4 closed: the constraint the model comment
                                                    -- claimed but never had. Repo upsert becomes
                                                    -- INSERT .. ON CONFLICT(environment,key_name) DO UPDATE,
                                                    -- so the duplicate-key race is unrepresentable.
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
-- Feature tables kept from v1 (a "clean six-table model" silently
-- kills /api/presets — the only Tart provider-override deploy path —
-- reveal auditing above, and the rows that index on-disk snapshots).
-- snapshot_operations is NOT kept: it is a proto-workflow_runs and
-- is subsumed (operation_type→workflow, progress→step_results).
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
-- workflow_runs — the engine cursor. One row per run; the row IS
-- the resumable state.
------------------------------------------------------------------
CREATE TABLE workflow_runs (
    id               TEXT PRIMARY KEY,              -- uuid4
    workflow         TEXT NOT NULL,                 -- definition name ('provision','deploy-waves','destroy',
                                                    -- 'snapshot-create','snapshot-restore',...)
    workflow_version INTEGER NOT NULL,              -- definition version pinned at spawn; resume replays
                                                    -- against the pinned version, never a newer file
    cluster_id       TEXT REFERENCES clusters(id),
    deployment_id    TEXT REFERENCES deployments(id),
    status           TEXT NOT NULL CHECK (status IN (
                         'pending','running','compensating',
                         'succeeded','failed','compensated','cancelled')),
                                                    -- CHECK kept: this enum is engine-owned, not Pillar-1's.
                                                    -- 'compensated' = failed AND undo stack fully unwound;
                                                    -- 'failed' = failed dirty (reconciliation's problem).
    cursor           INTEGER NOT NULL DEFAULT 0,    -- index of the NEXT step in the pinned definition
    attempt          INTEGER NOT NULL DEFAULT 0,    -- retry count of the step at cursor (Schedule state)
    step_results     TEXT NOT NULL DEFAULT '{}',    -- JSON {step_id: {outputs:{...}, finished_at}} — the typed
                                                    -- named bindings; also the undo stack (compensation walks
                                                    -- completed steps in reverse)
    args             TEXT NOT NULL DEFAULT '{}',    -- JSON: the RunWorkflow effect args, frozen at spawn
    error            TEXT,                          -- JSON {kind:'transient'|'permanent', step, message}
    cancel_requested INTEGER NOT NULL DEFAULT 0,    -- durable cooperative cancel token (H16): API sets it,
                                                    -- engine checks between steps — survives restart
    initiated_by     TEXT,
    version          INTEGER NOT NULL DEFAULT 0,    -- every cursor advance uses WHERE version = expected
    heartbeat_at     TEXT,                          -- runner liveness; reconciliation resumes non-terminal
                                                    -- runs with stale heartbeats
    created_at       TEXT NOT NULL,
    started_at       TEXT,
    finished_at      TEXT
);
CREATE INDEX ix_wr_active ON workflow_runs(status)
    WHERE status IN ('pending','running','compensating');
CREATE INDEX ix_wr_cluster ON workflow_runs(cluster_id, created_at DESC);
-- Two concurrent runs of the same workflow on one cluster are structurally impossible:
CREATE UNIQUE INDEX ux_wr_one_active ON workflow_runs(cluster_id, workflow)
    WHERE status IN ('pending','running','compensating') AND cluster_id IS NOT NULL;

------------------------------------------------------------------
-- outbox — durable effects, written in the SAME transaction as the
-- Persist + audit row, drained by the effect executor. H7 is closed
-- by persistence: a crash mid-broadcast replays from here.
------------------------------------------------------------------
CREATE TABLE outbox (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT, -- global delivery order
    effect       TEXT NOT NULL CHECK (effect IN ('notify','run_workflow','schedule_timer')),
                                                    -- CancelTimer is deliberately NOT an outbox row: it is an
                                                    -- inline transactional DELETE (see mapping below). Queuing
                                                    -- a cancel behind a due timer is a fire-before-cancel race.
    payload      TEXT NOT NULL,                     -- JSON-serialized Effect, exactly as the pure machine
                                                    -- emitted it (Pillar-1 effects are inert data by definition)
    cluster_id   TEXT,                              -- denormalized for observability
    dedupe_key   TEXT,                              -- timers: 'timer:ttl:<cluster_id>', etc.
    available_at TEXT NOT NULL,                     -- now for notify/run_workflow; now+after for timers —
                                                    -- ScheduleTimer IS a future-dated outbox row; no timer subsystem
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    processed_at TEXT
);
CREATE INDEX ix_outbox_pending ON outbox(available_at) WHERE processed_at IS NULL;
CREATE UNIQUE INDEX ux_outbox_dedupe ON outbox(dedupe_key)
    WHERE processed_at IS NULL AND dedupe_key IS NOT NULL;

PRAGMA user_version = 1;
```

### Effect → storage mapping (the co-design)

Of the five Pillar-1 effects, **two execute inside the transition transaction and three become outbox rows**:

| Effect | Storage action | Where |
|---|---|---|
| `Persist` | `UPDATE … WHERE version = expected` + `cluster_state_audits` INSERT | inline, same txn |
| `CancelTimer` | `DELETE FROM outbox WHERE dedupe_key = :key AND processed_at IS NULL` | inline, same txn (cancelling an unfired timer is a transactional fact, not a job) |
| `Notify` | outbox INSERT, `available_at = now` | drained → SSE hub; **log-don't-raise**, then mark processed |
| `RunWorkflow` | outbox INSERT, `available_at = now` | drained → engine inserts `workflow_runs` row, marks processed |
| `ScheduleTimer` | outbox `INSERT … ON CONFLICT(dedupe_key) DO UPDATE SET available_at, payload` | drained when due → Dispatcher re-enters `transition()` with the stored event (TTL extension = re-schedule same key) |

Executor drain loop: `SELECT … WHERE processed_at IS NULL AND available_at <= now ORDER BY seq`; transient failures increment `attempts` with backoff; after N attempts the row is marked processed with `last_error` set and logged loudly (poison-row policy, never a stuck queue).

### The write discipline

One transition = exactly one transaction. **Repositories never commit** (v1 gotcha 5 is a behavioral change to the salvaged pattern, not a copy); the `UnitOfWork` commits once on clean exit. Nothing outside the repositories touches these tables — the H10 class dies because every mutation callers needed (status flips, health-failure counters, audit decrypt) is a first-class repo method and crypto is detached from rows.

```python
# The ONLY code path that mutates clusters.status — the Dispatcher.
def apply(self, cluster_id: str, event: Event, *, trigger: str, initiated_by: str | None) -> ClusterRecord:
    with self.uow() as tx:                                    # one session, one commit, owned here
        cluster = self.repos.clusters.get(tx, cluster_id)     # DTO carries .version
        new, effects = transition(cluster, event)             # Pillar 1, pure
        self.repos.clusters.persist(tx, new, expected_version=cluster.version)  # raises StaleVersionError
        self.repos.state_audits.add(tx, from_state=cluster.status, to_state=new.status,
                                    event=event.name, trigger=trigger, initiated_by=initiated_by)
        for eff in effects:
            match eff:
                case Persist():      pass                     # already done above
                case CancelTimer():  self.repos.outbox.cancel(tx, eff.dedupe_key)   # inline DELETE
                case _:              self.repos.outbox.add(tx, eff, clock=self.clock)
    self.executor.poke()          # wake the drain loop; correctness NEVER depends on this
    return new
```

```sql
-- repos.clusters.persist emits exactly this; rowcount == 1 is the success signal
UPDATE clusters SET status=:status, ..., updated_at=:now, version = version + 1
WHERE id = :id AND version = :expected;
-- rowcount == 0  →  raise StaleVersionError(cluster_id, expected)
-- caller (API/reconciler/timer) re-reads and re-applies; no lock dict, no 30s timeout path (H8/H9)
```

The identical discipline applies to `deployments` (deployment machine) and to the engine's cursor writes on `workflow_runs`.

### v1 → v2 delta summary (regression audit)

| v1 | v2 | Why |
|---|---|---|
| `clusters.id` slug-or-UUID-or-`cluster_uuid` | uuid4 always; routes accept id-or-slug | gotcha 7; CIDR hashing gets a real UUID; salvaged `allocate_cluster_cidrs` unchanged |
| `environment` sentinels `'discovered'`/`'unmanaged'` | `kind` column | `get_key_class` now sees only real envs — and it **raises on unknown env** instead of defaulting DEV (gotcha 8), with the stamp columns making decrypt independent of the mapping entirely |
| `provider_config` grab-bag | inputs stay; outputs → `provider_resources`; `dns_hostname`/`dns_zone`/`last_reconciled_at` → columns; `created_by`/`repo` dropped | split by data direction + promote the string-keyed business reads |
| ORM crypto (`set_kubeconfig`, `_get_fernet` ×3) | one `CryptoService`; ciphertext + `key_class` stamp columns | removes the H10/H18 bypass motive |
| `deployment_audits.deployment_id INTEGER` | `TEXT` + real FK | gotcha 3 |
| `secrets` app-level uniqueness | `UNIQUE(environment,key_name)` + `ON CONFLICT` upsert | gotcha 4 |
| terminal-state string literals `['destroyed','failed']` | Pillar-1 `TERMINAL_STATES` constant + `ux_clusters_slug_live` partial index | slug-unique-while-live is now schema, not policy; `destroy-failed` deliberately live (still owns infra) |
| `snapshot_operations`, `consecutive_health_failures`-as-workflow-state | `workflow_runs` subsumes operations; health counter column kept, repo-method-only | zero silent regression on the health poll |
| `deployment_presets`, `snapshots`, `secrets_audit_log` | kept (renamed `secret_audits`) | live features; presets are the only provider-override path |
| dual schema authority (create_all + alembic) | numbered SQL + `user_version` runner, nothing else | gotchas 1–2 |

---

## Decision 8 — The composition root

One factory. Construction order **is** the dependency DAG; the only runtime cycle (machine → outbox → executor → engine → dispatcher → machine) is closed **through the database**, so the constructor graph is acyclic — no post-hoc setters, no `bind_*` calls. Nothing in v2 is constructed at import time: importing any v2 module has zero side effects.

```python
# seedpod/app/config.py
@dataclass(frozen=True)
class AppConfig:
    database_url: str
    secret_key_dev: str
    secret_key_prod: str | None = None
    environment: str = "development"
    config_dir: Path = Path("config")             # templates, profiles, rules, workflows, providers
    background_tasks: bool = True                 # False in tests: reconciler + orphan-resume off;
                                                  # the outbox executor ALWAYS runs (it is correctness)
    outbox_poll_interval: float = 0.25
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: tuple[str, ...] = ("*",)
    digitalocean_token: str | None = None
    github_token: str | None = None
    github_organization: str | None = None
    cloudflare_api_token: str | None = None
    enabled_providers: tuple[str, ...] = ("digitalocean", "kind", "tart", "orbstack")

    @classmethod
    def from_env(cls) -> "AppConfig": ...         # the ONLY place os.environ / .env is read
```

```python
# seedpod/app/factory.py — the entire wiring of the system, top to bottom.
def build_app(
    config: AppConfig,
    *,
    providers: Mapping[str, Provider] | None = None,   # test seam: {"fake": FakeProvider()}
    clock: Clock | None = None,                        # test seam: FrozenClock
    id_gen: Callable[[], str] | None = None,           # test seam: deterministic ids
) -> App:
    """Pure construction. No IO, no threads, no DB connection, no env reads, no schema apply."""
    clock = clock or SystemClock()
    id_gen = id_gen or (lambda: str(uuid4()))

    # 1 — leaves (no dependencies)
    db = Database(config.database_url)            # engine + sessionmaker + pragma listener; connects lazily
    crypto = CryptoService(dev_key=config.secret_key_dev, prod_key=config.secret_key_prod)
    hub = SSEHub()                                # in-memory pub/sub, no module global
    subprocesses = SubprocessManager()            # salvaged graceful-shutdown tracker, injected not global

    # 2 — persistence
    repos = Repositories(                         # salvaged session-in/DTO-out shape; commits removed
        clusters=ClusterRepository(), deployments=DeploymentRepository(),
        deployment_audits=DeploymentAuditRepository(crypto),
        state_audits=ClusterStateAuditRepository(),
        api_keys=ApiKeyRepository(), secrets=SecretRepository(crypto),
        secret_audits=SecretAuditRepository(), presets=PresetRepository(),
        snapshots=SnapshotRepository(),
        workflow_runs=WorkflowRunRepository(), outbox=OutboxRepository(),
    )
    uow = UnitOfWork(db)                          # `with uow() as tx:` — commit on exit, rollback on error

    # 3 — pure domain: nothing to construct; `transition()` is a function, import it.

    # 4 — rule engine: FAIL FAST (v1 swallowed RuleValidationError and ran ruleless)
    rules = RuleEngine.load(config.config_dir / "deployment-rules.yml")

    # 5 — providers: stateless, no DB, all context in the command (H18 gone by signature).
    #     ProviderDisabledError becomes absence from the mapping. Every provider's transport
    #     is a TrackedSubprocessRunner(subprocesses); only the tart provider's is additionally
    #     wrapped for its detached `tart run` launch (DR-0005):
    #     DetachedLaunchRunner(TrackedSubprocessRunner(subprocesses), launch_prefixes=(("tart", "run"),))
    providers = dict(providers) if providers is not None \
        else load_enabled_providers(config, subprocesses)

    # 6 — executor placeholder is NOT needed: dispatcher takes executor lazily? No —
    #     dispatcher only WRITES outbox rows; the executor is constructed later and
    #     `poke()`d via the App handle. Order stays a straight line:

    dispatcher = Dispatcher(uow=uow, repos=repos, clock=clock)   # the ONLY writer of transitions

    # 7 — workflow engine: pinned definitions + closed verb registry
    steps = StepRegistry.default(providers=providers, crypto=crypto, repos=repos, uow=uow,
                                 subprocesses=subprocesses, config_dir=config.config_dir, clock=clock)
    definitions = load_workflow_definitions(config.config_dir / "workflows")
    engine = WorkflowEngine(definitions, steps, uow=uow, repos=repos,
                            dispatcher=dispatcher, clock=clock)  # step done → Event → dispatcher.apply

    # 8 — effect executor: drains outbox → hub | engine | dispatcher (timer events)
    executor = EffectExecutor(uow=uow, repos=repos, hub=hub, engine=engine,
                              dispatcher=dispatcher, clock=clock,
                              poll_interval=config.outbox_poll_interval)
    dispatcher.attach_executor(executor)          # sole late wire: gives dispatcher .poke() —
                                                  # a latency optimization only; correctness never depends on it

    # 9 — thin application services (what the API calls; no god object)
    services = Services(
        clusters=ClusterService(dispatcher, repos, uow, id_gen=id_gen, clock=clock),
        deployments=DeploymentService(dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock),
        secrets=SecretService(crypto, repos, uow, clock=clock),        # per-env key_class resolved per call;
        api_keys=ApiKeyService(repos, uow, clock=clock),               #   unknown env RAISES, never DEV-defaults
        reconciliation=ReconciliationService(providers, repos, dispatcher, engine, uow, clock=clock),
    )

    # 10 — HTTP edge, constructed last; consumes services + hub, owns nothing
    api = create_api(services=services, hub=hub, config=config)

    app = App(config=config, db=db, crypto=crypto, hub=hub, subprocesses=subprocesses,
              repos=repos, uow=uow, providers=providers, dispatcher=dispatcher,
              engine=engine, executor=executor, services=services, api=api)
    api.state.app = app                           # lifespan + routes reach App through api.state — no globals
    return app
```

```python
# seedpod/app/app.py
@dataclass
class App:
    config: AppConfig; db: Database; crypto: CryptoService; hub: SSEHub
    subprocesses: SubprocessManager; repos: Repositories; uow: UnitOfWork
    providers: Mapping[str, Provider]; dispatcher: Dispatcher
    engine: WorkflowEngine; executor: EffectExecutor; services: Services; api: FastAPI

    async def start(self) -> None:               # ALL IO lives here, not in build_app
        migrate(self.db.engine, MIGRATIONS_DIR)   # the single schema authority, applied once, here
        await self.executor.start()               # drain pending outbox rows FIRST — crash replay (H7)
        if self.config.background_tasks:
            await self.engine.resume_inflight()   # cursor resume: status in (pending,running,compensating)
            await self.services.reconciliation.start()  # periodic + a real immediate first tick
                                                        # (v1's create_task-into-privates is gone)
    async def stop(self) -> None:                 # exact reverse
        if self.config.background_tasks:
            await self.services.reconciliation.stop()
            await self.engine.stop()              # cooperative; cursors persisted, resume next boot
        await self.executor.stop()
        await self.subprocesses.shutdown()
        await self.hub.close(grace_period=0.5)    # salvaged put_nowait shutdown dance (uvicorn/SSE deadlock)
        self.db.dispose()

    @asynccontextmanager
    async def running(self):
        await self.start()
        try: yield self
        finally: await self.stop()
```

```python
# seedpod/api/factory.py — FastAPI wiring without a module-level app
def create_api(*, services: Services, hub: SSEHub, config: AppConfig) -> FastAPI:
    @asynccontextmanager
    async def lifespan(api: FastAPI):
        install_sse_shutdown_signal_handlers(hub)
        async with api.state.app.running():
            yield

    api = FastAPI(title="Seedpod", lifespan=lifespan)
    api.add_middleware(PermissionEnforcementMiddleware, api_keys=services.api_keys)  # default-deny backstop,
    api.add_middleware(CORSMiddleware, allow_origins=list(config.cors_origins))      # kept in v1 order
    for router in all_routers():                  # clusters, deployments, auth, secrets, config, events,
        api.include_router(router, prefix="/api") # presets, registry, snapshots, health, workflows
    mount_static_ui(api)                          # SPA catch-all last, as v1
    return api

# seedpod/api/deps.py — route DI, the whole file. One seam.
def get_app(request: Request) -> App:              return request.app.state.app
def cluster_service(app=Depends(get_app)):         return app.services.clusters
def deployment_service(app=Depends(get_app)):      return app.services.deployments
def secret_service(app=Depends(get_app)):          return app.services.secrets
def api_key_service(app=Depends(get_app)):         return app.services.api_keys
def sse_hub(app=Depends(get_app)):                 return app.hub
# require_permission(...) factories salvaged from v1 api/dependencies.py, but they take
# api_key_service via Depends(get_app) — no late imports, no core-accessor indirection.
```

```python
# seedpod/__main__.py — the only entry point; nothing here runs at import
def main() -> None:
    setup_logging()                               # once (v1 called it twice)
    app = build_app(AppConfig.from_env())
    uvicorn.run(app.api, host=app.config.api_host, port=app.config.api_port,
                timeout_graceful_shutdown=30)
# start.py keeps only what's orthogonal to wiring: load_dotenv, PID-file singleton check,
# log rotation — then calls main(). Both salvaged verbatim.
```

### v1 global → v2 injection point (complete inventory)

| v1 global / import-time construct | v2 replacement |
|---|---|
| 8 repo singletons (`data/repositories.py` bottom) | `Repositories`, step 2; reachable only through `uow()` transactions |
| `production_session_provider` (+ H18 consumers `providers/kubernetes.py`, `utils/kubectl.py`) | **deleted**; kubeconfig is decrypted by the `kubectl-apply` step via `crypto`+repos and *passed in the command* |
| `database.engine`/`SessionLocal` module globals | `Database` instance, step 1 |
| `core/globals.py` rule engine | `RuleEngine.load` at step 4, fail-fast, injected into `DeploymentService` |
| `core/dependencies.py` stringly services dict + lazy `_production_services` + 4 accessors | typed `App`/`Services` dataclasses |
| `_provider_cache` / `get_provider_for_cluster` / `ProviderDisabledError` | `load_enabled_providers` mapping, step 5; disabled = absent |
| `_sse_manager`, `_subprocess_manager`, `_scheduler`, `get_default_job_registry()` | `hub`, `subprocesses`, engine+reconciler owned by `App.start/stop`, `StepRegistry` |
| `get_settings()` pydantic singleton | `AppConfig`, passed down; nothing imports it |
| module-level `app`/middleware/routers/static mount (`main.py`) | `create_api()`, called by the factory |
| `api/dependencies.py` late-import accessors + conftest's 5-seam override dance | one seam: `api.state.app` |
| `HTTPBearer` instance, `permission_check_performed` ContextVar | kept as module constants — stateless/request-scoped, not singleton hazards |

### Test construction — no conftest global wiring

```python
# tests/conftest.py — in full. No init_database repointing, no app.dependency_overrides,
# no patch() anywhere: the three keyword seams on build_app are the entire test surface.
@pytest.fixture
async def app(tmp_path):
    a = build_app(AppConfig(database_url=f"sqlite:///{tmp_path}/t.db",
                            secret_key_dev=Fernet.generate_key().decode(),
                            config_dir=Path("config"),
                            background_tasks=False),          # reconciler off; executor still on
                  providers={"fake": FakeProvider()},
                  clock=FrozenClock(), id_gen=sequential_ids())
    async with a.running():
        yield a

@pytest.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app.api)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c    # the ported acceptance spec (POST /api/version-update) runs against this
```

- **Pure-core tests** construct nothing: `transition(cluster, Event.PROVISIONED)` is an import.
- **Engine tests** construct `WorkflowEngine(defs, StepRegistry.for_tests(FakeProvider()), …)` directly.
- **DB-only unit tests** use `app.uow()` (or call `migrate()` on a bare `Database` themselves).
- v1 gotcha 10 (forget one of five global seams → silently hit `db/seedpod.db`) is **unrepresentable**: there is no production-DB default anywhere; every path to a database goes through the `AppConfig` you passed.

---

## Taste calls for the human

1. **Chose no `CHECK` on `clusters.status`/`deployments.status` over DDL-enforced state sets** because the Pillar-1 transition table must be the single authority (and Proposal 1's CHECK already demonstrated the failure mode by misspelling v1's hyphenated values) — flip if you want belt-and-braces DB integrity and accept a migration per state-set change.
2. **Chose timers as future-dated outbox rows (`dedupe_key` + `available_at`, cancel = inline DELETE) over a separate `timers` table** because one drain loop, one replay path, and transactional cancel are simpler and race-free — flip if timer volume or rescheduling semantics grow beyond TTL/health.
3. **Chose keeping v1's feature surface (`deployment_presets`, `snapshots`, `secret_audits`, `consecutive_health_failures`, `total_cost`) in `0001` over a lean six-table schema** because they back live endpoints and on-disk data and cost almost nothing — flip if you're willing to declare presets/snapshots out of scope for v2 parity and add them as later migrations when (if) their features port.
