"""Round-4 "repos-feature": ApiKeyRepository, SecretRepository, SecretAuditRepository,
PresetRepository, SnapshotRepository -- against real tmp SQLite (0001_initial.sql).
No mocks.

Covers: api-key hash lookup + active/expiry filtering (get_valid_by_hash) +
last_used_at touch; secret upsert idempotency (Seam D gotcha-4 closure -- two
upserts land as one row, value updated) + crypto round-trip with the key_class
stamp (decrypt never re-derives it from environment); secret_audits' action CHECK
+ a reveal audit row; preset CRUD + use_count/last_used_at touch; snapshot CRUD +
branch/profile list filters.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from seedpod.core.clock import FrozenClock
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import (
    ApiKeyRepository,
    ApiKeyRow,
    PresetRepository,
    PresetRow,
    SecretAuditRepository,
    SecretMetadataRow,
    SecretRepository,
    SnapshotRepository,
    SnapshotRow,
)
from seedpod.data.uow import UnitOfWork
from seedpod.services.crypto import CryptoService

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)
LATER = NOW + timedelta(hours=1)
EARLIER = NOW - timedelta(hours=1)

api_keys = ApiKeyRepository()
secret_audits = SecretAuditRepository()
presets = PresetRepository()
snapshots = SnapshotRepository()


def _insert_cluster(session, cluster_id: str, slug: str | None = None, *, now: datetime = NOW) -> None:
    """Minimal raw INSERT satisfying `clusters`' NOT NULL columns -- snapshots.source_cluster_id
    is a real FK (foreign_keys=ON), so tests need a valid parent row.

    Serializes `now` the same 'Z'/fixed-millisecond way every repository write does
    (`_iso` in seedpod/data/repositories.py) rather than raw `datetime.isoformat()`
    ('+00:00' offset form), so this fixture doesn't seed the test DB with
    convention-violating timestamps even though nothing in this file reads these
    columns back.
    """
    session.execute(
        text(
            """
            INSERT INTO clusters (id, name, slug, environment, status, provider, created_at, updated_at)
            VALUES (:id, :name, :slug, 'ephemeral', 'active', 'fake', :now, :now)
            """
        ),
        {
            "id": cluster_id,
            "name": cluster_id,
            "slug": slug or cluster_id,
            "now": now.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        },
    )


@pytest.fixture
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(database.engine)
    return database


@pytest.fixture
def uow(db):
    return UnitOfWork(db)


@pytest.fixture
def crypto():
    return CryptoService(dev_key=Fernet.generate_key(), prod_key=Fernet.generate_key())


def secret_repo(crypto) -> SecretRepository:
    return SecretRepository(crypto)


def make_api_key_row(key_hash: str, **overrides) -> ApiKeyRow:
    fields = {
        "id": None,
        "key_hash": key_hash,
        "username": "alice",
        "environment": "all",
        "permissions": ["*"],
        "is_active": True,
        "description": "test key",
        "created_by": "api:admin",
        "created_at": NOW,
        "expires_at": None,
        "last_used_at": None,
    }
    fields.update(overrides)
    return ApiKeyRow(**fields)


def make_preset_row(preset_id: str, **overrides) -> PresetRow:
    fields = {
        "id": preset_id,
        "name": f"preset-{preset_id}",
        "description": "a preset",
        "profile_name": "exampleco-stack",
        "environment": "ephemeral",
        "service_overrides": {"web": {"image": "custom:tag"}},
        "default_branch": "main",
        "default_ttl_hours": 24,
        "default_provider": None,  # DR-0046 -- the "preset does not care" case
        "naming_strategy": {"strategy": "branch-slug"},
        "created_by": "api:admin",
        "created_at": NOW,
        "last_used_at": None,
        "use_count": 0,
    }
    fields.update(overrides)
    return PresetRow(**fields)


def make_snapshot_row(snapshot_id: str, cluster_id: str, **overrides) -> SnapshotRow:
    fields = {
        "id": snapshot_id,
        "name": f"snap-{snapshot_id}",
        "description": None,
        "source_cluster_id": cluster_id,
        "source_cluster_slug": cluster_id,
        "branch": "main",
        "deployment_profile": "exampleco-stack",
        "services": [{"service_name": "db", "persistence_type": "postgres", "file": "db.sql"}],
        "storage_path": f"/var/snapshots/{snapshot_id}",
        "total_size_bytes": 1024,
        "is_auto": False,
        "created_by": "api:admin",
        "created_at": NOW,
    }
    fields.update(overrides)
    return SnapshotRow(**fields)


# ---------------------------------------------------------------------------
# ApiKeyRepository
# ---------------------------------------------------------------------------


async def test_insert_and_get_by_hash_round_trips(uow):
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("hash-1"))

    async with uow() as tx:
        fetched = api_keys.get_by_hash(tx, "hash-1")
    assert fetched is not None
    assert fetched.id is not None  # AUTOINCREMENT filled it in
    assert fetched.username == "alice"
    assert fetched.permissions == ["*"]


async def test_get_by_hash_missing_returns_none(uow):
    async with uow() as tx:
        assert api_keys.get_by_hash(tx, "does-not-exist") is None


async def test_get_valid_by_hash_active_and_unexpired(uow):
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("hash-1", expires_at=LATER))

    async with uow() as tx:
        fetched = api_keys.get_valid_by_hash(tx, "hash-1", now=NOW)
    assert fetched is not None
    assert fetched.key_hash == "hash-1"


async def test_get_valid_by_hash_none_when_expired(uow):
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("hash-1", expires_at=EARLIER))

    async with uow() as tx:
        assert api_keys.get_valid_by_hash(tx, "hash-1", now=NOW) is None
    # a plain get_by_hash still finds it -- filtering is get_valid_by_hash's job alone
    async with uow() as tx:
        assert api_keys.get_by_hash(tx, "hash-1") is not None


async def test_get_valid_by_hash_none_when_inactive(uow):
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("hash-1", is_active=False))

    async with uow() as tx:
        assert api_keys.get_valid_by_hash(tx, "hash-1", now=NOW) is None


async def test_get_valid_by_hash_no_expiry_is_valid_forever(uow):
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("hash-1", expires_at=None))

    async with uow() as tx:
        assert api_keys.get_valid_by_hash(tx, "hash-1", now=LATER + timedelta(days=3650)) is not None


async def test_get_valid_by_hash_expiry_comparison_survives_whole_second_boundary(uow):
    """`expires_at > :now` is a lexicographic TEXT comparison (0001_initial.sql) --
    it only orders correctly if every ISO-8601 timestamp `_iso` writes has FIXED
    width. A whole-second `expires_at` (no fractional part under variable-width
    formatting) compared against a fractional `now` used to sort as "later" than it
    really was (`'...00Z' > '...00.500000Z'` because `'Z' > '.'` lexicographically),
    granting up to ~1 extra second of validity past real expiry. `expires_at` here
    lands exactly on a whole second; `now` is 1ms past it -- must read as expired."""
    on_the_second = NOW.replace(microsecond=0)
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("hash-1", expires_at=on_the_second))

    just_after = on_the_second + timedelta(milliseconds=1)
    async with uow() as tx:
        assert api_keys.get_valid_by_hash(tx, "hash-1", now=just_after) is None


async def test_list_filters_by_username_environment_active_only(uow):
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("h1", username="alice", environment="staging"))
        api_keys.insert(tx, make_api_key_row("h2", username="alice", environment="all", is_active=False))
        api_keys.insert(tx, make_api_key_row("h3", username="bob", environment="staging"))

    async with uow() as tx:
        alice_keys = {r.key_hash for r in api_keys.list(tx, username="alice")}
    assert alice_keys == {"h1", "h2"}

    async with uow() as tx:
        staging_keys = {r.key_hash for r in api_keys.list(tx, environment="staging")}
    assert staging_keys == {"h1", "h3"}

    async with uow() as tx:
        active_alice = {r.key_hash for r in api_keys.list(tx, username="alice", active_only=True)}
    assert active_alice == {"h1"}


async def test_api_key_list_orders_created_at_desc(uow):
    """Salvaged ordering law (reference-code repositories.py:483
    `query.order_by(APIKey.created_at.desc())`) -- asserted by sequence, not just
    membership, so a regression to unordered results would fail this."""
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("older", created_at=EARLIER))
        api_keys.insert(tx, make_api_key_row("newer", created_at=NOW))

    async with uow() as tx:
        ordered = [r.key_hash for r in api_keys.list(tx)]
    assert ordered == ["newer", "older"]


async def test_touch_last_used(uow):
    clock = FrozenClock(NOW)
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("h1"))
        fetched = api_keys.get_by_hash(tx, "h1")

    async with uow() as tx:
        touched = api_keys.touch_last_used(tx, fetched.id, clock=clock)
    assert touched is True

    async with uow() as tx:
        fetched = api_keys.get_by_hash(tx, "h1")
    assert fetched.last_used_at == NOW


async def test_touch_last_used_missing_key_returns_false(uow):
    async with uow() as tx:
        assert api_keys.touch_last_used(tx, 999999, clock=FrozenClock(NOW)) is False


async def test_revoke_flips_is_active_and_reports_rowcount(uow):
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("h1"))
        fetched = api_keys.get_by_hash(tx, "h1")

    async with uow() as tx:
        revoked = api_keys.revoke(tx, fetched.id)
    assert revoked is True

    async with uow() as tx:
        fetched = api_keys.get_by_hash(tx, "h1")
    assert fetched.is_active is False

    async with uow() as tx:
        assert api_keys.revoke(tx, 999999) is False


async def test_key_hash_uniqueness_enforced(uow):
    async with uow() as tx:
        api_keys.insert(tx, make_api_key_row("dup"))

    with pytest.raises(IntegrityError):
        async with uow() as tx:
            api_keys.insert(tx, make_api_key_row("dup", username="mallory"))


# ---------------------------------------------------------------------------
# SecretRepository -- upsert idempotency + crypto round-trip
# ---------------------------------------------------------------------------


async def test_secret_upsert_idempotency_one_row_value_updated(uow, crypto):
    repo = secret_repo(crypto)
    clock = FrozenClock(NOW)
    async with uow() as tx:
        repo.upsert(tx, environment="staging", key_name="DATABASE_URL", value="postgres://v1",
                    key_class="DEV", clock=clock)

    clock.set(LATER)
    async with uow() as tx:
        repo.upsert(tx, environment="staging", key_name="DATABASE_URL", value="postgres://v2",
                    key_class="DEV", clock=clock)

    async with uow() as tx:
        rows = repo.list_for_environment(tx, "staging")
    assert len(rows) == 1  # one upsert, one row -- not two
    assert isinstance(rows[0], SecretMetadataRow)  # metadata only, ciphertext untouched
    assert rows[0].updated_at == LATER
    # v1's update path (reference-code repositories.py:564-569) never touched
    # created_at, and the ON CONFLICT clause correctly omits it from DO UPDATE SET --
    # lock that edge against a future careless `DO UPDATE SET *` rewrite.
    assert rows[0].created_at == NOW

    async with uow() as tx:
        fetched = repo.get(tx, "staging", "DATABASE_URL")
    assert fetched.value == "postgres://v2"
    assert fetched.updated_at == LATER
    assert fetched.created_at == NOW

    async with uow() as tx:
        count = tx.execute(text("SELECT COUNT(*) FROM secrets")).scalar()
    assert count == 1


async def test_secret_list_for_environment_never_decrypts(uow, crypto):
    """`list_for_environment` is the salvage of v1's `get_secrets_for_environment`
    ("metadata only, no decrypted values", reference-code
    seedpod/api/secrets.py:72) -- it must not touch `CryptoService.decrypt` at all.
    Proven by listing an environment holding a PROD-stamped row through a
    `SecretRepository` built with a `CryptoService` that has NO prod_key configured:
    a decrypting list would raise PermanentError here (crypto.py's `_fernet_for`),
    exactly where v1 served the metadata fine."""
    dev_key = Fernet.generate_key()
    writer = SecretRepository(CryptoService(dev_key=dev_key, prod_key=Fernet.generate_key()))
    clock = FrozenClock(NOW)
    async with uow() as tx:
        writer.upsert(tx, environment="production", key_name="API_TOKEN", value="super-secret",
                      key_class="PROD", clock=clock)

    no_prod_key_reader = SecretRepository(CryptoService(dev_key=dev_key))  # no prod_key at all
    async with uow() as tx:
        rows = no_prod_key_reader.list_for_environment(tx, "production")  # must not raise

    assert len(rows) == 1
    assert isinstance(rows[0], SecretMetadataRow)
    assert not hasattr(rows[0], "value")  # ciphertext untouched, never decrypted
    assert rows[0].key_class == "PROD"
    assert rows[0].key_name == "API_TOKEN"


async def test_secret_crypto_roundtrip_with_key_class_stamp(uow, crypto):
    repo = secret_repo(crypto)
    clock = FrozenClock(NOW)
    async with uow() as tx:
        repo.upsert(tx, environment="production", key_name="API_TOKEN", value="super-secret",
                    key_class="PROD", clock=clock)

    async with uow() as tx:
        fetched = repo.get(tx, "production", "API_TOKEN")
    assert fetched is not None
    assert fetched.value == "super-secret"  # decrypted back to plaintext
    assert fetched.key_class == "PROD"  # the stamp, read back verbatim

    # the ciphertext at rest is NOT the plaintext
    async with uow() as tx:
        raw = tx.execute(
            text("SELECT encrypted_value FROM secrets WHERE environment='production' AND key_name='API_TOKEN'")
        ).scalar()
    assert raw != "super-secret"


async def test_secret_decrypt_never_rederives_key_class_from_environment(uow, crypto):
    """A secret stamped DEV in an environment that would normally map to PROD
    still decrypts correctly -- decrypt reads the STAMP, not the environment."""
    repo = secret_repo(crypto)
    clock = FrozenClock(NOW)
    async with uow() as tx:
        # 'production' would map to PROD via key_class_for_environment, but this
        # row is explicitly stamped DEV -- decrypt must honor the stamp.
        repo.upsert(tx, environment="production", key_name="LEGACY", value="old-value",
                    key_class="DEV", clock=clock)

    async with uow() as tx:
        fetched = repo.get(tx, "production", "LEGACY")
    assert fetched.value == "old-value"
    assert fetched.key_class == "DEV"


async def test_secret_delete(uow, crypto):
    repo = secret_repo(crypto)
    clock = FrozenClock(NOW)
    async with uow() as tx:
        repo.upsert(tx, environment="staging", key_name="K", value="v", key_class="DEV", clock=clock)

    async with uow() as tx:
        deleted = repo.delete(tx, "staging", "K")
    assert deleted is True

    async with uow() as tx:
        assert repo.get(tx, "staging", "K") is None
        assert repo.delete(tx, "staging", "K") is False


# ---------------------------------------------------------------------------
# SecretAuditRepository -- action CHECK + reveal audit row
# ---------------------------------------------------------------------------


async def test_secret_audit_create_and_trail_filters(uow):
    clock = FrozenClock(NOW)
    async with uow() as tx:
        secret_audits.create(tx, environment="staging", key_name="K", action="create",
                             performed_by="api:admin", key_class="DEV", clock=clock)
        clock.set(LATER)
        secret_audits.create(tx, environment="staging", key_name="K", action="update",
                             performed_by="api:admin", key_class="DEV", clock=clock)

    async with uow() as tx:
        trail = secret_audits.get_audit_trail(tx, environment="staging", key_name="K")
    assert [a.action for a in trail] == ["update", "create"]  # newest first


async def test_secret_audit_reveal_row_and_get_last_reveal(uow):
    clock = FrozenClock(NOW)
    async with uow() as tx:
        secret_audits.create(tx, environment="staging", key_name="K", action="create",
                             performed_by="api:admin", key_class="DEV", clock=clock)
        clock.set(LATER)
        secret_audits.create(tx, environment="staging", key_name="K", action="reveal",
                             performed_by="api:bob", key_class="DEV",
                             context={"reason": "debugging"}, clock=clock)

    async with uow() as tx:
        last_reveal = secret_audits.get_last_reveal(tx, "staging", "K")
    assert last_reveal is not None
    assert last_reveal.action == "reveal"
    assert last_reveal.performed_by == "api:bob"
    assert last_reveal.context == {"reason": "debugging"}
    assert last_reveal.created_at == LATER


async def test_secret_audit_action_check_constraint_enforced(uow):
    clock = FrozenClock(NOW)
    with pytest.raises(IntegrityError):
        async with uow() as tx:
            secret_audits.create(tx, environment="staging", key_name="K", action="not-a-real-action",
                                 performed_by="api:admin", key_class="DEV", clock=clock)


# ---------------------------------------------------------------------------
# PresetRepository
# ---------------------------------------------------------------------------


async def test_insert_and_get_preset_round_trips(uow):
    async with uow() as tx:
        presets.insert(tx, make_preset_row("p1"))

    async with uow() as tx:
        fetched = presets.get(tx, "p1")
    assert fetched == make_preset_row("p1")


async def test_get_by_name(uow):
    async with uow() as tx:
        presets.insert(tx, make_preset_row("p1", name="my-preset"))

    async with uow() as tx:
        fetched = presets.get_by_name(tx, "my-preset")
    assert fetched is not None
    assert fetched.id == "p1"


async def test_preset_name_uniqueness_enforced(uow):
    async with uow() as tx:
        presets.insert(tx, make_preset_row("p1", name="dup"))

    with pytest.raises(IntegrityError):
        async with uow() as tx:
            presets.insert(tx, make_preset_row("p2", name="dup"))


async def test_list_filters_by_profile(uow):
    async with uow() as tx:
        presets.insert(tx, make_preset_row("p1", profile_name="exampleco-stack"))
        presets.insert(tx, make_preset_row("p2", profile_name="local-dev"))

    async with uow() as tx:
        exampleco = {p.id for p in presets.list(tx, profile="exampleco-stack")}
    assert exampleco == {"p1"}

    async with uow() as tx:
        all_presets = {p.id for p in presets.list(tx)}
    assert all_presets == {"p1", "p2"}


async def test_list_orders_last_used_nulls_last_then_created_at(uow):
    async with uow() as tx:
        presets.insert(tx, make_preset_row("never-used", created_at=NOW, last_used_at=None))
        presets.insert(tx, make_preset_row("used-earlier", created_at=EARLIER, last_used_at=EARLIER))
        presets.insert(tx, make_preset_row("used-later", created_at=EARLIER, last_used_at=LATER))

    async with uow() as tx:
        ordered = [p.id for p in presets.list(tx)]
    assert ordered == ["used-later", "used-earlier", "never-used"]


async def test_update_partial_fields_only(uow):
    async with uow() as tx:
        presets.insert(tx, make_preset_row("p1"))

    async with uow() as tx:
        updated = presets.update(tx, "p1", description="updated description", default_ttl_hours=48)
    assert updated is True

    async with uow() as tx:
        fetched = presets.get(tx, "p1")
    assert fetched.description == "updated description"
    assert fetched.default_ttl_hours == 48
    assert fetched.name == "preset-p1"  # untouched
    assert fetched.service_overrides == {"web": {"image": "custom:tag"}}  # untouched


async def test_update_with_no_fields_is_a_noop(uow):
    async with uow() as tx:
        presets.insert(tx, make_preset_row("p1"))
        updated = presets.update(tx, "p1")
        fetched = presets.get(tx, "p1")
    assert updated is False
    assert fetched == make_preset_row("p1")


async def test_update_missing_preset_returns_false(uow):
    """v1's `update_preset` (reference-code repositories.py:912-940) returned
    `None` for a missing preset vs. the updated DTO -- distinguishable from
    "updated". Here that's a rowcount bool, matching `delete`/`record_usage`."""
    async with uow() as tx:
        updated = presets.update(tx, "does-not-exist", description="x")
    assert updated is False


async def test_record_usage_touches_use_count_and_last_used_at(uow):
    clock = FrozenClock(NOW)
    async with uow() as tx:
        presets.insert(tx, make_preset_row("p1", use_count=0, last_used_at=None))

    async with uow() as tx:
        touched = presets.record_usage(tx, "p1", clock=clock)
    assert touched is True

    async with uow() as tx:
        fetched = presets.get(tx, "p1")
    assert fetched.use_count == 1
    assert fetched.last_used_at == NOW

    clock.set(LATER)
    async with uow() as tx:
        presets.record_usage(tx, "p1", clock=clock)

    async with uow() as tx:
        fetched = presets.get(tx, "p1")
    assert fetched.use_count == 2
    assert fetched.last_used_at == LATER


async def test_record_usage_missing_preset_returns_false(uow):
    async with uow() as tx:
        assert presets.record_usage(tx, "does-not-exist", clock=FrozenClock(NOW)) is False


async def test_delete_preset(uow):
    async with uow() as tx:
        presets.insert(tx, make_preset_row("p1"))

    async with uow() as tx:
        assert presets.delete(tx, "p1") is True

    async with uow() as tx:
        assert presets.get(tx, "p1") is None
        assert presets.delete(tx, "p1") is False


# ---------------------------------------------------------------------------
# SnapshotRepository
# ---------------------------------------------------------------------------


async def test_insert_and_get_snapshot_round_trips(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        snapshots.insert(tx, make_snapshot_row("s1", "c1"))

    async with uow() as tx:
        fetched = snapshots.get(tx, "s1")
    assert fetched == make_snapshot_row("s1", "c1")


async def test_get_missing_snapshot_returns_none(uow):
    async with uow() as tx:
        assert snapshots.get(tx, "does-not-exist") is None


async def test_list_filters_by_branch_and_profile(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        _insert_cluster(tx, "c2")
        snapshots.insert(tx, make_snapshot_row("s1", "c1", branch="main", deployment_profile="exampleco-stack"))
        snapshots.insert(tx, make_snapshot_row("s2", "c2", branch="feature/x", deployment_profile="exampleco-stack"))
        snapshots.insert(tx, make_snapshot_row("s3", "c1", branch="main", deployment_profile="local-dev"))

    async with uow() as tx:
        main_branch = {s.id for s in snapshots.list(tx, branch="main")}
    assert main_branch == {"s1", "s3"}

    async with uow() as tx:
        exampleco = {s.id for s in snapshots.list(tx, profile="exampleco-stack")}
    assert exampleco == {"s1", "s2"}

    async with uow() as tx:
        both = {s.id for s in snapshots.list(tx, branch="main", profile="exampleco-stack")}
    assert both == {"s1"}


async def test_list_orders_created_at_desc(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        snapshots.insert(tx, make_snapshot_row("older", "c1", created_at=EARLIER))
        snapshots.insert(tx, make_snapshot_row("newer", "c1", created_at=NOW))

    async with uow() as tx:
        ordered = [s.id for s in snapshots.list(tx)]
    assert ordered == ["newer", "older"]


async def test_delete_snapshot(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        snapshots.insert(tx, make_snapshot_row("s1", "c1"))

    async with uow() as tx:
        assert snapshots.delete(tx, "s1") is True

    async with uow() as tx:
        assert snapshots.get(tx, "s1") is None
        assert snapshots.delete(tx, "s1") is False


async def test_snapshot_source_cluster_fk_enforced(uow):
    with pytest.raises(IntegrityError):
        async with uow() as tx:
            snapshots.insert(tx, make_snapshot_row("s1", "no-such-cluster"))
