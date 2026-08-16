"""``seedpod/app/services/api_key_service.py`` -- real sqlite, ``FrozenClock``, no
Mock/patch. Pins the exact conftest contract:
``create_api_key(username=, environment=, permissions=[]) -> (record, plaintext)``.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from seedpod.app.services.api_key_service import ApiKeyService


async def test_create_api_key_round_trips_and_plaintext_authenticates(api_keys_repo, uow, clock):
    service = ApiKeyService(api_keys_repo, uow, clock)
    record, plaintext = await service.create_api_key(username="test-user", environment="all", permissions=["*"])

    assert record.username == "test-user"
    assert record.environment == "all"
    assert record.permissions == ["*"]
    assert record.is_active is True
    assert plaintext.startswith("seedpod_all_")

    validated = await service.validate(plaintext)
    assert validated is not None
    assert validated.id == record.id
    assert validated.username == "test-user"


async def test_validate_rejects_unknown_key(api_keys_repo, uow, clock):
    service = ApiKeyService(api_keys_repo, uow, clock)
    assert await service.validate("seedpod_all_not-a-real-key") is None


async def _last_used_in_db(api_keys_repo, uow, key_id):
    """Read `last_used_at` straight from the row, bypassing `ApiKeyService.get`.

    Load-bearing (DR-0044): `get()` OVERLAYS unflushed touches, so asserting through
    it cannot distinguish "the write happened" from "the buffer answered". Going to
    the repository is the only way to pin the write itself -- and when this suite's
    original `test_validate_touches_last_used_at` was left asserting through `get()`,
    it kept passing after `validate()` stopped writing, which is exactly the silent
    meaning-change this helper exists to prevent."""
    async with uow() as tx:
        return api_keys_repo.get_by_id(tx, key_id).last_used_at


async def test_validate_does_not_write_and_flush_records_the_use_time(api_keys_repo, uow, clock):
    """DR-0044: authentication is a read. The touch lands only on flush, stamped with
    the moment the key was USED rather than the moment the batch was written."""
    service = ApiKeyService(api_keys_repo, uow, clock)
    record, plaintext = await service.create_api_key(username="u", environment="all", permissions=[])
    assert record.last_used_at is None

    used_at = clock.now()
    await service.validate(plaintext)
    assert await _last_used_in_db(api_keys_repo, uow, record.id) is None  # nothing written yet

    clock.advance(timedelta(minutes=5))  # the flush happens LATER than the use
    assert await service.flush_last_used() == 1

    assert await _last_used_in_db(api_keys_repo, uow, record.id) == used_at
    assert used_at != clock.now()  # ...and is not just the flush time by coincidence


async def test_repeated_validations_flush_as_one_write(api_keys_repo, uow, clock):
    """The point of buffering: N authenticated requests in a window cost ONE
    transaction, not N. `flush_last_used` returns the number of ROWS touched, so a
    single key validated many times must report exactly 1."""
    service = ApiKeyService(api_keys_repo, uow, clock)
    record, plaintext = await service.create_api_key(username="u", environment="all", permissions=[])

    for _ in range(5):
        await service.validate(plaintext)

    assert await service.flush_last_used() == 1
    assert await service.flush_last_used() == 0  # buffer drained, no spurious rewrite
    assert await _last_used_in_db(api_keys_repo, uow, record.id) == clock.now()


async def test_revocation_is_immediate_despite_a_pending_touch(api_keys_repo, uow, clock):
    """The security property, tested at its edge. Deferring the WRITE must never
    defer the CHECK -- the lookup still runs per request, so a revoked key is
    rejected on the very next call even with its touch still unflushed."""
    service = ApiKeyService(api_keys_repo, uow, clock)
    record, plaintext = await service.create_api_key(username="u", environment="all", permissions=[])

    assert await service.validate(plaintext) is not None  # buffers a touch
    await service.revoke(record.id)

    assert await service.validate(plaintext) is None


async def test_reads_overlay_an_unflushed_touch(api_keys_repo, uow, clock):
    """DR-0044 decision 5: a key used seconds ago must not read "Never" on its own
    detail page just because the interval has not elapsed."""
    service = ApiKeyService(api_keys_repo, uow, clock)
    record, plaintext = await service.create_api_key(username="u", environment="all", permissions=[])
    await service.validate(plaintext)

    assert await _last_used_in_db(api_keys_repo, uow, record.id) is None  # genuinely unwritten
    assert (await service.get(record.id)).last_used_at == clock.now()
    assert [k.last_used_at for k in await service.list()] == [clock.now()]


async def test_a_failed_flush_keeps_the_touches_for_the_next_one(api_keys_repo, uow, clock):
    """A lost breadcrumb is cheap; losing it silently and forever is not. The buffer
    is swapped out before the write, so a failure has to fold the batch back in."""
    service = ApiKeyService(api_keys_repo, uow, clock)
    record, plaintext = await service.create_api_key(username="u", environment="all", permissions=[])
    await service.validate(plaintext)

    class _Boom(Exception):
        pass

    def _explode(*args, **kwargs):
        raise _Boom

    working, api_keys_repo.touch_last_used = api_keys_repo.touch_last_used, _explode
    try:
        with pytest.raises(_Boom):
            await service.flush_last_used()
    finally:
        api_keys_repo.touch_last_used = working

    assert await service.flush_last_used() == 1  # the touch survived the failure
    assert await _last_used_in_db(api_keys_repo, uow, record.id) == clock.now()


async def test_validate_rejects_expired_key(api_keys_repo, uow, clock):
    service = ApiKeyService(api_keys_repo, uow, clock)
    _, plaintext = await service.create_api_key(username="u", environment="all", permissions=[], expires_hours=1)
    clock.advance(timedelta(hours=2))
    assert await service.validate(plaintext) is None


async def test_revoke_deactivates_key(api_keys_repo, uow, clock):
    service = ApiKeyService(api_keys_repo, uow, clock)
    record, plaintext = await service.create_api_key(username="u", environment="all", permissions=[])
    assert await service.revoke(record.id) is True
    assert await service.validate(plaintext) is None


async def test_list_filters_by_username(api_keys_repo, uow, clock):
    service = ApiKeyService(api_keys_repo, uow, clock)
    await service.create_api_key(username="alice", environment="all", permissions=[])
    await service.create_api_key(username="bob", environment="all", permissions=[])

    alice_keys = await service.list(username="alice")
    assert len(alice_keys) == 1
    assert alice_keys[0].username == "alice"

    permissions_are_lists = all(isinstance(k.permissions, list) for k in await service.list())
    assert permissions_are_lists
