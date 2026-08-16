"""``seedpod/app/services/secret_service.py`` -- real sqlite, real ``CryptoService``,
``FrozenClock``, no Mock/patch. Covers reveal-writes-an-audit-row and the
unknown-environment RAISE (gotcha 8: never a silent DEV default).
"""

from __future__ import annotations

import pytest

from seedpod.app.services.secret_service import SecretNotFound, SecretService
from seedpod.core.errors import PermanentError


@pytest.fixture
def secret_service(crypto, secrets_repo, secret_audits_repo, uow, clock):
    return SecretService(crypto, secrets_repo, secret_audits_repo, uow, clock)


async def test_upsert_then_reveal_round_trips_plaintext(secret_service):
    await secret_service.upsert("ephemeral", "DATABASE_URL", "postgresql://x", actor="api:test")
    value = await secret_service.reveal("ephemeral", "DATABASE_URL", actor="api:test")
    assert value == "postgresql://x"


async def test_reveal_writes_an_audit_row(secret_service, secret_audits_repo, uow):
    await secret_service.upsert("ephemeral", "DATABASE_URL", "postgresql://x", actor="api:creator")
    await secret_service.reveal("ephemeral", "DATABASE_URL", actor="api:revealer")

    async with uow() as tx:
        trail = secret_audits_repo.get_audit_trail(tx, environment="ephemeral", key_name="DATABASE_URL")

    actions = [row.action for row in trail]
    assert "create" in actions
    assert "reveal" in actions
    reveal_row = next(row for row in trail if row.action == "reveal")
    assert reveal_row.performed_by == "api:revealer"
    assert reveal_row.key_class == "DEV"


async def test_reveal_missing_secret_raises_not_found(secret_service):
    with pytest.raises(SecretNotFound):
        await secret_service.reveal("ephemeral", "NOPE", actor="api:test")


async def test_unknown_environment_raises_on_upsert(secret_service):
    with pytest.raises(PermanentError):
        await secret_service.upsert("not-a-real-env", "KEY", "value", actor="api:test")


async def test_unknown_environment_raises_on_reveal(secret_service):
    with pytest.raises(PermanentError):
        await secret_service.reveal("not-a-real-env", "KEY", actor="api:test")


async def test_second_upsert_is_recorded_as_update(secret_service, secret_audits_repo, uow):
    await secret_service.upsert("ephemeral", "KEY", "v1", actor="api:test")
    await secret_service.upsert("ephemeral", "KEY", "v2", actor="api:test")

    async with uow() as tx:
        trail = secret_audits_repo.get_audit_trail(tx, environment="ephemeral", key_name="KEY")
    actions = sorted(row.action for row in trail)
    assert actions == ["create", "update"]

    value = await secret_service.reveal("ephemeral", "KEY", actor="api:test")
    assert value == "v2"


async def test_production_environment_uses_prod_key_class(secret_service, secrets_repo, uow):
    await secret_service.upsert("production", "KEY", "prod-value", actor="api:test")
    async with uow() as tx:
        row = secrets_repo.get(tx, "production", "KEY")
    assert row.key_class == "PROD"
    assert row.value == "prod-value"


async def test_delete_removes_secret_and_audits(secret_service, secrets_repo, uow):
    await secret_service.upsert("ephemeral", "KEY", "v1", actor="api:test")
    deleted = await secret_service.delete("ephemeral", "KEY", actor="api:test")
    assert deleted is True

    async with uow() as tx:
        assert secrets_repo.get(tx, "ephemeral", "KEY") is None

    assert await secret_service.delete("ephemeral", "KEY", actor="api:test") is False
