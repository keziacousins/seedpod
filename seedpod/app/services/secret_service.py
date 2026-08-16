"""``SecretService`` -- CRUD + reveal over the ``secrets`` table, per ui-contract's
``GET /api/secrets?environment=``, ``POST /api/secrets``,
``GET /api/secrets/{env}/{key}/reveal``, ``DELETE /api/secrets/{env}/{key}``.

Constructor shape: docs/design/seam-d-foundation.md Decision 8 step 9's
``SecretService(crypto, repos, uow, clock=clock)``, made concrete: ``repos`` here
is the ``SecretRepository``/``SecretAuditRepository`` pair directly (neither is in
the Dispatcher-facing ``Repositories`` bundle -- ``seedpod/data/repositories.py``'s
own docstring: "the next component wires them alongside the four app-services
that need them" -- this is that component), matching how ``DeploymentService``
takes ``DeploymentAuditRepository`` directly rather than through ``Repositories``.

**Gotcha 8, closed at the crypto layer, not duplicated here**: "per-env key_class
resolved per call; unknown env RAISES, never DEV-defaults" is
``CryptoService.key_class_for_environment``'s own job (``seedpod/services/
crypto.py``) -- this service calls it and lets the ``PermanentError`` propagate
unchanged; it does not re-implement or catch-and-reword the check.

Salvaged from ``reference-code/seedpod/seedpod/api/secrets.py`` (the route
bodies) and ``reference-code/seedpod/seedpod/core/auth.py``'s ``SecretManager``
(the encrypt/decrypt-per-environment shape, now routed through the one
``CryptoService`` instead of a fresh per-environment Fernet cipher -- H10/H18
closed, see ``seedpod/services/crypto.py``'s own module docstring).
"""

from __future__ import annotations

from seedpod.core.clock import Clock
from seedpod.data.repositories import SecretAuditRepository, SecretMetadataRow, SecretRepository
from seedpod.data.uow import UnitOfWork
from seedpod.services.crypto import CryptoService

__all__ = ["SecretService", "SecretNotFound"]


class SecretNotFound(LookupError):
    pass


class SecretService:
    def __init__(
        self,
        crypto: CryptoService,
        secrets: SecretRepository,
        secret_audits: SecretAuditRepository,
        uow: UnitOfWork,
        clock: Clock,
    ) -> None:
        self._crypto = crypto
        self._secrets = secrets
        self._secret_audits = secret_audits
        self._uow = uow
        self._clock = clock

    async def list_for_environment(self, environment: str) -> list[SecretMetadataRow]:
        """Metadata only -- never decrypts (``SecretRepository.list_for_environment``'s
        own discipline; permission model parity with v1's "secrets:read = metadata
        only")."""
        async with self._uow() as tx:
            return self._secrets.list_for_environment(tx, environment)

    async def upsert(self, environment: str, key_name: str, value: str, *, actor: str) -> None:
        key_class = self._crypto.key_class_for_environment(environment)  # raises on unknown env
        async with self._uow() as tx:
            existed = self._secrets.get(tx, environment, key_name) is not None
            self._secrets.upsert(tx, environment=environment, key_name=key_name, value=value,
                                  key_class=key_class, clock=self._clock)
            self._secret_audits.create(
                tx, environment=environment, key_name=key_name,
                action="update" if existed else "create", performed_by=actor, key_class=key_class,
                clock=self._clock,
            )

    async def reveal(self, environment: str, key_name: str, *, actor: str) -> str:
        """Writes a ``reveal`` audit row -- ui-contract's own permission-sensitive
        surface (v1's "reveal-secret requests get their own audit trail",
        ``SecretAuditRepository``'s module docstring)."""
        key_class = self._crypto.key_class_for_environment(environment)
        async with self._uow() as tx:
            row = self._secrets.get(tx, environment, key_name)
            if row is None:
                raise SecretNotFound(f"{environment}/{key_name}")
            self._secret_audits.create(
                tx, environment=environment, key_name=key_name, action="reveal",
                performed_by=actor, key_class=key_class, clock=self._clock,
            )
            return row.value

    async def delete(self, environment: str, key_name: str, *, actor: str) -> bool:
        key_class = self._crypto.key_class_for_environment(environment)
        async with self._uow() as tx:
            deleted = self._secrets.delete(tx, environment, key_name)
            if deleted:
                self._secret_audits.create(
                    tx, environment=environment, key_name=key_name, action="delete",
                    performed_by=actor, key_class=key_class, clock=self._clock,
                )
            return deleted
