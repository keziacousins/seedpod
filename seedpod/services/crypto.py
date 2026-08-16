"""seedpod/services/crypto.py — ``SecretManager`` (Pillar-2's minimal per-field
primitive) and ``CryptoService`` (the Pillar-4/foundation service coherence-review's
type glossary and seam-d-foundation.md's Decision 8 factory actually name).

``SecretManager`` is the minimal primitive docs/design/seam-b-engine.md §2.1 requires
("fields typed SecretStr are Fernet-encrypted via the salvaged SecretManager") for
Pillar 2's own correctness: a step's ``SecretStr`` ``Output``/``Params`` field must
round-trip through ``workflow_steps.output``/``.params`` byte-identically across a
crash/resume, and pydantic's own ``model_dump(mode="json")`` masking to the literal
``"**********"`` is NOT encryption — it is irreversible destruction of the value.
Masking still has a place (never let a secret reach an *event*/outbox payload — see
engine.py's plain vs encrypted dump split), but persisted rows that later feed a real
binding must decrypt back to the original value. Kept exactly as Pillar 2 built and
committed it (``seedpod/engine/engine.py`` constructs one with zero args by default) —
NOT rewritten here.

``CryptoService`` is the real thing: coherence-review §2's type glossary entry ("Fernet
DEV/PROD, key_class stamping; only crypto site") and seam-d-foundation.md's composition
root (``crypto = CryptoService(dev_key=config.secret_key_dev, prod_key=
config.secret_key_prod)``). Salvaged from THREE v1 sites that each independently
re-derived a Fernet cipher from ``get_key_class(self.environment)`` — the exact
H10/H18 bypass motive coherence-review's v1→v2 delta table calls out
(seam-d-foundation.md "ORM crypto (set_kubeconfig, _get_fernet ×3) → one CryptoService"):

- ``reference-code/seedpod/seedpod/core/auth.py``'s ``SecretManager.__init__``/
  ``encrypt_secret``/``decrypt_secret`` (lines 25-76) — the two-key-class construction
  shape and the plain encrypt/decrypt round trip.
- ``reference-code/seedpod/seedpod/core/database.py``'s ``Cluster._get_fernet``/
  ``set_kubeconfig``/``get_kubeconfig`` (lines 95-124).
- ``reference-code/seedpod/seedpod/core/database.py``'s ``DeploymentAudit._get_fernet``/
  ``set_resolved_manifests``/``get_resolved_manifests``/``set_resolved_secrets``/
  ``get_resolved_secrets`` (lines 336-380).

Two deliberate v2 changes, both pinned by seam-d-foundation.md's gotcha-8 note
("``get_key_class`` now sees only real envs ... raises on unknown env instead of
defaulting DEV"):

1. ``key_class_for_environment`` RAISES ``PermanentError`` on an environment outside the
   known DEV/PROD mapping. v1's ``get_key_class`` (reference-code .../core/config.py:
   343-345, ``ENVIRONMENT_KEY_MAPPING.get(environment, "DEV")``) silently defaulted any
   typo'd or unmapped environment to the DEV key — a class of bug where a secret meant
   for a real environment gets encrypted under the throwaway dev key with nothing to
   flag it. Not ported.
2. ``decrypt`` takes ``key_class`` as an explicit argument and NEVER re-derives it from
   an environment string — the caller reads the stamped ``key_class``/
   ``kubeconfig_key_class`` column and passes it straight back in, so decrypt is correct
   even if an environment's DEV/PROD mapping changes after the row was written (the
   "stamp columns make decrypt independent of the mapping entirely" sentence in
   seam-d-foundation.md's delta table).

This is still NOT full key management (no rotation, no persistence of which key
generated a given row beyond the caller-supplied ``key_class`` stamp) — that scope is
explicitly left to whatever `seedpod create-secret`/CLI-equivalent work comes later.
"""

from __future__ import annotations

from collections.abc import Mapping

from cryptography.fernet import Fernet

from seedpod.core.errors import ErrorCode, PermanentError

__all__ = ["SecretManager", "CryptoService"]


class SecretManager:
    """Fernet symmetric encryption over a single key.

    ``key``, if omitted, is generated fresh per instance — fine for tests and a
    single-process dev run (a resumed run within the SAME process/instance still
    decrypts correctly), but the composition root MUST inject a stable, persisted
    key for any real deployment: a fresh key across a real process restart would
    make every previously-encrypted row permanently undecryptable.
    """

    def __init__(self, key: bytes | str | None = None) -> None:
        if key is None:
            key = Fernet.generate_key()
        elif isinstance(key, str):
            key = key.encode("ascii")
        self._fernet = Fernet(key)

    def encrypt(self, plaintext: str) -> str:
        return self._fernet.encrypt(plaintext.encode("utf-8")).decode("ascii")

    def decrypt(self, ciphertext: str) -> str:
        return self._fernet.decrypt(ciphertext.encode("ascii")).decode("utf-8")


def _key_bytes(key: bytes | str) -> bytes:
    return key.encode("ascii") if isinstance(key, str) else key


class CryptoService:
    """Two-key (DEV/PROD) Fernet encryption with ``key_class`` stamping — THE crypto
    site (coherence-review §2 type glossary). See the module docstring for salvage
    provenance and the two deliberate v1 deviations (raise-on-unknown-env,
    stamp-driven decrypt).

    ``dev_key`` is required (mirrors v1's ``SEEDPOD_SECRET_KEY_DEV`` always being
    configured); ``prod_key`` is optional — a deployment that never touches production
    doesn't need one, but ``encrypt``/``decrypt`` with ``key_class="PROD"`` raise if
    it's missing (v1's ``ValueError("SEEDPOD_SECRET_KEY_PROD is required for
    production environment")``, reference-code .../core/auth.py:34-36, now a
    ``PermanentError`` instead of a bare ``ValueError`` — Conflict 6's "one taxonomy
    home" applies to services too).
    """

    # v1's ENVIRONMENT_KEY_MAPPING (reference-code/seedpod/seedpod/core/config.py:
    # 333-339), salvaged verbatim EXCEPT for the fallback: an environment outside this
    # mapping now raises instead of silently resolving to "DEV" (gotcha 8, see module
    # docstring).
    _ENVIRONMENT_KEY_MAPPING: Mapping[str, str] = {
        "local": "DEV",
        "development": "DEV",
        "ephemeral": "DEV",
        "staging": "DEV",
        "production": "PROD",
    }

    def __init__(self, dev_key: bytes | str, prod_key: bytes | str | None = None) -> None:
        self._fernets: dict[str, Fernet] = {"DEV": Fernet(_key_bytes(dev_key))}
        if prod_key is not None:
            self._fernets["PROD"] = Fernet(_key_bytes(prod_key))

    def key_class_for_environment(self, environment: str) -> str:
        """DEV or PROD for a real environment name; raises on anything else — never a
        silent DEV default (gotcha 8)."""
        try:
            return self._ENVIRONMENT_KEY_MAPPING[environment]
        except KeyError:
            raise PermanentError(
                f"crypto.key_class_for_environment: unknown environment {environment!r}",
                code=ErrorCode.INVALID_INPUT,
                provider="crypto",
                command="key_class_for_environment",
                detail={"environment": environment},
            ) from None

    def encrypt(self, plaintext: str, key_class: str) -> str:
        """Encrypt under the key for ``key_class`` ('DEV' or 'PROD'). The caller is
        responsible for persisting ``key_class`` alongside the ciphertext (the "stamp"
        columns — ``kubeconfig_key_class``, ``key_class`` — seam-d-foundation.md's
        DDL); ``decrypt`` reads it back rather than re-deriving it."""
        return self._fernet_for(key_class).encrypt(plaintext.encode("utf-8")).decode("ascii")

    def decrypt(self, ciphertext: str, key_class: str) -> str:
        """Decrypt using the STAMPED ``key_class`` — never re-derived from an
        environment string (module docstring, deviation 2)."""
        return self._fernet_for(key_class).decrypt(ciphertext.encode("ascii")).decode("utf-8")

    def _fernet_for(self, key_class: str) -> Fernet:
        try:
            return self._fernets[key_class]
        except KeyError:
            if key_class == "PROD":
                raise PermanentError(
                    "crypto: PROD key not configured (SEEDPOD_SECRET_KEY_PROD is required "
                    "to encrypt/decrypt a PROD-class secret)",
                    code=ErrorCode.INVALID_INPUT,
                    provider="crypto",
                    command="fernet_for",
                    detail={"key_class": key_class},
                ) from None
            raise PermanentError(
                f"crypto: unknown key_class {key_class!r} (must be 'DEV' or 'PROD')",
                code=ErrorCode.INVALID_INPUT,
                provider="crypto",
                command="fernet_for",
                detail={"key_class": key_class},
            ) from None
