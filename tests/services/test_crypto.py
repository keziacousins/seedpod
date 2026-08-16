"""tests/services/test_crypto.py — round-trip, key_class stamping, and the two
deliberate v1 deviations (raise-on-unknown-env, stamp-driven decrypt never
re-derives from an environment string) for ``seedpod.services.crypto``.

No transport, no Mock/patch — ``CryptoService`` is pure in-process Fernet, so these
are plain unit tests exercising the real ``cryptography`` library.
"""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.services.crypto import CryptoService, SecretManager

_DEV_KEY = Fernet.generate_key()
_PROD_KEY = Fernet.generate_key()


# ============================================================================
# SecretManager — the Pillar-2 minimal per-field primitive
# ============================================================================


def test_secret_manager_round_trip():
    manager = SecretManager(_DEV_KEY)

    ciphertext = manager.encrypt("hunter2")

    assert ciphertext != "hunter2"
    assert manager.decrypt(ciphertext) == "hunter2"


def test_secret_manager_generates_key_when_omitted():
    manager = SecretManager()

    ciphertext = manager.encrypt("value")

    assert manager.decrypt(ciphertext) == "value"


def test_secret_manager_accepts_str_key():
    key_str = _DEV_KEY.decode("ascii")
    manager = SecretManager(key_str)

    assert manager.decrypt(manager.encrypt("value")) == "value"


def test_secret_manager_different_instances_different_keys_do_not_cross_decrypt():
    from cryptography.fernet import InvalidToken

    a = SecretManager()
    b = SecretManager()

    ciphertext = a.encrypt("secret")

    with pytest.raises(InvalidToken):
        b.decrypt(ciphertext)


# ============================================================================
# CryptoService — THE crypto site
# ============================================================================


def test_encrypt_decrypt_round_trip_dev():
    crypto = CryptoService(dev_key=_DEV_KEY)

    ciphertext = crypto.encrypt("plaintext-value", key_class="DEV")

    assert ciphertext != "plaintext-value"
    assert crypto.decrypt(ciphertext, key_class="DEV") == "plaintext-value"


def test_encrypt_decrypt_round_trip_prod():
    crypto = CryptoService(dev_key=_DEV_KEY, prod_key=_PROD_KEY)

    ciphertext = crypto.encrypt("prod-secret", key_class="PROD")

    assert crypto.decrypt(ciphertext, key_class="PROD") == "prod-secret"


def test_dev_and_prod_ciphertexts_are_not_interchangeable():
    """Decrypting a DEV ciphertext under the PROD key must not silently succeed —
    ``cryptography`` itself refuses (``InvalidToken``); ``CryptoService`` doesn't
    swallow or reclassify that failure."""
    from cryptography.fernet import InvalidToken

    crypto = CryptoService(dev_key=_DEV_KEY, prod_key=_PROD_KEY)

    dev_ciphertext = crypto.encrypt("value", key_class="DEV")

    with pytest.raises(InvalidToken):
        crypto.decrypt(dev_ciphertext, key_class="PROD")


def test_prod_key_not_configured_raises_on_encrypt():
    crypto = CryptoService(dev_key=_DEV_KEY)  # no prod_key

    with pytest.raises(PermanentError) as excinfo:
        crypto.encrypt("value", key_class="PROD")

    assert excinfo.value.code == ErrorCode.INVALID_INPUT
    assert excinfo.value.provider == "crypto"


def test_prod_key_not_configured_raises_on_decrypt():
    crypto = CryptoService(dev_key=_DEV_KEY)

    with pytest.raises(PermanentError):
        crypto.decrypt("whatever-ciphertext", key_class="PROD")


def test_unknown_key_class_raises():
    crypto = CryptoService(dev_key=_DEV_KEY, prod_key=_PROD_KEY)

    with pytest.raises(PermanentError) as excinfo:
        crypto.encrypt("value", key_class="STAGING")

    assert excinfo.value.code == ErrorCode.INVALID_INPUT
    assert excinfo.value.detail == {"key_class": "STAGING"}


# ============================================================================
# key_class_for_environment — deliberate v1 deviation: raise, never DEV-default
# ============================================================================


@pytest.mark.parametrize(
    "environment,expected",
    [
        ("local", "DEV"),
        ("development", "DEV"),
        ("ephemeral", "DEV"),
        ("staging", "DEV"),
        ("production", "PROD"),
    ],
)
def test_key_class_for_known_environments(environment, expected):
    crypto = CryptoService(dev_key=_DEV_KEY, prod_key=_PROD_KEY)

    assert crypto.key_class_for_environment(environment) == expected


def test_key_class_for_unknown_environment_raises_never_dev_defaults():
    """v1's ``get_key_class`` silently defaulted an unmapped environment to DEV
    (reference-code .../core/config.py:343-345). Deliberately NOT ported — an
    unknown environment must RAISE, never fall through to a DEV key."""
    crypto = CryptoService(dev_key=_DEV_KEY, prod_key=_PROD_KEY)

    with pytest.raises(PermanentError) as excinfo:
        crypto.key_class_for_environment("typo-envvironment")

    assert excinfo.value.code == ErrorCode.INVALID_INPUT
    assert excinfo.value.provider == "crypto"
    assert excinfo.value.detail == {"environment": "typo-envvironment"}


def test_decrypt_never_rederives_key_class_from_environment():
    """decrypt() takes an explicit key_class and has no environment parameter at
    all — the stamped key_class is authoritative even if a caller's environment
    string doesn't map cleanly. This test documents the contract shape: decrypt's
    only key-selection input is the stamp."""
    import inspect

    signature = inspect.signature(CryptoService.decrypt)
    assert "environment" not in signature.parameters
    assert "key_class" in signature.parameters
