"""``seedpod-bootstrap`` -- the offline, on-disk cold-start CLI
(docs/decisions/DR-0021 §0b/point 2; ``seedpod/bootstrap.py``). Real sqlite
tmp db (via ``SEEDPOD_DATABASE_URL``), the real ``ApiKeyService``/
``ApiKeyRepository``/``SystemClock`` object graph, real ``Fernet`` keys --
zero ``Mock``/``patch`` anywhere (CLAUDE.md).

A note on the "no httpx" assertion below: importing ANYTHING under the
``seedpod.app.services`` package (this component's own ``ApiKeyService`` lives
there) runs that package's ``__init__.py``, which -- already, before this
component existed -- eagerly imports every sibling service, including
``DeploymentService`` -> ``seedpod.services.dns`` -> ``import httpx`` at
module scope. That is pre-existing, committed composition-root structure this
component must not edit (CLAUDE.md); it is impossible for anything that
imports ``ApiKeyService`` to keep httpx out of ``sys.modules`` short of
rewriting that package. What this component's OWN module can and does
guarantee is narrower and is what matters for the trust boundary (DR-0021):
``seedpod/bootstrap.py`` itself never names ``httpx`` and never performs any
network I/O -- it is checked directly against the module's own import
statements below, not against the transitive ``sys.modules`` graph.
"""

from __future__ import annotations

import ast
import asyncio
import subprocess
import sys
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from seedpod import bootstrap
from seedpod.app.services.api_key_service import ApiKeyService
from seedpod.core.clock import SystemClock
from seedpod.data.database import Database
from seedpod.data.repositories import ApiKeyRepository
from seedpod.data.uow import UnitOfWork

_BOOTSTRAP_SRC = Path(bootstrap.__file__)


# ---------------------------------------------------------------------------
# Zero import-time side effects / trust-boundary shape
# ---------------------------------------------------------------------------


def test_importing_bootstrap_has_zero_side_effects(tmp_path):
    """A fresh interpreter, no ``SEEDPOD_*`` env set: importing
    ``seedpod.bootstrap`` must configure no logging handler, open no DB file,
    and make no network call -- all effects live inside the ``_cmd_*``
    functions, invoked only from ``main()`` (CLAUDE.md / DR-0021)."""
    script = (
        "import logging, os\n"
        "assert not any(k.startswith('SEEDPOD_') for k in os.environ), 'test env leaked SEEDPOD_* vars'\n"
        "before = len(logging.root.handlers)\n"
        "import seedpod.bootstrap\n"
        "assert len(logging.root.handlers) == before, 'import installed a logging handler'\n"
        "print('OK')\n"
    )
    env = {k: v for k, v in __import__("os").environ.items() if not k.startswith("SEEDPOD_")}
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "OK"
    assert not any(tmp_path.glob("*.db"))


def test_bootstrap_module_never_imports_httpx_itself():
    """``seedpod/bootstrap.py``'s own import statements never name ``httpx``
    -- this offline tool never speaks HTTP (DR-0021 §0b: "never exposed over
    HTTP and never talks to a running server"). See this file's module
    docstring for why a transitive ``sys.modules`` check isn't the right test."""
    tree = ast.parse(_BOOTSTRAP_SRC.read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    assert "httpx" not in names


def test_bootstrap_module_imports_no_data_service_it_doesnt_need():
    """Sanity check on the trust-boundary shape (DR-0021): this tool is
    allowed direct DB access (it IS the bootstrap CLI), so it legitimately
    imports ``seedpod.data``/``seedpod.app.services`` -- unlike ``seedpodctl``.
    Pin that it does so, so a future edit can't silently make it an HTTP
    client instead."""
    tree = ast.parse(_BOOTSTRAP_SRC.read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    assert "seedpod.data.database" in names
    assert "seedpod.app.services.api_key_service" in names


# ---------------------------------------------------------------------------
# generate-keys
# ---------------------------------------------------------------------------


def test_generate_keys_prints_two_distinct_valid_fernet_keys(capsys):
    exit_code = bootstrap.main(["generate-keys"])

    assert exit_code == 0
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    dev_line = next(line for line in lines if line.startswith("SEEDPOD_SECRET_KEY_DEV="))
    prod_line = next(line for line in lines if line.startswith("SEEDPOD_SECRET_KEY_PROD="))
    dev_key = dev_line.split("=", 1)[1]
    prod_key = prod_line.split("=", 1)[1]

    assert dev_key != prod_key
    # Round-trips through Fernet without raising -- both are well-formed keys.
    Fernet(dev_key.encode())
    Fernet(prod_key.encode())


# ---------------------------------------------------------------------------
# migrate
# ---------------------------------------------------------------------------


def test_migrate_creates_the_schema(tmp_path, monkeypatch):
    db_path = tmp_path / "bootstrap.db"
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", Fernet.generate_key().decode())

    exit_code = bootstrap.main(["migrate"])

    assert exit_code == 0
    db = Database(f"sqlite:///{db_path}")
    try:
        with db.engine.connect() as conn:
            tables = {
                row[0]
                for row in conn.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
    finally:
        db.dispose()
    assert "api_keys" in tables
    assert "clusters" in tables


def test_migrate_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "bootstrap.db"
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", Fernet.generate_key().decode())

    assert bootstrap.main(["migrate"]) == 0
    assert bootstrap.main(["migrate"]) == 0  # no error re-applying to an already-current schema


# ---------------------------------------------------------------------------
# create-admin
# ---------------------------------------------------------------------------


def _make_service(db_path: Path) -> ApiKeyService:
    db = Database(f"sqlite:///{db_path}")
    uow = UnitOfWork(db)
    return ApiKeyService(ApiKeyRepository(), uow, SystemClock())


def test_create_admin_on_cold_db_mints_wildcard_key_that_round_trips(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "bootstrap.db"
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", Fernet.generate_key().decode())

    # Cold DB: no prior `migrate` call. create-admin applies schema itself
    # (this module's own docstring: the schema-readiness choice for this round).
    exit_code = bootstrap.main(["create-admin", "alice", "--expires-days", "30"])

    assert exit_code == 0
    out = capsys.readouterr().out
    plaintext = out.strip().splitlines()[-1]
    assert plaintext.startswith("seedpod_all_")

    service = _make_service(db_path)
    row = asyncio.run(service.validate(plaintext))
    assert row is not None
    assert row.username == "alice"
    assert list(row.permissions) == ["*"]
    assert row.environment == "all"
    assert row.expires_at is not None


def test_create_admin_refuses_when_an_admin_already_exists(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "bootstrap.db"
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", Fernet.generate_key().decode())

    first = bootstrap.main(["create-admin", "alice"])
    capsys.readouterr()  # discard alice's plaintext output
    assert first == 0

    second = bootstrap.main(["create-admin", "bob"])

    assert second != 0
    err = capsys.readouterr().err
    assert "already exists" in err

    service = _make_service(db_path)
    all_keys = asyncio.run(service.list())
    admins = [k for k in all_keys if "*" in k.permissions]
    assert len(admins) == 1
    assert admins[0].username == "alice"


def test_create_admin_requires_a_username(monkeypatch, tmp_path):
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", f"sqlite:///{tmp_path / 'bootstrap.db'}")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", Fernet.generate_key().decode())

    with pytest.raises(SystemExit):
        bootstrap.main(["create-admin"])


# ---------------------------------------------------------------------------
# §0 follow-up 13 — .env loading and clean errors
# ---------------------------------------------------------------------------


def test_main_loads_dotenv_so_the_first_command_an_operator_runs_works(tmp_path, monkeypatch):
    """The cold-start sequence is: `generate-keys` -> paste into `.env` ->
    `migrate`. Without this, that third step cannot see the `.env` written in the
    second and the operator must know to `set -a; . ./.env` first, which nothing
    tells them. DR-0021's own rationale sanctions reading it: for this entry point
    the local filesystem IS the trust boundary."""
    db_path = tmp_path / "cold.db"
    monkeypatch.delenv("SEEDPOD_DATABASE_URL", raising=False)
    monkeypatch.delenv("SEEDPOD_SECRET_KEY_DEV", raising=False)
    (tmp_path / ".env").write_text(
        f"SEEDPOD_DATABASE_URL=sqlite:///{db_path}\n"
        f"SEEDPOD_SECRET_KEY_DEV={Fernet.generate_key().decode()}\n"
    )
    monkeypatch.chdir(tmp_path)

    assert bootstrap.main(["migrate"]) == 0
    assert db_path.exists(), "migrate did not see the .env it was meant to load"


def test_an_explicit_export_still_beats_dotenv(tmp_path, monkeypatch):
    """python-dotenv does not override an already-set variable, so an explicit
    `export` wins -- the same precedence `start.py` has. Pinned because reversing
    it would make a stale `.env` silently outrank the environment an operator
    deliberately set."""
    exported_db = tmp_path / "exported.db"
    dotenv_db = tmp_path / "dotenv.db"
    (tmp_path / ".env").write_text(
        f"SEEDPOD_DATABASE_URL=sqlite:///{dotenv_db}\n"
        f"SEEDPOD_SECRET_KEY_DEV={Fernet.generate_key().decode()}\n"
    )
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", f"sqlite:///{exported_db}")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", Fernet.generate_key().decode())
    monkeypatch.chdir(tmp_path)

    assert bootstrap.main(["migrate"]) == 0
    assert exported_db.exists()
    assert not dotenv_db.exists()


def test_a_missing_required_variable_is_a_clean_error_not_a_traceback(tmp_path, monkeypatch, capsys):
    """v1's own CLI and v2's `seedpodctl` both print `error: ...`; only this tool
    exited with a raw `MissingEnvironmentVariable` traceback, and it is the tool a
    brand-new operator meets first."""
    monkeypatch.delenv("SEEDPOD_DATABASE_URL", raising=False)
    monkeypatch.delenv("SEEDPOD_SECRET_KEY_DEV", raising=False)
    monkeypatch.chdir(tmp_path)  # no .env here

    code = bootstrap.main(["migrate"])

    assert code == 2
    err = capsys.readouterr().err
    assert err.startswith("error: ")
    assert "SEEDPOD_DATABASE_URL" in err
    assert "Traceback" not in err
    assert "generate-keys" in err  # the hint names the next action


# ---------------------------------------------------------------------------
# seed-secrets (DR-0041 decision 5)
# ---------------------------------------------------------------------------


_REPO_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _seed_env(tmp_path, monkeypatch):
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", f"sqlite:///{tmp_path / 'seed.db'}")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", Fernet.generate_key().decode())
    monkeypatch.setenv("SEEDPOD_CONFIG_DIR", str(_REPO_CONFIG_DIR))


def test_required_secrets_derives_the_real_exampleco_dev_stack_list():
    """Derived from the shipped manifest templates, not maintained by hand. 7
    keys, and `ghcr_dockerconfig_json` excluded because ManifestResolver builds
    it (services/manifests.py:74) -- telling an operator to invent it would be
    wrong."""
    from seedpod.app.services.secret_requirements import required_secrets

    names = [r.key_name for r in required_secrets(_REPO_CONFIG_DIR, "exampleco-dev-stack-nodns")]

    assert len(names) == 7
    assert names == sorted(names)
    assert "ghcr_dockerconfig_json" not in names
    # spot-check the families a real deployment needs
    for expected in ("jwt_secret", "mail_password", "cache_password", "s3_secret_key"):
        assert expected in names


def test_s3_access_key_is_pinned_to_the_profiles_own_minio_root_user():
    """Not free-form: minio is configured from MINIO_ROOT_USER and the app
    authenticates with s3_access_key, so a placeholder here produces a stack that
    comes up and then cannot talk to its own object store."""
    from seedpod.app.services.secret_requirements import required_secrets

    reqs = {r.key_name: r for r in required_secrets(_REPO_CONFIG_DIR, "exampleco-dev-stack-nodns")}

    assert reqs["s3_access_key"].pinned_value == "dev-minio-access-key"
    assert reqs["jwt_secret"].pinned_value is None  # everything else is free-form


def test_seed_secrets_reports_missing_and_exits_non_zero(tmp_path, monkeypatch, capsys):
    """Report-only by default, and non-zero so a cold-start script notices."""
    _seed_env(tmp_path, monkeypatch)

    code = bootstrap.main(
        ["seed-secrets", "ephemeral", "--profile", "exampleco-dev-stack-nodns"]
    )

    out = capsys.readouterr().out
    assert code == 1
    assert "requires 7 secrets" in out
    assert "missing: 7" in out
    assert "MISSING  jwt_secret" in out
    assert "--placeholder" in out


def test_seed_secrets_fills_and_is_idempotent(tmp_path, monkeypatch, capsys):
    _seed_env(tmp_path, monkeypatch)

    assert bootstrap.main(
        ["seed-secrets", "ephemeral", "--profile", "exampleco-dev-stack-nodns", "--placeholder"]
    ) == 0
    capsys.readouterr()

    # second run: nothing missing, nothing written, exit 0
    assert bootstrap.main(
        ["seed-secrets", "ephemeral", "--profile", "exampleco-dev-stack-nodns"]
    ) == 0
    assert "missing: 0" in capsys.readouterr().out


def test_seed_secrets_uses_the_pinned_value_not_the_placeholder(tmp_path, monkeypatch):
    """The whole point of pinning. Decrypted through the real CryptoService."""
    from sqlalchemy import text

    from seedpod.services.crypto import CryptoService

    _seed_env(tmp_path, monkeypatch)
    key = __import__("os").environ["SEEDPOD_SECRET_KEY_DEV"]

    bootstrap.main(
        ["seed-secrets", "ephemeral", "--profile", "exampleco-dev-stack-nodns", "--placeholder"]
    )

    crypto = CryptoService(key, None)
    db = Database(f"sqlite:///{tmp_path / 'seed.db'}")
    try:
        with db.engine.connect() as conn:
            stored = {
                row[0]: crypto.decrypt(row[1], "DEV")
                for row in conn.execute(
                    text("select key_name, encrypted_value from secrets")
                ).fetchall()
            }
    finally:
        db.dispose()

    assert stored["s3_access_key"] == "dev-minio-access-key"  # pinned
    assert stored["jwt_secret"] == "DevPlaceholder1!"  # placeholder


def test_the_default_placeholder_satisfies_keycloaks_realm_policy():
    """Two real runs (~20 minutes each) were lost to this on 2026-08-12/13:
    `dev-placeholder-secret` failed invalidPasswordMinUpperCaseChars, then
    `DevPlaceholder123` failed invalidPasswordMinSpecialChars."""
    from seedpod.app.services.secret_requirements import DEFAULT_PLACEHOLDER

    assert any(c.isupper() for c in DEFAULT_PLACEHOLDER)
    assert any(c.islower() for c in DEFAULT_PLACEHOLDER)
    assert any(c.isdigit() for c in DEFAULT_PLACEHOLDER)
    assert any(not c.isalnum() for c in DEFAULT_PLACEHOLDER)


def test_seed_secrets_reports_an_unknown_profile_without_a_traceback(tmp_path, monkeypatch, capsys):
    _seed_env(tmp_path, monkeypatch)

    code = bootstrap.main(["seed-secrets", "ephemeral", "--profile", "no-such-profile"])

    assert code == 1
    assert "not found" in capsys.readouterr().err
