"""``seedpod-bootstrap`` -- the OFFLINE, on-disk-only cold-start CLI
(docs/decisions/DR-0021 §0b/point 2). One of v2's THREE trust-model entry
points, and the ONLY one with direct DB/filesystem access -- it exists solely
to break the chicken-and-egg of cold start (no credential exists yet, so the
HTTP user CLI, ``seedpodctl``, cannot authenticate against anything). It never
makes an HTTP call and is never exposed over HTTP; local filesystem access
*is* its trust boundary (DR-0021's rationale section).

Three subcommands, argparse (stdlib -- v2 does not depend on ``click``):

- ``generate-keys`` -- prints two ``Fernet.generate_key()`` values as
  ``SEEDPOD_SECRET_KEY_DEV``/``_PROD`` lines ready to paste into ``.env``.
  Touches no DB. Salvaged from
  ``reference-code/seedpod/seedpod_cli.py:35-49``'s ``generate_keys``.
- ``migrate`` -- ``Database(AppConfig.from_env().database_url)`` +
  ``migrate(db.engine, MIGRATIONS_DIR)`` (``seedpod/data/migrate.py``, Seam D
  Decision 6): applies v2's *own* numbered schema to a cold DB. Idempotent --
  running it twice is a no-op the second time. **No v1-data-migration path
  exists or is wanted** (Kezia, 2026-07-19 scope note, DR-0021 §0b): this is
  not an importer, it is v2's schema authority applied fresh.
- ``create-admin <username>`` -- mints the FIRST API key directly, through the
  SAME object graph and hashing the server uses
  (``Database`` -> ``UnitOfWork`` -> ``ApiKeyRepository`` ->
  ``ApiKeyService(repo, uow, SystemClock())``, seam-d Decision 8 step 9's
  construction shape), rather than a raw ``INSERT`` (v1's
  ``bootstrap_admin``, ``reference-code/seedpod/seedpod_cli.py:52-116``,
  went straight through ``SQLAlchemyAPIKeyRepository`` beneath a raw
  session). Salvages v1's "refuse if an admin already exists" guard
  (``seedpod_cli.py:63-72``) verbatim in spirit, adapted to v2's permissions
  **list** shape (``["*"]``, the bare-wildcard super-permission -- NOT v1's
  ``{"admin:*": True, "deployments:*": True, ...}`` category-dict, per this
  round's brief). The plaintext key is printed exactly once and never stored.

  **Schema-readiness choice (this component, stated per its own brief):**
  ``create-admin`` calls ``migrate()`` itself before minting the key, rather
  than requiring a separate prior ``migrate`` invocation and erroring on a
  missing ``api_keys`` table. ``migrate()`` is idempotent (a no-op against an
  already-current schema), so this makes "cold DB, zero prior setup" ->
  ``create-admin <username>`` a single working command -- the more literal
  reading of "breaks the cold-start chicken-and-egg" (DR-0021's stated
  purpose for this whole tool) -- while a standalone ``migrate`` subcommand
  still exists for operators who want to apply schema without also minting a
  key (e.g. re-running migrations after a v2 upgrade).

Zero import-time side effects (CLAUDE.md / DR-0021): importing this module
reads no environment variable, opens no database, makes no network call.
Every effect happens inside the ``_cmd_*`` functions, invoked only from
``main()``.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence

from cryptography.fernet import Fernet

from seedpod.app.config import AppConfig, MissingEnvironmentVariable
from seedpod.app.services.api_key_service import ApiKeyService
from seedpod.app.services.secret_requirements import (
    DEFAULT_PLACEHOLDER,
    SecretRequirement,
    required_secrets,
)
from seedpod.app.services.secret_service import SecretService
from seedpod.core.clock import SystemClock
from seedpod.data.database import Database
from seedpod.data.migrate import MIGRATIONS_DIR
from seedpod.data.migrate import migrate as run_migrations
from seedpod.data.repositories import ApiKeyRepository, SecretAuditRepository, SecretRepository
from seedpod.data.uow import UnitOfWork
from seedpod.services.crypto import CryptoService

__all__ = ["main"]


def _cmd_generate_keys(_args: argparse.Namespace) -> int:
    """Salvaged from ``reference-code/seedpod/seedpod_cli.py:35-49``: two
    fresh Fernet keys, dev + prod. No DB, no filesystem write -- the caller
    pastes the printed lines into ``.env`` (or exports them) themselves."""
    dev_key = Fernet.generate_key().decode()
    prod_key = Fernet.generate_key().decode()
    print(f"SEEDPOD_SECRET_KEY_DEV={dev_key}")
    print(f"SEEDPOD_SECRET_KEY_PROD={prod_key}")
    return 0


def _cmd_migrate(_args: argparse.Namespace) -> int:
    config = AppConfig.from_env()
    db = Database(config.database_url)
    try:
        run_migrations(db.engine, MIGRATIONS_DIR)
    finally:
        db.dispose()
    print(f"migrated {config.database_url!r} to the current schema version")
    return 0


def _cmd_create_admin(args: argparse.Namespace) -> int:
    config = AppConfig.from_env()
    db = Database(config.database_url)
    try:
        # Idempotent; see this module's docstring for why create-admin applies
        # schema itself rather than requiring a prior `migrate` run.
        run_migrations(db.engine, MIGRATIONS_DIR)
        uow = UnitOfWork(db)
        service = ApiKeyService(ApiKeyRepository(), uow, SystemClock())
        return asyncio.run(_create_admin(service, args.username, args.expires_days))
    finally:
        db.dispose()


async def _create_admin(service: ApiKeyService, username: str, expires_days: int) -> int:
    # v1's admin-already-exists guard (seedpod_cli.py:63-72), adapted to v2's
    # permissions LIST: the bare "*" super-wildcard, not v1's "admin:*" dict key.
    existing = await service.list()
    for row in existing:
        if "*" in row.permissions:
            print(
                f"an admin key already exists (username={row.username!r}); "
                "refusing to mint a second one",
                file=sys.stderr,
            )
            return 1

    row, plaintext = await service.create_api_key(
        username=username,
        environment=None,
        permissions=["*"],
        expires_hours=expires_days * 24,
    )
    print("bootstrap admin key created:")
    print(f"  username:   {row.username}")
    print(f"  environment: {row.environment}")
    print(f"  expires_at: {row.expires_at.isoformat() if row.expires_at else 'never'}")
    print()
    print("save this key now -- it will not be shown again:")
    print(plaintext)
    return 0


def _cmd_seed_secrets(args: argparse.Namespace) -> int:
    """DR-0041 decision 5. Cold-starting a dev stack needed ~20 secret names that
    were recorded nowhere; this derives them from the profile's own templates."""
    config = AppConfig.from_env()
    try:
        requirements = required_secrets(config.config_dir, args.profile)
    except Exception as exc:  # noqa: BLE001 -- surface the loader's own message verbatim
        print(f"error: {exc}", file=sys.stderr)
        return 1

    db = Database(config.database_url)
    try:
        run_migrations(db.engine, MIGRATIONS_DIR)
        uow = UnitOfWork(db)
        crypto = CryptoService(config.secret_key_dev, config.secret_key_prod)
        service = SecretService(
            crypto,
            SecretRepository(crypto),  # the repo encrypts/decrypts; it takes crypto too
            SecretAuditRepository(),
            uow,
            SystemClock(),
        )
        return asyncio.run(
            _seed_secrets(service, args.environment, args.profile, requirements, args.placeholder)
        )
    finally:
        db.dispose()


async def _seed_secrets(
    service: SecretService,
    environment: str,
    profile: str,
    requirements: Sequence[SecretRequirement],
    placeholder: str | None,
) -> int:
    existing = {row.key_name for row in await service.list_for_environment(environment)}
    missing = [req for req in requirements if req.key_name not in existing]

    print(f"profile {profile!r} requires {len(requirements)} secrets in {environment!r}")
    print(f"  present: {len(requirements) - len(missing)}")
    print(f"  missing: {len(missing)}")

    if not missing:
        print("nothing to do")
        return 0

    if placeholder is None:
        print()
        for req in missing:
            pin = f"  (must be {req.pinned_value!r})" if req.pinned_value else ""
            print(f"  MISSING  {req.key_name}{pin}")
        print()
        print("re-run with --placeholder to fill these with a development value")
        return 1  # non-zero so a cold-start script notices

    print()
    for req in missing:
        # A pinned requirement ignores the placeholder entirely: the value is not
        # free-form, and seeding it with anything else produces a stack that comes
        # up and then fails to authenticate -- worse than one that fails to start.
        value = req.pinned_value or placeholder
        await service.upsert(environment, req.key_name, value, actor="bootstrap:seed-secrets")
        note = "  (pinned by the profile)" if req.pinned_value else ""
        print(f"  seeded   {req.key_name}{note}")

    print()
    print(f"seeded {len(missing)} secrets. These are DEVELOPMENT placeholders -- never production.")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="seedpod-bootstrap",
        description=(
            "Offline, on-disk cold-start tool for seedpod v2 (DR-0021). "
            "The only entry point with direct DB access; never talks HTTP."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "generate-keys", help="print two fresh Fernet dev/prod encryption keys"
    ).set_defaults(func=_cmd_generate_keys)

    subparsers.add_parser(
        "migrate", help="apply v2's numbered schema to the configured (cold) database"
    ).set_defaults(func=_cmd_migrate)

    seed = subparsers.add_parser(
        "seed-secrets",
        help="report (and optionally fill) the secrets a deployment profile needs",
    )
    seed.add_argument("environment", help="secret environment, e.g. ephemeral")
    seed.add_argument("--profile", required=True, help="deployment profile name")
    seed.add_argument(
        "--placeholder",
        nargs="?",
        const=DEFAULT_PLACEHOLDER,
        default=None,
        help=(
            "fill missing secrets with this value (bare flag uses "
            f"{DEFAULT_PLACEHOLDER!r}, which satisfies Keycloak's realm password "
            "policy -- upper, lower, digit and symbol; two real runs were lost "
            "discovering that). Omit to report only."
        ),
    )
    seed.set_defaults(func=_cmd_seed_secrets)

    create_admin = subparsers.add_parser(
        "create-admin", help="mint the first API key (refuses if an admin key already exists)"
    )
    create_admin.add_argument("username")
    create_admin.add_argument(
        "--expires-days", type=int, default=365, help="key lifetime in days (default: 365)"
    )
    create_admin.set_defaults(func=_cmd_create_admin)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Loads ``.env`` before dispatching, and turns a missing required variable
    into a CLI error rather than a traceback (backlog §0 follow-up 13).

    **``load_dotenv()`` is called HERE, not at import time** -- this module's own
    "zero import-time side effects" contract (CLAUDE.md / DR-0021) is unchanged:
    importing ``seedpod.bootstrap`` still reads no environment variable and opens
    nothing. It is called at all because this is the FIRST command a new operator
    runs (``seedpod-bootstrap migrate``), immediately after writing the ``.env``
    that ``generate-keys`` told them to write -- and without this they must know
    to ``set -a; . ./.env`` first, which nothing tells them. DR-0021's own
    rationale sanctions it: for this entry point "local filesystem access *is*
    its trust boundary", and ``.env`` is a local file.

    ``python-dotenv`` does not override already-exported variables, so an
    explicit ``export`` still wins -- the same precedence ``start.py`` has.

    **``usecwd=True`` is load-bearing.** Bare ``load_dotenv()`` resolves its path
    by walking up from the CALLING MODULE's file, so it would find the checkout's
    own ``.env`` no matter which directory the operator ran the command in -- a
    genuinely surprising result for a CLI, and one that silently points a cold-start
    at the developer's database. Searching from the working directory is what an
    operator means by "my .env"; running from a subdirectory still walks up."""
    from dotenv import find_dotenv, load_dotenv

    load_dotenv(find_dotenv(usecwd=True))
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except MissingEnvironmentVariable as exc:
        # A raw traceback is the wrong register for "you have not finished
        # setting up" -- `seedpodctl` already prints `error: ...` cleanly for its
        # own failures (ctl/cli.py:678-697) and this matches it.
        print(f"error: {exc}", file=sys.stderr)
        print("hint: run `seedpod-bootstrap generate-keys` and put the output in .env", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
