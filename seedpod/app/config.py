"""``AppConfig`` -- the one settings object every v2 database/provider/service
construction path is threaded through (docs/design/seam-d-foundation.md Decision 8).

Salvaged data (env var names, the ``SEEDPOD_`` prefix convention, and the four
unprefixed token aliases) from ``reference-code/seedpod/seedpod/core/config.py``'s
pydantic-settings ``Settings`` class (lines 79-146); the *shape* is fresh (a plain
frozen dataclass, not a pydantic-settings singleton) per Decision 8's exact field
list. ``from_env()`` is the ONLY place ``os.environ`` is read anywhere in v2 --
every other constructor takes an ``AppConfig`` (or a value threaded down from one),
never reaches for the environment itself (coherence-review's v1-global inventory:
"``get_settings()`` pydantic singleton -> ``AppConfig``, passed down; nothing
imports it").

v1's ``load_dotenv()`` (core/config.py:17, a module-import-time side effect) is
deliberately NOT called here: Decision 8 assigns that to ``start.py``/
``seedpod/__main__.py`` ("keeps only what's orthogonal to wiring: load_dotenv,
PID-file singleton check, log rotation -- then calls main()"), neither of which
this component builds. ``from_env()`` only reads whatever is already in
``os.environ`` by the time it runs -- constructing an ``AppConfig`` has zero side
effects of its own (CLAUDE.md: "importing any v2 module has zero side effects").

**No production-DB default anywhere** (CLAUDE.md hard rule; v1 gotcha 10 --
"forget one of five global seams -> silently hit db/seedpod.db" -- is
unrepresentable in v2): unlike v1's ``Settings.database_url`` default of
``"sqlite:///./db/seedpod.db"``, ``from_env()`` raises loudly if
``SEEDPOD_DATABASE_URL``/``SEEDPOD_SECRET_KEY_DEV`` are unset, exactly mirroring
``AppConfig``'s own dataclass signature (both fields are required, no default).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

__all__ = ["AppConfig", "MissingEnvironmentVariable"]


class MissingEnvironmentVariable(RuntimeError):
    """Raised by ``AppConfig.from_env()`` for a required-but-unset variable --
    fail-fast, matching ``RuleEngine.load``'s discipline (CLAUDE.md), rather than
    v1's silent ``sqlite:///./db/seedpod.db`` fallback."""


@dataclass(frozen=True)
class AppConfig:
    """Docs/design/seam-d-foundation.md Decision 8's exact field set. Every
    field below is real construction data for exactly one composition-root step
    (``factory.py``'s numbered comments) -- nothing here is unused."""

    database_url: str
    secret_key_dev: str
    secret_key_prod: str | None = None
    environment: str = "development"
    config_dir: Path = field(default_factory=lambda: Path("config"))  # templates, profiles, rules, workflows, providers
    background_tasks: bool = True  # False in tests: reconciler + health + orphan-resume off;
    #                                the outbox executor and timer poller ALWAYS run (correctness)
    outbox_poll_interval: float = 0.25
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: tuple[str, ...] = ("*",)
    digitalocean_token: str | None = None
    github_token: str | None = None
    github_organization: str | None = None
    cloudflare_api_token: str | None = None
    enabled_providers: tuple[str, ...] = ("digitalocean", "kind", "tart", "orbstack")
    snapshot_storage_path: Path = field(default_factory=lambda: Path("data/snapshots"))  # Round 6,
    #   api-features: local on-disk root SnapshotService dumps service data under (one
    #   subdirectory per snapshot id) -- DR-0020's real fail-open pre-destroy snapshot and
    #   POST /api/snapshots both write here. Not part of Decision 8's original field list
    #   (the snapshot subsystem didn't exist yet); additive, matching this dataclass's own
    #   "grows additively" discipline (mirrors every other Round-6 AppConfig addition).
    log_level: str = "INFO"  # Round 7, entrypoint/server-runner (DR-0021 §0a/point 1):
    #   consumed by seedpod/app/logging.py's setup_logging(config) -- salvaged verbatim
    #   from reference-code's Settings.log_level default. Additive, same discipline as
    #   snapshot_storage_path above.
    log_format: str = "json"  # "json" | "text" -- salvaged from reference-code's
    #   Settings.log_format default; drives setup_logging's console formatter choice
    #   (file handler is always JSON, matching v1).
    log_to_console: bool = False  # v1's start.py default (LOG_TO_CONSOLE unset -> file-only);
    #   v1 read this directly from os.environ in start.py rather than through its pydantic
    #   Settings -- v2 folds it into AppConfig.from_env() instead so from_env() stays the
    #   ONLY os.environ reader anywhere in v2 (module docstring).
    log_to_file: bool = True  # v1's start.py default (LOG_TO_FILE unset -> true).
    log_dir: Path = field(default_factory=lambda: Path("logs"))  # v1's start.py hardcoded
    #   "logs" literal for both rotate_logs_on_startup and setup_logging's file handler --
    #   made configurable here (additive) rather than hardcoded, matching this dataclass's
    #   own "grows additively" discipline; setup_logging(config) reads it, never os.environ.

    # DR-0041 Amendment B: where the single-instance flock lives. Default matches the
    # pre-existing `start.py` behaviour (a `seedpod.pid` beside the process's cwd), so
    # nothing changes for a repo-root dev run; a packaged appliance points it at an
    # absolute path under `var/`, since a release root is not a directory anyone cd's into.
    pid_file: Path = field(default_factory=lambda: Path("seedpod.pid"))

    # DR-0041 decision 3: where the BUILT SPA lives (`ui/dist`), a sibling of
    # config/ found the same way config_dir is. None -- the default -- mounts
    # nothing, which is what keeps the vite dev-server workflow (SPA on its own
    # origin, VITE_API_URL, CORS) working unchanged.
    ui_dir: Path | None = None

    @classmethod
    def from_env(cls) -> AppConfig:
        """The ONLY place ``os.environ`` is read in v2. ``SEEDPOD_``-prefixed
        names for everything v1 didn't already carve an exception for; the four
        third-party tokens keep v1's unprefixed alias verbatim (reference-code
        .../core/config.py:117-146: ``alias="DIGITALOCEAN_TOKEN"`` etc -- CI/deploy
        tooling already sets these unprefixed names, so v2 keeps reading them that
        way rather than silently breaking existing deployment secrets)."""
        return cls(
            database_url=_require("SEEDPOD_DATABASE_URL"),
            secret_key_dev=_require("SEEDPOD_SECRET_KEY_DEV"),
            secret_key_prod=os.environ.get("SEEDPOD_SECRET_KEY_PROD"),
            environment=os.environ.get("SEEDPOD_ENVIRONMENT", "development"),
            config_dir=Path(os.environ.get("SEEDPOD_CONFIG_DIR", "config")),
            background_tasks=_bool_env("SEEDPOD_BACKGROUND_TASKS", default=True),
            outbox_poll_interval=float(os.environ.get("SEEDPOD_OUTBOX_POLL_INTERVAL", "0.25")),
            api_host=os.environ.get("SEEDPOD_API_HOST", "0.0.0.0"),
            api_port=int(os.environ.get("SEEDPOD_API_PORT", "8000")),
            cors_origins=_csv_env("SEEDPOD_CORS_ORIGINS", default=("*",)),
            digitalocean_token=os.environ.get("DIGITALOCEAN_TOKEN"),
            github_token=os.environ.get("GITHUB_TOKEN"),
            github_organization=os.environ.get("GITHUB_ORGANIZATION"),
            cloudflare_api_token=os.environ.get("CLOUDFLARE_API_TOKEN"),
            enabled_providers=_csv_env(
                "SEEDPOD_ENABLED_PROVIDERS", default=("digitalocean", "kind", "tart", "orbstack")
            ),
            snapshot_storage_path=Path(os.environ.get("SEEDPOD_SNAPSHOT_STORAGE_PATH", "data/snapshots")),
            log_level=os.environ.get("SEEDPOD_LOG_LEVEL", "INFO"),
            log_format=os.environ.get("SEEDPOD_LOG_FORMAT", "json"),
            log_to_console=_bool_env("SEEDPOD_LOG_TO_CONSOLE", default=False),
            log_to_file=_bool_env("SEEDPOD_LOG_TO_FILE", default=True),
            log_dir=Path(os.environ.get("SEEDPOD_LOG_DIR", "logs")),
            pid_file=Path(os.environ.get("SEEDPOD_PID_FILE", "seedpod.pid")),
            ui_dir=Path(_ui_dir) if (_ui_dir := os.environ.get("SEEDPOD_UI_DIR")) else None,
        )


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise MissingEnvironmentVariable(f"{name} must be set (no production-DB-style default in v2)")
    return value


def _bool_env(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _csv_env(name: str, *, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    values = tuple(item.strip() for item in raw.split(",") if item.strip())
    return values or default
