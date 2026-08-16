"""Structured logging setup for seedpod v2's server-runner entry point
(docs/decisions/DR-0021 §0a/point 1; docs/design/seam-d-foundation.md Decision
8's ``setup_logging()`` reference). A composition/edge concern -- lives under
``seedpod/app/``, NOT ``seedpod/core/`` (core is pure/IO-free, no logging
configuration, CLAUDE.md).

Salvaged near-verbatim from ``reference-code/seedpod/seedpod/core/logging.py``:
``CorrelationFilter`` (lines 26-37), ``SafeStreamHandler`` (40-55),
``rotate_logs_on_startup`` (58-86), ``JSONFormatter`` (89-120), ``setup_logging``
(123-191). NOT ported: the convenience ``log_api_request``/``log_cluster_*``/
``LoggerMixin``/``get_logger`` helpers below those (v1 lines 192-327) -- this
round's brief asks only for the rotation + setup surface, not a logging-helper
library nothing in v2 calls yet.

The only real change from v1: ``setup_logging``'s signature. v1 took five
scalar kwargs sourced from a pydantic ``Settings`` singleton (itself an
os.environ reader) plus two more read directly via ``os.getenv`` in
``start.py``. v2 takes one ``AppConfig`` instead, so ``AppConfig.from_env()``
stays the ONE place ``os.environ`` is read anywhere in v2 (CLAUDE.md /
``seedpod/app/config.py``'s own docstring) -- this module never reads the
environment itself.

Both public functions are pure side-effecting utilities: importing this module
installs no handler and touches no filesystem ("importing any v2 module has
zero side effects", CLAUDE.md). Everything happens only when a caller invokes
``setup_logging(config)`` / ``rotate_logs_on_startup(...)`` -- both of which
``seedpod/__main__.py``/``start.py`` call explicitly, never at import time.

``setup_logging`` is idempotent (salvaged behavior, not a v2 addition): it
always resets ``logging.root.handlers`` to ``[]`` before installing the
handlers the given config calls for, so calling it twice never accumulates
duplicate handlers.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from contextlib import suppress
from contextvars import ContextVar
from datetime import UTC, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from seedpod.app.config import AppConfig

__all__ = [
    "CorrelationFilter",
    "JSONFormatter",
    "SafeStreamHandler",
    "get_correlation_id",
    "rotate_logs_on_startup",
    "set_correlation_id",
    "setup_logging",
]

# Context variable for tracking request correlation IDs (v1 parity: reference-
# code/.../core/logging.py:20).
correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


class CorrelationFilter(logging.Filter):
    """Add the current correlation ID to every log record (v1 verbatim)."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id.get("")
        return True


class SafeStreamHandler(logging.StreamHandler):
    """``StreamHandler`` that silently swallows errors during ``emit`` (v1
    verbatim) -- during shutdown, stdout/stderr may already be closed, and a
    console log line is never worth crashing shutdown over."""

    def emit(self, record: logging.LogRecord) -> None:
        with suppress(Exception):
            super().emit(record)

    def handleError(self, record: logging.LogRecord) -> None:  # noqa: N802 - stdlib override name
        """Disabled: the default ``handleError`` writes to stderr, which is
        exactly the failure mode this handler exists to avoid during
        shutdown (v1 verbatim)."""


class JSONFormatter(logging.Formatter):
    """Structured JSON log formatter (v1 verbatim)."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        cid = getattr(record, "correlation_id", "")
        if cid:
            log_data["correlation_id"] = cid

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        _reserved = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "getMessage", "correlation_id", "message",
        }
        for key, value in record.__dict__.items():
            if key not in _reserved:
                log_data[key] = value

        return json.dumps(log_data, default=str)


def rotate_logs_on_startup(
    log_dir: Path | str = "logs",
    log_name: str = "seedpod.log",
    retention: int = 10,
) -> None:
    """Rotate any pre-existing log file at process startup (v1 verbatim):
    renames the current file with a startup timestamp, then prunes all but
    the most recent ``retention`` startup-rotated files. Salvaged as its own
    callable so ``start.py`` can run it BEFORE ``AppConfig``/logging even
    exist (DR-0021: start.py's job is orthogonal to composition-root wiring).
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    current_log = log_path / log_name
    if current_log.exists():
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        rotated_log = log_path / f"{log_name}.startup-{timestamp}"
        current_log.rename(rotated_log)

        startup_logs = sorted(log_path.glob(f"{log_name}.startup-*"))
        if len(startup_logs) > retention:
            for old_log in startup_logs[:-retention]:
                old_log.unlink()


def setup_logging(config: AppConfig) -> None:
    """Configure the root logger per ``config``'s ``log_*`` fields (v1's
    ``setup_logging``, re-pointed at one ``AppConfig`` instead of five scalar
    kwargs -- module docstring). Idempotent: always clears
    ``logging.root.handlers`` first, so repeat calls never duplicate
    handlers.
    """
    handlers: list[logging.Handler] = []

    if config.log_to_file:
        log_path = Path(config.log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        file_handler = TimedRotatingFileHandler(
            log_path / "seedpod.log",
            when="midnight",
            backupCount=30,
            encoding="utf-8",
        )
        file_handler.addFilter(CorrelationFilter())
        file_handler.setFormatter(JSONFormatter())
        handlers.append(file_handler)

    if config.log_to_console:
        console_handler = SafeStreamHandler(sys.stdout)
        console_handler.addFilter(CorrelationFilter())
        if config.log_format.lower() == "json":
            console_handler.setFormatter(JSONFormatter())
        else:
            console_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
        handlers.append(console_handler)

    logging.root.handlers = []
    for handler in handlers:
        logging.root.addHandler(handler)
    logging.root.setLevel(getattr(logging, config.log_level.upper()))

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    logging.getLogger("seedpod").info(
        "Logging initialized with level=%s, format=%s, file=%s, console=%s",
        config.log_level,
        config.log_format,
        config.log_to_file,
        config.log_to_console,
    )


def get_correlation_id() -> str:
    """Get the current correlation ID (v1 verbatim)."""
    return correlation_id.get("")


def set_correlation_id(cid: str | None = None) -> str:
    """Set the correlation ID for the current context (v1 verbatim)."""
    if cid is None:
        cid = str(uuid.uuid4())[:8]
    correlation_id.set(cid)
    return cid
