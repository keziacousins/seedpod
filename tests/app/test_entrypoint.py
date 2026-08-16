"""The server-runner component (docs/decisions/DR-0021 §0a/point 1):
``seedpod/__main__.py`` (``build_server``/``main``), ``seedpod/app/logging.py``
(``setup_logging``/``rotate_logs_on_startup``), the additive ``AppConfig``
``log_*`` fields, and repo-root ``start.py`` (``check_pid_file()``'s PID-file
singleton guard, salvaged verbatim from
``reference-code/seedpod/start.py:22-75``). Real sqlite tmp db, ``FrozenClock``,
hand-built ``FakeProvider`` -- zero Mock/patch (CLAUDE.md). ``uvicorn.run``
itself is never invoked (it blocks and binds a real port) -- ``build_server``
is exercised directly and the attached lifespan is driven as an async context
manager over a real ``build_app().api``, exactly as the module's own
docstring recommends. ``check_pid_file()`` is loaded from ``start.py`` via
``importlib.util`` (it lives at the repo root, not inside the ``seedpod``
package) and exercised in-process against a ``tmp_path``-scoped
``PROJECT_ROOT`` (never the real repo root), plus two real-subprocess tests
for its ``atexit``/``SIGTERM`` cleanup paths (real signals, no Mock/patch).
"""

from __future__ import annotations

import ast
import logging
import os
import socket
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from cryptography.fernet import Fernet

from seedpod.app.config import AppConfig
from seedpod.app.logging import rotate_logs_on_startup, setup_logging
from seedpod.app.singleton import (
    AlreadyRunning,
    PortUnavailable,
    assert_port_available,
    single_instance,
)
from seedpod.core.clock import FrozenClock
from tests.fakes import FakeProvider, sequential_ids

_NOW = datetime(2026, 7, 19, 12, 0, 0, tzinfo=UTC)
_REPO_ROOT = Path(__file__).parent.parent.parent
_START_PY = _REPO_ROOT / "start.py"


def _config(tmp_path: Path, test_config_dir: Path, **overrides) -> AppConfig:
    overrides.setdefault("background_tasks", False)
    return AppConfig(
        database_url=f"sqlite:///{tmp_path}/entrypoint.db",
        secret_key_dev=Fernet.generate_key().decode(),
        config_dir=test_config_dir,
        **overrides,
    )


# ---------------------------------------------------------------------------
# Zero import-time side effects
# ---------------------------------------------------------------------------


def test_importing_dunder_main_has_zero_side_effects(tmp_path):
    """A fresh interpreter, no ``SEEDPOD_*`` env set: importing
    ``seedpod.__main__`` must configure no logging handler, open no DB file,
    and make no network call -- all effects live inside ``main()``/
    ``build_server()`` (CLAUDE.md / this module's own docstring).
    """
    script = (
        "import logging, os\n"
        "assert not any(k.startswith('SEEDPOD_') for k in os.environ), 'test env leaked SEEDPOD_* vars'\n"
        "before = len(logging.root.handlers)\n"
        "import seedpod.__main__\n"
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
    assert not (tmp_path / "seedpod.pid").exists()
    assert not any(tmp_path.glob("*.db"))


def test_importing_bootstrap_adjacent_start_module_is_unaffected(tmp_path):
    """Importing ``seedpod.app.logging`` (the new module this component adds)
    is equally inert -- no handler installed, no directory created."""
    before = len(logging.root.handlers)
    before_dirs = set(tmp_path.iterdir())
    import seedpod.app.logging  # noqa: F401

    assert len(logging.root.handlers) == before
    assert set(tmp_path.iterdir()) == before_dirs


# ---------------------------------------------------------------------------
# build_server / lifespan wiring
# ---------------------------------------------------------------------------


async def test_lifespan_start_applies_migrations_and_health_probe_works(tmp_path, test_config_dir):
    from seedpod.__main__ import build_server

    config = _config(tmp_path, test_config_dir)
    app = build_server(
        config,
        providers={"fake": FakeProvider()},
        clock=FrozenClock(_NOW),
        id_gen=sequential_ids(),
    )

    # Before the lifespan runs, migrate() has never been called -- no tables.
    with app.db.engine.connect() as conn:
        tables_before = {
            row[0]
            for row in conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "clusters" not in tables_before

    async with app.api.router.lifespan_context(app.api):
        # start() ran: migrations applied (H7's schema authority), executor live.
        with app.db.engine.connect() as conn:
            tables = {
                row[0]
                for row in conn.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "clusters" in tables
        assert app.executor.running

        transport = httpx.ASGITransport(app=app.api)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/health/detailed")
        assert resp.status_code == 200
        assert resp.json()["database"]["connected"] is True

    # stop() ran on exit: clean, observable teardown.
    assert not app.executor.running
    assert not app.timers.running


async def test_lifespan_is_the_only_thing_that_starts_the_runtime(tmp_path, test_config_dir):
    """Building the server (``build_server``) must NOT itself start anything --
    only entering the lifespan does (mirrors ``build_app``'s own "pure
    construction" contract, coherence-review Conflict 15)."""
    from seedpod.__main__ import build_server

    config = _config(tmp_path, test_config_dir)
    app = build_server(
        config,
        providers={"fake": FakeProvider()},
        clock=FrozenClock(_NOW),
        id_gen=sequential_ids(),
    )
    assert not app.executor.running
    async with app.api.router.lifespan_context(app.api):
        assert app.executor.running
    assert not app.executor.running


async def test_lifespan_stop_is_reached_even_on_exception(tmp_path, test_config_dir):
    """``App.stop()`` runs on the way out even if the body raises -- the
    ``async with app.running()`` inside the attached lifespan is a normal
    context manager, so this is really pinning that ``_attach_lifespan``
    didn't accidentally swallow exceptions."""
    from seedpod.__main__ import build_server

    config = _config(tmp_path, test_config_dir)
    app = build_server(
        config,
        providers={"fake": FakeProvider()},
        clock=FrozenClock(_NOW),
        id_gen=sequential_ids(),
    )

    class _Boom(Exception):
        pass

    with pytest.raises(_Boom):
        async with app.api.router.lifespan_context(app.api):
            assert app.executor.running
            raise _Boom

    assert not app.executor.running


# ---------------------------------------------------------------------------
# setup_logging / rotate_logs_on_startup
# ---------------------------------------------------------------------------


def test_setup_logging_writes_to_configured_dir(tmp_path):
    log_dir = tmp_path / "logs"
    config = AppConfig(
        database_url="sqlite:///:memory:",
        secret_key_dev=Fernet.generate_key().decode(),
        log_to_file=True,
        log_to_console=False,
        log_dir=log_dir,
    )
    try:
        setup_logging(config)
        logging.getLogger("seedpod.test").info("hello from the entrypoint test")
        for handler in logging.root.handlers:
            handler.flush()

        log_file = log_dir / "seedpod.log"
        assert log_file.exists()
        assert "hello from the entrypoint test" in log_file.read_text()
    finally:
        for handler in list(logging.root.handlers):
            handler.close()
        logging.root.handlers = []


def test_setup_logging_is_idempotent(tmp_path):
    config = AppConfig(
        database_url="sqlite:///:memory:",
        secret_key_dev=Fernet.generate_key().decode(),
        log_to_file=True,
        log_to_console=True,
        log_dir=tmp_path / "logs",
    )
    try:
        setup_logging(config)
        first_count = len(logging.root.handlers)
        setup_logging(config)
        second_count = len(logging.root.handlers)
        setup_logging(config)
        third_count = len(logging.root.handlers)

        assert first_count == second_count == third_count
    finally:
        for handler in list(logging.root.handlers):
            handler.close()
        logging.root.handlers = []


def test_setup_logging_console_only_installs_no_file_handler(tmp_path):
    log_dir = tmp_path / "logs"
    config = AppConfig(
        database_url="sqlite:///:memory:",
        secret_key_dev=Fernet.generate_key().decode(),
        log_to_file=False,
        log_to_console=True,
        log_dir=log_dir,
    )
    try:
        setup_logging(config)
        assert not log_dir.exists()
    finally:
        for handler in list(logging.root.handlers):
            handler.close()
        logging.root.handlers = []


def test_rotate_logs_on_startup_rotates_existing_file(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    current = log_dir / "seedpod.log"
    current.write_text("previous run's log lines\n")

    rotate_logs_on_startup(log_dir=log_dir, retention=10)

    assert not current.exists()
    rotated = list(log_dir.glob("seedpod.log.startup-*"))
    assert len(rotated) == 1
    assert rotated[0].read_text() == "previous run's log lines\n"


def test_rotate_logs_on_startup_prunes_to_retention(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    for i in range(12):
        (log_dir / f"seedpod.log.startup-2026010{i % 10}-000000").write_text("old")

    rotate_logs_on_startup(log_dir=log_dir, retention=10)

    remaining = list(log_dir.glob("seedpod.log.startup-*"))
    assert len(remaining) == 10


def test_rotate_logs_on_startup_is_a_noop_with_no_existing_log(tmp_path):
    log_dir = tmp_path / "logs"
    rotate_logs_on_startup(log_dir=log_dir, retention=10)
    assert log_dir.exists()
    assert not list(log_dir.glob("seedpod.log.startup-*"))


# ---------------------------------------------------------------------------
# AppConfig.from_env additive log_* fields
# ---------------------------------------------------------------------------


def test_from_env_reads_additive_log_fields(monkeypatch, tmp_path):
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", f"sqlite:///{tmp_path}/x.db")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", Fernet.generate_key().decode())
    monkeypatch.setenv("SEEDPOD_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("SEEDPOD_LOG_FORMAT", "text")
    monkeypatch.setenv("SEEDPOD_LOG_TO_CONSOLE", "true")
    monkeypatch.setenv("SEEDPOD_LOG_TO_FILE", "false")
    monkeypatch.setenv("SEEDPOD_LOG_DIR", str(tmp_path / "custom-logs"))

    config = AppConfig.from_env()

    assert config.log_level == "DEBUG"
    assert config.log_format == "text"
    assert config.log_to_console is True
    assert config.log_to_file is False
    assert config.log_dir == tmp_path / "custom-logs"


def test_from_env_log_field_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", f"sqlite:///{tmp_path}/x.db")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", Fernet.generate_key().decode())
    for name in (
        "SEEDPOD_LOG_LEVEL",
        "SEEDPOD_LOG_FORMAT",
        "SEEDPOD_LOG_TO_CONSOLE",
        "SEEDPOD_LOG_TO_FILE",
        "SEEDPOD_LOG_DIR",
    ):
        monkeypatch.delenv(name, raising=False)

    config = AppConfig.from_env()

    assert config.log_level == "INFO"
    assert config.log_format == "json"
    assert config.log_to_console is False
    assert config.log_to_file is True
    assert config.log_dir == Path("logs")


# ---------------------------------------------------------------------------
# The startup rotation call -- now in seedpod/__main__.py (DR-0041 Amendment B)
# ---------------------------------------------------------------------------


def test_rotate_logs_call_uses_configured_log_dir_not_a_hardcoded_default():
    """Static regression guard. The call moved out of ``start.py`` and into
    ``seedpod/__main__.py`` (DR-0041 Amendment B: the console script an artifact
    ships had none of start.py's operational behaviour), so this guard follows it.

    ``main()`` cannot be executed to completion in a test -- it ends in
    ``uvicorn.run``, which blocks and binds a real port -- so this pins the actual
    call rather than letting a future edit reintroduce a hardcoded ``"logs"``
    literal that would silently diverge from ``SEEDPOD_LOG_DIR``."""
    tree = ast.parse((_REPO_ROOT / "seedpod" / "__main__.py").read_text())
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "rotate_logs_on_startup"
    ]
    assert len(calls) == 1
    (call,) = calls
    log_dir_kwarg = next(kw for kw in call.keywords if kw.arg == "log_dir")
    assert not (
        isinstance(log_dir_kwarg.value, ast.Constant) and log_dir_kwarg.value.value == "logs"
    ), "log_dir must be threaded from AppConfig.from_env(), not hardcoded"
    assert (
        isinstance(log_dir_kwarg.value, ast.Attribute) and log_dir_kwarg.value.attr == "log_dir"
    ), "expected rotate_logs_on_startup(log_dir=config.log_dir, ...)"


def test_start_py_is_a_shim_and_owns_no_operational_behaviour():
    """DR-0041 Amendment B, pinned. ``start.py`` used to own ``load_dotenv()``, a
    PID-file singleton and the startup rotation, while the ``seedpod`` console
    script owned none of them -- so the packaged path was the less capable one.
    All three now live in ``seedpod/__main__.py`` and must not creep back."""
    tree = ast.parse(_START_PY.read_text())
    names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "check_pid_file" not in names
    assert "load_dotenv" not in names
    assert "rotate_logs_on_startup" not in names
    assert not any(
        isinstance(node, ast.FunctionDef) and node.name == "check_pid_file"
        for node in ast.walk(tree)
    ), "the singleton lives in seedpod/app/singleton.py now, shared by both entry points"


# ---------------------------------------------------------------------------
# single_instance -- the flock-based singleton (DR-0041 Amendment B)
# ---------------------------------------------------------------------------


def test_importing_start_module_has_zero_side_effects(tmp_path):
    """A fresh interpreter, cwd'd to an empty tmp_path: importing ``start.py``
    under any name but ``__main__`` must do nothing at all."""
    script = (
        "import importlib.util\n"
        f"spec = importlib.util.spec_from_file_location('start_check', r'{_START_PY}')\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(module)\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=tmp_path, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "OK"
    assert not (tmp_path / "seedpod.pid").exists()
    assert not (_REPO_ROOT / "seedpod.pid").exists()


def test_single_instance_writes_its_pid_and_releases_on_exit(tmp_path):
    lock = tmp_path / "var" / "seedpod.pid"

    with single_instance(lock) as pid:
        assert pid == os.getpid()
        assert lock.read_text().strip() == str(os.getpid())

    assert not lock.exists()  # released AND tidied


def test_single_instance_refuses_a_second_holder_and_names_the_first(tmp_path):
    """flock is held per open-file-description, so a second acquisition fails even
    from within this process -- which is what makes this testable without a
    subprocess, and is also exactly the guarantee a second `bin/seedpod` needs."""
    lock = tmp_path / "seedpod.pid"

    with single_instance(lock):
        with pytest.raises(AlreadyRunning) as exc_info:
            with single_instance(lock):
                pass

    assert str(os.getpid()) in str(exc_info.value)


def test_single_instance_takes_a_lock_whose_holder_died(tmp_path):
    """The case the old advisory pid file needed explicit stale-cleanup code for.
    The kernel drops an flock when the holder dies however it dies, so a survivor
    file with a dead pid in it is simply takeable -- no cleanup path to get wrong,
    and nothing to race against."""
    lock = tmp_path / "seedpod.pid"
    holder = subprocess.Popen(
        [
            sys.executable, "-c",
            "import sys, time\n"
            f"sys.path.insert(0, {str(_REPO_ROOT)!r})\n"
            "from seedpod.app.singleton import single_instance\n"
            f"ctx = single_instance({str(lock)!r})\n"
            "ctx.__enter__()\n"
            "print('HELD', flush=True)\n"
            "time.sleep(60)\n",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "HELD", holder.stderr.read()
        with pytest.raises(AlreadyRunning):
            with single_instance(lock):
                pass
        holder.kill()  # SIGKILL: no atexit, no signal handler, no chance to tidy
        holder.wait(timeout=10)
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait(timeout=5)

    assert lock.exists(), "the file survives a hard kill -- only the lock is gone"
    with single_instance(lock) as pid:
        assert pid == os.getpid()


def test_deleting_the_lock_file_DOES_defeat_the_lock_and_the_port_check_is_why_that_is_survivable(tmp_path):
    """The honest limit of the lock, pinned so nobody re-derives it the hard way.

    ``rm -f seedpod.pid`` followed by a start still gets through: ``os.open(...,
    O_CREAT)`` after an unlink makes a NEW inode, and an flock on that is
    uncontended -- the first holder's lock is on an inode with no name. There is no
    filesystem-only fix for unlink-then-recreate, which is precisely why
    ``assert_port_available`` exists and is not redundant with the lock.

    (Written after this test failed against a docstring claiming a held flock "does
    not care what the filesystem says". It cares if you replace the file.)"""
    lock = tmp_path / "seedpod.pid"

    with single_instance(lock):
        lock.unlink()  # exactly what the hurried 2026-08-14 restart script did
        with single_instance(lock) as second:  # and it succeeds -- this is the gap
            assert second == os.getpid()

    # What actually stops two servers is the port, which only one can hold.


# ---------------------------------------------------------------------------
# assert_port_available
# ---------------------------------------------------------------------------


def _run_main(tmp_path: Path, **env_overrides) -> subprocess.CompletedProcess:
    """``python -m seedpod`` in a subprocess, with cwd at ``tmp_path`` -- NOT the
    repo root, deliberately: ``main()`` calls ``load_dotenv()``, which searches
    upward from the cwd and would find this repo's real ``.env`` (real tokens,
    real Fernet keys). A test that reads it would both pollute the process
    environment and depend on a file no other test needs."""
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONPATH": str(_REPO_ROOT),
        "SEEDPOD_DATABASE_URL": f"sqlite:///{tmp_path}/never-opened.db",
        "SEEDPOD_SECRET_KEY_DEV": Fernet.generate_key().decode(),
        "SEEDPOD_LOG_DIR": str(tmp_path / "log"),
        "SEEDPOD_LOG_TO_FILE": "false",
        "SEEDPOD_BACKGROUND_TASKS": "false",
        **env_overrides,
    }
    return subprocess.run(
        [sys.executable, "-m", "seedpod"],
        capture_output=True, text=True, cwd=tmp_path, env=env, timeout=60,
    )


def test_main_refuses_a_second_start_with_one_line_not_a_traceback(tmp_path):
    """DR-0041 decision 4's refusal, end to end through the real entry point.

    The guard itself is tested above; what this pins is the SHAPE of the answer.
    ``single_instance`` composes its message carefully -- the holding pid and the
    command to stop it -- and letting the exception propagate would bury that
    sentence under a ``contextlib`` stack trace. The exit code is distinct (75,
    EX_TEMPFAIL) so a script can tell "already running" from "crashed"."""
    from seedpod.__main__ import EXIT_REFUSED_TO_START

    lock = tmp_path / "seedpod.pid"
    with single_instance(lock):  # this test process is the incumbent
        result = _run_main(tmp_path, SEEDPOD_PID_FILE=str(lock))

    assert result.returncode == EXIT_REFUSED_TO_START
    assert "Traceback" not in result.stderr
    assert result.stderr.strip().startswith("seedpod: ")
    assert str(os.getpid()) in result.stderr  # names the pid holding the lock
    assert "stop it first" in result.stderr


def test_main_refuses_a_held_port_with_one_line_not_a_traceback(tmp_path):
    """The other half of the pair, and the one that actually stops two servers:
    the lock cannot see "something else is listening on 8000", and the 2026-08-14
    stale server answered /health perfectly."""
    from seedpod.__main__ import EXIT_REFUSED_TO_START

    with socket.socket() as held:
        held.bind(("127.0.0.1", 0))
        held.listen(1)
        port = held.getsockname()[1]
        result = _run_main(
            tmp_path,
            SEEDPOD_PID_FILE=str(tmp_path / "seedpod.pid"),
            SEEDPOD_API_HOST="127.0.0.1",
            SEEDPOD_API_PORT=str(port),
        )

    assert result.returncode == EXIT_REFUSED_TO_START
    assert "Traceback" not in result.stderr
    assert str(port) in result.stderr
    assert "worse than no server" in result.stderr


def test_assert_port_available_passes_on_a_free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        free_port = probe.getsockname()[1]
    assert_port_available("127.0.0.1", free_port)  # nothing raised


def test_assert_port_available_refuses_a_held_port():
    """The lock answers "is another seedpod running". It cannot answer "is
    ANYTHING listening" -- and the stale server that prompted all this answered
    /health perfectly well, which is what made a restart look successful."""
    with socket.socket() as held:
        held.bind(("127.0.0.1", 0))
        held.listen(1)
        port = held.getsockname()[1]

        with pytest.raises(PortUnavailable) as exc_info:
            assert_port_available("127.0.0.1", port)

    assert str(port) in str(exc_info.value)
