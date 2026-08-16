"""``bin/seedpod``, ``bin/seedpod-bootstrap``, ``bin/seedpodctl`` -- the launchers
(DR-0041 decision 4, decision 2's layout, Amendment C's log directory).

These replace the ad-hoc scripts that lived in ``/tmp`` on minimax, and the
reason they get real tests is that the scripts they replace are exactly what
caused the 2026-08-14 incident: a restart script written in a hurry, doing
``rm -f seedpod.pid``, whose health check then went green against the stale
server it had failed to kill. A launcher is operational code. It gets the same
treatment as the rest.

**How this tests a shell script honestly.** A stub stands in for
``.venv/bin/python`` and records its argv and the environment it was handed.
Nothing is mocked and no debug affordance was added to the launchers to make
them testable -- the assertions are about the process the launcher actually
execs, which is the whole of what a launcher does.

The one thing NOT tested here is the port/singleton guard: that lives in
``seedpod/__main__.py`` (Amendment B) and is covered in
``tests/app/test_entrypoint.py``. It is deliberately not in the shell, because a
guard in a wrapper is only as strong as the next script that forgets the wrapper.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BIN = REPO_ROOT / "bin"

STUB = """#!/bin/sh
{
    printf 'ARGV %s\\n' "$*"
    env
    printf 'END\\n'
} >> "$STUB_LOG"
"""

# A .env that is realistic in the one way that matters: it carries secrets AND a
# cwd-relative path variable, which is what the launcher must override.
DOT_ENV = """SEEDPOD_SECRET_KEY_DEV=a-dev-fernet-key
DIGITALOCEAN_TOKEN=dop_v1_not_a_real_token
SEEDPOD_ENABLED_PROVIDERS=tart
SEEDPOD_API_PORT=8000
SEEDPOD_CONFIG_DIR=config
SEEDPOD_LOG_DIR=logs
"""


@pytest.fixture
def release(tmp_path):
    """A release root in decision 2's layout, **including the symlink**:
    ``<home>/current -> <home>/releases/<release>``, beside ``<home>/var``.

    The symlink is not incidental scenery. A first version of this fixture used a
    real directory for ``current`` and passed against a launcher that resolved
    its state directory as ``$RELEASE_ROOT/../var`` -- which the kernel resolves
    physically, landing in ``releases/`` and refusing to start on a correctly
    installed release. Installing a real artifact by hand found it in seconds.
    A fixture that is one indirection simpler than production tests a layout
    nobody runs.
    """
    home = tmp_path / "seedpod"
    real = home / "releases" / "2.0.0a0+testtest"
    root = home / "current"
    real.mkdir(parents=True)
    root.symlink_to(real)
    (root / "bin").mkdir(parents=True)
    for script in BIN.iterdir():
        shutil.copy2(script, root / "bin" / script.name)

    venv_bin = root / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    stub = venv_bin / "python"
    stub.write_text(STUB, encoding="utf-8")
    stub.chmod(0o755)

    (root / "config").mkdir()
    (root / "ui" / "dist").mkdir(parents=True)
    (root / "ui" / "dist" / "index.html").write_text("<!doctype html>", encoding="utf-8")

    (home / "var").mkdir()
    (home / "var" / ".env").write_text(DOT_ENV, encoding="utf-8")
    return root


def run(script: Path, *args: str, log: Path | None = None, **overrides):
    """Invoke a launcher with a deliberately bare environment -- inheriting the
    developer's own SEEDPOD_* variables would hide precisely the bugs this is
    looking for."""
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(script.parents[2]),
        "STUB_LOG": str(log) if log else "/dev/null",
        **overrides,
    }
    return subprocess.run([str(script), *args], capture_output=True, text=True, env=env)


def calls(log: Path) -> list[dict]:
    """Each stub invocation as ``{"argv": str, ...environment}``."""
    out = []
    current: dict | None = None
    for line in log.read_text(encoding="utf-8").splitlines():
        if line.startswith("ARGV "):
            current = {"argv": line[5:]}
        elif line == "END":
            out.append(current)
            current = None
        elif current is not None and "=" in line:
            key, _, value = line.partition("=")
            current[key] = value
    return out


def test_the_server_launcher_execs_the_packaged_entry_point(release, tmp_path):
    log = tmp_path / "calls.log"
    result = run(release / "bin" / "seedpod", log=log)
    assert result.returncode == 0, result.stderr

    invocations = calls(log)
    assert [c["argv"] for c in invocations] == [
        f"-m seedpod.app.release {release}",  # the banner, before anything else
        "-m seedpod",
    ]


def test_every_path_the_server_gets_is_absolute_and_points_where_the_layout_says(release):
    """AppConfig's config_dir, log_dir and snapshot_storage_path all default to
    cwd-relative values. A release root is not a directory anyone cd's into, so
    the launcher exporting absolutes is the thing standing between the appliance
    and a database created wherever the operator was standing."""
    log = release.parent / "calls.log"
    run(release / "bin" / "seedpod", log=log)
    env = calls(log)[-1]
    var = release.parent / "var"

    assert env["SEEDPOD_CONFIG_DIR"] == str(release / "config")
    assert env["SEEDPOD_LOG_DIR"] == str(var / "log")
    assert env["SEEDPOD_PID_FILE"] == str(var / "seedpod.pid")
    assert env["SEEDPOD_SNAPSHOT_STORAGE_PATH"] == str(var / "data" / "snapshots")
    assert env["SEEDPOD_DATABASE_URL"] == f"sqlite:///{var}/db/seedpod.db"
    for key, value in env.items():
        if key.startswith("SEEDPOD_") and key.endswith(("_DIR", "_PATH", "_FILE")):
            assert value.startswith("/"), f"{key} is not absolute: {value}"


def test_state_lives_under_var_and_never_inside_the_release(release):
    """Decision 2's boundary, asserted as a boundary: nothing an upgrade would
    replace holds state. `current` gets swapped on every upgrade -- anything
    written under it is gone."""
    log = release.parent / "calls.log"
    run(release / "bin" / "seedpod", log=log)
    env = calls(log)[-1]
    var = str(release.parent / "var")

    for key in (
        "SEEDPOD_LOG_DIR",
        "SEEDPOD_PID_FILE",
        "SEEDPOD_SNAPSHOT_STORAGE_PATH",
        "SEEDPOD_DATABASE_URL",
    ):
        assert var in env[key], f"{key} does not live under var/: {env[key]}"
        assert str(release) not in env[key], f"{key} points inside the release root"


def test_state_resolves_through_the_current_symlink_not_around_it(release):
    """The regression test for the `..` bug. `current` is a symlink into
    `releases/`, so any path built with `..` silently means `releases/..`. State
    must resolve to `<home>/var`, and must not be looked for -- or created --
    under `releases/`."""
    home = release.parent
    log = home / "calls.log"
    result = run(release / "bin" / "seedpod", log=log)
    assert result.returncode == 0, result.stderr

    env = calls(log)[-1]
    assert env["SEEDPOD_LOG_DIR"] == str(home / "var" / "log")
    assert "releases" not in env["SEEDPOD_LOG_DIR"]
    assert not (home / "releases" / "var").exists()

    # And the release root stays the symlink, so an upgrade that re-points
    # `current` moves the running config with it.
    assert env["SEEDPOD_CONFIG_DIR"] == str(release / "config")
    assert "releases" not in env["SEEDPOD_CONFIG_DIR"]


def test_the_launcher_creates_the_state_directories_it_points_at(release):
    run(release / "bin" / "seedpod", log=release.parent / "calls.log")
    var = release.parent / "var"
    assert (var / "db").is_dir()
    assert (var / "log").is_dir()
    assert (var / "data" / "snapshots").is_dir()


def test_dot_env_supplies_secrets_but_the_launcher_owns_the_paths(release):
    """The precedence rule, and the reason for it. `var/.env` here sets
    `SEEDPOD_CONFIG_DIR=config` and `SEEDPOD_LOG_DIR=logs` -- both cwd-relative,
    both plausible leftovers from a repo checkout. Secrets come through; the
    stale relative paths do not."""
    log = release.parent / "calls.log"
    run(release / "bin" / "seedpod", log=log)
    env = calls(log)[-1]

    assert env["SEEDPOD_SECRET_KEY_DEV"] == "a-dev-fernet-key"
    assert env["DIGITALOCEAN_TOKEN"] == "dop_v1_not_a_real_token"
    assert env["SEEDPOD_ENABLED_PROVIDERS"] == "tart"
    assert env["SEEDPOD_API_PORT"] == "8000"

    assert env["SEEDPOD_CONFIG_DIR"] != "config"
    assert env["SEEDPOD_LOG_DIR"] != "logs"


def test_an_absolute_config_dir_from_the_operator_wins(release, tmp_path):
    """DR-0041 erratum E3. Config stopped being a property of the release when the
    real deployment configuration moved to its own private repo -- the `config/`
    an artifact carries is a generic default. So an absolute SEEDPOD_CONFIG_DIR
    survives, where every other path variable is overridden."""
    theirs = tmp_path / "seedpod-config"
    theirs.mkdir()
    log = release.parent / "calls.log"
    run(release / "bin" / "seedpod", log=log, SEEDPOD_CONFIG_DIR=str(theirs))
    env = calls(log)[-1]

    assert env["SEEDPOD_CONFIG_DIR"] == str(theirs)
    # The rest of the layout is unmoved by it.
    assert env["SEEDPOD_LOG_DIR"] == str(release.parent / "var" / "log")


def test_an_absolute_config_dir_in_var_dot_env_wins(release, tmp_path):
    """The mechanism erratum E3 actually documents, and the one an appliance uses:
    the operator records the path once in `var/.env` rather than exporting it in
    every shell. It has to beat the release default while the relative paths in
    the same file still lose."""
    theirs = tmp_path / "seedpod-config"
    theirs.mkdir()
    env_file = release.parent / "var" / ".env"
    env_file.write_text(f"{DOT_ENV}SEEDPOD_CONFIG_DIR={theirs}\n", encoding="utf-8")

    log = release.parent / "calls.log"
    run(release / "bin" / "seedpod", log=log)
    env = calls(log)[-1]

    assert env["SEEDPOD_CONFIG_DIR"] == str(theirs)
    assert env["SEEDPOD_LOG_DIR"] == str(release.parent / "var" / "log")


def test_a_relative_config_dir_is_still_discarded(release):
    """The exception is for absolute paths only. `SEEDPOD_CONFIG_DIR=config` is
    the stale-checkout leftover the absolutes exist to prevent -- it is the value
    `var/.env` carries in this fixture, and it must not survive just because
    operators may now own this variable."""
    log = release.parent / "calls.log"
    run(release / "bin" / "seedpod", log=log, SEEDPOD_CONFIG_DIR="config")
    env = calls(log)[-1]

    assert env["SEEDPOD_CONFIG_DIR"] == str(release / "config")


def test_a_config_dir_that_does_not_exist_refuses_instead_of_falling_back(release, tmp_path):
    """An operator who set this and mistyped it wants to hear so. Falling back to
    the release's own config would start seedpod serving the generic example
    profiles under the name of a real deployment -- the silent-wrong-config
    failure, which is worse than not starting."""
    missing = tmp_path / "not-there"
    result = run(release / "bin" / "seedpod", SEEDPOD_CONFIG_DIR=str(missing))

    assert result.returncode == 78, result.stderr
    assert str(missing) in result.stderr
    assert "SEEDPOD_CONFIG_DIR" in result.stderr


def test_the_spa_is_pointed_at_only_when_a_bundle_is_really_there(release):
    """Decision 3: unset mounts nothing, which keeps the vite workflow working.
    Pointing at an empty directory instead would make `mount_spa` refuse at
    startup -- a bundle-less release would fail to boot rather than serve the API
    alone."""
    log = release.parent / "calls.log"
    run(release / "bin" / "seedpod", log=log)
    assert calls(log)[-1]["SEEDPOD_UI_DIR"] == str(release / "ui" / "dist")

    (release / "ui" / "dist" / "index.html").unlink()
    log2 = release.parent / "calls2.log"
    run(release / "bin" / "seedpod", log=log2)
    assert "SEEDPOD_UI_DIR" not in calls(log2)[-1]


def test_version_prints_the_banner_and_does_not_start_a_server(release):
    log = release.parent / "calls.log"
    result = run(release / "bin" / "seedpod", "--version", log=log)
    assert result.returncode == 0
    assert [c["argv"] for c in calls(log)] == [f"-m seedpod.app.release {release}"]


def test_a_missing_runtime_names_the_command_that_creates_it(release):
    """Decision 1: the venv is materialised by `uv sync --locked`, not shipped
    (erratum E1). The first thing anyone does with a fresh artifact is run the
    launcher before installing, so this message is on the common path."""
    shutil.rmtree(release / ".venv")
    result = run(release / "bin" / "seedpod")
    assert result.returncode == 78
    assert "uv sync --locked" in result.stderr


def test_a_missing_state_directory_is_a_refusal_not_a_guess(release):
    """Guessing would create state somewhere nobody expects -- and state is the
    one thing an upgrade must never touch."""
    shutil.rmtree(release.parent / "var")
    result = run(release / "bin" / "seedpod")
    assert result.returncode == 78
    assert "mkdir -p" in result.stderr
    assert "var" in result.stderr


def test_seedpod_var_can_be_pointed_elsewhere(release, tmp_path):
    elsewhere = tmp_path / "state-on-another-disk"
    elsewhere.mkdir()
    log = tmp_path / "calls.log"
    run(release / "bin" / "seedpod", log=log, SEEDPOD_VAR=str(elsewhere))
    env = calls(log)[-1]
    assert env["SEEDPOD_LOG_DIR"] == str(elsewhere / "log")


def test_the_bootstrap_launcher_execs_the_offline_cli_with_its_arguments(release):
    """It reaches the database and the secret store directly, so it must resolve
    the SAME config_dir and database_url the server does -- a divergence here
    would not surface until the data disagreed."""
    log = release.parent / "calls.log"
    result = run(
        release / "bin" / "seedpod-bootstrap",
        "seed-secrets",
        "development",
        "--profile",
        "exampleco-dev-stack-nodns",
        log=log,
    )
    assert result.returncode == 0, result.stderr

    (call,) = calls(log)
    assert call["argv"] == (
        "-m seedpod.bootstrap seed-secrets development --profile exampleco-dev-stack-nodns"
    )
    assert call["SEEDPOD_CONFIG_DIR"] == str(release / "config")
    assert call["SEEDPOD_DATABASE_URL"] == f"sqlite:///{release.parent / 'var'}/db/seedpod.db"


def test_the_ctl_launcher_execs_the_http_client_with_its_arguments(release):
    log = release.parent / "calls.log"
    result = run(release / "bin" / "seedpodctl", "clusters", "list", log=log)
    assert result.returncode == 0, result.stderr

    (call,) = calls(log)
    assert call["argv"] == "-m seedpod.ctl.cli clusters list"


def test_no_launcher_prints_a_traceback_when_it_refuses(release):
    """Every refusal in the shared env script is a message and an exit code. A
    traceback from a launcher tells an operator nothing they can act on."""
    shutil.rmtree(release / ".venv")
    for name in ("seedpod", "seedpod-bootstrap", "seedpodctl"):
        result = run(release / "bin" / name)
        assert result.returncode == 78
        assert "Traceback" not in result.stderr
        assert result.stderr.startswith("seedpod: ")
