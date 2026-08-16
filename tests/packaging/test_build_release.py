"""``scripts/build_release.py`` -- what ships, and more importantly what does not
(DR-0041 decision 2 + Amendment A).

**The load-bearing test here is the exclusion sweep**, and it is written to walk
the whole staged tree rather than to check a list of known-bad paths. Amendment A
is a rule about the artifact's *shape* -- "only what running requires" -- and a
rule like that decays the moment someone adds an entry to `DIRECTORIES` for a
good reason and nobody notices what came with it. `reference-code/` is the case
that matters: it holds a real `.env`, a real `admin-api-key.txt`, and an embedded
git history, and the cost of it reaching an artifact is not a big tarball.

The build runs as a subprocess, deliberately -- the thing under test is the
command a release job invokes, including its exit codes and its refusals.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "build_release.py"

# Every one of these is either secret-bearing, source-only, or build-only.
# Matched against each path COMPONENT, so a nested `tests/` is caught too.
FORBIDDEN_COMPONENTS = {
    "reference-code",
    ".git",
    "tests",
    "docs",
    "node_modules",
    "src",  # ui/src -- the bundle ships, the sources it was built from do not
    "__pycache__",
    ".venv",
    "db",
    "logs",
    ".pytest_cache",
    ".ruff_cache",
    ".hypothesis",
}
FORBIDDEN_NAMES = {
    ".env",
    "admin-api-key.txt",
    "start.py",
    "seedpod.pid",
    "CLAUDE.md",
    # Not an Amendment A exclusion -- a trap. pyenv reads `.python-version` too,
    # and on a host where `uv` is a pyenv shim, shipping one makes `uv` itself
    # unrunnable ("pyenv: uv: command not found"). The interpreter pin lives in
    # the install command. Asserted here because re-adding it is the obvious
    # thing to do and it fails in a way that looks like a broken uv install.
    ".python-version",
}


def _fake_ui(tmp_path: Path) -> Path:
    """A stand-in bundle, so the test never depends on whether someone has run
    `npm run build` in this checkout."""
    ui = tmp_path / "fake-ui"
    (ui / "assets").mkdir(parents=True)
    (ui / "index.html").write_text("<!doctype html><title>seedpod</title>", encoding="utf-8")
    (ui / "assets" / "index.js").write_text("console.log('hi')", encoding="utf-8")
    return ui


def _build(tmp_path: Path, *extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--out",
            str(tmp_path / "out"),
            "--ui-dir",
            str(_fake_ui(tmp_path)),
            # The checkout under test may or may not be clean; the build's own
            # dirty-tree refusal is not what these tests are about.
            "--allow-dirty",
            *extra,
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )


def _stage(tmp_path: Path) -> Path:
    result = _build(tmp_path, "--stage-only")
    assert result.returncode == 0, result.stderr
    staged = [p for p in (tmp_path / "out").iterdir() if p.is_dir()]
    assert len(staged) == 1, staged
    return staged[0]


def test_the_artifact_carries_what_running_requires(tmp_path):
    staged = _stage(tmp_path)
    for required in (
        "pyproject.toml",
        "uv.lock",
        "README.md",
        "MANIFEST.json",
        "seedpod/__main__.py",
        "seedpod/app/release.py",
        "config/deployment-rules.yml",
        "bin/seedpod",
        "bin/_seedpod_env.sh",
        "ui/dist/index.html",
    ):
        assert (staged / required).exists(), f"artifact is missing {required}"


def test_the_artifact_carries_nothing_else(tmp_path):
    """The sweep. Walks every staged path rather than checking a known list, so
    a future addition to the copy set cannot smuggle something in unnoticed."""
    staged = _stage(tmp_path)
    offenders = []
    for path in staged.rglob("*"):
        parts = path.relative_to(staged).parts
        if set(parts) & FORBIDDEN_COMPONENTS or path.name in FORBIDDEN_NAMES:
            offenders.append(path.relative_to(staged).as_posix())
        if path.suffix in {".pyc", ".pyo"}:
            offenders.append(path.relative_to(staged).as_posix())
    assert offenders == [], f"artifact contains files Amendment A excludes: {offenders}"


def test_the_manifest_stamps_provenance_the_launcher_can_read(tmp_path):
    """Amendment A's whole point: with no `.git` in the artifact, this file is
    the only thing that can answer "which code is this?"."""
    import hashlib

    staged = _stage(tmp_path)
    manifest = json.loads((staged / "MANIFEST.json").read_text(encoding="utf-8"))

    assert manifest["version"] == "2.0.0a0"
    assert manifest["release"].startswith("2.0.0a0+")
    assert len(manifest["git_sha"]) == 40
    assert manifest["release"].split("+")[1].startswith(manifest["git_sha"][:8])
    assert manifest["built_at"].startswith("20")
    assert manifest["requires_python"] == ">=3.11"
    assert manifest["ui_bundle"] is True
    assert isinstance(manifest["git_dirty"], bool)

    lock = hashlib.sha256((staged / "uv.lock").read_bytes()).hexdigest()
    assert manifest["uv_lock_sha256"] == lock


def test_the_stamped_manifest_is_what_describe_release_reads(tmp_path):
    """Both halves of Amendment A in one assertion: the build writes it, and the
    module the launcher calls reads it back. They were written together and this
    is the seam where they could silently drift apart."""
    from seedpod.app.release import describe_release

    staged = _stage(tmp_path)
    line = describe_release(staged)
    assert "unstamped" not in line
    assert "WARNING" not in line  # the staged uv.lock matches its own stamp
    manifest = json.loads((staged / "MANIFEST.json").read_text(encoding="utf-8"))
    assert manifest["release"] in line


def test_launchers_arrive_executable(tmp_path):
    """A +x bit lost in transit turns install into a debugging session."""
    staged = _stage(tmp_path)
    for name in ("seedpod", "seedpod-bootstrap", "seedpodctl"):
        assert (staged / "bin" / name).stat().st_mode & 0o111, f"bin/{name} is not executable"


def test_it_refuses_to_build_without_a_ui_bundle_and_names_the_command(tmp_path):
    """DR-0041 decision 3 promises node is not needed on the appliance. That is
    only true if the bundle ships, so a missing one is a refusal -- discovering
    it during install is the failure being prevented."""
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--out",
            str(tmp_path / "out"),
            "--ui-dir",
            str(tmp_path / "does-not-exist"),
            "--allow-dirty",
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    assert result.returncode == 2
    assert "npm run build" in result.stderr
    assert "Traceback" not in result.stderr  # a refusal, not a crash


def test_it_refuses_a_bundle_with_a_developer_api_url_compiled_in(tmp_path):
    """The regression test for the one defect that reached a real appliance.

    `ui/.env` is gitignored and says VITE_API_URL=http://localhost:8000 on a dev
    machine; `npm run build` inherited it and the artifact shipped a bundle that
    sent every request to localhost. That works on the server host and fails from
    every other client -- login "failed with that key" from a laptop, with nothing
    in the server log, because the request never arrived.

    The build must refuse it rather than let it install."""
    ui = tmp_path / "contaminated-ui"
    (ui / "assets").mkdir(parents=True)
    (ui / "index.html").write_text("<!doctype html>", encoding="utf-8")
    (ui / "assets" / "index.js").write_text(
        'const API_BASE_URL="http://localhost:8000";fetch(API_BASE_URL+"/api/clusters")',
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--out", str(tmp_path / "out"),
         "--ui-dir", str(ui), "--allow-dirty", "--stage-only"],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )

    assert result.returncode == 2
    assert "loopback" in result.stderr
    assert "index.js" in result.stderr
    assert "VITE_API_URL" in result.stderr  # names the likely cause
    assert "Traceback" not in result.stderr
    assert not list((tmp_path / "out").glob("*")), "nothing should have been staged"


def test_the_repo_bundle_is_built_same_origin(tmp_path):
    """The other half: ui/.env.production clears VITE_API_URL, so a real
    `npm run build` in this repo produces a bundle the guard accepts. Skipped
    when no bundle has been built -- ui/dist is gitignored."""
    bundle = REPO_ROOT / "ui" / "dist"
    if not (bundle / "index.html").is_file():
        pytest.skip("ui/dist not built in this checkout")

    for asset in bundle.rglob("*.js"):
        text = asset.read_text(encoding="utf-8", errors="ignore")
        assert "localhost:" not in text, f"{asset.name} has a loopback API base compiled in"


def test_the_tarball_unpacks_to_a_single_release_directory(tmp_path):
    """Install is `tar -xzf … -C ~/seedpod/releases/`. A tarball that unpacked
    loose files would scatter them across every installed release."""
    result = _build(tmp_path)
    assert result.returncode == 0, result.stderr

    tarballs = list((tmp_path / "out").glob("seedpod-*.tar.gz"))
    assert len(tarballs) == 1
    with tarfile.open(tarballs[0]) as tar:
        names = tar.getnames()
    roots = {Path(n).parts[0] for n in names}
    assert len(roots) == 1
    release = roots.pop()
    assert release.startswith("2.0.0a0+")
    assert tarballs[0].name == f"seedpod-{release}.tar.gz"
    assert f"{release}/MANIFEST.json" in names
    assert f"{release}/bin/seedpod" in names

    # And the staging directory is cleaned up -- dist/ holds the deliverable.
    assert not (tmp_path / "out" / release).exists()
