#!/usr/bin/env python3
"""Assemble a release artifact (DR-0041 decision 2 + Amendment A).

    python scripts/build_release.py                # -> dist/seedpod-<release>.tar.gz
    python scripts/build_release.py --stage-only   # leave the tree, skip the tarball
    python scripts/build_release.py --ui-dir path  # point at a bundle built elsewhere (CI)

**What goes in is defined by what running requires, and nothing else.** Amendment
A: the resolved runtime, ``config/``, ``ui/dist/``, ``bin/`` and an ops README --
**not** ``tests/``, ``docs/``, ``ui/src/``, ``ui/node_modules/``, ``.git/``, and
never ``reference-code/``. Copying a source checkout to the appliance is the
mechanism DR-0041 exists to replace; an artifact that quietly re-becomes a
checkout would replace nothing.

**On "the resolved ``.venv``" -- see DR-0041 erratum E1.** Amendment A listed
``.venv`` among the artifact's contents. A virtualenv is not portable: its
console scripts carry the absolute path they were created at, its wheels are
built for one platform and one interpreter ABI, and the CI job that produces this
artifact runs on ubuntu while the appliance is macOS/arm64. So the artifact
carries ``uv.lock`` and the appliance materialises the venv with
``uv sync --locked`` -- which is decision 1's own install command, resolving the
two halves in decision 1's favour. The lock IS the resolved runtime; the ``.venv``
is just its local instantiation.

**Why this refuses to build without ``ui/dist``.** DR-0041 decision 3 makes the
appliance serve its own SPA, and states node is not needed there at all. That
only holds if the bundle ships. A silently UI-less artifact would push the npm
toolchain back onto the appliance at the worst possible moment -- while
installing.

This script is build tooling and is deliberately **not** part of the ``seedpod``
package: anything under ``seedpod/`` ships, and a build script has no business on
an appliance.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tarfile
import tomllib
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Copied verbatim into the artifact root.
#
# **There is deliberately no `.python-version`.** It was the obvious way to pin
# the interpreter for decision 1, and it broke `uv` on the first host it met:
# pyenv reads `.python-version` as well, and where `uv` is installed as a pyenv
# shim (this laptop), a version pyenv does not have makes `pyenv: uv: command
# not found` -- the file pins the interpreter by disabling the tool that
# installs it. The pin lives in the install command instead
# (`uv sync --locked --python 3.11`, README), which no other tool reads.
FILES = ("pyproject.toml", "uv.lock", "README.md")

# (source, destination-inside-artifact). `ui/dist` is re-rooted so the artifact
# keeps the `ui/dist` shape `SEEDPOD_UI_DIR` and the launcher already expect.
DIRECTORIES = (
    ("seedpod", "seedpod"),
    ("config", "config"),
    ("bin", "bin"),
)

# Never copied out of any directory above.
PRUNE = shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo", ".DS_Store")


class BuildError(RuntimeError):
    """A refusal with a fixable cause -- printed without a traceback."""


def git(*args: str) -> str:
    """Best-effort ``git``. Returns ``""`` rather than raising: the build must
    still work from an exported tree with no git available, it just stamps
    ``unknown`` and says so."""
    try:
        out = subprocess.run(
            ("git", *args), cwd=REPO_ROOT, capture_output=True, text=True, check=False
        )
    except OSError:
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def project_version() -> str:
    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return str(data["project"]["version"])


def requires_python() -> str:
    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return str(data["project"].get("requires-python", ""))


def resolve_ui_dir(explicit: str | None) -> Path:
    """The bundle, or a refusal naming the exact command that produces one."""
    ui_dir = Path(explicit).resolve() if explicit else REPO_ROOT / "ui" / "dist"
    if not (ui_dir / "index.html").is_file():
        raise BuildError(
            f"no built SPA at {ui_dir} (expected index.html).\n"
            "DR-0041 decision 3 ships the bundle in the artifact so the appliance needs no node.\n"
            "Build it first:  cd ui && npm ci && npm run build\n"
            "(or pass --ui-dir at a bundle built elsewhere, e.g. CI's)"
        )
    assert_same_origin_bundle(ui_dir)
    return ui_dir


def assert_same_origin_bundle(ui_dir: Path) -> None:
    """Refuse a bundle that has a developer's API URL compiled into it.

    **This shipped once, and the failure is worth stating exactly.** `ui/.env` is
    gitignored and on a dev machine reads `VITE_API_URL=http://localhost:8000`
    for the vite dev-server workflow. `npm run build` picked it up, and the
    2026-08-15 artifact went to the appliance with `http://localhost:8000`
    compiled in. It then behaves perfectly *on the server host* -- localhost is
    the server there -- and fails for every other client, which is the worst
    possible shape for a bug: it works for whoever tests it locally and the
    request never even arrives from anywhere else. The symptom was a login that
    "failed with that key" from a laptop, with nothing in the server log.

    `ui/.env.production` now clears the variable, so this should be unreachable.
    It is checked anyway, because the thing being defended against is a build
    picking up ambient developer configuration -- and the next way that happens
    will not be the way that already did.

    `localhost`/`127.0.0.1` are the right things to look for and carry no false
    positives: a production bundle has no legitimate reason to name the loopback
    interface, whereas a general "absolute URL" check would trip over every
    documentation link in the UI.
    """
    needles = ("localhost:", "127.0.0.1:")
    offenders = []
    for asset in sorted(ui_dir.rglob("*.js")) + sorted(ui_dir.rglob("*.html")):
        try:
            text = asset.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        found = [n for n in needles if n in text]
        if found:
            offenders.append(f"{asset.relative_to(ui_dir)} ({', '.join(found)})")
    if offenders:
        raise BuildError(
            "the built SPA has a loopback address compiled into it: "
            + "; ".join(offenders)
            + "\nThat bundle works only on the server host -- every other client sends its"
            "\nrequests to its own machine, and the appliance never sees them."
            "\nAlmost certainly `ui/.env` leaking VITE_API_URL into the build."
            "\nFix:  cd ui && npm run build   (mode=production loads .env.production,"
            "\nwhich clears VITE_API_URL; DR-0041 decision 3 serves the SPA same-origin)"
        )


def build_release(
    out_dir: Path,
    *,
    ui_dir_arg: str | None = None,
    allow_dirty: bool = False,
    archive: bool = True,
) -> tuple[Path, dict]:
    """Stage the artifact under ``out_dir`` and (by default) tar it up. Returns
    the path a caller cares about -- the tarball, or the staged directory with
    ``--stage-only`` -- and the manifest that was stamped."""
    ui_dir = resolve_ui_dir(ui_dir_arg)

    sha = git("rev-parse", "HEAD") or "unknown"
    branch = git("rev-parse", "--abbrev-ref", "HEAD") or "unknown"
    dirty = bool(git("status", "--porcelain"))
    if dirty and not allow_dirty:
        raise BuildError(
            "working tree is dirty; a stamped sha would name a commit that is not what you built.\n"
            "Commit, stash, or pass --allow-dirty (which stamps the release id `.dirty` so two "
            "artifacts cannot collide)."
        )

    version = project_version()
    short = sha[:8] if sha != "unknown" else "nogit"
    release = f"{version}+{short}{'.dirty' if dirty else ''}"

    staged = out_dir / release
    if staged.exists():
        shutil.rmtree(staged)
    staged.mkdir(parents=True)

    for name in FILES:
        source = REPO_ROOT / name
        if source.is_file():
            shutil.copy2(source, staged / name)

    for source_name, dest_name in DIRECTORIES:
        shutil.copytree(REPO_ROOT / source_name, staged / dest_name, ignore=PRUNE)

    shutil.copytree(ui_dir, staged / "ui" / "dist", ignore=PRUNE)

    # Executability is part of the artifact: a launcher that arrives without its
    # +x bit turns install into a debugging session.
    for script in (staged / "bin").iterdir():
        script.chmod(0o755)

    lock = staged / "uv.lock"
    manifest = {
        "release": release,
        "version": version,
        "git_sha": sha,
        "git_branch": branch,
        "git_dirty": dirty,
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "uv_lock_sha256": hashlib.sha256(lock.read_bytes()).hexdigest() if lock.is_file() else "",
        "requires_python": requires_python(),
        "ui_bundle": True,
    }
    (staged / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    if not archive:
        return staged, manifest

    tarball = out_dir / f"seedpod-{release}.tar.gz"
    with tarfile.open(tarball, "w:gz") as tar:
        tar.add(staged, arcname=release)
    shutil.rmtree(staged)
    return tarball, manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Assemble a seedpod release artifact.")
    parser.add_argument("--out", default=str(REPO_ROOT / "dist"), help="output directory (dist/)")
    parser.add_argument("--ui-dir", default=None, help="built SPA to ship (default ui/dist)")
    parser.add_argument("--allow-dirty", action="store_true", help="build from a dirty tree")
    parser.add_argument(
        "--stage-only", action="store_true", help="leave the staged tree, skip the tarball"
    )
    args = parser.parse_args(argv)

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        path, manifest = build_release(
            out_dir,
            ui_dir_arg=args.ui_dir,
            allow_dirty=args.allow_dirty,
            archive=not args.stage_only,
        )
    except BuildError as exc:
        print(f"build-release: {exc}", file=sys.stderr)
        return 2

    print(f"{manifest['release']}  sha {manifest['git_sha'][:12]}  built {manifest['built_at']}")
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
