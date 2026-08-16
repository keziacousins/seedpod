"""``MANIFEST.json`` -- what a built release says about itself (DR-0041
decision 4, as corrected by Amendment A).

Decision 4 required the launcher to "print the resolved release version and git
sha at startup, so 'which code is this?' is answerable from the terminal that
started it". That line was written assuming a git tree to ask. **Amendment A
removed the git tree**: the artifact carries no ``.git``, no ``tests/``, no
``docs/``, so ``git rev-parse`` on the appliance would print nothing at all --
and a launcher that cannot answer "which code is this?" is precisely the failure
the 2026-08-14 stale server made unforgettable, where an 18-hour-old process
answered ``/health`` perfectly and made a restart *look* successful.

So the build **stamps** the answer at assembly time and this module reads it
back. ``scripts/build_release.py`` writes ``MANIFEST.json`` at the release root;
``bin/seedpod`` prints ``describe_release()`` before it execs the server.

**Nothing here raises.** A missing, truncated or hand-edited manifest returns a
sentence saying so rather than an exception -- the whole point is a launcher that
always answers, and refusing to start because a *provenance file* is unreadable
would trade the real failure for a sillier one. "unknown" is a legitimate answer;
a traceback in place of a banner is not.

**The one active check.** If ``uv.lock`` sits beside the manifest and its hash
does not match the stamped one, the description says so. A release root is a
mutable directory on someone's laptop; ``uv sync`` against an edited lock is how
an appliance quietly stops matching what CI resolved (decision 1's whole reason
for pinning the lock), and it is otherwise invisible.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

__all__ = [
    "MANIFEST_NAME",
    "ReleaseManifest",
    "default_release_root",
    "describe_release",
    "read_manifest",
]

MANIFEST_NAME = "MANIFEST.json"


@dataclass(frozen=True)
class ReleaseManifest:
    """The stamped fields. All optional-with-defaults: a manifest from an older
    build must still describe itself rather than fail to parse, since the
    launcher that reads it is the thing you reach for when something is already
    wrong."""

    release: str = "unknown"
    version: str = "unknown"
    git_sha: str = "unknown"
    git_branch: str = "unknown"
    git_dirty: bool = False
    built_at: str = "unknown"
    uv_lock_sha256: str = ""
    requires_python: str = ""
    ui_bundle: bool = False

    @classmethod
    def from_mapping(cls, data: dict) -> ReleaseManifest:
        known = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in data.items() if k in known})


def default_release_root() -> Path:
    """The directory ``seedpod/`` lives in -- correct for a dev checkout and for
    an appliance, where ``uv sync`` installs the project from the release root.

    The launchers pass the root explicitly (they know it: it is ``bin/``'s
    parent), so this is the fallback for ``python -m seedpod.app.release`` run by
    hand, not the path the appliance depends on.
    """
    return Path(__file__).resolve().parents[2]


def read_manifest(root: Path) -> ReleaseManifest | None:
    """``None`` when there is no readable manifest -- an unstamped tree, which is
    what every dev checkout is."""
    path = Path(root) / MANIFEST_NAME
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    try:
        return ReleaseManifest.from_mapping(data)
    except TypeError:
        return None


def sha256_of(path: Path) -> str:
    """Hex digest of a file, or ``""`` if it cannot be read. Used for
    ``uv.lock`` at both ends: stamped by the build, re-checked here."""
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return ""


def describe_release(root: Path | None = None) -> str:
    """One line, for a terminal. Never raises."""
    root = Path(root) if root is not None else default_release_root()
    manifest = read_manifest(root)
    if manifest is None:
        return f"seedpod: unstamped tree at {root} (no {MANIFEST_NAME}) -- not a built release"

    parts = [f"seedpod {manifest.release}"]
    sha = manifest.git_sha[:12] if manifest.git_sha != "unknown" else "unknown"
    branch = f" on {manifest.git_branch}" if manifest.git_branch != "unknown" else ""
    dirty = " +dirty" if manifest.git_dirty else ""
    parts.append(f"sha {sha}{branch}{dirty}")
    parts.append(f"built {manifest.built_at}")
    parts.append("ui bundle" if manifest.ui_bundle else "NO ui bundle")
    line = " | ".join(parts)

    lock = root / "uv.lock"
    if manifest.uv_lock_sha256 and lock.exists():
        actual = sha256_of(lock)
        if actual and actual != manifest.uv_lock_sha256:
            line += "\n  WARNING: uv.lock differs from the hash stamped at build time."
            line += " `uv sync --locked` here resolves something CI never saw."
    return line


def main(argv: list[str] | None = None) -> int:
    """``python -m seedpod.app.release [release_root]`` -- how ``bin/seedpod``
    prints the banner. The launcher is ``sh``, and ``sh`` has no JSON parser it
    can count on (``jq`` is not installed on a stock macOS); the interpreter it
    is about to exec anyway does.
    """
    import sys

    args = list(sys.argv[1:] if argv is None else argv)
    print(describe_release(Path(args[0]) if args else None))
    return 0


if __name__ == "__main__":  # pragma: no cover -- exercised as a subprocess
    raise SystemExit(main())
