"""``seedpod/app/release.py`` -- reading a stamped release manifest (DR-0041
Amendment A).

The property under test throughout is **"it always answers"**. This module is
what a launcher calls to say which code is about to run, so every degenerate
input -- no manifest, truncated JSON, a JSON array, a manifest from a future
build with fields this version has never heard of -- has to produce a sentence,
not an exception. A traceback where the banner should be would take away the one
piece of information you reach for when a server is already behaving strangely.
"""

from __future__ import annotations

import json

from seedpod.app.release import (
    MANIFEST_NAME,
    ReleaseManifest,
    default_release_root,
    describe_release,
    read_manifest,
)

GOOD = {
    "release": "2.0.0a0+abc12345",
    "version": "2.0.0a0",
    "git_sha": "abc12345def67890",
    "git_branch": "main",
    "git_dirty": False,
    "built_at": "2026-08-15T09:00:00+00:00",
    "uv_lock_sha256": "0" * 64,
    "requires_python": ">=3.11",
    "ui_bundle": True,
}


def _stamp(root, **overrides):
    data = {**GOOD, **overrides}
    (root / MANIFEST_NAME).write_text(json.dumps(data), encoding="utf-8")
    return data


def test_reads_every_stamped_field(tmp_path):
    _stamp(tmp_path)
    manifest = read_manifest(tmp_path)
    assert manifest == ReleaseManifest(**GOOD)


def test_describes_a_stamped_release_with_sha_branch_and_build_time(tmp_path):
    _stamp(tmp_path)
    line = describe_release(tmp_path)
    assert "2.0.0a0+abc12345" in line
    assert "abc12345def6" in line  # 12 chars of sha, not the whole thing
    assert "on main" in line
    assert "2026-08-15T09:00:00+00:00" in line
    assert "ui bundle" in line


def test_an_unstamped_tree_says_so_instead_of_raising(tmp_path):
    assert read_manifest(tmp_path) is None
    line = describe_release(tmp_path)
    assert "unstamped" in line
    assert str(tmp_path) in line


def test_unreadable_manifests_degrade_to_unstamped(tmp_path):
    """Truncated JSON, a JSON array, and a directory in the manifest's place --
    every one of these is "no usable manifest", never a crash."""
    (tmp_path / MANIFEST_NAME).write_text('{"release": "2.0', encoding="utf-8")
    assert read_manifest(tmp_path) is None
    assert "unstamped" in describe_release(tmp_path)

    (tmp_path / MANIFEST_NAME).write_text("[1, 2, 3]", encoding="utf-8")
    assert read_manifest(tmp_path) is None

    other = tmp_path / "as-a-directory"
    (other / MANIFEST_NAME).mkdir(parents=True)
    assert read_manifest(other) is None
    assert "unstamped" in describe_release(other)


def test_a_manifest_from_a_newer_build_still_describes_itself(tmp_path):
    """Forward compatibility, and it is not hypothetical: the manifest schema
    will grow. An unknown key must be ignored rather than blow up the banner on
    the appliance that received the newer artifact."""
    _stamp(tmp_path, something_added_later="whatever", another=[1, 2])
    manifest = read_manifest(tmp_path)
    assert manifest is not None
    assert manifest.release == "2.0.0a0+abc12345"


def test_a_manifest_missing_fields_falls_back_per_field(tmp_path):
    (tmp_path / MANIFEST_NAME).write_text(json.dumps({"version": "9.9.9"}), encoding="utf-8")
    manifest = read_manifest(tmp_path)
    assert manifest is not None
    assert manifest.version == "9.9.9"
    assert manifest.git_sha == "unknown"
    line = describe_release(tmp_path)
    assert "unknown" in line
    assert "NO ui bundle" in line  # absence is stated, not implied


def test_a_dirty_build_is_marked_in_the_description(tmp_path):
    _stamp(tmp_path, git_dirty=True)
    assert "+dirty" in describe_release(tmp_path)


def test_an_edited_uv_lock_is_called_out(tmp_path):
    """The one active check. A release root is a mutable directory; `uv sync`
    against a lock that no longer matches what CI resolved is decision 1's
    failure mode, and nothing else would ever mention it."""
    _stamp(tmp_path)
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    line = describe_release(tmp_path)
    assert "WARNING" in line
    assert "uv.lock" in line


def test_a_matching_uv_lock_is_silent(tmp_path):
    import hashlib

    lock = tmp_path / "uv.lock"
    lock.write_text("version = 1\n", encoding="utf-8")
    _stamp(tmp_path, uv_lock_sha256=hashlib.sha256(lock.read_bytes()).hexdigest())
    assert "WARNING" not in describe_release(tmp_path)


def test_no_uv_lock_present_is_not_a_warning(tmp_path):
    """An artifact staged without a lock is a build bug, not a runtime one, and
    the launcher is the wrong place to shout about it."""
    _stamp(tmp_path)
    assert "WARNING" not in describe_release(tmp_path)


def test_default_release_root_is_the_directory_holding_the_package():
    root = default_release_root()
    assert (root / "seedpod" / "app" / "release.py").is_file()


def test_describe_release_defaults_to_the_running_tree():
    """No argument at all still answers -- this repo is an unstamped checkout,
    and saying so IS the answer."""
    assert describe_release().startswith("seedpod")
