# shellcheck shell=sh
#
# Shared release-root resolution for bin/seedpod, bin/seedpod-bootstrap and
# bin/seedpodctl (DR-0041 decision 2 + decision 4). Sourced, never executed.
# Callers set SEEDPOD_BIN_DIR to their own directory first, then source this.
#
# The layout it resolves against (decision 2):
#
#   ~/seedpod/
#     releases/<release>/   pyproject.toml uv.lock seedpod/ config/ ui/dist/ bin/ .venv/
#     current -> releases/<release>
#     var/                  .env db/ data/ log/ admin-api-key.txt seedpod.pid
#
# **Why every path here is absolute.** AppConfig's config_dir, log_dir and
# snapshot_storage_path all default to cwd-relative values (`config`, `logs`,
# `data/snapshots`) -- fine in a repo you cd into, wrong in a release root nobody
# does. DR-0041 makes exporting absolutes the launcher's job precisely so those
# defaults never get a chance to resolve against whatever directory the operator
# happened to be standing in.
#
# **Why the launcher wins over var/.env for the path variables only.** .env is
# sourced FIRST, then the path variables below are overwritten (plus
# SEEDPOD_UI_DIR, when a bundle is actually present). .env is the
# right home for secrets and tokens; it is the wrong home for "where does this
# release keep its state", because that answer is a property of the layout, not
# of the operator's preferences -- and a stale relative path in .env is exactly
# the class of bug the absolutes exist to prevent. Everything else in .env wins.
#
# **SEEDPOD_CONFIG_DIR is the one exception, and it is a recent one**
# (DR-0041 erratum E3). Config is no longer a property of the release: the real
# deployment configuration lives in its own private repo, and the `config/` an
# artifact carries is only a generic default. So an ABSOLUTE SEEDPOD_CONFIG_DIR
# wins here. A RELATIVE one still does not -- `SEEDPOD_CONFIG_DIR=config` is the
# stale-checkout leftover the absolutes exist to prevent, and is discarded
# exactly as before.

set -eu

if [ -z "${SEEDPOD_BIN_DIR:-}" ]; then
    echo "seedpod: SEEDPOD_BIN_DIR unset (launcher bug: set it before sourcing)" >&2
    exit 70
fi

# Logical, not physical: through `current` this is `~/seedpod/current`, whose
# parent is the release home. Resolving symlinks here would land in
# releases/<release>/ and lose the one indirection an upgrade swaps.
RELEASE_ROOT=$(dirname "$SEEDPOD_BIN_DIR")

# `dirname`, NOT `$RELEASE_ROOT/..`, and the difference is not cosmetic: `..` is
# resolved by the kernel against the PHYSICAL parent, so through the `current`
# symlink it means `releases/`, and the state directory would be looked for at
# `~/seedpod/releases/var`. `dirname` is pure string manipulation and stays on
# the logical path. (Found by installing a real artifact; the first version of
# this script used `..` and refused to start on a correctly-laid-out release.)
SEEDPOD_HOME=$(dirname "$RELEASE_ROOT")

if [ -z "${SEEDPOD_VAR:-}" ]; then
    if [ -d "$SEEDPOD_HOME/var" ]; then
        SEEDPOD_VAR="$SEEDPOD_HOME/var"
    else
        # Deliberately a refusal rather than a mkdir. Guessing would silently
        # create a state directory somewhere nobody expects, and state is the one
        # thing an upgrade must never touch (decision 2).
        echo "seedpod: no state directory found." >&2
        echo "  Looked for: $SEEDPOD_HOME/var" >&2
        echo "  Expected layout: ~/seedpod/{current -> releases/<release>, var/}" >&2
        echo "  Fix: mkdir -p ~/seedpod/var   (or set SEEDPOD_VAR to an existing directory)" >&2
        exit 78
    fi
fi

PY="$RELEASE_ROOT/.venv/bin/python"
if [ ! -x "$PY" ]; then
    echo "seedpod: no runtime at $PY" >&2
    echo "  DR-0041 decision 1: the venv is materialised on the appliance, not shipped." >&2
    echo "  Fix: cd $RELEASE_ROOT && uv sync --locked --python 3.11" >&2
    # --python is explicit rather than a `.python-version` file: pyenv reads that
    # filename too, and where uv is a pyenv shim the file breaks uv itself.
    exit 78
fi

# Captured BEFORE .env is sourced, because `set -a; . .env` overwrites the
# inherited environment. Without this, a stale `SEEDPOD_CONFIG_DIR=config` left
# in .env would silently beat a value the operator exported deliberately in the
# shell they are standing in. Explicit beats file; file beats release default.
SEEDPOD_CONFIG_DIR_FROM_SHELL="${SEEDPOD_CONFIG_DIR:-}"

# Secrets and tokens first; the path variables below then override.
if [ -f "$SEEDPOD_VAR/.env" ]; then
    set -a
    . "$SEEDPOD_VAR/.env"
    set +a
fi

mkdir -p "$SEEDPOD_VAR/db" "$SEEDPOD_VAR/log" "$SEEDPOD_VAR/data/snapshots"

if [ -n "$SEEDPOD_CONFIG_DIR_FROM_SHELL" ]; then
    SEEDPOD_CONFIG_DIR="$SEEDPOD_CONFIG_DIR_FROM_SHELL"
fi

# Absolute: the operator owns it, keep it. Anything else (unset, or relative):
# this release's own config. See the header note.
case "${SEEDPOD_CONFIG_DIR:-}" in
    /*) : ;;
    *) SEEDPOD_CONFIG_DIR="$RELEASE_ROOT/config" ;;
esac

# A refusal rather than a silent fallback to the release default: an operator who
# set this and mistyped it wants to hear so, not to watch seedpod come up serving
# somebody else's profiles.
if [ ! -d "$SEEDPOD_CONFIG_DIR" ]; then
    echo "seedpod: config directory not found: $SEEDPOD_CONFIG_DIR" >&2
    echo "  Set SEEDPOD_CONFIG_DIR in $SEEDPOD_VAR/.env to an absolute path," >&2
    echo "  or unset it to use this release's own config ($RELEASE_ROOT/config)." >&2
    exit 78
fi

SEEDPOD_LOG_DIR="$SEEDPOD_VAR/log"
SEEDPOD_PID_FILE="$SEEDPOD_VAR/seedpod.pid"
SEEDPOD_SNAPSHOT_STORAGE_PATH="$SEEDPOD_VAR/data/snapshots"
SEEDPOD_DATABASE_URL="sqlite:///$SEEDPOD_VAR/db/seedpod.db"
export SEEDPOD_CONFIG_DIR SEEDPOD_LOG_DIR SEEDPOD_PID_FILE
export SEEDPOD_SNAPSHOT_STORAGE_PATH SEEDPOD_DATABASE_URL

# Only when a bundle is actually there: unset means "mount nothing", which is
# what keeps the vite dev-server workflow working (decision 3). Pointing at an
# empty directory would instead make mount_spa refuse at startup.
if [ -f "$RELEASE_ROOT/ui/dist/index.html" ]; then
    SEEDPOD_UI_DIR="$RELEASE_ROOT/ui/dist"
    export SEEDPOD_UI_DIR
fi

seedpod_banner() {
    "$PY" -m seedpod.app.release "$RELEASE_ROOT"
}
