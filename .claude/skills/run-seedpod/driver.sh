#!/usr/bin/env bash
# Cold-start seedpod and drive it, without leaving anything in the checkout.
#
# Every piece of run state -- database, logs, Fernet keys, admin key, the PID
# file -- lives under a scratch directory outside the repo. That is not
# tidiness: `scripts/build_release.py` refuses to build from a dirty tree and
# counts UNTRACKED files as dirty (`git status --porcelain`), so a smoke that
# drops `seedpod.pid` or `db/` into the checkout silently blocks the next
# release. Hence SEEDPOD_PID_FILE, SEEDPOD_LOG_DIR and an absolute sqlite path.
#
# Usage:
#   driver.sh up      build the SPA, init state, launch, wait for health
#   driver.sh smoke   assert the API + SPA-fallback invariants (exit 1 on fail)
#   driver.sh key     print the admin API key (for browser login)
#   driver.sh url     print the base URL
#   driver.sh logs    tail the server log
#   driver.sh down    stop the server, leave state
#   driver.sh reset   stop and delete all state
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# Deliberately /tmp and not $TMPDIR: on macOS TMPDIR is a per-user path like
# /var/folders/6g/.../T/ WITH a trailing slash, which both makes the documented
# state path unpredictable and produces `//seedpod-smoke`. Override with
# SEEDPOD_SMOKE_DIR if /tmp is unsuitable.
STATE="${SEEDPOD_SMOKE_DIR:-/tmp/seedpod-smoke}"
PORT="${SEEDPOD_SMOKE_PORT:-8099}"
BASE="http://127.0.0.1:${PORT}"

mkdir -p "$STATE/logs"

# ---------------------------------------------------------------------------
# Environment. Two entries here are load-bearing and non-obvious:
#
# SEEDPOD_ENABLED_PROVIDERS=none -- "none" is not a magic word, it is a name no
#   provider has. `load_enabled_providers` constructs nothing for an unknown
#   name (seedpod/app/factory.py), which is exactly what a UI smoke wants.
#   Setting this to the EMPTY STRING does NOT work: `_csv_env` ends with
#   `return values or default`, so empty falls back to all four providers, and
#   startup then dies in `digitalocean.check_ready()` with the genuinely
#   baffling `Illegal header value b'Bearer '` -- httpx rejecting the header
#   built from an absent DIGITALOCEAN_TOKEN. Boot failure, not a warning.
#
# SEEDPOD_UI_DIR -- unset means NO SPA is mounted at all and `/` 404s with the
#   server otherwise healthy. It must also already contain index.html or
#   `mount_spa` raises SpaNotBuilt and the process exits.
# ---------------------------------------------------------------------------
env_file="$STATE/env.sh"
write_env() {
  # Keys are generated once and reused, so a `down`/`up` cycle can still read
  # the database it wrote last time.
  if [ ! -f "$STATE/keys.env" ]; then
    (cd "$REPO" && uv run seedpod-bootstrap generate-keys) | grep '^SEEDPOD_SECRET_KEY_' > "$STATE/keys.env"
  fi
  {
    echo "export SEEDPOD_DATABASE_URL=\"sqlite:///$STATE/seedpod.db\""
    cat "$STATE/keys.env" | sed 's/^/export /'
    echo "export SEEDPOD_ENABLED_PROVIDERS=none"
    echo "export SEEDPOD_BACKGROUND_TASKS=false"
    echo "export SEEDPOD_UI_DIR=ui/dist"
    echo "export SEEDPOD_API_HOST=127.0.0.1"
    echo "export SEEDPOD_API_PORT=$PORT"
    echo "export SEEDPOD_PID_FILE=\"$STATE/seedpod.pid\""
    echo "export SEEDPOD_LOG_DIR=\"$STATE/logs\""
    echo "export SEEDPOD_LOG_TO_FILE=false"
    echo "export SEEDPOD_LOG_TO_CONSOLE=true"
    echo "export SEEDPOD_ENVIRONMENT=development"
  } > "$env_file"
}

load_env() { set -a; . "$env_file"; set +a; }

cmd_up() {
  write_env; load_env
  cd "$REPO"

  if [ ! -f ui/dist/index.html ]; then
    echo "==> building the SPA (mount_spa raises SpaNotBuilt without it)"
    (cd ui && npm ci --ignore-scripts && npm run build)
  fi

  echo "==> migrating $STATE/seedpod.db"
  uv run seedpod-bootstrap migrate

  if [ ! -f "$STATE/admin-key.txt" ]; then
    echo "==> creating the admin key (printed ONCE, so it is captured here)"
    uv run seedpod-bootstrap create-admin smoke \
      | grep -oE 'seedpod_[a-z]+_[0-9a-f]+' > "$STATE/admin-key.txt"
  fi

  if curl -sf -o /dev/null "$BASE/api/health" 2>/dev/null; then
    echo "==> already up at $BASE"; return 0
  fi

  echo "==> launching"
  # nohup + </dev/null + disown, all three. Without `</dev/null` the server
  # inherits the caller's stdin, and a plain `cmd &` keeps the caller's stdout
  # pipe open -- so `driver.sh up | tail` hangs forever even though the server
  # started fine. That is not hypothetical; it is what the first version did.
  nohup uv run python start.py </dev/null > "$STATE/server.log" 2>&1 &
  echo $! > "$STATE/launcher.pid"
  disown 2>/dev/null || true

  for _ in $(seq 1 40); do
    if curl -sf -o /dev/null "$BASE/api/health" 2>/dev/null; then
      echo "==> up at $BASE"
      echo "==> API key: $(cat "$STATE/admin-key.txt")"
      return 0
    fi
    sleep 1
  done
  echo "!! failed to come up; last 40 lines:" >&2
  tail -40 "$STATE/server.log" >&2
  return 1
}

# Assertions, not a transcript. Each one is an invariant the SPA depends on.
fail=0
check() { # check <label> <expected> <actual>
  if [ "$2" = "$3" ]; then printf '  ok   %-46s %s\n' "$1" "$3"
  else printf '  FAIL %-46s expected=%s got=%s\n' "$1" "$2" "$3"; fail=1; fi
}

cmd_smoke() {
  echo "==> smoking $BASE"
  check "GET /api/health" 200 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/health")"
  check "GET / serves the SPA" 200 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/")"
  check "GET / is html" "text/html" "$(curl -s -o /dev/null -w '%{content_type}' "$BASE/" | cut -d';' -f1)"
  # preact-router uses real paths, so a deep link must fall back to index.html.
  check "deep link /clusters -> index.html" 200 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/clusters")"
  check "deep link /health -> index.html" 200 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/health")"
  # ...but the fallback must NEVER swallow an unknown /api path (spa.py's
  # _API_PREFIXES). A 200 HTML here reads to a client as "the API broke".
  check "unknown /api/* stays 404" 404 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/clusterz")"
  check "unknown /api/* stays JSON" "application/json" "$(curl -s -o /dev/null -w '%{content_type}' "$BASE/api/clusterz" | cut -d';' -f1)"
  # Auth actually gates the API.
  check "GET /api/clusters unauthenticated" 401 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/clusters")"
  local key; key="$(cat "$STATE/admin-key.txt")"
  check "GET /api/clusters with key" 200 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $key" "$BASE/api/clusters")"
  check "GET /api/config/overview with key" 200 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $key" "$BASE/api/config/overview")"
  # Every asset index.html references must actually resolve.
  local n=0
  while read -r a; do
    n=$((n+1))
    check "asset $a" 200 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE$a")"
  done < <(curl -s "$BASE/" | grep -oE '/assets/[^"]+')
  [ "$n" -gt 0 ] || { echo "  FAIL index.html referenced no /assets/* -- bad build"; fail=1; }

  if [ "$fail" = 0 ]; then echo "==> all checks passed"; else echo "==> FAILURES"; return 1; fi
}

cmd_down() {
  [ -f "$STATE/launcher.pid" ] && kill "$(cat "$STATE/launcher.pid")" 2>/dev/null || true
  pkill -f 'python start.py' 2>/dev/null || true
  sleep 1
  rm -f "$STATE/launcher.pid"
  echo "==> stopped"
}

case "${1:-}" in
  up)    cmd_up ;;
  smoke) cmd_smoke ;;
  key)   cat "$STATE/admin-key.txt" ;;
  url)   echo "$BASE" ;;
  logs)  tail -f "$STATE/server.log" ;;
  down)  cmd_down ;;
  reset) cmd_down; rm -rf "$STATE"; echo "==> state deleted" ;;
  *)     sed -n '2,20p' "$0"; exit 2 ;;
esac
