#!/bin/bash
# Set up a Snuba dev/test environment in an ephemeral Linux container
# (e.g. a Claude Code cloud session) so that the test suite can run.
#
# This is intentionally a standalone, on-demand script (not a session-start
# hook): `uv sync` plus pulling the service images takes several minutes, so we
# only want to pay that cost when we actually need to run tests.
#
# What it does (all steps are idempotent and safe to re-run):
#   1. Install sentry-devenv (via the official bash installer).
#   2. Build the Python virtualenv AND the native rust_snuba extension
#      (`uv sync` compiles rust_snuba into the venv, so no separate
#      `maturin develop` step is needed).
#   3. Make sure the Docker daemon is running.
#   4. Bring up the service dependencies via devservices (clickhouse, redis,
#      kafka).
#
# Usage:  bash scripts/setup-test-env.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------------------
# 1. Install sentry-devenv (global, into ~/.local/share/sentry-devenv/bin).
#    devservices itself is installed by `uv sync` below (it is a dev
#    dependency and ends up at .venv/bin/devservices).
# ---------------------------------------------------------------------------
DEVENV_BIN="$HOME/.local/share/sentry-devenv/bin"
if [ -x "$DEVENV_BIN/devenv" ]; then
  log "sentry-devenv already installed ($DEVENV_BIN/devenv)"
else
  log "Installing sentry-devenv"
  installer="$(mktemp -t install-devenv.XXXX.sh)"
  curl -fsSL https://raw.githubusercontent.com/getsentry/devenv/main/install-devenv.sh -o "$installer"
  # The installer only runs when invoked as a file literally named
  # `install-devenv.sh`, so rename before executing.
  mv "$installer" "$(dirname "$installer")/install-devenv.sh"
  installer="$(dirname "$installer")/install-devenv.sh"
  chmod +x "$installer"
  # CI=1 makes the installer non-interactive; </dev/null stops the trailing
  # login shell from blocking. Non-fatal: the functional path is `uv sync`.
  if ! CI=1 SHELL="${SHELL:-/bin/bash}" "$installer" </dev/null; then
    log "WARN: devenv install failed; continuing (uv provides the venv)."
  fi
  rm -f "$installer"
fi
export PATH="$DEVENV_BIN:$PATH"

# ---------------------------------------------------------------------------
# 2. Build the Python virtualenv + the rust_snuba native extension.
# ---------------------------------------------------------------------------
log "Syncing Python dependencies and building rust_snuba (uv sync)"
uv sync --frozen --active

# ---------------------------------------------------------------------------
# 3. Make sure the Docker daemon is running (cloud containers don't start one).
# ---------------------------------------------------------------------------
log "Ensuring the Docker daemon is running"
if docker info >/dev/null 2>&1; then
  echo "Docker daemon already running."
else
  echo "Starting dockerd ..."
  sudo dockerd >/tmp/dockerd.log 2>&1 &
  for _ in $(seq 1 30); do
    if docker info >/dev/null 2>&1; then break; fi
    sleep 1
  done
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon did not become ready. Last log lines:" >&2
    tail -n 20 /tmp/dockerd.log >&2 || true
    exit 1
  fi
  echo "Docker daemon is up."
fi

# ---------------------------------------------------------------------------
# 4. Bring up the service dependencies (clickhouse, redis, kafka).
# ---------------------------------------------------------------------------
log "Starting devservices (clickhouse, redis, kafka)"
.venv/bin/devservices up

# ---------------------------------------------------------------------------
# Done.
# ---------------------------------------------------------------------------
log "Environment ready. To run tests:"
cat <<'EOF'
  export PATH="$PWD/.venv/bin:$PATH"

  # Run pytest from inside the tests/ directory. Invoking it from the repo
  # root loads test_distributed_migrations/conftest.py, whose pytest_configure
  # connects to a multi-node clickhouse ("clickhouse-query") that devservices
  # does not provide in single-node mode.
  (cd tests && SNUBA_SETTINGS=test ../.venv/bin/pytest <path/to/test_file.py> -m "not ci_only")

  # For rust tests you also need the rust env vars:
  #   . scripts/rust-envvars && (cd rust_snuba && cargo test --workspace)
EOF
