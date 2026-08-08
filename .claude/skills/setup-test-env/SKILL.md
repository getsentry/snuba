---
name: setup-test-env
description: Set up the Snuba dev/test environment in a Claude Code cloud session so the test suite can run. Use when the user wants to run tests, install devenv/devservices, start clickhouse/redis/kafka, or otherwise prepare an ephemeral Linux container for development. Installs sentry-devenv, builds the Python venv and the rust_snuba native extension, starts Docker, and brings up devservices.
---

# Set up the Snuba test environment

Use this skill to make tests runnable in an ephemeral Linux container (such as a
Claude Code cloud session), where nothing is installed and the Docker daemon is
not running.

## How to run it

Run the setup script from the repo root:

```bash
bash scripts/setup-test-env.sh
```

It is idempotent, so it is safe to re-run. It takes several minutes on a cold
container (`uv sync` builds the rust extension, and the clickhouse/redis/kafka
images have to be pulled), and is much faster afterwards because the container
state is cached.

The script:

1. Installs `sentry-devenv` via the official bash installer
   (`~/.local/share/sentry-devenv/bin/devenv`).
2. Runs `uv sync --frozen --active`, which creates `.venv` **and** compiles the
   native `rust_snuba` extension into it (no separate `maturin develop` needed).
   `devservices` is a dev dependency, so it lands at `.venv/bin/devservices`.
3. Starts the Docker daemon (`sudo dockerd`) if it isn't already running.
4. Runs `.venv/bin/devservices up` to start clickhouse, redis and kafka.

## Running tests after setup

Run pytest **from inside the `tests/` directory**:

```bash
(cd tests && SNUBA_SETTINGS=test ../.venv/bin/pytest <path/to/test_file.py> -m "not ci_only")
```

Why from inside `tests/`: invoking pytest from the repo root loads
`test_distributed_migrations/conftest.py`, whose `pytest_configure` connects to a
multi-node clickhouse host (`clickhouse-query`) that devservices does not provide
in single-node mode, causing an `INTERNALERROR` before any test runs. Running
from within `tests/` avoids loading that sibling conftest.

For rust tests, source the rust env vars first:

```bash
. scripts/rust-envvars && (cd rust_snuba && cargo test --workspace)
```

## Notes / gotchas

- `devenv` refuses to run as root and `devenv sync` is Homebrew-based, so in the
  cloud container the functional setup path is `uv sync` + `devservices`, not
  `devenv sync`. The global `devenv` install is included only for parity with
  local laptop setups.
- The clickhouse container's `nofile` ulimit in `devservices/config.yml` is set
  to a value (soft 1024 / hard 4096) that fits inside the container's capability
  set; the production-style 262144 cannot be applied without `CAP_SYS_RESOURCE`.
- If `devservices up` reports clickhouse is unhealthy, check
  `docker ps` / `docker logs snuba-clickhouse-1`.
