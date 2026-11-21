#!/bin/sh
# Runs sentry's Snuba tests in one pytest session: the snuba_ci subset, or
# every Snuba test when API_CHANGES is true. With a -m filter, every shard
# collects sentry's whole suite, so one session collects it once instead of
# twice. Run from the sentry checkout.
set -eu

markers=snuba_ci
if [ "${API_CHANGES:-}" = true ]; then
  markers="snuba_ci or snuba"
fi

# pytest splits PYTEST_ADDOPTS like a shell, so the quotes are part of the value.
PYTEST_ADDOPTS=$(printf "%s -k 'not __In' -m '%s' -n %s --dist=loadfile -vv" \
  "${PYTEST_ADDOPTS:-}" "$markers" "$XDIST_WORKERS")
export PYTEST_ADDOPTS
exec make test-python-ci
