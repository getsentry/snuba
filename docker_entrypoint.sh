#!/bin/bash
set -e

if [[ -z "${ENABLE_HEAPTRACK}" ]]; then
  set -- heaptrack snuba "$@"
else
  set -- snuba "$@"
fi

exec "$@"
