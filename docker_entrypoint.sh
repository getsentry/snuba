#!/bin/bash
set -e

if [[ -z "${ENABLE_HEAPTRACK}" ]]; then
  set -- snuba "$@"
else
  set -- heaptrack snuba "$@"
fi

exec "$@"
