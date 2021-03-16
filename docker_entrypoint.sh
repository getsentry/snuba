#!/bin/bash
set -e

# first check if we're passing flags, if so
# prepend with snuba
if [ "${1:0:1}" = '-' ]; then
    set -- snuba "$@"
fi

if snuba "$1" --help > /dev/null 2>&1; then
  set -- snuba "$@"
  set gosu snuba "$@"
fi

exec "$@"
