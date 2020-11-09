#!/bin/bash

set -ex

# first check if we're passing flags, if so
# prepend with snuba
if [ "${1:0:1}" = '-' ]; then
    set -- snuba "$@"
fi

set -- snuba "$@"
set gosu snuba "$@"

exec "$@"
