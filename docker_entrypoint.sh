#!/bin/bash

set -ex

# first check if we're passing flags, if so
# prepend with sentry
if [ "${1:0:1}" = '-' ]; then
    set -- snuba "$@"
fi

if [ "$1" = 'api' ]; then
  if [ "$#" -gt 1 ]; then
    echo "Running Snuba API server with arguments:" "${@:2}"
    set -- uwsgi --master --manage-script-name --mount /=snuba.api:application "${@:2}"
  else
    _default_args="--socket /tmp/snuba.sock --http 0.0.0.0:1218 --http-keepalive"
    echo "Running Snuba API server with default arguments: $_default_args"
    set -- uwsgi --master --manage-script-name --mount /=snuba.api:application $_default_args
  fi
  set -- gosu snuba "$@"
fi

if snuba "$1" --help > /dev/null 2>&1; then
  set -- snuba "$@"
fi

if [ "$1" = 'snuba' -a "$(id -u)" = '0' ]; then
    set -- gosu snuba "$@"
fi

exec "$@"
