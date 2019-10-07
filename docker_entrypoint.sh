#!/bin/bash

set -ex

if [ $1 = 'bash' ]; then
  exec bash
fi

# first check if we're passing flags, if so
# prepend with snuba
if [ "${1:0:1}" = '-' ]; then
    set -- snuba "$@"
fi

if [ "$1" = 'api' ]; then
  if [ "$#" -gt 1 ]; then
    echo "Running Snuba API server with arguments:" "${@:2}"
    set -- uwsgi --master --manage-script-name --wsgi-file snuba/views.py --die-on-term "${@:2}"
  else
    _default_args="--socket /tmp/snuba.sock --http 0.0.0.0:1218 --http-keepalive"
    echo "Running Snuba API server with default arguments: $_default_args"
    set -- uwsgi --master --manage-script-name --wsgi-file snuba/views.py --die-on-term $_default_args
  fi
  set -- "$@"
fi

if snuba "$1" --help > /dev/null 2>&1; then
  set -- snuba "$@"
fi

exec gosu snuba "$@"
