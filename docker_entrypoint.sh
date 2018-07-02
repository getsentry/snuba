#!/bin/bash

print_help() {
    echo "Available commands: api, consumer, optimize, cleanup."
    echo "Additional arguments are appended as arguments to the respective program."
}

case $1 in
"api")
    if [ "$#" -gt 1 ]; then
        echo "Running Snuba API server with arguments:" "${@:2}"
        exec uwsgi --master --manage-script-name --pypy-wsgi snuba.api "${@:2}"
    else
        _default_args="--socket /tmp/snuba.sock --http 0.0.0.0:1218 --http-keepalive"
        echo "Running Snuba API server with default arguments: $_default_args"
        exec uwsgi --master --manage-script-name --pypy-wsgi snuba.api $_default_args
    fi
    ;;
"consumer")
    echo "Running Snuba consumer with arguments:" "${@:2}"
    exec ./bin/consumer "${@:2}"
    ;;
"optimize"|"optimizer")
    echo "Running Snuba optimizer with arguments:" "${@:2}"
    exec ./bin/optimize "${@:2}"
    ;;
"cleanup"|"cleaner")
    echo "Running Snuba partition cleaner with arguments:" "${@:2}"
    exec ./bin/cleanup "${@:2}"
    ;;
"sh"|"/bin/sh"|"bash"|"/bin/bash")
    bash "${@:2}"
    ;;
"-h")
    print_help
    ;;
"--help")
    print_help
    ;;
*)
    >&2 echo "ERROR: Unknown command: $1"
    exit 1
    ;;
esac
