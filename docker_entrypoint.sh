#!/bin/bash

print_help() {
    echo "Available commands: api, processor, writer."
    echo "Additional arguments are appended as arguments to the respective program."
}

case $1 in
"api")
    if [ "$#" -gt 1 ]; then
        echo "Running Snuba API server with arguments:" "${@:2}"
        exec uwsgi --master --manage-script-name --mount /=snuba.api:app "${@:2}"
    else
        _default_args="--socket /tmp/snuba.sock --http 0.0.0.0:8000"
        echo "Running Snuba API server with default arguments: $_default_args"
        exec uwsgi --master --manage-script-name --mount /=snuba.api:app $_default_args
    fi
    ;;
"processor")
    echo "Running Snuba processor with arguments:" "${@:2}"
    exec ./bin/processor "${@:2}"
    ;;
"writer")
    echo "Running Snuba writer with arguments:" "${@:2}"
    exec ./bin/writer "${@:2}"
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
