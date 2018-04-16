#!/bin/bash

print_help() {
    echo "Available commands: api, processor, writer."
    echo "Additional arguments are appended as arguments to the respective program."
}

case $1 in
"api")
    echo "Running Snuba API server with arguments:" "${@:2}"
    exec gunicorn snuba.api:app -b 0.0.0.0:8000 "${@:2}"
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
