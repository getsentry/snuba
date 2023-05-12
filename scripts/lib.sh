#!/bin/bash
# Module containing code shared across various shell scripts
# Execute functions from this module via the script do.sh

# Check if a command is available
# Exec
require() {
    command -v "$1" >/dev/null 2>&1
}

query_big_sur() {
    if require sw_vers && sw_vers -productVersion | grep -E "11\." >/dev/null; then
        return 0
    fi
    return 1
}
