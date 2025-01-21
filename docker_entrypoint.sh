#!/bin/bash
set -e

# first check if we're passing flags, if so
# prepend with snuba
if [ "${1:0:1}" = '-' ]; then
    set -- snuba "$@"
fi

help_result=$(snuba "${1}" --help)
help_return=$?

if [[ "${help_return}" -eq 0 ]]; then
  set -- snuba "$@"
else
  # Print the error message if it returns non-zero, to help with troubleshooting.
  printf "Error running snuba ${1} --help, passing command to exec directly."
  printf "\n${help_result}"
fi

if [ -n "${ENABLE_HEAPTRACK:-}" ]; then
  file_path="./profiler_data/profile_$(date '+%Y%m%d_%H%M%S')"
  set -- heaptrack -o "${file_path}" "$@"
fi

exec "$@"
