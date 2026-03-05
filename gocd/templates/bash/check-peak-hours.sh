#!/bin/bash

# Prevent rust consumer deploys to US during peak hours (6am-10am PT)
# to avoid expensive kafka rebalances that cause backlog processing
# and high clickhouse insert load (especially eap-items).

current_hour=$(TZ="America/Los_Angeles" date +"%H")

if [ "$current_hour" -ge 6 ] && [ "$current_hour" -lt 10 ]; then
  echo "ERROR: Rust consumer deploys to US are blocked during peak hours (6am-10am PT)."
  echo "Current time in PT: $(TZ='America/Los_Angeles' date +'%H:%M %Z')"
  echo "Deploys cause expensive kafka rebalances that backlog processing"
  echo "and can cause too many inserts/s on clickhouse."
  echo "You can feel free to override this via the GoCD UI for an urgent change."
  exit 1
fi

echo "Current time in PT: $(TZ='America/Los_Angeles' date +'%H:%M %Z') - outside peak hours, proceeding."
