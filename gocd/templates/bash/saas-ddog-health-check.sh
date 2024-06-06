#!/bin/bash

/devinfra/scripts/checks/datadog/monitor_status.py --dry-run=true \
  113296727 \
  42722121 \
  146517092


# Above monitor IDs map to following monitors respectively:
# Snuba - SLO - High API error rate
# Snuba - Too many restarts on Snuba pods
# Snuba Pods Crashlooping in {{sentry_region.name}}
