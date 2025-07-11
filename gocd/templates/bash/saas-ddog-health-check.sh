#!/bin/bash

checks-datadog-monitor-status --dry-run=true \
  113296727 \
  42722121


# Above monitor IDs map to following monitors respectively:
# Snuba - SLO - High API error rate
# Snuba - Too many restarts on Snuba pods
