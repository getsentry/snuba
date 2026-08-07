#!/bin/bash

checks-datadog-monitor-status --dry-run=true \
  ${DATADOG_MONITOR_IDS}


# DATADOG_MONITOR_IDS is set per-region in snuba-{py,rs}.libsonnet:
# us: 311884335 311884334
#   311884335 - [us] [Deploy pipeline] Snuba pods are crashlooping
#   311884334 - [us] [Deploy pipeline] Snuba - High API error rate
# de: 311884404 311884405
#   311884404 - [de] [Deploy pipeline] Snuba pods are crashlooping
#   311884405 - [de] [Deploy pipeline] Snuba - High API error rate
