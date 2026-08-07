#!/bin/bash

checks-datadog-monitor-status --dry-run=true \
  ${DATADOG_MONITOR_IDS}
