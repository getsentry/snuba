#!/bin/bash

checks-datadog-monitor-status \
  ${DATADOG_MONITOR_IDS}

# DATADOG_MONITOR_IDS are the region deploy-pipeline gates
# (crashloop + API error rate), tagged deploy-pipeline:true.
