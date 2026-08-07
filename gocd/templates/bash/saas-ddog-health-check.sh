#!/bin/bash

# Region-specific deploy-pipeline monitors from
# https://github.com/getsentry/datadog-terraform/pull/630
case "${SENTRY_REGION}" in
  us)
    MONITOR_IDS="42722121 311884335 311884334"
    ;;
  de)
    MONITOR_IDS="42722121 311884404 311884405"
    ;;
  *)
    echo "Unsupported SENTRY_REGION='${SENTRY_REGION}' for saas datadog health check" >&2
    exit 1
    ;;
esac

checks-datadog-monitor-status --dry-run=true \
  ${MONITOR_IDS}


# Monitor ID map:
# 42722121  - [global] Snuba - Too many restarts on Snuba pods
# 311884335 - [us] [Deploy pipeline] Snuba pods are crashlooping
# 311884334 - [us] [Deploy pipeline] Snuba - High API error rate
# 311884404 - [de] [Deploy pipeline] Snuba pods are crashlooping
# 311884405 - [de] [Deploy pipeline] Snuba - High API error rate
