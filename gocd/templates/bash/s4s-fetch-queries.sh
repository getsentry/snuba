#!/bin/bash
eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")
/devinfra/scripts/k8s/k8stunnel

/devinfra/scripts/k8s/k8s-spawn-job.py \
  --label-selector="service=snuba-${SNUBA_SERVICE_NAME}" \
  --container-name="${SNUBA_SERVICE_NAME}" \
  "snuba-query-fetcher" \
  "us.gcr.io/sentryio/snuba:${GO_REVISION_SNUBA_REPO}" \
  -- \
  snuba query-fetcher "--querylog-host ${QUERYLOG_HOST} --querylog-port ${QUERYLOG_PORT} --window-hours ${WINDOW_HOURS} --tables ${TABLES} --gcs-bucket ${GCS_BUCKET}"
