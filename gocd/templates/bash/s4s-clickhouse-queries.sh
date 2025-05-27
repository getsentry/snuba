#!/bin/bash

if [ "${SNUBA_CMD_TYPE}" == "fetcher" ]
then
    ARGS="--querylog-host ${QUERYLOG_HOST} --querylog-port ${QUERYLOG_PORT} --window-hours ${WINDOW_HOURS} --tables ${TABLES} --gcs-bucket ${GCS_BUCKET}"
fi

if [ "${SNUBA_CMD_TYPE}" == "replayer" ]
then
    ARGS="--clickhouse-host ${CLICKHOUSE_HOST} --clickhouse-port ${CLICKHOUSE_PORT}  --gcs-bucket ${GCS_BUCKET}"
fi

if [ "${SNUBA_CMD_TYPE}" == "comparer" ]
then
    ARGS="--gcs-bucket ${GCS_BUCKET}"
fi

SNUBA_COMPONENT_NAME="query-${SNUBA_CMD_TYPE}-gocd"
SNUBA_CMD="query-${SNUBA_CMD_TYPE} ${ARGS[@]}"

eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")
/devinfra/scripts/k8s/k8stunnel

/devinfra/scripts/k8s/k8s-spawn-job.py \
  --label-selector="service=snuba,component=${SNUBA_COMPONENT_NAME}" \
  --container-name="${SNUBA_COMPONENT_NAME}" \
  --try-deployments-and-statefulsets \
  "snuba-query-${SNUBA_CMD_TYPE}" \
  "us-central1-docker.pkg.dev/sentryio/snuba/image:${GO_REVISION_SNUBA_REPO}" \
  -- \
  snuba $SNUBA_CMD
