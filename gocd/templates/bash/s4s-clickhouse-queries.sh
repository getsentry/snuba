#!/bin/bash

FETCHER_ARGS="--querylog-host ${QUERYLOG_HOST} --querylog-port ${QUERYLOG_PORT} --window-hours ${WINDOW_HOURS} --tables ${TABLES} --gcs-bucket ${GCS_BUCKET}"
REPLAYER_ARGS="--clickhouse-host ${CLICKHOUSE_HOST} --clickhouse-port ${CLICKHOUSE_PORT}  --gcs-bucket ${GCS_BUCKET}"
COMPARER_ARGS="--gcs-bucket ${GCS_BUCKET}"

SNUBA_SERVICE_NAME="query-${SNUBA_CMD_TYPE}-gocd"

if [ "${SNUBA_CMD_TYPE}" == "fetcher" ]
then
    ARGS=$FETCHER_ARGS
fi

if [ "${SNUBA_CMD_TYPE}" == "replayer" ]
then
    ARGS=$REPLAYER_ARGS_ARGS
fi

if [ "${SNUBA_CMD_TYPE}" == "comparer" ]
then
    ARGS=$COMPARER_ARGS
fi

SNUBA_SERVICE_NAME="query-${SNUBA_CMD_TYPE}-gocd"

eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")
/devinfra/scripts/k8s/k8stunnel

/devinfra/scripts/k8s/k8s-spawn-job.py \
  --label-selector="service=snuba-${SNUBA_SERVICE_NAME}" \
  --container-name="${SNUBA_SERVICE_NAME}" \
  --try-deployments-and-statefulsets \
  "snuba-query-${SNUBA_CMD_TYPE}" \
  "us.gcr.io/sentryio/snuba:${GO_REVISION_SNUBA_REPO}" \
  -- \
  snuba "query-${SNUBA_CMD_TYPE} ${ARGS[@]}"
