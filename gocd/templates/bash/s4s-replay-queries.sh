#!/bin/bash
args=( $REPLAYER_ARGS )
eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")
/devinfra/scripts/k8s/k8stunnel

/devinfra/scripts/k8s/k8s-spawn-job.py \
  --label-selector="service=snuba,component=${SNUBA_COMPONENT_NAME}" \
  --container-name="${SNUBA_COMPONENT_NAME}" \
  "snuba-query-replayer" \
  "us.gcr.io/sentryio/snuba:${GO_REVISION_SNUBA_REPO}" \
  -- \
  snuba query-replayer "${args[@]}"
