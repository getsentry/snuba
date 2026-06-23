#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

IMAGE_TAG="${GO_REVISION_SNUBA_REPO}"
if [ "${SENTRY_REGION}" = "s4s2" ]; then
  IMAGE_TAG="${GO_REVISION_SNUBA_REPO}-distroless"
fi

/devinfra/scripts/get-cluster-credentials \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-docker.pkg.dev/sentryio/snuba-mr/image:${IMAGE_TAG}" \
  --container-name="consumer" \
  --container-name="eap-accepted-outcomes-consumer" \
  --container-name="eap-items-consumer" \
  --container-name="errors-replacer" \
  --container-name="generic-metrics-counters-consumer" \
  --container-name="generic-metrics-distributions-consumer" \
  --container-name="generic-metrics-sets-consumer" \
  --container-name="loadbalancer-outcomes-consumer" \
  --container-name="metrics-consumer" \
  --container-name="outcomes-billing-consumer" \
  --container-name="outcomes-consumer" \
  --container-name="profile-chunks-consumer" \
  --container-name="profiles-consumer" \
  --container-name="profiling-functions-consumer" \
  --container-name="querylog-consumer" \
  --container-name="replays-consumer" \
  --container-name="transactions-consumer-new"
