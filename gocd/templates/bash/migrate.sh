#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")
/devinfra/scripts/get-cluster-credentials

IMAGE_TAG="${GO_REVISION_SNUBA_REPO}"

k8s-spawn-job \
  --label-selector="service=${SNUBA_SERVICE_NAME}" \
  --container-name="${SNUBA_SERVICE_NAME}" \
  "snuba-migrate" \
  "us-docker.pkg.dev/sentryio/snuba-mr/image:${IMAGE_TAG}" \
  -- \
  snuba migrations migrate --check-dangerous -r complete -r partial
