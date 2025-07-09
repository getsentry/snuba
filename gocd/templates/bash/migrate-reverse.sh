#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")
/devinfra/scripts/k8s/k8stunnel

k8s-spawn-job \
  --label-selector="service=${SNUBA_SERVICE_NAME}" \
  --container-name="${SNUBA_SERVICE_NAME}" \
  "snuba-migrate-reverse" \
  "us-central1-docker.pkg.dev/sentryio/snuba/image:${GO_REVISION_SNUBA_REPO}" \
  -- \
  snuba migrations reverse-in-progress
