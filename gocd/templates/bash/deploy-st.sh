#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

IMAGE_TAG="${GO_REVISION_SNUBA_REPO}-distroless"

/devinfra/scripts/get-cluster-credentials

k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-docker.pkg.dev/sentryio/snuba-mr/image:${IMAGE_TAG}" \
  --container-name="snuba" \
  --container-name="snuba-admin"

k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-docker.pkg.dev/sentryio/snuba-mr/image:${IMAGE_TAG}" \
  --type="cronjob" \
  --container-name="cleanup"
