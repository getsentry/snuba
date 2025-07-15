#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

/devinfra/scripts/get-cluster-credentials

k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-central1-docker.pkg.dev/sentryio/snuba/image:${GO_REVISION_SNUBA_REPO}" \
  --container-name="snuba" \
  --container-name="snuba-admin"

k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-central1-docker.pkg.dev/sentryio/snuba/image:${GO_REVISION_SNUBA_REPO}" \
  --type="cronjob" \
  --container-name="cleanup"
