#!/bin/bash

# At the time of writing (2023-06-28) the single tenant deployments
# have been using a different migration process compared to the
# US deployment of snuba.
# This script should be merged with migrate.sh if we can figure
# out a common migration script for all regions.

eval $(regions-project-env-vars --region="${SENTRY_REGION}")
/devinfra/scripts/get-cluster-credentials

k8s-spawn-job \
  --label-selector="service=${SNUBA_SERVICE_NAME}" \
  --container-name="${SNUBA_SERVICE_NAME}" \
  "snuba-migrate" \
  "us-docker.pkg.dev/sentryio/snuba-mr/image:${GO_REVISION_SNUBA_REPO}" \
  -- \
  snuba migrations migrate --check-dangerous
