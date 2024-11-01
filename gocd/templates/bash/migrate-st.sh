#!/bin/bash

# At the time of writing (2023-06-28) the single tenant deployments
# have been using a different migration process compared to the
# US deployment of snuba.
# This script should be merged with migrate.sh if we can figure
# out a common migration script for all regions.

eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")
/devinfra/scripts/k8s/k8stunnel

/devinfra/scripts/k8s/k8s-spawn-job.py \
  --label-selector="service=${SNUBA_SERVICE_NAME}" \
  --container-name="${SNUBA_SERVICE_NAME}" \
  "snuba-migrate" \
  "us-central1-docker.pkg.dev/sentryio/snuba/image:${GO_REVISION_SNUBA_REPO}" \
  -- \
  snuba migrations migrate --check-dangerous
