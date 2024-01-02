#!/bin/bash

eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")
/devinfra/scripts/k8s/k8stunnel

/devinfra/scripts/k8s/k8s-spawn-job.py \
  --context="gke_${GCP_PROJECT}_${GKE_REGION}-${GKE_CLUSTER_ZONE}_${GKE_CLUSTER}" \
  --label-selector="service=${SNUBA_SERVICE_NAME}" \
  --container-name="${SNUBA_SERVICE_NAME}" \
  "snuba-migrate" \
  "ghcr.io/getsentry/snuba:${GO_REVISION_SNUBA_REPO}" \
  -- \
  snuba migrations migrate --check-dangerous -r complete -r partial
