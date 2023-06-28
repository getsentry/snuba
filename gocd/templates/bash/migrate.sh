#!/bin/bash

eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")

/devinfra/scripts/k8s/k8stunnel \
&& /devinfra/scripts/k8s/k8s-spawn-job.py \
  --context="gke_${GCP_PROJECT}_${GKE_REGION}-${GKE_CLUSTER_ZONE}_${GKE_CLUSTER}" \
  --label-selector="service=snuba-admin" \
  --container-name="snuba-admin" \
  "snuba-migrate" "us.gcr.io/sentryio/snuba:${GO_REVISION_SNUBA_REPO}" \
  -- snuba migrations migrate -r complete -r partial
