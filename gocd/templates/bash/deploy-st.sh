#!/bin/bash

eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")

/devinfra/scripts/k8s/k8stunnel

/devinfra/scripts/k8s/k8s-deploy.py \
  --context="gke_${GCP_PROJECT}_${GKE_REGION}-${GKE_CLUSTER_ZONE}_${GKE_CLUSTER}" \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us.gcr.io/sentryio/snuba:${GO_REVISION_SNUBA_REPO}" \
  --container-name="snuba" \
  --container-name="snuba-admin"

/devinfra/scripts/k8s/k8s-deploy.py \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us.gcr.io/sentryio/snuba:${GO_REVISION_SNUBA_REPO}" \
  --type="cronjob" \
  --container-name="cleanup"
