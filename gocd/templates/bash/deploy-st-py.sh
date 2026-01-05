#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

/devinfra/scripts/get-cluster-credentials \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-docker.pkg.dev/sentryio/snuba-mr/image:${GO_REVISION_SNUBA_REPO}" \
  --container-name="api" \
  --container-name="eap-items-subscriptions-executor" \
  --container-name="eap-items-subscriptions-scheduler" \
  --container-name="errors-replacer" \
  --container-name="events-subscriptions-consumer" \
  --container-name="generic-metrics-counters-subscriptions-executor" \
  --container-name="generic-metrics-counters-subscriptions-scheduler" \
  --container-name="generic-metrics-distributions-subscriptions-executor" \
  --container-name="generic-metrics-distributions-subscriptions-scheduler" \
  --container-name="generic-metrics-sets-subscriptions-executor" \
  --container-name="generic-metrics-sets-subscriptions-scheduler" \
  --container-name="generic-metrics-gauges-subscriptions-executor" \
  --container-name="generic-metrics-gauges-subscriptions-scheduler" \
  --container-name="group-attributes-consumer" \
  --container-name="lw-deletions-search-issues-consumer" \
  --container-name="lw-deletions-eap-items-consumer" \
  --container-name="metrics-counters-subscriptions-scheduler" \
  --container-name="metrics-sets-subscriptions-scheduler" \
  --container-name="metrics-subscriptions-executor" \
  --container-name="search-issues-consumer" \
  --container-name="snuba-admin" \
  --container-name="transactions-subscriptions-consumer" \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-docker.pkg.dev/sentryio/snuba-mr/image:${GO_REVISION_SNUBA_REPO}" \
  --type="cronjob" \
  --container-name="optimize" \
  --container-name="cleanup" \
  --container-name="cardinality-report"
