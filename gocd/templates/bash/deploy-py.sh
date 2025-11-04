#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

/devinfra/scripts/get-cluster-credentials \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-docker.pkg.dev/sentryio/snuba-mr/image:${GO_REVISION_SNUBA_REPO}" \
  --container-name="api" \
  --container-name="dlq-consumer" \
  --container-name="eap-items-subscriptions-executor" \
  --container-name="eap-items-subscriptions-scheduler" \
  --container-name="errors-replacer" \
  --container-name="events-subscriptions-executor" \
  --container-name="events-subscriptions-scheduler" \
  --container-name="genmetrics-counters-subs-scheduler" \
  --container-name="genmetrics-counts-subs-executor" \
  --container-name="genmetrics-distributions-subs-executor" \
  --container-name="genmetrics-distributions-subs-scheduler" \
  --container-name="genmetrics-sets-subs-executor" \
  --container-name="genmetrics-sets-subs-scheduler" \
  --container-name="group-attributes-consumer" \
  --container-name="lw-deletions-search-issues-consumer" \
  --container-name="metrics-counters-subscriptions-scheduler" \
  --container-name="metrics-sets-subscriptions-scheduler" \
  --container-name="metrics-subscriptions-executor" \
  --container-name="outcomes-billing-consumer" \
  --container-name="search-issues-consumer" \
  --container-name="snuba-admin" \
  --container-name="transactions-subscriptions-executor" \
  --container-name="transactions-subscriptions-scheduler" \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-docker.pkg.dev/sentryio/snuba-mr/image:${GO_REVISION_SNUBA_REPO}" \
  --type="cronjob" \
  --container-name="optimize" \
  --container-name="cleanup" \
  --container-name="cardinality-report"
