#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

/devinfra/scripts/get-cluster-credentials \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-docker.pkg.dev/sentryio/snuba-mr/image:${GO_REVISION_SNUBA_REPO}" \
  --container-name="api" \
  --container-name="consumer" \
  --container-name="dlq-consumer" \
  --container-name="eap-items-consumer" \
  --container-name="eap-items-subscriptions-executor" \
  --container-name="eap-items-subscriptions-scheduler" \
  --container-name="errors-replacer" \
  --container-name="events-subscriptions-executor" \
  --container-name="events-subscriptions-scheduler" \
  --container-name="generic-metrics-counters-consumer" \
  --container-name="generic-metrics-distributions-consumer" \
  --container-name="generic-metrics-sets-consumer" \
  --container-name="genmetrics-counters-subs-scheduler" \
  --container-name="genmetrics-counts-subs-executor" \
  --container-name="genmetrics-distributions-subs-executor" \
  --container-name="genmetrics-distributions-subs-scheduler" \
  --container-name="genmetrics-sets-subs-executor" \
  --container-name="genmetrics-sets-subs-scheduler" \
  --container-name="group-attributes-consumer" \
  --container-name="loadbalancer-outcomes-consumer" \
  --container-name="lw-deletions-search-issues-consumer" \
  --container-name="metrics-consumer" \
  --container-name="metrics-counters-subscriptions-scheduler" \
  --container-name="metrics-sets-subscriptions-scheduler" \
  --container-name="metrics-subscriptions-executor" \
  --container-name="outcomes-billing-consumer" \
  --container-name="outcomes-consumer" \
  --container-name="profile-chunks-consumer" \
  --container-name="profiles-consumer" \
  --container-name="profiling-functions-consumer" \
  --container-name="querylog-consumer" \
  --container-name="replays-consumer" \
  --container-name="search-issues-consumer" \
  --container-name="snuba-admin" \
  --container-name="transactions-consumer-new" \
  --container-name="transactions-subscriptions-executor" \
  --container-name="transactions-subscriptions-scheduler" \
  --container-name="snuba" \
&& k8s-deploy \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-docker.pkg.dev/sentryio/snuba-mr/image:${GO_REVISION_SNUBA_REPO}" \
  --type="cronjob" \
  --container-name="cleanup" \
  --container-name="optimize" \
  --container-name="cardinality-report"
