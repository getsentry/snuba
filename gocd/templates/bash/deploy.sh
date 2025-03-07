#!/bin/bash

eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")

/devinfra/scripts/k8s/k8stunnel \
&& /devinfra/scripts/k8s/k8s-deploy.py \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-central1-docker.pkg.dev/sentryio/snuba/image:${GO_REVISION_SNUBA_REPO}" \
  --container-name="api" \
  --container-name="consumer" \
  --container-name="dlq-consumer" \
  --container-name="eap-items-log-consumer" \
  --container-name="eap-items-span-consumer" \
  --container-name="eap-mutations-consumer" \
  --container-name="eap-spans-consumer" \
  --container-name="eap-spans-profiled-consumer" \
  --container-name="eap-spans-subscriptions-executor" \
  --container-name="eap-spans-subscriptions-scheduler" \
  --container-name="errors-replacer" \
  --container-name="events-subscriptions-executor" \
  --container-name="events-subscriptions-scheduler" \
  --container-name="generic-metrics-counters-consumer" \
  --container-name="generic-metrics-distributions-consumer" \
  --container-name="generic-metrics-gauges-consumer" \
  --container-name="generic-metrics-sets-consumer" \
  --container-name="genmetrics-counters-subs-scheduler" \
  --container-name="genmetrics-counts-subs-executor" \
  --container-name="genmetrics-distributions-subs-executor" \
  --container-name="genmetrics-distributions-subs-scheduler" \
  --container-name="genmetrics-gauges-subs-executor" \
  --container-name="genmetrics-gauges-subs-scheduler" \
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
  --container-name="spans-consumer" \
  --container-name="transactions-consumer-new" \
  --container-name="transactions-subscriptions-executor" \
  --container-name="transactions-subscriptions-scheduler" \
  --container-name="uptime-results-consumer" \
  --container-name="snuba" \
&& /devinfra/scripts/k8s/k8s-deploy.py \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-central1-docker.pkg.dev/sentryio/snuba/image:${GO_REVISION_SNUBA_REPO}" \
  --type="cronjob" \
  --container-name="cleanup" \
  --container-name="optimize" \
  --container-name="cardinality-report" \
&& /devinfra/scripts/k8s/k8s-deploy.py \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-central1-docker.pkg.dev/sentryio/snuba/image:${GO_REVISION_SNUBA_REPO}" \
  --type="statefulset" \
  --container-name="spans-exp-static-on"
