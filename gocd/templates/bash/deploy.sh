#!/bin/bash

eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")

/devinfra/scripts/k8s/k8stunnel \
&& /devinfra/scripts/k8s/k8s-deploy.py \
  --label-selector="${LABEL_SELECTOR}" \
  --image="us-central1-docker.pkg.dev/sentryio/snuba/image:${GO_REVISION_SNUBA_REPO}" \
  --container-name="api" \
  --container-name="consumer" \
  --container-name="errors-consumer" \
  --container-name="errors-replacer" \
  --container-name="events-subscriptions-executor" \
  --container-name="events-subscriptions-scheduler" \
  --container-name="generic-metrics-counters-consumer" \
  --container-name="generic-metrics-counters-subscriptions-executor" \
  --container-name="generic-metrics-counters-subscriptions-scheduler" \
  --container-name="generic-metrics-distributions-consumer" \
  --container-name="generic-metrics-distributions-subscriptions-executor" \
  --container-name="generic-metrics-distributions-subscriptions-scheduler" \
  --container-name="generic-metrics-gauges-subscriptions-scheduler" \
  --container-name="generic-metrics-gauges-subscriptions-executor" \
  --container-name="generic-metrics-gauges-consumer" \
  --container-name="generic-metrics-sets-consumer" \
  --container-name="generic-metrics-sets-subscriptions-executor" \
  --container-name="generic-metrics-sets-subscriptions-scheduler" \
  --container-name="loadbalancer-outcomes-consumer" \
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
  --container-name="replacer" \
  --container-name="replays-consumer" \
  --container-name="search-issues-consumer" \
  --container-name="snuba-admin" \
  --container-name="transactions-consumer-new" \
  --container-name="transactions-subscriptions-executor" \
  --container-name="transactions-subscriptions-scheduler" \
  --container-name="spans-consumer" \
  --container-name="rust-querylog-consumer" \
  --container-name="rust-errors-consumer" \
  --container-name="rust-metrics-consumer" \
  --container-name="spans-exp-static-off" \
  --container-name="dlq-consumer" \
  --container-name="group-attributes-consumer" \
  --container-name="metrics-summaries-consumer" \
  --container-name="eap-spans-consumer" \
  --container-name="eap-mutations-consumer" \
  --container-name="eap-spans-profiled-consumer" \
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
