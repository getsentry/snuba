#!/bin/bash

/devinfra/scripts/checks/githubactions/checkruns.py \
  getsentry/snuba \
  ${GO_REVISION_SNUBA_REPO} \
  "Tests and code coverage (test)" \
  "Tests and code coverage (test_distributed)" \
  "Tests and code coverage (test_distributed_migrations)" \
  "Dataset Config Validation" \
  "sentry (0)" \
  "sentry (1)" \
  "sentry (2)" \
  "sentry (3)" \
  "sentry (4)" \
  "sentry (5)" \
  "sentry (6)" \
  "sentry (7)" \
  "Tests on multiple clickhouse versions (21.8.13.1.altinitystable)"
