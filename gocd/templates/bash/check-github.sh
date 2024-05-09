#!/bin/bash

/devinfra/scripts/checks/githubactions/checkruns.py \
  --timeout-mins 60 \
  getsentry/snuba \
  ${GO_REVISION_SNUBA_REPO} \
  "Tests and code coverage (test)" \
  "Tests and code coverage (test_distributed)" \
  "Tests and code coverage (test_distributed_migrations)" \
  "Dataset Config Validation" \
  "sentry (0)" \
  "sentry (1)" \
  "sentry (2)" \
  "sentry (3)"
