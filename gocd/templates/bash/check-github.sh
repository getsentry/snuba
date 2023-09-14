#!/bin/bash

/devinfra/scripts/checks/githubactions/checkruns.py \
  getsentry/snuba \
  ${GO_REVISION_SNUBA_REPO} \
  "Tests and code coverage (test)" \
  "Tests and code coverage (test_distributed)" \
  "Tests and code coverage (test_distributed_migrations)" \
  "Dataset Config Validation" \
  "sentry"
