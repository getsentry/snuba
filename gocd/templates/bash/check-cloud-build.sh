#!/bin/bash

/devinfra/scripts/checks/googlecloud/check_cloudbuild.py \
  sentryio \
  snuba \
  build-on-branch-push \
  ${GO_REVISION_SNUBA_REPO} \
  master
