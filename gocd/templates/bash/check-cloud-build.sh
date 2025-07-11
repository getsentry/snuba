#!/bin/bash

checks-googlecloud-check-cloudbuild \
  sentryio \
  snuba \
  build-on-branch-push \
  ${GO_REVISION_SNUBA_REPO} \
  master
