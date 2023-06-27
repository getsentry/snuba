#!/bin/bash

/devinfra/scripts/checks/googlecloud/checkcloudbuild.py \
  ${GO_REVISION_SNUBA_REPO} \
  sentryio \
  "us.gcr.io/sentryio/snuba"
