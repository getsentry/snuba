#!/bin/bash

/devinfra/scripts/checks/googlecloud/checkcloudbuild.py \
  ${GO_REVISION_SNUBA_REPO} \
  sentryio \
  "us-central1-docker.pkg.dev/sentryio/snuba/image"
