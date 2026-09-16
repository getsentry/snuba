#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

IMAGE_TAG="${GO_REVISION_SNUBA_REPO}"

echo "TODO drop"
echo "We are collapsing this into the python deploy script, check that"
