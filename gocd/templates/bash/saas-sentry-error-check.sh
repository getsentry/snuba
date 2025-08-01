#!/bin/bash

checks-sentry-release-error-events \
  --project-id=300688 \
  --project-slug=snuba \
  --release="${GO_REVISION_SNUBA_REPO}" \
  --sentry-environment="${SENTRY_ENVIRONMENT}" \
  --duration=5 \
  --error-events-limit=500 \
  --skip-warnings=true \


# --skip-check=${SKIP_CANARY_CHECKS}
