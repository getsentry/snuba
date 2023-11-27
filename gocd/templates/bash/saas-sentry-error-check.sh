#!/bin/bash

/devinfra/scripts/checks/sentry/release_error_events.py \
  --project-id=300688 \
  --project-slug=snuba \
  --release="${GO_REVISION_SNUBA_REPO}" \
  --duration=5 \
  --error-events-limit=500 \
  --skip-warnings=true \


# --skip-check=${SKIP_CANARY_CHECKS}
