#!/bin/bash

/devinfra/scripts/checks/sentry/release_new_issues.py \
  --project-id=300688 \
  --project-slug=snuba \
  --release="${GO_REVISION_SNUBA_REPO}" \
  --new-issues-limit=0 \
  # --skip-check=${SKIP_CANARY_CHECKS}
