#!/bin/bash

checks-sentry-release-new-issues \
  --project-id=300688 \
  --project-slug=snuba \
  --release="${GO_REVISION_SNUBA_REPO}" \
  --new-issues-limit=0 \
  --additional-query="issue.type:error !level:info" \


# --skip-check=${SKIP_CANARY_CHECKS}
