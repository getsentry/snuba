#!/bin/bash

sentry-cli releases new "${GO_REVISION_SNUBA_REPO}"
sentry-cli releases set-commits "${GO_REVISION_SNUBA_REPO}" --commit "getsentry/snuba@${GO_REVISION_SNUBA_REPO}"
sentry-cli releases deploys "${GO_REVISION_SNUBA_REPO}" new -e canary
