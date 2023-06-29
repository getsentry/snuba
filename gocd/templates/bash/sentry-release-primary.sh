#!/bin/bash

sentry-cli releases deploys "${GO_REVISION_SNUBA_REPO}" new -e production
sentry-cli releases finalize "${GO_REVISION_SNUBA_REPO}"
