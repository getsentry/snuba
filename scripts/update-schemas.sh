#!/bin/bash

SCHEMAS_REPO='https://github.com/getsentry/sentry-data-schemas.git'
LATEST_VERSION=$(git ls-remote $SCHEMAS_REPO HEAD | awk '{ print $1}')

REGEX='^git\+https\:\/\/github\.com\/getsentry\/sentry\-data\-schemas\.git\@([a-f0-9]+)\#subdirectory=py'
TEMP='git+https://github.com/getsentry/sentry-data-schemas.git@c123123#subdirectory=py'

echo $TEMP | sed "s/c([1-f0-9]+)/\$LATEST_VERSION/"
