#!/bin/bash

deploy_sha=`snuba/scripts/fetch_service_refs.py --pipeline "$PIPELINE_FIRST_STEP"`
snuba/scripts/check-migrations.py --to $deploy_sha --workdir snuba
