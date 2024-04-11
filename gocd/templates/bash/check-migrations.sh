#!/bin/bash

deploy_sha=`snuba/scripts/fetch_service_refs.py --pipeline "deploy-snuba-s4s"`
snuba/scripts/check-migrations.py --to $deploy_sha --workdir snuba
