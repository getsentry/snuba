.PHONY: develop setup-git test install-python-dependencies

pyenv-setup:
	@./scripts/pyenv_setup.sh

develop: install-python-dependencies setup-git

setup-git:
	mkdir -p .git/hooks && cd .git/hooks && ln -sf ../../config/hooks/* ./
	pip install 'pre-commit==2.9.0'
	pre-commit install --install-hooks

test:
	SNUBA_SETTINGS=test pytest -vv -v -m "not ci_only"

tests: test

api-tests:
	SNUBA_SETTINGS=test pytest -vv tests/*_api.py

install-python-dependencies:
	# This installs confluent-kafka from our GC storage since there's no arm64 wheel
	# https://github.com/confluentinc/confluent-kafka-python/issues/1190
	# This is a temp fix for developers on Big Sur
	pip install https://storage.googleapis.com/python-arm64-wheels/confluent_kafka-1.5.0-cp38-cp38-macosx_11_0_arm64.whl
	pip install -e .
	pip install -r requirements-test.txt

snubadocs:
	pip install -U -r ./docs-requirements.txt
	sphinx-build -W -b html docs/source docs/build

build-admin:
	cd snuba/admin && yarn install && yarn run build

watch-admin:
	cd snuba/admin && yarn install && yarn run watch
