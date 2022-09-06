.PHONY: develop setup-git test install-python-dependencies

pyenv-setup:
	@./scripts/pyenv_setup.sh

develop: install-python-dependencies setup-git

setup-git:
	mkdir -p .git/hooks && cd .git/hooks && ln -sf ../../config/hooks/* ./
	pip install 'pre-commit==2.18.1'
	pre-commit install --install-hooks

test:
	SNUBA_SETTINGS=test pytest -vv -v -m "not ci_only"

tests: test

api-tests:
	SNUBA_SETTINGS=test pytest -vv tests/*_api.py

backend-typing:
	mypy snuba tests --strict --config-file mypy.ini --exclude 'tests/datasets|tests/query|tests/state|tests/snapshots|tests/clickhouse|tests/test_split.py|tests/test_consumer.py'

install-python-dependencies:
	pip uninstall -qqy uwsgi  # pip doesn't do well with swapping drop-ins
	pip install `grep ^-- requirements.txt` -e .
	pip install `grep ^-- requirements.txt` -r requirements-test.txt

snubadocs:
	pip install -U -r ./docs-requirements.txt
	sphinx-build -W -b html docs/source docs/build

build-admin:
	cd snuba/admin && yarn install && yarn run build

watch-admin:
	cd snuba/admin && yarn install && yarn run watch
