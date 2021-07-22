.PHONY: develop setup-git test install-python-dependencies

pyenv-setup:
	@./scripts/pyenv_setup.sh

develop: install-python-dependencies setup-git

setup-git:
	mkdir -p .git/hooks && cd .git/hooks && ln -sf ../../config/hooks/* ./
	pip install 'pre-commit==2.9.0'
	pre-commit install --install-hooks

test:
	SNUBA_SETTINGS=test pytest -vv

tests: test

api-tests:
	SNUBA_SETTINGS=test pytest -vv tests/*_api.py

install-python-dependencies:
	pip install -e .
	pip install -r requirements-test.txt

snubadocs:
	pip install -U -r ./docs-requirements.txt
	sphinx-build -W -b html docs/source docs/build
