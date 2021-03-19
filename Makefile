.PHONY: develop setup-git test install-python-dependencies

develop: install-python-dependencies setup-git

setup-git:
	mkdir -p .git/hooks && cd .git/hooks && ln -sf ../../config/hooks/* ./
	pip install 'pre-commit==2.4.0'
	pre-commit install --install-hooks

test:
	SNUBA_SETTINGS=test pytest -vv

tests: test

api-tests:
	SNUBA_SETTINGS=test pytest -vv tests/*_api.py

install-python-dependencies:
	pip install -e .
	pip install -r test-requirements.txt
