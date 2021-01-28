.PHONY: develop setup-git test install-python-dependencies

develop: install-python-dependencies setup-git

setup-git:
	mkdir -p .git/hooks && cd .git/hooks && ln -sf ../../config/hooks/* ./
	pip install 'pre-commit==2.4.0'
	pre-commit install --install-hooks

test:
	SNUBA_SETTINGS=test pytest -vv

tests: test

install-python-dependencies:
	pip install -e .
