.PHONY: develop setup-git test install-python-dependencies

develop: install-python-dependencies setup-git

setup-git:
	pip install 'pre-commit==2.4.0'
	pre-commit install --install-hooks

test:
	SNUBA_SETTINGS=test py.test -vv

install-python-dependencies:
	pip install -e .
