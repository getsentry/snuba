.PHONY: develop setup-git test install-python-dependencies

develop: install-python-dependencies setup-git fetch-and-validate-schema

setup-git:
	pip install 'pre-commit==2.4.0'
	pre-commit install --install-hooks

test:
	SNUBA_SETTINGS=test py.test -vv

install-python-dependencies:
	pip install -e .

fetch-and-validate-schema:
	mkdir -p schema
	curl https://raw.githubusercontent.com/getsentry/sentry-data-schemas/main/relay/event.schema.json -o schema/event.schema.json
	mypy snuba > /dev/null || (if [ "$$?" -gt 1 ]; then exit 1; fi)
.PHONY: fetch-and-validate-schema
