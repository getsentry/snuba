.PHONY: develop setup-git test install-python-dependencies

develop: install-python-dependencies setup-git fetchschemas

setup-git:
	pip install 'pre-commit==2.4.0'
	pre-commit install --install-hooks

test:
	SNUBA_SETTINGS=test py.test -vv

install-python-dependencies:
	pip install -e .

fetchschemas:
	mkdir -p schema
	curl https://getsentry.github.io/relay/event-schema/event.schema.json -o schema/event.schema.json
	mypy snuba > /dev/null || (if [ "$$?" -gt 1 ]; then exit 1; fi)
.PHONY: fetchschemas
