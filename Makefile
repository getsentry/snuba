.PHONY: develop setup-git test install-python-dependencies install-py-dev

reset-python:
	pre-commit clean
	rm -rf .venv
.PHONY: reset-python

develop: install-python-dependencies install-rs-dev install-brew-dev setup-git

setup-git:
	mkdir -p .git/hooks && cd .git/hooks && ln -sf ../../config/hooks/* ./
	pip install 'pre-commit==2.18.1'
	pre-commit install --install-hooks

test:
	SNUBA_SETTINGS=test pytest -vv tests -v -m "not ci_only"

test-distributed-migrations:
	docker build . -t snuba-test
	SNUBA_IMAGE=snuba-test docker-compose --profile multi_node -f docker-compose.gcb.yml down
	SNUBA_IMAGE=snuba-test SNUBA_SETTINGS=test_distributed_migrations docker-compose --profile multi_node -f docker-compose.gcb.yml up -d
	SNUBA_IMAGE=snuba-test SNUBA_SETTINGS=test_distributed_migrations TEST_LOCATION=test_distributed_migrations docker-compose --profile multi_node -f docker-compose.gcb.yml run --rm snuba-test

test-initialization:
	docker build . -t snuba-test
	SNUBA_IMAGE=snuba-test docker-compose -f docker-compose.gcb.yml down
	SNUBA_IMAGE=snuba-test SNUBA_SETTINGS=test_initialization docker-compose -f docker-compose.gcb.yml up -d
	SNUBA_IMAGE=snuba-test SNUBA_SETTINGS=test_initialization TEST_LOCATION=test_initialization docker-compose -f docker-compose.gcb.yml run --rm snuba-test

test-distributed:
	docker build . -t snuba-test
	SNUBA_IMAGE=snuba-test docker-compose -f docker-compose.gcb.yml down
	SNUBA_IMAGE=snuba-test SNUBA_SETTINGS=test_distributed docker-compose -f docker-compose.gcb.yml run --rm snuba-test

tests: test

api-tests:
	SNUBA_SETTINGS=test pytest -vv tests/*_api.py

backend-typing:
	mypy snuba tests scripts --strict --config-file mypy.ini --exclude 'tests/datasets|tests/query|tests/test_split.py'

install-python-dependencies:
	pip uninstall -qqy uwsgi  # pip doesn't do well with swapping drop-ins
	pip install `grep ^-- requirements.txt` -r requirements-build.txt
	pip install `grep ^-- requirements.txt` -e .
	pip install `grep ^-- requirements.txt` -r requirements-test.txt
.PHONY: install-python-dependencies

# install-rs-dev/install-py-dev mimick sentry's naming conventions
install-rs-dev:
	which cargo || (echo "!!! You need an installation of Rust in order to develop snuba. Go to https://rustup.rs to get one." && exit 1)
	. scripts/rust-envvars && cd rust_snuba/ && maturin develop
.PHONY: install-rs-dev

install-py-dev: install-python-dependencies
.PHONY: install-py-dev

install-brew-dev:
	brew bundle
.PHONY: install-brew-dev

snubadocs:
	pip install -U -r ./docs-requirements.txt
	sphinx-build -W -b html docs/source docs/build

build-admin:
	cd snuba/admin && yarn install && yarn run build

watch-admin:
	cd snuba/admin && yarn install && yarn run watch

test-admin:
	cd snuba/admin && yarn install && yarn run test
	SNUBA_SETTINGS=test pytest -vv tests/admin/

test-frontend-admin:
	cd snuba/admin && yarn install && yarn run test

validate-configs:
	python3 snuba/validate_configs.py

generate-config-docs:
	pip install -U -r ./docs-requirements.txt
	python3 -m snuba.datasets.configuration.generate_config_docs

watch-rust-snuba:
	. scripts/rust-envvars && \
		cd rust_snuba/ && cargo watch -s 'maturin develop'
.PHONY: watch-rust-snuba

test-rust:
	. scripts/rust-envvars && \
		cd rust_snuba && \
		cargo test --workspace
.PHONY: test-rust

lint-rust:
	. scripts/rust-envvars && \
		cd rust_snuba && \
		cargo clippy --workspace --all-targets --no-deps -- -D warnings
.PHONY: lint-rust

format-rust:
	. scripts/rust-envvars && \
		cd rust_snuba && \
		cargo +stable fmt --all
.PHONY: format-rust

format-rust-ci:
	. scripts/rust-envvars && \
		cd rust_snuba && \
		cargo +stable fmt --all --check
.PHONY: format-rust-ci

gocd:
	rm -rf ./gocd/generated-pipelines
	mkdir -p ./gocd/generated-pipelines
	cd ./gocd/templates && jb install && jb update
	find . -type f \( -name '*.libsonnet' -o -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnetfmt -i
	find . -type f \( -name '*.libsonnet' -o -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnet-lint -J ./gocd/templates/vendor
	cd ./gocd/templates && jsonnet --ext-code output-files=true -J vendor -m ../generated-pipelines ./snuba.jsonnet
	cd ./gocd/generated-pipelines && find . -type f \( -name '*.yaml' \) -print0 | xargs -n 1 -0 yq -p json -o yaml -i
.PHONY: gocd
