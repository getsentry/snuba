.PHONY: develop setup-git test install-python-dependencies install-py-dev

pyenv-setup:
	@./scripts/pyenv_setup.sh

develop: install-python-dependencies setup-git

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
	mypy snuba tests --strict --config-file mypy.ini --exclude 'tests/datasets|tests/query|tests/state|tests/snapshots|tests/clickhouse|tests/test_split.py|tests/test_copy_tables.py'

install-python-dependencies:
	pip uninstall -qqy uwsgi  # pip doesn't do well with swapping drop-ins
	pip install `grep ^-- requirements.txt` -r requirements-build.txt
	pip install `grep ^-- requirements.txt` -e .
	pip install `grep ^-- requirements.txt` -r requirements-test.txt

install-py-dev: install-python-dependencies

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
	cd rust_snuba/ && cargo watch -s 'maturin develop'
.PHONY: watch-rust-snuba

test-rust:
	cd rust_snuba/rust_arroyo/ && cargo test
	cd rust_snuba && cargo test
.PHONY: test-rust

lint-rust:
	cd rust_snuba/rust_arroyo/ && cargo clippy -- -D warnings
	cd rust_snuba && cargo clippy -- -D warnings

.PHONY: lint-rust
