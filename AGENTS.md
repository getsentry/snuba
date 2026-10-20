# Snuba

Snuba is Sentry's query and ingestion service on top of ClickHouse. The Python service is in `snuba/`, and the Rust Kafka consumers are in `rust_snuba/`.

## Commands

Run these from the repository root.

- `devenv sync` syncs `.venv` and installs pre-commit hooks. `make install-rs-dev` builds `rust_snuba` into `.venv`.
- `devservices up` starts ClickHouse, Redis, and Kafka. The tests need them.
- `make test` runs the Python tests. Run one file with `SNUBA_SETTINGS=test pytest tests/path/to/test_file.py`.
- `make test-rust` runs the Rust tests.
- `make lint-check` and `make typecheck` run ruff and mypy. `pre-commit run --files <files>` runs the commit hooks.
- `make lint-rust` and `make format-rust` run clippy and rustfmt.
- `make validate-configs` validates the dataset YAML in `snuba/datasets/configuration/`.
- `make generate-config-docs` and then `make snubadocs` build the Sphinx docs. Warnings fail the build.
- `make listen-metrics` prints the DogStatsD metrics Snuba emits locally.

## Invariants

- After you change Rust code, rerun `make install-rs-dev` before Python code or tests can use it.
- Never edit `docs/source/configuration/*.md` by hand. `make generate-config-docs` writes them from `snuba/datasets/configuration/json_schema.py`.
- Add every new page under `docs/source/` to a toctree, or the docs build fails.
- Snuba assumes UTC everywhere.

## Docs

| Topic | Doc |
| --- | --- |
| Local setup and running from source | [docs/source/contributing/environment.rst](docs/source/contributing/environment.rst) |
| Services Snuba needs, settings | [docs/source/getstarted.rst](docs/source/getstarted.rst) |
| Architecture and data flows | [docs/source/architecture/overview.rst](docs/source/architecture/overview.rst) |
| Datasets, entities, storages | [docs/source/architecture/datamodel.rst](docs/source/architecture/datamodel.rst) |
| Query processing pipeline | [docs/source/architecture/queryprocessing.rst](docs/source/architecture/queryprocessing.rst) |
| Python and Rust consumers, healthchecks | [docs/source/architecture/consumer.rst](docs/source/architecture/consumer.rst) |
| Rust consumer pipeline and batching | [rust_snuba/ARCHITECTURE.md](rust_snuba/ARCHITECTURE.md) |
| Building and testing `rust_snuba` | [rust_snuba/README.md](rust_snuba/README.md) |
| Dataset YAML schemas | [docs/source/configuration/overview.rst](docs/source/configuration/overview.rst) |
| HTTP query API, subscriptions | [docs/source/query/overview.rst](docs/source/query/overview.rst), routes in [snuba/web/views.py](snuba/web/views.py) |
| SnQL and MQL | [docs/source/language/snql.rst](docs/source/language/snql.rst), [docs/source/language/mql.rst](docs/source/language/mql.rst) |
| Running and writing migrations | [docs/source/migrations/overview.md](docs/source/migrations/overview.md) |
| Local and distributed migration modes | [docs/source/migrations/modes.rst](docs/source/migrations/modes.rst) |
| ClickHouse topology, schema design, versions | [docs/source/clickhouse/](docs/source/clickhouse/) |
| Data slicing | [docs/source/architecture/slicing.rst](docs/source/architecture/slicing.rst) |
| Allocation policy config overrides | [snuba/query/allocation_policies/OVERRIDES.md](snuba/query/allocation_policies/OVERRIDES.md) |
| Admin UI | [snuba/admin/README.md](snuba/admin/README.md) |
| GoCD pipelines | [gocd/README.md](gocd/README.md) |
| Profiling | [docs/source/profiler.rst](docs/source/profiler.rst) |
