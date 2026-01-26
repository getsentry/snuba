# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Snuba is a service providing a rich data model and query layer on top of ClickHouse, with fast Kafka-based ingestion. Originally developed for Sentry error search and aggregation, it now supports most time series features across multiple datasets.

**Key Technologies:**
- Python 3.11 (main application)
- Rust (performance-critical consumers via PyO3/maturin)
- ClickHouse (data storage)
- Kafka (ingestion)
- Flask (HTTP API + Admin UI)
- React/TypeScript (Admin UI frontend)

## Development Commands

### Setup and Installation

```bash
# Initial setup - install dependencies and build Rust extension
make develop

# Reset Python environment
make reset-python

# Install Rust development dependencies
make install-rs-dev

# Watch Rust code for auto-rebuild during development
make watch-rust-snuba
```

### Running Tests

```bash
# Run Python tests (excludes CI-only tests)
make test
# Or directly:
SNUBA_SETTINGS=test pytest -vv tests -m "not ci_only"

# Run single test file
SNUBA_SETTINGS=test pytest -vv tests/path/to/test_file.py

# Run single test function
SNUBA_SETTINGS=test pytest -vv tests/path/to/test_file.py::test_function_name

# Run API tests
make api-tests

# Run Rust tests
make test-rust

# Run distributed migration tests (requires Docker)
make test-distributed-migrations

# Run admin UI tests
make test-admin
# Or frontend only:
make test-frontend-admin
```

### Linting and Formatting

```bash
# Python (uses ruff, configured in pyproject.toml)
ruff format .
ruff check --fix .

# Type checking
mypy .

# Rust
make lint-rust
make format-rust

# Pre-commit hooks (runs all checks)
pre-commit run --all-files
```

### Running Snuba Locally

```bash
# Run API server
snuba api

# Run admin UI
snuba admin

# Run a consumer
snuba consumer --storage=<storage_name> --consumer-group=<group>

# Run Rust consumer (experimental)
snuba rust-consumer --storage=<storage_name>

# Development server (auto-reload)
snuba devserver
```

### Migrations

```bash
# Apply all pending migrations
snuba migrations migrate --force

# List migration status
snuba migrations list

# Generate migration from storage YAML changes
snuba migrations generate path/to/storage.yaml

# Run specific migration
snuba migrations run --group <group> --migration-id <id>

# Reverse migration
snuba migrations reverse --group <group> --migration-id <id>
```

### Admin UI Development

```bash
# Build admin UI for production
make build-admin

# Watch mode for development
make watch-admin
```

### Configuration Validation

```bash
# Validate YAML configs
make validate-configs
```

## Architecture Overview

### Core Abstraction Layers (Dataset → Entity → Storage)

Snuba uses a three-tier data model abstraction defined in YAML configs:

**Dataset** (`snuba/datasets/configuration/<dataset>/dataset.yaml`)
- Highest level grouping of related data
- Contains multiple entities
- Example: `metrics` dataset groups all metric entity types

**Entity** (`snuba/datasets/configuration/<dataset>/entities/<entity>.yaml`)
- Logical data model that users query against
- Defines abstract column schema visible to users
- Maps to one or more Storage objects
- Contains query processors, validators, and join relationships
- Examples: `events`, `transactions`, `metrics_counters`

**Storage** (`snuba/datasets/configuration/<dataset>/storages/<storage>.yaml`)
- Physical data model mapping to actual ClickHouse tables
- Three types:
  - `ReadableTableStorage`: Query-only tables/views
  - `WritableTableStorage`: Tables supporting both read and write
  - `CdcStorage`: Change Data Capture streams
- Contains physical schema, stream loader config, and allocation policies

**Relationship:** Dataset (1→N) Entity (1→N) Storage

### Query Processing Pipeline

Queries flow through staged pipeline (`snuba/pipeline/`):

1. **Request Parsing** (`snuba/web/query.py`)
   - Parse SnQL/MQL query syntax
   - Build `Request` object with query settings

2. **Entity Processing** (`snuba/pipeline/stages/query_processing.py`)
   - Apply `LogicalQueryProcessor` transformations
   - Validate against entity schema

3. **Storage Selection & Planning**
   - Select appropriate storage(s) for query
   - Build `ClickhouseQueryPlan`
   - Translate logical query to physical ClickHouse query

4. **Storage Processing** (`snuba/pipeline/composite_storage_processing.py`)
   - Apply `ClickhouseQueryProcessor` transformations
   - Add mandatory conditions (project filters, time ranges)
   - Optimize query structure

5. **Execution** (`snuba/pipeline/stages/query_execution.py`)
   - Format to ClickHouse SQL
   - Apply rate limiting via allocation policies
   - Execute via Reader interface
   - Cache results and log query metadata

### Consumer/Ingestion Architecture

**Two consumer implementations:**

**Python Consumer** (`snuba/consumers/`)
- Uses Arroyo framework (Kafka consumer library)
- `ConsumerBuilder` pattern for configuration
- `KafkaConsumerStrategyFactory` defines processing strategy

**Rust Consumer** (`rust_snuba/`) - Preferred for production
- 10x lower memory usage, higher throughput
- Exposed to Python via PyO3/maturin bindings
- Processing steps:
  1. HealthCheck (liveness probe)
  2. Message Transformers (parse JSON, parallel threadpool)
  3. Reduce (batch rows, open ClickHouse connection)
  4. ClickhouseWriterStep (finalize batch, wait for 200 OK)
  5. COGS & CommitLog (produce audit metadata)

**Key Components:**
- `MessageProcessor` (`snuba/processor.py`): Converts Kafka message to ClickHouse row
- `InsertBatch`: Collection of rows with timestamps
- Stream loader: Kafka configuration in storage YAML

### Migration System

**Purpose:** Manage ClickHouse schema evolution across deployments

**Key Concepts:**
- **Migration Groups** (`snuba/migrations/groups.py`): Organize migrations by dataset/feature
- **ClickhouseNodeMigration**: SQL-based schema changes
  - Define `forwards_ops()` and `backwards_ops()`
  - Use operations: `CreateTable`, `AddColumn`, `DropColumn`, etc.
  - Specify `OperationTarget.LOCAL` or `.DISTRIBUTED`
  - Order matters: local before distributed for adds, reverse for drops
- **CodeMigration**: Python-based data migrations
- **Storage Sets** (`snuba/clusters/storage_sets.py`): Group colocated storages
- **Auto-generation**: Modify storage YAML, run `snuba migrations generate`

### Python-Rust Integration

- **Python**: Orchestration, configuration, HTTP API, CLI
- **Rust**: High-performance message processing, batch writing
- Rust code built with maturin, exposed via PyO3 bindings
- Rust environment setup via `. scripts/rust-envvars`

### Admin UI

**Location:** `snuba/admin/`

**Stack:**
- Backend: Flask (`snuba/admin/views.py`)
- Frontend: React/TypeScript (`snuba/admin/static/`)
- Build: Yarn + Webpack

**Features:**
- Migration management
- Query tools (SnQL/MQL, explain plans, cardinality analysis)
- Kafka/DLQ management
- Runtime config editing
- Audit logging

## Configuration

### Environment Variables

Key environment variables (typically set in `.env`):

- `SNUBA_SETTINGS`: Settings module (e.g., `test`, `docker`, `production`)
- `CLICKHOUSE_HOST`: ClickHouse server hostname
- `DEFAULT_BROKERS`: Kafka broker addresses
- `REDIS_HOST`: Redis server for rate limiting/caching
- `USE_REDIS_CLUSTER`: Enable Redis cluster mode

### Settings Files

Settings modules in `snuba/settings/`:
- `base.py`: Default settings
- `test.py`: Test environment overrides
- `docker.py`: Docker environment

### YAML Configuration

Dataset/entity/storage configs in `snuba/datasets/configuration/`:
- Validated on load via JSON schemas
- Run `make validate-configs` to check syntax
- Pre-commit hook validates on commit

## Common Patterns

### Adding a New Storage

1. Create storage YAML in `snuba/datasets/configuration/<dataset>/storages/<name>.yaml`
2. Define schema, stream loader, processors
3. Generate migration: `snuba migrations generate path/to/<name>.yaml`
4. Review generated migration in `snuba/snuba_migrations/`
5. Apply migration: `snuba migrations migrate --force`

### Adding Query Processors

**Entity-level (Logical):**
- Implement `LogicalQueryProcessor` in `snuba/query/processors/`
- Add to entity YAML's `query_processors` section

**Storage-level (Physical):**
- Implement `ClickhouseQueryProcessor` in `snuba/query/processors/`
- Add to storage YAML's `query_processors` section

### Message Processing

1. Define schema in `snuba/datasets/schemas/`
2. Implement `MessageProcessor` subclass
3. Register processor in storage YAML's `stream_loader` section
4. For Rust: implement in `rust_snuba/src/processors/`

## Testing Patterns

### Writing Tests

- Test files in `tests/` mirror source structure
- Use `SNUBA_SETTINGS=test` environment variable
- Fixtures in `tests/fixtures/`
- Use `@pytest.mark.ci_only` for slow/integration tests
- Mock ClickHouse with `tests/base.py` utilities

### Test Database

Tests use in-memory SQLite for migrations state, real ClickHouse for queries (when needed).

## Important Notes

- **Direct commits to master are blocked** by pre-commit hook
- **Rust changes require**: Source `. scripts/rust-envvars` before cargo commands
- **Migration order matters**: Always create local tables before distributed tables, drop in reverse order
- **ClickHouse batching**: Consumers batch inserts (target: 1 INSERT/sec/replica)
- **Storage sets**: Storages in the same set must be colocated on the same ClickHouse cluster
- **Query processors**: Entity processors run before storage selection, storage processors run after

## Documentation

- Full docs: https://getsentry.github.io/snuba/
- Build docs: `make snubadocs`
- Architecture docs in `docs/source/architecture/`
- Configuration docs auto-generated from YAML schemas
