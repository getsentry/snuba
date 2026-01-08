# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Snuba is a service that provides a rich data model on top of ClickHouse with fast ingestion and query optimization. It serves as the primary data infrastructure for Sentry, supporting time series queries and aggregations across multiple datasets.

## Architecture

- **Hybrid Python/Rust codebase**: Core application in Python with performance-critical consumers written in Rust
- **ClickHouse backend**: Distributed data store with specialized schemas for different data types
- **Kafka ingestion**: Message streaming with consumer groups for data ingestion
- **Dataset-driven design**: Modular architecture where each dataset (errors, transactions, metrics, etc.) has its own processing logic
- **Query language**: Custom SnQL (Snuba Query Language) that translates to ClickHouse SQL

### Key Components

- `snuba/datasets/` - Dataset definitions and processors for different data types
- `snuba/query/` - Query parsing, optimization, and execution engine
- `snuba/consumers/` - Kafka consumer framework and message processing
- `snuba/migrations/` - Database schema migration system
- `rust_snuba/` - Rust-based high-performance consumers
- `snuba/admin/` - React-based admin interface
- `snuba/web/rpc/` - **Protobuf-based RPC query interface (actively developed)**

## Development Commands

### Setup
```bash
make develop          # Full development setup including Rust components
uv sync              # Install Python dependencies
devservices up snuba # Start ClickHouse and other required services
make install-rs-dev  # Build Rust components for development
```

### Testing
```bash
pytest               # Run Python tests
make test-rust       # Run Rust tests
make api-tests       # Test API endpoints specifically
make test-admin      # Test admin interface (Python + JavaScript)
make test-frontend-admin  # JavaScript-only admin tests
```

### Code Quality
```bash
pre-commit run --all-files  # Run all linters and formatters
black .                     # Format Python code
isort .                     # Sort Python imports
flake8                      # Python linting
mypy                        # Type checking
make lint-rust              # Rust linting with clippy
make format-rust            # Format Rust code
```

### Build & Admin Interface
```bash
make build-admin     # Build admin React app for production
make watch-admin     # Development mode with file watching
make validate-configs # Validate dataset configurations
```

### Migrations
```bash
snuba migrations migrate --force  # Apply database migrations
make apply-migrations            # Apply migrations via Makefile
```

### Documentation
```bash
make snubadocs       # Build Sphinx documentation
```

## Protobuf RPC Interface (snuba/web/rpc/)

The RPC interface is the **actively developed query interface** using Protocol Buffers for structured communication. Key aspects:

### Architecture
- **RPCEndpoint base class**: Generic framework for protobuf-based endpoints
- **TraceItemDataResolver**: Resolvers for different trace item types (spans, logs, etc.)
- **Storage Routing**: Intelligent query routing based on data volume and accuracy requirements
- **Visitor Pattern**: Proto message traversal and transformation using visitor pattern

### Key Components
- `snuba/web/rpc/v1/` - Version 1 RPC endpoints (time series, trace tables, etc.)
- `snuba/web/rpc/common/` - Shared utilities for filter conversion and pagination
- `snuba/web/rpc/proto_visitor.py` - Visitor pattern implementation for protobuf traversal
- `snuba/web/rpc/storage_routing/` - Routing strategies for query optimization

### Main Endpoints
- **TimeSeriesRequest** - Time series aggregation queries
- **TraceItemTableRequest** - Tabular trace data with filtering and sorting
- **TraceItemStats** - Statistical analysis of trace items
- **TraceItemDetails** - Detailed trace item information

### Data Flow
1. Protobuf request parsed and validated
2. Storage routing determines optimal query path
3. Proto visitors transform request into internal query format
4. Query executed against ClickHouse
5. Results formatted and returned as protobuf response

## Rust Consumer Architecture

The Rust consumers (`rust_snuba/`) use a pipeline architecture:
1. **HealthCheck** - Kubernetes liveness probe
2. **Message Transformers** - Parse and validate messages (can call Python processors)
3. **Reduce** - Batch rows and stream to ClickHouse
4. **ClickhouseWriterStep** - Finalize batches and handle responses
5. **COGS/CommitLog** - Produce metadata after successful writes

## Query Processing

1. **Parsing** - SnQL/MQL/RPC queries parsed into AST
2. **Entity Resolution** - Map logical entities to physical storages
3. **Query Planning** - Optimize and translate to ClickHouse SQL
4. **Execution** - Run against ClickHouse cluster with result caching

## Key Configuration Files

- `pyproject.toml` - Python dependencies and tool configuration
- `rust_snuba/Cargo.toml` - Rust dependencies and build settings
- `snuba/settings/` - Environment-specific configuration
- `.pre-commit-config.yaml` - Code quality hooks
- `snuba/admin/package.json` - Frontend dependencies

## Testing Strategy

- Unit tests for individual components
- Integration tests with ClickHouse and Kafka
- API endpoint tests
- Distributed migration testing with Docker
- Frontend tests with Jest and React Testing Library
