# rust_snuba

`rust_snuba` is the Rust implementation of Snuba's Kafka consumers. It is compiled with PyO3 and maturin into a Python wheel, and `snuba rust-consumer` launches it from the Python CLI.

For the processing pipeline and batching, see [ARCHITECTURE.md](ARCHITECTURE.md). For how the Rust and Python consumers relate, see [Snuba consumers](../docs/source/architecture/consumer.rst).

## Build

Run these from the repository root after `devenv sync`:

- `make install-rs-dev` builds `rust_snuba` into `.venv` once.
- `make watch-rust-snuba` rebuilds it on every change.

## Test and lint

- `make test-rust` runs `cargo test --workspace`.
- `make lint-rust` runs clippy with warnings as errors.
- `make format-rust` runs `cargo fmt --all`.
