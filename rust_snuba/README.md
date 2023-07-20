# Rust impl of consumers (Experimental)

Rust consumers are an experimental project in Snuba. Most users should not be running this code!

The goal of this project is to provide a feature complete Rust equivalent to the `snuba consumer` and `snuba multistorage-consumer` functionality that is currently written in Python code.

## How to run

1. Run `make watch-rust-snuba`.
2. `snuba rust-consumer` can now be used to run a simple Rust consumer that currently does not insert into clickhouse.
