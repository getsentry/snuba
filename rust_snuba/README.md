# Rust impl of consumers (Experimental)

Rust consumers are an experimental project in Snuba. Most users should not be running this code!

The goal of this project is to provide a feature complete Rust equivalent to the `snuba consumer` and `snuba multistorage-consumer` functionality that is currently written in Python code.

## How to run

1. Run `make watch-rust-snuba`.
2. `snuba rust-consumer` can now be used to run a simple Rust consumer that currently does not insert into clickhouse.

**Debug**
To run with a debugger
1. ensure you have CodeLLDB vscode extension (vadimcn.vscode-lldb)
2. `make watch-rust-snuba` -- build & watch rust
2. go to the vscode debug tab and find the target (we only have eap items at this point)
3. you can use `scripts/generate_items.py` to publish to the topic

## Troubleshooting
* Make sure `devservices up`
* If it says you are missing the kafka topic: `snuba bootstrap --force`
