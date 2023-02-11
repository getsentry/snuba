# Rust impl of consumers (Experimental)

## Running

First build the binaries
```
make build-rust
```
or
```
cd rust_snuba
cargo build --all-targets
```

Run the consumer
`cargo run --bin querylog_consumer`
or
`./target/debug/querylog_consumer`
