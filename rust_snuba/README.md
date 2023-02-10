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
`snuba rust_consumer --storage querylog`
or
`./target/debug/examples/querylog_consumer`
