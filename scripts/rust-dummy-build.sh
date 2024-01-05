#!/bin/sh
set -ex

cd ./rust_snuba/
mkdir -p ./target/wheels/
for f in ./src/lib.rs ./rust_arroyo/src/lib.rs ./benches/processors.rs; do
    mkdir -p "`dirname $f`"
    touch "$f"
done
