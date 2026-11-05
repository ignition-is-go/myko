#!/bin/sh
set -eu
export RUST_TEST_THREADS=1
for task in check-auth-format test-auth test-auth-macros test-auth-graph test-permissions test-auth-client test-auth-server check-auth-client-wasm check-auth-core check-auth check-permissions check-permissions-client-contract; do
    cargo flux run "$task" --no-cache
done
