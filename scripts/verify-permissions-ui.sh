#!/bin/sh
set -eu
export RUST_TEST_THREADS=1
for task in check-auth-format test-permissions-ui-backend test-permissions-ui-client test-permissions-ui-bridge check-permissions-ui-bridge test-permissions-ui check-permissions-ui-wasm check-permissions-ui-backend check-auth-core check-permissions-ui; do
    cargo flux run "$task" --no-cache
done
