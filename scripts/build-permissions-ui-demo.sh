#!/bin/sh
set -eu
repository=$(pwd)
export CARGO_TARGET_DIR="$repository/target/agent"
cd libs/myko/permissions-leptos
trunk build examples/demo/index.html --example permissions-demo --dist "$repository/target/permissions-ui-demo"
