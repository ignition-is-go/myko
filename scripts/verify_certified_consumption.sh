#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."

cargo fmt -p myko-authority -p myko-federation -p myko-redb -p myko-wire -- --check
cargo test -p myko-authority -p myko-federation -p myko-redb -p myko-wire \
  -j 4 --target-dir target/agent
cargo clippy -p myko-authority -p myko-federation -p myko-redb -p myko-wire \
  --all-targets -j 4 --target-dir target/agent -- -D warnings
cargo test -p myko-node --test scope_continuity -j 4 --target-dir target/agent
