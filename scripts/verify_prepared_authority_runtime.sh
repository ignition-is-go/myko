#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."
cargo test -p myko-authority --test certified_coordinator prepared_runtime -j 4 --target-dir target/agent
cargo test -p myko-authority --test certified_coordinator prepared_lifecycle -j 4 --target-dir target/agent
cargo test -p myko-authority --test certified_coordinator certified_approval -j 4 --target-dir target/agent
cargo test -p myko-federation --test control_chain -j 4 --target-dir target/agent
cargo clippy -p myko-authority -p myko-federation --all-targets -j 4 --target-dir target/agent -- -D warnings
