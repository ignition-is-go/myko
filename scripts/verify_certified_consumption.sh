#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."

packages=(-p myko-authority -p myko-federation -p myko-redb -p myko-wire -p myko -p myko-local -p myko-iroh -p myko-swift -p myko-server)

cargo fmt "${packages[@]}" -- --check
cargo test "${packages[@]}" \
  --features myko-swift/native-ffi -j 4 --target-dir target/agent
cargo clippy "${packages[@]}" \
  --features myko-swift/native-ffi --all-targets -j 4 --target-dir target/agent -- -D warnings
cargo test -p myko-node --test scope_continuity -j 4 --target-dir target/agent
