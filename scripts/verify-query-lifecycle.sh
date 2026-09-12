#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."

cargo test -p myko -p myko-local -p myko-iroh --features schema --lib handler_authorization \
  --target-dir target/agent -j 4
cargo test -p myko-iroh --features schema --test handler_contract --test handler_open_contract \
  --test handler_authorization \
  --target-dir target/agent -j 4
cargo test -p myko-authority --features schema,myko-node/schema --lib \
  --test certified_history --test certified_consumption --test controller_rotation \
  --target-dir target/agent -j 4
bash scripts/measure-authority-lifecycle.sh
cargo test -p myko-macros --lib --target-dir target/agent -j 4
cargo test -p myko-federation --lib --target-dir target/agent -j 4
cargo test -p myko --features schema --lib window_publication \
  --target-dir target/agent -j 4
cargo test -p myko --features schema --lib --tests \
  --target-dir target/agent -j 4
cargo test -p myko-local --lib --target-dir target/agent -j 4
cargo test -p myko-node --features schema \
  --test handler_namespaces --test scope_continuity --target-dir target/agent -j 4
