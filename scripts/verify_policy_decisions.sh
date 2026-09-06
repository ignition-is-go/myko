#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."
cargo test -p myko-federation --lib -j 4 --target-dir target/agent
cargo test -p myko --lib server::federated_session -j 4 --target-dir target/agent
cargo test -p myko --test prepared_command_recovery -j 4 --target-dir target/agent
cargo test -p myko-redb --test authority_unavailable --test prepared_live_boundary --test prepared_effect_integrity -j 4 --target-dir target/agent
cargo test -p myko-authority --lib --test certified_history -j 4 --target-dir target/agent
cargo test -p myko-local -p myko-iroh --lib -j 4 --target-dir target/agent
cargo test -p myko-node --test durable_node -j 4 --target-dir target/agent
bash scripts/verify_prepared_authority_runtime.sh
cargo clippy -p myko-redb -p myko-node --all-targets -j 4 --target-dir target/agent -- -D warnings
