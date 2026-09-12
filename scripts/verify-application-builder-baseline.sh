#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Component checks do not by themselves prove mesh failover.
cargo test -p myko-items --target-dir target/agent -j 4
cargo test -p myko-items --features schema --target-dir target/agent -j 4
cargo test -p myko-iroh -p myko-node --features schema --test generated_schema \
  --target-dir target/agent -j 4
cargo test -p myko-wire --test handler_contract --target-dir target/agent -j 4
cargo test -p myko-iroh --features schema --test handler_contract \
  --target-dir target/agent -j 4
cargo test -p myko-iroh --features schema --test handler_open_contract \
  --target-dir target/agent -j 4
cargo test -p myko-iroh --test handler_contract --target-dir target/agent -j 4
cargo test -p myko --features schema --lib application::service_contract \
  --target-dir target/agent -j 4
cargo test -p myko --features schema --test prepared_command_recovery \
  --target-dir target/agent -j 4
cargo test -p myko --features schema --lib server::federated_source::tests \
  --target-dir target/agent -j 4
cargo test -p myko --features schema --lib core::report::output::tests \
  --target-dir target/agent -j 4
cargo test -p myko-node --features schema --test handler_namespaces \
  --target-dir target/agent -j 4
cargo test -p myko-local --features myko/schema --lib --target-dir target/agent -j 4
cargo test -p myko-federation --test command_scope_readiness \
  --target-dir target/agent -j 4
cargo test -p myko-redb --lib tests::incomplete_scope_admission_gate_survives_reopen \
  --target-dir target/agent -j 4 -- --exact
cargo test -p myko-federation --test execution_assignment \
  --target-dir target/agent -j 4
cargo test -p myko-redb --test execution_controller \
  --target-dir target/agent -j 4
cargo test -p myko-federation --test handler_service_identity \
  --target-dir target/agent -j 4
cargo test -p myko-wire --lib --target-dir target/agent -j 4
cargo test -p myko --lib client::durable_handler::tests \
  --target-dir target/agent -j 4
cargo test -p myko --lib core::query::filter::tests \
  --target-dir target/agent -j 4
cargo test -p myko-local --lib --target-dir target/agent -j 4
cargo test -p myko-authority --test control_realms --target-dir target/agent -j 4
cargo test -p myko-iroh --test execution_evidence --target-dir target/agent -j 4
cargo test -p myko-iroh --test execution_coordination --target-dir target/agent -j 4
cargo test -p myko-node --test scope_continuity \
  replacement_node_materializes_scope_after_founder_and_relay_leave \
  --target-dir target/agent -j 4 -- --exact

printf '%s\n' 'Baseline passed. Stable-handle failover and the full contract remain separate acceptance checks.'
