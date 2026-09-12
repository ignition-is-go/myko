#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."

# A passing diagnostic reproduces unsafe empty Current through a typed handler.
# This is not a successful failover acceptance test.
cargo test -p myko-iroh --test execution_coordination \
  history_boundary::fresh_assignment_quorum_does_not_certify_application_history \
  --target-dir target/agent -j 4 -- --exact --nocapture
