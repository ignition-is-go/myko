#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."

case "${1-}" in
  '') ;;
  --trace)
    export MYKO_AUTHORITY_TRACE=1
    export RUST_LOG=myko::server::federated_session=trace,myko_authority::certified::coordinator=debug,myko_redb=debug,myko_iroh::evidence_client=debug
    mkdir -p target/agent
    grant_trace=$(mktemp target/agent/authority-lifecycle.XXXXXX.log)
    echo "Request trace: $grant_trace"
    exec > "$grant_trace" 2>&1
    ;;
  *) echo 'Usage: bash scripts/measure-authority-lifecycle.sh [--trace]' >&2; exit 2 ;;
esac

# Checks real grant revocation/recovery for item, query, report, and view owners.
# Prints command, subscription, denial, and shutdown timings.
cargo test -p myko-authority --features schema,myko-node/schema \
  --test certified_coordinator grant_subscriptions \
  --target-dir target/agent -j 4 -- --nocapture
