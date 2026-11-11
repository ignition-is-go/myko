#!/bin/sh
set -eu

pg_bin=$(pg_config --bindir)
history_tmp=$(mktemp -d "${TMPDIR:-/tmp}/myko-history.XXXXXX")
cleanup() {
    "$pg_bin/pg_ctl" -D "$history_tmp/data" -m immediate -w stop >/dev/null 2>&1 || true
    rm -rf "$history_tmp"
}
trap cleanup EXIT HUP INT TERM

mkdir "$history_tmp/socket"
"$pg_bin/initdb" -D "$history_tmp/data" --username=postgres --auth=trust >"$history_tmp/init.log"
"$pg_bin/pg_ctl" -D "$history_tmp/data" -l "$history_tmp/postgres.log" \
    -o "-F -c listen_addresses= -k $history_tmp/socket" -w start
export MYKO_PROVENANCE_TEST_POSTGRES="postgresql://postgres@localhost/postgres?host=$history_tmp/socket"
export MYKO_HISTORY_TEST_URL="$MYKO_PROVENANCE_TEST_POSTGRES"
export RUST_TEST_THREADS=1
cargo test --locked --jobs 4 -p myko-server --lib "$@" -- --ignored --test-threads=1
