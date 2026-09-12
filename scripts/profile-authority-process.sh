#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."
grant_repo=$(pwd -P)
grant_pid=${1-}
case "$grant_pid" in
  ''|*[!0-9]*) echo 'Usage: bash scripts/profile-authority-process.sh TEST_PID' >&2; exit 2 ;;
esac
grant_executable=$(readlink -f "/proc/$grant_pid/exe")
case "$grant_executable" in
  "$grant_repo"/target/agent/debug/deps/certified_coordinator-*) ;;
  *) echo "PID is not this checkout's certified_coordinator test binary" >&2; exit 2 ;;
esac

grant_profile=$(mktemp -d "$grant_repo/target/agent/authority-profile.XXXXXX")
echo "Recording eight seconds from test PID $grant_pid into $grant_profile"
perf record --event cpu-clock --freq 49 --call-graph dwarf \
  --pid "$grant_pid" --output "$grant_profile/perf.data" -- sleep 8
perf report --stdio --no-children --percent-limit 1 \
  --input "$grant_profile/perf.data"
