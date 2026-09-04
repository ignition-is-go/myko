#!/usr/bin/env bash

set -euo pipefail

mode="${1:-final}"
failures=0

fail() {
  printf 'FAIL: %s\n' "$1"
  failures=$((failures + 1))
}

pass() {
  printf 'PASS: %s\n' "$1"
}

require_absent() {
  local label="$1"
  local pattern="$2"
  shift 2
  local paths=()
  local path

  for path in "$@"; do
    if [[ -e "$path" ]]; then
      paths+=("$path")
    fi
  done

  if ((${#paths[@]} == 0)); then
    pass "$label"
    return
  fi

  if rg -n -U --glob '*.rs' --glob '*.toml' "$pattern" "${paths[@]}" >/dev/null; then
    fail "$label"
  else
    local status=$?
    if ((status == 1)); then
      pass "$label"
    else
      fail "$label (checker error $status)"
    fi
  fi
}

require_present() {
  local label="$1"
  local pattern="$2"
  shift 2

  if rg -n -U --glob '*.rs' --glob '*.toml' "$pattern" "$@" >/dev/null; then
    pass "$label"
  else
    local status=$?
    if ((status == 1)); then
      fail "$label"
    else
      fail "$label (checker error $status)"
    fi
  fi
}

require_path_absent() {
  local label="$1"
  local path="$2"

  if [[ -e "$path" ]]; then
    fail "$label"
  else
    pass "$label"
  fi
}

require_path_present() {
  local label="$1"
  local path="$2"

  if [[ -e "$path" ]]; then
    pass "$label"
  else
    fail "$label"
  fi
}

require_max_lines() {
  local label="$1"
  local maximum="$2"
  local path="$3"
  local lines

  if [[ ! -f "$path" ]]; then
    fail "$label (missing $path)"
    return
  fi

  lines=$(wc -l < "$path")
  if ((lines <= maximum)); then
    pass "$label"
  else
    fail "$label ($path has $lines lines; maximum is $maximum)"
  fi
}

check_modularity() {
  require_max_lines \
    'federation crate root only wires owned modules' \
    300 \
    libs/myko/federation/src/lib.rs
  require_max_lines \
    'Iroh crate root only wires owned modules' \
    300 \
    libs/myko/iroh/src/lib.rs
  require_max_lines \
    'authority crate root only wires owned modules' \
    300 \
    libs/myko/authority/src/lib.rs
  require_max_lines \
    'local transport crate root only wires owned modules' \
    300 \
    libs/myko/local/src/lib.rs
  require_path_present \
    'authority evaluator has its own module' \
    libs/myko/authority/src/evaluator.rs
  require_present \
    'authority evaluation uses a typed staged context' \
    'struct EvaluationContext' \
    libs/myko/authority/src/evaluator.rs
  require_present \
    'authority evaluation has explicit grant and delegation stages' \
    'fn (resolve_grants|resolve_delegations)' \
    libs/myko/authority/src/evaluator.rs
  require_absent \
    'authority crate root no longer contains the monolithic evaluator' \
    '^fn evaluate\(' \
    libs/myko/authority/src/lib.rs
}

check_handlers() {
  require_present \
    'retained myko owns QueryHandler' \
    'pub trait QueryHandler' \
    libs/myko/core/src
  require_present \
    'retained myko owns ReportHandler' \
    'pub trait ReportHandler' \
    libs/myko/core/src
  require_present \
    'retained myko owns ViewHandler' \
    'pub trait ViewHandler' \
    libs/myko/core/src
  require_absent \
    'no duplicate public handler family remains' \
    'pub trait (QueryHandler|ReportHandler|ViewHandler)' \
    libs/myko/app/src
  require_absent \
    'no duplicate handler runtime remains' \
    '(HandlerRuntime|ErasedHandlerSubscription|LiveCollectionWriter|collection_from_subscription|ProjectionQueryFactory|ItemQueryFactory)' \
    libs/myko/app/src libs/myko/node/src
  require_path_absent 'myko-app crate is deleted' libs/myko/app
  require_path_absent 'myko-app-macros crate is deleted' libs/myko/app-macros
}

check_session() {
  require_present \
    'retained ClientSession is the session owner' \
    'pub struct ClientSession' \
    libs/myko/core/src/server
  require_present \
    'retained SessionSink is the delivery boundary' \
    'pub trait SessionSink' \
    libs/myko/core/src
  require_absent \
    'no NodeSessionService owner remains' \
    'NodeSessionService' \
    libs/myko
  require_absent \
    'session and transport delivery use no unbounded channels' \
    '(flume::unbounded|unbounded_channel)' \
    libs/myko/core/src/client libs/myko/core/src/server libs/myko/federation/src libs/myko/local/src libs/myko/iroh/src libs/myko/server/src
  require_present \
    'durable event subscriptions have an explicit queue bound' \
    'DURABLE_EVENT_SUBSCRIPTION_CAPACITY' \
    libs/myko/federation/src
  require_absent \
    'durable subscriptions do not eagerly clone unbounded replay history' \
    'pub struct EventSubscription[^}]*backlog:[[:space:]]*VecDeque' \
    libs/myko/federation/src
  require_present \
    'durable replication exports use bounded history pages' \
    'fn events_page\(' \
    libs/myko/federation/src
  require_present \
    'from-now subscriptions use the indexed durable tail' \
    'fn latest_position\(' \
    libs/myko/federation/src
  require_absent \
    'from-now subscriptions do not scan complete history' \
    'pub fn subscribe_from_now[^}]*events_after' \
    libs/myko/federation/src
  require_present \
    'scope catalogs page through the backend index' \
    'pub fn scope_ids_page\(' \
    libs/myko/federation/src
  require_absent \
    'remote scope catalogs do not materialize the complete catalog' \
    'node\.scope_ids\(\)' \
    libs/myko/core/src/server/federated_session.rs
  require_absent \
    'retained client has no unbounded disconnected frame buffer' \
    'pending_sends:[[:space:]]*Mutex<Vec<WsFrame>>' \
    libs/myko/core/src/client
  require_present \
    'retained client caps disconnected frame admission' \
    'MAX_DISCONNECTED_SENDS' \
    libs/myko/core/src/client
  require_present \
    'retained report dispatch coalesces behind a bounded wake channel' \
    'flume::bounded::<\(\)>\(1\)' \
    libs/myko/core/src/client
  require_path_absent 'myko-session crate is deleted' libs/myko/session
}

check_client() {
  require_present \
    'retained MykoClient is the application client' \
    'pub struct MykoClient' \
    libs/myko/core/src/client
  require_present \
    'retained MykoClient owns the durable handler connector contract' \
    'pub trait HandlerConnector' \
    libs/myko/core/src/client
  require_present \
    'local and Iroh transports adapt the retained handler client' \
    'impl HandlerConnector for (LocalHandlerConnector|IrohHandlerConnector)' \
    libs/myko/local/src libs/myko/iroh/src
  require_present \
    'native node routes retained application clients by node identity' \
    'pub fn application_client\(&self, source_node: NodeId\)' \
    libs/myko/node/src
  require_present \
    'native node routes retained command clients by node identity' \
    'pub fn command_client\(&self, source_node: NodeId\)' \
    libs/myko/node/src
  require_present \
    'retained query map watch remains public' \
    'pub struct QueryMapWatch' \
    libs/myko/core/src/client
  require_present \
    'retained view map watch remains public' \
    'pub struct ViewMapWatch' \
    libs/myko/core/src/client
  require_absent \
    'no duplicate typed application client remains' \
    '(ApplicationClient|LocalApplicationClient|IrohApplicationClient)' \
    libs/myko/local/src libs/myko/iroh/src libs/myko/node/src
  require_path_absent 'myko-runtime crate is deleted' libs/myko/runtime
}

check_request_and_authority() {
  require_present \
    'prepared request is the retained request boundary' \
    'pub enum PreparedRequest' \
    libs/myko/core/src
  require_present \
    'typed access target is the authorization input' \
    'pub enum AccessTarget' \
    libs/myko/core/src libs/myko/federation/src libs/myko/authority/src
  require_absent \
    'optional-bag access request is deleted' \
    'pub struct AccessRequest' \
    libs/myko
  require_absent \
    'repeated access metadata request matching is deleted' \
    '(fn access_metadata|normalized_claims)' \
    libs/myko
  require_absent \
    'synthetic internal authority bypass is deleted' \
    'is_internal_authority_request' \
    libs/myko
}

check_domain_integrity() {
  require_absent \
    'typed item projections do not erase IDs to strings' \
    'BTreeMap<String,[[:space:]]*ItemState' \
    libs/myko/items/src
  require_absent \
    'collection writers do not publish a second mutable lifecycle truth' \
    'pub struct LiveCollectionWriter[^}]*state:[[:space:]]*Cell<LiveCollectionState' \
    libs/myko/federation/src
  require_absent \
    'named steady-state compatibility branches are deleted' \
    '(legacy_scope|restore_legacy|legacy_cursor|compatibility_gateway|LegacyEndpoint|RemoteCommandResponse|has_legacy_scope_metadata|qualified_scope_suffix|alias[[:space:]]*=[[:space:]]*"following")' \
    libs/myko/core/src libs/myko/items/src libs/myko/node/src libs/myko/local/src libs/myko/iroh/src libs/myko/server/src
  require_absent \
    'all-replication cursors use the versioned selection key' \
    'ReplicationSelection::All[[:space:]]*=>[[:space:]]*peer_id' \
    libs/myko/iroh/src
  require_path_absent 'duplicate WebSocket gateway crate is deleted' libs/myko/websocket-gateway
}

check_workspace() {
  require_absent \
    'workspace does not reference deleted duplicate crates' \
    'libs/myko/(app|app-macros|runtime|session|websocket-gateway)' \
    Cargo.toml
  require_present \
    'workspace defaults include retained myko' \
    'default-members[[:space:]]*=[^]]*"libs/myko/core"' \
    Cargo.toml
  require_present \
    'workspace defaults include retained myko-server' \
    'default-members[[:space:]]*=[^]]*"libs/myko/server"' \
    Cargo.toml
}

case "$mode" in
  handlers)
    check_handlers
    ;;
  session)
    check_session
    ;;
  client)
    check_client
    ;;
  request)
    check_request_and_authority
    ;;
  final)
    check_modularity
    check_handlers
    check_session
    check_client
    check_request_and_authority
    check_domain_integrity
    check_workspace
    ;;
  *)
    printf 'usage: %s {handlers|session|client|request|final}\n' "$0" >&2
    exit 2
    ;;
esac

if ((failures > 0)); then
  printf '%s architecture check(s) failed\n' "$failures" >&2
  exit 1
fi

printf 'Architecture checks passed for phase: %s\n' "$mode"
