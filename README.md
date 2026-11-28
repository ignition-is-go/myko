# Myko

**Rust-first state, reactive queries, and live clients.** Myko is an event-sourcing
and CQRS framework for applications that need authoritative server state and
clients that stay synchronized as that state changes.

Define domain entities and operations in Rust. Myko supplies typed IDs, generated
queries and commands, reactive reports and views, graph relationships, persistence,
WebSocket subscriptions, and generated cross-language contracts. The reactive
engine is [Hyphae](https://github.com/ignition-is-go/hyphae).

## How it fits together

```text
Client command → handler → entity events → canonical stores
                                  ├→ persistence and replication
                                  └→ reactive queries, views and reports → client diffs
```

- **Items** are current entity state with typed identity. SET and DEL events
  update their stores; persistence supports replay and history.
- **Commands** validate application intent and emit mutations. Relationship
  effects can produce additional mutations, with causal work limits and loop
  suppression in the server pipeline.
- **Queries** select entities; **views** can produce computed rows; **reports**
  compute reactive results. Clients subscribe instead of polling for each change.
- **Relationships** express ownership, lifecycle dependencies and graph edges.
- **Clients** receive initial state followed by live changes, and resubscribe
  after reconnecting.

Postgres durability, peer federation, authentication, and graph projections are
configured by the host application. Myko does not require a separate graph database.

## Start with an entity

Add `myko` to your application. Add `myko-server` and Tokio when hosting a server;
use `myko-leptos` or `myko-gpui` for the corresponding Rust UI integration.
Package versions live in the individual crate manifests and npm package manifests;
the root workspace version is not the release version of every package.

```rust
use myko::prelude::*;

#[myko_item]
pub struct Camera {
    #[searchable]
    #[myko_rename]
    pub name: String,
}

let camera = Camera {
    id: CameraId::from("camera-1"),
    name: "Front of house".into(),
};
```

`#[myko_item]` adds an `id: CameraId` field and generates the `CameraId` string
newtype. IDs serialize as strings; the macro does not add a public `hash` field.
The declaration also registers serialization, reflection and type-generation
metadata, plus these operations:

| Generated API                      | Purpose                                        |
| ---------------------------------- | ---------------------------------------------- |
| `GetAllCameras`                    | Query all cameras                              |
| `GetCamerasByIds`                  | Query a set of typed IDs                       |
| `CameraQuery`, `GetCamerasByQuery` | Typed filters and filtered query               |
| `GetCameraById`                    | Report returning an optional camera            |
| `CountAllCameras`                  | Count report                                   |
| `DeleteCamera`, `DeleteCameras`    | Delete commands                                |
| `RenameCamera`                     | Rename command, opted in with `#[myko_rename]` |

Use `#[myko_command]`, `#[myko_query]`, `#[myko_view]` and `#[myko_report]` for
application operations. Handlers use a request context to read state and emit
changes. Keep domain types in Rust and generate their client contracts.

### Ownership and lifecycle

Relationship fields use the referenced entity's typed ID:

```rust
use myko::prelude::*;

#[myko_item]
pub struct Project {
    pub name: String,
}

#[myko_item]
pub struct Device {
    #[belongs_to(Project)]
    pub project_id: ProjectId,
    pub name: String,
}
```

Deleting the project cascades to its devices. `#[owns_many(...)]` describes owned
collections, and `#[ensure_for(...)]` creates dependent entities as prerequisites
appear. `#[myko_setter]` opts fields into generated setter commands;
`#[searchable]` opts supported fields into search indexing.

### Field indexes

A query with equality filters on indexed fields reads only the rows under
those keys, live and as a snapshot, instead of testing every row:

```rust
use myko::prelude::*;

#[myko_item]
#[index(control_id, session_id)]
pub struct Fader {
    #[indexed]
    pub service_id: String,
    #[indexed(each)]
    pub groups: Vec<Arc<str>>,
    pub control_id: Arc<str>,
    pub session_id: Arc<str>,
    pub level: i64,
}

// Routed through the `service_id` index.
let query = FaderQuery {
    service_id: Some(StringFilter::Eq("mixer".into())),
    ..Default::default()
};
// A row whose `groups` contains "main".
let in_group = FaderQuery {
    groups: Some(ContainsFilter::Eq("main".into())),
    ..Default::default()
};
```

- `#[indexed]` files a row under its field's value. `#[indexed(each)]` on a
  `Vec<T>` files it under each element, and the field's query filter becomes
  `ContainsFilter<T>`. `#[index(a, b)]` on the item declares a compound key.
- A field can be a key when its type implements `IndexKey`: ids, strings,
  `bool`, integers and `Option` of those. A JSON-like value opts in with
  `impl_index_key_serde!` (keyed by its JSON text) and
  `impl_filterable_by_key!`. Floats are rejected at compile time.
- `Eq` and `In` filters route; `Contains`, `StartsWith` and ranges do not.
  Unrouted fields still filter the routed rows.
- When several routes apply, an `id` filter wins, then the route that pins
  the most fields, then a `belongs_to` route before a declared index, then
  declaration order.
- `#[belongs_to]` fields are indexed this way already. An index is built
  the first time a query routes through it and is kept for the store's
  lifetime. Each write to the store then costs one key extraction per
  index, and wakes only the queries on the keys it adds or removes.

### Graph edges

An ordinary item can also be a typed edge. Its existing ID, fields and events
remain its canonical storage representation:

```rust
use myko::prelude::*;

#[myko_item]
pub struct Vertex {
    pub name: String,
}

#[myko_item]
pub struct Link {
    pub from_id: VertexId,
    pub to_id: VertexId,
}

#[myko_edge]
impl GraphEdge for Link {
    type Ends = Directed<ConcreteEndpoint<Vertex>, ConcreteEndpoint<Vertex>>;

    fn ends(&self) -> (VertexId, VertexId) {
        (self.from_id.clone(), self.to_id.clone())
    }

    const PAIR_POLICY: PairPolicy = PairPolicy::Unique;
    const PAIR_PROJECTION: PairProjectionPolicy = PairProjectionPolicy::Eager;
}
```

Graph macros generate endpoint and exact-pair queries, connect/ensure/disconnect
commands, synchronization helpers and client metadata. Categories
(`#[myko_category]`, `#[myko_in(...)]`) let endpoints accept an open set of entity
types. Policies control uniqueness, scope, self-loops and endpoint deletion.
Adjacency is demand-driven by default; eager indexes are opt-in, including
per-endpoint projection. Observational validation can introduce graph metadata to
existing items while retaining invalid history and recording diagnostics.

See the [graph architecture and benchmark reference](docs/superpowers/specs/2026-08-13-graph-edges-and-entity-categories-design.md)
for policies, generated APIs, traversal and projection tradeoffs.

## Run a server

This starts a server with in-memory state when no Postgres URL is supplied and
uses Postgres when `MYKO_POSTGRES_URL` is set:

```rust
use myko_server::{MykoServer, postgres::PostgresConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _telemetry = myko_server::telemetry::init_from_env();
    let mut builder = MykoServer::builder()
        .with_bind_addr("127.0.0.1:5155".parse()?);

    if let Some(postgres) = PostgresConfig::try_from_env()? {
        builder = builder.with_postgres(postgres);
    }

    builder.build().run().await
}
```

Link your application's entity and operation crates into the host so their
inventory registrations are included. Use `after_init` for services that need the
initialized context, or `try_after_init` for fallible initialization that must
abort startup on failure.

| Endpoint    | Transport                               |
| ----------- | --------------------------------------- |
| `/myko`     | Myko WebSocket gateway                  |
| `/myko/mcp` | MCP Streamable HTTP, WebSocket, and SSE |

The server shares one TCP listener across these routes. Rust and TypeScript
clients default to JSON and can opt into CBOR. The server accepts both formats.
Autosocket supplies reconnecting native and WASM transport.

### Authentication and permissions

Myko 6.12 adds app-owned authentication and local operation authorization:

| Package                   | Responsibility                                                           |
| ------------------------- | ------------------------------------------------------------------------ |
| `myko-auth`               | Authenticator/authorizer contracts, sessions, operation scopes           |
| `myko-auth0`              | Auth0 authentication and machine credential support                      |
| `myko-permissions`        | Persisted principals, groups, roles, grants and protected administration |
| `myko-permissions-leptos` | Leptos permission administration UI                                      |

Configure a `MykoAuthProvider` with `MykoServer::builder().with_auth_provider(...)`.
`ScopedAuthorizer` resolves generated entity read/write/delete requirements and
ownership inheritance against local policy. Custom commands and reports declare
requirements with `#[myko_scope(...)]`; unmapped operations are denied by that
authorizer. These permissions apply to entity types and operations, not individual
project instances or query rows.

**Raw client SET/DEL events require explicit entity opt-in**, even without an
auth provider:

```rust
use myko::prelude::*;

#[myko_item]
#[myko_external(SET)]
pub struct Observation {
    pub value: String,
}
```

Use `#[myko_external(SET, DEL)]` when both are intended. This schema declaration
does not bypass authorization. Authorized command handlers can still perform
server-side mutations without enabling raw events. Without an auth provider,
commands, queries, views and reports retain their existing trusted-entry behavior.

Start with [authentication and authorization](docs/auth/design.md),
[persisted permissions and bootstrap](docs/auth/permissions.md), and
[Leptos permission administration](docs/auth/leptos-permissions.md).

## Live clients and UI integration

The Rust client exposes list and fine-grained map watches for queries and views.
`watch_query_state` and `watch_view_state` distinguish waiting for an initial
response from a successful empty result. Readiness resets for a new reconnect
epoch. Identical Rust query/view watches share subscriptions and decoded state
until their final consumer drops.

| Integration  | Location and role                                                                                                |
| ------------ | ---------------------------------------------------------------------------------------------------------------- |
| Rust client  | `myko`, native and WASM                                                                                          |
| Leptos       | `myko-leptos`: live queries, views, reports and fine-grained stores                                              |
| GPUI         | [`myko-gpui`](libs/myko/gpui/README.md): foreground-thread state updates, observation and command state          |
| TypeScript   | `@myko/core`, in `libs/myko/ts`: client plus generated contracts                                                 |
| Svelte / Vue | [`ui-svelte`](libs/myko/ui-svelte/README.md), [`ui-vue`](libs/myko/ui-vue/README.md): TypeScript client adapters |
| Python       | [`py`](libs/myko/py/README.md), [`python`](libs/myko/python/README.md)                                           |
| C++ / C#     | [`cpp`](libs/myko/cpp/README.md), [`csharp`](libs/myko/csharp/README.md)                                         |

The Python, C++ and C# integrations have their own build instructions and release
state; they are not members of the root Rust workspace. Do not assume all
bindings expose every Rust API or share its package version.

## MCP for application operations

The server exposes `search`, `execute` and `connection_status` tools. Search
registered queries, views, reports and commands; request `details: true` for wire
arguments, result wrappers and validated examples. Execute a JavaScript async
function body to combine operations:

```javascript
const cameras = await myko.query("GetAllCameras", {});
return { count: cameras.items.length };
```

Query/view results contain `items` and `count`; command/report payloads are in
`result`. Each call validates arguments against the operation's compiled
serialization contract. Schema resources are available at
`myko://schema/<kind>/<id>`.

Connect an MCP HTTP client to `http://localhost:5155/myko/mcp`. The stdio bridge is
also available through `myko::mcp::McpServer::run_stdio`, using `MYKO_ADDRESS` to
reach a running server.

Clients can narrow discovery and execution with
`X-Myko-Tool-Visibility-Allow` / `-Deny` name patterns and
`X-Myko-Tool-Callable-Allow` / `-Deny` argument constraints. Stdio equivalents are
`MYKO_MCP_TOOL_VISIBILITY_ALLOW`, `MYKO_MCP_TOOL_VISIBILITY_DENY`,
`MYKO_MCP_TOOL_CALLABLE_ALLOW`, and `MYKO_MCP_TOOL_CALLABLE_DENY`.
These client-selected restrictions narrow access; configure server authentication
and authorization to establish the caller's authority. See the
[MCP Code Mode guide](docs/mcp-code-mode.md) for input contracts, output schemas,
and domain discovery.

## Persistence, telemetry and diagnostics

The Postgres persister supplies durable events, startup hydration and catch-up.
`PostgresConfig::try_from_env()` loads `MYKO_POSTGRES_URL`, optional
`MYKO_POSTGRES_TABLE` (default `myko_events`) and `MYKO_POSTGRES_CHANNEL` (default
`myko_events_notify`).

Every time limit the server enforces can be changed at deploy time without rebuilding.
Unset keeps the default, `0` disables the limit (wait forever / never expire), and any
value that is not a non-negative integer fails startup with the variable named.

| Variable | Default | Bounds |
| --- | --- | --- |
| `MYKO_POSTGRES_CATCH_UP_TIMEOUT_SECS` | 300 | startup wait for the Postgres consumer to catch up |
| `MYKO_POSTGRES_FLUSH_TIMEOUT_SECS` | 10 | wait for queued events to be durably written |
| `MYKO_POSTGRES_CONNECT_TIMEOUT_SECS` | 10 | opening a Postgres connection |
| `MYKO_POSTGRES_TCP_USER_TIMEOUT_SECS` | 30 | unacknowledged writes before a dead peer is dropped |
| `MYKO_HTTP_HEAD_TIMEOUT_SECS` | 10 | reading an incoming HTTP request head |
| `MYKO_HTTP_AUTH_TIMEOUT_SECS` | 10 | authenticating a WebSocket or MCP request |
| `MYKO_HTTP_SHUTDOWN_DRAIN_TIMEOUT_MS` | 250 | draining a socket before close |
| `MYKO_OAUTH_BODY_TIMEOUT_SECS` | 10 | reading an OAuth endpoint request body |
| `MYKO_WS_OUTBOUND_COMMAND_TTL_SECS` | 10 | age before an unanswered outbound command is dropped |
| `MYKO_MCP_EXECUTE_TIMEOUT_SECS` | 30 | one MCP Code Mode script call, including async hops |
| `MYKO_MCP_SCRIPT_DEADLINE_SECS` | 10 | synchronous JavaScript compute inside one script |
| `MYKO_MCP_QUERY_TIMEOUT_SECS` | 5 | an MCP query response |
| `MYKO_MCP_REPORT_TIMEOUT_SECS` | 5 | an MCP report value |
| `MYKO_MCP_COMMAND_TIMEOUT_SECS` | 10 | an MCP command response |
| `MYKO_MCP_CONNECT_TIMEOUT_SECS` | 5 | the MCP client connecting to a Myko server |

Many servers can share one events table, so a server never changes shared schema as
a side effect of booting. The schema is a numbered, forward-only list of migrations
recorded in `<table>_schema_migrations`. By default, startup only reads the recorded
version and fails when it is older than the server requires; a newer version is
accepted. An events table created before migrations existed counts as the baseline,
so servers upgraded onto an existing database keep starting without a migration
run. Servers require only the baseline migration, so they start while a runner
is still applying the slower ones. The latest-state table and its backfill run
before the index rebuilds, because they are what makes startup fast. The one exception is a database without the events table, which the first
server bootstraps. To apply pending migrations, run
`myko_server::postgres::migrate(&config)` from a deploy job, or start one server with
`MYKO_POSTGRES_MIGRATE=true`; that server then waits for the migrations before it
becomes ready. Concurrent runners serialize on an advisory lock. Index migrations use
`CREATE INDEX CONCURRENTLY`, so inserts continue, but on a large table they can take
hours. Rerunning after an interruption is safe.

Startup hydrates only the entity types the server registers. It reads
`<table>_latest`, a latest-event-per-item table that an insert trigger maintains, once
its backfill migration is recorded; until then it walks the events table with a loose
index scan.

Postgres node settings belong to its deployment. At startup the server logs one
warning when settings are far from these recommendations for an append-only log:

| setting | recommendation |
| --- | --- |
| `shared_buffers` | about 25% of node RAM |
| `effective_cache_size` | about 75% of node RAM |
| `maintenance_work_mem` | 1 to 2 GB |
| `max_wal_size` | 8 GB or more |
| `random_page_cost` | 1.1 on SSD |
| `effective_io_concurrency` | 200 on SSD |
| `wal_compression` | `lz4` |
| `autovacuum` | `on` |

`myko-telemetry` is independently usable by applications and SDK hosts; the server
re-exports it. Initialize once and retain the guard through shutdown. It provides
console logging and optional OpenTelemetry logs, traces and metrics over OTLP
HTTP/protobuf:

```sh
OTEL_SERVICE_NAME=my-application
OTEL_EXPORTER_OTLP_ENDPOINT=https://collector.example.com
RUST_LOG=info
```

Without an exporter endpoint, logging stays local. Per-signal OTel endpoint
variables override the base endpoint; `MYKO_TRACING_ENDPOINT` is a legacy fallback.
Client delivery diagnostics are opt-in: see the
[delivery metrics guide](docs/observability/client-delivery.md).
For view sampling behavior, see [view sampling](docs/view-sampling.md); graph
benchmarks are recorded in the graph architecture reference linked above.

## Migrating to Myko 7.0

Myko 7.0 builds on Hyphae 4.0, changes two maintenance functions, and raises the
WebSocket size limits. Partitioned servers and persisted OAuth grants are new and
opt-in; a server that does not use them behaves as before, and the wire format is
unchanged apart from the size limits.

### Upgrade Hyphae with Myko

Myko 7.0 requires Hyphae 4.0. Follow
[Migrating to Hyphae 4.0](https://github.com/ignition-is-go/hyphae#migrating-to-hyphae-40)
for your own Hyphae code. The changes Myko applications hit most often:

- Remove `features = ["scheduler"]` from your `hyphae` dependency; the scheduler is
  always compiled.
- Delete `.with_name(...)` calls on cells and maps.
- Replace `interval_precise_source` with `interval_source`.

### Remove calls to deleted API

| Removed or changed | What to do |
| --- | --- |
| `myko::query::sweep_all_belongs_to_source_indexes()` | `myko::query::sweep_belongs_to_source_indexes(&registry)`. Each registry now owns its belongs-to indexes, so the sweep takes the registry. A server already sweeps its own during cache cleanup. |
| `myko::view::erase_typed_view_map(map)` | `myko::view::ErasedViewMap::from_typed(map)`, which reads the typed map as erased rows without building a second map. A view's own plan already erases its output. |
| `dispatch_metrics::record_command` and the generated per-kind recorders | Pass `&'static str` ids, such as string literals, instead of borrowed strings. |

### Check deployments that mix versions

`WS_MAX_MESSAGE_SIZE_BYTES` and `WS_MAX_FRAME_SIZE_BYTES` rise from 64 MiB to 1 GiB.
A peer still on Myko 6 rejects a frame larger than 64 MiB, so upgrade clients and
servers together when they exchange large snapshots.

### Check code that depends on view timing

A switched view now builds its new selection off the propagating thread. When the
selection changes, the view's output updates once that build finishes, not inside
the wave that changed the selection. Graphs built or dropped from selector, map or
project closures also run off the wave, so a slow build no longer holds unrelated
writes.

### Adopt the new features where you need them

- **Partitions.** `MykoServer::builder().with_partitions(schema)` runs each partition
  of a server in its own Hyphae scheduling domain. Declare partition keys on items;
  reads and writes against the flow are refused.
- **Persisted OAuth grants.** The built-in OAuth authority can keep grants in
  Postgres through `PostgresOAuthStore`, an `OAuthStore`, and restore them after a
  restart with `restore_oauth`. The store creates its table on first use; no
  migration run is needed.
- **Authorizers.** `Authorizer::inspects_proposed_events` has a default
  implementation, so existing authorizers compile unchanged. One that returns
  `false` receives `Value::Null` as the proposed item, which lets the transport
  skip building it.

## Develop this repository

Use the pinned toolchain in [`rust-toolchain.toml`](rust-toolchain.toml), Bun, and
[Cargo Flux](https://github.com/ignition-is-go/cargo-flux) (CI installs 0.11.5).
Native GPUI checks need platform libraries; the
[quality-gate workflow](.github/workflows/quality-gates.yml) lists the Linux packages.
Postgres is needed for durable server use, not every unit test.

```sh
bun install --frozen-lockfile
cargo flux run check                       # includes generation prerequisites
cargo flux run lint
RUST_TEST_THREADS=1 cargo flux run test
cargo flux run check-wasm
cargo flux run check-auth-format            # Rust formatting check
cargo flux run format-rust                  # apply Rust formatting
```

Hyphae's reactive scheduler is process-global, so Rust tests run serially within
each test binary. Check `.bacon-locations` before duplicating background checks.
Focused tasks, including auth, permissions, Postgres and MCP checks, are defined
in [`flux.toml`](flux.toml). Contributor and agent conventions live in
[`AGENTS.md`](AGENTS.md).

For exported Rust contract changes, run `cargo flux run gen` from the appropriate
package. Do not hand-edit generated TypeScript. Coordinate generation with any
active watcher rather than running a second generator against the same files.

Releases use the [release](.github/workflows/release.yml) and
[publish](.github/workflows/publish.yml) workflows: `main` is the production
channel and `dev` is canary. Rust and npm packages are published in dependency
order; publication can resume after a partial release.

## License

Check each package's manifest. The current framework, server, auth, UI bridge and
Autosocket Rust crates declare **AGPL-3.0-or-later**. `myko-telemetry` declares
**MIT OR Apache-2.0**. JavaScript packages and other bindings carry their own
license declarations.
