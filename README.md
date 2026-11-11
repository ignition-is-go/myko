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
`myko_events_notify`). `MYKO_POSTGRES_CATCH_UP_TIMEOUT_SECS` bounds startup wait
(default 300 seconds; 0 disables the limit).

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
