# Myko 7

Myko 7 is a greenfield application framework for typed, event-sourced,
federated applications. Application code declares entities, commands, scopes,
queries, and authorization policy; Myko owns immutable history, persistence,
current-state materialization, native replication, resumable subscriptions,
Hyphae reactivity, and transport cursors.

The v7 alpha lives beside the legacy v6 crates while the new model is proven by
Forrest. Ordinary Cargo commands at this repository root target the v7 native
stack. Legacy v6 packages and the optional WebSocket gateway remain available
through explicit package or `--workspace` commands.

## Foundation

```text
application command
        |
        v
typed Myko context -----> immutable atomic batch -----> typed projections
        |                           |                         |
        |                           +---- Redb history        +---- app views
        |                           +---- Iroh federation     +---- subscriptions
        v
typed result

optional short-lived edge: WebSocket gateway -> the same Myko node
```

The v7 boundary is deliberately not a socket protocol:

- `#[myko_item(service = "...", ...)]` declares an entity's stable owning
  service, typed ID, schema version, and scope placement.
- `#[myko_command(service = "...", name = "...")]` declares a stable command
  body and typed result.
- command contexts enforce service and scope atomicity, encode mutations, and
  durably commit or reject work;
- current-state queries and follows infer service from the item schema;
- Redb and Iroh are replaceable framework adapters;
- WebSocket is an opt-in compatibility edge for short-lived clients, not a
  dependency of nodes, persistence, commands, federation, or native clients.

## V7 crates

| Crate | Responsibility |
| --- | --- |
| `myko-items-macros` | `#[myko_item]` and `#[myko_command]` generation |
| `myko-items` | typed item, mutation, projection, and query contracts |
| `myko-app` | registered reactive query, report, and view handlers over Hyphae |
| `myko-federation` | transport-neutral nodes, commands, history, scopes, and Hyphae subscription lifecycle |
| `myko-redb` | durable immutable journal and replication checkpoints |
| `myko-iroh` | authenticated native replication, control, and typed streams |
| `myko-local` | owner-local Unix peer transport for typed state and command follows |
| `myko-node` | restartable Redb + Iroh node composition and peer supervision |
| `myko-ratatui` | non-visual Hyphae lifecycle and coalesced redraw helpers |
| `myko-websocket-gateway` | optional short-lived edge adapter |

## Declaring application state

```rust
use myko_items::{myko_command, myko_item};

#[myko_item(service = "example.projects", scope_root)]
pub struct Project {
    pub title: String,
}

#[myko_item(service = "example.projects", scoped_by = Project)]
pub struct Task {
    pub project_id: ProjectId,
    pub title: String,
    pub complete: bool,
}

#[myko_command(
    service = "example.projects",
    name = "example.create_task",
    result = Task
)]
pub struct CreateTask {
    pub project_id: ProjectId,
    pub title: String,
}
```

The macros generate typed IDs and baseline queries. Item mutations also carry
the owning service. Myko rejects a handler write or forged raw batch when its
item service differs from the command's service, preventing one Rust entity
from silently splitting into unrelated service namespaces.

## Native nodes and clients

`myko-node::DurableIrohNode` restores a stable Myko identity, Iroh key, Redb
journal, configured peers, and source-aware follower cursors from one data
directory. `myko-iroh` exposes authenticated command clients, bounded typed
current-state reads, cursor-stable snapshot-then-live item and command streams,
scope-filtered history, and provisional live topics.

`myko_iroh::load_or_create_secret_key` gives lightweight clients the same
owner-only persistent transport identity handling without requiring Redb or a
durable replication node. This lets an application grant a TUI or mobile client
once and retain that authenticated principal across launches while keeping its
Myko projection ephemeral.

Native bootstrap uses a versioned descriptor that binds the authenticated Iroh
endpoint to the expected Myko source log. Clients can verify that pair through
a bounded identity handshake before submitting commands, and durable pinned
followers reject a source mismatch without ingesting history.

Myko also supplies a versioned, bounded pairing protocol on a separate Iroh
ALPN. A node issues an expiring one-use bearer while retaining only its SHA-256
verifier. Redemption authenticates the remote Iroh endpoint, binds both Iroh
and Myko identities into an HMAC transcript, and gives both operators the same
six-digit comparison code. It does not install a durable follower or grant any
application capability; applications make those decisions explicitly after
confirmation. The outer ticket, QR, file, or discovery encoding remains
application-selectable, and restarting the issuing node safely invalidates its
in-memory outstanding invitations.

Native and owner-local applications do not import raw event history or poll current state just
to render a screen. Embedded Redb nodes and authenticated Iroh peers can
materialize typed snapshot-then-live queries as one coherent Hyphae cell
containing the value, source cursor, and liveness state. `myko-local` carries
the same typed snapshot/follow contract over a protected Unix socket without
making the local app an Iroh endpoint. `myko-app` registers application-owned
query, report, and view handlers, retains their long-lived dependency drivers,
and erases them only at a transport boundary. Handler subscriptions may use a
single log cursor or an application-defined composite frontier. Transport-backed
reactive subscriptions retain their last coherent value while disconnected,
publish `Resynchronizing`, and restore the same Hyphae cell from a fresh gap-free
snapshot/follow boundary after the peer returns. `myko-ratatui` retains those cell
subscriptions and emits bounded, coalesced redraw wakeups; it does not provide
widgets or copy Myko data into a second UI store. Replicas use the separate
durable history follower.

The optional edge is explicit:

```bash
cargo test -p myko-websocket-gateway
```

Starting or linking a durable/native node does not bind a WebSocket listener.
An application may supervise `myko-websocket-gateway` over the same node when a
browser or other short-lived compatibility client needs one.

## Development

The default commands exercise the v7 native foundation:

```bash
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

To include every retained legacy v6 workspace member, use `--workspace`
explicitly. To exercise the complete v7 adapter matrix, name the packages:

```bash
cargo test \
  -p myko-items-macros -p myko-items -p myko-app -p myko-federation \
  -p myko-redb -p myko-iroh -p myko-local -p myko-node -p myko-ratatui \
  -p myko-websocket-gateway \
  --all-features
```

The strict workspace lint policy forbids unsafe code, panics, unchecked
indexing, unchecked arithmetic, TODO stubs, and related shortcuts in the new
foundation.

## Design status

The current implementation proves native command control, typed entities,
durable journals, authenticated Iroh replication, paginated typed state,
snapshot-then-live reactive subscriptions, scoped access control, revocation, peer
supervision, identity-bound one-use pairing, registered query/report/view handlers,
owner-local handler streams, gap-free service wakeups, and
optional WebSocket edges. Forrest is the application-level proof: it
multiplexes many harness sessions per node and uses Myko for durable messages,
commands, agent mail, access grants, federation, and native clients.

Multi-writer reconciliation, scoped readiness, coordinated invariants, richer
discovery and pairing UX, snapshots, retention, richer cross-source derived views,
and production mobile/TUI clients are still active design and implementation work. The governing constraints are in
[the federation first-principles document](docs/superpowers/specs/2026-08-22-myko-federation-first-principles.md).

License: AGPL-3.0-or-later.
