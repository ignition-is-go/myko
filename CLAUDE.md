# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Rocketship (rship) is a centralized control platform for orchestrating reactive event relationships within networks of integrated multimedia systems. It's a polyglot monorepo combining TypeScript, Rust, Python, C#, and Swift.

**Core Concept**: External software **Services** run **Executors** that connect to the rship server via WebSocket. Executors publish **Targets** (interactable entities) with **Emitters** (state observers) and **Actions** (commands). **Bindings** define reactive relationships between Emitters and Actions, organized into **Scenes** and **Calendars**.

## Development Commands

### JavaScript/TypeScript

```bash
pnpm install                          # Install dependencies
pnpm dev --filter @rship/server       # Run server in watch mode (MYKO_PORT=5155)
pnpm dev --filter @rship/ui           # Run UI dev server
pnpm typecheck-sdk                    # Type check SDK
pnpm typecheck-postgres               # Type check postgres lib
pnpm build --filter <package>         # Build specific package
pnpm format:all                       # Format all code with prettier
```

### Rust

```bash
cargo build --release                 # Build release
cargo test                            # Run all tests
cargo test -- --nocapture             # Run tests with output
cargo test <test_name>                # Run single test
cargo clippy -- -D warnings           # Lint with clippy
cargo fmt                             # Format Rust code
cargo bench                           # Run benchmarks (libs/myko/rs/benches/)
cargo test --features bench           # Run with benchmark entities
```

### Python

```bash
uv pip install -e .                   # Install package in editable mode
pytest                                # Run all tests
pytest -k <test_name>                 # Run single test
```

### Multi-language Publishing

```bash
pnpm jsr:publish                      # Publish TypeScript to JSR
pnpm py:publish                       # Publish Python packages
pnpm rust:publish                     # Publish Rust crates
pnpm cs:publish                       # Publish C# packages
```

### Versioning

```bash
pnpm versionstamp                     # Generate version metadata
pnpm versionwrite                     # Update versions across packages
pnpm gen                              # Run scaffolding and generate entity index
```

## Architecture

### Core Framework: Myko (`/libs/myko/`)

Event-sourcing CQRS framework powering rship's reactive architecture:

- **@myko/core**: Base event sourcing primitives
  - `MItem`: Base entity with MD5 content hashing
  - `MEvent`: Immutable SET/DEL events with timestamps
  - `MCommand`: Command specifications (intent)
  - `MQuery`: State snapshots
  - `MSaga`: Observable-based event processors
- **@myko/ws**: Real-time bidirectional WebSocket with MessagePack encoding
- **@myko/gateway**: Server bootstrap, Auth0 integration, OpenTelemetry tracing
- **@myko/kafka**: Kafka-based event persistence
- **@myko/sqlite / @myko/postgres / @myko/surreal**: Storage backends
- **@myko/rs**: Rust server/client library with actor-based architecture
  - `MykoServer`: Actor-based server using `ractor`
  - `MykoClient`: WebSocket client with connection management
  - Types exported via `ts-rs` for TypeScript consumption
  - Modules: `actors/` (event, query, command, report, ws, kafka), `server/`, `client/`
- **@myko/macros**: Proc macros for entity/query/report/command generation
  - `#[myko_item]`: Auto-generates queries, reports, and commands (see below)
  - `#[myko_query]`, `#[myko_report]`, `#[myko_command]`: Manual definitions
- **@myko/ts**: NAPI-based TypeScript client wrapping @myko/rs
  - Thin wrapper (~95 lines Rust, ~75 lines TypeScript)
  - RxJS Observable APIs over native callbacks
  - Build: `pnpm build --filter @myko/ts`
  - Type generation: `pnpm --filter @myko/rs gen` (runs `cargo test --lib`)

**Pattern**: Commands → Events → State Updates → Queries

### Entity System (`/libs/entities/`)

Rship-specific domain entities built on Myko:

- **Core entities**: Target, Instance, Machine, Emitter, Action, Binding, Scene, Calendar, Pulse, EventTrack
- **BindingNode trees**: Complex execution graphs with Expression → Condition → Constraint → Delay → Action
- **Handlers** (`/handlers/`): Business logic per entity type (e.g., `binding-handler.ts`, `scene-handler.ts`)
- Uses `reflect-metadata` for runtime type registration

Entity handlers are loaded by the server at startup and process Commands to generate Events.

### SDK (`/libs/sdk/`)

Multi-language executor development kit:

- **TypeScript** (primary): `RshipExecClient` with fluent API
  - `InstanceProxy → TargetProxy → EmitterProxy | ActionProxy`
- **Rust, Python, C#, Swift**: Multi-language support for executor development

Executors use the SDK to:

1. Connect to rship server via WebSocket
2. Declare Instances, Targets, Emitters, Actions
3. Push Pulses (real-time data from Emitters)
4. Receive and execute Actions

### Link/RPC Layer (`/libs/link/`)

gRPC-based RPC for controller management:

- **Protocol Buffers** define Link service (`link.proto`)
  - Methods: Disconnect, SetRshipUrl, GetControllers, ConnectController, etc.
- **TypeScript bindings**: Auto-generated via `protoc-gen-ts_proto`
- **Rust implementation**: gRPC server in `/link/core/`

Used for managing external controller connections (hardware control surfaces, etc.).

### Asset Store (`/libs/asset-store/`)

Actor-based S3-compatible file storage system:

- **Core (Rust)**: Actor-based using `ractor`
  - Storage Manager, Upload Manager, Presence Manager, WebSocket Manager actors
- **Client (TypeScript)**: Type-safe NAPI-RS bindings
- Supports MinIO, AWS S3 with multipart uploads and real-time WebSocket updates

### Communication & Data Flow

```
Executor (Push Pulses via WebSocket)
    ↓
Server (Process via Entity Handlers, Execute Bindings)
    ├─ Commands from UI → Actions to Executors
    └─ Events → Real-time Updates to UI
    ↓
Persistence (Kafka Event Log)
```

**WebSocket Message Types**:

1. **Commands**: `MWrappedCommand` with transaction ID
2. **Events**: SET (create/update) or DEL (delete) with timestamp
3. **Pulses**: Real-time emitter data (not persisted)
4. **Queries**: State snapshots

**Binding Execution**: BindingNode trees process Pulses through expression evaluation, conditions, constraints, delays, and finally invoke Actions.

### Applications (`/apps/`)

- **server**: Main Bun-based server
  - Entry: `/apps/server/src/main.ts`
  - Bootstraps Myko gateway, loads entity handlers, sets up persistence
  - Required environment variables:
    - `KAFKA_BROKERS` - Comma-separated Kafka broker addresses
    - `MYKO_HOST_ADDRESS` - Server host address
    - `RSHIP_CLUSTER_SECRET` - Cluster authentication secret
    - `AUTH_0_DOMAIN` - Auth0 domain for authentication
    - `MYKO_PORT` - Server port (typically 5155 for dev)
  - Optional: `MYKO_TRACING_ENDPOINT` - OpenTelemetry tracing endpoint
- **ui**: Svelte 5 + SvelteKit web UI
  - Real-time editor, 3D visualization (Threlte + Three.js)
  - Schema-based forms, Auth0 authentication
  - Styling: Tailwind CSS 4 + daisyUI 5 (see `.github/daisyui.instructions.md`)
  - Cross-platform: Web, iOS/Android via Capacitor
- **execs**: Executor implementations (each integrates an external system)
  - TypeScript: ableton-cli, protocol-router, viewpoint, ventuz, noise
  - Python: demo-py, music-analysis
  - C#: disguise, pixera, dirigera
  - visionOS (Swift), touch-host (TouchDesigner)

### Multi-Language Type Sharing

- **Rust → TypeScript**: `ts-rs` derive macros generate TypeScript types from Rust structs
  - Types generated via `cargo test --lib` (ts-rs uses test harness for codegen)
  - Output to `bindings/` directory, re-exported via `bindings/index.ts`
  - Run `pnpm --filter @myko/rs gen` to regenerate types
- **Protocol Buffers**: Language-agnostic schemas for RPC (Link layer)
- **NAPI-RS**: Rust native modules with auto-generated TypeScript bindings (Asset Store, Sync, @myko/ts)

## Code Style

### Commits

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat(scope): description` - New features
- `fix(scope): description` - Bug fixes
- `chore(scope): description` - Maintenance tasks

Commits drive release notes and CI workflows.

### Comments

TODO & NOTE comments should include author's initials:

```typescript
// TODO(ts): need to implement
// NOTE(ts): informational message
```

### Formatting

- **JS/TS**: Use `prettier` with `prettier-plugin-organize-imports`
- **Rust**: Use `rustfmt`
- Lines under 120 characters
- Comments explain _why_, not _what_

### Naming Conventions

- **JS/TS**: `camelCase` for variables/functions, `PascalCase` for classes/types
- **Rust**: `snake_case` for variables/functions, `PascalCase` for structs/enums
- **Python**: Follow PEP8 (`snake_case` for variables/functions, `PascalCase` for classes)

## Project Structure

```
/apps/
├── server/           # Main Bun server (entry: src/main.ts)
├── ui/               # Svelte 5 UI with SvelteKit
├── execs/            # 15+ executor implementations
├── asset_store/      # Asset storage service
├── linkd/            # Link daemon
└── myko/             # Myko server standalone

/libs/
├── myko/             # Event sourcing framework (13 modules)
├── entities/         # Rship entity definitions & handlers
├── sdk/              # Executor SDK (6 languages)
├── link/             # gRPC RPC layer
├── asset-store/      # File storage system (Rust core + TS client)
├── types/            # Shared TypeScript types
├── sync/             # Sync/FFI layer
└── [20+ integration libraries]
```

## Key Implementation Patterns

### Event Sourcing + CQRS

All state changes flow through immutable events:

1. UI/Executor sends Command
2. Entity handler validates and generates Events
3. Events persisted to Kafka
4. Sagas react to events and may generate new Commands
5. Queries provide current state snapshots

### Reactive Streams (RxJS)

Heavy use of Observables for real-time data flow. Entity handlers and UI components subscribe to event streams.

### Hash-Based Versioning

`MItem` uses MD5 content hashing for optimistic concurrency control and conflict detection.

### Actor Model

Asset Store and @myko/rs use `ractor` for concurrent processing with supervision trees. Key actors in @myko/rs:
- `EventManager` / `EventHandler` - Event persistence and dispatch
- `QueryManager` / `QueryRunner` - Reactive query execution
- `ReportManager` / `ReportRunner` - Computed report handling
- `CommandManager` - Command routing and execution
- `WebSocketServer` / `WebSocketConnection` - Client connections
- `KafkaProducer` / `KafkaConsumer` - Optional Kafka integration

### Stateless Executors

Executors are bridges to external software. State remains in the external system; Executors translate between native APIs and rship's abstract model.

### Myko Item Macro (`#[myko_item]`)

The `#[myko_item]` attribute macro generates a complete CRUD infrastructure for entity types:

```rust
#[myko_item]
pub struct Target {
    pub name: String,
    // id: Arc<str> and hash: Arc<str> added automatically
}
```

**Auto-generated queries:**
- `GetAllTargets` - Fetch all entities
- `GetTargetsByIds { ids: Vec<Arc<str>> }` - Fetch by ID list
- `GetTargetsByQuery { partial: PartialTarget }` - Fetch by partial match

**Auto-generated reports (reactive):**
- `CountAllTargets` - Count all entities
- `CountTargets { partial: PartialTarget }` - Count with filter
- `GetTargetById { id: Arc<str> }` - Get single entity by ID

**Auto-generated commands:**
- `DeleteTarget { id: Arc<str> }` - Delete single entity
- `DeleteTargets { ids: Vec<Arc<str>> }` - Bulk delete

Also generates: `PartialTarget` struct, trait implementations (`WithId`, `ToValue`, `Eventable`, `AnyItem`), and registration via `inventory`.

## Important Notes

- **Server Runtime**: Uses Bun, not Node.js
- **Package Manager**: pnpm with workspaces
- **Monorepo**: All packages in `/apps/` and `/libs/` defined in `pnpm-workspace.yaml`
- **Type Safety**: Extensive use of TypeScript with strict null checks
- **Real-time Performance**: See CRUSH.md for Rust optimization guidelines (actor patterns, lock-free structures, channel sizing, serialization)
- **Submodules**: TouchDesigner and Unreal integrations are git submodules (auto-updated via preinstall hook)

## Environment Setup

1. Install dependencies: `pnpm install` (runs git submodule update automatically)
2. Version stamping runs automatically in postinstall
3. Development requires Bun runtime for server
4. Rust toolchain for native modules
5. Python with `uv` for Python packages

## Debugging & Development Workflow

### Running Specific Packages

```bash
pnpm dev --filter @rship/server        # Run server with hot reload
pnpm dev --filter @rship/ui            # Run UI dev server
pnpm build --filter @rship/sdk         # Build specific package
```

### Working with Executors

1. Start rship server first (`pnpm dev --filter @rship/server`)
2. Run executor (varies by language - see executor's README)
3. Executor connects via WebSocket to publish Targets/Emitters/Actions
4. Use UI to create Bindings between Emitters and Actions

### Common Issues

- **Kafka connection errors**: Ensure `KAFKA_BROKERS` is set and Kafka is running
- **WebSocket connection fails**: Check `MYKO_PORT` and `MYKO_HOST_ADDRESS` match between server and clients
- **Type generation out of sync**: Run `pnpm versionstamp` to regenerate types
- **Submodule not initialized**: Run `git submodule update --init --recursive --remote`

## Code Integration Guidelines

These guidelines capture lessons learned from previous code contributions to help Claude Code produce changes that align with project standards.

### 1. Respect Explicit Opt-In Patterns

**Pattern**: This project uses environment variable guards for diagnostic/monitoring features even when they only produce debug logs.

**Example**:

```typescript
// Preferred: Explicit opt-in with env guard
if (process.env['MEMORY_MONITOR'] !== 'true') {
  return
}
setInterval(() => {
  logger.debug('Memory stats...')
}, 5000)

// Don't: Remove guards and rely solely on log level
// Even though debug logs won't show at INFO level, the interval still runs
// and collects data unnecessarily
```

**Rationale**: Environment guards prevent performance overhead (intervals, data collection) even when logs won't be displayed. This is intentional design, not cruft to remove.

### 2. Commit Organization

**Pattern**: Separate critical bug fixes from nice-to-have improvements.

**Example**:

```
Commit 1: fix: myko gateway memory leaks
  - Core subscription cleanup (takeUntil + finalize)
  - Client disconnect handling
  - Repo caching for watchId/clientDisconnect
  - Log cleanup sagas

Commit 2: fix(server): better debug logging control
  - Migrate console.log to MykoLogger
  - Add MYKO_INITIAL_LOG_LEVEL support
  - Consolidate diagnostic output format
```

**Rationale**: Makes it easier to cherry-pick critical fixes, revert non-essential changes, and understand git history. Bug fixes should be complete and include all related changes in one commit.

### 3. Code Formatting

**Pattern**: Let automated formatters (prettier, rustfmt) handle formatting. Don't try to match formatting manually in edits.

**Why**: The user will run formatters anyway. Focus on logical correctness, not whitespace alignment. Mismatched formatting creates noisy diffs and merge conflicts.

### 4. Comprehensive Issue Resolution

**Pattern**: When fixing systemic issues (memory leaks, race conditions, etc.), address all instances across the codebase in a single commit.

**Example**: For the memory leak fix, included:

- All scene engine methods (6 methods fixed)
- Repo-level caching (watchId)
- Bootstrap-level caching (clientDisconnect)
- Related cleanup sagas (LinkLog, ExecLog)
- Diagnostic tools for future debugging

**Rationale**: Partial fixes leave technical debt and make it harder to verify the issue is fully resolved. Group related changes together so the entire fix can be reviewed, tested, and potentially reverted as a unit.

### 5. Import Cleanup

**Pattern**: Remove unused imports as part of the change that makes them unused, not as a separate "cleanup" commit.

**Example**:

```typescript
// When replacing takeWhile with takeUntil:
import {
  takeUntil,  // Added
- takeWhile,  // Removed in same commit
  tap,
} from 'rxjs'
```

**Rationale**: Keeps commits atomic and prevents dead code from accumulating between commits.

### 6. Prefer Existing Patterns

**Pattern**: Before suggesting architectural changes, check if the project already has established patterns for similar functionality.

**Example**: The project already had:

- MykoLogger for structured logging
- Environment variable guards for diagnostic features
- Debug log level for non-production diagnostics

Don't suggest inventing new patterns when existing ones work fine.

### 7. Performance-Conscious Defaults

**Pattern**: This project prioritizes runtime performance over convenience. Diagnostic features should be:

- Opt-in via environment variables
- Use debug/verbose log levels (not info)
- Minimal overhead when disabled

**Why**: Rship handles real-time multimedia control with high message throughput. Even "cheap" operations like collecting memory stats every 5 seconds add up at scale.

### 8. URL Path Design for Reverse Proxies

**Pattern**: Use query parameters instead of path segments for dynamic identifiers that may contain special characters (especially `/` or `%2F`).

**Example**:

```rust
// Preferred: Query parameter approach
GET /asset?key=folder%2Fsubfolder%2Ffile.png
GET /thumbnail?key=textures%2Fwood.jpg

// Avoid: Path segment approach
GET /assets/folder%2Fsubfolder%2Ffile.png/download
GET /thumbnails/textures%2Fwood.jpg
```

**Why**: Reverse proxies like Traefik often decode `%2F` to `/` in path segments before forwarding requests, breaking routes that expect encoded slashes. Query parameters are not decoded by proxies and reach the backend intact. This affects any identifier that could contain forward slashes (file paths, S3 object keys, etc.).

## Myko Framework: TypeScript to Rust Migration Guide

This section documents the TypeScript Myko framework functionality for migration to `@myko/rs`.

### Core Concepts (Language-Agnostic)

**Items** (`MItem`):
- Base entity with `id: ID` and `hash: string` (MD5 content hash)
- Hash is auto-computed from all fields except `hash` itself
- Used for optimistic concurrency and change detection
- Items are registered in a global constructor registry by type name

**Events** (`MEvent`):
- Immutable records of state changes: `SET` (create/update) or `DEL` (delete)
- Fields: `tx` (transaction ID), `itemType`, `item`, `changeType`, `createdAt`, `sourceId`, `options`
- `options.preventRelationshipUpdates`: Skip cascade behavior when true

**Commands** (`MCommand<T>`):
- Intent to mutate state, returns result of type `T`
- Has `tx` (transaction ID), `userToken` for auth
- Wrapped as `MWrappedCommand { command, commandId }`

**Queries** (`MQuery<T extends MItem>`):
- Request for live data stream, returns `Observable<T[]>`
- Has `tx`, `commandClientId`
- Wrapped as `MWrappedQuery { query, queryId, queryItemType }`
- Results are reactive and update as underlying data changes

**Reports** (`MReport<T>`):
- Computed/derived data request, returns `Observable<T>` (any type, not just items)
- Similar to queries but for aggregations, transformations, complex joins
- Wrapped as `MWrappedReport { report, reportId }`

### Bus Architecture

**EventBus** (`eventBus`):
- Central publish/subscribe for all events
- `publishSet(item, tx)`: Recalculates hash, emits SET event
- `publishDel(item, tx)`: Recalculates hash, emits DEL event
- `publishAll(events[])`: Batch publish
- Handles relationship cascades on startup and during runtime

**CommandBus** (`commandBus`):
- Routes commands to registered handlers
- `execute(command)`: Returns `Promise<T>` with command result
- Handler registration via `bind(handler, commandId)`
- Decorators: `@MykoCommand()` on class, `@MykoCommandHandler(CommandClass)` on handler

**QueryBus** (`queryBus`):
- Routes queries to handlers, provides caching
- `watch(query)`: Returns live `Observable<T[]>` with caching
- `execute(query)`: One-shot `Promise<T[]>` (first value from watch)
- Cache key: `${queryId}:${JSON.stringify(queryWithoutTx)}`
- Results are shared via `ReplaySubject(1)` and `distinctUntilChanged` by hash set

**ReportBus** (`reportBus`):
- Similar to QueryBus but for computed values
- `watch(report)`: Returns live `Observable<T>`
- `execute(report)`: One-shot `Promise<T>`
- Same caching pattern as QueryBus

### Repository Pattern

**Abstract Repo** (`Repo<T>`):
- Wraps persistence and provides reactive queries
- Constructed with `persister` (handles load/save) and event subscription

**Key Methods**:
```
getId(id): Promise<T | null>           // Get single item
getIds(ids[]): Promise<T[]>            // Get multiple items
get(query): Promise<T[]>               // Filter by partial match
getFilter(fn): Promise<T[]>            // Filter by predicate
getIndex(key, value): Promise<T[]>     // Index lookup
getSearch(query): Promise<T[]>         // Full-text search (flexsearch)

watchId(id): Observable<T | null>      // Live single item (cached)
watchIds(ids[]): Observable<T[]>       // Live multiple items
watch(query): Observable<T[]>          // Live partial match filter
watchFilter(fn): Observable<T[]>       // Live predicate filter
watchSearch(query): Observable<T[]>    // Live full-text search
```

**Caching**: `watchId` results are cached per ID with `shareReplay({ bufferSize: 1, refCount: true })`, cleaned up via `finalize`.

### Relationship System

**@belongsTo(EntityType)**: Foreign key relationship
- On parent DELETE: cascade delete all children with matching foreign key
- Relation stored as `{ type: 'belongs-to', foreignType, localType, localKey, foreignKey: 'id' }`

**@ownsMany(EntityType)**: Parent owns array of child IDs
- On parent DELETE: cascade delete all referenced children
- On child DELETE: remove child ID from parent's array, recalculate hash
- Orphan cleanup on startup: delete children not referenced by any parent

**@ensureFor(EntityType)**: Auto-create entity for each instance of dependency
- On dependency SET: ensure local entity exists with matching foreign key
- Creates Cartesian product for multiple dependencies
- Uses `@defaultValue(val)` for auto-populated fields

**@searchable()**: Mark field for full-text search indexing

### WebSocket Protocol

**Message Types**:
```
ws:m:event              - Server broadcasts MEvent
ws:m:command            - Client sends MWrappedCommand
ws:m:command-response   - Server responds with { tx, response }
ws:m:command-error      - Server responds with { tx, message }
ws:m:query              - Client sends MWrappedQuery
ws:m:query-response     - Server responds with { tx, sequence, upserts[], deletes[] }
ws:m:query-error        - Server responds with { tx, message }
ws:m:query-cancel       - Client cancels query subscription
ws:m:report             - Client sends MWrappedReport
ws:m:report-response    - Server responds with { tx, response }
ws:m:report-error       - Server responds with { tx, message }
ws:m:report-cancel      - Client cancels report subscription
ws:m:ping               - Heartbeat { id, timestamp }
```

**Query Response Delta Protocol**:
- First response has `sequence: 0` and full initial state in `upserts`
- Subsequent responses increment `sequence` and only include changed items
- Client maintains `Map<ID, hash>` to track seen items
- `upserts`: Items with new/changed hash
- `deletes`: IDs no longer in result set

### Handler Patterns

**Query Handler** (most common pattern):
```
execute(query): Observable<T[]>
  - Use repo(Entity, query).watch(partialMatch) for filtered results
  - Use repo(Entity, query).watchIds(ids) for specific IDs
  - Use repo(Entity, query).watchFilter(fn) for complex predicates
```

**Report Handler** (computed values):
```
execute(report): Observable<T>
  - Compose from other queries/reports via switchMap, combineLatest
  - Return aggregated/transformed data
  - Examples: counts, sums, tree structures, search results
```

**Command Handler**:
```
execute(command): Promise<T>
  - Validate input, fetch required entities
  - Call eventBus.publishSet/publishDel to mutate state
  - Return result (often new entity ID)
  - Can call commandBus.execute for sub-commands
```

### Standard Query Types (per entity)

For entity `Foo`, the TS codebase typically defines:
- `GetFoos` - Get all (optionally scoped)
- `GetFoosByIds { ids: ID[] }` - Get by ID list
- `GetFoosByQuery { partial: Partial<Foo> }` - Filter by partial match
- Additional domain-specific queries as needed

### Standard Command Types (per entity)

For entity `Foo`:
- `CreateFoo { ...fields }` - Returns new ID
- `DeleteFoo { fooId: ID }` - Returns void
- `RenameFoo { fooId: ID, newName: string }` - Returns void
- `UpdateFoo { fooId: ID, ...fields }` - Returns void
- Domain-specific mutations as needed

### Context Propagation

All commands/queries/reports carry context:
- `tx`: Transaction ID (UUID) for correlation
- `commandClientId`: Originating client ID
- `lineage`: Trace of where command originated (e.g., `['client']`)
- `userToken`: Auth token (optional)

Context is passed via `.withContext(parentCtx)` method and propagated through sub-operations.

### Saga Pattern

Sagas are event processors that react to events and produce commands:
```
(events$: Observable<MEvent>) => Observable<MCommand>
```

Used for:
- Cross-entity side effects
- Cleanup operations
- Log retention
- Time-based automation

### Framework vs Application Entities

**Framework Entities** (in @myko/core - must be in @myko/rs):
- `Server`: Cluster node identity and discovery
- `Client`: Connected WebSocket clients with windback state
- `EventContainer`, `GetEventLog`: Event history querying

**Application Entities** (in @rship/entities - dogfooding example, NOT part of framework):
- `Target`, `Emitter`, `Action`, `Scene`, `Binding`, etc.
- These are rship-specific domain entities built ON TOP of myko
- They demonstrate how to use `#[myko_item]` and custom handlers
- Not migrated as part of @myko/rs - they remain a separate application layer

### Peer & Federation System

**Purpose**: Multi-server clustering where servers discover each other and can proxy queries/commands/reports across the cluster.

**Server Entity** (`Server`):
```
id: ID (hostId, typically UUID)
version: string
address: string (IP where reachable)
port: number
startedAt: string (ISO DateTime)
```

**Server Lifecycle**:
1. On startup, delete any stale `Server` records with same address:port
2. Publish new `Server` entity with current hostId
3. Watch `GetPeerServers` query for other servers

**Peer Discovery** (entity-based):
- On init, subscribe to `GetPeerServers` query (returns all `Server` entities except self)
- When new `Server` entities appear in the query results, connect to them
- Servers publish their own `Server` entity on startup, which propagates via event replication
- Optional: Docker DNS seeding via `tasks.<MYKO_SERVICE_NAME>` can bootstrap initial `Server` entities

**Peer Connection** (`PeerClientRegistry`):
1. For each discovered server, create `WSMClient` connection
2. On connect, verify server ID matches expected via `GetConnectedServer` query
3. Subscribe to `ServerEventLog` report to receive peer's events
4. Forward received events to local `peerBus`
5. On disconnect, clean up and delete peer's `Server` entity

**Federation Wrappers**:
```
PeerQuery { query: MQuery, peerId: ID }
  - Routes query to specific peer server
  - If peerId === hostId, execute locally
  - Otherwise, forward via peer WebSocket client

PeerCommand { command: MCommand, peerId: ID }
  - Routes command to specific peer server
  - Validates requesting client is local before forwarding

PeerReport { report: MReport, peerId: ID }
  - Routes report to specific peer server
  - Returns EMPTY if peer not connected
```

**Peer Monitoring Reports**:
```
PeerAlive { peerId: ID } -> Observable<number | false>
  - Pings peer every second, returns latency or false if dead

PeerLastSeen { peerId: ID } -> Observable<string>
  - ISO timestamp of last successful ping

ServerEventLog -> Observable<MEvent>
  - Stream of all events from this server (for peers to subscribe)
```

**Key Queries**:
```
GetConnectedServer -> Server[]  // Returns this server's entity
GetPeerServers -> Server[]      // Returns all other servers in cluster
GetServers -> Server[]          // Returns all known servers
GetServersByClientIds { clientIds[] } -> Server[]  // Servers hosting specific clients
```

**Event Replication**:
- Each server subscribes to peers' `ServerEventLog`
- Received events are published to local `peerBus` (separate from main `eventBus`)
- This enables read-your-writes consistency within a server, eventual consistency across cluster

**Repo `peerQuery` Option**:
- Repos can be configured with `peerQuery: (ids: ID[]) => Observable<T[]>`
- Enables fetching items by ID from peer servers when not found locally
- Used for cross-server entity resolution

### Windback (Time-Travel / State Viewing)

**Purpose**: Allow clients to view historical state.

**TypeScript (Current - Time-Based)**:
```
Client { serverId: ID, windback?: string (ISO DateTime) }
SetClientWindbackTime { windback: string } -> boolean
ClearClientWindbackTime -> void
HistoryProvider interface for event lookup by time:
  - getEntityHistory(id, start?, end?)
  - getItemAsOfTime<T>(id, itemType, time)
  - getAllItemsAsOfTime<T>(itemType, time)
  - getEventsForTransaction(txId)
  - getEventsInTimeRange(start, end?)
```

**Rust (Planned - Commit-Based)**:
Instead of time-based lookup requiring Kafka backwards traversal, Rust uses explicit snapshots:
```
Snapshot { name, message, scope, parent_id }
SnapshotData { snapshot_id, item_type, items }
CreateSnapshot { name, message, scope }
SetClientWindbackSnapshot { snapshot_id }
ClearClientWindback
RestoreSnapshot { snapshot_id }
```
See "Windback Design (Rust)" section for full design.

**Common Behavior**:
- Commands blocked during windback unless marked `allowDuringWindback: true`
- Queries return historical/snapshot data instead of live
- Reports may behave differently (some historical, some live)

### Authentication

**Auth Service Interface** (`MykoAuthService`):
```
canActivate(token: string) -> Promise<boolean>
  - Validates JWT via Auth0 JWKS or matches peerSecret

getUserId(token: string) -> Promise<string | undefined>
  - Extracts `sub` claim from JWT

getPeerToken() -> string
  - Returns cluster secret for peer-to-peer auth
```

**Command Auth Options**:
```
@MykoCommand({ noAuth: true })  // Skip auth check
@MykoCommand({ allowDuringWindback: true })  // Allow in windback mode
```

**noAuthCommands Registry**: Set of command IDs that bypass authentication (e.g., initial handshake commands).

### What's Already Implemented in @myko/rs

**Done**:
- Actor system: Server, EventManager, QueryManager, ReportManager, CommandManager
- WebSocket server/connection actors
- Kafka producer/consumer actors (optional)
- Item/Event/Query/Report/Command base types
- `#[myko_item]` macro generating queries, reports, commands
- Server/Client entities with basic queries
- In-memory mode (Kafka optional)
- Query delta protocol
- Signal stream utilities

**Not Yet Implemented**:
- Relationship system (@belongsTo, @ownsMany, @ensureFor cascades)
- Full-text search (flexsearch equivalent)
- Windback/history provider
- Authentication (Auth0/JWT validation)
- Peer discovery and federation (PeerQuery/PeerCommand/PeerReport)
- Saga pattern
- ReballanceItem command
- Most rship domain entities (need to be defined with `#[myko_item]`)

### Migration Checklist for Rust

**Core Framework** (in @myko/rs):
1. ✅ Item Registration via `inventory` + `#[myko_item]`
2. ✅ Hash Computation (MD5 of serialized content)
3. ✅ Bus Implementation (actor-based with ractor)
4. ✅ Query/Report delta protocol
5. ✅ WebSocket server with MessagePack
6. ✅ Kafka integration (optional)
7. 📐 Relationship Cascades (belongs-to, owns-many, ensure-for) - DESIGNED
8. 📐 Full-text Search integration (tantivy) - DESIGNED
9. 📐 Context propagation (tx, clientId, lineage, hostId) - DESIGNED
10. 📐 Authentication (async-oidc-jwt-validator + peerSecret) - DESIGNED
11. 📐 Windback/Snapshots (version-control approach) - DESIGNED
12. 📐 Peer Discovery (entity-based via GetPeerServers) - DESIGNED
13. 📐 Federation Handlers (PeerQuery/PeerCommand/PeerReport) - DESIGNED
14. 📐 Event Replication (ServerEventLog subscription) - DESIGNED
15. 📐 Saga pattern (stateful stream processors) - DESIGNED

**Application Layer** (separate from framework, e.g. rship):
- Define domain entities using `#[myko_item]`
- Implement custom query/report/command handlers
- Application-specific logic (scene engine, binding execution, etc.)

### Relationship System Design (Rust)

#### Registration via Inventory

```rust
pub enum Relation {
    BelongsTo {
        local_type: &'static str,    // e.g., "Binding"
        local_key: &'static str,     // e.g., "scope_id"
        foreign_type: &'static str,  // e.g., "Scene"
    },
    OwnsMany {
        local_type: &'static str,    // e.g., "Scene"
        local_key: &'static str,     // e.g., "node_ids"
        foreign_type: &'static str,  // e.g., "BindingNode"
    },
    EnsureFor {
        local_type: &'static str,
        dependencies: &'static [(&'static str, &'static str)], // [(foreign_type, local_key)]
        make_default: fn() -> Value,
    },
}

inventory::collect!(Relation);
```

#### Field-Level Attribute Macros

```rust
#[myko_item]
pub struct Binding {
    #[belongs_to(Scene)]
    pub scope_id: Arc<str>,
    pub emitter_id: Arc<str>,
}

#[myko_item]
pub struct Scene {
    pub name: String,
    #[owns_many(BindingNode)]
    pub node_ids: Vec<Arc<str>>,
}

#[myko_item]
#[ensure_for(Project, Session)]  // Creates one per Project×Session combination
pub struct SessionVariable {
    pub project_id: Arc<str>,
    pub session_id: Arc<str>,
    #[default_value("unnamed")]
    pub name: String,
}
```

#### RelationshipManager Actor

Dedicated actor that subscribes to events and processes cascades:

```rust
pub struct RelationshipManager {
    relations: Vec<Relation>,
    host_id: Uuid,
    event_handler: ActorRef<EventHandlerMsg>,
}

enum RelationshipManagerMsg {
    EventOccurred(MEvent),
    EstablishRelations,  // Called after Kafka catchup, before WebSocket
}
```

#### Cascade Logic

```rust
impl RelationshipManager {
    async fn process_cascade(&self, relation: &Relation, event: &MEvent) {
        // Only process events originating from this server (includes executor connections)
        if event.source_id != self.host_id {
            return;
        }
        if event.options.prevent_relationship_updates {
            return;
        }

        match (relation, event.change_type) {
            // belongs-to: Parent deleted → delete all children
            (Relation::BelongsTo { foreign_type, local_type, local_key }, ChangeType::Del)
                if event.item_type == *foreign_type => {
                    let children = self.query_by_field(local_type, local_key, &event.item.id).await;
                    for child in children {
                        self.publish_del(child, &event.tx).await;
                    }
                }

            // owns-many: Parent deleted → delete all owned children
            (Relation::OwnsMany { local_type, local_key, foreign_type }, ChangeType::Del)
                if event.item_type == *local_type => {
                    let child_ids: Vec<Arc<str>> = event.item.get_field(local_key);
                    for child in self.get_by_ids(foreign_type, &child_ids).await {
                        self.publish_del(child, &event.tx).await;
                    }
                }

            // owns-many: Child deleted → remove from parent's array, recalculate hash
            (Relation::OwnsMany { local_type, local_key, foreign_type }, ChangeType::Del)
                if event.item_type == *foreign_type => {
                    let parents = self.query_array_contains(local_type, local_key, &event.item.id).await;
                    for mut parent in parents {
                        parent.remove_from_array(local_key, &event.item.id);
                        self.publish_set(parent, &event.tx).await;
                    }
                }

            // ensure-for: Dependency created → ensure local entity exists
            (Relation::EnsureFor { dependencies, local_type, make_default }, ChangeType::Set)
                if dependencies.iter().any(|(ft, _)| *ft == event.item_type) => {
                    self.ensure_cartesian_product(relation, &event.tx).await;
                }

            _ => {}
        }
    }
}
```

#### Internal Query Interface

EventHandler exposes sync query methods for RelationshipManager (not over WebSocket):

```rust
impl EventHandler {
    pub(crate) fn query_by_field_sync(
        &self,
        item_type: &str,
        field: &str,
        value: &str
    ) -> Vec<Value> {
        // Direct lookup on in-memory HashMap
    }

    pub(crate) fn query_array_contains_sync(
        &self,
        item_type: &str,
        field: &str,
        value: &str
    ) -> Vec<Value> {
        // Scan items where field (array) contains value
    }
}
```

#### Compile-Time Cycle Detection

Use build.rs to detect circular relationships at compile time:

```rust
// build.rs
fn main() {
    let relations = scan_for_relations("src/");
    if let Some(cycle) = detect_cycle(&relations) {
        let out = std::env::var("OUT_DIR").unwrap();
        std::fs::write(
            format!("{}/relation_cycle_check.rs", out),
            format!("compile_error!(\"Circular relationship detected: {}\");", cycle)
        ).unwrap();
    }
    println!("cargo:rerun-if-changed=src/");
}

// In lib.rs
include!(concat!(env!("OUT_DIR"), "/relation_cycle_check.rs"));
```

#### Startup Sequence

1. Kafka consumers catch up to head
2. `RelationshipManager::EstablishRelations` runs:
   - Orphan cleanup for owns-many relationships
   - Ensure-for initialization (Cartesian product creation)
3. WebSocket server starts accepting connections

#### Design Decisions

| Decision | Rationale |
|----------|-----------|
| Separate actor | Keeps EventManager simple, relationship logic isolated |
| Inventory registration | Consistent with existing item/query/report pattern |
| Field-level attributes | Ergonomic, matches TS decorator placement |
| Internal sync queries | Fast, avoids query protocol overhead |
| Same tx propagation | Cascaded events share transaction ID |
| Source filtering | Only process events from this server (includes local executors) |
| Compile-time cycle check | Fail fast on invalid relationship graphs |

### Full-Text Search Design (Rust)

#### Library Choice: tantivy

Use **tantivy** with RAM directory for in-memory full-text search (Rust equivalent of flexsearch).

#### Searchable Trait and Macro

```rust
pub trait Searchable {
    fn searchable_fields() -> &'static [&'static str];
    fn searchable_text(&self) -> Vec<(&'static str, String)>;
}

#[myko_item]
pub struct Target {
    #[searchable]
    pub name: String,
    #[searchable]
    pub category: String,
    pub service_id: Arc<str>,  // not searchable
}

// Generates Searchable impl + inventory registration
```

#### SearchManager Actor

```rust
pub struct SearchManager {
    indices: HashMap<String, EntityIndex>,  // item_type -> tantivy index
    subscriptions: Vec<SearchSubscription>,
}

struct EntityIndex {
    index: Index,          // tantivy RAM index
    writer: IndexWriter,
    reader: IndexReader,
    id_field: Field,
    text_field: Field,     // Combined searchable text
}

pub enum SearchManagerMsg {
    EventOccurred(MEvent),
    Search { item_type: String, query: String, reply: RpcReplyPort<Vec<Arc<str>>> },
    Subscribe { item_type: String, query: String, subscriber: ActorRef<SearchUpdate> },
    Unsubscribe { subscription_id: Uuid },
}
```

#### Indexing Logic

```rust
impl SearchManager {
    fn handle_event(&mut self, event: &MEvent) {
        let Some(idx) = self.indices.get_mut(&event.item_type) else { return };

        match event.change_type {
            ChangeType::Set => {
                // Delete existing (handles updates)
                idx.writer.delete_term(Term::from_field_text(idx.id_field, &event.item.id));

                // Add document with combined searchable text
                let mut doc = Document::new();
                doc.add_text(idx.id_field, &event.item.id);
                doc.add_text(idx.text_field, &extract_searchable_text(&event.item));
                idx.writer.add_document(doc).unwrap();
            }
            ChangeType::Del => {
                idx.writer.delete_term(Term::from_field_text(idx.id_field, &event.item.id));
            }
        }

        // Debounced commit (10-50ms) for imperceptible batching
        self.schedule_commit(&event.item_type);
    }

    fn search(&self, item_type: &str, query: &str) -> Vec<Arc<str>> {
        let Some(idx) = self.indices.get(item_type) else { return vec![] };

        let searcher = idx.reader.searcher();
        let query_parser = QueryParser::for_index(&idx.index, vec![idx.text_field]);
        let parsed = query_parser.parse_query(&query.to_lowercase()).unwrap();

        searcher.search(&parsed, &TopDocs::with_limit(1000))
            .unwrap()
            .iter()
            .filter_map(|(_, addr)| {
                searcher.doc(*addr).ok()?.get_first(idx.id_field)?.as_text().map(Arc::from)
            })
            .collect()
    }
}
```

#### Reactive Subscriptions

```rust
struct SearchSubscription {
    id: Uuid,
    item_type: String,
    query: String,
    subscriber: ActorRef<SearchUpdate>,
    last_results: HashSet<Arc<str>>,
}

impl SearchManager {
    fn notify_subscribers(&mut self, item_type: &str) {
        for sub in &mut self.subscriptions {
            if sub.item_type != item_type { continue }

            let new_results: HashSet<_> = self.search(&sub.item_type, &sub.query).into_iter().collect();
            if new_results != sub.last_results {
                sub.last_results = new_results.clone();
                let _ = sub.subscriber.send_message(SearchUpdate::Results(new_results.into_iter().collect()));
            }
        }
    }
}
```

#### EntitySearch Report (Framework-Level)

```rust
#[myko_report]
pub struct EntitySearch {
    pub entity_type: String,
    pub query: String,
    pub show_all_on_empty: bool,
}

// Handler: SearchManager returns IDs → QueryManager fetches full items
```

#### Registration via Inventory

```rust
pub struct SearchableRegistration {
    pub item_type: &'static str,
    pub fields: &'static [&'static str],
    pub extractor: fn(&Value) -> String,
}

inventory::collect!(SearchableRegistration);
```

#### Design Decisions

| Decision | Rationale |
|----------|-----------|
| tantivy + RAM | Rust-native, fast, in-memory like flexsearch |
| Separate actor | Independent scaling, doesn't block EventManager |
| Combined text field | Simple queries, sufficient for UI search |
| Debounced commits | Real-time feel with imperceptible batching (10-50ms) |
| Returns IDs only | Search finds, QueryManager fetches - separation of concerns |
| Subscription diffing | Only notify on actual result changes |
| Fuzzy matching | Enabled by default for typo tolerance (tantivy FuzzyTermQuery) |
| Field ranking | Not needed now, can add later if required |

### Saga Pattern Design (Rust)

Sagas are stateful stream processors that react to events and emit commands on state transitions.

#### Saga Trait

```rust
#[myko_saga]
pub struct MySaga;

impl Saga for MySaga {
    fn build(events: EventStream) -> CommandStream {
        events
            .of_items::<Target>()
            .of_type(ChangeType::Set)
            // ... stream operations
            .into_commands(|event| Some(MyCommand { ... }))
    }
}
```

#### Stream Operators

```rust
pub trait SagaStream: Stream<Item = MEvent> {
    // Filtering
    fn of_items<T: Item>(self) -> impl SagaStream;
    fn of_type(self, change_type: ChangeType) -> impl SagaStream;
    fn filter<F: Fn(&MEvent) -> bool>(self, f: F) -> impl SagaStream;

    // State accumulation
    fn scan<S, F>(self, initial: S, f: F) -> impl Stream<Item = S>
        where F: Fn(&mut S, MEvent) -> S;

    // Transition detection
    fn pairwise(self) -> impl Stream<Item = (MEvent, MEvent)>;
    fn distinct_until_changed<K, F>(self, key: F) -> impl SagaStream
        where F: Fn(&MEvent) -> K, K: Eq;

    // Timing
    fn debounce(self, duration: Duration) -> impl SagaStream;
    fn buffer_time(self, duration: Duration) -> impl Stream<Item = Vec<MEvent>>;
    fn throttle(self, duration: Duration) -> impl SagaStream;

    // Per-entity state
    fn group_by<K, F>(self, key: F) -> impl Stream<Item = impl SagaStream>
        where F: Fn(&MEvent) -> K, K: Eq + Hash;

    // Async operations
    fn flat_map_async<F, Fut>(self, f: F) -> impl SagaStream
        where F: Fn(MEvent) -> Fut, Fut: Future<Output = Option<MEvent>>;

    // Terminal
    fn into_commands<F, C>(self, f: F) -> CommandStream
        where F: Fn(MEvent) -> Option<C>, C: Command;
}
```

#### Example: State Transition Detection

```rust
#[myko_saga]
pub struct BuildStateTransitionSaga;

impl Saga for BuildStateTransitionSaga {
    fn build(events: EventStream) -> CommandStream {
        events
            .of_items::<ActiveScene>()
            .of_type(ChangeType::Set)
            .pairwise()
            .filter_map(|(prev, curr)| {
                // Detect: building-on → active transition
                if prev.item.build_status == "building-on"
                   && curr.item.build_status == "active" {
                    Some(SceneBuiltOn { scene_id: curr.item.scene_id }.into_command())
                } else {
                    None
                }
            })
    }
}
```

#### Example: Accumulated State

```rust
#[myko_saga]
pub struct PlaybackResumeSaga;

impl Saga for PlaybackResumeSaga {
    fn build(events: EventStream) -> CommandStream {
        events
            .of_items::<CuePlayback>()
            .scan(
                PlaybackState::default(),
                |state, event| {
                    let was_paused = state.is_paused;
                    state.is_paused = event.item.state == "paused";
                    state.is_active = event.item.state == "active";
                    state.just_resumed = was_paused && state.is_active;
                    state.clone()
                }
            )
            .filter(|state| state.just_resumed)
            .map(|state| ResumePlayback { id: state.id }.into_command())
    }
}
```

#### Example: Per-Entity Grouped State

```rust
#[myko_saga]
pub struct PerSceneTransitionSaga;

impl Saga for PerSceneTransitionSaga {
    fn build(events: EventStream) -> CommandStream {
        events
            .of_items::<ActiveScene>()
            .group_by(|e| e.item.scene_id.clone())  // Separate state per scene
            .flat_map(|scene_stream| {
                scene_stream
                    .pairwise()
                    .filter_map(|(prev, curr)| detect_transition(&prev, &curr))
            })
    }
}
```

#### Example: Debounced Batch Cleanup

```rust
#[myko_saga]
pub struct DebouncedCleanupSaga;

impl Saga for DebouncedCleanupSaga {
    fn build(events: EventStream) -> CommandStream {
        events
            .of_items::<LogEntry>()
            .of_type(ChangeType::Set)
            .buffer_time(Duration::from_secs(5))
            .filter(|batch| !batch.is_empty())
            .flat_map_async(|_batch, ctx| async move {
                let old = ctx.repo::<LogEntry>().filter(|e| e.is_old()).await;
                old.into_iter()
                    .map(|e| DeleteLogEntry { id: e.id }.into_command())
                    .collect::<Vec<_>>()
            })
    }
}
```

#### Actor Implementation

Each saga compiles to an actor with accumulated state:

```rust
struct SagaActor<S: Saga> {
    state: S::State,
}

impl<S: Saga> Actor for SagaActor<S> {
    async fn handle(&mut self, msg: SagaMsg, ctx: &ActorContext) {
        if let SagaMsg::Event(event) = msg {
            if let Some(commands) = self.saga.process(event, &mut self.state) {
                for cmd in commands {
                    ctx.send(command_manager, CommandManagerMsg::Execute(cmd));
                }
            }
        }
    }
}
```

#### Design Decisions

| Decision | Rationale |
|----------|-----------|
| Stream combinators | Match RxJS patterns from TS codebase |
| `scan` for state | Accumulate across events for transition detection |
| `pairwise` | Compare prev/current for state machine transitions |
| `group_by` | Per-entity state tracking (e.g., per-scene) |
| `buffer_time`/`debounce` | Batch rapid events, reduce command frequency |
| Actor-based | Each saga is isolated, owns its state |

### Context Propagation Design (Rust)

All sub-operations (commands, queries, reports, events) automatically inherit context from the originating request.

#### RequestContext Structure

```rust
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Transaction ID - same across all operations in one request
    pub tx: Uuid,

    /// Originating client (WebSocket connection that started this)
    pub client_id: Option<Arc<str>>,

    /// User identity from JWT (if authenticated)
    pub user_id: Option<Arc<str>>,

    /// Call chain for tracing: ["client", "CreateScene", "CreateFolder"]
    pub lineage: Vec<Arc<str>>,

    /// Server that received the original request
    pub host_id: Uuid,
}

impl RequestContext {
    /// Extend lineage when making a sub-call
    pub fn child(&self, operation: &str) -> Self {
        let mut lineage = self.lineage.clone();
        lineage.push(Arc::from(operation));
        Self { lineage, ..self.clone() }
    }
}
```

#### CommandContext (for command handlers)

```rust
pub struct CommandContext {
    req: RequestContext,
    command_manager: ActorRef<CommandManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
    event_manager: ActorRef<EventManagerMsg>,
}

impl CommandContext {
    /// Execute sub-command (context auto-propagated + lineage extended)
    pub async fn execute<C: Command>(&self, cmd: C) -> Result<C::Response> {
        let child_ctx = self.req.child(C::NAME);
        self.command_manager.call(CommandManagerMsg::Execute {
            command: Box::new(cmd),
            context: child_ctx,
        }).await
    }

    /// Query with context auto-injected
    pub async fn query<Q: Query>(&self, query: Q) -> Result<Q::Response> {
        let child_ctx = self.req.child(Q::NAME);
        self.query_manager.call(QueryManagerMsg::Execute {
            query: Box::new(query),
            context: child_ctx,
        }).await
    }

    /// One-shot report with context
    pub async fn report<R: Report>(&self, report: R) -> Result<R::Response> {
        let child_ctx = self.req.child(R::NAME);
        self.report_manager.call(ReportManagerMsg::Execute {
            report: Box::new(report),
            context: child_ctx,
        }).await
    }

    /// Publish SET event with context
    pub async fn publish_set<T: Item>(&self, item: T) -> Result<()> {
        self.event_manager.send(EventManagerMsg::PublishSet {
            item: item.into_value(),
            context: self.req.clone(),
        })
    }

    /// Publish DEL event with context
    pub async fn publish_del<T: Item>(&self, item: T) -> Result<()> {
        self.event_manager.send(EventManagerMsg::PublishDel {
            item: item.into_value(),
            context: self.req.clone(),
        })
    }

    /// Access context for inspection
    pub fn request(&self) -> &RequestContext { &self.req }
    pub fn tx(&self) -> Uuid { self.req.tx }
    pub fn client_id(&self) -> Option<&str> { self.req.client_id.as_deref() }
    pub fn host_id(&self) -> Uuid { self.req.host_id }
}
```

#### QueryContext / ReportContext (restricted access)

```rust
pub struct QueryContext {
    req: RequestContext,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
    // No command_manager or event_manager - queries are read-only
}

pub struct ReportContext {
    req: RequestContext,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
}
```

#### Handler Signatures

```rust
#[myko_command_handler(CreateScene)]
async fn handle(cmd: CreateScene, ctx: &CommandContext) -> Result<SceneId> {
    // Full access: commands, queries, reports, events
    let project = ctx.query(GetProjectById { id: cmd.project_id }).await?;
    ctx.publish_set(Scene { ... }).await;
    Ok(scene_id)
}

#[myko_query_handler(GetScenesByProject)]
fn handle(query: GetScenesByProject, ctx: &QueryContext) -> QueryStream<Scene> {
    // Read-only: queries, reports (no commands, no events)
}

#[myko_report_handler(SceneCount)]
fn handle(report: SceneCount, ctx: &ReportContext) -> ReportStream<usize> {
    // Read-only: queries, reports (no commands, no events)
}
```

#### WebSocket Initial Context Creation

```rust
impl WebSocketConnection {
    fn handle_command(&self, wrapped: WrappedCommand) {
        let context = RequestContext {
            tx: wrapped.tx,
            client_id: Some(self.client_id.clone()),
            user_id: self.user_id.clone(),
            lineage: vec![Arc::from("client")],
            host_id: self.host_id,
        };

        self.command_manager.send(CommandManagerMsg::Execute {
            command: parse_command(wrapped),
            context,
        });
    }
}
```

#### Tracing Integration

```rust
impl CommandContext {
    pub async fn execute<C: Command>(&self, cmd: C) -> Result<C::Response> {
        let child_ctx = self.req.child(C::NAME);

        let span = tracing::info_span!(
            "command",
            tx = %child_ctx.tx,
            client_id = ?child_ctx.client_id,
            command = C::NAME,
            lineage = ?child_ctx.lineage,
        );

        async { self.command_manager.call(...).await }.instrument(span).await
    }
}
```

#### Design Decisions

| Decision | Rationale |
|----------|-----------|
| Context as method receiver | All sub-calls go through ctx, automatic injection |
| Separate context types | CommandContext can mutate, QueryContext is read-only |
| `child()` for lineage | Explicit extension, traceable call chains |
| Tracing span integration | OpenTelemetry-ready, distributed tracing |
| No manual `.withContext()` | Cleaner than TS - context is structural |
| host_id in context | Consistent naming with rest of codebase |

### Authentication Design (Rust)

#### Crates

- [`async-oidc-jwt-validator`](https://crates.io/crates/async-oidc-jwt-validator) - JWT validation with auto JWKS discovery/caching
- [`jsonwebtoken`](https://crates.io/crates/jsonwebtoken) - Low-level JWT decode (for extracting claims)

#### AuthService Trait

```rust
#[async_trait]
pub trait AuthService: Send + Sync {
    async fn validate(&self, token: &str) -> Result<bool>;
    async fn get_user_id(&self, token: &str) -> Result<Option<Arc<str>>>;
    fn peer_token(&self) -> &str;
}
```

#### Auth0 Implementation

```rust
use async_oidc_jwt_validator::Validator;

pub struct Auth0Service {
    peer_secret: String,
    validator: Validator,
}

impl Auth0Service {
    pub async fn new(peer_secret: String, auth0_domain: String) -> Result<Self> {
        let issuer_url = format!("https://{auth0_domain}/");
        let validator = Validator::new(&issuer_url).await?;
        Ok(Self { peer_secret, validator })
    }
}

#[async_trait]
impl AuthService for Auth0Service {
    async fn validate(&self, token: &str) -> Result<bool> {
        // Fast path: peer secret
        if token == self.peer_secret {
            return Ok(true);
        }

        // OIDC JWT validation (JWKS cached internally)
        self.validator.validate(token).await.map(|_| true)
    }

    async fn get_user_id(&self, token: &str) -> Result<Option<Arc<str>>> {
        if token == self.peer_secret {
            return Ok(None);  // Peer tokens have no user
        }

        let claims = self.validator.validate(token).await?;
        Ok(claims.sub.map(Arc::from))
    }

    fn peer_token(&self) -> &str {
        &self.peer_secret
    }
}
```

#### Command Auth Attributes

```rust
#[myko_command]                        // Default: requires auth
#[myko_command(no_auth)]               // Skip auth check
#[myko_command(allow_during_windback)] // Can execute in windback mode
#[myko_command(no_auth, allow_during_windback)]  // Both
```

#### AuthRegistry (runtime lookup)

```rust
pub struct AuthRegistry {
    no_auth_commands: HashSet<&'static str>,
    windback_allowed: HashSet<&'static str>,
}

impl AuthRegistry {
    pub fn from_inventory() -> Self { /* collect from inventory */ }
    pub fn requires_auth(&self, cmd: &str) -> bool { !self.no_auth_commands.contains(cmd) }
    pub fn allowed_during_windback(&self, cmd: &str) -> bool { self.windback_allowed.contains(cmd) }
}
```

#### WebSocket Auth Flow

```rust
impl WebSocketConnection {
    async fn handle_set_user(&mut self, token: String) {
        match self.auth.validate(&token).await {
            Ok(true) => {
                self.authenticated = true;
                self.user_id = self.auth.get_user_id(&token).await.ok().flatten();
                self.is_peer = token == self.auth.peer_token();
                self.send(AuthSuccess).await;
            }
            _ => {
                self.send(AuthFailed).await;
                self.disconnect().await;
            }
        }
    }

    async fn handle_command(&mut self, cmd: WrappedCommand) {
        // Check auth
        if self.auth_registry.requires_auth(&cmd.command_id) && !self.authenticated {
            return self.send(CommandError { message: "Auth required" }).await;
        }

        // Check windback
        if self.windback.is_some() && !self.auth_registry.allowed_during_windback(&cmd.command_id) {
            return self.send(CommandError { message: "Not allowed during windback" }).await;
        }

        // Execute...
    }
}
```

#### Design Decisions

| Decision | Rationale |
|----------|-----------|
| `async-oidc-jwt-validator` | Batteries-included JWKS caching, works with Auth0 |
| Peer secret fast path | Skip JWT decode for cluster traffic |
| Attribute macros | Compile-time command auth metadata |
| Secure by default | Commands require auth unless `no_auth` |
| Queries/reports open | Read operations don't need auth by default |

### Windback Design (Rust) - Version-Control Approach

Rather than time-based windback requiring Kafka backwards-lookup, we use a version-control model where users explicitly commit snapshots of state at meaningful points.

#### Core Concepts

| Concept | Description |
|---------|-------------|
| Snapshot | Named save-point of state, like a git commit |
| Scope | What entities to include (Scene, Project, or custom) |
| Commit | User action that creates a snapshot |
| Checkout | View historical state from a snapshot |
| Restore | Copy snapshot state back to live |

#### Snapshot Entity

```rust
#[myko_item]
pub struct Snapshot {
    /// Human-readable name (e.g., "Pre-show setup")
    pub name: String,

    /// Description of what this snapshot captures
    pub message: String,

    /// When this snapshot was created
    pub created_at: DateTime<Utc>,

    /// User who created this snapshot
    pub created_by: Arc<str>,

    /// What this snapshot covers
    pub scope: SnapshotScope,

    /// Previous snapshot in this scope's chain (for history)
    pub parent_id: Option<Arc<str>>,

    /// Total item count (for display)
    pub item_count: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SnapshotScope {
    /// All entities belonging to a scene
    Scene { scene_id: Arc<str> },

    /// All entities in a project
    Project { project_id: Arc<str> },

    /// Custom selection of entity types
    Custom {
        item_types: Vec<String>,
        filter: Option<Value>,
    },

    /// Global - all entities (use sparingly)
    Global,
}
```

#### SnapshotData (Stored State)

```rust
/// Stores actual entity state at snapshot time
/// Separate from Snapshot metadata for efficient loading
#[myko_item]
pub struct SnapshotData {
    pub snapshot_id: Arc<str>,
    pub item_type: String,
    /// Serialized items at snapshot time
    pub items: Vec<Value>,
}
```

#### Commands

```rust
#[myko_command]
pub struct CreateSnapshot {
    pub name: String,
    pub message: String,
    pub scope: SnapshotScope,
}
// Response: Snapshot (the created snapshot)

#[myko_command(allow_during_windback)]
pub struct ListSnapshots {
    pub scope: SnapshotScope,
}
// Response: Vec<Snapshot>

#[myko_command]
pub struct RestoreSnapshot {
    pub snapshot_id: Arc<str>,
}
// Response: () - replaces live state with snapshot data

#[myko_command]
pub struct DeleteSnapshot {
    pub snapshot_id: Arc<str>,
}
// Response: ()

// Client windback mode (viewing snapshots)
#[myko_command(allow_during_windback)]
pub struct SetClientWindbackSnapshot {
    pub snapshot_id: Arc<str>,
}
// Response: true

#[myko_command(allow_during_windback)]
pub struct ClearClientWindback;
// Response: ()
```

#### SnapshotManager Actor

```rust
pub struct SnapshotManager {
    /// Persistent storage for snapshot data (Postgres, SQLite, etc.)
    storage: Box<dyn SnapshotStorage>,

    /// Query manager for fetching current state
    query_manager: ActorRef<QueryManagerMsg>,
}

pub enum SnapshotManagerMsg {
    Create { scope: SnapshotScope, name: String, message: String, user_id: Arc<str>, reply: RpcReplyPort<Snapshot> },
    GetData { snapshot_id: Arc<str>, reply: RpcReplyPort<Vec<SnapshotData>> },
    Delete { snapshot_id: Arc<str>, reply: RpcReplyPort<()> },
}
```

#### Storage Trait

```rust
#[async_trait]
pub trait SnapshotStorage: Send + Sync {
    /// Store snapshot and all its data atomically
    async fn save(&self, snapshot: Snapshot, data: Vec<SnapshotData>) -> Result<()>;

    /// Load snapshot data for a given snapshot
    async fn load(&self, snapshot_id: &str) -> Result<Vec<SnapshotData>>;

    /// Delete snapshot and all associated data
    async fn delete(&self, snapshot_id: &str) -> Result<()>;
}

// Implementations: PostgresSnapshotStorage, SqliteSnapshotStorage
```

#### Create Snapshot Flow

```rust
impl SnapshotManager {
    async fn create_snapshot(
        &self,
        scope: SnapshotScope,
        name: String,
        message: String,
        user_id: Arc<str>,
    ) -> Result<Snapshot> {
        // 1. Query all entities matching scope
        let items = self.collect_scope_items(&scope).await?;

        // 2. Find parent (previous snapshot in this scope)
        let parent_id = self.find_latest_snapshot(&scope).await?;

        // 3. Create snapshot metadata
        let snapshot = Snapshot {
            id: Uuid::new_v4().to_string().into(),
            name,
            message,
            created_at: Utc::now(),
            created_by: user_id,
            scope,
            parent_id,
            item_count: items.iter().map(|d| d.items.len() as u32).sum(),
            hash: None, // computed
        };

        // 4. Persist atomically
        self.storage.save(snapshot.clone(), items).await?;

        Ok(snapshot)
    }

    async fn collect_scope_items(&self, scope: &SnapshotScope) -> Result<Vec<SnapshotData>> {
        match scope {
            SnapshotScope::Scene { scene_id } => {
                // Collect: Scene, BindingNodes, Bindings, etc. that belong to this scene
                self.collect_scene_entities(scene_id).await
            }
            SnapshotScope::Project { project_id } => {
                // All project entities
                self.collect_project_entities(project_id).await
            }
            SnapshotScope::Custom { item_types, filter } => {
                self.collect_custom_entities(item_types, filter.as_ref()).await
            }
            SnapshotScope::Global => {
                self.collect_all_entities().await
            }
        }
    }
}
```

#### Query Interception (Windback Mode)

```rust
impl QueryManager {
    async fn execute_query(
        &self,
        query: Box<dyn Query>,
        ctx: &RequestContext,
        client_state: &ClientState,
    ) -> Result<QueryStream> {
        // Check if client is in windback mode
        if let Some(snapshot_id) = &client_state.windback_snapshot_id {
            return self.execute_windback_query(query, snapshot_id).await;
        }

        // Normal live query
        self.execute_live_query(query, ctx).await
    }

    async fn execute_windback_query(
        &self,
        query: Box<dyn Query>,
        snapshot_id: &str,
    ) -> Result<QueryStream> {
        // Load snapshot data
        let data = self.snapshot_manager.call(GetData { snapshot_id }).await?;

        // Find matching items and return static stream
        let items = data
            .iter()
            .filter(|d| d.item_type == query.item_type())
            .flat_map(|d| d.items.clone())
            .filter(|item| query.matches(item))
            .collect::<Vec<_>>();

        // Return static stream (no updates - it's historical)
        Ok(QueryStream::static_snapshot(items))
    }
}
```

#### WebSocket Client State

```rust
pub struct ClientState {
    /// If set, client is viewing this snapshot instead of live data
    pub windback_snapshot_id: Option<Arc<str>>,
}

impl WebSocketConnection {
    async fn handle_command(&mut self, cmd: WrappedCommand) {
        // Block commands during windback (except allowed ones)
        if self.client_state.windback_snapshot_id.is_some() {
            if !self.auth_registry.allowed_during_windback(&cmd.command_id) {
                return self.send(CommandError {
                    message: "Commands blocked during snapshot viewing. Exit windback mode first."
                }).await;
            }
        }

        // Execute...
    }
}
```

#### Restore Flow

```rust
impl SnapshotManager {
    async fn restore_snapshot(
        &self,
        snapshot_id: &str,
        ctx: &RequestContext,
    ) -> Result<()> {
        // 1. Load snapshot data
        let snapshot = self.get_snapshot(snapshot_id).await?;
        let data = self.storage.load(snapshot_id).await?;

        // 2. For each item type in scope, replace live state
        for snapshot_data in data {
            // Delete all current items of this type in scope
            let current = self.query_current(&snapshot.scope, &snapshot_data.item_type).await?;
            for item in current {
                self.event_manager.send(PublishDel { item, context: ctx.clone() }).await?;
            }

            // Publish all snapshot items
            for item in snapshot_data.items {
                self.event_manager.send(PublishSet { item, context: ctx.clone() }).await?;
            }
        }

        Ok(())
    }
}
```

#### Snapshot Comparison / Diff

```rust
#[myko_report]
pub struct SnapshotDiff {
    pub from_snapshot_id: Arc<str>,
    pub to_snapshot_id: Option<Arc<str>>, // None = compare to live
}

pub struct DiffResult {
    pub added: Vec<ItemSummary>,
    pub removed: Vec<ItemSummary>,
    pub changed: Vec<ItemChange>,
}

pub struct ItemChange {
    pub item_id: Arc<str>,
    pub item_type: String,
    pub from_hash: Arc<str>,
    pub to_hash: Arc<str>,
}
```

#### Design Decisions

| Decision | Rationale |
|----------|-----------|
| Commit-based vs time-based | Users think in terms of meaningful save points, not timestamps |
| Scopes | Snapshots make sense for logical units (Scene, Project), not arbitrary entity sets |
| Separate SnapshotData | Metadata loads fast; bulk data loads on demand |
| Static query streams | Historical data doesn't change - no need for reactive updates |
| Parent chain | Enables "undo" by restoring previous snapshot, history navigation |
| Restore copies data | Snapshot preserved after restore; creates new live events |

#### Benefits Over Time-Based Windback

| Benefit | Description |
|---------|-------------|
| No Kafka lookups | Snapshots stored separately, no backwards traversal needed |
| User intent captured | "Pre-show setup" is more meaningful than "2024-01-15 3:00 PM" |
| Efficient storage | Only store snapshots at user-chosen points, not every event |
| Simple mental model | Like git: commit, checkout, restore |
| Branching possible | Future: could support branching from snapshots |

### Peer Discovery & Federation Design (Rust)

Multi-server clustering where servers discover each other and proxy operations across the cluster.

#### Core Entities (Framework Level)

```rust
#[myko_item]
pub struct Server {
    /// Server version (e.g., "1.0.0")
    pub version: String,
    /// IP address where reachable (e.g., "10.0.0.5")
    pub address: String,
    /// Port the server is listening on
    pub port: u16,
    /// When this server started (ISO DateTime)
    pub started_at: String,
}

#[myko_query(Server)]
pub struct GetConnectedServer;  // Returns this server

#[myko_query(Server)]
pub struct GetPeerServers;  // Returns all servers except self

#[myko_report]
pub struct ServerEventLog;  // Streams events originating from this server
```

#### PeerManager Actor

Central actor managing peer connections:

```rust
pub struct PeerManager {
    host_id: Uuid,
    host_address: String,
    host_port: u16,

    /// Active peer connections
    peers: HashMap<Uuid, PeerConnection>,

    /// Addresses we're trying to connect to (prevents duplicates)
    connecting: HashSet<String>,

    /// Authentication service for peer tokens
    auth: Arc<dyn AuthService>,

    /// Local event bus to publish peer events
    event_manager: ActorRef<EventManagerMsg>,

    /// Query manager for GetPeerServers subscription
    query_manager: ActorRef<QueryManagerMsg>,
}

pub struct PeerConnection {
    server_id: Uuid,
    client: MykoClient,
    /// Subscription to peer's ServerEventLog
    event_subscription: Option<JoinHandle<()>>,
}

pub enum PeerManagerMsg {
    /// Server entity appeared - try to connect
    ServerDiscovered(Server),
    /// Server entity removed - disconnect
    ServerRemoved { server_id: Uuid },
    /// Peer connected successfully
    PeerConnected { server_id: Uuid, client: MykoClient },
    /// Peer disconnected
    PeerDisconnected { server_id: Uuid },
    /// Forward query to peer
    ForwardQuery { peer_id: Uuid, query: Box<dyn Query>, reply: RpcReplyPort<QueryStream> },
    /// Forward command to peer
    ForwardCommand { peer_id: Uuid, command: Box<dyn Command>, reply: RpcReplyPort<CommandResult> },
    /// Forward report to peer
    ForwardReport { peer_id: Uuid, report: Box<dyn Report>, reply: RpcReplyPort<ReportStream> },
    /// Peer event received (from ServerEventLog subscription)
    PeerEventReceived(MEvent),
}
```

#### Discovery Flow

```rust
impl PeerManager {
    async fn start(&self, ctx: &ActorContext) {
        // 1. Clean up stale server records with same address:port
        let stale = self.query_servers_at_address(&self.host_address, self.host_port).await;
        for server in stale {
            self.event_manager.send(PublishDel { item: server, tx: Uuid::new_v4() }).await;
        }

        // 2. Publish our own Server entity
        let server = Server {
            id: self.host_id.to_string().into(),
            version: env!("CARGO_PKG_VERSION").into(),
            address: self.host_address.clone(),
            port: self.host_port,
            started_at: Utc::now().to_rfc3339(),
            hash: None,
        };
        self.event_manager.send(PublishSet { item: server, tx: Uuid::new_v4() }).await;

        // 3. Subscribe to GetPeerServers query
        let peer_stream = self.query_manager.call(Subscribe {
            query: Box::new(GetPeerServers),
        }).await;

        // 4. Spawn task to handle peer discovery
        let self_ref = ctx.actor_ref().clone();
        tokio::spawn(async move {
            while let Some(servers) = peer_stream.next().await {
                for server in servers {
                    self_ref.send_message(PeerManagerMsg::ServerDiscovered(server));
                }
            }
        });
    }

    async fn handle_server_discovered(&mut self, server: Server) {
        let address_key = format!("{}:{}", server.address, server.port);

        // Skip if already connected or connecting
        if self.peers.contains_key(&server.id.parse().unwrap())
           || self.connecting.contains(&address_key) {
            return;
        }

        self.connecting.insert(address_key.clone());

        // Connect in background
        let self_ref = self.actor_ref.clone();
        let auth_token = self.auth.peer_token().to_string();
        tokio::spawn(async move {
            match MykoClient::connect(&server.address, server.port, &auth_token).await {
                Ok(client) => {
                    // Verify server ID matches
                    if let Ok(connected) = client.query_one::<Server>(GetConnectedServer).await {
                        if connected.id == server.id {
                            self_ref.send_message(PeerManagerMsg::PeerConnected {
                                server_id: server.id.parse().unwrap(),
                                client,
                            });
                            return;
                        }
                    }
                    // ID mismatch - delete stale Server entity
                    self_ref.send_message(PeerManagerMsg::ServerRemoved {
                        server_id: server.id.parse().unwrap(),
                    });
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to peer {}: {}", address_key, e);
                }
            }
        });
    }

    async fn handle_peer_connected(&mut self, server_id: Uuid, client: MykoClient) {
        // Subscribe to peer's event log
        let event_stream = client.watch_report(ServerEventLog).await;
        let self_ref = self.actor_ref.clone();

        let subscription = tokio::spawn(async move {
            while let Some(event) = event_stream.next().await {
                self_ref.send_message(PeerManagerMsg::PeerEventReceived(event));
            }
            // Stream ended - peer disconnected
            self_ref.send_message(PeerManagerMsg::PeerDisconnected { server_id });
        });

        self.peers.insert(server_id, PeerConnection {
            server_id,
            client,
            event_subscription: Some(subscription),
        });

        tracing::info!("Connected to peer: {}", server_id);
    }

    async fn handle_peer_disconnected(&mut self, server_id: Uuid) {
        if let Some(peer) = self.peers.remove(&server_id) {
            if let Some(sub) = peer.event_subscription {
                sub.abort();
            }
            // Publish DEL for the peer's Server entity
            self.event_manager.send(PublishDel {
                item: Server { id: server_id.to_string().into(), ..Default::default() },
                tx: Uuid::new_v4(),
            }).await;
        }
        tracing::info!("Peer disconnected: {}", server_id);
    }
}
```

#### Federation Wrappers

```rust
/// Route query to specific peer
#[myko_command]
pub struct PeerQuery {
    pub query: Box<dyn Query>,
    pub peer_id: Uuid,
}

/// Route command to specific peer
#[myko_command]
pub struct PeerCommand {
    pub command: Box<dyn Command>,
    pub peer_id: Uuid,
}

/// Route report to specific peer
#[myko_command]
pub struct PeerReport {
    pub report: Box<dyn Report>,
    pub peer_id: Uuid,
}
```

#### Federation Handlers

```rust
#[myko_query_handler(PeerQuery)]
async fn handle_peer_query(
    query: PeerQuery,
    ctx: &QueryContext,
    peer_manager: ActorRef<PeerManagerMsg>,
) -> QueryStream {
    // Local query - execute directly
    if query.peer_id == ctx.host_id() {
        return ctx.query(query.query).await;
    }

    // Forward to peer
    peer_manager.call(ForwardQuery {
        peer_id: query.peer_id,
        query: query.query,
    }).await.unwrap_or_else(|_| QueryStream::empty())
}

#[myko_command_handler(PeerCommand)]
async fn handle_peer_command(
    command: PeerCommand,
    ctx: &CommandContext,
    peer_manager: ActorRef<PeerManagerMsg>,
) -> Result<()> {
    let peer = peer_manager.call(GetPeer { id: command.peer_id }).await?;

    match peer {
        Some(peer) => {
            // Verify requesting client is local before forwarding
            let local_clients = ctx.query(GetClientsByQuery {
                partial: Client { server_id: Some(ctx.host_id().to_string().into()), ..Default::default() }
            }).await?;

            if !local_clients.iter().any(|c| c.id == ctx.client_id().unwrap_or_default()) {
                bail!("Only local clients can forward commands to peers");
            }

            peer.client.send_command(command.command).await
        }
        None => bail!("Peer not found: {}", command.peer_id),
    }
}

#[myko_report_handler(PeerReport)]
async fn handle_peer_report(
    report: PeerReport,
    ctx: &ReportContext,
    peer_manager: ActorRef<PeerManagerMsg>,
) -> ReportStream {
    // Local report - execute directly
    if report.peer_id == ctx.host_id() {
        return ctx.report(report.report).await;
    }

    // Forward to peer (returns empty if peer not connected)
    peer_manager.call(ForwardReport {
        peer_id: report.peer_id,
        report: report.report,
    }).await.unwrap_or_else(|_| ReportStream::empty())
}
```

#### Event Replication

```rust
impl PeerManager {
    async fn handle_peer_event(&mut self, event: MEvent, ctx: &RequestContext) {
        // Forward peer events to local event bus
        // These events originated on another server but should update local state
        match event.change_type {
            ChangeType::Set => {
                self.event_manager.send(PublishSet {
                    item: event.item,
                    context: ctx.clone(),
                    options: EventOptions { from_peer: true, ..Default::default() },
                }).await;
            }
            ChangeType::Del => {
                self.event_manager.send(PublishDel {
                    item: event.item,
                    context: ctx.clone(),
                    options: EventOptions { from_peer: true, ..Default::default() },
                }).await;
            }
        }
    }
}
```

#### ServerEventLog Report Handler

```rust
#[myko_report_handler(ServerEventLog)]
fn handle_server_event_log(
    _report: ServerEventLog,
    ctx: &ReportContext,
    event_manager: ActorRef<EventManagerMsg>,
) -> impl Stream<Item = MEvent> {
    // Stream events that originated from this server
    event_manager.subscribe()
        .filter(move |e| e.source_id == ctx.host_id())
}
```

#### Peer Query Option for Repositories

```rust
pub struct RepoConfig<T> {
    /// Optional function to query peers for missing items
    pub peer_query: Option<fn(&[Arc<str>]) -> BoxFuture<'static, Vec<T>>>,
}

impl<T: Item> Repository<T> {
    /// Get item by ID, falling back to peer query if not found locally
    pub async fn get_with_fallback(&self, id: &str) -> Option<T> {
        // Try local first
        if let Some(item) = self.get_id(id).await {
            return Some(item);
        }

        // Try peer query if configured
        if let Some(peer_query) = &self.config.peer_query {
            let results = peer_query(&[Arc::from(id)]).await;
            return results.into_iter().next();
        }

        None
    }
}
```

#### Design Decisions

| Decision | Rationale |
|----------|-----------|
| Entity-based discovery | No external service discovery needed; servers self-announce via Server entities |
| PeerManager actor | Centralizes peer connection lifecycle; easy to query peer state |
| ServerEventLog streaming | Real-time event replication without polling |
| Address deduplication | Prevents multiple connections to same server during startup race |
| Peer token auth | Cluster traffic uses shared secret, not JWT |
| Delete stale on startup | Handles server restarts with same address:port |
| from_peer event option | Prevents cascade loops; relationship handlers skip peer events |

---

## Performance-Critical Implementation Decisions

**Target**: Hundreds of clients, thousands of messages/second per client. Show-critical, zero-tolerance for dropped messages or latency spikes.

### 1. Serialization Strategy

**Problem**: JSON serialization is 5-10x slower than binary formats.

**Decision**: MessagePack everywhere, with pre-serialized caching.

```rust
// Message types pre-serialize to bytes once, reuse for all clients
pub struct CachedMessage {
    /// Pre-serialized MessagePack bytes
    bytes: Arc<[u8]>,
}

impl CachedMessage {
    pub fn new<T: Serialize>(msg: &T) -> Self {
        let bytes = rmp_serde::to_vec_named(msg).unwrap();
        Self { bytes: bytes.into() }
    }
}

// WebSocket send is zero-copy
impl WebSocketConnection {
    fn send(&mut self, msg: &CachedMessage) {
        self.tx.send(Message::Binary(msg.bytes.clone())); // Arc clone only
    }
}
```

**Rationale**: When broadcasting to 100 clients, serialize once, send 100 times. Arc<[u8]> clone is 2 atomic ops vs full serialization.

### 2. Event Fan-Out Architecture

**Problem**: Current design routes all messages through Server actor, creating a bottleneck.

**Decision**: Direct actor references with sharded broadcast.

```rust
// Sharded broadcast channels (one per CPU core)
pub struct ShardedBroadcast<T> {
    shards: Vec<broadcast::Sender<T>>,
}

impl<T: Clone> ShardedBroadcast<T> {
    pub fn new(shard_count: usize, capacity: usize) -> Self {
        Self {
            shards: (0..shard_count)
                .map(|_| broadcast::channel(capacity).0)
                .collect(),
        }
    }

    pub fn subscribe(&self, shard: usize) -> broadcast::Receiver<T> {
        self.shards[shard % self.shards.len()].subscribe()
    }

    pub fn send(&self, msg: T) {
        for shard in &self.shards {
            let _ = shard.send(msg.clone());
        }
    }
}

// Event distribution: EventManager → ShardedBroadcast → QueryHandlers
// No single-actor bottleneck
```

**Rationale**: Ractor actors are single-threaded. Sharding distributes load across cores.

### 3. Query Runner Optimization

**Problem**: `MutableBTreeMap::lock_mut()` on every update creates contention.

**Decision**: Lock-free concurrent map with batch updates.

```rust
use dashmap::DashMap;

pub struct QueryRunnerState {
    /// Lock-free concurrent map
    items: Arc<DashMap<Arc<str>, Arc<dyn AnyItem>, ahash::RandomState>>,

    /// Signal for subscribers (debounced)
    signal: Arc<Notify>,
}

impl QueryRunner {
    fn process_batch(&self, updates: Vec<ProcessUpdateData>) {
        // Batch updates without per-item locking
        for update in updates {
            match update {
                ProcessUpdateData::Set(item) => {
                    if self.matches(&item) {
                        self.items.insert(item.id(), item);
                    } else {
                        self.items.remove(&item.id());
                    }
                }
                ProcessUpdateData::Del(id) => {
                    self.items.remove(&id);
                }
            }
        }
        // Single notification after batch
        self.signal.notify_waiters();
    }
}
```

**Rationale**: DashMap uses sharded locking internally. Batching reduces notification overhead.

### 4. Zero-Copy Item References

**Problem**: `Arc<dyn AnyItem>` requires vtable lookup and prevents inlining.

**Decision**: Type-erased but cache-friendly item storage.

```rust
/// Compact item representation for hot paths
#[repr(C)]
pub struct ItemSlot {
    /// Item ID (inline for fast comparison)
    id: [u8; 36],  // UUID as bytes, no allocation
    /// Item hash (inline for change detection)
    hash: [u8; 16], // MD5 as bytes
    /// Type discriminant
    type_id: u16,
    /// Serialized item data
    data: Arc<[u8]>,
}

impl ItemSlot {
    #[inline]
    pub fn id_matches(&self, id: &[u8]) -> bool {
        self.id == id
    }

    #[inline]
    pub fn hash_matches(&self, hash: &[u8]) -> bool {
        self.hash == hash
    }
}
```

**Rationale**: Inline IDs/hashes enable SIMD comparison. No vtable indirection on hot paths.

### 5. WebSocket Batching

**Problem**: Per-message WebSocket frames have ~14 byte overhead + syscall per send.

**Decision**: Accumulate and flush with configurable interval.

```rust
pub struct BatchingWebSocket {
    pending: Vec<CachedMessage>,
    flush_interval: Duration,
    max_batch_size: usize,
}

impl BatchingWebSocket {
    async fn run(&mut self) {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    self.pending.push(msg);
                    if self.pending.len() >= self.max_batch_size {
                        self.flush().await;
                    }
                }
                _ = interval.tick() => {
                    if !self.pending.is_empty() {
                        self.flush().await;
                    }
                }
            }
        }
    }

    async fn flush(&mut self) {
        // Combine all pending messages into single frame
        let batch = BatchMessage { items: std::mem::take(&mut self.pending) };
        let bytes = rmp_serde::to_vec_named(&batch).unwrap();
        self.ws.send(Message::Binary(bytes)).await;
    }
}
```

**Config**: `flush_interval: 8ms` (120fps compatible), `max_batch_size: 64`.

### 6. Channel Sizing and Backpressure

**Problem**: Unbounded channels cause memory exhaustion; small bounded channels drop messages.

**Decision**: Tiered channel strategy with explicit backpressure.

```rust
/// Channel sizing based on actor role
pub struct ChannelConfig {
    /// High-frequency paths (events, queries)
    pub hot_path: usize,      // 16384 (power of 2 for ring buffer efficiency)

    /// Medium-frequency (commands, reports)
    pub warm_path: usize,     // 4096

    /// Low-frequency (admin, config)
    pub cold_path: usize,     // 256
}

/// Backpressure-aware send
pub async fn send_with_backpressure<T>(
    tx: &mpsc::Sender<T>,
    msg: T,
    timeout: Duration,
) -> Result<(), BackpressureError> {
    match tokio::time::timeout(timeout, tx.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(BackpressureError::ChannelClosed),
        Err(_) => Err(BackpressureError::Timeout),
    }
}

// Monitor channel fill levels
pub fn channel_pressure(tx: &mpsc::Sender<T>) -> f32 {
    let capacity = tx.capacity();
    let available = tx.max_capacity() - capacity;
    available as f32 / tx.max_capacity() as f32
}
```

**Rationale**: Different paths have different throughput needs. Explicit backpressure prevents silent drops.

### 7. Memory Allocation Strategy

**Problem**: Frequent small allocations fragment memory and stress the allocator.

**Decision**: Arena allocators for per-request data, object pools for hot types.

```rust
// Use jemalloc for better multi-threaded performance
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Object pool for frequently allocated types
pub struct ItemPool {
    pool: crossbeam::queue::ArrayQueue<Box<ItemSlot>>,
}

impl ItemPool {
    pub fn acquire(&self) -> PooledItem {
        let item = self.pool.pop().unwrap_or_else(|| Box::new(ItemSlot::default()));
        PooledItem { item, pool: self }
    }
}

impl Drop for PooledItem {
    fn drop(&mut self) {
        // Return to pool instead of deallocating
        let _ = self.pool.pool.push(std::mem::take(&mut self.item));
    }
}
```

**Rationale**: jemalloc handles multi-threaded allocation better than system allocator. Pools eliminate allocation on hot paths.

### 8. Hash Function Selection

**Problem**: MD5 for content hashing is slow (crypto-grade).

**Decision**: Keep MD5 for compatibility, but use fast hashes internally.

```rust
use ahash::AHasher;  // Hardware-accelerated, non-cryptographic

/// Fast hash for internal lookups (not persisted)
#[inline]
pub fn fast_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = AHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}

/// MD5 only for content hashing (must match TypeScript)
pub fn content_hash(item: &impl Serialize) -> [u8; 16] {
    let json = serde_json::to_string(item).unwrap();
    md5::compute(json.as_bytes()).0
}

// Use ahash for all internal HashMaps
pub type FastHashMap<K, V> = HashMap<K, V, ahash::RandomState>;
pub type FastHashSet<K> = HashSet<K, ahash::RandomState>;
```

**Rationale**: ahash is 5-10x faster than SipHash. MD5 only used where cross-language compatibility matters.

### 9. Query Matching Optimization

**Problem**: Per-item closure evaluation is expensive for complex queries.

**Decision**: Compile queries to optimized matchers at registration time.

```rust
/// Compiled query matcher - no dynamic dispatch on hot path
pub enum CompiledMatcher {
    /// Match all items
    All,
    /// Match by single field equality
    FieldEq { field_offset: usize, value: Arc<[u8]> },
    /// Match by ID set (for watchIds)
    IdSet(Arc<FastHashSet<Arc<str>>>),
    /// Complex predicate (fallback)
    Predicate(Arc<dyn Fn(&ItemSlot) -> bool + Send + Sync>),
}

impl CompiledMatcher {
    #[inline]
    pub fn matches(&self, item: &ItemSlot) -> bool {
        match self {
            Self::All => true,
            Self::FieldEq { field_offset, value } => {
                // Direct byte comparison, no deserialization
                &item.data[*field_offset..][..value.len()] == &**value
            }
            Self::IdSet(set) => set.contains(item.id_str()),
            Self::Predicate(f) => f(item),
        }
    }
}
```

**Rationale**: Most queries are simple field matches. Avoid deserializing entire item just to check one field.

### 10. Saga Event Filtering

**Problem**: Broadcasting all events to all sagas wastes CPU.

**Decision**: Pre-filter by item type at registration.

```rust
pub struct SagaRegistry {
    /// Sagas indexed by item types they care about
    by_item_type: FastHashMap<&'static str, Vec<Arc<dyn Saga>>>,
    /// Sagas that want all events (rare)
    all_events: Vec<Arc<dyn Saga>>,
}

impl SagaRegistry {
    pub fn dispatch(&self, event: &MEvent) {
        // Only notify sagas interested in this item type
        if let Some(sagas) = self.by_item_type.get(event.item_type()) {
            for saga in sagas {
                saga.process(event);
            }
        }
        for saga in &self.all_events {
            saga.process(event);
        }
    }
}
```

**Rationale**: Most sagas only care about 1-2 entity types. Pre-filtering eliminates 90%+ of saga invocations.

### 11. Relationship Cascade Batching

**Problem**: Cascade deletes can generate hundreds of events.

**Decision**: Batch cascade operations with single transaction.

```rust
impl RelationshipManager {
    async fn process_cascade(&mut self, trigger_event: &MEvent) {
        let mut batch = EventBatch::new(trigger_event.tx.clone());

        // Collect all cascade events
        self.collect_cascades(trigger_event, &mut batch).await;

        // Publish as single batch (one Kafka transaction)
        if !batch.is_empty() {
            self.event_manager.send(PublishBatch(batch)).await;
        }
    }

    async fn collect_cascades(&self, event: &MEvent, batch: &mut EventBatch) {
        for relation in self.relations.iter() {
            match relation.cascade_for(event) {
                Some(CascadeAction::DeleteChildren(query)) => {
                    let children = self.query_internal(&query).await;
                    for child in children {
                        batch.add_del(child);
                        // Recursive cascades
                        self.collect_cascades(&child.to_del_event(), batch).await;
                    }
                }
                // ... other cascade types
                None => {}
            }
        }
    }
}
```

**Rationale**: Single transaction prevents partial cascade states. Batching reduces Kafka overhead.

### 12. Snapshot Storage Efficiency

**Problem**: Full item serialization for snapshots is wasteful.

**Decision**: Delta compression from previous snapshot.

```rust
pub struct SnapshotData {
    /// Base snapshot (for delta chain)
    pub base_id: Option<Arc<str>>,

    /// If base_id is set, only stores changes from base
    pub delta: Option<SnapshotDelta>,

    /// Full items (if no base or for periodic full snapshots)
    pub full_items: Option<Vec<CompressedItem>>,
}

pub struct SnapshotDelta {
    pub added: Vec<CompressedItem>,
    pub modified: Vec<(Arc<str>, CompressedItem)>,  // (id, new_value)
    pub removed: Vec<Arc<str>>,  // Just IDs
}

/// LZ4 compressed item data
pub struct CompressedItem {
    pub id: Arc<str>,
    pub data: Arc<[u8]>,  // LZ4 compressed MessagePack
}
```

**Rationale**: Consecutive snapshots often differ by <5%. Delta + LZ4 reduces storage 10-50x.

### 13. Search Index Updates

**Problem**: Per-event tantivy commits are expensive.

**Decision**: Micro-batched commits with debouncing.

```rust
impl SearchManager {
    async fn run(&mut self) {
        let mut pending = Vec::with_capacity(256);
        let mut last_commit = Instant::now();

        loop {
            // Collect events for up to 10ms or 256 items
            let deadline = tokio::time::sleep(Duration::from_millis(10));
            tokio::pin!(deadline);

            loop {
                tokio::select! {
                    event = self.event_rx.recv() => {
                        pending.push(event);
                        if pending.len() >= 256 {
                            break;
                        }
                    }
                    _ = &mut deadline => break,
                }
            }

            if !pending.is_empty() {
                self.batch_index(&pending);
                pending.clear();

                // Commit every 50ms minimum
                if last_commit.elapsed() > Duration::from_millis(50) {
                    self.commit();
                    last_commit = Instant::now();
                }
            }
        }
    }
}
```

**Rationale**: tantivy commit is expensive (~1ms). Batching amortizes cost across many updates.

### 14. Authentication Token Caching

**Problem**: JWT validation is CPU-intensive (crypto operations).

**Decision**: LRU cache for validated tokens.

```rust
use quick_cache::sync::Cache;

pub struct CachedAuthService {
    inner: Arc<dyn AuthService>,
    /// Cache: token_hash → (is_valid, user_id, expires_at)
    cache: Cache<u64, (bool, Option<Arc<str>>, Instant)>,
}

impl CachedAuthService {
    pub async fn validate(&self, token: &str) -> Result<bool> {
        let hash = fast_hash(&token);

        // Check cache first
        if let Some((valid, _, expires)) = self.cache.get(&hash) {
            if Instant::now() < expires {
                return Ok(valid);
            }
        }

        // Validate and cache
        let valid = self.inner.validate(token).await?;
        let user_id = if valid { self.inner.get_user_id(token).await? } else { None };

        // Cache for 5 minutes (less than JWT expiry)
        self.cache.insert(hash, (valid, user_id, Instant::now() + Duration::from_secs(300)));

        Ok(valid)
    }
}
```

**Rationale**: Same token used repeatedly for WebSocket messages. Cache hit is nanoseconds vs milliseconds for crypto.

### 15. Peer Event Deduplication

**Problem**: Network partitions can cause duplicate events from peers.

**Decision**: Bloom filter for recent event IDs.

```rust
use growable_bloom_filter::GrowableBloom;

pub struct EventDeduplicator {
    /// Bloom filter for O(1) duplicate check
    seen: GrowableBloom,
    /// Exact set for false-positive verification (bounded)
    exact: FastHashSet<(Arc<str>, Arc<str>)>,  // (item_id, tx)
    /// Cleanup threshold
    max_exact: usize,
}

impl EventDeduplicator {
    pub fn is_duplicate(&mut self, event: &MEvent) -> bool {
        let key = (event.item_id().clone(), event.tx.clone());
        let hash = fast_hash(&key);

        // Fast path: definitely not seen
        if !self.seen.contains(&hash) {
            self.seen.insert(&hash);
            self.exact.insert(key);
            self.maybe_cleanup();
            return false;
        }

        // Bloom filter positive - verify with exact set
        if self.exact.contains(&key) {
            return true;  // Actual duplicate
        }

        // False positive from bloom filter
        self.exact.insert(key);
        false
    }
}
```

**Rationale**: Bloom filters are space-efficient for high-cardinality duplicate detection. False positives are rare and verified.

---

## Performance Benchmarks (Targets)

| Metric | Target | Priority | Notes |
|--------|--------|----------|-------|
| WebSocket latency | <5ms p99 | **CRITICAL** | End-to-end message round trip, show-critical |
| Event throughput | 100,000/sec | High | Per server, all entity types combined |
| Query update latency | <1ms p99 | High | From event to subscriber notification |
| Snapshot create | <100ms | Medium | For 10,000 entities |
| Search query | <10ms | Medium | For 100,000 indexed items |
| Memory per client | Unconstrained | Low | Optimize for latency, not memory |

## Crate Selection

| Purpose | Crate | Rationale |
|---------|-------|-----------|
| Allocator | tikv-jemallocator | Best multi-threaded performance |
| HashMap | dashmap + ahash | Lock-free concurrent + fast hashing |
| Channels | crossbeam-channel | Faster than tokio::sync for hot paths |
| Serialization | rmp-serde | MessagePack, 3-5x faster than JSON |
| Compression | lz4_flex | Fastest compression, good ratio |
| Bloom filter | growable-bloom-filter | Space-efficient deduplication |
| LRU cache | quick-cache | Concurrent, low-latency |
| Search | tantivy | Rust-native full-text search |

## Actor Structure Refactoring

### Current Architecture Problems

```
┌─────────────────────────────────────────────────────────────────┐
│                        Server Actor                             │
│  (BOTTLENECK: all messages route through single-threaded actor) │
└───────────────────────────┬─────────────────────────────────────┘
                            │
    ┌───────────┬───────────┼───────────┬───────────┬─────────────┐
    ▼           ▼           ▼           ▼           ▼             ▼
 EventMgr   QueryMgr   ReportMgr   CommandMgr   SagaMgr    WebSocketServer
    │           │
    ▼           ▼
 EventHandler  QueryHandler ──► QueryRunner (per subscription)
    │
    └──► Server ──► QueryManager  (INDIRECT: extra hop through Server)
```

**Identified Bottlenecks:**

| Problem | Location | Impact |
|---------|----------|--------|
| Server as message router | `ServerMsg` enum | All traffic through 1 actor |
| Indirect event→query routing | `EventHandler` → `Server` → `QueryManager` | +1 message hop per event |
| Clone per handler | `update_data.clone()` in QueryManager loop | N allocations per event |
| JSON on WS receive thread | `MessageHandler::ProcessText` | Blocks socket reads |
| No update batching | `QueryRunnerMsg::ProcessUpdate` | Per-item actor messages |
| HashMap lookup per update | `state.handlers.get(&item_type)` | Cache misses on hot path |

### Target Architecture

```
                    ┌─────────────────────┐
                    │  Broadcast Channel  │ (sharded, lock-free)
                    │   (events only)     │
                    └──────────┬──────────┘
                               │
       ┌───────────────────────┼───────────────────────┐
       ▼                       ▼                       ▼
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│ QueryShard0 │         │ QueryShard1 │         │ QueryShardN │
│ (handlers)  │         │ (handlers)  │         │ (handlers)  │
└─────────────┘         └─────────────┘         └─────────────┘

WebSocket ──► ParsePool ──► EventManager ──► broadcast
                                   │
                                   ▼
                              KafkaProducer (async, no wait)
```

### Required Changes

#### 1. Eliminate Server Actor as Router

**Current**: All managers reference `server: ActorRef<ServerMsg>` and route through it.

**Change**: Direct actor references between managers.

```rust
// BEFORE: EventHandler routes through Server
state.server.send_message(ServerMsg::QueryManagerMsg(
    QueryManagerMsg::ProcessUpdate(data, entity_name)
));

// AFTER: EventHandler has direct QueryManager reference
state.query_manager.send_message(
    QueryManagerMsg::ProcessUpdate(data, entity_name)
);
```

**Files to modify:**
- `actors/server.rs` - Remove routing logic, keep only lifecycle
- `actors/event/event_handler.rs` - Add `query_manager: ActorRef<QueryManagerMsg>`
- `actors/event/event_manager.rs` - Pass query_manager to EventHandler
- Remove `ServerMsg::QueryManagerMsg`, `ServerMsg::ReportManagerMsg`, etc.

#### 2. Sharded Event Broadcast

**Current**: EventManager sends to SagaManager, then EventHandler sends to Server→QueryManager.

**Change**: Single broadcast channel that all consumers subscribe to.

```rust
pub struct EventBroadcast {
    /// One sender per CPU core
    shards: Vec<broadcast::Sender<Arc<EventBatch>>>,
}

pub struct EventBatch {
    events: Vec<MEvent>,
    /// Pre-serialized for WebSocket send
    serialized: Arc<[u8]>,
}

// EventManager publishes once
impl EventManager {
    fn handle_event(&self, event: MEvent) {
        let batch = self.batcher.add(event);
        if let Some(batch) = batch {
            self.broadcast.send(Arc::new(batch));
        }
    }
}

// QueryManager subscribes
impl QueryManager {
    async fn run(&mut self) {
        let mut rx = self.broadcast.subscribe(self.shard_id);
        while let Ok(batch) = rx.recv().await {
            for event in &batch.events {
                self.dispatch_to_handlers(event);
            }
        }
    }
}
```

#### 3. Replace QueryRunner Actor with Shared State

**Current**: One `QueryRunner` actor per active subscription, each receiving individual updates.

**Change**: Shared `DashMap` per QueryHandler, runners read from it.

```rust
pub struct QueryHandler {
    /// Shared state - lock-free concurrent map
    items: Arc<DashMap<Arc<str>, Arc<dyn AnyItem>, ahash::RandomState>>,

    /// Subscribers get notified via broadcast
    notify: broadcast::Sender<()>,
}

// No more per-runner actors for simple queries
// Complex queries (with transforms) still get dedicated runners

impl QueryHandler {
    fn process_update(&self, update: ProcessUpdateData) {
        match update {
            ProcessUpdateData::Set(item) => {
                if self.matches(&item) {
                    self.items.insert(item.id(), item);
                } else {
                    self.items.remove(&item.id());
                }
            }
            ProcessUpdateData::Del(id) => {
                self.items.remove(&id);
            }
        }
        // Notify all subscribers (debounced externally)
        let _ = self.notify.send(());
    }
}
```

#### 4. Async JSON Parsing Pool

**Current**: `MessageHandler` parses JSON synchronously in actor.

**Change**: Dedicated parsing thread pool, actor just dispatches.

```rust
pub struct ParsePool {
    pool: rayon::ThreadPool,
    tx: mpsc::Sender<ParsedMessage>,
}

impl MessageHandler {
    fn handle(&mut self, msg: MessageHandlerMsg) {
        match msg {
            MessageHandlerMsg::ProcessText(data) => {
                // Don't parse here - send to pool
                let tx = self.parsed_tx.clone();
                self.parse_pool.spawn(move || {
                    let parsed = serde_json::from_str(&data.text);
                    tx.send((data.client_id, parsed));
                });
            }
            MessageHandlerMsg::ProcessParsed(client_id, msg) => {
                // Route already-parsed message
                self.route(client_id, msg);
            }
        }
    }
}
```

#### 5. Batch Updates to QueryHandlers

**Current**: Each update is a separate `QueryHandlerMsg::ProcessUpdate`.

**Change**: Batch updates, single message.

```rust
pub enum QueryHandlerMsg {
    // REMOVE: ProcessUpdate(ProcessUpdateData),

    // ADD: Batched updates
    ProcessBatch(Vec<ProcessUpdateData>),
}

impl QueryManager {
    fn dispatch_batch(&self, batch: &[MEvent]) {
        // Group by item_type
        let mut by_type: FastHashMap<&str, Vec<ProcessUpdateData>> = FastHashMap::default();
        for event in batch {
            by_type.entry(event.item_type())
                .or_default()
                .push(event.into());
        }

        // Single message per handler
        for (item_type, updates) in by_type {
            if let Some(handlers) = self.handlers.get(item_type) {
                for handler in handlers.values() {
                    handler.send_message(QueryHandlerMsg::ProcessBatch(updates.clone()));
                }
            }
        }
    }
}
```

#### 6. Pre-Index Handlers by Item Type

**Current**: `HashMap<Arc<str>, HashMap<...>>` lookup on every update.

**Change**: Pre-built flat index with direct references.

```rust
pub struct HandlerIndex {
    /// Flat array of (item_type_hash, handler_ref) sorted by hash
    /// Binary search is faster than HashMap for small N
    entries: Vec<(u64, Vec<ActorRef<QueryHandlerMsg>>)>,
}

impl HandlerIndex {
    #[inline]
    fn get(&self, item_type: &str) -> Option<&[ActorRef<QueryHandlerMsg>]> {
        let hash = fast_hash(item_type);
        self.entries
            .binary_search_by_key(&hash, |(h, _)| *h)
            .ok()
            .map(|i| self.entries[i].1.as_slice())
    }
}
```

### Migration Plan

| Phase | Changes | Risk |
|-------|---------|------|
| 1 | Direct actor refs (remove Server routing) | Low - internal refactor |
| 2 | Batch updates to handlers | Low - additive |
| 3 | Sharded broadcast channel | Medium - new data flow |
| 4 | Replace QueryRunner with shared DashMap | Medium - API change |
| 5 | Async parse pool | Low - internal optimization |

### New Actor Hierarchy

```rust
// Simplified - fewer actors, more shared state

pub struct MykoServer {
    // Core managers (long-lived)
    event_manager: ActorRef<EventManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
    command_manager: ActorRef<CommandManagerMsg>,
    saga_manager: ActorRef<SagaManagerMsg>,

    // Shared infrastructure (not actors)
    event_broadcast: Arc<EventBroadcast>,
    parse_pool: Arc<ParsePool>,
    ws_server: Arc<WebSocketServer>,  // Now just manages connections
}

// EventHandler is no longer an actor - just a struct with DashMap
pub struct EventStore {
    items: DashMap<Arc<str>, ItemSlot, ahash::RandomState>,
    kafka_tx: Option<mpsc::Sender<ProduceEventData>>,
}

// QueryHandler manages shared state, not per-query actors
pub struct QueryHandler {
    items: Arc<DashMap<...>>,
    subscribers: broadcast::Sender<()>,
    matcher: CompiledMatcher,
}
```

### Performance Impact

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Event routing hops | 3 (EH→S→QM→QH) | 1 (broadcast) | 3x fewer messages |
| Updates per event | N (one per handler) | 1 (batched) | N× fewer messages |
| Locks per update | 2 (BTreeMap) | 0 (DashMap) | Lock-free hot path |
| Parse blocking | Yes (actor) | No (thread pool) | Parallel parsing |
| Clone per handler | Yes | No (Arc batch) | Zero-copy fan-out |
