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
- `QueryManager` / `QueryHandler` / `QueryRunner` - Reactive query execution
- `ReportManager` / `ReportRunner` - Computed report handling
- `CommandManager` - Command routing and execution
- `WebSocketServer` / `WebSocketConnection` - Client connections
- `KafkaProducer` / `KafkaConsumer` - Optional Kafka integration
- `SagaManager` / `SagaRunner` - Event stream processors
- `RelationshipManager` - Cascade operations (belongs-to, owns-many, ensure-for)
- `PeerManager` - Peer discovery and federation

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
- Servers publish their own `Server` entity on startup
- Optional: Docker DNS seeding via `tasks.<MYKO_SERVICE_NAME>` can bootstrap initial `Server` entities

**Peer Connection** (`PeerClientRegistry`):
1. For each discovered server, create `WSMClient` connection
2. On connect, verify server ID matches expected via `GetConnectedServer` query
3. On disconnect, clean up and delete peer's `Server` entity

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
```

**Key Queries**:
```
GetConnectedServer -> Server[]  // Returns this server's entity
GetPeerServers -> Server[]      // Returns all other servers in cluster
GetServers -> Server[]          // Returns all known servers
GetServersByClientIds { clientIds[] } -> Server[]  // Servers hosting specific clients
```

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
- **Relationship system**: `#[belongs_to(Type)]`, `#[owns_many(Type)]`, `#[ensure_for(Type)]` field attributes
  - BelongsTo cascades: Parent DEL → cascade delete children
  - OwnsMany cascades: Parent DEL → delete children, Child DEL → update parent arrays
  - EnsureFor: Auto-create entities for each dependency combination
  - Orphan cleanup on startup for both BelongsTo and OwnsMany
- **Saga pattern**: Stateful stream processors for reactive event processing
  - SagaManager and SagaRunner actors
  - EventBus for high-throughput lock-free event distribution
  - Stream operators: `of_item_type()`, `of_change_type()`, `pairwise()`, `scan()`
  - SagaContext with command execution access
  - Inventory-based saga registration
- **Peer discovery and federation**: Multi-server clustering
  - PeerManager actor with entity-based discovery via GetPeerServers
  - ForwardQuery/ForwardCommand/ForwardReport for cross-server proxying
  - Peer health tracking (latency, last_seen)
  - Automatic connection/reconnection to discovered peers

**Not Yet Implemented**:
- Full-text search (tantivy integration)
- Windback/snapshots
- Authentication (Auth0/JWT validation)
- RebalanceItem command
- Most rship domain entities (need to be defined with `#[myko_item]`)

### Migration Checklist for Rust

**Core Framework** (in @myko/rs):
1. ✅ Item Registration via `inventory` + `#[myko_item]`
2. ✅ Hash Computation (MD5 of serialized content)
3. ✅ Bus Implementation (actor-based with ractor)
4. ✅ Query/Report delta protocol
5. ✅ WebSocket server with MessagePack
6. ✅ Kafka integration (optional)
7. ✅ Relationship Cascades (belongs-to, owns-many, ensure-for)
8. ✅ Saga pattern (stateful stream processors with EventBus)
9. ✅ Peer Discovery (entity-based via GetPeerServers)
10. ✅ Federation Handlers (ForwardQuery/ForwardCommand/ForwardReport)
11. 📐 Full-text Search integration (tantivy) - DESIGNED
12. 📐 Context propagation (tx, clientId, lineage, hostId) - DESIGNED
13. 📐 Authentication (async-oidc-jwt-validator + peerSecret) - DESIGNED
14. 📐 Windback/Snapshots (version-control approach) - DESIGNED

**Application Layer** (separate from framework, e.g. rship):
- Define domain entities using `#[myko_item]`
- Implement custom query/report/command handlers
- Application-specific logic (scene engine, binding execution, etc.)

### Relationship System (Rust) - ✅ IMPLEMENTED

See `libs/myko/rs/src/actors/relationship/relationship_manager.rs` for implementation.

**Usage:**
```rust
#[myko_item]
pub struct Binding {
    #[belongs_to(Scene)]      // Parent DEL → cascade delete this child
    pub scope_id: Arc<str>,
}

#[myko_item]
pub struct Scene {
    #[owns_many(BindingNode)]  // Parent DEL → delete children; Child DEL → update array
    pub node_ids: Vec<Arc<str>>,
}

#[myko_item]
pub struct SessionVariable {
    #[ensure_for(Project)]  // Auto-create one per Project×Session combination
    pub project_id: Arc<str>,
    #[ensure_for(Session)]
    pub session_id: Arc<str>,
    #[default_value("unnamed")]
    pub name: String,
}
```

**Behavior:**
- BelongsTo: Parent DEL → cascade delete all children with matching FK
- OwnsMany: Parent DEL → delete children; Child DEL → remove from parent array
- EnsureFor: Dependency SET → create entity for each combination (Cartesian product)
- Orphan cleanup on startup for both BelongsTo and OwnsMany
- Events share transaction ID for traceability

### Client ID Auto-Population (Rust) - ✅ IMPLEMENTED

The `#[myko_client_id]` field attribute auto-populates a field with the WebSocket client ID when events are processed.

**Usage:**
```rust
#[myko_item]
pub struct Instance {
    #[myko_client_id]
    pub client_id: Option<String>,  // Auto-populated with WebSocket client ID
}
```

**Behavior:**
- Field is populated during event processing in EventManager
- Only set if field is null/missing (allows explicit override)
- Uses camelCase field name in JSON (e.g., `clientId`)
- Useful for tracking which client created an entity

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

### Saga Pattern (Rust) - ✅ IMPLEMENTED

See `libs/myko/rs/src/saga/` and `libs/myko/rs/src/actors/saga/` for implementation.

Sagas are stateful stream processors that react to events and emit commands on state transitions.

**Usage:**
```rust
impl Saga for MyTransitionSaga {
    fn build(events: EventStream, _ctx: Arc<SagaContext>) -> CommandStream {
        Box::pin(events
            .of_item_type("Scene")
            .of_change_type(MEventType::SET)
            .pairwise()  // Compare prev/current for transitions
            .filter_map(|(prev, curr)| async move {
                if prev.item["status"] != curr.item["status"] {
                    Some(NotifyStatusChange { ... }.into())
                } else {
                    None
                }
            }))
    }
}
```

**Stream Operators:**
| Operator | Purpose |
|----------|---------|
| `of_item_type(name)` | Filter by item type |
| `of_change_type(SET/DEL)` | Filter by change type |
| `pairwise()` | Compare prev/current for state transitions |
| `scan(initial, f)` | Accumulate state across events |

**Architecture:**
- SagaManager discovers sagas via `inventory` and spawns SagaRunner actors
- SagaRunners subscribe to EventBus for high-throughput event reception
- Commands emitted by sagas are sent to CommandManager

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

### Peer Discovery & Federation (Rust) - ✅ IMPLEMENTED

See `libs/myko/rs/src/actors/peer/peer_manager.rs` for implementation.

Multi-server clustering where servers discover each other and proxy operations across the cluster.

**Key Components:**
- `Server` entity: Self-announces each server (id, version, address, port, started_at)
- `PeerManager` actor: Manages peer connections, discovery via `GetPeerServers` query subscription
- `ForwardQuery/ForwardCommand/ForwardReport`: Proxy operations to peer servers

**Behavior:**
- On startup: Clean up stale Server records at same address:port, publish own Server entity
- Discovery: Subscribe to `GetPeerServers` query, connect to discovered servers
- Deduplication: `connecting: HashSet<Uuid>` prevents duplicate connection attempts (keyed by server ID)
- On disconnect: Delete peer's Server entity

**Design Decisions:**
| Decision | Rationale |
|----------|-----------|
| Entity-based discovery | No external service discovery needed; servers self-announce via Server entities |
| Server ID deduplication | Prevents duplicate connections during startup race (keyed by ID, not address) |
| Peer token auth | Cluster traffic uses shared secret, not JWT |

---

## Performance Optimization

See `libs/myko/rs/OPTIMIZATION.md` for detailed performance optimization strategies, benchmarks, crate selection, and actor structure refactoring plans.
- use cargo check when you're doing checks instead of running a full build