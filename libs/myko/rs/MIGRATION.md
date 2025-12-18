# Myko Framework: TypeScript to Rust Migration Guide

This document captures the TypeScript Myko framework functionality for migration to `@myko/rs`.

## Core Concepts (Language-Agnostic)

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

## Bus Architecture

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

## Repository Pattern

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

## Relationship System

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

## WebSocket Protocol

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

## Handler Patterns

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

## Standard Query/Command Types

For entity `Foo`, typically define:
- `GetFoos` - Get all (optionally scoped)
- `GetFoosByIds { ids: ID[] }` - Get by ID list
- `GetFoosByQuery { partial: Partial<Foo> }` - Filter by partial match
- `CreateFoo { ...fields }` - Returns new ID
- `DeleteFoo { fooId: ID }` - Returns void
- `UpdateFoo { fooId: ID, ...fields }` - Returns void

## Context Propagation

All commands/queries/reports carry context:
- `tx`: Transaction ID (UUID) for correlation
- `commandClientId`: Originating client ID
- `lineage`: Trace of where command originated (e.g., `['client']`)
- `userToken`: Auth token (optional)

Context is passed via `.withContext(parentCtx)` method and propagated through sub-operations.

## Saga Pattern

Sagas are event processors that react to events and produce commands:
```
(events$: Observable<MEvent>) => Observable<MCommand>
```

Used for: cross-entity side effects, cleanup operations, log retention, time-based automation.

## Framework vs Application Entities

**Framework Entities** (in @myko/core - must be in @myko/rs):
- `Server`: Cluster node identity and discovery
- `Client`: Connected WebSocket clients with windback state
- `EventContainer`, `GetEventLog`: Event history querying

**Application Entities** (in @rship/entities - NOT part of framework):
- `Target`, `Emitter`, `Action`, `Scene`, `Binding`, etc.
- Rship-specific domain entities built ON TOP of myko

## Peer & Federation System

Multi-server clustering where servers discover each other and proxy queries/commands/reports.

**Server Entity**: `{ id, version, address, port, startedAt }`

**Server Lifecycle**:
1. On startup, delete stale `Server` records with same address:port
2. Publish new `Server` entity with current hostId
3. Watch `GetPeerServers` query for other servers

**Federation Wrappers**:
- `PeerQuery { query, peerId }` - Routes query to specific peer
- `PeerCommand { command, peerId }` - Routes command to specific peer
- `PeerReport { report, peerId }` - Routes report to specific peer

## Windback (Time-Travel)

**TypeScript (Current - Time-Based)**:
```
Client { serverId: ID, windback?: string (ISO DateTime) }
SetClientWindbackTime { windback: string } -> boolean
ClearClientWindbackTime -> void
```

**Rust (Planned - Commit-Based)**:
Uses explicit snapshots instead of time-based lookup:
```
Snapshot { name, message, scope, parent_id }
SnapshotData { snapshot_id, item_type, items }
CreateSnapshot { name, message, scope }
SetClientWindbackSnapshot { snapshot_id }
```

**Common Behavior**:
- Commands blocked during windback unless marked `allowDuringWindback: true`
- Queries return historical/snapshot data instead of live

## Authentication

**Auth Service Interface**:
```
canActivate(token) -> Promise<boolean>   // Validates JWT or peerSecret
getUserId(token) -> Promise<string?>     // Extracts `sub` claim
getPeerToken() -> string                 // Cluster secret
```

**Command Auth Options**:
- `@MykoCommand({ noAuth: true })` - Skip auth check
- `@MykoCommand({ allowDuringWindback: true })` - Allow in windback mode

---

## Rust Implementation Status

### Done
- Actor system: Server, EventManager, QueryManager, ReportManager, CommandManager
- WebSocket server/connection actors
- Kafka producer/consumer actors (optional)
- Item/Event/Query/Report/Command base types
- `#[myko_item]` macro generating queries, reports, commands
- Server/Client entities with basic queries
- Query delta protocol
- Relationship system: `#[belongs_to]`, `#[owns_many]`, `#[ensure_for]`
- Saga pattern with EventBus
- Peer discovery and federation
- Full-text search (tantivy)
- Context propagation

### Not Yet Implemented
- Windback/snapshots
- Authentication (Auth0/JWT validation)
- RebalanceItem command

---

## Rust-Specific Patterns

### Relationship System Usage

```rust
#[myko_item]
pub struct Binding {
    #[belongs_to(Scene)]      // Parent DEL → cascade delete
    pub scope_id: Arc<str>,
}

#[myko_item]
pub struct Scene {
    #[owns_many(BindingNode)]  // Parent DEL → delete children
    pub node_ids: Vec<Arc<str>>,
}

#[myko_item]
pub struct SessionVariable {
    #[ensure_for(Project)]     // Auto-create per dependency
    pub project_id: Arc<str>,
    #[default_value("unnamed")]
    pub name: String,
}
```

### Client ID Auto-Population

```rust
#[myko_item]
pub struct Instance {
    #[myko_client_id]
    pub client_id: Option<String>,  // Auto-populated with WebSocket client ID
}
```

### Full-Text Search

```rust
#[myko_item]
pub struct Target {
    #[searchable]
    pub name: String,
    #[searchable]
    pub category: String,
    pub service_id: Arc<str>,  // not searchable
}
```

### Saga Pattern

```rust
impl Saga for MyTransitionSaga {
    fn build(events: EventStream, _ctx: Arc<SagaContext>) -> CommandStream {
        Box::pin(events
            .of_item_type("Scene")
            .of_change_type(MEventType::SET)
            .pairwise()
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

### Context in Handlers

```rust
#[myko_command_handler(CreateScene)]
async fn handle(cmd: CreateScene, ctx: &CommandContext) -> Result<SceneId> {
    let project = ctx.query(GetProjectById { id: cmd.project_id }).await?;
    ctx.publish_set(Scene { ... }).await;
    Ok(scene_id)
}
```

---

## Design Documents

For detailed designs of unimplemented features, see:
- Authentication: Auth0 + `async-oidc-jwt-validator`
- Windback: Version-control approach with Snapshot entities
- See `OPTIMIZATION.md` for performance strategies
