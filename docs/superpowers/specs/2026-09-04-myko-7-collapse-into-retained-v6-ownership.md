# Myko 7 collapse into retained Myko 6 ownership

**Date:** 2026-09-04
**Status:** implemented; replaces `2026-09-03-v6-v7-convergence-design.md`

## Problem

Myko 7 added real product value in durable commands, Redb-backed history, authenticated Iroh replication, typed services and scopes, authority, and node discovery and pairing. It also rebuilt handler registration, reactive execution, session ownership, client watches, and UI-facing collection state that retained Myko 6 code already owns in `myko` core, `ClientSession`, `SessionSink`, `MykoClient`, `QueryMapWatch`, `ViewMapWatch`, and the existing Leptos and GPUI adapters. The result is two application stacks in one workspace. The current `myko-runtime` and `myko-session` work narrows duplication inside the new stack, but it still preserves a second steady-state owner for handlers, sessions, and clients. The final design must keep the new federation and authority semantics, move them into the retained v6 owners, delete the duplicate v7 stack as callers migrate, and avoid any lasting compatibility bridge or dual wire contract.

## Usage

Caller usage stays centered on one public `myko` authoring surface and one public client/session runtime.

```rust
use myko::{
    CommandHandler, CommandResult, Myko, MykoApplication, MykoClient, QueryHandler,
    QueryMapWatch, ReportHandler, ViewHandler,
};

#[myko_service]
pub struct Messaging;

#[myko_item(service = Messaging, scoped_by = Conversation)]
pub struct Message {
    pub conversation_id: ConversationId,
    pub body: String,
}

#[myko_query(Message)]
pub struct ConversationMessages {
    pub conversation_id: ConversationId,
}

impl QueryHandler for ConversationMessages {
    fn build_view(
        ctx: myko::query::QueryBuildArgs<Self>,
    ) -> Option<impl myko::MapQuery<Key = std::sync::Arc<str>, Value = std::sync::Arc<dyn myko::AnyItem>>>
    where
        Self: Send + Sync + 'static,
    {
        let source = ctx
            .federated_items::<Message>()
            .scope(self.conversation_id.scope_id())
            .follow_authoritative();
        Some(source.filter_map_entries(move |_id, message| {
            (message.conversation_id == self.conversation_id).then_some(message)
        }))
    }
}

#[myko_command(Message, result = CommandResult<MessageId>)]
pub struct SendMessage {
    pub conversation_id: ConversationId,
    pub body: String,
}

impl CommandHandler for SendMessage {
    fn scope(&self, _node_id: myko::NodeId) -> ConversationId {
        self.conversation_id
    }

    fn execute(
        self,
        mut ctx: myko::command::CommandContext<Messaging, Conversation>,
    ) -> Result<CommandResult<MessageId>, myko::CommandError> {
        // TODO: typed set/delete through durable local-origin command context
        not_implemented!()
    }
}

let application = MykoApplication::builder()
    .service::<Messaging>()
    .build()?;

let node = Myko::node()
    .application(application)
    .journal(redb_backend)
    .iroh(native_endpoint)
    .build()?;

let client = MykoClient::connect(native_connector)?;
let watch: QueryMapWatch<Message> =
    client.watch_query_map(ConversationMessages { conversation_id })?;
let unread = client.watch_report(ConversationUnread { conversation_id })?;
node.dispatch_declared::<SendMessage, _>(|ctx| SendMessage::from(ctx).execute(ctx))?;
```

Transport authors feed decoded frames into `ApplicationHost::prepare` and `ClientSession::run`; they do not build a second typed application client.

## Shape

### Public contracts

```rust
pub struct MykoApplication {
    services: ActivatedServices,
    resources: ApplicationResources,
    handlers: HandlerCatalog,
}

pub struct ActivatedServices {
    by_id: std::collections::BTreeMap<ServiceId, ActivatedService>,
}

pub struct HandlerCatalog {
    queries: std::collections::BTreeMap<&'static str, std::sync::Arc<dyn QueryRegistration>>,
    reports: std::collections::BTreeMap<&'static str, std::sync::Arc<dyn ReportRegistration>>,
    views: std::collections::BTreeMap<&'static str, std::sync::Arc<dyn ViewRegistration>>,
    commands: std::collections::BTreeMap<CommandTypeId, std::sync::Arc<dyn CommandRegistration>>,
}

pub struct ApplicationHost {
    node: myko_federation::Node,
    application: std::sync::Arc<MykoApplication>,
    router: PreparedRequestRouter,
}

pub enum PreparedRequest {
    ReadHistory(ReadHistoryOp),
    FollowHistory(FollowHistoryOp),
    FollowItems(FollowItemsOp),
    FollowHandler(FollowHandlerOp),
    SubmitCommand(SubmitCommandOp),
    WatchCommand(WatchCommandOp),
    WatchCommands(WatchCommandsOp),
    ApproveAuthority(ApproveAuthorityOp),
}

pub enum AccessTarget {
    History { selection: ScopeSelection },
    Items {
        source: SourceSelector,
        service_id: ServiceId,
        scope_id: ScopeId,
        item_type: ItemTypeId,
    },
    Handler(PreparedHandlerAccess),
    Command { command_id: CommandId, command_type: CommandTypeId },
    AuthorityApproval { realm_id: RealmId, challenge_id: ChallengeId },
}

pub struct PendingHandlerFrame {
    epoch: u64,
    frontier: CursorFrontier,
    delta: HandlerDelta,
    resync_required: bool,
}

pub enum DeliveryPolicy {
    LosslessBounded { capacity: usize },
    CoalescingMap { capacity: usize },
}

pub struct SessionStreamState {
    epoch: u64,
    sequence: u64,
    frontier: CursorFrontier,
    window: Option<QueryWindow>,
    policy: DeliveryPolicy,
}

// Owned by `myko-items`; retained `myko` consumes it as a typed source.
pub struct ItemProjection<T: MykoItem> {
    items: std::collections::BTreeMap<T::Id, ItemState<T>>,
    next_revision: u64,
}
```

`PreparedRequest` is the only request interpretation boundary. `AccessTarget` is the only authorization input shape. `PendingHandlerFrame` is the only live delivery unit. `myko-items::ItemProjection<T>` stores typed IDs internally and erases them only in persistence and wire codecs.

### Retained owners and moved semantics

`myko` keeps ownership of:

- `QueryHandler`, `ReportHandler`, `ViewHandler`, and `CommandHandler`;
- inventory registration, service activation, and application resources;
- one lazy `MapQuery` and `Materialize` boundary;
- server-side typed projection caches and handler installation;
- `ClientSession`, `SessionSink`, bounded pending delivery, and continuation state;
- `MykoClient`, reconnect epochs, shared watch caches, `QueryMapWatch`, and `ViewMapWatch`;
- UI adapters for Leptos, GPUI, and the same keyed watch contract for Ratatui and Swift.

`myko-federation` keeps ownership of:

- immutable event history and `EventJournal`;
- local-origin command admission, claiming, retry, and idempotent ingestion;
- replay-then-follow typed item and command projections;
- source identity, checkpoints, selected replication, source reset, and composite frontiers;
- typed service, item, scope, and command identifiers that matter to durable state.

`myko-wire` is the sole owner of serialized client and server frames and their codecs. It has no handler traits, request routing, authorization, session state, or compatibility decoder. Retained `myko` converts each decoded frame once into its non-serializable `PreparedRequest`.

`myko-authority` keeps ownership of:

- principals, capabilities, claims, grants, delegation, approvals, obligations, and leases;
- evaluation over authoritative reactive facts and durable approval records;
- revocation and continuation checks.

`myko-node`, `myko-iroh`, `myko-local`, and retained `myko-server` keep ownership of:

- transport authentication, framing, discovery, pairing, peer supervision, readiness, and joined shutdown;
- wiring `PreparedRequest` into `ClientSession` and `ApplicationHost`;
- native replication and connector setup.

They do not own handler traits, query or view materialization, map reconciliation, or a second typed application client.

### Exact final crate and module ownership

```text
libs/myko/core/                    package `myko`, the only public framework/runtime owner
  src/application/
    activation.rs                  typed service activation and inventory filtering
    catalog.rs                     query/report/view/command registrations
    resources.rs                   app resources and capability registry
    prepared_request.rs            PreparedRequest + AccessTarget
  src/core/query/
    traits.rs                      retained QueryHandler + QueryBuildArgs
    federated_source.rs            private typed replay/follow adapters into MapQuery
  src/core/report/
    handler.rs                     retained ReportHandler + report materialization
  src/core/view/
    traits.rs                      retained ViewHandler + ViewBuildArgs
  src/core/command/
    handler.rs                     retained CommandHandler with typed durable context
  src/server/
    context.rs                     typed projection caches and handler install boundary
    client_session.rs              ClientSession, SessionSink, PendingHandlerFrame
    delivery.rs                    DeliveryPolicy and coalescing rules
    prepared_router.rs             one PreparedRequest router
  src/client/
    mod.rs                         MykoClient transport-neutral client
    query_map.rs                   QueryMapWatch and reconnect cache
    view_map.rs                    ViewMapWatch and reconnect cache
    report.rs                      scalar watches and lifecycle

libs/myko/macros/                  package `myko-macros`
  src/service.rs                   #[myko_service]
  src/item.rs                      #[myko_item]
  src/query.rs                     #[myko_query]
  src/report.rs                    #[myko_report]
  src/view.rs                      #[myko_view]
  src/command.rs                   #[myko_command]

libs/myko/items/                   package `myko-items`
  src/schema.rs                    generated type ids and operation ids
  src/projection.rs                typed ItemProjection<T>
  src/generated_queries.rs         all/by-id/by-ids query helpers

libs/myko/federation/              package `myko-federation`
  src/journal.rs                   EventJournal and history readers
  src/commands.rs                  command admission, claim, retry, watch
  src/projection.rs                typed item and command replay/follow
  src/replication.rs               source identity, checkpoints, resets
  src/topology.rs                  scope topology and selection
  src/request_types.rs             durable command and projection domain types

libs/myko/authority/               package `myko-authority`
  src/facts.rs                     authoritative fact projection
  src/prepare.rs                   PreparedAuthorityOp conversion
  src/evaluate.rs                  staged policy evaluation
  src/trusted_commit.rs            framework-only durable approval writer

libs/myko/node/                    package `myko-node`
  src/application.rs               application host construction
  src/status.rs                    readiness and lifecycle
  src/peer.rs                      peer reconciliation
  src/pairing.rs                   pairing and discovery commands
  src/discovery.rs                 discovery orchestration

libs/myko/local/                   package `myko-local`, sink and connector only
libs/myko/iroh/                    package `myko-iroh`, sink, connector, replication only
libs/myko/server/                  package `myko-server`, process and WebSocket edge adapter only
libs/myko/{leptos,gpui,ratatui}/   keyed UI adapters over MykoClient watches
```

### Current to final dependency graph

```text
myko (v6 core/client/server/ui)             myko-app -> myko-federation -> myko-items
        ^                                         ^            ^
        |                                         |            |
        +------------------- not the active base  |            |
                                                  runtime      authority/session logic
local/iroh/websocket -> myko-session -> myko-app <- myko-runtime
node -> myko-app + myko-session + local + iroh + federation + items
authority -> myko-app + myko-session + federation + items
```

```text
myko (public facade + handler/session/client runtime)
  -> myko-macros
  -> myko-items
  -> myko-federation
  -> myko-wire

myko-node
  -> myko
  -> myko-federation
  -> myko-redb
  -> myko-local
  -> myko-iroh
  -> myko-discovery

myko-local / myko-iroh / myko-server
  -> myko
  -> myko-wire
  -> transport-specific auth and IO crates

myko-authority
  -> myko
  -> myko-federation
  -> myko-items

myko-leptos / myko-gpui / myko-ratatui / myko-swift / generated TS bindings
  -> final Rust-generated watch and command types
```
The key cut is that every handler, session, and client path points to `myko`, not to `myko-app`, `myko-runtime`, or `myko-session`.

### Request, authority, session, and reactive flow

Request flow:

```text
wire frame
  -> myko-wire decode
  -> ApplicationHost::prepare
  -> PreparedRequest
  -> AccessTarget
  -> authority decision
  -> one route:
       history read/follow
       item snapshot/follow
       handler watch
       command submit/watch
       authority approval
```

Reactive flow:

```text
EventJournal + source checkpoint
  -> typed ItemProjection<T> / command projection
  -> private federated_source adapter in myko
  -> QueryHandler::build_view / ViewHandler::build_cell / ReportHandler::compute
  -> one materialization in MykoServerContext
  -> ClientSession pending frames with bounded delivery
  -> SessionSink encodes wire
  -> MykoClient decodes and applies keyed diff
  -> QueryMapWatch / ViewMapWatch
  -> Leptos / GPUI / Ratatui / Swift render from the same keyed map
```

### Commit and publication boundary

A local command becomes visible through one ordered commit protocol:

```text
admission authorization
  -> local-origin check
  -> stage typed mutations and lifecycle transition
  -> effect authorization over the staged claims
  -> append one CommittedBatch to EventJournal
  -> apply the complete batch to typed projections
  -> publish one MapRevision { diff, frontier, epoch, liveness }
  -> advance durable source checkpoints after successful ingestion and publication
```

`CommittedBatch` is the atomic journal unit. Projection code applies all of it or none of it. A Hyphae transaction publishes row changes and the corresponding `MapRevision` in the same propagation wave, so readers cannot observe new rows with an old frontier or liveness state. Command lifecycle transitions in the batch become observable only after the append succeeds. Remote checkpoints advance only after the batch has been durably ingested, projected, and published.

```rust
pub struct CommittedBatch {
    pub source: NodeId,
    pub service: ServiceId,
    pub scope: ScopeId,
    pub command: Option<CommandId>,
    pub mutations: Vec<ItemMutation>,
    pub lifecycle: Vec<CommandTransition>,
    pub authority_effect: Option<AuthorityEffect>,
}

pub struct MapRevision {
    pub diff: HandlerDelta,
    pub frontier: CursorFrontier,
    pub epoch: u64,
    pub liveness: SubscriptionLiveness,
}
```

The source checkpoint key is `(transport namespace, authenticated peer identity, source node id, service/scope selection)`. A changed source node id always discards the previous cursor and replays from the beginning. A cursor from a scope-filtered stream is never reused for a whole-node or differently scoped stream.

Weak single-flight handler caches key on operation id, service, source, scope selection, a typed-parameter digest, authority revision, and reconnect epoch. Inventory registrations are filtered by activated service before entering the catalog.

Authority flow:

```text
PreparedRequest
  -> PreparedAuthorityOp
  -> AuthorityFacts snapshot at authoritative frontier
  -> staged evaluation:
       authenticate identity
       validate scope and source coverage
       validate claims and capabilities
       validate approvals, leases, and continuation
       derive effect decision
  -> if durable approval is required:
       trusted framework capability writes approval command
  -> final allow/deny/resync decision
```

Session flow:

```text
PreparedRequest::Follow*
  -> ClientSession stream slot
  -> SessionStreamState { epoch, sequence, frontier, policy }
  -> wake on source change or auth change
  -> drain pending typed frames
  -> LosslessBounded for control and durable history
  -> CoalescingMap for keyed live diffs
  -> overflow or cursor gap => resync_required = true
```

### v6 sharp edges to keep, repair, or delete

- Keep `MapQuery`, `Materialize`, one materialization boundary, typed pending rows, reconnect caches, watch dedupe, and session-owned subscription guards.
- Keep `MykoClient` map and view watches, but port in v7 epoch, source, and frontier semantics so reconnect and source reset are first-class.
- Keep weak single-flight caches, but include service, source, scope selection, typed parameters, authority revision, and reconnect epoch in keys. A linked inventory registration is active only when its owning service is activated.
- Repair `ClientSession` so every control or durable stream is bounded and lossless. Do not reintroduce v6 unbounded disconnected sends.
- Repair the projection boundary so live state has one truth. `LiveCollection` as `rows + state + revision` does not move forward as a public owner. List watches and `ready()` are derived adapters over one map subscription and its atomic revision publication.
- Delete WebSocket-era assumptions as native contracts. Transaction IDs stay transport bookkeeping, not the semantic identity of a subscription.
- Delete stringly typed `ItemProjection<T>` storage. Store `T::Id`; erase only in persistence and wire codecs.
- Delete optional-bag `AccessRequest` and repeated `NodeRequest` matching. Parse once into `PreparedRequest`.
- Delete synthetic internal-principal authority bypass. Framework-owned durable approvals use a private trusted commit capability instead.

## Synthesis decision

This design keeps the right half of the 2026-09-03 convergence plan and deletes the wrong half. It keeps durable federation, Redb, typed services and scopes, authority, local-origin command execution, authenticated Iroh replication, discovery, pairing, and composite frontiers. It rejects `myko-runtime` as the base of the final system, rejects `myko-app` as the long-term public handler family, and rejects any plan that mines v6 behavior while deleting v6 ownership. The chosen base is the retained `myko` core, client, session, and UI runtime, widened just enough to host the new durable and authority semantics.

## Migration units

Treat the current dirty tree as a staging branch, not as a sequence of mergeable compatibility phases. The branch may use small local commits for verification, but each unit migrates callers and deletes the old API in the same wave, and the branch does not merge until the full collapse gates pass.

1. Replace the verifier before more architecture work.
   - Add `scripts/check-myko-collapse.sh` and delete `scripts/check-v7-convergence.sh`.
   - Make it fail while any duplicate public handler family, runtime client, or session crate remains.
   - Add hard gates for `cargo test -p forrest-mobile-core --lib`, especially the projection lifetime and deletion propagation failures.

2. Rehome public authoring to `myko`.
   - Move `myko-app` traits, service activation, and registration glue into `myko` and `myko-macros`.
   - Migrate every `myko-app` caller and macro expansion to the retained `myko` module path.
   - Delete `libs/myko/app` and `libs/myko/app-macros` in the same wave.

3. Port federated reactive sources into retained `myko` query, view, and report owners.
   - Add private `federated_source` adapters behind `QueryBuildArgs`, `ViewBuildArgs`, and `ReportContext`.
   - Materialize through `MykoServerContext`, not through `HandlerRuntime`.
   - Delete `HandlerRuntime`, `ErasedHandlerSubscription` ownership in `myko-app`, and any public `LiveCollection`-based UI contract that duplicates `QueryMapWatch` and `ViewMapWatch`.

4. Collapse to one session owner.
   - Move `NodeSessionService` request routing and follow logic into `myko::server::{prepared_router,client_session}` and `myko-node`.
   - Keep `ClientSession` as the only owner of guards, cancellation, cursor, window, epoch, sequence, and pending delivery.
   - Delete `libs/myko/session` in the same wave.

5. Collapse to one client runtime.
   - Extend `MykoClient` with connector traits needed by local, Iroh, and WebSocket.
   - Migrate `myko-runtime::ApplicationClient`, `node::ApplicationClient`, `LocalApplicationClient`, and `IrohApplicationClient` callers to `MykoClient`.
   - Delete `libs/myko/runtime` and the transport-specific typed client aliases in the same wave.

6. Replace request and authority interpretation.
   - Introduce `PreparedRequest`, `PreparedAuthorityOp`, and `AccessTarget`.
   - Convert the final wire request to `PreparedRequest` exactly once.
   - Delete `NodeSessionService::access_metadata`, the optional-bag `AccessRequest`, and repeated `NodeRequest` rematching in the same wave.

7. Refactor authority evaluation around authoritative facts and trusted commits.
   - Split `myko-authority` into fact loading, prepare, evaluate, and trusted commit modules.
   - Keep durable approval records and continuation checks.
   - Delete fallback request-time fact loading and the synthetic internal bypass.

8. Flatten crate roots and remove compatibility.
   - Split `myko-federation`, `myko-authority`, `myko-node`, and `myko-iroh` root files by ownership.
   - Remove legacy scope, peer, cursor, and wire aliases.
   - Move the v7 WebSocket framing into retained `myko-server` and delete `myko-websocket-gateway`.
   - Regenerate TS, Swift, Python, and C# bindings from the final Rust types in the same wave.

## Rewritten verifier and consumer gates

`scripts/check-myko-collapse.sh final` should prove the destination, not the superseded intermediate state.

Structural checks:

- `libs/myko/app`, `libs/myko/app-macros`, `libs/myko/runtime`, `libs/myko/session`, and `libs/myko/websocket-gateway` are absent from `members` and `default-members`.
- exactly one public `QueryHandler`, `ReportHandler`, `ViewHandler`, and `CommandHandler` family remains, all under `libs/myko/core`.
- exactly one public session owner remains, rooted at `libs/myko/core/src/server/client_session.rs`.
- exactly one public application client/watch runtime remains, rooted at `libs/myko/core/src/client`.
- `LocalApplicationClient`, `IrohApplicationClient`, `node::ApplicationClient`, `HandlerRuntime`, `collection_from_subscription`, optional-bag `AccessRequest`, and repeated `access_metadata` style request matching are absent.
- `ItemProjection<T>` stores typed item IDs.
- no compatibility scope, peer, cursor, or wire alias remains in steady-state code.

Behavioral gates:

- `cargo test -p forrest-mobile-core --lib`
- `cargo test -p forrest-mobile-core --lib tests::local_agent_runtime_stream_projects_typed_live_output -- --exact --nocapture`
- `cargo test -p forrest-mobile-core --lib tests::local_declared_network_resource_can_be_removed -- --exact --nocapture`
- retained Myko 6 query/report/view/client/session/UI tests
- durable command lifecycle, Redb restart, Iroh replication, source reset, authority revocation, discovery, pairing, and joined shutdown suites
- one-row mutation produces one keyed update over embedded, local, Iroh, and optional WebSocket connectors
- bounded lossless durable and control streams, plus coalescing-or-resync live map streams
- `cargo flux run gen`, then generated binding diffs match the final Rust-owned contract
- `cargo flux run check`, `cargo flux run test`, and strict Clippy with the repo target-dir rules

## Tradeoffs accepted

- We accept a fatter `myko` crate in exchange for one real framework runtime instead of two competing ones.
- We accept a branch-local atomic migration in exchange for deleting the old APIs immediately and avoiding a long-lived bridge.
- We accept wire and binding breakage in exchange for one clean cross-transport contract.
- We accept broad caller churn in macros and imports in exchange for restoring obvious ownership.

## Alternatives considered

- Keep `myko-runtime` as the final shared runtime. Rejected because it leaves `MykoClient`, `ClientSession`, and the v6 watch owners alive beside it, so the duplication merely moves.
- Delete retained Myko 6 ownership and port its behavior into Myko 7 crates. Rejected because it inverts the user directive and strands the mature watch/session/UI owners we already trust.
- Introduce a broad `myko-kernel` or `myko-reactive` crate first. Rejected because it would become a new god crate before the final ownership seams are proven.
- Preserve both old and new wire formats. Rejected because compatibility is explicitly out of scope and would force the repeated-request and alias code to survive.

## Resolved risks

- Handler installation authorization is derived from the typed registration and prepared request, while admission, continuation, and effect checks remain authority-owned.
- Retained map revisions carry frontier, epoch, liveness, diff, and resynchronization state without a second collection model.
- Generated bindings come from the final Rust-owned contract; binding-edge adapters do not own a second Rust runtime.
- Wrong-direction application, macro, session, runtime, and gateway crates were deleted after their callers moved.

## Deletion predicates

The collapse is not done until all of these are true at once:

- `rg -n 'pub trait (QueryHandler|ReportHandler|ViewHandler)' libs/myko` returns only `libs/myko/core`.
- `rg -n '(HandlerRuntime|ApplicationClient|LocalApplicationClient|IrohApplicationClient)' libs/myko` returns no production runtime owners outside retained `myko` client and session modules.
- `rg -n 'collection_from_subscription|AccessRequest|access_metadata|normalized_claims' libs/myko` returns no steady-state ownership path for the deleted bridge and optional-bag request model.
- `find libs/myko -maxdepth 2 -name Cargo.toml` shows no `app`, `app-macros`, `runtime`, `session`, or `websocket-gateway` crate.
- `Cargo.toml` `default-members` and `members` no longer include those crates.
- `cargo flux run gen` produces the shipped TS, Swift, Python, C#, and docs-site bindings from the final Rust types without compatibility aliases.
- `cargo test -p forrest-mobile-core --lib` passes, including the projection lifetime and deletion propagation cases.

## Completion

`scripts/check-myko-collapse.sh final` now proves the deletion predicates and focused ownership rules. The Myko generation, workspace checks, tests, strict Clippy partitions, focused federation/authority/Iroh suites, refreshed graph coverage, and downstream Forrest consumer gates pass against one source snapshot. No old wire format or compatibility decoder is retained.
