# Myko 7 v6 Reactivity Restoration Audit

Date: 2026-08-31

## Scope

This audit compares the Myko 7 application and federation path with the retained
Myko 6.5.9 query, report, view, client-session, and type-erasure machinery. It
covers `myko-app`, `myko-federation`, `myko-wire`, the local, Iroh, and WebSocket
adapters, `myko-ratatui`, and Forrest's conversation consumers.

The federation-first-principles specification is authoritative. In particular,
typed live collections must deliver a consistent initial result, a durable
cursor, typed additions/updates/removals, and explicit liveness. Hyphae remains
the application reactivity graph; transports do not own handlers.

## Executive finding

The desired architecture already exists in the retained v6 implementation, and
the first v7 commits already performed the useful WebSocket unwind:

- `WsWriter` became the transport-neutral `SessionSink`;
- `ClientSession` retained subscription guards and collection diff state;
- `PendingQueryResponse` retained typed `Arc<dyn AnyItem>` rows until a sink
  converted them to a wire response;
- views continued to materialize `MapQuery` plans into `CellMap`s once at the
  registration boundary;
- clients continued to apply incremental responses directly to local
  `CellMap`s.

The regression is a parallel handler stack in `myko-app`. It represents every
query, report, and view as `LiveSubscription<T>`, represents collections as
`LiveSubscription<Vec<T>>`, and originally mapped typed handler state into
`serde_json::Value` before a transport observed it. The new local, Iroh, and
compatibility WebSocket adapters attached to that parallel stack instead of the
retained session machinery.

## Component disposition

### Keep from Myko 7

- immutable node event history and Redb persistence;
- node identity, scopes, typed item mutations, and generated item IDs;
- command admission, durable lifecycle, and application command handlers;
- gap-free snapshot/follow contracts with explicit source identity and cursor;
- independent access operations and asymmetric trust;
- the shared `myko-wire` request/frame envelope;
- local Unix socket, authenticated Iroh, and optional WebSocket adapters;
- transport-neutral discovery and pairing;
- `LiveSubscriptionState` for scalar lifecycle and coherent cursor/liveness;
- explicit application activation of coarse `MykoModule` capability sets;
- link-time handler discovery scoped by the activated module rather than
  process-global activation of every linked handler.

### Restore from the v6 architecture

- `ViewHandler` and collection-query outputs backed by keyed Hyphae `CellMap`s;
- `MapQuery` composition and single materialization at the handler boundary;
- typed `MapDiff` propagation rather than comparing whole vectors;
- type-erased typed rows (`Arc<dyn ...>`) retained until the wire sink;
- pending typed responses separated from serialized wire responses;
- one session object owning subscription guards and cancellation;
- direct client-side application of upserts/deletes to a `CellMap`;
- UI bindings that observe the map and lifecycle without copying a second
  whole collection store;
- request deduplication/caching and reconnect epochs where they remain useful.

### Do not restore

- WebSocket-specific naming or ownership in the handler/session layer;
- a global WebSocket writer registry as the definition of client liveness;
- WebSocket transaction IDs as the native local/Iroh subscription identity;
- JSON as the internal type-erasure representation;
- a conceptual Myko client/server split between full nodes;
- polling, timer-driven view rebuilds, or adapter-owned application handlers.

## Baseline violations found

1. `myko-app::ViewHandler::build` returns `LiveSubscription<Vec<Item>, Cursor>`.
   Any changed row changes the whole collection value.
2. `HandlerContext::query` publishes each eager `ItemQuery::Output` as a whole
   replacement. Generated collection queries therefore discard item-level
   change identity before Hyphae composition begins.
3. The committed `ErasedHandlerSubscription` maps values and cursors into
   `serde_json::Value` inside `myko-app`; the in-progress keyed-vector bridge
   delays that conversion but still reconstructs diffs from whole vectors.
4. The new adapters each contain their own handler-follow loop instead of
   retaining one shared session implementation and acting as sinks.
5. Local and Iroh reactive clients reconstruct a complete `Vec` after every
   delta, so downstream UI code still receives whole-collection replacement.
6. `myko-ratatui::LiveBinding` only binds scalar/vector lifecycle cells; it has
   no `CellMap` lifecycle binding comparable to the retained v6 client maps.
7. Forrest's `AgentConversationView` has one row, `AgentViewSnapshot`, which
   itself owns every prompt and message. A row-level transport cannot make that
   shape fine-grained.
8. Forrest retains a separate legacy native conversation subscription alongside
   the application view path, producing two conversation read models.

## Restoration completed in this pass

- `MykoApplication::builder()` is the application composition boundary. Every
  generated `MykoItem` is a module, so applications select item types rather
  than enumerating handlers at the node or transport boundary.
- `#[myko_item]` exposes its generated all/by-ID/by-IDs queries as associated
  contracts. Selecting the item module activates them automatically.
- custom query, report, and view macros submit inventory registrations tagged
  with their owning item module. `MykoApplication` filters that inventory to
  only the selected items.
- Forrest selects Agent, AgentMessageItem, WorkspaceAliasItem, and
  AccessGrantItem. Its former marker modules and handwritten handler lists are
  gone; no daemon, transport, or node constructor builds a handler schema.
- `ViewHandler` now always returns a keyed `LiveCollection`; the temporary
  `KeyedViewHandler` registration fork has been removed.
- `LiveCollection` owns an immutable `CellMap`, lifecycle cell, and atomic typed
  `LiveCollectionRevision` carrying `MapDiff` plus its exact cursor/liveness.
- `HandlerContext::collection_from_subscription` is the explicitly named
  compatibility seam for eager v7 producers. Its reconciliation is typed and
  emits only changed rows.
- handler erasure queues typed revisions and performs JSON conversion only when
  a transport asks for its next frame.
- local, Iroh, and WebSocket handler loops drain all queued frames before
  sleeping, preventing a coalesced bounded wake-up from stranding updates.
- ordinary views use the incremental frame shape on every transport; there is
  no snapshot-view versus keyed-view API split.
- reactive local and Iroh clients expose `LiveCollection` and apply decoded
  changes to client-side `CellMap`s.
- `myko-ratatui::CollectionBinding` observes collection revisions and renders
  from the authoritative map without a copied reactive list store.
- Forrest conversation prompts and messages are independent stable rows.
  Streaming an existing message updates only that message row. The TUI binds
  directly to the local collection; compatibility snapshot callers rebuild a
  snapshot only at their API edge.

## Residual restoration work

1. Extend the same item-owned inventory registration to command handlers,
   startup hooks, and application services as those v6 capabilities move onto
   the federation-first runtime.
2. Item-query producers still commonly compute eager `Vec` outputs. Port v6's
   `MapQuery` composition into the v7 item-query surface so Forrest can delete
   `collection_from_subscription` rather than merely containing it.
3. The three adapters share `ErasedHandlerSubscription` and identical wire
   frames, but still own similar outer follow loops. Extract the remaining
   authorization/reconnect/session orchestration without giving any transport
   ownership of handler state.
4. Move the transport-boundary pending/frame conversion types out of
   `myko-app` into the shared session/wire layer. The current implementation
   delays serialization correctly, but its module placement still implies too
   much wire knowledge in the app crate.
5. Convert the mobile conversation owner from the compatibility Iroh `Vec`
   watch to `IrohReactiveViewSubscription`/`LiveCollection` when the Swift-side
   incremental row API is introduced.
6. Delete Forrest's older native composite conversation read model after every
   remaining non-application consumer migrates.
7. Restore v6 request-cache/deduplication semantics and explicit reconnect
   epochs after the shared session type is established.

## Target handler/session path

```text
durable Node snapshot/follow
        |
        v
typed projection CellMap(s) + lifecycle cell
        |
        v
query/view MapQuery composition
        |
        v
single materialized typed CellMap
        |
        v
typed MapDiff -> PendingHandlerUpdate<erased typed rows>
        |
        v
transport-neutral HandlerSession + SessionSink
        |
        +-------- local Unix socket --------+
        +-------- authenticated Iroh -------+--> shared NodeFrame wire shape
        +-------- WebSocket compatibility --+
        |
        v
client applies typed upserts/deletes to local CellMap
        |
        v
myko-ratatui / myko-gpui / myko-leptos lifecycle binding
```

Serialization belongs only in the final `PendingHandlerUpdate -> NodeFrame`
conversion and the inverse `NodeFrame -> typed client update` conversion.
Diff calculation, equality, ordering, joins, filtering, and lifecycle ownership
remain typed.

## Collection lifecycle contract

A live collection consists of:

- a keyed immutable `CellMap` used for composition and rendering;
- a coherent lifecycle cell containing cursor and liveness;
- typed map diffs emitted in the same Hyphae publication wave as the lifecycle
  revision;
- retained dependency and subscription guards;
- an initial `MapDiff::Initial`, followed by insert/update/remove/batch diffs;
- explicit replacement after resynchronization or source-history change.

Ordering is expressed by stable map keys, as in v6. Chronological views use a
compound sortable key ending in the stable item ID. There is no full order-ID
array on ordinary row-content updates.

## Forrest migration

The conversation view becomes a real collection. Messages and prompt lifecycle
records are separate keyed rows with stable IDs and sortable chronology keys.
Agent/source identity and the composite dependency frontier live in the view's
lifecycle/request context rather than wrapping every transcript in one row.

The TUI and mobile core hold the returned `CellMap` plus lifecycle owner. A
streaming content update changes one message row, produces one typed update,
and schedules one rerender. Neither transport nor UI receives the unchanged
conversation transcript.

## Migration order

1. Introduce the transport-neutral live-collection and pending-response types.
2. Move handler subscription ownership into a shared session object.
3. Change `ViewHandler` to return keyed reactive collections and preserve a
   compatibility constructor from a typed live list only during migration.
4. Make all three transports sinks over the same pending response.
5. Make local and Iroh clients expose typed `CellMap` watches.
6. Add `myko-ratatui` collection lifecycle bindings.
7. Split Forrest conversation snapshots into stable transcript rows and delete
   the duplicate native conversation read model once consumers are migrated.
8. Restore collection-query composition so generated queries do not reduce
   item state to eager vectors before views consume it.

## Acceptance criteria

- no `serde_json::Value` is used to calculate equality or collection diffs;
- changing one transcript message serializes and sends only that row;
- local, Iroh, and WebSocket emit identical handler update shapes;
- scalar reports retain coherent value/cursor/liveness behavior;
- map lifecycle survives reconnect and explicitly reports stale state;
- dropping the client owner cancels the session and all dependency guards;
- Forrest TUI and mobile consume the same typed collection contract;
- strict workspace formatting, Clippy, and focused transport/reactivity tests
  pass on Hyphae 3.1.
