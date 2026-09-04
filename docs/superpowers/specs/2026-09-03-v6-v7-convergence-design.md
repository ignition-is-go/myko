# Myko v6/v7 convergence design

**Date:** 2026-09-03

**Status:** Accepted for implementation

## Decision

Myko 7 keeps the v7 durable federation model and restores the useful v6 reactive execution model inside it.

The public framework remains one coherent `myko` API. Internally, responsibilities stay in focused crates. We will not replace the current split with a broad kernel crate, and we will not make the v7 application layer depend on the legacy v6 `myko` crate.

The retained v6 invariants are:

- query and view handlers return lazy keyed Hyphae plans;
- report handlers return lazy scalar pipelines;
- the framework materializes each handler plan once at its installation boundary;
- one session owner retains guards, cancellation, sequence, cursor, window, and diff state;
- rows remain typed through equality, ordering, filtering, diffing, and pending delivery;
- serialization happens only when a pending update becomes a wire frame.

The retained v7 invariants are:

- immutable event history and durable command lifecycle;
- typed services, items, commands, scopes, and application activation;
- one-service, one-scope atomic command batches;
- local-origin command execution and explicit forwarding;
- authenticated asymmetric federation and continuation authorization;
- canonical `NodeRequest` and `NodeFrame` semantics for local, Iroh, and WebSocket transports;
- replay-then-follow projections with explicit cursor and liveness.

## Author-facing API

The existing declaration style remains. The macros generate the contract and its registration. A handler that lacks macro-generated registration must be impossible to construct as an application handler.

```rust
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
    type Key = MessageId;
    type Row = Message;

    fn build(
        &self,
        ctx: &QueryContext,
    ) -> Result<impl MapQuery<Key = Self::Key, Value = Arc<Self::Row>>, AppError> {
        Ok(ctx
            .items::<Message>()?
            .filter_map_entries(|id, message| {
                (message.conversation_id == self.conversation_id)
                    .then(|| (id, message))
            }))
    }
}

let application = MykoApplication::builder()
    .service::<Messaging>()
    .build()?;
```

Application authors activate services, not individual handlers. Inventory registration remains the link between a compiled declaration and its owning service. Activation filters the inventory by typed service ownership; it does not enable every linked handler and does not require a handwritten handler list.

Commands may associate with a service or an item. An item association infers the service and immediate scope parent. A service association declares its scope root. Service selection and scope relationships are typed; text is limited to persistence, diagnostics, and wire encoding.

Query, view, and report remain separate domain contracts:

- a query returns keyed items;
- a view returns a keyed derived row type;
- a report returns a scalar value.

They may share installation and subscription machinery without becoming aliases. Snapshot evaluation of any of them is available only through `CommandContext`.

## Internal ownership

Dependencies point downward:

```text
myko                         public facade and prelude
  -> myko-node               native composition and lifecycle
  -> transport adapters      local, Iroh, WebSocket framing/connectivity
  -> myko-runtime            activated plans and connection sessions
  -> myko-wire               canonical serialized requests and frames
  -> myko-federation         durable history, command lifecycle, authority primitives
  -> myko-items              typed declarations and generated identities
```

`myko-runtime` is introduced only when session and plan ownership are extracted. It is not a generic framework kernel. Its allowed modules are:

```text
runtime/
  application.rs     activated service catalog and resources
  registry.rs        inventory-filtered typed handler factories
  plan_cache.rs      weak keyed installations and one materialization boundary
  handler.rs         typed query/view/report installations
  session.rs         guard, cancellation, sequence, cursor, window, and epoch ownership
  pending.rs         typed pending scalar and map updates
  mailbox.rs         bounded lossless and coalescing delivery policies
```

It does not own item schemas, history, authority evaluation, wire DTOs, transport framing, or node composition.

The existing `myko` package eventually becomes the thin public facade. That cut happens only after the v6 callers have migrated; the facade will not hide two live runtimes.

## Reactive path

```text
durable commit
  -> typed projection source applies one keyed mutation
  -> query/view builds a lazy MapQuery; report builds a lazy Pipeline
  -> registry installs the plan once
  -> connection session owns the subscription and typed diff accumulator
  -> pending typed update
  -> myko-wire encodes NodeFrame
  -> shared client applies the keyed delta to one client map
  -> UI binding observes that map
```

There is no `LiveSubscription<Vec<T>>` to `LiveCollection<T>` reconstruction in this path. One changed row remains one changed row.

The Hyphae map key is the only row identity. Query and view handlers do not
declare a second `item_key` function, and transport clients do not recreate
keys from decoded values. Initial collection states and subsequent upserts
carry the materialized map key across the wire.

`LiveCollection` must have one authoritative publication. A collection revision contains its typed diff, cursor, and liveness. Its materialized rows and derived status update in the same Hyphae batch. Rows, state, and revision cannot be independently writable truths.

## Command path

```text
typed command
  -> one boundary conversion prepares routing, access target, and execution metadata
  -> authorization
  -> local typed handler or unchanged forwarding
  -> CommandContext takes bounded snapshots and stages typed mutations
  -> one atomic durable commit
  -> projection update
```

Application code does not create command IDs, envelopes, requests, principals, or scope strings. `DeclaredCommand` and `CommandRequest` remain framework-private.

The request boundary converts `NodeRequest` exactly once into a typed `PreparedRequest`. Each prepared operation contains only the fields that operation can use. Authorization consumes a structural `AccessTarget`, not an optional-field bag. Routing, metrics, authorization, and execution do not rematch the wire request.

## Session and transport path

One connection session owns every live operation. It retains:

- the subscription guard and cancellation token;
- sequence, cursor, reconnect epoch, and query window state;
- the typed pending-update accumulator;
- the explicit delivery policy.

Finite control and durable history use bounded lossless delivery and apply backpressure. Compatible keyed live diffs may coalesce. A cursor or epoch discontinuity produces resynchronization instead of guessing. Queue capacity cannot be omitted.

Local, Iroh, and WebSocket adapters authenticate, frame, and move bytes. They do not implement typed commands, query/view/report decoding, map reconciliation, handler ownership, or application lifecycle. A shared client consumes canonical frames once.

## First landing unit

The first change flips handler wire ownership without changing application behavior:

1. Move `HandlerRequest`, `ErasedHandlerState`, and `ErasedViewDelta` from `myko-app` into `myko-wire::handler`.
2. Update all imports.
3. Remove the `myko-wire -> myko-app` Cargo dependency.
4. Delete the original definitions from `myko-app`.
5. Prove round trips in `myko-wire` and run focused app/session/transport checks.

This is an intermediate ownership correction, not the final typed pending-update boundary. Later, `myko-runtime` emits typed pending updates and `myko-wire` alone erases them into these DTOs.

## Migration sequence

1. Flip handler protocol ownership and remove the wire-to-app dependency.
2. Restore lazy keyed query/view plans and scalar report pipelines. Migrate macros and callers, then delete `ItemQuery::execute`, `ProjectionQueryFactory`, and `collection_from_subscription`.
3. Extract the one handler-session owner and typed pending updates. Replace unbounded channels with explicit delivery policies.
4. Introduce one transport-neutral client and connection contract. Migrate local, Iroh, and WebSocket together, then delete transport-specific typed clients and owner enums.
5. Prepare each wire request once into structural routing and authorization types. Delete the optional-bag `AccessRequest` and repeated `NodeRequest` interpretation.
6. Make collection publication coherent, store typed item IDs internally, inject the runtime owner, and implement service/scope readiness.
7. Refactor authority evaluation into typed stages over a reactive projection and a private trusted-write capability.
8. Remove steady-state legacy branches, split crate roots by the ownership map, migrate bindings, and repurpose `myko` as the sole facade.

Every phase migrates callers and deletes the superseded path. There are no long-lived compatibility layers or legacy feature switches.

## Alternatives rejected

### Depend on the v6 `myko` crate

Rejected because the crate also owns legacy wire, Autosocket, server, client, cache, and macro behavior. The dependency would preserve both runtimes and prevent the existing `myko` package from becoming the v7 facade without a cycle.

### Put everything in `myko-kernel`

Rejected because schema, history, application activation, Hyphae execution, authorization, session, wire, client, and transport would again share one god crate. Module names would not enforce the dependency boundaries.

### Publish `myko-reactive` immediately

Rejected for the first cut. The reusable core is real, but its exact ownership is not proven until plan installation, session lifetime, and client application use the same implementation. Keep it inside the narrow runtime first. Extract it later only if at least two independent lower-level consumers need it without application/session semantics.

### Merge query, view, and report

Rejected because they express different domain results and because the existing Myko authoring model depends on those distinctions. Shared machinery is not a reason to erase domain concepts.

### Replace inventory with handwritten handler lists

Rejected because it repeats declarations and permits registration drift. The application selects typed services; generated registrations carry typed ownership and the registry filters them.

## Design red flags

Stop and revise the design if any of these becomes necessary:

- a query, view, or report can be registered without its macro;
- service activation requires listing every handler;
- `myko-wire` depends on application or runtime behavior;
- federation or item crates depend upward on runtime, wire, or transports;
- a transport owns a handler guard, cursor, diff accumulator, or typed client implementation;
- a handler compares or diffs `serde_json::Value`;
- a one-row mutation produces a whole-vector replacement;
- query, view, or report context exposes a snapshot or mutation capability;
- a new crate collects unrelated history, schema, runtime, wire, and transport responsibilities;
- the public `myko` facade hides a second active v6 runtime.

## Open design questions

- Hyphae must expose a revision hook that couples a map diff with the exact cursor and liveness publication. If it cannot, that capability belongs in Hyphae rather than a compensating Myko callback-order repair.
- Shared handler installations are safe only when authorization does not redact rows differently for identical parameters. Principal-sensitive redaction requires an authority-sensitive installation key or a typed post-plan filter.
- Window changes need a transport-neutral stream-control operation and cannot reuse WebSocket transaction identity.
- Multi-source views need a typed frontier, not one fabricated scalar cursor.
