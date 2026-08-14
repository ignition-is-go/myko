# 09 — Commands and Routing

**Normative.** Source: spec §11, §12.1. Invariant prefix `CR`.

---

## 1. What stays on the wire

> **CR-1** — **Projections become a placement decision; commands and records stay on the wire.**
> Queries, views, and reports are pure functions of local state (08 §5). Commands have side effects
> and need validation at a node that owns the state; records *are* the replication substrate.

Queries became wire operations for exactly one reason — the client had no store. `NodeScoped` (10 §3)
removes that.

## 2. Authoritative execution requires completeness

> **CR-2** — **Optimistic execution is always allowed and always provisional. Authoritative execution
> requires a node complete for the target scope** (01 NM-5).

Handlers validate against state — uniqueness checks, existence checks, cross-entity preconditions —
and every one gives a **wrong answer against a filtered view, silently**. Relationship cascades are
the most visible instance, not a separate problem: they walk the graph through the registry (08 §4)
and would reach only the children a filtered node happens to hold.

> **CR-3** — **`HANDLER` implies complete** for the scopes whose *state* it authoritatively mutates.

> **CR-4** — **Routing targets narrow** to nodes that both hold the handler **and** are complete for
> the scope.

This reinforces the tiered topology with a rule rather than a tendency: **the mesh tier decides, the
spoke tier predicts.**

> **CR-5** — **The rule governs authoritative state mutation, not side effects.** Handlers do two
> separable things: they *decide* (validate against state, then write) and they *act* (render
> something, move a light, play audio). Only deciding needs a complete view.

> **Rejected: carving out cascades.** "Run commands locally but cascades on a complete node" fixes the
> visible symptom and leaves validation broken. It also needs a cascade *owner* — since replicated
> records apply as `Remote` with no cascade (04 MG-17), having complete nodes cascade on inbound
> records would make **every** complete node cascade the same deletion independently. Convergent under
> merge, but wasteful, and choosing one responsible node is a leader election this design avoids.

## 3. Two dispatch modes

Not every command is a state mutation looking for an owner. **Some are addressed at a specific node
because that node is the thing being acted on** — show this scene in that browser, drive that fixture,
play on that device. A control system is largely made of these.

> **CR-6** — **Dispatch mode is a property of the command, not of the node.**

| Mode | Routed by | Requires completeness | Example |
|---|---|---|---|
| **Scope-routed** | `(command_id, scope) → owner` | **yes** (CR-2) | `CreateInvoice` |
| **Node-addressed** | `node_id → that node` | **no** | `SetActiveScene { node }` |

> **CR-7** — Node-addressed commands are exempt because **they are not making authoritative state
> decisions**. They act on the target node itself, or on state that node is the source of truth for. A
> filtered node executing one is not validating against a partial view of shared state; there is
> nothing global to validate against.

CR-7 is what lets **a browser be controlled through the mesh** despite holding a narrow filter.

Two constraints keep it honest:

> **CR-8** — **Any shared-state writes a node-addressed handler produces still follow CR-2.** They go
> out as ordinary writes through the optimistic-then-authoritative path (§5). The exemption covers the
> acting, not the deciding.

> **CR-9** — **Node-addressed delivery is at-most-once and unacknowledged by default.** It is not a
> state mutation, so it does not converge and is not replayed by anti-entropy. **If the target is
> offline, it does not happen.** Anything needing durability writes state and lets the target react to
> it.

> **CR-10** — **Gateways route in both directions** (01 §6): they carry attached nodes' commands into
> the mesh *and* deliver node-addressed commands back out to them.

### The routing table

> **CR-11** — **The routing table is primary**: `(command_id, scope) → complete nodes holding that
> handler`, built from gossiped manifests (01 §7) and validated against the handshake (01 NM-16).

```rust
pub struct RoutingTable {
    // (QualifiedName, ScopeId) -> eligible nodes, each with the manifest
    // generation it was learned from.
    entries: DashMap<(QualifiedName, ScopeId), Vec<RouteTarget>>,
}

pub struct RouteTarget {
    pub node_id: NodeId,
    pub generation: u64,
    pub complete: bool,          // 01 NM-5 — non-complete targets are not eligible
    pub consistency: ConsistencyDecl,
}
```

## 4. Route at ingress, execute locally

Commands are a **primary integration path** between services — a client sends `CreateInvoice` and it
routes to whichever node owns billing. But **handlers rarely dispatch nested commands across service
boundaries**; a handler's nested commands are normally same-service and stay in-process.

> **CR-12** — **`CommandHandler::execute` stays synchronous.** No breakage across myko or rship.

> **CR-13** — **There is exactly one interposition point**:
> `libs/myko/server/src/ws_handler.rs:1383 execute_command_job`, which today scans local inventory and
> errors on a miss (`:1447`, "Command handler not found"). It becomes:
>
> ```
> resolve owner for (command_id, scope)
>   if owner is me AND I am complete for this scope  -> proceed exactly as today
>   else                                             -> forward on the serve plane
> ```

> **CR-14** — **Cross-service nested calls get an explicit async API** — visibly different, so the
> network boundary is legible in handler code, and rare by convention rather than prohibition.

### Loop safety

`RequestContext.lineage` (`libs/myko/core/src/core/request.rs:52`) is an in-process call chain with no
hop count or TTL.

> **CR-15** — Cross-node routing adds a **hop limit and a visited-node set** to the forwarded request.
> Exceeding the limit is an error returned to the origin, never a silent drop.

## 5. Optimistic execution

> **CR-16** — **The same handler runs twice** — a general model, not an offline special case:
>
> 1. **Optimistically, on the originating node**, against whatever state it holds, for immediate
>    feedback. The result is a *prediction* and may be wrong, since validation ran against a partial
>    view.
> 2. **Authoritatively, on a node complete for the scope** — the real result.

The prediction is then **rebased** on the authoritative outcome. Offline (08 §8) is simply the case
where step 2 is deferred until reconnect; online, the round trip is short and the rebase usually a
no-op.

10 §3 is what makes this available at all: `CommandHandler::execute` becomes real on wasm, so the
identical Rust handler runs on both sides.

> **CR-17** — **A rebase is not a merge conflict** (04 MG-30). A superseded prediction never committed
> anywhere and MUST NOT be logged as data loss.

> **CR-18** — **The outbox holds commands, not records.** The command is what gets re-executed.
> Records produced optimistically are provisional and discarded on rebase. (The one exception is
> edge-owned entities, §7, which replay as records.)

> **CR-19** — **Provisional state is an overlay, not a merge.** LWW cannot un-apply, so optimistic
> records land in a provisional layer the reactive graph reads *through* (07 §4); rebase drops the
> overlay and applies the authoritative records — **never compensating writes**.

> **CR-20** — **Sagas and relationship rules do not fire on provisional apply** — the same
> no-produce discipline as remote apply (04 MG-16).

> **CR-21** — **Handlers with external side effects must not run optimistically.** Sending an email or
> charging a card cannot happen twice. This needs a marker on the handler, and **the default must be
> safe: opt in to optimistic execution, never opt out.**

```rust
#[myko_command(CreateInvoiceResult, optimistic)]   // opt-in — CR-21
pub struct CreateInvoice { /* … */ }
```

> **CR-22** — **Prediction accuracy is bounded by the filter, and that is acceptable.** A filtered node
> predicts correctly for entities it holds and cannot predict effects on those it does not — which
> are, by definition, not on its screen. A predicted cascade reaches only the children it holds; the
> authoritative run reaches all of them, and the rebase reconciles the difference.

This is the one place a handler's result is allowed to differ between nodes, and it is intentional.

## 6. Consistency is declared per command

When multiple complete nodes advertise a `command_id` for a scope, routing needs a rule — and no one
rule fits every command.

**OCC alone cannot exclude.** Preconditions are execution-time-only (04 MG-23, deliberately — see the
divergence proof there), so two load-balanced nodes can both read `occupied == false` inside the
replication window, both pass, and both emit. **Serialization has to come from routing**, and how much
a command needs is the command's own business.

```rust
pub struct ConsistencyDecl {
    pub routing: Routing,   // Sticky | Any
    pub occ: Occ,           // Enforced | BestEffort
}
```

> **CR-23** — **`routing = sticky` is the default mechanism.** The origin rendezvous-hashes
> `(scope, routing_key)` over eligible nodes, so same-entity commands serialize at one node in steady
> state. It costs nothing — the hash is a local computation — and degrades to any-eligible on
> failover.

> **CR-24** — Sticky routing needs a **routing key**: the argument identifying the target entity,
> marked `#[routing_key]`, defaulting to the conventional id argument where unambiguous. A sticky
> command with no resolvable routing key is a **compile error**, not a silent fallback to `any`.

> **CR-25** — **`occ = enforced` is confirm-then-write**: the origin treats the write as committed only
> once the executing node confirms preconditions. It is a real compare-and-set in steady state and
> **honestly best-effort during failover races** — and the response says which it was.

> **CR-26** — **`routing = any, occ = best_effort`** is maximum availability for commands that tolerate
> it: idempotent updates, blind writes, telemetry.

> **CR-27** — The declaration lives on the command and **travels in the manifest** (01 §7), so origins
> route accordingly without a side channel.

Seat-booking buys serialization; telemetry buys availability. Horizontal scaling is preserved —
distinct entities hash to distinct owners — with no rebalance story needed beyond the hash.

## 7. Edge-owned entities

Some entities have a natural single writer at the edge: a device's own status, a sensor's readings, a
session's presence. Routing those through a command to a complete node adds a round trip to learn what
only the edge node knows.

> **CR-28** — **A type may be declared edge-owned** — `#[myko_item(edge_owned)]` — giving each entity
> an **owning node, stamped at creation**. The owner **direct-publishes records for its own entities
> on the replication plane**: the single sanctioned bypass of command validation.

This inverts the old model, in which direct event publish was the general path.

> **CR-29** — **The replication-plane write rule, stated crisply:** a node may direct-write only
> records for entities it owns. **Every other write arrives as a command** (CR-2).

Under the v1 trust model (05 SC-20) CR-29 is a protocol rule rather than an enforced defence — but it
is *stated*, so an open-mesh future hardens a rule instead of discovering a hole.

Consequences:

> **CR-30** — **Single writer by construction** — no concurrent-write races, LWW trivially sound, OCC
> unnecessary for owned entities.

> **CR-31** — **Per-origin log streams are gap-checkable** exactly where it matters most: the owner's
> records carry the log-layer sequence (03 §3, 01 NM-10), so `LOGGED` contiguity is verifiable for the
> streams edge nodes produce.

> **CR-32** — **Offline replay ships records, not commands, for owned entities** — the one place 04
> §7's record-level comparison survives, and where a loss signals an ownership violation rather than
> an ordinary conflict (04 MG-29).

> **CR-33** — **Gateway-attached owners publish through their gateway** (01 NM-12, 06 TP-25): the
> record's `origin` is the attached node, and the gateway injects it into the replication plane on the
> owner's behalf. Termination still stands — the gateway is the owner's on-ramp, not its proxy for
> reaching anyone.

## 8. Idempotency

A routed command that times out gets retried, and a retry after partial execution must not
double-execute. Counters (04 §3) make the hazard concrete: a re-executed `+1` is a wrong number, not a
stale one.

> **CR-34** — **Every routed command carries an idempotency key**: `(origin NodeId, origin-local id)`.

> **CR-35** — **Handler nodes keep a dedup window keyed on the idempotency key and return the recorded
> result on replay.** The window must outlive the origin's retry budget; it is configured as such, not
> as a fixed duration guessed independently.

> **CR-36** — **The CR-21 side-effect marker gates retry re-execution the same way it gates optimistic
> execution.** A handler that cannot run twice is **exactly-once-or-error**, never silently re-run.

---

## Command lifecycle, end to end

```
   Origin node                         Executing node (complete for scope)
        │
   [1]  │ optimistic run (if opted in, CR-21)
        │   → provisional records → overlay (CR-19)
        │   → sagas suppressed (CR-20)
        │
   [2]  │ resolve (command_id, scope) → RouteTarget (CR-11)
        │   sticky: rendezvous_hash(scope, routing_key) (CR-23/24)
        │
   [3]  │── CommandFrame { idempotency_key, hop_limit,   ──►│
        │      visited[], args, consistency } (CR-34/15)    │
        │                                                   │ dedup window hit? (CR-35)
        │                                                   │   yes → return recorded result
        │                                                   │
        │                                                   │ complete for scope? (CR-2)
        │                                                   │   no  → reroute or error
        │                                                   │
        │                                                   │ execute; read-set tracked (04 MG-19)
        │                                                   │ preconditions checked HERE (04 MG-23)
        │                                                   │
        │                                                   │ occ=enforced? confirm before ack (CR-25)
        │◄───────── CommandResult { result, occ_outcome } ──│
        │                                                   │
   [4]  │ rebase: drop overlay, apply authoritative          │ records replicate as Remote
        │ records (CR-19). Difference → UX event,            │ to every peer (04 MG-16)
        │ never a conflict record (CR-17)                    │
```

---

## Invariant index

| ID | One line |
|---|---|
| CR-1 | Projections are placement; commands and records stay on the wire |
| CR-2 | Optimistic is always allowed; authoritative requires completeness |
| CR-3 | `HANDLER` implies complete for scopes it authoritatively mutates |
| CR-4 | Routing targets are handler-holding **and** complete |
| CR-5 | The rule governs deciding, not acting |
| CR-6 | Dispatch mode is a property of the command |
| CR-7 | Node-addressed commands are exempt from completeness |
| CR-8 | Shared-state writes from node-addressed handlers still follow CR-2 |
| CR-9 | Node-addressed delivery is at-most-once, unacknowledged |
| CR-10 | Gateways route in both directions |
| CR-11 | The routing table is primary, built from manifests |
| CR-12 | `CommandHandler::execute` stays synchronous |
| CR-13 | One interposition point: `execute_command_job` |
| CR-14 | Cross-service nested calls get an explicit async API |
| CR-15 | Hop limit + visited set; exceeding it errors, never drops |
| CR-16 | The same handler runs twice: optimistic then authoritative |
| CR-17 | A rebase is not a conflict |
| CR-18 | The outbox holds commands, not records |
| CR-19 | Provisional state is an overlay; rebase drops it |
| CR-20 | Sagas and relationship rules do not fire on provisional apply |
| CR-21 | Optimistic execution is opt-in, never opt-out |
| CR-22 | Prediction accuracy is filter-bounded, and that is fine |
| CR-23 | `routing = sticky` is the default; rendezvous hash |
| CR-24 | A sticky command with no routing key is a compile error |
| CR-25 | `occ = enforced` is confirm-then-write, honest about failover |
| CR-26 | `any` + `best_effort` for availability-first commands |
| CR-27 | The consistency declaration travels in the manifest |
| CR-28 | `edge_owned` types have an owning node that direct-publishes |
| CR-29 | Direct-write only what you own; everything else is a command |
| CR-30 | Edge ownership means single writer by construction |
| CR-31 | Owned streams carry `log_seq` and are gap-checkable |
| CR-32 | Owned entities replay as records, not commands |
| CR-33 | Gateway-attached owners publish through their gateway |
| CR-34 | Every routed command carries an idempotency key |
| CR-35 | Dedup window outlives the origin's retry budget |
| CR-36 | Side-effecting handlers are exactly-once-or-error |
