# 05 — Scopes and Capabilities

**Normative.** Source: spec §1.1, §5. Invariant prefix `SC`.

---

## 1. The scope model

A crate declares a **scope root** — typically `Organization`. Every entity belongs to exactly one
scope. A mesh hosts many scopes; each node is opted in to a subset.

> **SC-1** — An entity is addressed by `(service.Type, scope, id)`. There is no unscoped entity;
> framework and shared-reference entities live in a designated **global scope** (§8).

> **SC-2** — **Scope sharing is type sharing.** A scope spanning services requires those services to
> share the scope-root entity — i.e. to link the crate defining it. There is no separate mechanism,
> because 02 TI-2 already guarantees that linking the same crate yields the same qualified type and
> the same schema by construction.

Services linking a common `identity.Organization` share the scope namespace, and a grant for org 5
spans all of them. Services that declare their own scope roots have genuinely separate scope
namespaces, and a grant in one means nothing in the other.

A consequence to plan around: **scope-root crates are the most load-bearing shared crates in a
deployment**, and their versioning discipline (02 §3) matters more than any other crate's.

## 2. Scope lives in the record header

> **SC-3** — **The scope id is in the record header (03 §3), not the entity body.**

A `LOGGED` node that cannot parse `billing.Invoice` must still decide whether the record belongs to a
scope it serves. If scope were in the body, schema-free storage and scope partitioning would be
mutually exclusive.

> **SC-4** — Scope may be *logically* derived from a relationship — `#[scoped_by(Organization)]`, a
> sibling of the existing `#[belongs_to]` family — but it is **denormalized onto the header at emit
> time**. The receiver never derives it.

> **SC-5** — **Entity→scope binding is immutable.** Moving an entity between scopes would be a
> cross-partition transaction with no atomicity across the nodes serving each side. Changing
> organization is delete + recreate.

SC-5 costs little precisely because the *mutable* half of tenancy lives elsewhere: identity→scope
access (§4) changes freely without moving a single record.

## 3. References

> **SC-6** — **Cross-scope references are forbidden**, and rejected in the relationship macros at
> compile time where the target type's scope root is statically known, and at emit time otherwise. An
> entity in org 1 referencing org 2 is a **tenancy violation**, not a partitioning inconvenience.

> **SC-7** — **Cross-service references within a scope are normal.** `billing.Invoice` referencing
> `identity.User`, both in org 5, is how services compose. It resolves through 02 TI-2: link
> `identity`, subscribe to its types, resolve locally. Don't link it and you hold an opaque id —
> explicit, and fine.

## 4. Access is granted by signed capability

Entity→scope binding is immutable; **identity→scope access is mutable**. Nothing in the data plane
moves when access changes — only what a node may replicate.

Putting the mapping in the replicated data plane fails both ways: in a global scope, every node learns
every tenant's membership; inside the scope it grants, you need the grant in order to replicate the
scope containing the grant.

> **SC-8** — Access is a **signed capability** presented in the control handshake (06 §4) and verified
> by signature: *"the authority for scope S asserts that NodeId X may replicate scope S, at these
> rights, until T."* There is no replicated ACL and no bootstrap paradox.

The transport already supplies the primitive: node identity is an ed25519 keypair (06 §1).

### Encoding

```rust
// myko-core::mesh::capability

pub struct ScopeGrant {
    pub version: u8,                  // 1
    pub scope_id: ScopeId,
    pub subject: NodeId,              // 32 bytes — the granted node
    pub authority: NodeId,            // 32 bytes — the deployment authority key
    pub rights: Rights,
    pub not_before: u64,              // unix ms
    pub not_after: u64,               // unix ms — §5
    pub serial: u64,                  // authority-local, monotonic; for audit and dedup
}

bitflags::bitflags! {
    pub struct Rights: u16 {
        const REPLICATE         = 1 << 0;  // receive and hold records for the scope
        const EXECUTE           = 1 << 1;  // execute commands authoritatively (09 §2)
        const DURABILITY_TARGET = 1 << 2;  // may advertise itself as relied-upon (01 NM-3)
        const HISTORY           = 1 << 3;  // may hold and serve log history (07 §5)
        const RESTORE           = 1 << 4;  // may run RestoreEntityTree (08 §9)
        const RESTORE_EXACT     = 1 << 5;  // may run it in Exact mode — strictly stronger
    }
}
```

> **SC-9** — The signed bytes are the **canonical CBOR encoding** (03 RF-17) of the `ScopeGrant`
> struct's fields in declaration order, prefixed with the domain separator `"myko.grant.v1\0"`. The
> wire form is `(grant_bytes, ed25519_signature)`. Signing a non-canonical encoding is a conformance
> failure — two encoders must produce identical bytes for one grant.

> **SC-10** — A verifier MUST check, in this order: signature validity against `authority`; that
> `authority` is the deployment authority it was provisioned with; `not_before <= now <= not_after`
> against its own clock; and that the claimed rights cover the operation being attempted. Any failure
> rejects the operation, not merely the grant.

> **SC-11** — `RESTORE_EXACT` is a **separate right from `RESTORE`**, because Exact mode destroys
> post-*T* work (08 §9). A deployment may grant preview-and-merge broadly and exact-restore narrowly.

### The management plane

> **SC-12** — **The authority is the management plane, and the framework provides it.** Nodes and
> grants are themselves myko entities in a management scope, mutated by framework commands. Issuance,
> node inventory, and rotation are ordinary command handling, not a parallel PKI.

```rust
#[myko_item(namespace = "mesh")]
pub struct Node {
    pub node_id: String,             // hex ed25519 public key
    pub display_name: String,
    pub enrolled_at: String,
    pub retired_at: Option<String>,
    pub last_manifest_generation: u64,
}

#[myko_item(namespace = "mesh")]
#[belongs_to(Node)]
pub struct ScopeGrantRecord {
    pub node_id: String,
    pub scope_id: String,
    pub rights: u16,
    pub not_after: String,
    pub serial: u64,
    pub revoked: bool,               // "stop renewing" bookkeeping — §5
}
```

Framework commands: `EnrollNode`, `RetireNode`, `GrantScopeAccess { node, scope, rights, ttl }`,
`RevokeScopeAccess`, `RenewScopeAccess`.

> **SC-13** — The handler for the management scope **holds the deployment authority keypair** and
> emits the signed capability as the command's effect. The keypair never leaves that handler's node,
> and `GrantScopeAccess` is therefore a scope-routed command (09 §3) whose eligible nodes are exactly
> the authority holders.

> **SC-14** — **Bootstrap is the one genuinely new mechanism.** The first node mints the deployment
> keypair; every later node is provisioned by pairing against it, out of band, receiving the authority
> public key and its initial grants. Nothing else in this design requires a step outside ordinary
> command handling.

## 5. Revocation

> **SC-15** — **Revocation is "stop renewing."** Short TTLs replace distributed revocation lists.

> **SC-16** — **TTL is configurable per scope**, because this tensions directly against offline
> operation: a partitioned node cannot renew. Sensitivity and offline requirements vary together, so
> the knob belongs where they are both known.

`RevokeScopeAccess` sets `revoked` on the `ScopeGrantRecord` and stops renewal. Existing grants remain
valid until `not_after`. A deployment needing faster revocation shortens the TTL for that scope and
accepts the offline cost.

## 6. Partitioning is the authorization boundary

> **SC-17** — **A node cannot leak what it never received.** Physical absence beats read-time
> filtering, and buys data locality, blast-radius reduction, and jurisdictional compliance. **The
> replication boundary is the authorization boundary** — which is what makes local-first projection
> safe at all.

Myko has no authorization model today. SC-17 keeps that from becoming permanent.

Two qualifications, both load-bearing:

> **SC-18** — **The guarantee is asymmetric.** *Never-granted* is strong. *Revoked* is weak — the node
> had the data, and revocation cannot retract what was delivered to a node you do not control.
> **Grant conservatively.**

> **SC-19** — **Complete only on single-identity nodes.** A node's effective scope set is the union of
> the identities it serves. A shared server holding org 5 for user A holds it while serving user B,
> who lacks access. **Shared nodes need identity-level filtering on top of partitioning.**

### The v1 trust model

> **SC-20** — **Nodes within a deployment are operator-trusted.** Capabilities govern *what replicates
> where* — tenancy, locality, blast radius — not defence against a malicious node. Specifically:
> `actor` (03 §3) is asserted by the origin and trusted; the direct-write rule (09 §7) is a protocol
> rule, not an enforced barrier; and MG-12's actor check catches bugs, not attacks.

An open mesh spanning parties that do not trust each other would need record-level write authorization
and attestable attribution on top. That is the same boundary 01 §2 draws for schema, restated for
security — and it is where this design stops.

## 7. Scope-aware anti-entropy and eviction

> **SC-21** — **Merkle trees are keyed per `(item_type, scope)`.** Per-`item_type` roots are *actively
> wrong* under partitioning: nodes serving orgs {1,2} and {2,3} never match roots, legitimately. Naive
> anti-entropy reads that as divergence, "repairs" it by exchanging everything, and pushes org-1 data
> onto a node not authorized to hold it.

> **SC-22** — Anti-entropy sessions **negotiate the scope intersection first**, and reconcile only
> shared scopes.

> **SC-23** — **A node never accepts repair data for a scope it does not serve.** Fail safe, at the
> receiver, independent of what the sender believed.

### Eviction

On losing access, a node purges that scope from its store and log.

> **SC-24** — **Local eviction MUST NOT emit DEL records.** The entities exist for every other node;
> routing eviction through the deletion path would replicate tombstones mesh-wide and **destroy the
> organization's data everywhere**. Eviction is a store-level purge producing no records — one of the
> local-only operations of 07 §2.

Anti-entropy handles the aftermath for free: sessions negotiate scope intersection first (SC-22), and
the node no longer claims that scope.

## 8. Consequences

> **SC-25** — **Write admission is per-scope**: "is a durability target reachable *for this scope*?"
> (07 §7).

> **SC-26** — **Routing keys on scope**: the routing table is `(command_id, scope) → nodes` (09 §3).

> **SC-27** — **Gossip topics are per-scope**, so partitioning falls out of topic membership. Scaling
> risk M2 (a node serving 1000 orgs holds 1000 HyParView/PlumTree memberships) is measured in phase 2,
> and blocks nothing.

> **SC-28** — **The global scope holds framework entities and shared reference data — never
> scope-root records.** Replicating the org roster to every node would leak the tenant list that §4
> deliberately keeps out of the data plane. A scope root lives in the scope it defines, and nodes
> discover scopes through grants, not enumeration.

---

## Invariant index

| ID | One line |
|---|---|
| SC-1 | Entities are addressed `(service.Type, scope, id)`; nothing is unscoped |
| SC-2 | Scope sharing is type sharing — no separate mechanism |
| SC-3 | Scope id lives in the record header |
| SC-4 | `#[scoped_by]` derives it logically; it is denormalized at emit time |
| SC-5 | Entity→scope binding is immutable |
| SC-6 | Cross-scope references are forbidden |
| SC-7 | Cross-service references within a scope are normal |
| SC-8 | Access is a signed capability, verified in the handshake |
| SC-9 | Grant bytes are canonical CBOR with a domain separator |
| SC-10 | Verification order: signature, authority, validity window, rights |
| SC-11 | `RESTORE_EXACT` is a separate, stronger right |
| SC-12 | The management plane is myko entities and framework commands |
| SC-13 | The authority keypair lives in the management-scope handler |
| SC-14 | Bootstrap-by-pairing is the one new mechanism |
| SC-15 | Revocation is "stop renewing" |
| SC-16 | TTL is per-scope, because it trades against offline operation |
| SC-17 | The replication boundary is the authorization boundary |
| SC-18 | Never-granted is strong; revoked is weak. Grant conservatively |
| SC-19 | Shared nodes need identity-level filtering on top |
| SC-20 | v1 trust model: nodes are operator-trusted |
| SC-21 | Merkle trees are keyed per `(item_type, scope)` |
| SC-22 | Anti-entropy negotiates the scope intersection first |
| SC-23 | Never accept repair data for an unserved scope |
| SC-24 | Eviction emits no records — ever |
| SC-25 | Write admission is per-scope |
| SC-26 | Routing keys on `(command_id, scope)` |
| SC-27 | Gossip topics are per-scope |
| SC-28 | The global scope holds no scope-root records |
