# First-class graph edges and downstream-defined entity categories

- **Status**: proposed
- **Date**: 2026-08-13
- **Scope**: Myko core, macros, relationship/query indexing, server context, and
  backend-neutral type generation. Additive Myko 6.x extension.
- **Compatibility target**: existing items, events, persistence rows, generated
  item/query APIs, and relationship behavior remain unchanged until an
  application explicitly opts a type into the new model.

## Summary

Myko already stores a graph implicitly. Items are vertices, and an item with
one or more `#[belongs_to]` fields describes relationships to other vertices.
For a binary association, the ordinary item representation is already a good
canonical event-sourced edge record: it has an ID, arbitrary payload, SET/DEL
history, persistence, reactive storage, generated queries, and cascade
behavior.

What Myko lacks is a framework-level distinction between **ownership/reference
relationships** and **navigable connectivity**. It also lacks a way to declare
that an endpoint may be any downstream-defined family of item types, or that an
endpoint has an indexed address below the item boundary, such as a port, slot,
or channel that is not itself a Myko item.

This specification adds three related, additive concepts:

1. **Entity categories**: downstream-defined open sets such as `TagTarget`.
   Categories constrain erased endpoints without teaching Myko their domain
   meaning.
2. **Edges**: ordinary Myko items with additional endpoint, direction,
   pair, lifecycle, and indexing metadata. They remain in the normal
   event/store/persistence pipeline.
3. **Endpoint qualifiers**: indexed scalar components below an endpoint item,
   such as a port ID. They allow both broad adjacency queries for an item and
   narrower adjacency queries for a qualified address.

Edges use the existing item event type, entity stores, persistence schema, and
wire protocol. Optional graph projections derive from canonical item state and
are benchmarked against Myko's fully indexed `belongs_to` query path.

## 1. Motivation

### 1.1 Associations are already domain records

Many useful connections carry data:

- membership plus role and status;
- attachment plus author and timestamp;
- dependency plus ordering or condition;
- network link plus observed health;
- node connection plus endpoint address;
- scheduled inclusion plus offsets.

Such a connection is not merely two IDs in an implementation-only join table.
It may have independent history, commands, authorization, and payload. Treating
it as an ordinary item is therefore a strength, not a workaround.

```rust
#[myko_item]
pub struct ProjectMember {
    #[belongs_to(Project)]
    pub project_id: ProjectId,

    #[belongs_to(User)]
    pub user_id: UserId,

    pub role: ProjectRole,
}
```

This representation already participates in Myko's canonical SET/DEL event
stream and typed query generation.

### 1.2 Connectivity is not the same as ownership

`belongs_to` currently expresses a child-to-parent relationship. It supports
orphan cleanup and cascade deletion. A peer association may instead need:

- deletion of the association when either endpoint disappears, without deleting
  the opposite endpoint;
- a restriction that prevents endpoint deletion while associations exist;
- forward and reverse navigation;
- an exact endpoint-pair lookup;
- directed or undirected semantics;
- parallel edges or pair uniqueness;
- distinct A/B positions when both ends use the same item type.

Those semantics should not be inferred from field count or from two coincidental
`belongs_to` attributes.

### 1.3 Current query indexes are excellent but demand-driven

For required `belongs_to` fields, Myko generates routes for every non-empty
subset of pinned fields. A two-endpoint association can therefore route by its
first endpoint, second endpoint, or exact pair. Live buckets are lazy and weakly
retained, which is efficient when few keys are watched.

A newly live bucket, however, is backfilled from the current store. Repeated
cold subscriptions, broad `In` filters, and graph traversal can repeatedly pay
store scans and build application-local adjacency maps. Relationship cascades
also maintain a separate reverse index. A declared edge gives Myko permission
to consolidate these concerns behind a reusable adjacency projection when the
workload justifies its steady memory and write cost.

### 1.4 Open-world relationships require polymorphic endpoints

A generic tag facility should be definable downstream. Myko should not contain
a built-in `Tag`, `TagTarget`, or `TagAssignment` concept. An application should
be able to declare the category and use a stable erased reference:

```rust
#[myko_category]
pub struct TagTarget;

#[myko_in(TagTarget)]
#[myko_item]
pub struct Article {
    pub title: Arc<str>,
}

#[myko_in(TagTarget)]
#[myko_item]
pub struct Image {
    pub url: Arc<str>,
}
```

A tag assignment can have one concrete end and one category-constrained erased
end. Other edge types can constrain both erased ends to independently defined
categories.

### 1.5 Endpoints can have non-entity addresses

Connections may terminate at a port or slot defined inside an item's schema.
The item is a Myko entity; the port is not. The framework should index both:

- every connection incident to the item;
- every connection incident to a particular port on that item.

The port must not be promoted to a fake entity merely to obtain an index.

### 1.6 Current implementation seams

The design deliberately extends existing Myko machinery:

- `core/item/traits.rs` defines the erased `AnyItem`/`Eventable` contract that
  edge items continue to implement.
- `store/entity_store.rs` and `store/registry.rs` already partition canonical
  reactive state by registered item type.
- `server/context.rs` already provides the mutation ordering for store reduce,
  relationship enforcement, search, and persistence; graph projection hooks
  belong in this pipeline.
- `core/relationship/mod.rs` and `server/relationship_manager.rs` own current
  `BelongsTo`, `OwnsMany`, `EnsureFor`, cascade, and orphan behavior. Edge
  connectivity is additive and must not silently reinterpret these relations.
- `core/query/registration.rs` owns demand-driven `BelongsToSourceIndex`
  routing and is the correctness/performance baseline for endpoint queries.
- `codegen_types.rs` owns the backend-neutral aggregate catalog to which edge
  and entity-category registrations are added.
- `wire/event` and the server persistence implementations already store items
  generically by item type and ID; they require no edge-specific envelope.

## 2. Goals

1. Let applications declare binary, payload-bearing graph edges while retaining
   all normal item semantics.
2. Support concrete, one-of, category-constrained, and fully erased endpoint
   requirements.
3. Let downstream crates invent categories without changes to Myko core.
4. Preserve distinct A/B positions for same-type ends, direction, self-loop
   policy, parallel edges, and optional pair uniqueness.
5. Support indexed endpoint qualifiers without treating qualifier values as
   entities.
6. Generate forward, reverse, qualified, and optional exact-pair APIs.
7. Provide opt-in eager adjacency for cold lookup and traversal workloads while
   preserving demand-driven indexing as a valid default.
8. Expose edge and category schema through backend-neutral typegen.
9. Allow applications to adopt the features type-by-type after upgrading to
   Myko 6, with no flag-day data migration.
10. Establish a path to consolidate edge-specific query and cascade indexes only
    after equivalence tests and benchmarks.

## 3. Non-goals

1. A separate `EdgeStore`, `AnyEdge`, edge event kind, or persistence table.
2. Automatically reclassifying every multi-`belongs_to` item as an edge.
3. Replacing `belongs_to`, `owns_many`, or `ensure_for`.
4. Making arbitrary transitive closure cheap or automatically reactive.
5. Providing global DAG, cycle, or reachability constraints without explicit
   application logic and coordination.
6. Treating qualifier values as independently persistent or referentially
   complete entities.
7. Solving generalized hyperedges. Items with three or more semantic
   endpoints remain ordinary items until a separate design establishes useful
   APIs and non-combinatorial indexing.
8. Generating a permanently closed cross-language union for an open-world
   category.

## 4. Terminology

- **Item / vertex**: an ordinary Myko item that may participate in edges.
- **Edge item**: an ordinary item with an additional `EdgeRegistration`.
- **End**: position A or B in an edge's `Directed<A, B>` or `Undirected<A, B>`
  shape.
- **Concrete endpoint**: constrained to one item type.
- **Polymorphic endpoint**: stored as an erased item reference and constrained
  by one-of, category, or any-item rules.
- **Entity category**: a downstream-defined, type-level set of eligible item
  types, visible in runtime and typegen registrations.
- **Qualifier**: a scalar address component within an endpoint item, such as a
  port, slot, lane, or channel ID.
- **Adjacency projection**: derived forward/reverse/pair indexes over canonical
  edge item state.
- **Pair uniqueness**: at most one edge of a type for a given qualified or
  unqualified endpoint pair.

## 5. Compatibility boundary

An edge remains an item. The following remain canonical and unchanged:

```text
MEvent { event_type: SET | DEL, item_type, item }
StoreRegistry[item_type] -> EntityStore
persistence key = (item_type, item_id)
wire item = { itemType, item }
```

Edge identity is obtained from registrations at runtime. No `itemKind` field is
required in stored or wire records.

An existing item can opt in by adding metadata that references its existing
fields. Its serialized field names, item type, ID, event history, persistence
rows, ordinary CRUD, and generated query types must stay identical.

Separate inventory registrations are required rather than adding mandatory
fields to public registration structs. Adding a field to `ItemRegistration` or
`TypegenCatalog` would break downstream struct literals and previously expanded
code. `GraphSchemaCatalog` is therefore collected separately and passed beside
an ordinary `TypegenCatalog`; it is not a new required field on that catalog.

The additive guarantee has two explicit levels:

1. An application with no edge or category registrations keeps its existing
   mutation path and runtime behavior. The empty graph registry must have a
   single fast-path check and must not install stores, projections, locks, or
   reactive subscriptions.
2. Annotating an existing item as an edge preserves its serialized shape,
   identity, persistence history, and ordinary generated APIs. The application
   is deliberately opting into the declared graph validation, indexing, and
   endpoint-lifecycle behavior; those new checks are not claimed to be
   behaviorally invisible for that item.

Myko's existing single-item and batch paths currently disagree about
reduce/persist ordering. Normalizing non-graph mutations is desirable, but it
is a separate runtime change with its own tests and release note, not a hidden
prerequisite of this additive graph feature.

## 6. Downstream-defined entity categories

An **entity category** is an open, downstream-defined set of item types eligible
for a graph-end position. It supplies no methods, behavior, or handler
authority. It is distinct from Myko's sealed handler-context capabilities such
as querying and event publishing.

Categories are many-to-many: an item type may belong to any number of categories,
and a category may contain item types registered by independently built crates.
Membership is type-level. Instance-specific eligibility remains the edge
validator's responsibility.

### 6.1 Definition and identity

A category is a real Rust marker type:

```rust
#[myko_category]
pub struct TagTarget;
```

The attribute automatically emits its inventory registration. Its display name
is `stringify!(TagTarget)`, and its qualified registry identity is derived from
`module_path!()` plus the type name. The API accepts no manually typed category
name or ID. Renaming or moving a category is an explicit schema migration.

Expansion implements:

```rust
pub trait EntityCategory: Send + Sync + 'static {
    const ID: &'static str;
    const NAME: &'static str;
}
```

Category IDs are used for inventory aggregation and typegen. Persisted edges
store endpoint item types and IDs, not category IDs. Rust `TypeId` is never a
stable category identity.

### 6.2 Many-to-many membership

Items opt into one or more categories with a focused attribute:

```rust
#[myko_in(TagTarget, SearchTarget, LinkTarget)]
#[myko_item]
pub struct Article {
    pub title: Arc<str>,
}
```

The attribute accepts category Rust types only and emits both static membership
and erased runtime registrations:

```rust
pub trait InCategory<C: EntityCategory>: Eventable {}

impl InCategory<TagTarget> for Article {}
impl InCategory<SearchTarget> for Article {}
impl InCategory<LinkTarget> for Article {}

pub struct ItemCategoryRegistration {
    pub item_type: &'static str,
    pub entity_category_id: &'static str,
    pub crate_path: &'static str,
}
```

Duplicate membership is rejected by Rust coherence or startup registration
validation. Categories do not implicitly inherit from one another. Every name
and registry ID is derived from a referenced Rust type.

### 6.3 Eligibility is not authorization

Category membership means instances of a type are structurally eligible for a
particular edge end. It does not say that an instance exists, is valid for this
edge, or may be referenced by the caller. Authoritative edge creation checks:

1. the item type is registered;
2. the type belongs to the required category;
3. the referenced instance exists, when required by policy;
4. domain validation and authorization accept the operation.

Both ends may independently be concrete, category-constrained, selected from a
closed type set, or unrestricted erased item references. A category does not by
itself name the opposite end or allowed edge type; the edge's `Ends` type defines
that compatibility. The same category can therefore participate in several edge
schemas without coupling those schemas together.

## 7. Stable erased item references

Polymorphic endpoints use a serializable Myko identity:

```rust
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityRef {
    pub entity_type: Arc<str>,
    pub id: Arc<str>,
}
```

`EntityRef` uses Myko's registered item type identity, never Rust `TypeId`.
Typed conversions prevent ordinary Rust callers from constructing strings:

```rust
impl<T> From<&T> for EntityRef
where
    T: Eventable + WithTypedId,
{
    fn from(item: &T) -> Self {
        Self::new(T::ENTITY_NAME_STATIC, item.id())
    }
}
```

A generated typed API may enforce category membership:

```rust
fn attach_tag<T>(tag: TagId, target: &T) -> Result<TagAssignmentId>
where
    T: AnyItem + InCategory<TagTarget>;
```

Dynamic clients use `EntityRef`; server validation consults the category
registry.

## 8. Edge declaration

### 8.1 Direction is part of the edge shape

`#[myko_item]` remains responsible for ordinary item generation. Edge semantics
are declared by a `GraphEdge` implementation; `#[myko_edge]` automatically emits
its runtime and typegen registration.

The `Ends` associated type encodes direction and both end requirements together.
The core trait contract is:

```rust
pub trait GraphEdge: Eventable + Sized {
    type Ends: EdgeEnds;
    type Scope: EdgeScope;
    type Validator: EdgeValidator<Self>;

    fn ends(&self) -> <Self::Ends as EdgeEnds>::Values;

    fn scope(&self) -> Option<<Self::Scope as EdgeScope>::Value> {
        None
    }

    const PAIR_POLICY: PairPolicy = PairPolicy::Parallel;
    const PAIR_PROJECTION: PairProjectionPolicy =
        PairProjectionPolicy::IntersectAdjacency;
    const ADJACENCY: AdjacencyPolicy = AdjacencyPolicy::DemandDriven;
    const SELF_LOOPS: SelfLoopPolicy = SelfLoopPolicy::Allow;
    const A_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::CascadeEdge;
    const B_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::CascadeEdge;
}
```

`Directed<A, B>` and `Undirected<A, B>` implement `EdgeEnds` when A and B
implement `EndpointSpec`; their `Values` type is `(A::Value, B::Value)`.
`#[myko_edge]` inserts `Scope = NoScope` and `Validator = NoEdgeValidator` when
the attributed impl omits them. This keeps stable Rust compatibility without
making callers repeat framework defaults. `NoScope::Value` is `()`, and the
default `scope()` returns `None`; a non-empty scope type overrides it.

A concrete edge then reads:

```rust
#[myko_item]
pub struct Membership {
    #[belongs_to(Group)]
    pub group_id: GroupId,

    #[belongs_to(Member)]
    pub member_id: MemberId,

    pub permission: Permission,
}

#[myko_edge]
impl GraphEdge for Membership {
    type Ends = Directed<ConcreteEndpoint<Group>, ConcreteEndpoint<Member>>;

    fn ends(&self) -> (GroupId, MemberId) {
        (self.group_id.clone(), self.member_id.clone())
    }

    const PAIR_POLICY: PairPolicy = PairPolicy::Parallel;
    const ADJACENCY: AdjacencyPolicy = AdjacencyPolicy::DemandDriven;
}
```

`Directed<A, B>` means A to B. `Undirected<A, B>` assigns no directional meaning
to the positions. Direction cannot be omitted or contradicted by another
constant. Internally the positions are A and B; the public API does not require
every domain to call them source and target.

The trait supplies defaults for pair, adjacency, self-loop, and deletion
policies. `#[myko_edge]` derives registration metadata from the implementation.
There is no separate registration call, nested attribute grammar, manually typed
role, item name, category name, or field name.

### 8.2 End requirements are independent types

Either end may independently use any supported requirement:

```rust
ConcreteEndpoint<Article>
OneOfEndpoint<(Article, Image)>
CategoryEndpoint<TagTarget>
AnyItemEndpoint
QualifiedEndpoint<Article, PortId>
```

The pair wrapper composes them freely:

```rust
Directed<ConcreteEndpoint<Tag>, CategoryEndpoint<TagTarget>>
Directed<CategoryEndpoint<LinkOrigin>, CategoryEndpoint<LinkTarget>>
Undirected<CategoryEndpoint<NetworkParticipant>, CategoryEndpoint<NetworkParticipant>>
Directed<AnyItemEndpoint, CategoryEndpoint<AuditTarget>>
```

The second example erases both ends while constraining each to a different open
category. Categories are not inherently tied to the receiving side of a connection; they
classify eligibility for whichever end references them.

The typed requirements lower into neutral runtime metadata:

```rust
pub enum EndpointRequirement {
    Concrete(&'static str),
    OneOf(&'static [&'static str]),
    Category(&'static str),
    AnyRegisteredItem,
}
```

All strings in this erased representation are generated from registered Rust
types. `AnyItemEndpoint` is an explicit escape hatch; a category-constrained end
preserves more schema intent.

### 8.3 Category-constrained erased ends

```rust
#[myko_category]
pub struct TagTarget;

#[myko_item]
pub struct Tag {
    pub name: Arc<str>,
}

#[myko_item]
pub struct TagAssignment {
    #[belongs_to(Tag)]
    pub tag_id: TagId,
    pub target: EntityRef,
    pub attached_at: DateTime<Utc>,
}

#[myko_edge]
impl GraphEdge for TagAssignment {
    type Ends = Directed<ConcreteEndpoint<Tag>, CategoryEndpoint<TagTarget>>;

    fn ends(&self) -> (TagId, EntityRef) {
        (self.tag_id.clone(), self.target.clone())
    }

    const A_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::CascadeEdge;
    const B_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::CascadeEdge;
    const PAIR_POLICY: PairPolicy = PairPolicy::Unique;
}
```

`TagTarget` is downstream-defined. Myko supplies only category membership,
erased identity, validation, and graph projection machinery.

Both ends can be erased:

```rust
#[myko_category]
pub struct LinkOrigin;

#[myko_category]
pub struct LinkTarget;

#[myko_item]
pub struct TypedLink {
    pub origin: EntityRef,
    pub target: EntityRef,
}

#[myko_edge]
impl GraphEdge for TypedLink {
    type Ends = Directed<
        CategoryEndpoint<LinkOrigin>,
        CategoryEndpoint<LinkTarget>,
    >;

    fn ends(&self) -> (EntityRef, EntityRef) {
        (self.origin.clone(), self.target.clone())
    }
}
```

An item may belong to `LinkOrigin`, `LinkTarget`, both, or neither, alongside any
number of unrelated categories.

### 8.4 Undirected edges

```rust
#[myko_edge]
impl GraphEdge for Friendship {
    type Ends = Undirected<
        CategoryEndpoint<NetworkParticipant>,
        CategoryEndpoint<NetworkParticipant>,
    >;

    fn ends(&self) -> (EntityRef, EntityRef) {
        (self.person_a.clone(), self.person_b.clone())
    }
}
```

Undirected ends must have compatible requirement, qualifier, validation, and
deletion schemas because pair canonicalization may exchange A and B. Startup
registration rejects an asymmetric `Undirected<A, B>` schema. The stored item
fields retain their original order; canonicalization affects graph keys only.

### 8.5 Pair policy

```rust
pub enum PairPolicy {
    Parallel,
    Unique,
}
```

Parallel edges are the default. Edge identity remains the ordinary item ID.
Endpoint-derived identity is an application-level strategy for set-like edge
types and is not introduced by graph registration.

For `PairPolicy::Unique`, the complete end addresses—including qualifier
values—form the unique pair. `Undirected` canonicalizes the two addresses;
`Directed` preserves A-to-B order.

### 8.6 End deletion policy

```rust
pub enum EndpointDeletePolicy {
    CascadeEdge,
    RestrictEndpointDelete,
    RetainDangling,
}
```

`CascadeEdge` deletes the incident edge only. `RestrictEndpointDelete` requires
an authoritative incident-edge index in the command's consistency boundary.
`RetainDangling` preserves an edge whose referenced endpoint no longer exists.
Policies are configured independently as `A_DELETE` and `B_DELETE`; directed
semantics do not change their enforcement.

## 9. Endpoint qualifiers

### 9.1 Typed qualifier model

A qualifier is a scalar component of an endpoint address. It is represented by
a Rust type and accessor, not a field-name or index-name string.

```rust
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize, TS)]
pub struct PortId(pub Arc<str>);

impl EndpointQualifier for PortId {}

#[myko_item]
pub struct WorkflowConnection {
    #[belongs_to(WorkflowNode)]
    pub source_node_id: WorkflowNodeId,
    pub source_port_id: PortId,

    #[belongs_to(WorkflowNode)]
    pub target_node_id: WorkflowNodeId,
    pub target_port_id: PortId,
}

#[myko_edge]
impl GraphEdge for WorkflowConnection {
    type Ends = Directed<
        QualifiedEndpoint<WorkflowNode, PortId>,
        QualifiedEndpoint<WorkflowNode, PortId>,
    >;
    type Validator = ValidateWorkflowConnection;

    fn ends(
        &self,
    ) -> ((WorkflowNodeId, PortId), (WorkflowNodeId, PortId)) {
        (
            (self.source_node_id.clone(), self.source_port_id.clone()),
            (self.target_node_id.clone(), self.target_port_id.clone()),
        )
    }
}

```

`WorkflowNode` is the referential endpoint. `PortId` addresses something defined
by the node's schema and is not a Myko item.

The qualifier's schema name comes from `PortId`; its stable index encoding comes
from `EndpointQualifier`. No caller writes `qualifier = "port"`,
`field = "sourcePortId"`, or an index-name string.

### 9.2 Projection rule

For an endpoint with no qualifier, Myko exposes the entity-level adjacency
projection. For an endpoint with one qualifier, Myko automatically exposes both:

```text
entity
(entity, qualifier)
```

This directly supports broad node adjacency and narrow port adjacency without a
configuration grammar.

Each endpoint supports zero or one qualifier value.
Multiple address components may be wrapped in one strongly typed qualifier
struct:

```rust
pub struct SocketAddress {
    pub port: PortId,
    pub channel: ChannelId,
}

impl EndpointQualifier for SocketAddress {}
```

That composite is indexed as one canonical value. Prefix queries inside a
composite qualifier, such as `(entity, port)` without its channel, are outside
the binary-edge API. The design does not enumerate string field lists or every
field subset.

### 9.3 Erased registration

The typed endpoint lowers to generated extractors:

```rust
pub struct EdgeEndpointRegistration {
    pub position: EndPosition,
    pub requirement: EndpointRequirement,
    pub extract_entity: fn(&dyn Any) -> Option<EntityRef>,
    pub qualifier: Option<EndpointQualifierRegistration>,
    pub on_delete: EndpointDeletePolicy,
}

pub struct EndpointQualifierRegistration {
    pub qualifier_type: &'static str,
    pub extract: fn(&dyn Any) -> Option<IndexValue>,
}
```

`position`, item/category IDs, and `qualifier_type` are generated from
the trait's Rust types. The erased form contains strings only because it must
cross inventory/typegen/runtime boundaries.

`IndexValue` has canonical equality, hashing, and serialization. Supported
qualifiers are string/ID-like newtypes and typed composites that encode into
canonical CBOR bytes. Floating-point qualifiers are excluded.

### 9.4 Qualifier validation and lifecycle

A qualifier extractor proves only that an edge contains a value. It cannot
prove that the referenced item's current schema defines that port or slot.
Applications register a typed validator through the `GraphEdge` trait:

```rust
impl EdgeValidator<WorkflowConnection> for ValidateWorkflowConnection {
    fn validate(
        ctx: &EdgeValidationContext<'_>,
        edge: &WorkflowConnection,
    ) -> Result<()> {
        let source = ctx.get(&edge.source_node_id)?;
        let target = ctx.get(&edge.target_node_id)?;

        ensure!(source.schema().has_output(&edge.source_port_id));
        ensure!(target.schema().has_input(&edge.target_port_id));
        Ok(())
    }
}
```

The read-only `EdgeValidationContext` exposes only the operations needed for
validation. It is not a raw `MykoServerContext` and cannot publish events or
mutate graph projections.

Schema changes can remove a qualifier without deleting the endpoint item. The
framework validates new/changed authoritative edges, preserves historical edges
during replay, exposes invalid-edge diagnostics, and leaves automatic
repair/deletion to an explicit reconciler. Silent cascade on schema changes is
unsafe.

### 9.5 Endpoint movement

Changing an endpoint entity or qualifier changes topology. The graph projection
processes it as removal of the old address plus insertion of the new address.
Domain APIs should normally expose this as disconnect/connect even if the
canonical SET event contains an update.

## 10. Runtime registrations

Edge metadata is separate from `ItemRegistration`. Typed `Directed` and
`Undirected` shapes lower to:

```rust
pub enum EdgeShapeKind {
    Directed,
    Undirected,
}

pub enum EndPosition {
    A,
    B,
}

pub type ErasedEdgeValidator = for<'a> fn(
    &EdgeValidationContext<'a>,
    &dyn AnyItem,
) -> Result<()>;

pub struct EdgeRegistration {
    pub edge_type: &'static str,
    pub crate_path: &'static str,
    pub shape: EdgeShapeKind,
    pub pair_policy: PairPolicy,
    pub pair_projection: PairProjectionPolicy,
    pub endpoints: &'static [EdgeEndpointRegistration; 2],
    pub scope: Option<EdgeScopeRegistration>,
    pub adjacency: AdjacencyPolicy,
    pub self_loops: SelfLoopPolicy,
    pub validate: Option<ErasedEdgeValidator>,
}
```

The registration is inventory-collected and discoverable by edge type.
`#[myko_edge]` generates an erased validator trampoline that downcasts the
`AnyItem` to the declared edge type and calls `E::Validator`; a downcast failure
is a registration invariant error. `NoEdgeValidator` lowers to `None`. Macro
expansion continues to emit ordinary `ItemRegistration`, query, relationship,
and typegen registrations.

Scope is metadata and an optional projection component, not automatically a
third graph endpoint. It follows the same type-driven rule as endpoints:

```rust
#[myko_edge]
impl GraphEdge for DocumentLink {
    type Ends = Directed<
        ConcreteEndpoint<Document>,
        ConcreteEndpoint<Document>,
    >;
    type Scope = ConcreteScope<Workspace>;

    fn scope(&self) -> Option<WorkspaceId> {
        Some(self.workspace_id.clone())
    }

    // ends() omitted
}
```

`NoScope` is the default associated scope type. `#[myko_edge]` generates
the erased scope extractor and registered type identity; the caller supplies no
field-name or item-name strings. A scoped adjacency key can prevent cross-tenant
or cross-document collisions and bound traversal.

## 11. Graph index

### 11.1 Keys

IDs are not globally unique across stores, so every node key includes type:

```rust
#[derive(Clone, Hash, Eq, PartialEq)]
pub struct NodeKey {
    pub entity_type: Arc<str>,
    pub id: Arc<str>,
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct EdgeKey {
    pub edge_type: Arc<str>,
    pub id: Arc<str>,
}
```

A qualified address adds role-specific qualifier values:

```rust
pub struct EndpointAddress {
    pub node: NodeKey,
    pub qualifiers: SmallVec<[IndexValue; 2]>,
}
```

### 11.2 Logical projections

Each edge-type/scope shard holds one coherent state:

```rust
pub struct GraphShardState {
    generation: u64,
    outgoing: HashMap<NodeKey, EdgeIdSet>,
    incoming: HashMap<NodeKey, EdgeIdSet>,
    endpoints_by_edge: HashMap<EdgeKey, EdgeEndpoints>,
    by_pair: Option<HashMap<PairKey, EdgeIdSet>>,
    qualified_outgoing: HashMap<EndpointAddress, EdgeIdSet>,
    qualified_incoming: HashMap<EndpointAddress, EdgeIdSet>,
}
```

Incidence and pair buckets keep one edge ID inline and promote to a sorted set
only when a second distinct edge arrives. Removal demotes a set back to the
inline form. This preserves deterministic results without paying for a tree
allocation in the common singleton-bucket case.

`by_pair` is present for `PairProjectionPolicy::Eager`. Qualified maps are
populated when the associated endpoint type has a qualifier. Undirected edges
require identical endpoint schemas and canonicalize complete endpoint addresses
for pair lookup while retaining incidence from both endpoints.

The containing graph index maps shard keys to `Arc<RwLock<GraphShardState>>`.
A mutation spanning two scope shards acquires both shard locks in canonical
order. Physical layout changes must preserve the coherent snapshot and
publication contract in section 11.4 and pass the benchmark plan.

### 11.3 Adjacency policies

```rust
pub enum AdjacencyPolicy {
    DemandDriven,
    Eager,
}

pub enum PairProjectionPolicy {
    IntersectAdjacency,
    Eager,
}
```

- **DemandDriven** preserves the current weak bucket behavior and lowest idle
  memory.
- **Eager** retains forward/reverse adjacency for cold lookup and traversal.

`GraphEdge::PAIR_PROJECTION` lowers directly to
`EdgeRegistration::pair_projection`. `PairProjectionPolicy::IntersectAdjacency`
implements `between` by intersecting or filtering the smaller incidence set.
`PairProjectionPolicy::Eager` maintains a direct pair projection. `PairPolicy::Unique` always maintains the minimal
authoritative reservation map required for enforcement, regardless of the read
projection choice. Semantic uniqueness and physical read optimization remain
separate.

Qualifier projections follow the endpoint type automatically.

### 11.4 Coherent update and publication

The index is derived state. The canonical mutation remains the item SET/DEL.
Each edge-type/scope shard owns one `RwLock<GraphShardState>` containing its
outgoing, incoming, endpoint, pair, and qualifier maps plus a monotonically
increasing in-memory generation. A SET captures old topology before canonical
replacement, then applies removal and insertion to all maps under one shard
write lock. Direct readers hold the shard read lock and therefore cannot observe
half an update.

Reactive adjacency cells publish the resulting diffs through one Hyphae batch
after the locked state is coherent. Every diff carries the committed shard
generation. Live readers initialize from a locked snapshot and then accept only
later generations, closing the snapshot/subscribe race. The index exposes
`Building { watermark }`, `Ready { generation }`, and `Failed` readiness; graph
APIs do not represent a partial rebuild as complete.

This is a correctness-first physical design. Sharding and lock granularity may
be optimized after profiling, but separate independently readable DashMaps do
not satisfy the publication guarantee.

Projection handlers are idempotent for replay and duplicate delivery. A
projection can be rebuilt entirely from canonical edge stores. Because current
canonical events do not provide one durable global sequence, rebuild obtains a
settled store snapshot under the server's mutation barrier, records its local
watermark, then admits subsequent mutations through the same coordinator.
Federated ordering guarantees remain those of canonical Myko items rather than
a new global graph sequence.

### 11.5 Index consolidation

Graph adjacency is maintained alongside existing `RelationshipManager` and
`BelongsToSourceIndex` structures, with shadow comparisons proving equivalent
endpoint membership.

Only after validation may edge queries and cascades share the graph index and
retire duplicate edge-specific FK structures. Generic non-edge `belongs_to`
behavior remains unchanged.

## 12. Query and traversal APIs

### 12.1 Typed one-hop APIs

The edge shape determines the API vocabulary. `Directed<A, B>` exposes:

```rust
ctx.edges::<Membership>().from(&group_id);
ctx.edges::<Membership>().to(&member_id);
ctx.edges::<Membership>().between(&group_id, &member_id);
```

`Undirected<A, B>` exposes:

```rust
ctx.edges::<Friendship>().incident(&person_id);
ctx.edges::<Friendship>().between(&person_a_id, &person_b_id);
```

Qualified lookups take generated strongly typed addresses:

```rust
ctx.edges::<WorkflowConnection>()
    .from_at(&WorkflowConnectionAAddress {
        node_id: node_id.clone(),
        qualifier: port_id.clone(),
    });

pub struct WorkflowConnectionAAddress {
    pub node_id: WorkflowNodeId,
    pub qualifier: PortId,
}
```

A/B appear only where a neutral positional name is unavoidable, such as a
generated address type. Directed operations use from/to; undirected operations
use incident/between. There are no manually supplied role or qualifier names.

### 12.2 Reactive APIs

```rust
ctx.edges::<TagAssignment>().watch_from(&tag_id);
ctx.edges::<TagAssignment>().watch_to(&target_ref);
ctx.edges::<WorkflowConnection>().watch_to_at(&address);
ctx.edges::<Friendship>().watch_incident(&person_id);
```

For an eagerly projected endpoint, watch construction subscribes while holding
the graph authority barrier and seeds its initial `CellMap` from the adjacency
bucket. This closes the subscribe/snapshot race without scanning the canonical
edge store. Demand-driven endpoints preserve scan-equivalent initialization;
both plans route later canonical diffs incrementally by their old and new
endpoints.

One-shot and reactive APIs return edge items so payload remains available and
ordinary Myko semantics remain explicit.

### 12.3 Traversal

Bounded traversal uses the following builder:

```rust
ctx.traverse::<WorkflowConnection>()
    .start(node_id)
    .direction(Direction::Forward)
    .within_scope(workflow_id)
    .max_depth(8)
    .max_nodes(10_000)
    .execute();
```

Traversal complexity remains proportional to visited nodes and incident edges.
High-degree hubs and large result sets remain expensive. APIs require explicit
bounds and must not imply arbitrary reactive transitive closure is free.

Live reachability is a separate algorithmic feature. Edge removal, cycles, and
multiple supporting paths require reference counts or recomputation. It should
not be silently implemented as a chain of one-hop subscriptions.

Graph reads participate in Myko's existing capability-scoped handler contexts.
Core defines a sealed `GraphQuerying: ServerScoped` handler authority, with the
same native/wasm signature surface as existing context capabilities. It is
implemented exactly where `Querying` is implemented; this avoids granting
new read reach to a handler kind that cannot already query canonical items.
Compile-fail matrix tests prove other contexts cannot call graph reads, and no
graph read handle exposes mutation. Downstream entity categories such as
`TagTarget` cannot implement or grant `GraphQuerying`.

Graph mutation remains ordinary command/event publication through the mandatory
edge enforcement funnel, not an unscoped index mutation API. Typed validators
receive only `EdgeValidationContext`, never raw `MykoServerContext`.

### 12.4 Query planner behavior

For an edge query, the planner chooses in order:

1. exact qualified pair projection, if fully bound and present;
2. exact unqualified pair projection;
3. narrowest fully bound endpoint qualifier projection;
4. endpoint adjacency;
5. existing demand-driven `belongs_to` route;
6. full-store scan with residual filtering.

Plan selection and fallback must be observable in diagnostics and benchmarks.

## 13. Mutation enforcement, invariants, and concurrency

### 13.1 One mandatory mutation funnel

Edge invariants apply regardless of entry point. Typed SET, erased SET, generated
CRUD, batches, WebSocket commands, and other authoritative local mutations all
consult `EdgeRegistration`. A non-participating item takes the existing fast
path unchanged. A participating edge, or an endpoint whose deletion has graph
policy, passes through the graph coordinator; no public authoritative mutation
path may bypass its preflight.

The hook receives mutation mode plus old and new canonical values:

```rust
pub enum EdgeApplyMode {
    Authoritative,
    Replay,
    Import,
    Federated,
    Observe,
}

pub struct EdgeMutation<'a> {
    pub mode: EdgeApplyMode,
    pub old: Option<&'a dyn AnyItem>,
    pub new: Option<&'a dyn AnyItem>,
}
```

`Authoritative` mode enforces endpoint types, entity categories, existence,
self-loop policy, pair uniqueness, and the typed application validator. Replay,
import, and federated modes preserve canonical history and convergence; they
verify and diagnose invalid/dangling topology rather than silently dropping or
rewriting events. `Observe` builds shadow projections without changing mutation
acceptance and is the safe first mode for annotating existing item types.

Endpoint DEL passes through the same coordinator before persistence so
`RestrictEndpointDelete` and incident-edge cascades cannot be bypassed.

For a participating authoritative mutation, the coordinator orders work as:

1. acquire the required canonical pair/shard locks and run preflight;
2. open the Hyphae mutation batch and reduce canonical item state;
3. update the derived graph projection to the same generation;
4. enqueue persistence while the reactive batch is still closed;
5. close the batch, publish reactive diffs, and drain relationship/cascade and
   other downstream effects through the guarded causal work queue;
6. release the mutation locks after canonical state, projection state, and the
   persistence enqueue agree.

This is intentionally **reduce, then persist**: accepted state becomes locally
canonical immediately. Persistence is nevertheless enqueued before reactive
query or relationship work can spin or delay it during a large import. A
configuration with an acknowledged durability barrier may delay command
acknowledgement, but it must not delay local reduction or graph publication.

### 13.2 Causal loop protection

Reduce-before-effects is part of loop safety, not only a latency choice. The
existing relationship rules remain foundational:

- a DEL removes the item before its cascades run, so canonical state is the
  visited set and a delete cycle converges when it reaches an already absent
  item;
- bookkeeping mutations equivalent to `RelationshipFixup` do not start another
  structural cascade;
- statically detectable non-convergent creation cycles, such as recursive
  `ensure_for` schemas that mint fresh IDs, are rejected at registration.

Graph and relationship effects additionally run through one transaction-scoped
causal work queue rather than unbounded recursive publication. Every derived
mutation inherits a root transaction ID and records its cause, depth, and effect
kind. The queue:

1. suppresses an exact repeated transition identified by item type, item ID,
   operation, and canonical content hash within the same root transaction;
2. bounds causal depth and total derived mutations independently;
3. preserves FIFO order within a root transaction while allowing independent
   roots to proceed under the normal mutation locks;
4. emits the complete causal chain and offending transition when a duplicate or
   budget breach is detected.

Exact-transition suppression does not treat every second write to an item as a
loop: a genuinely different canonical value may progress. Depth and total-work
budgets catch alternating or ever-changing cycles that never repeat an exact
value. Synchronously derived mutations must propagate the causal token across
relationship, graph, saga, and reactive-effect entry points; a mutation arriving
later from an external client is a new root transaction.

When a budget is exhausted, the coordinator stops scheduling further derived
effects, returns a structured loop-protection error, and marks the causal chain
in diagnostics. It does not roll back canonical mutations that were already
reduced and enqueued for persistence. Operators can therefore distinguish a
bounded partial cascade from an unavailable or silently spinning server.

### 13.3 Authoritative uniqueness mechanism

`PairPolicy::Unique` uses a mutation-authority reservation keyed by the complete
canonical pair:

```text
(edge type, optional scope, A address, B address)
```

The pair includes qualifier values. Undirected edges canonicalize complete
addresses only when A and B schemas are compatible; asymmetric end
requirements are invalid for undirected edges.

The server owns a striped pair-lock table. An authoritative single mutation
acquires its pair lock; a pair-changing SET acquires old and new locks in sorted
canonical order. A batch computes all affected keys, sorts/deduplicates them,
and acquires them before validation. While held, preflight checks the canonical
edge store plus other mutations in the same batch, reduces the canonical
stores, updates graph projection state, and enqueues persistence before the
reactive batch drains. Locks release only after canonical state, projection
state, and the persistence enqueue agree.

A persistence-enqueue failure occurs after local reduction. It returns an error
without rolling back already visible canonical state; the mutation remains the
uniqueness authority, and persistence health/retry machinery must expose and
repair the durability gap. Graph projection updates are derived and infallible;
a detected projection invariant violation marks that edge index unready and
schedules rebuild rather than rolling back canonical state.

This mechanism supports independent edge IDs and parallel payload-rich records
while enforcing unique topology. A deterministic pair ID remains an optional
idempotency strategy for new edge types, not the uniqueness authority.

### 13.4 Application commands

Myko does not generate a universal `Connect<Edge>` command because it cannot
construct an arbitrary payload-bearing edge. Applications construct and
validate typed edge items through normal commands. Myko generates lookup and
disconnect helpers, which require no payload factory.

Runtime-dependent cardinality remains in the typed application validator; Myko
cannot infer it solely from static schema metadata.

### 13.5 Self-loops

```rust
pub enum SelfLoopPolicy {
    Allow,
    Reject,
}
```

`GraphEdge::SELF_LOOPS` defaults to `Allow` for additive compatibility and lowers
to `EdgeRegistration::self_loops`. Authoritative preflight applies it to the
complete typed end addresses before uniqueness reservation.

Same-type A/B positions remain distinct during validation and index updates. A
self-loop must not be counted, cascaded, or deleted twice merely because it is
present through both directions.

## 14. Deletion, orphan handling, and history

Endpoint deletion operates on incident edges according to registration policy.
Eager edge types plan directly from their entity-incidence projection, making
planning proportional to endpoint degree. Demand-driven edge types retain a
canonical-store scan fallback rather than paying idle incidence memory. The
cascade plan is deduplicated by `(edge type, edge ID)`, and any incident
`RestrictEndpointDelete` aborts before canonical reduction even when the same
self-loop is cascade-eligible through its other role.
For high-degree nodes, per-edge cascade can create a large event storm. The
framework must retain explicit, auditable DEL events when individual edge
history matters. Cascades use configured chunking, fan-out limits, and
backpressure controls.

Existing `#[belongs_to]` registrations always carry their current cascade/orphan
semantics. An edge endpoint backed by such a field is therefore compatible only
with `CascadeEdge` during the additive phases. Macro/startup validation rejects
`RestrictEndpointDelete` or `RetainDangling` on the same endpoint while its field
also participates in `belongs_to`; otherwise the relationship manager could
silently cascade before graph policy runs. Non-cascading concrete references use
a typed ID field plus the `GraphEdge` endpoint extractor without `belongs_to`.
Erased `EntityRef` endpoints are enforced solely by graph metadata.

`RestrictEndpointDelete` is accepted only with an eager authoritative incidence
index and the mutation coordinator enabled. Demand-driven weak buckets cannot
authoritatively prove that no incident edge exists.

Startup/replay behavior:

1. canonical items load normally;
2. edge registrations identify edge stores;
3. adjacency is rebuilt or restored from a verified checkpoint;
4. dangling endpoints are reported or reconciled according to policy;
5. live graph APIs become ready only after the projection reaches the canonical
   store version.

Historical edges with `RetainDangling` remain queryable by endpoint reference
even if the endpoint item no longer exists.

## 15. Distribution and consistency

An edge should be appended once as canonical state, not independently appended
to both endpoint streams unless the persistence layer supplies a real atomic
multi-stream transaction. Forward and reverse adjacency are projections with
idempotent event/version processing.

Partitioning tradeoffs remain explicit:

- partition by directed A/from end improves outgoing locality;
- partition by edge/pair balances edge writes;
- incoming traversal may require a reverse projection;
- high-degree nodes can become hot keys;
- polymorphic endpoints may reside on different authorities.

`RestrictEndpointDelete` requires authoritative knowledge of incident edges. It
must not rely on an eventually consistent remote projection. Cross-authority edges use `RetainDangling`; cascade and restrict policies require
both endpoints and the edge mutation authority to share an authoritative
consistency boundary.

## 16. Type generation

Graph schema is represented in backend-neutral catalogs:

```rust
pub struct GraphSchemaCatalog {
    pub entity_categories: Vec<&'static EntityCategoryRegistration>,
    pub item_categories: Vec<&'static ItemCategoryRegistration>,
    pub edges: Vec<&'static EdgeRegistration>,
}
```

`GraphSchemaCatalog::collect*` mirrors the crate/group selection rules of
`TypegenCatalog::collect*`. Renderers receive the two catalogs as separate
inputs (or through a new wrapper type) so no required field is added to the
existing public `TypegenCatalog` struct.

Renderers consume end requirements, A/B positions, `Directed`/`Undirected`
shape, qualifier types, pair policy, and available projections. No
renderer-specific callback belongs in these neutral records.

The stable erased cross-language shape is `EntityRef { entity_type, id }`. An
aggregate application catalog may additionally generate a closed convenience
union for the category members known to that aggregate. The open `EntityRef`
remains canonical because independently built crates can add category
memberships.

An edge continues to appear in ordinary generated item/query exports. Graph
metadata and helpers are additive and do not remove or rename existing item
APIs.

## 17. Observability

Expose at least:

- registered edge and category schemas;
- adjacency policy per edge type;
- edge count and retained index entries;
- broad and qualified bucket counts;
- cold backfill count and duration;
- query plan selected;
- projection rebuild duration and version/lag;
- invalid endpoint and qualifier counts;
- cascade size and duration;
- causal depth, derived-mutation count, duplicate-transition suppression, and
  loop-budget exhaustion;
- uniqueness/cardinality rejection counts.

These metrics are necessary to decide whether eager adjacency is an efficiency
win for a workload.

## 18. Security and validation

An erased `entityType` is untrusted wire input. The server must:

1. resolve it through registered item metadata rather than arbitrary type
   construction;
2. validate endpoint category or one-of constraints;
3. validate item existence according to policy;
4. apply caller authorization to both endpoints and the edge command;
5. prevent cross-scope references unless allowed;
6. bound traversal depth, node count, and result size;
7. avoid leaking the existence of unauthorized endpoint items through errors or
   adjacency results.

Qualifier values are also untrusted and require application validation where
they refer to schema-defined ports or slots.

Strict validation applies at the authoritative command boundary. Replay,
import, and federated delivery may observe an edge before its endpoint because
of ordering rather than invalid canonical history. Those apply paths must not
silently discard the edge. They retain it as pending/dangling according to its
schema, index it only where safe, and activate or diagnose it when endpoint
state catches up. This distinction is required for deterministic convergence.

## 19. Adoption and migration examples

### 19.1 Framework upgrade with no declarations

Upgrading Myko adds no graph registrations, indexes, validation, memory use, or
behavior unless an application uses `#[myko_category]`, `#[myko_in(...)]`, or
`#[myko_edge]`. Existing item and query code continues unchanged.

### 19.2 Existing concrete association

A pre-edge application already models a payload-bearing association as an item:

```rust
#[myko_item]
pub struct Membership {
    #[belongs_to(Group)]
    pub group_id: GroupId,

    #[belongs_to(Member)]
    pub member_id: MemberId,

    pub permission: Permission,
}
```

It queries `Membership` through generated field filters and manually joins the
results to `Group` or `Member`. Migration leaves that declaration unchanged and
adds one self-registering trait implementation:

```rust
#[myko_edge]
impl GraphEdge for Membership {
    type Ends = Directed<ConcreteEndpoint<Group>, ConcreteEndpoint<Member>>;

    fn ends(&self) -> (GroupId, MemberId) {
        (self.group_id.clone(), self.member_id.clone())
    }

    const A_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::CascadeEdge;
    const B_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::CascadeEdge;
}
```

The existing item API remains valid:

```rust
ctx.query::<Membership>()
    .where_eq(MembershipField::GroupId, group_id.clone());
```

The graph API becomes an additional view over the same records:

```rust
ctx.edges::<Membership>().from(&group_id);
ctx.edges::<Membership>().between(&group_id, &member_id);
```

No stored record or event is rewritten. For example, the serialized item remains:

```json
{
  "id": "membership-1",
  "groupId": "group-1",
  "memberId": "member-1",
  "permission": "editor"
}
```

Its item type, ID, SET/DEL envelopes, persistence key, generated item types,
existing query registrations, and `belongs_to` cascade behavior are unchanged.

### 19.3 Existing polymorphic association

An application may already encode polymorphism in ordinary fields:

```rust
#[derive(Clone, Serialize, Deserialize)]
pub enum TargetRef {
    Article(ArticleId),
    Image(ImageId),
}

#[myko_item]
pub struct TagAssignment {
    #[belongs_to(Tag)]
    pub tag_id: TagId,
    pub target: TargetRef,
    pub attached_at: DateTime<Utc>,
}
```

The edge-aware form retains both serialized shapes. Eligible endpoint types gain
metadata-only category memberships:

```rust
#[myko_category]
pub struct TagTarget;

#[myko_in(TagTarget)]
#[myko_item]
pub struct Article {
    // existing fields
}

#[myko_in(TagTarget)]
#[myko_item]
pub struct Image {
    // existing fields
}
```

`TargetRef` provides the stable erased reference without changing its enum
representation:

```rust
impl IntoEntityRef for TargetRef {
    fn entity_ref(&self) -> EntityRef {
        match self {
            Self::Article(id) => EntityRef::of::<Article>(id),
            Self::Image(id) => EntityRef::of::<Image>(id),
        }
    }
}

#[myko_edge]
impl GraphEdge for TagAssignment {
    type Ends = Directed<ConcreteEndpoint<Tag>, CategoryEndpoint<TagTarget>>;

    fn ends(&self) -> (TagId, EntityRef) {
        (self.tag_id.clone(), self.target.entity_ref())
    }

    const A_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::CascadeEdge;
    const B_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::RetainDangling;
}
```

This adds type eligibility checks and generic graph queries without replacing
historical events. A new application with no compatibility constraint can store
`EntityRef` directly instead of defining `TargetRef`.

### 19.4 Existing qualified connection

An item that already stores node and port IDs also retains its shape. Its edge
implementation returns typed endpoint addresses:

```rust
#[myko_edge]
impl GraphEdge for Connection {
    type Ends = Directed<
        QualifiedEndpoint<Node, PortId>,
        QualifiedEndpoint<Node, PortId>,
    >;

    fn ends(&self) -> ((NodeId, PortId), (NodeId, PortId)) {
        (
            (self.source_node_id.clone(), self.source_port_id.clone()),
            (self.target_node_id.clone(), self.target_port_id.clone()),
        )
    }
}
```

Existing filters by node and port remain available. Graph queries additionally
provide entity-level node adjacency and exact `(node, port)` adjacency.

### 19.5 Deployment sequence for existing data

An existing edge item is introduced in typed observational mode:

```rust
server.set_edge_apply_mode::<Membership>(EdgeApplyMode::Observe);
```

The server rebuilds the derived graph view from canonical item stores and
compares it with existing query and relationship results. Observational mode
records missing endpoints, invalid categories, invalid qualifiers, pair
collisions, and projection mismatches without rejecting historical or new
canonical events.

After diagnostics are clean, the application changes the typed mode to
`EdgeApplyMode::Authoritative`. All mutation entry points then enforce the edge
schema. `AdjacencyPolicy::Eager` may be selected in the `GraphEdge`
implementation when measured lookup/traversal benefit justifies retained
memory. `ADJACENCY` remains the source-compatible default for both ends;
predominantly directional workloads may override `A_ADJACENCY` or
`B_ADJACENCY` independently. A cold end continues to use canonical scans,
while exact-pair lookup filters the hot end's incident bucket rather than
scanning the complete edge store.

Index consolidation is internal. Existing item queries and relationship
behavior remain available regardless of whether graph adjacency eventually
backs their implementation.

## 20. Testing strategy

### 20.1 Macro and registration tests

- `#[myko_category]` generates the marker implementation and inventory entry;
- `#[myko_in(...)]` generates compile-time membership and erased registration;
- `#[myko_edge]` generates `EdgeRegistration` from the attributed trait impl;
- no declaration requires a separate registration call or schema-name string;
- category IDs are stable and crate-qualified;
- concrete and erased endpoint extraction;
- same-type A and B positions remain distinct;
- existing item expansion is byte/schema compatible after edge annotation;
- qualifier projections are derived exactly from typed endpoint schemas;
- edge schema appears in neutral typegen.

### 20.2 Runtime correctness

- forward, reverse, and pair lookups;
- parallel edges;
- unqualified and qualified adjacency;
- endpoint and qualifier updates remove old index entries;
- self-loops are handled once per role/policy;
- delete cycles converge through reduce-before-cascade semantics;
- exact repeated transitions are suppressed within one causal chain;
- alternating/non-repeating effect loops stop at configured depth or work
  budgets and report the complete causal chain;
- independent root transactions do not share duplicate-transition state;
- batch SET/DEL produces atomically coherent adjacency publication;
- replay/rebuild equals live-maintained state;
- cascade, restrict, and dangling policies;
- category mismatch, unknown type, unknown ID, and unauthorized reference;
- qualifier invalidation diagnostics;
- weak/demand-driven bucket teardown does not leak;
- eager and demand-driven results are identical.

### 20.3 Concurrency

- concurrent unique-pair connects admit exactly one edge;
- concurrent endpoint deletion/connect obey declared consistency semantics;
- duplicate and reordered projection events are idempotent;
- readers never observe half of a bidirectional projection update;
- high-fanout mutation remains bounded/backpressured.

### 20.4 Typegen

- neutral edge/category schema snapshots;
- open `EntityRef` generation;
- aggregate closed category union where supported;
- typed qualifier address generation;
- existing item/query exports remain present.

## 21. Benchmark plan

Compare four implementations:

A. existing item with fully routed endpoint fields;
B. edge registration using demand-driven indexes;
C. ordinary edge item plus eager forward/reverse adjacency;
D. eager adjacency plus qualified and pair projections.

Also compare a graph-capable build with zero graph registrations against the
same workload before graph support. This isolates the cost of the empty-registry
fast path and protects the no-opt-in compatibility boundary.

Datasets:

- uniform degree and Zipf/high-degree hubs;
- sparse and dense graphs;
- small and rich edge payloads;
- concrete and polymorphic endpoints;
- zero qualifiers, one scalar qualifier, and one composite qualifier;
- 1%, 10%, and 50% active endpoint subscriptions;
- parallel and unique-pair edge types.

Operations:

- exact pair lookup;
- all incident edges for an item;
- incident edges for a qualified endpoint;
- forward/reverse live subscription, cold and warm;
- two- through five-hop bounded traversal;
- payload-filtered adjacency;
- connect/disconnect and endpoint move;
- endpoint deletion at low and high degree;
- startup replay/projection rebuild.

Measurements:

- p50/p95/p99 latency;
- allocations and CPU/cache behavior;
- retained bytes per edge and per live subscription;
- logical and physical writes per mutation;
- mutation throughput and graph-shard/pair-lock wait time;
- causal-guard overhead for shallow ordinary mutations and wide valid cascades;
- cold backfill time and number of store scans;
- steady diff latency and subscriber fanout;
- replay/rebuild time;
- projection lag;
- cascade event count and duration.

Expected hypotheses:

- Existing fully indexed items and eager adjacency have the same one-hop
  asymptotics.
- Demand-driven indexing wins idle memory and unused mutation cost when few
  endpoints are watched.
- Eager adjacency wins cold subscriptions and repeated/multi-hop traversal.
- Qualified adjacency wins when node degree is much larger than port/slot
  degree.
- Pair projection wins exact-pair and uniqueness checks but adds memory/write
  amplification.
- Rich payload filtering reduces adjacency's advantage unless matching payload
  indexes also exist.
- Hubs dominate output-size and fanout costs under every representation.
- A graph-capable application with zero registrations should be statistically
  indistinguishable from the existing mutation/query baseline.

### 21.1 Initial implementation measurements

`libs/myko/core/benches/graph.rs` is the executable acceptance matrix for the
first implementation. A short Criterion run on 2026-08-14 (10 samples, 100 ms
warmup, 200 ms measurement) produced:

| scenario | baseline | graph path | result |
| --- | ---: | ---: | ---: |
| ordinary SET, catalog present versus disabled | 1.98–2.61 µs disabled | 2.07–2.81 µs catalog present | confidence intervals overlap; no detected regression |
| 1,000-edge high-degree lookup returning 1,000 edges | 34.95–35.09 µs scan | 30.20–30.32 µs eager | about 1.16× faster; output materialization dominates |
| 10,000-edge sparse lookup returning 10 edges | 424.2–425.9 µs scan | 373.0–374.5 ns eager | about 1,137× faster |
| exact-pair existence among 10,000 edges | 426.8–429.0 µs scan | 104.7–105.3 ns pair projection | about 4,075× faster; ID lookup is 118.8–119.2 ns and typed materialization is 154.3–155.4 ns |
| sparse endpoint-delete planning among 10,000 edges with 10 incident | 419.3–421.3 µs conservative typed scan | 828.8–833.3 ns eager incidence | about 506× faster; the baseline scans only the populated store and is cheaper than the replaced dynamic all-registration path |
| 1,000-edge batch write | 309.1–310.2 µs plain | 1.815–1.823 ms projected | about 5.9× write cost for validation, causal hashing, and four maintained projections; inline singleton pair IDs improved the projected path by about 1.1%, within the benchmark's noise threshold |
| Sparse reactive watch initialization, 10,000 edges / 1,000 sources | 443.49–446.64 µs canonical select | 20.205–20.284 µs index-seeded | about 22× faster initialization for an eager endpoint; both remain live incremental maps |
| 1,000-edge projected batch, both ends versus A only | 1.817–1.832 ms both ends | 1.621–1.630 ms A only | one-sided projection is about 10.9% faster and omits the cold endpoint and entity-incidence maps |
| sparse hot-end lookup, both ends versus A only | 121.1–122.4 ns both ends | 123.0–123.8 ns A only | hot-end lookup remains within 2%; the write/memory saving does not trade away lookup complexity |
| singleton-inline incidence buckets | 1.815–1.823 ms prior 1,000-edge projected batch | 1.802–1.812 ms inline incidence | write time remains within noise while singleton endpoint buckets no longer allocate a tree; sparse hot-end lookup remains 121.2–121.6 ns |
| two writers, 200 attempted unique-edge writes | n/a | 361–396 µs | bounded authority-lock contention, no uniqueness race |

These are development-machine microbenchmarks, not release SLOs. They validate
the intended shape of the trade: the non-participating path stays within noise,
eager adjacency has a very large real gain when the selected neighborhood is
sparse relative to the edge store, and that gain is purchased with measurable
write amplification. CI should retain the benchmark definitions; release
qualification should rerun the full dataset/percentile matrix above.

## 22. Delivery phases

### Phase 1: schema and reflection

- `EntityCategory`, `InCategory`, and separate inventories;
- `EntityRef`;
- `EdgeRegistration`, `Directed`/`Undirected` shapes, end requirements, and qualifiers;
- `#[myko_category]`, `#[myko_in(...)]`,
  `GraphEdge`, and `#[myko_edge]`;
- neutral typegen schema;
- no new production query path.

### Phase 2: demand-driven graph APIs

- typed one-hop and qualified APIs backed by existing stores/indexes;
- category/existence validation;
- graph diagnostics;
- equivalence tests against generated item queries.

### Phase 3: eager adjacency projection

- from/to, incident, and typed qualifier indexes;
- optional pair projection;
- replay/rebuild and observability;
- shadow comparison with current relationship/query indexes.

### Phase 4: bounded traversal

- scoped, bounded BFS/DFS primitives;
- limits, authorization, and diagnostics;
- no general live transitive closure yet.

### Phase 5: consolidation

- route edge queries and cascades through shared adjacency where proven;
- retain generic relationship machinery for non-edge items;
- remove duplicate edge-specific structures only with benchmark and equivalence
  evidence.

## 23. Acceptance criteria

The graph-edge feature is acceptable when:

1. An application with no category or edge registrations has unchanged item,
   relationship, query, wire, persistence, generated-binding, and runtime
   behavior, installs no graph runtime state, and shows no material regression
   in mutation throughput or allocation count.
2. Two independent writers can add distinct incident edges without rewriting an
   endpoint item or losing either connection.
3. Concrete endpoint misuse fails at compile time, while an erased endpoint
   with an unknown type, invalid ID, or category mismatch fails
   deterministically with an actionable runtime error.
4. Existing edge items can be annotated without changing their serialized
   shape, item/ID identity, ordinary generated APIs, or replayed state.
5. Directed, undirected-incidence, pair, and typed qualifier projections produce the same
   results as a scan-based reference model after SET, endpoint-changing SET,
   DEL, batches, restart, and replay.
6. Broad item adjacency and qualified endpoint adjacency both return exact live
   diffs, with lookup cost proportional to the selected bucket plus output.
7. The graph index is rebuildable from canonical item state and exposes enough
   version/lag information that consumers never mistake a partial rebuild for a
   complete graph.
8. Edge and entity-category schema is emitted from the neutral aggregate
   graph catalog and consumed beside the existing `TypegenCatalog` without
   adding required fields or embedding renderer callbacks in neutral
   registrations.
9. Demand-driven adjacency is the default. An application selects eager
   adjacency per edge or per endpoint only when benchmarks against the fully
   routed item baseline show a workload benefit within its retained-bytes-per-edge
   budget. One-sided projection must retain scan-equivalent behavior on the cold
   end and for reverse traversal.
10. The handler-context authority model remains sealed; downstream entity
    categories cannot grant querying, event-publishing, or graph-reading
    authority.
11. Representative Rust and generated-client call sites can declare and query
    concrete, category-constrained, and qualified edges without handwritten
    entity/category/field-name strings or duplicate wire types.
12. Participating authoritative mutations reduce canonical and graph state,
    enqueue persistence before reactive work drains, and only then publish
    downstream effects; tests cover persistence-enqueue failure and a
    relationship workload that does not settle promptly.
13. Every synchronously derived relationship, graph, saga, or reactive mutation
    carries one root causal token. Delete cycles converge, exact transitions are
    suppressed per root, changing-value loops stop at explicit depth/work
    budgets, and loop termination produces actionable diagnostics instead of
    blocking persistence or spinning indefinitely.

## 24. Decision

An edge is an ordinary event-sourced Myko item plus separate graph metadata.
Entity categories are downstream-defined and many-to-many. Erased ends use
stable `EntityRef` values and category validation. Non-entity addresses are
typed, indexed qualifiers. `Directed<A, B>` and `Undirected<A, B>` encode the
edge shape, and either end may independently be concrete or erased.

No second storage, event, or persistence system is introduced. Demand-driven
adjacency is the default; eager adjacency is opt-in. Index consolidation
requires equivalence and performance evidence. Graph schema remains in a
separately collected catalog. Participating authoritative mutations use
preflight, reduce/project, persistence enqueue, then guarded causal effect
publication; the empty-registry path remains unchanged.
