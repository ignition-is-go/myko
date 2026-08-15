//! First-class graph metadata over ordinary Myko items.
//!
//! Graph edges do not introduce another storage or event model. An edge is an
//! [`Eventable`] item whose typed endpoint metadata is collected separately.

// Graph APIs intentionally return rich domain errors at many small typed
// boundaries, and coherent reads deliberately retain one shard guard through
// snapshot construction. Their shared module docs describe those contracts.
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![allow(clippy::significant_drop_tightening)]

use std::{
    any::type_name,
    collections::{BTreeSet, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    sync::{Arc, Mutex, MutexGuard, RwLock},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::item::{AnyItem, Eventable};
use crate::common::with_id::WithTypedId;

/// A stable, serializable reference to any registered Myko item.
#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
#[ts(crate = "crate::ts_rs")]
pub struct EntityRef {
    pub entity_type: Arc<str>,
    pub id: Arc<str>,
}

impl EntityRef {
    #[must_use]
    pub fn new(entity_type: impl Into<Arc<str>>, id: impl Into<Arc<str>>) -> Self {
        Self {
            entity_type: entity_type.into(),
            id: id.into(),
        }
    }
}

impl<T> From<&T> for EntityRef
where
    T: Eventable + WithTypedId,
{
    fn from(item: &T) -> Self {
        Self::new(T::ENTITY_NAME_STATIC, item.id())
    }
}

crate::register_typegen_type!(EntityRef);
crate::mark_framework_typegen_type!(EntityRef);
crate::impl_filterable_eq!(EntityRef);

/// An open, downstream-defined family of eligible item types.
pub trait EntityCategory: Send + Sync + 'static {
    const ID: &'static str;
    const NAME: &'static str;
}

/// Compile-time proof that an item belongs to an entity category.
pub trait InCategory<C: EntityCategory>: Eventable {}

pub struct EntityCategoryRegistration {
    pub id: &'static str,
    pub name: &'static str,
    pub crate_path: &'static str,
}

inventory::collect!(EntityCategoryRegistration);

pub struct ItemCategoryRegistration {
    pub item_type: &'static str,
    pub entity_category_id: &'static str,
    pub crate_path: &'static str,
}

inventory::collect!(ItemCategoryRegistration);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
#[ts(crate = "crate::ts_rs")]
pub enum EdgeShapeKind {
    Directed,
    Undirected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
#[ts(crate = "crate::ts_rs")]
pub enum EndPosition {
    A,
    B,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
#[ts(crate = "crate::ts_rs")]
pub enum PairPolicy {
    Parallel,
    Unique,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
#[ts(crate = "crate::ts_rs")]
pub enum PairProjectionPolicy {
    IntersectAdjacency,
    Eager,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
#[ts(crate = "crate::ts_rs")]
pub enum AdjacencyPolicy {
    DemandDriven,
    Eager,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
#[ts(crate = "crate::ts_rs")]
pub enum SelfLoopPolicy {
    Allow,
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
#[ts(crate = "crate::ts_rs")]
pub enum EndpointDeletePolicy {
    CascadeEdge,
    RestrictEndpointDelete,
    RetainDangling,
}

crate::register_typegen_type!(
    EdgeShapeKind,
    EndPosition,
    PairPolicy,
    PairProjectionPolicy,
    AdjacencyPolicy,
    SelfLoopPolicy,
    EndpointDeletePolicy,
);
crate::mark_framework_typegen_type!(
    EdgeShapeKind,
    EndPosition,
    PairPolicy,
    PairProjectionPolicy,
    AdjacencyPolicy,
    SelfLoopPolicy,
    EndpointDeletePolicy,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointRequirement {
    Concrete(&'static str),
    OneOf(&'static [&'static str]),
    Category(&'static str),
    AnyRegisteredItem,
}

/// Canonical bytes used as an indexed qualifier or scope component.
#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct IndexValue(Arc<[u8]>);

impl IndexValue {
    /// Encode a typed value into stable CBOR index bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot be encoded.
    pub fn from_serializable<T: Serialize>(value: &T) -> Result<Self> {
        let mut bytes = Vec::new();
        ciborium::into_writer(value, &mut bytes).context("encode graph index value")?;
        Ok(Self(bytes.into()))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Typed scalar or composite address below an endpoint entity.
pub trait EndpointQualifier:
    Clone + Debug + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// Encode this qualifier for equality and hash indexing.
    ///
    /// # Errors
    ///
    /// Returns an error when canonical encoding fails.
    fn index_value(&self) -> Result<IndexValue> {
        IndexValue::from_serializable(self)
    }
}

impl<T> EndpointQualifier for T where
    T: Clone + Debug + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

#[derive(Clone, Debug, Hash, Eq, Ord, PartialEq, PartialOrd)]
pub struct EndpointValue {
    pub entity: EntityRef,
    pub qualifier: Option<IndexValue>,
}

pub trait EndpointSpec: Send + Sync + 'static {
    type Value: Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static;

    fn requirement() -> EndpointRequirement;
    #[must_use]
    fn qualifier_type() -> Option<&'static str> {
        None
    }
    fn erase(value: &Self::Value) -> Result<EndpointValue>;
}

pub struct ConcreteEndpoint<T>(PhantomData<T>);

impl<T> EndpointSpec for ConcreteEndpoint<T>
where
    T: Eventable + WithTypedId,
    T::Id: Clone + Debug + Serialize + DeserializeOwned + Send + Sync + Into<Arc<str>> + 'static,
{
    type Value = T::Id;

    fn requirement() -> EndpointRequirement {
        EndpointRequirement::Concrete(T::ENTITY_NAME_STATIC)
    }

    fn erase(value: &Self::Value) -> Result<EndpointValue> {
        Ok(EndpointValue {
            entity: EntityRef::new(T::ENTITY_NAME_STATIC, value.clone().into()),
            qualifier: None,
        })
    }
}

pub struct CategoryEndpoint<C>(PhantomData<C>);

impl<C: EntityCategory> EndpointSpec for CategoryEndpoint<C> {
    type Value = EntityRef;

    fn requirement() -> EndpointRequirement {
        EndpointRequirement::Category(C::ID)
    }

    fn erase(value: &Self::Value) -> Result<EndpointValue> {
        Ok(EndpointValue {
            entity: value.clone(),
            qualifier: None,
        })
    }
}

pub struct AnyItemEndpoint;

impl EndpointSpec for AnyItemEndpoint {
    type Value = EntityRef;

    fn requirement() -> EndpointRequirement {
        EndpointRequirement::AnyRegisteredItem
    }

    fn erase(value: &Self::Value) -> Result<EndpointValue> {
        Ok(EndpointValue {
            entity: value.clone(),
            qualifier: None,
        })
    }
}

pub trait EndpointTypeSet: Send + Sync + 'static {
    const TYPES: &'static [&'static str];
}

macro_rules! endpoint_type_set {
    ($($name:ident),+ $(,)?) => {
        impl<$($name),+> EndpointTypeSet for ($($name,)+)
        where
            $($name: Eventable,)+
        {
            const TYPES: &'static [&'static str] = &[$($name::ENTITY_NAME_STATIC),+];
        }
    };
}

endpoint_type_set!(A);
endpoint_type_set!(A, B);
endpoint_type_set!(A, B, C);
endpoint_type_set!(A, B, C, D);
endpoint_type_set!(A, B, C, D, E);
endpoint_type_set!(A, B, C, D, E, F);

pub struct OneOfEndpoint<S>(PhantomData<S>);

impl<S: EndpointTypeSet> EndpointSpec for OneOfEndpoint<S> {
    type Value = EntityRef;

    fn requirement() -> EndpointRequirement {
        EndpointRequirement::OneOf(S::TYPES)
    }

    fn erase(value: &Self::Value) -> Result<EndpointValue> {
        Ok(EndpointValue {
            entity: value.clone(),
            qualifier: None,
        })
    }
}

pub struct QualifiedEndpoint<T, Q>(PhantomData<(T, Q)>);

/// Strongly typed entity-plus-qualifier address used by qualified endpoints.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize, crate::TS)]
#[serde(rename_all = "camelCase")]
#[ts(crate = "crate::ts_rs")]
pub struct QualifiedAddress<I, Q> {
    pub entity: I,
    pub qualifier: Q,
}

impl<T, Q> EndpointSpec for QualifiedEndpoint<T, Q>
where
    T: Eventable + WithTypedId,
    T::Id: Clone + Debug + Serialize + DeserializeOwned + Send + Sync + Into<Arc<str>> + 'static,
    Q: EndpointQualifier,
{
    type Value = QualifiedAddress<T::Id, Q>;

    fn requirement() -> EndpointRequirement {
        EndpointRequirement::Concrete(T::ENTITY_NAME_STATIC)
    }

    fn qualifier_type() -> Option<&'static str> {
        Some(type_name::<Q>())
    }

    fn erase(value: &Self::Value) -> Result<EndpointValue> {
        Ok(EndpointValue {
            entity: EntityRef::new(T::ENTITY_NAME_STATIC, value.entity.clone().into()),
            qualifier: Some(value.qualifier.index_value()?),
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct EdgeEndpoints {
    pub a: EndpointValue,
    pub b: EndpointValue,
}

pub struct EdgeEndpointRegistration {
    pub position: EndPosition,
    pub requirement: fn() -> EndpointRequirement,
    pub qualifier_type: fn() -> Option<&'static str>,
}

pub trait EdgeEnds: Send + Sync + 'static {
    type Values;
    const SHAPE: EdgeShapeKind;
    const ENDPOINTS: [EdgeEndpointRegistration; 2];

    fn erase(values: &Self::Values) -> Result<EdgeEndpoints>;
}

/// The typed A/B endpoint specifications shared by directed and undirected edges.
pub trait TypedEdgeEnds: EdgeEnds {
    type A: EndpointSpec;
    type B: EndpointSpec;
}

pub struct Directed<A, B>(PhantomData<(A, B)>);

impl<A: EndpointSpec, B: EndpointSpec> EdgeEnds for Directed<A, B> {
    type Values = (A::Value, B::Value);
    const SHAPE: EdgeShapeKind = EdgeShapeKind::Directed;
    const ENDPOINTS: [EdgeEndpointRegistration; 2] = [
        EdgeEndpointRegistration {
            position: EndPosition::A,
            requirement: A::requirement,
            qualifier_type: A::qualifier_type,
        },
        EdgeEndpointRegistration {
            position: EndPosition::B,
            requirement: B::requirement,
            qualifier_type: B::qualifier_type,
        },
    ];

    fn erase(values: &Self::Values) -> Result<EdgeEndpoints> {
        Ok(EdgeEndpoints {
            a: A::erase(&values.0)?,
            b: B::erase(&values.1)?,
        })
    }
}

impl<A: EndpointSpec, B: EndpointSpec> TypedEdgeEnds for Directed<A, B> {
    type A = A;
    type B = B;
}

pub struct Undirected<A, B>(PhantomData<(A, B)>);

impl<A: EndpointSpec, B: EndpointSpec> EdgeEnds for Undirected<A, B> {
    type Values = (A::Value, B::Value);
    const SHAPE: EdgeShapeKind = EdgeShapeKind::Undirected;
    const ENDPOINTS: [EdgeEndpointRegistration; 2] = [
        EdgeEndpointRegistration {
            position: EndPosition::A,
            requirement: A::requirement,
            qualifier_type: A::qualifier_type,
        },
        EdgeEndpointRegistration {
            position: EndPosition::B,
            requirement: B::requirement,
            qualifier_type: B::qualifier_type,
        },
    ];

    fn erase(values: &Self::Values) -> Result<EdgeEndpoints> {
        Ok(EdgeEndpoints {
            a: A::erase(&values.0)?,
            b: B::erase(&values.1)?,
        })
    }
}

impl<A: EndpointSpec, B: EndpointSpec> TypedEdgeEnds for Undirected<A, B> {
    type A = A;
    type B = B;
}

pub trait EdgeScope: Send + Sync + 'static {
    type Value;
    fn scope_type() -> Option<&'static str>;
    fn erase(value: &Self::Value) -> Result<IndexValue>;
}

pub struct NoScope;

impl EdgeScope for NoScope {
    type Value = ();

    fn scope_type() -> Option<&'static str> {
        None
    }

    fn erase(_value: &Self::Value) -> Result<IndexValue> {
        bail!("NoScope has no index value")
    }
}

pub struct ConcreteScope<T>(PhantomData<T>);

impl<T> EdgeScope for ConcreteScope<T>
where
    T: Eventable + WithTypedId,
    T::Id: Serialize,
{
    type Value = T::Id;

    fn scope_type() -> Option<&'static str> {
        Some(T::ENTITY_NAME_STATIC)
    }

    fn erase(value: &Self::Value) -> Result<IndexValue> {
        IndexValue::from_serializable(value)
    }
}

/// Read-only services exposed to edge validators.
pub struct EdgeValidationContext<'a> {
    exists: &'a dyn Fn(&EntityRef) -> bool,
}

impl<'a> EdgeValidationContext<'a> {
    #[must_use]
    pub fn new(exists: &'a dyn Fn(&EntityRef) -> bool) -> Self {
        Self { exists }
    }

    #[must_use]
    pub fn exists(&self, entity: &EntityRef) -> bool {
        (self.exists)(entity)
    }
}

pub trait EdgeValidator<E: GraphEdge>: Send + Sync + 'static {
    /// Validate one authoritative edge mutation.
    ///
    /// # Errors
    ///
    /// Returns an actionable domain validation error.
    fn validate(ctx: &EdgeValidationContext<'_>, edge: &E) -> Result<()>;
}

pub struct NoEdgeValidator;

impl<E: GraphEdge> EdgeValidator<E> for NoEdgeValidator {
    fn validate(_ctx: &EdgeValidationContext<'_>, _edge: &E) -> Result<()> {
        Ok(())
    }
}

pub trait GraphEdge: Eventable + Sized {
    type Ends: EdgeEnds;
    type Scope: EdgeScope;
    type Validator: EdgeValidator<Self>;

    fn ends(&self) -> <Self::Ends as EdgeEnds>::Values;

    fn scope(&self) -> Option<<Self::Scope as EdgeScope>::Value> {
        None
    }

    const PAIR_POLICY: PairPolicy = PairPolicy::Parallel;
    const PAIR_PROJECTION: PairProjectionPolicy = PairProjectionPolicy::IntersectAdjacency;
    /// Backward-compatible default projection policy for both endpoints.
    const ADJACENCY: AdjacencyPolicy = AdjacencyPolicy::DemandDriven;
    /// Endpoint-A projection policy. Override only this end for predominantly
    /// forward traversal workloads without allocating the B-side index.
    const A_ADJACENCY: AdjacencyPolicy = Self::ADJACENCY;
    /// Endpoint-B projection policy. Override only this end for predominantly
    /// reverse traversal workloads without allocating the A-side index.
    const B_ADJACENCY: AdjacencyPolicy = Self::ADJACENCY;
    const SELF_LOOPS: SelfLoopPolicy = SelfLoopPolicy::Allow;
    const A_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::CascadeEdge;
    const B_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::CascadeEdge;
}

pub type ErasedEdgeValidator = for<'a> fn(&EdgeValidationContext<'a>, &dyn AnyItem) -> Result<()>;
pub type EdgeExtractor = fn(&dyn AnyItem) -> Result<EdgeEndpoints>;
pub type EdgeScopeExtractor = fn(&dyn AnyItem) -> Result<Option<IndexValue>>;

pub struct EdgeRegistration {
    pub edge_type: &'static str,
    pub crate_path: &'static str,
    pub shape: EdgeShapeKind,
    pub pair_policy: PairPolicy,
    pub pair_projection: PairProjectionPolicy,
    pub endpoints: &'static [EdgeEndpointRegistration; 2],
    pub scope_type: fn() -> Option<&'static str>,
    pub adjacency: AdjacencyPolicy,
    pub self_loops: SelfLoopPolicy,
    pub a_delete: EndpointDeletePolicy,
    pub b_delete: EndpointDeletePolicy,
    pub extract: EdgeExtractor,
    pub extract_scope: EdgeScopeExtractor,
    pub validate: Option<ErasedEdgeValidator>,
}

inventory::collect!(EdgeRegistration);

/// Ordinary query-protocol registration generated for graph edge operations.
///
/// Kept separate from [`crate::query::QueryRegistration`] so graph query
/// parameter types do not need to participate in the general binding catalog;
/// graph codegen renders its more ergonomic endpoint-aware helpers directly.
pub struct GraphQueryRegistration {
    pub query_id: &'static str,
    pub edge_type: &'static str,
    pub parse: crate::query::QueryParseFn,
    pub cell_factory: crate::query::QueryCellFactory,
    pub window_cell_factory: GraphWindowQueryCellFactory,
}

inventory::collect!(GraphQueryRegistration);

pub type GraphWindowQueryCellFactory =
    fn(
        std::sync::Arc<dyn crate::query::AnyQuery>,
        std::sync::Arc<crate::store::StoreRegistry>,
        std::sync::Arc<crate::request::RequestContext>,
        std::sync::Arc<crate::server::MykoServerContext>,
        crate::wire::QueryWindow,
    ) -> Result<Option<crate::query::WindowedQuerySource>, String>;

/// Server-side bounded source generated for graph query parameter types.
#[doc(hidden)]
pub trait GraphWindowQueryFactory: crate::query::QueryParams {
    fn window_cell_factory(
        query: std::sync::Arc<dyn crate::query::AnyQuery>,
        registry: std::sync::Arc<crate::store::StoreRegistry>,
        request: std::sync::Arc<crate::request::RequestContext>,
        server: std::sync::Arc<crate::server::MykoServerContext>,
        window: crate::wire::QueryWindow,
    ) -> Result<Option<crate::query::WindowedQuerySource>, String>;
}

/// Type-erased factory helper used by generated endpoint graph queries.
#[doc(hidden)]
pub fn graph_window_query_at<Q, E, F>(
    query: &std::sync::Arc<dyn crate::query::AnyQuery>,
    registry: std::sync::Arc<crate::store::StoreRegistry>,
    request: std::sync::Arc<crate::request::RequestContext>,
    server: std::sync::Arc<crate::server::MykoServerContext>,
    window: crate::wire::QueryWindow,
    position: EndPosition,
    endpoint: F,
) -> Result<Option<crate::query::WindowedQuerySource>, String>
where
    Q: crate::query::QueryParams + Clone,
    E: GraphEdge,
    E::Ends: TypedEdgeEnds,
    F: FnOnce(&Q) -> Result<EndpointValue>,
{
    let any_ref: &dyn std::any::Any = query.as_ref();
    let query: crate::query::QueryRequest<Q> =
        crate::common::downcast::downcast_request(any_ref, "graph window query payload")?;
    let endpoint = endpoint(&query.query).map_err(|error| error.to_string())?;
    let query_context = std::sync::Arc::new(crate::query::QueryContext { req: request });
    crate::query::QueryBuildContext::new(query_context, registry, Some(server))
        .graph_window_at::<E>(position, &endpoint, window)
}

/// Type-erased factory helper used by generated exact-pair graph queries.
#[doc(hidden)]
pub fn graph_window_query_between<Q, E, F>(
    query: &std::sync::Arc<dyn crate::query::AnyQuery>,
    registry: std::sync::Arc<crate::store::StoreRegistry>,
    request: std::sync::Arc<crate::request::RequestContext>,
    server: std::sync::Arc<crate::server::MykoServerContext>,
    window: crate::wire::QueryWindow,
    endpoints: F,
) -> Result<Option<crate::query::WindowedQuerySource>, String>
where
    Q: crate::query::QueryParams + Clone,
    E: GraphEdge,
    E::Ends: TypedEdgeEnds,
    F: FnOnce(&Q) -> Result<(EndpointValue, EndpointValue)>,
{
    let any_ref: &dyn std::any::Any = query.as_ref();
    let query: crate::query::QueryRequest<Q> =
        crate::common::downcast::downcast_request(any_ref, "graph window query payload")?;
    let (a, b) = endpoints(&query.query).map_err(|error| error.to_string())?;
    let query_context = std::sync::Arc::new(crate::query::QueryContext { req: request });
    crate::query::QueryBuildContext::new(query_context, registry, Some(server))
        .graph_window_between::<E>(&a, &b, window)
}

/// Generated client-query types for one registered edge item.
///
/// The `#[myko_edge]` macro implements this trait, allowing generic Rust client
/// helpers to construct the same ordinary queries emitted by TypeScript graph
/// codegen without exposing query names at call sites.
pub trait GraphClientQueries: GraphEdge
where
    Self::Ends: TypedEdgeEnds,
{
    type FromQuery: crate::query::QueryParams + crate::query::QueryItemType<Item = Self>;
    type ToQuery: crate::query::QueryParams + crate::query::QueryItemType<Item = Self>;
    type BetweenQuery: crate::query::QueryParams + crate::query::QueryItemType<Item = Self>;

    fn from_query(
        endpoint: &<<Self::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Self::FromQuery;
    fn to_query(
        endpoint: &<<Self::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Self::ToQuery;
    fn between_query(
        a: &<<Self::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<Self::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Self::BetweenQuery;
}

/// Generated live aggregate reports for one registered edge item.
///
/// These reports keep graph cardinality checks on the server and send only a
/// scalar update to remote clients when the answer changes.
pub trait GraphClientAggregates: GraphEdge
where
    Self::Ends: TypedEdgeEnds,
{
    type CountFromReport: crate::report::ReportParams
        + crate::report::ReportOutputType<Output = usize>;
    type CountToReport: crate::report::ReportParams
        + crate::report::ReportOutputType<Output = usize>;
    type CountBetweenReport: crate::report::ReportParams
        + crate::report::ReportOutputType<Output = usize>;
    type ExistsBetweenReport: crate::report::ReportParams
        + crate::report::ReportOutputType<Output = bool>;

    fn count_from_report(
        endpoint: &<<Self::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Self::CountFromReport;
    fn count_to_report(
        endpoint: &<<Self::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Self::CountToReport;
    fn count_between_report(
        a: &<<Self::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<Self::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Self::CountBetweenReport;
    fn exists_between_report(
        a: &<<Self::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<Self::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Self::ExistsBetweenReport;
}

/// Generated command types for authoritative graph mutations.
///
/// These commands use Myko's ordinary command protocol, so callers receive the
/// existing validation errors, reconnect queuing, transaction IDs, and command
/// completion state without a graph-specific mutation channel.
pub trait GraphClientMutations: GraphEdge + crate::common::with_id::WithTypedId {
    type ConnectCommand: crate::command::CommandParams<Result = ()>;
    type ConnectManyCommand: crate::command::CommandParams<Result = usize>;
    type EnsureResult: serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + PartialEq
        + Send
        + Sync
        + 'static;
    type EnsureCommand: crate::command::CommandParams<Result = Self::EnsureResult>;
    type DisconnectResult: serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + PartialEq
        + Send
        + Sync
        + 'static;
    type DisconnectCommand: crate::command::CommandParams<Result = Self::DisconnectResult>;
    type DisconnectManyResult: serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + PartialEq
        + Send
        + Sync
        + 'static;
    type DisconnectManyCommand: crate::command::CommandParams<Result = Self::DisconnectManyResult>;

    fn connect_command(edge: &Self) -> Self::ConnectCommand;
    fn connect_many_command(edges: &[Self]) -> Self::ConnectManyCommand;
    fn ensure_command(edge: &Self) -> Self::EnsureCommand;
    fn disconnect_command(id: &Self::Id) -> Self::DisconnectCommand;
    fn disconnect_many_command(ids: &[Self::Id]) -> Self::DisconnectManyCommand;
}

/// Endpoint-specific projection policy emitted separately from the legacy
/// edge registration so existing manual `EdgeRegistration` literals remain
/// source-compatible.
pub struct EdgeAdjacencyRegistration {
    pub edge_type: &'static str,
    pub a: AdjacencyPolicy,
    pub b: AdjacencyPolicy,
}

inventory::collect!(EdgeAdjacencyRegistration);

impl EdgeRegistration {
    /// Effective per-end projection policies, falling back to the legacy
    /// whole-edge policy for manually submitted registrations.
    #[must_use]
    pub fn endpoint_adjacency(&self) -> [AdjacencyPolicy; 2] {
        inventory::iter::<EdgeAdjacencyRegistration>
            .into_iter()
            .find(|registration| registration.edge_type == self.edge_type)
            .map_or([self.adjacency; 2], |registration| {
                [registration.a, registration.b]
            })
    }
}

pub fn extract_edge<E: GraphEdge>(item: &dyn AnyItem) -> Result<EdgeEndpoints> {
    let edge = item
        .as_any()
        .downcast_ref::<E>()
        .context("edge registration item type mismatch")?;
    E::Ends::erase(&edge.ends())
}

pub fn extract_edge_scope<E: GraphEdge>(item: &dyn AnyItem) -> Result<Option<IndexValue>> {
    let edge = item
        .as_any()
        .downcast_ref::<E>()
        .context("edge registration item type mismatch")?;
    edge.scope().as_ref().map(E::Scope::erase).transpose()
}

pub fn validate_edge<E: GraphEdge>(
    ctx: &EdgeValidationContext<'_>,
    item: &dyn AnyItem,
) -> Result<()> {
    let edge = item
        .as_any()
        .downcast_ref::<E>()
        .context("edge validator item type mismatch")?;
    E::Validator::validate(ctx, edge)
}

/// Backend-neutral graph registrations selected for generated bindings.
#[derive(Default)]
pub struct GraphSchemaCatalog {
    pub entity_categories: Vec<&'static EntityCategoryRegistration>,
    pub item_categories: Vec<&'static ItemCategoryRegistration>,
    pub edges: Vec<&'static EdgeRegistration>,
}

impl GraphSchemaCatalog {
    #[must_use]
    pub fn collect(crate_name: &str) -> Self {
        Self::collect_crates([crate_name])
    }

    #[must_use]
    pub fn collect_crates<I, S>(crate_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let crate_names = crate_names
            .into_iter()
            .map(|name| name.as_ref().to_owned())
            .collect::<HashSet<_>>();
        Self::collect_matching(|path| {
            path.split("::")
                .next()
                .is_some_and(|name| crate_names.contains(name))
        })
    }

    fn collect_matching(selected: impl Fn(&str) -> bool) -> Self {
        Self {
            entity_categories: inventory::iter::<EntityCategoryRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_path))
                .collect(),
            item_categories: inventory::iter::<ItemCategoryRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_path))
                .collect(),
            edges: inventory::iter::<EdgeRegistration>
                .into_iter()
                .filter(|entry| selected(entry.crate_path))
                .collect(),
        }
    }
}

/// How graph validation treats a canonical item mutation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EdgeApplyMode {
    #[default]
    Authoritative,
    Replay,
    Import,
    Federated,
    Observe,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphReadiness {
    Building { watermark: u64 },
    Ready { generation: u64 },
    Failed,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphDiagnostics {
    pub generation: u64,
    pub edge_count: usize,
    pub adjacency_entries: usize,
    pub pair_entries: usize,
    pub invalid_mutations: u64,
    pub uniqueness_rejections: u64,
}

/// Derived work required before an endpoint can be deleted.
#[derive(Default)]
pub struct EndpointDeletePlan {
    pub cascade_edges: Vec<Arc<dyn AnyItem>>,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct EdgePairKey {
    scope: Option<IndexValue>,
    a: EndpointValue,
    b: EndpointValue,
}

#[derive(Clone, Debug)]
struct IndexedEdge {
    id: Arc<str>,
    endpoints: EdgeEndpoints,
    scope: Option<IndexValue>,
}

#[derive(Clone, Debug)]
enum EdgeIds {
    One(Arc<str>),
    Many(BTreeSet<Arc<str>>),
}

impl EdgeIds {
    fn insert(&mut self, id: Arc<str>) {
        match self {
            Self::One(existing) if *existing == id => {}
            Self::One(existing) => {
                *self = Self::Many(BTreeSet::from([existing.clone(), id]));
            }
            Self::Many(ids) => {
                ids.insert(id);
            }
        }
    }

    fn remove(&mut self, id: &Arc<str>) -> bool {
        match self {
            Self::One(existing) => existing == id,
            Self::Many(ids) => {
                ids.remove(id);
                let empty = ids.is_empty();
                let singleton = (ids.len() == 1).then(|| ids.first().cloned()).flatten();
                if let Some(remaining) = singleton {
                    *self = Self::One(remaining);
                }
                empty
            }
        }
    }

    fn ids(&self) -> Vec<Arc<str>> {
        match self {
            Self::One(id) => vec![id.clone()],
            Self::Many(ids) => ids.iter().cloned().collect(),
        }
    }

    fn window(&self, window: Option<&crate::wire::QueryWindow>) -> Vec<Arc<str>> {
        let (offset, limit) =
            window.map_or((0, usize::MAX), |window| (window.offset, window.limit));
        if limit == 0 {
            return Vec::new();
        }
        match self {
            Self::One(id) => (offset == 0).then(|| id.clone()).into_iter().collect(),
            Self::Many(ids) => ids.iter().skip(offset).take(limit).cloned().collect(),
        }
    }

    fn extend_cloned(&self, target: &mut BTreeSet<Arc<str>>) {
        match self {
            Self::One(id) => {
                target.insert(id.clone());
            }
            Self::Many(ids) => target.extend(ids.iter().cloned()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(ids) => ids.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn contains(&self, candidate: &Arc<str>) -> bool {
        match self {
            Self::One(id) => id == candidate,
            Self::Many(ids) => ids.contains(candidate),
        }
    }

    fn first(&self) -> Option<&Arc<str>> {
        match self {
            Self::One(id) => Some(id),
            Self::Many(ids) => ids.first(),
        }
    }

    fn conflicting_id(&self, candidate: &str) -> Option<&Arc<str>> {
        match self {
            Self::One(id) => (id.as_ref() != candidate).then_some(id),
            Self::Many(ids) => ids.iter().find(|id| id.as_ref() != candidate),
        }
    }
}

#[derive(Default)]
struct EdgeTypeState {
    generation: u64,
    edges: HashMap<Arc<str>, IndexedEdge>,
    a: HashMap<EndpointValue, EdgeIds>,
    b: HashMap<EndpointValue, EdgeIds>,
    a_entities: HashMap<EntityRef, EdgeIds>,
    b_entities: HashMap<EntityRef, EdgeIds>,
    pairs: HashMap<EdgePairKey, EdgeIds>,
}

#[derive(Default)]
struct GraphState {
    generation: u64,
    edge_types: HashMap<&'static str, EdgeTypeState>,
    invalid_mutations: u64,
    uniqueness_rejections: u64,
    failed: bool,
}

type GraphWatchMap = hyphae::CellMap<Arc<str>, Arc<dyn AnyItem>>;
type GraphWatchDiff = hyphae::MapDiff<Arc<str>, Arc<dyn AnyItem>>;

/// Coherent in-memory projection over all registered edge item stores.
///
/// The runtime is only constructed when graph registrations exist, preserving
/// the pre-graph mutation fast path for applications that do not opt in.
pub struct GraphIndex {
    registrations: HashMap<&'static str, &'static EdgeRegistration>,
    endpoint_adjacency: HashMap<&'static str, [AdjacencyPolicy; 2]>,
    categories: HashMap<&'static str, HashSet<&'static str>>,
    registry: Arc<crate::store::StoreRegistry>,
    state: RwLock<GraphState>,
    authority: Mutex<()>,
}

impl GraphIndex {
    #[must_use]
    pub fn from_inventory(registry: Arc<crate::store::StoreRegistry>) -> Option<Self> {
        let registrations = inventory::iter::<EdgeRegistration>
            .into_iter()
            .map(|registration| (registration.edge_type, registration))
            .collect::<HashMap<_, _>>();
        let has_categories = inventory::iter::<EntityCategoryRegistration>
            .into_iter()
            .next()
            .is_some();
        if registrations.is_empty() && !has_categories {
            return None;
        }
        let overrides = inventory::iter::<EdgeAdjacencyRegistration>
            .into_iter()
            .map(|registration| (registration.edge_type, [registration.a, registration.b]))
            .collect::<HashMap<_, _>>();
        let endpoint_adjacency = registrations
            .values()
            .map(|registration| {
                (
                    registration.edge_type,
                    overrides
                        .get(registration.edge_type)
                        .copied()
                        .unwrap_or([registration.adjacency; 2]),
                )
            })
            .collect();
        for registration in registrations.values() {
            if registration.shape == EdgeShapeKind::Undirected {
                let a = &registration.endpoints[0];
                let b = &registration.endpoints[1];
                assert!(
                    (a.requirement)() == (b.requirement)()
                        && (a.qualifier_type)() == (b.qualifier_type)(),
                    "undirected edge {} has asymmetric endpoint schemas",
                    registration.edge_type
                );
            }
        }

        let mut categories: HashMap<&'static str, HashSet<&'static str>> = HashMap::new();
        for membership in inventory::iter::<ItemCategoryRegistration> {
            categories
                .entry(membership.entity_category_id)
                .or_default()
                .insert(membership.item_type);
        }
        Some(Self {
            registrations,
            endpoint_adjacency,
            categories,
            registry,
            state: RwLock::new(GraphState::default()),
            authority: Mutex::new(()),
        })
    }

    pub(crate) fn lock_authority(&self) -> MutexGuard<'_, ()> {
        self.authority
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[must_use]
    pub fn coordinates(&self, item: &dyn AnyItem, change: crate::wire::MEventType) -> bool {
        if self.registration(item.entity_type()).is_some() {
            return true;
        }
        if change != crate::wire::MEventType::DEL {
            return false;
        }
        self.registrations.values().any(|registration| {
            [
                (&registration.endpoints[0], registration.a_delete),
                (&registration.endpoints[1], registration.b_delete),
            ]
            .into_iter()
            .any(|(endpoint, policy)| {
                policy != EndpointDeletePolicy::RetainDangling
                    && self.requirement_accepts(&(endpoint.requirement)(), item.entity_type())
            })
        })
    }

    #[must_use]
    pub fn registration(&self, edge_type: &str) -> Option<&'static EdgeRegistration> {
        self.registrations.get(edge_type).copied()
    }

    fn projects_endpoint(&self, edge_type: &str, position: EndPosition) -> bool {
        self.endpoint_adjacency
            .get(edge_type)
            .is_some_and(|policies| {
                let [a, b] = *policies;
                (match position {
                    EndPosition::A => a,
                    EndPosition::B => b,
                }) == AdjacencyPolicy::Eager
            })
    }

    fn projects_any_endpoint(&self, edge_type: &str) -> bool {
        self.projects_endpoint(edge_type, EndPosition::A)
            || self.projects_endpoint(edge_type, EndPosition::B)
    }

    /// Rebuild the entire derived index from canonical edge stores.
    pub fn rebuild(&self) -> Result<u64> {
        {
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *state = GraphState::default();
        }
        for edge_type in self.registrations.keys() {
            let Some(store) = self.registry.get(edge_type) else {
                continue;
            };
            for (_, item) in store.snapshot() {
                self.apply(None, Some(item.as_ref()))?;
            }
        }
        Ok(self.generation())
    }

    fn requirement_accepts(&self, requirement: &EndpointRequirement, entity_type: &str) -> bool {
        match requirement {
            EndpointRequirement::Concrete(expected) => *expected == entity_type,
            EndpointRequirement::OneOf(expected) => expected.contains(&entity_type),
            EndpointRequirement::Category(category) => self
                .categories
                .get(category)
                .is_some_and(|members| members.contains(entity_type)),
            EndpointRequirement::AnyRegisteredItem => {
                crate::item::lookup_item_registration(entity_type).is_some()
            }
        }
    }

    fn exists(&self, entity: &EntityRef) -> bool {
        self.registry
            .get(&entity.entity_type)
            .and_then(|store| store.get_value(&entity.id))
            .is_some()
    }

    fn pair_key(
        registration: &EdgeRegistration,
        endpoints: &EdgeEndpoints,
        scope: Option<IndexValue>,
    ) -> EdgePairKey {
        Self::pair_key_from_values(registration, &endpoints.a, &endpoints.b, scope)
    }

    fn pair_key_from_values(
        registration: &EdgeRegistration,
        a: &EndpointValue,
        b: &EndpointValue,
        scope: Option<IndexValue>,
    ) -> EdgePairKey {
        let (a, b) = if registration.shape == EdgeShapeKind::Undirected && b < a {
            (b.clone(), a.clone())
        } else {
            (a.clone(), b.clone())
        };
        EdgePairKey { scope, a, b }
    }

    fn projects_pairs(registration: &EdgeRegistration) -> bool {
        registration.pair_policy == PairPolicy::Unique
            || registration.pair_projection == PairProjectionPolicy::Eager
    }

    fn uses_unscoped_pair_fast_path(registration: &EdgeRegistration) -> bool {
        Self::projects_pairs(registration) && (registration.scope_type)().is_none()
    }

    fn endpoints_match(
        registration: &EdgeRegistration,
        endpoints: &EdgeEndpoints,
        a: &EndpointValue,
        b: &EndpointValue,
    ) -> bool {
        (endpoints.a == *a && endpoints.b == *b)
            || (registration.shape == EdgeShapeKind::Undirected
                && endpoints.a == *b
                && endpoints.b == *a)
    }

    fn indexed_ids_between(
        &self,
        registration: &EdgeRegistration,
        edges: &EdgeTypeState,
        a: &EndpointValue,
        b: &EndpointValue,
        scope: Option<&IndexValue>,
    ) -> Vec<Arc<str>> {
        let a_eager = self.projects_endpoint(registration.edge_type, EndPosition::A);
        let b_eager = self.projects_endpoint(registration.edge_type, EndPosition::B);

        // Preserve the allocation-free hot path for ordinary directed edges
        // when both incidence indexes are present.
        if a_eager && b_eager && registration.shape == EdgeShapeKind::Directed {
            let (small, other) = match (edges.a.get(a), edges.b.get(b)) {
                (Some(a_ids), Some(b_ids)) if a_ids.len() <= b_ids.len() => (a_ids, b_ids),
                (Some(a_ids), Some(b_ids)) => (b_ids, a_ids),
                _ => return Vec::new(),
            };
            let accepted = |id: &Arc<str>| {
                other.contains(id)
                    && scope.is_none_or(|scope| {
                        edges
                            .edges
                            .get(id)
                            .is_some_and(|edge| edge.scope.as_ref() == Some(scope))
                    })
            };
            return match small {
                EdgeIds::One(id) => {
                    if accepted(id) {
                        vec![id.clone()]
                    } else {
                        Vec::new()
                    }
                }
                EdgeIds::Many(ids) => ids.iter().filter(|id| accepted(id)).cloned().collect(),
            };
        }

        // With one projected end, inspect only that endpoint's incident set
        // and validate the cold end from the retained indexed edge record.
        let mut candidates = BTreeSet::new();
        if a_eager {
            if let Some(ids) = edges.a.get(a) {
                ids.extend_cloned(&mut candidates);
            }
            if registration.shape == EdgeShapeKind::Undirected
                && let Some(ids) = edges.a.get(b)
            {
                ids.extend_cloned(&mut candidates);
            }
        }
        if b_eager {
            if let Some(ids) = edges.b.get(b) {
                ids.extend_cloned(&mut candidates);
            }
            if registration.shape == EdgeShapeKind::Undirected
                && let Some(ids) = edges.b.get(a)
            {
                ids.extend_cloned(&mut candidates);
            }
        }
        candidates
            .into_iter()
            .filter(|id| {
                edges.edges.get(id).is_some_and(|edge| {
                    Self::endpoints_match(registration, &edge.endpoints, a, b)
                        && scope.is_none_or(|scope| edge.scope.as_ref() == Some(scope))
                })
            })
            .collect()
    }

    /// Validate an edge mutation before canonical reduction.
    ///
    /// Replay-like modes retain invalid history and emit diagnostics; only an
    /// authoritative command is rejected.
    pub fn preflight(
        &self,
        old: Option<&dyn AnyItem>,
        new: Option<&dyn AnyItem>,
        mode: EdgeApplyMode,
    ) -> Result<()> {
        let item = new
            .or(old)
            .context("graph mutation has no canonical item")?;
        let Some(registration) = self.registration(item.entity_type()) else {
            return Ok(());
        };
        let validate = || -> Result<()> {
            let candidate = new.context("edge deletion has no new value")?;
            let endpoints = (registration.extract)(candidate)?;
            for (endpoint, specification) in [
                (&endpoints.a, &registration.endpoints[0]),
                (&endpoints.b, &registration.endpoints[1]),
            ] {
                let requirement = (specification.requirement)();
                if !self.requirement_accepts(&requirement, &endpoint.entity.entity_type) {
                    bail!(
                        "{} endpoint {:?} rejects entity type {}",
                        registration.edge_type,
                        specification.position,
                        endpoint.entity.entity_type
                    );
                }
                if !self.exists(&endpoint.entity) {
                    bail!(
                        "{} endpoint {:?} does not exist: {}:{}",
                        registration.edge_type,
                        specification.position,
                        endpoint.entity.entity_type,
                        endpoint.entity.id
                    );
                }
            }
            if registration.self_loops == SelfLoopPolicy::Reject && endpoints.a == endpoints.b {
                bail!("{} rejects self-loops", registration.edge_type);
            }
            if let Some(validate) = registration.validate {
                let exists = |entity: &EntityRef| self.exists(entity);
                let context = EdgeValidationContext::new(&exists);
                validate(&context, candidate)?;
            }
            if registration.pair_policy == PairPolicy::Unique {
                let scope = (registration.extract_scope)(candidate)?;
                let key = Self::pair_key(registration, &endpoints, scope);
                let state = self
                    .state
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(existing_ids) = state
                    .edge_types
                    .get(registration.edge_type)
                    .and_then(|edge_type| edge_type.pairs.get(&key))
                    && let Some(existing_id) = existing_ids.conflicting_id(candidate.id().as_ref())
                {
                    bail!(
                        "{} pair is already occupied by edge {}",
                        registration.edge_type,
                        existing_id
                    );
                }
            }
            Ok(())
        };

        if new.is_none() {
            return Ok(());
        }
        if let Err(error) = validate() {
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.invalid_mutations = state.invalid_mutations.saturating_add(1);
            if error.to_string().contains("already occupied") {
                state.uniqueness_rejections = state.uniqueness_rejections.saturating_add(1);
            }
            if mode == EdgeApplyMode::Authoritative {
                return Err(error);
            }
            tracing::warn!(edge_type = item.entity_type(), %error, "retaining invalid graph history");
        }
        Ok(())
    }

    /// Validate reservations that conflict only within one not-yet-reduced batch.
    pub fn preflight_batch(&self, items: &[Arc<dyn AnyItem>], mode: EdgeApplyMode) -> Result<()> {
        let mut reservations: HashMap<(&'static str, EdgePairKey), Arc<str>> = HashMap::new();
        for item in items {
            let Some(registration) = self.registration(item.entity_type()) else {
                continue;
            };
            if registration.pair_policy != PairPolicy::Unique {
                continue;
            }
            let endpoints = (registration.extract)(item.as_ref())?;
            let scope = (registration.extract_scope)(item.as_ref())?;
            let key = (
                registration.edge_type,
                Self::pair_key(registration, &endpoints, scope),
            );
            if let Some(existing) = reservations.insert(key, item.id())
                && existing != item.id()
            {
                let mut state = self
                    .state
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.invalid_mutations = state.invalid_mutations.saturating_add(1);
                state.uniqueness_rejections = state.uniqueness_rejections.saturating_add(1);
                let error = anyhow::anyhow!(
                    "{} batch contains duplicate pair reservations {} and {}",
                    registration.edge_type,
                    existing,
                    item.id()
                );
                if mode == EdgeApplyMode::Authoritative {
                    return Err(error);
                }
                tracing::warn!(edge_type = registration.edge_type, %error);
            }
        }
        Ok(())
    }

    fn remove_indexed(
        registration: &EdgeRegistration,
        state: &mut EdgeTypeState,
        edge: &IndexedEdge,
    ) {
        for (map, endpoint) in [
            (&mut state.a, &edge.endpoints.a),
            (&mut state.b, &edge.endpoints.b),
        ] {
            if let Some(ids) = map.get_mut(endpoint) {
                ids.remove(&edge.id);
                if ids.is_empty() {
                    map.remove(endpoint);
                }
            }
        }
        for (map, endpoint) in [
            (&mut state.a_entities, &edge.endpoints.a.entity),
            (&mut state.b_entities, &edge.endpoints.b.entity),
        ] {
            if let Some(ids) = map.get_mut(endpoint) {
                ids.remove(&edge.id);
                if ids.is_empty() {
                    map.remove(endpoint);
                }
            }
        }
        if Self::projects_pairs(registration) {
            let key = Self::pair_key(registration, &edge.endpoints, edge.scope.clone());
            if let Some(ids) = state.pairs.get_mut(&key)
                && ids.remove(&edge.id)
            {
                state.pairs.remove(&key);
            }
        }
    }

    fn insert_incidence<K>(map: &mut HashMap<K, EdgeIds>, key: K, id: Arc<str>)
    where
        K: Eq + std::hash::Hash,
    {
        match map.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => entry.get_mut().insert(id),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(EdgeIds::One(id));
            }
        }
    }

    fn insert_indexed(
        &self,
        registration: &EdgeRegistration,
        state: &mut EdgeTypeState,
        edge: IndexedEdge,
    ) {
        if self.projects_endpoint(registration.edge_type, EndPosition::A) {
            Self::insert_incidence(&mut state.a, edge.endpoints.a.clone(), edge.id.clone());
            Self::insert_incidence(
                &mut state.a_entities,
                edge.endpoints.a.entity.clone(),
                edge.id.clone(),
            );
        }
        if self.projects_endpoint(registration.edge_type, EndPosition::B) {
            Self::insert_incidence(&mut state.b, edge.endpoints.b.clone(), edge.id.clone());
            Self::insert_incidence(
                &mut state.b_entities,
                edge.endpoints.b.entity.clone(),
                edge.id.clone(),
            );
        }
        if Self::projects_pairs(registration) {
            match state.pairs.entry(Self::pair_key(
                registration,
                &edge.endpoints,
                edge.scope.clone(),
            )) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().insert(edge.id.clone());
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(EdgeIds::One(edge.id.clone()));
                }
            }
        }
        if self.projects_any_endpoint(registration.edge_type) {
            state.edges.insert(edge.id.clone(), edge);
        }
    }

    /// Atomically update all adjacency projections after canonical reduction.
    pub fn apply(&self, old: Option<&dyn AnyItem>, new: Option<&dyn AnyItem>) -> Result<u64> {
        let item = new
            .or(old)
            .context("graph mutation has no canonical item")?;
        let Some(registration) = self.registration(item.entity_type()) else {
            return Ok(self.generation());
        };
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let edge_type = state.edge_types.entry(registration.edge_type).or_default();
        if let Some(old) = old {
            let indexed = IndexedEdge {
                id: old.id(),
                endpoints: (registration.extract)(old)?,
                scope: (registration.extract_scope)(old)?,
            };
            edge_type.edges.remove(&old.id());
            Self::remove_indexed(registration, edge_type, &indexed);
        }
        if let Some(new) = new {
            let indexed = IndexedEdge {
                id: new.id(),
                endpoints: (registration.extract)(new)?,
                scope: (registration.extract_scope)(new)?,
            };
            self.insert_indexed(registration, edge_type, indexed);
        }
        edge_type.generation = edge_type.generation.saturating_add(1);
        state.generation = state.generation.saturating_add(1);
        Ok(state.generation)
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation
    }

    #[must_use]
    pub fn readiness(&self) -> GraphReadiness {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.failed {
            GraphReadiness::Failed
        } else {
            GraphReadiness::Ready {
                generation: state.generation,
            }
        }
    }

    #[must_use]
    pub fn diagnostics(&self) -> GraphDiagnostics {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        GraphDiagnostics {
            generation: state.generation,
            edge_count: state
                .edge_types
                .values()
                .map(|edges| edges.edges.len())
                .sum(),
            adjacency_entries: state
                .edge_types
                .values()
                .map(|edges| {
                    edges
                        .a_entities
                        .len()
                        .saturating_add(edges.b_entities.len())
                })
                .sum(),
            pair_entries: state
                .edge_types
                .values()
                .map(|edges| edges.pairs.len())
                .sum(),
            invalid_mutations: state.invalid_mutations,
            uniqueness_rejections: state.uniqueness_rejections,
        }
    }

    /// Enforce restrict/cascade policies for an authoritative endpoint DEL.
    #[allow(clippy::too_many_lines)]
    pub fn endpoint_delete_plan(&self, endpoint: &EntityRef) -> Result<EndpointDeletePlan> {
        let mut cascade = HashSet::new();

        // Eager projections already retain entity-level incidence. Resolve all
        // indexed policies while holding one coherent graph snapshot, then
        // materialize only the incident canonical items after releasing it.
        // Checking both roles before returning the cascade set is load-bearing
        // for self-loops: Restrict must win over Cascade regardless of role.
        {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (edge_type, registration) in &self.registrations {
                let Some(edges) = state.edge_types.get(edge_type) else {
                    continue;
                };
                for (position, ids, policy) in [
                    (
                        EndPosition::B,
                        edges.b_entities.get(endpoint),
                        registration.b_delete,
                    ),
                    (
                        EndPosition::A,
                        edges.a_entities.get(endpoint),
                        registration.a_delete,
                    ),
                ] {
                    if !self.projects_endpoint(edge_type, position) {
                        continue;
                    }
                    let Some(ids) = ids else {
                        continue;
                    };
                    match policy {
                        EndpointDeletePolicy::RestrictEndpointDelete => {
                            if let Some(id) = ids.first() {
                                bail!(
                                    "cannot delete {}:{}; {} edge {} is incident at {:?}",
                                    endpoint.entity_type,
                                    endpoint.id,
                                    edge_type,
                                    id,
                                    position
                                );
                            }
                        }
                        EndpointDeletePolicy::CascadeEdge => match ids {
                            EdgeIds::One(id) => {
                                cascade.insert((*edge_type, id.clone()));
                            }
                            EdgeIds::Many(ids) => {
                                cascade.extend(ids.iter().cloned().map(|id| (*edge_type, id)));
                            }
                        },
                        EndpointDeletePolicy::RetainDangling => {}
                    }
                }
            }
        }

        // Demand-driven edge types intentionally retain no incidence map. They
        // pay the canonical scan only when endpoint deletion needs a policy
        // decision; types that retain dangling edges at both roles need no work.
        for (edge_type, registration) in &self.registrations {
            let a_needs_scan = !self.projects_endpoint(edge_type, EndPosition::A)
                && registration.a_delete != EndpointDeletePolicy::RetainDangling;
            let b_needs_scan = !self.projects_endpoint(edge_type, EndPosition::B)
                && registration.b_delete != EndpointDeletePolicy::RetainDangling;
            if !a_needs_scan && !b_needs_scan {
                continue;
            }
            let Some(store) = self.registry.get(edge_type) else {
                continue;
            };
            for (_, item) in store.snapshot() {
                let indexed = IndexedEdge {
                    id: item.id(),
                    endpoints: (registration.extract)(item.as_ref())?,
                    scope: (registration.extract_scope)(item.as_ref())?,
                };
                for (position, value, policy) in [
                    (EndPosition::A, &indexed.endpoints.a, registration.a_delete),
                    (EndPosition::B, &indexed.endpoints.b, registration.b_delete),
                ] {
                    if self.projects_endpoint(edge_type, position)
                        || policy == EndpointDeletePolicy::RetainDangling
                        || &value.entity != endpoint
                    {
                        continue;
                    }
                    match policy {
                        EndpointDeletePolicy::RestrictEndpointDelete => bail!(
                            "cannot delete {}:{}; {} edge {} is incident at {:?}",
                            endpoint.entity_type,
                            endpoint.id,
                            edge_type,
                            indexed.id,
                            position
                        ),
                        EndpointDeletePolicy::CascadeEdge => {
                            cascade.insert((*edge_type, indexed.id.clone()));
                        }
                        EndpointDeletePolicy::RetainDangling => {}
                    }
                }
            }
        }
        let cascade_edges = cascade
            .into_iter()
            .filter_map(|(edge_type, id)| {
                self.registry
                    .get(edge_type)
                    .and_then(|store| store.get_value(&id))
            })
            .collect();
        Ok(EndpointDeletePlan { cascade_edges })
    }

    #[must_use]
    pub fn edge_ids_at(
        &self,
        edge_type: &str,
        position: EndPosition,
        endpoint: &EndpointValue,
    ) -> Vec<Arc<str>> {
        let Some(registration) = self.registration(edge_type) else {
            return Vec::new();
        };
        if !self.projects_endpoint(edge_type, position) {
            return self.registry.get(edge_type).map_or_else(Vec::new, |store| {
                store
                    .snapshot()
                    .into_iter()
                    .filter_map(|(_, item)| {
                        let endpoints = (registration.extract)(item.as_ref()).ok()?;
                        let candidate = match position {
                            EndPosition::A => endpoints.a,
                            EndPosition::B => endpoints.b,
                        };
                        (candidate == *endpoint).then(|| item.id())
                    })
                    .collect()
            });
        }
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(edges) = state.edge_types.get(edge_type) else {
            return Vec::new();
        };
        let map = match position {
            EndPosition::A => &edges.a,
            EndPosition::B => &edges.b,
        };
        map.get(endpoint).map_or_else(Vec::new, EdgeIds::ids)
    }

    fn watch_diff_matching(
        registration: &EdgeRegistration,
        matches_endpoints: &impl Fn(&EdgeEndpoints) -> bool,
        diff: &GraphWatchDiff,
    ) -> Option<GraphWatchDiff> {
        let matches = |item: &Arc<dyn AnyItem>| {
            (registration.extract)(item.as_ref())
                .is_ok_and(|endpoints| matches_endpoints(&endpoints))
        };
        match diff {
            GraphWatchDiff::Initial { entries } => Some(GraphWatchDiff::Initial {
                entries: entries
                    .iter()
                    .filter(|(_, item)| matches(item))
                    .cloned()
                    .collect(),
            }),
            GraphWatchDiff::Insert { key, value } => {
                matches(value).then(|| GraphWatchDiff::Insert {
                    key: key.clone(),
                    value: value.clone(),
                })
            }
            GraphWatchDiff::Remove { key, old_value } => {
                matches(old_value).then(|| GraphWatchDiff::Remove {
                    key: key.clone(),
                    old_value: old_value.clone(),
                })
            }
            GraphWatchDiff::Update {
                key,
                old_value,
                new_value,
            } => match (matches(old_value), matches(new_value)) {
                (true, true) => Some(GraphWatchDiff::Update {
                    key: key.clone(),
                    old_value: old_value.clone(),
                    new_value: new_value.clone(),
                }),
                (true, false) => Some(GraphWatchDiff::Remove {
                    key: key.clone(),
                    old_value: old_value.clone(),
                }),
                (false, true) => Some(GraphWatchDiff::Insert {
                    key: key.clone(),
                    value: new_value.clone(),
                }),
                (false, false) => None,
            },
            GraphWatchDiff::Batch { changes } => {
                let changes = changes
                    .iter()
                    .filter_map(|change| {
                        Self::watch_diff_matching(registration, matches_endpoints, change)
                    })
                    .collect::<Vec<_>>();
                (!changes.is_empty()).then_some(GraphWatchDiff::Batch { changes })
            }
        }
    }

    fn watch_diff_affects_matching(
        registration: &EdgeRegistration,
        matches_endpoints: &impl Fn(&EdgeEndpoints) -> bool,
        diff: &GraphWatchDiff,
    ) -> bool {
        let matches = |item: &Arc<dyn AnyItem>| {
            (registration.extract)(item.as_ref())
                .is_ok_and(|endpoints| matches_endpoints(&endpoints))
        };
        match diff {
            GraphWatchDiff::Initial { .. } => true,
            GraphWatchDiff::Insert { value, .. } => matches(value),
            GraphWatchDiff::Remove { old_value, .. } => matches(old_value),
            GraphWatchDiff::Update {
                old_value,
                new_value,
                ..
            } => matches(old_value) || matches(new_value),
            GraphWatchDiff::Batch { changes } => changes.iter().any(|change| {
                Self::watch_diff_affects_matching(registration, matches_endpoints, change)
            }),
        }
    }

    fn watch_window_matching<F, M>(
        self: &Arc<Self>,
        edge_type: &'static str,
        initial_window: crate::wire::QueryWindow,
        select: F,
        matches_endpoints: M,
    ) -> Result<crate::query::WindowedQuerySource>
    where
        F: Fn(&Self, Option<&crate::wire::QueryWindow>) -> crate::query::WindowedQuerySnapshot
            + Send
            + Sync
            + 'static,
        M: Fn(&EdgeEndpoints) -> bool + Send + Sync + 'static,
    {
        use hyphae::{Materialize, Mutable, Signal, Watchable};

        let registration = self
            .registration(edge_type)
            .context("edge type is not registered")?;
        let store = self.registry.get_or_create(edge_type);
        let _authority = self.lock_authority();
        let window = Arc::new(Mutex::new(Some(initial_window)));
        let select = Arc::new(select);
        let initial = {
            let current = window
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            select(self, current.as_ref())
        };
        let snapshots = hyphae::Cell::new(Arc::new(initial));

        let snapshots_weak = snapshots.downgrade();
        let graph_weak = Arc::downgrade(self);
        let window_for_diffs = window.clone();
        let select_for_diffs = select.clone();
        let store_keepalive = store.clone();
        let diffs = store.diffs().materialize();
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let guard = diffs.subscribe(move |signal| {
            let _ = &store_keepalive;
            if first.load(std::sync::atomic::Ordering::Relaxed)
                && first.swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return;
            }
            let Signal::Value(diff) = signal else {
                return;
            };
            if !Self::watch_diff_affects_matching(registration, &matches_endpoints, diff.as_ref()) {
                return;
            }
            let Some(graph) = graph_weak.upgrade() else {
                return;
            };
            let Some(snapshots) = snapshots_weak.upgrade() else {
                return;
            };
            let next = {
                let window = window_for_diffs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                select_for_diffs(&graph, window.as_ref())
            };
            snapshots.set(Arc::new(next));
        });
        snapshots.own(guard);

        let snapshots_weak = snapshots.downgrade();
        let graph_weak = Arc::downgrade(self);
        let set_window = move |next: Option<crate::wire::QueryWindow>| {
            let Some(graph) = graph_weak.upgrade() else {
                return;
            };
            let next_snapshot = {
                let _authority = graph.lock_authority();
                let mut current = window
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if *current == next {
                    return;
                }
                *current = next;
                select(&graph, current.as_ref())
            };
            if let Some(snapshots) = snapshots_weak.upgrade() {
                snapshots.set(Arc::new(next_snapshot));
            }
        };

        Ok(crate::query::WindowedQuerySource::new(
            snapshots.lock(),
            set_window,
        ))
    }

    fn window_snapshot_at(
        &self,
        edge_type: &str,
        position: EndPosition,
        endpoint: &EndpointValue,
        window: Option<&crate::wire::QueryWindow>,
    ) -> crate::query::WindowedQuerySnapshot {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ids = state
            .edge_types
            .get(edge_type)
            .and_then(|edges| match position {
                EndPosition::A => edges.a.get(endpoint),
                EndPosition::B => edges.b.get(endpoint),
            });
        let total_count = ids.map_or(0, EdgeIds::len);
        let selected = ids.map_or_else(Vec::new, |ids| ids.window(window));
        drop(state);
        let store = self.registry.get_or_create(edge_type);
        let entries = selected
            .into_iter()
            .filter_map(|id| store.get_value(&id).map(|item| (id, item)))
            .collect();
        crate::query::WindowedQuerySnapshot {
            entries,
            total_count,
            window: window.cloned(),
        }
    }

    fn window_snapshot_between(
        &self,
        edge_type: &str,
        a: &EndpointValue,
        b: &EndpointValue,
        window: Option<&crate::wire::QueryWindow>,
    ) -> crate::query::WindowedQuerySnapshot {
        let Some(registration) = self.registration(edge_type) else {
            return crate::query::WindowedQuerySnapshot {
                entries: Vec::new(),
                total_count: 0,
                window: window.cloned(),
            };
        };
        let key = Self::pair_key_from_values(registration, a, b, None);
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ids = state
            .edge_types
            .get(edge_type)
            .and_then(|edges| edges.pairs.get(&key));
        let total_count = ids.map_or(0, EdgeIds::len);
        let selected = ids.map_or_else(Vec::new, |ids| ids.window(window));
        drop(state);
        let store = self.registry.get_or_create(edge_type);
        let entries = selected
            .into_iter()
            .filter_map(|id| store.get_value(&id).map(|item| (id, item)))
            .collect();
        crate::query::WindowedQuerySnapshot {
            entries,
            total_count,
            window: window.cloned(),
        }
    }

    /// Build a live canonical edge map with an index-seeded initial snapshot.
    ///
    /// The authority barrier closes the subscribe/snapshot race with the
    /// canonical mutation pipeline. Eager endpoints seed in O(bucket) time;
    /// demand-driven endpoints retain scan-equivalent behavior. Subsequent
    /// canonical diffs are routed by their old/new endpoints without rescanning.
    fn watch_matching<F>(
        &self,
        edge_type: &str,
        initial_ids: impl FnOnce(&Self) -> Vec<Arc<str>>,
        matches_endpoints: F,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<dyn AnyItem>, hyphae::CellImmutable>>
    where
        F: Fn(&EdgeEndpoints) -> bool + Send + Sync + 'static,
    {
        use hyphae::{Materialize, Signal, Watchable};

        let registration = self
            .registration(edge_type)
            .context("edge type is not registered")?;
        let store = self.registry.get_or_create(edge_type);
        let _authority = self.lock_authority();
        let initial_ids = initial_ids(self);
        let result = GraphWatchMap::new();
        let result_weak = result.downgrade();
        let store_keepalive = store.clone();
        let diffs = store.diffs().materialize();
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let guard = diffs.subscribe(move |signal| {
            let _ = &store_keepalive;
            if first.load(std::sync::atomic::Ordering::Relaxed)
                && first.swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return;
            }
            let Signal::Value(diff) = signal else {
                return;
            };
            let Some(result) = result_weak.upgrade() else {
                return;
            };
            if let Some(diff) =
                Self::watch_diff_matching(registration, &matches_endpoints, diff.as_ref())
            {
                result.apply_diff_owned(diff);
            }
        });
        result.own(guard);

        let entries = initial_ids
            .into_iter()
            .filter_map(|id| store.get_value(&id).map(|item| (id, item)))
            .collect();
        result.apply_diff_owned(GraphWatchDiff::Initial { entries });
        Ok(result.lock())
    }

    fn apply_watch_count_diff(
        registration: &EdgeRegistration,
        matches_endpoints: &impl Fn(&EdgeEndpoints) -> bool,
        diff: &GraphWatchDiff,
        count: &mut usize,
    ) {
        let matches = |item: &Arc<dyn AnyItem>| {
            (registration.extract)(item.as_ref())
                .is_ok_and(|endpoints| matches_endpoints(&endpoints))
        };
        match diff {
            GraphWatchDiff::Initial { entries } => {
                *count = entries.iter().filter(|(_, item)| matches(item)).count();
            }
            GraphWatchDiff::Insert { value, .. } => {
                if matches(value) {
                    *count = count.saturating_add(1);
                }
            }
            GraphWatchDiff::Remove { old_value, .. } => {
                if matches(old_value) {
                    *count = count.saturating_sub(1);
                }
            }
            GraphWatchDiff::Update {
                old_value,
                new_value,
                ..
            } => match (matches(old_value), matches(new_value)) {
                (true, false) => *count = count.saturating_sub(1),
                (false, true) => *count = count.saturating_add(1),
                (true, true) | (false, false) => {}
            },
            GraphWatchDiff::Batch { changes } => {
                for change in changes {
                    Self::apply_watch_count_diff(registration, matches_endpoints, change, count);
                }
            }
        }
    }

    /// Build a live scalar cardinality with an index-seeded initial value.
    ///
    /// Unlike [`Self::watch_matching`], this never loads matching edge items
    /// merely to count them. Canonical diffs adjust the scalar incrementally.
    fn watch_count_matching<F>(
        &self,
        edge_type: &str,
        initial_count: impl FnOnce(&Self) -> usize,
        matches_endpoints: F,
    ) -> Result<hyphae::Cell<usize, hyphae::CellImmutable>>
    where
        F: Fn(&EdgeEndpoints) -> bool + Send + Sync + 'static,
    {
        use hyphae::{Materialize, Mutable, Signal, Watchable};

        let registration = self
            .registration(edge_type)
            .context("edge type is not registered")?;
        let store = self.registry.get_or_create(edge_type);
        let _authority = self.lock_authority();
        let initial_count = initial_count(self);
        let result = hyphae::Cell::new(initial_count);
        let result_weak = result.downgrade();
        let count = Arc::new(Mutex::new(initial_count));
        let store_keepalive = store.clone();
        let diffs = store.diffs().materialize();
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let guard = diffs.subscribe(move |signal| {
            let _ = &store_keepalive;
            if first.load(std::sync::atomic::Ordering::Relaxed)
                && first.swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return;
            }
            let Signal::Value(diff) = signal else {
                return;
            };
            let Some(result) = result_weak.upgrade() else {
                return;
            };
            let next = {
                let mut count = count
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let previous = *count;
                Self::apply_watch_count_diff(
                    registration,
                    &matches_endpoints,
                    diff.as_ref(),
                    &mut count,
                );
                (*count != previous).then_some(*count)
            };
            if let Some(next) = next {
                result.set(next);
            }
        });
        result.own(guard);
        Ok(result.lock())
    }

    /// Build a live canonical edge map at one endpoint.
    pub(crate) fn watch_at(
        &self,
        edge_type: &str,
        position: EndPosition,
        endpoint: &EndpointValue,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<dyn AnyItem>, hyphae::CellImmutable>> {
        let endpoint_for_seed = endpoint.clone();
        let endpoint_for_diffs = endpoint.clone();
        self.watch_matching(
            edge_type,
            move |graph| graph.edge_ids_at(edge_type, position, &endpoint_for_seed),
            move |endpoints| {
                let candidate = match position {
                    EndPosition::A => &endpoints.a,
                    EndPosition::B => &endpoints.b,
                };
                *candidate == endpoint_for_diffs
            },
        )
    }

    pub(crate) fn watch_count_at(
        &self,
        edge_type: &str,
        position: EndPosition,
        endpoint: &EndpointValue,
    ) -> Result<hyphae::Cell<usize, hyphae::CellImmutable>> {
        let endpoint_for_seed = endpoint.clone();
        let endpoint_for_diffs = endpoint.clone();
        self.watch_count_matching(
            edge_type,
            move |graph| graph.edge_count_at(edge_type, position, &endpoint_for_seed),
            move |endpoints| {
                let candidate = match position {
                    EndPosition::A => &endpoints.a,
                    EndPosition::B => &endpoints.b,
                };
                *candidate == endpoint_for_diffs
            },
        )
    }

    pub(crate) fn watch_window_at(
        self: &Arc<Self>,
        edge_type: &'static str,
        position: EndPosition,
        endpoint: &EndpointValue,
        window: crate::wire::QueryWindow,
    ) -> Result<Option<crate::query::WindowedQuerySource>> {
        if !self.projects_endpoint(edge_type, position) {
            return Ok(None);
        }
        let endpoint_for_select = endpoint.clone();
        let endpoint_for_diffs = endpoint.clone();
        self.watch_window_matching(
            edge_type,
            window,
            move |graph, window| {
                graph.window_snapshot_at(edge_type, position, &endpoint_for_select, window)
            },
            move |endpoints| {
                let candidate = match position {
                    EndPosition::A => &endpoints.a,
                    EndPosition::B => &endpoints.b,
                };
                *candidate == endpoint_for_diffs
            },
        )
        .map(Some)
    }

    /// Build a live canonical edge map for one exact endpoint pair.
    pub(crate) fn watch_between(
        &self,
        edge_type: &str,
        a: &EndpointValue,
        b: &EndpointValue,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<dyn AnyItem>, hyphae::CellImmutable>> {
        let a_for_seed = a.clone();
        let b_for_seed = b.clone();
        let a_for_diffs = a.clone();
        let b_for_diffs = b.clone();
        let registration = self
            .registration(edge_type)
            .context("edge type is not registered")?;
        self.watch_matching(
            edge_type,
            move |graph| graph.edge_ids_between(edge_type, &a_for_seed, &b_for_seed),
            move |endpoints| {
                Self::endpoints_match(registration, endpoints, &a_for_diffs, &b_for_diffs)
            },
        )
    }

    pub(crate) fn watch_count_between(
        &self,
        edge_type: &str,
        a: &EndpointValue,
        b: &EndpointValue,
    ) -> Result<hyphae::Cell<usize, hyphae::CellImmutable>> {
        let a_for_seed = a.clone();
        let b_for_seed = b.clone();
        let a_for_diffs = a.clone();
        let b_for_diffs = b.clone();
        let registration = self
            .registration(edge_type)
            .context("edge type is not registered")?;
        self.watch_count_matching(
            edge_type,
            move |graph| graph.edge_count_between(edge_type, &a_for_seed, &b_for_seed),
            move |endpoints| {
                Self::endpoints_match(registration, endpoints, &a_for_diffs, &b_for_diffs)
            },
        )
    }

    pub(crate) fn watch_window_between(
        self: &Arc<Self>,
        edge_type: &'static str,
        a: &EndpointValue,
        b: &EndpointValue,
        window: crate::wire::QueryWindow,
    ) -> Result<Option<crate::query::WindowedQuerySource>> {
        let registration = self
            .registration(edge_type)
            .context("edge type is not registered")?;
        if !Self::projects_pairs(registration) {
            return Ok(None);
        }
        let a_for_select = a.clone();
        let b_for_select = b.clone();
        let a_for_diffs = a.clone();
        let b_for_diffs = b.clone();
        self.watch_window_matching(
            edge_type,
            window,
            move |graph, window| {
                graph.window_snapshot_between(edge_type, &a_for_select, &b_for_select, window)
            },
            move |endpoints| {
                Self::endpoints_match(registration, endpoints, &a_for_diffs, &b_for_diffs)
            },
        )
        .map(Some)
    }

    /// Build a live canonical edge map incident to an endpoint on either side.
    pub(crate) fn watch_incident(
        &self,
        edge_type: &str,
        endpoint: &EndpointValue,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<dyn AnyItem>, hyphae::CellImmutable>> {
        let endpoint_for_seed = endpoint.clone();
        let endpoint_for_diffs = endpoint.clone();
        self.watch_matching(
            edge_type,
            move |graph| {
                let mut ids = graph.edge_ids_at(edge_type, EndPosition::A, &endpoint_for_seed);
                ids.extend(graph.edge_ids_at(edge_type, EndPosition::B, &endpoint_for_seed));
                ids.sort();
                ids.dedup();
                ids
            },
            move |endpoints| endpoints.a == endpoint_for_diffs || endpoints.b == endpoint_for_diffs,
        )
    }

    #[must_use]
    pub fn edge_ids_between(
        &self,
        edge_type: &str,
        a: &EndpointValue,
        b: &EndpointValue,
    ) -> Vec<Arc<str>> {
        let Some(registration) = self.registration(edge_type) else {
            return Vec::new();
        };
        if Self::uses_unscoped_pair_fast_path(registration) {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = Self::pair_key_from_values(registration, a, b, None);
            return state
                .edge_types
                .get(edge_type)
                .and_then(|edges| edges.pairs.get(&key))
                .map_or_else(Vec::new, EdgeIds::ids);
        }
        if !self.projects_any_endpoint(edge_type) {
            return self.registry.get(edge_type).map_or_else(Vec::new, |store| {
                store
                    .snapshot()
                    .into_iter()
                    .filter_map(|(_, item)| {
                        let endpoints = (registration.extract)(item.as_ref()).ok()?;
                        Self::endpoints_match(registration, &endpoints, a, b).then(|| item.id())
                    })
                    .collect()
            });
        }
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(edges) = state.edge_types.get(edge_type) else {
            return Vec::new();
        };
        self.indexed_ids_between(registration, edges, a, b, None)
    }

    #[must_use]
    pub fn edge_count_at(
        &self,
        edge_type: &str,
        position: EndPosition,
        endpoint: &EndpointValue,
    ) -> usize {
        let Some(registration) = self.registration(edge_type) else {
            return 0;
        };
        if !self.projects_endpoint(edge_type, position) {
            return self.registry.get(edge_type).map_or(0, |store| {
                store
                    .snapshot()
                    .into_iter()
                    .filter(|(_, item)| {
                        (registration.extract)(item.as_ref()).is_ok_and(|endpoints| {
                            let candidate = match position {
                                EndPosition::A => endpoints.a,
                                EndPosition::B => endpoints.b,
                            };
                            candidate == *endpoint
                        })
                    })
                    .count()
            });
        }
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(edges) = state.edge_types.get(edge_type) else {
            return 0;
        };
        let map = match position {
            EndPosition::A => &edges.a,
            EndPosition::B => &edges.b,
        };
        map.get(endpoint).map_or(0, EdgeIds::len)
    }

    #[must_use]
    pub fn has_edge_at(
        &self,
        edge_type: &str,
        position: EndPosition,
        endpoint: &EndpointValue,
    ) -> bool {
        let Some(registration) = self.registration(edge_type) else {
            return false;
        };
        if !self.projects_endpoint(edge_type, position) {
            return self.registry.get(edge_type).is_some_and(|store| {
                store.snapshot().into_iter().any(|(_, item)| {
                    (registration.extract)(item.as_ref()).is_ok_and(|endpoints| {
                        let candidate = match position {
                            EndPosition::A => endpoints.a,
                            EndPosition::B => endpoints.b,
                        };
                        candidate == *endpoint
                    })
                })
            });
        }
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.edge_types.get(edge_type).is_some_and(|edges| {
            let map = match position {
                EndPosition::A => &edges.a,
                EndPosition::B => &edges.b,
            };
            map.get(endpoint).is_some_and(|ids| !ids.is_empty())
        })
    }

    #[must_use]
    pub fn edge_count_between(
        &self,
        edge_type: &str,
        a: &EndpointValue,
        b: &EndpointValue,
    ) -> usize {
        let Some(registration) = self.registration(edge_type) else {
            return 0;
        };
        if Self::uses_unscoped_pair_fast_path(registration) {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = Self::pair_key_from_values(registration, a, b, None);
            return state
                .edge_types
                .get(edge_type)
                .and_then(|edges| edges.pairs.get(&key))
                .map_or(0, EdgeIds::len);
        }
        self.edge_ids_between(edge_type, a, b).len()
    }

    #[must_use]
    pub fn has_edge_between(&self, edge_type: &str, a: &EndpointValue, b: &EndpointValue) -> bool {
        let Some(registration) = self.registration(edge_type) else {
            return false;
        };
        if Self::uses_unscoped_pair_fast_path(registration) {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = Self::pair_key_from_values(registration, a, b, None);
            return state
                .edge_types
                .get(edge_type)
                .and_then(|edges| edges.pairs.get(&key))
                .is_some_and(|ids| !ids.is_empty());
        }
        !self.edge_ids_between(edge_type, a, b).is_empty()
    }

    #[must_use]
    pub fn edge_ids_between_in_scope(
        &self,
        edge_type: &str,
        a: &EndpointValue,
        b: &EndpointValue,
        scope: &IndexValue,
    ) -> Vec<Arc<str>> {
        let Some(registration) = self.registration(edge_type) else {
            return Vec::new();
        };
        if Self::projects_pairs(registration) {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = Self::pair_key_from_values(registration, a, b, Some(scope.clone()));
            return state
                .edge_types
                .get(edge_type)
                .and_then(|edges| edges.pairs.get(&key))
                .map_or_else(Vec::new, EdgeIds::ids);
        }
        if !self.projects_any_endpoint(edge_type) {
            return self.registry.get(edge_type).map_or_else(Vec::new, |store| {
                store
                    .snapshot()
                    .into_iter()
                    .filter_map(|(_, item)| {
                        let endpoints = (registration.extract)(item.as_ref()).ok()?;
                        let item_scope = (registration.extract_scope)(item.as_ref()).ok()?;
                        (item_scope.as_ref() == Some(scope)
                            && Self::endpoints_match(registration, &endpoints, a, b))
                        .then(|| item.id())
                    })
                    .collect()
            });
        }
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(edges) = state.edge_types.get(edge_type) else {
            return Vec::new();
        };
        self.indexed_ids_between(registration, edges, a, b, Some(scope))
    }

    #[must_use]
    pub fn edge_count_between_in_scope(
        &self,
        edge_type: &str,
        a: &EndpointValue,
        b: &EndpointValue,
        scope: &IndexValue,
    ) -> usize {
        let Some(registration) = self.registration(edge_type) else {
            return 0;
        };
        if Self::projects_pairs(registration) {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = Self::pair_key_from_values(registration, a, b, Some(scope.clone()));
            return state
                .edge_types
                .get(edge_type)
                .and_then(|edges| edges.pairs.get(&key))
                .map_or(0, EdgeIds::len);
        }
        self.edge_ids_between_in_scope(edge_type, a, b, scope).len()
    }

    #[must_use]
    pub fn has_edge_between_in_scope(
        &self,
        edge_type: &str,
        a: &EndpointValue,
        b: &EndpointValue,
        scope: &IndexValue,
    ) -> bool {
        let Some(registration) = self.registration(edge_type) else {
            return false;
        };
        if Self::projects_pairs(registration) {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = Self::pair_key_from_values(registration, a, b, Some(scope.clone()));
            return state
                .edge_types
                .get(edge_type)
                .and_then(|edges| edges.pairs.get(&key))
                .is_some_and(|ids| !ids.is_empty());
        }
        !self
            .edge_ids_between_in_scope(edge_type, a, b, scope)
            .is_empty()
    }

    fn traversal_neighbors(
        &self,
        edge_type: &str,
        node: &EntityRef,
        direction: Direction,
        scope: Option<&IndexValue>,
    ) -> Vec<(Arc<str>, EntityRef)> {
        let Some(registration) = self.registration(edge_type) else {
            return Vec::new();
        };
        let shape = registration.shape;
        let needs_a = direction != Direction::Reverse || shape == EdgeShapeKind::Undirected;
        let needs_b = direction != Direction::Forward || shape == EdgeShapeKind::Undirected;
        let can_use_projection = (!needs_a || self.projects_endpoint(edge_type, EndPosition::A))
            && (!needs_b || self.projects_endpoint(edge_type, EndPosition::B));
        if !can_use_projection {
            return self.registry.get(edge_type).map_or_else(Vec::new, |store| {
                store
                    .snapshot()
                    .into_iter()
                    .filter_map(|(_, item)| {
                        let endpoints = (registration.extract)(item.as_ref()).ok()?;
                        let edge_scope = (registration.extract_scope)(item.as_ref()).ok()?;
                        if scope.is_some() && edge_scope.as_ref() != scope {
                            return None;
                        }
                        let neighbor = if endpoints.a.entity == *node
                            && (direction != Direction::Reverse
                                || registration.shape == EdgeShapeKind::Undirected)
                        {
                            endpoints.b.entity
                        } else if endpoints.b.entity == *node
                            && (direction != Direction::Forward
                                || registration.shape == EdgeShapeKind::Undirected)
                        {
                            endpoints.a.entity
                        } else {
                            return None;
                        };
                        Some((item.id(), neighbor))
                    })
                    .collect()
            });
        }
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(edges) = state.edge_types.get(edge_type) else {
            return Vec::new();
        };
        let mut candidates = BTreeSet::new();
        if (direction != Direction::Reverse || shape == EdgeShapeKind::Undirected)
            && let Some(ids) = edges.a_entities.get(node)
        {
            ids.extend_cloned(&mut candidates);
        }
        if (direction != Direction::Forward || shape == EdgeShapeKind::Undirected)
            && let Some(ids) = edges.b_entities.get(node)
        {
            ids.extend_cloned(&mut candidates);
        }
        candidates
            .into_iter()
            .filter_map(|id| {
                let edge = edges.edges.get(&id)?;
                if scope.is_some() && edge.scope.as_ref() != scope {
                    return None;
                }
                let neighbor = if edge.endpoints.a.entity == *node
                    && (direction != Direction::Reverse || shape == EdgeShapeKind::Undirected)
                {
                    edge.endpoints.b.entity.clone()
                } else if edge.endpoints.b.entity == *node
                    && (direction != Direction::Forward || shape == EdgeShapeKind::Undirected)
                {
                    edge.endpoints.a.entity.clone()
                } else {
                    return None;
                };
                Some((id, neighbor))
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Direction {
    #[default]
    Forward,
    Reverse,
    Both,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TraversalResult {
    pub nodes: Vec<EntityRef>,
    pub edge_ids: Vec<Arc<str>>,
    pub truncated: bool,
}

pub struct TraversalBuilder<'a, E>
where
    E: GraphEdge,
    E::Ends: TypedEdgeEnds,
{
    context: &'a crate::server::MykoServerContext,
    start: Option<EntityRef>,
    direction: Direction,
    scope: Option<IndexValue>,
    max_depth: Option<usize>,
    max_nodes: Option<usize>,
    marker: PhantomData<E>,
}

impl<'a, E> TraversalBuilder<'a, E>
where
    E: GraphEdge,
    E::Ends: TypedEdgeEnds,
{
    pub(crate) const fn new(context: &'a crate::server::MykoServerContext) -> Self {
        Self {
            context,
            start: None,
            direction: Direction::Forward,
            scope: None,
            max_depth: None,
            max_nodes: None,
            marker: PhantomData,
        }
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn start(mut self, value: <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value) -> Self {
        self.start = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(&value)
            .ok()
            .map(|endpoint| endpoint.entity);
        self
    }

    /// Explicit endpoint-A spelling for forward-oriented traversals.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn start_from(self, value: <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value) -> Self {
        self.start(value)
    }

    /// Start from a typed endpoint-B value, typically with reverse traversal.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn start_to(
        mut self,
        value: <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Self {
        self.start = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(&value)
            .ok()
            .map(|endpoint| endpoint.entity);
        self
    }

    #[must_use]
    pub const fn direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }

    /// Restrict traversal to one typed scope value.
    pub fn within_scope<T: Serialize>(mut self, scope: T) -> Result<Self> {
        self.scope = Some(IndexValue::from_serializable(&scope)?);
        Ok(self)
    }

    #[must_use]
    pub const fn max_depth(mut self, max_depth: usize) -> Self {
        self.max_depth = Some(max_depth);
        self
    }

    #[must_use]
    pub const fn max_nodes(mut self, max_nodes: usize) -> Self {
        self.max_nodes = Some(max_nodes);
        self
    }

    /// Execute a bounded breadth-first traversal.
    pub fn execute(self) -> Result<TraversalResult> {
        let start = self.start.context("traversal start endpoint is invalid")?;
        let max_depth = self.max_depth.context("traversal max_depth is required")?;
        let max_nodes = self.max_nodes.context("traversal max_nodes is required")?;
        if max_nodes == 0 {
            bail!("traversal max_nodes must be greater than zero");
        }
        let graph = self
            .context
            .graph_index()
            .map(AsRef::as_ref)
            .context("application has no graph registrations")?;
        let mut visited = HashSet::from([start.clone()]);
        let mut queue = std::collections::VecDeque::from([(start.clone(), 0_usize)]);
        let mut edge_ids = BTreeSet::new();
        let mut truncated = false;
        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            for (edge_id, neighbor) in graph.traversal_neighbors(
                E::ENTITY_NAME_STATIC,
                &node,
                self.direction,
                self.scope.as_ref(),
            ) {
                edge_ids.insert(edge_id);
                if visited.insert(neighbor.clone()) {
                    if visited.len() >= max_nodes {
                        truncated = true;
                        queue.clear();
                        break;
                    }
                    queue.push_back((neighbor, depth.saturating_add(1)));
                }
            }
        }
        visited.remove(&start);
        let mut nodes = visited.into_iter().collect::<Vec<_>>();
        nodes.sort();
        Ok(TraversalResult {
            nodes,
            edge_ids: edge_ids.into_iter().collect(),
            truncated,
        })
    }
}

/// Typed one-hop access to one registered edge item type.
pub struct EdgeQuery<'a, E> {
    context: &'a crate::server::MykoServerContext,
    marker: PhantomData<E>,
}

impl<'a, E> EdgeQuery<'a, E>
where
    E: GraphEdge,
    E::Ends: TypedEdgeEnds,
{
    pub(crate) const fn new(context: &'a crate::server::MykoServerContext) -> Self {
        Self {
            context,
            marker: PhantomData,
        }
    }

    fn graph(&self) -> Result<&GraphIndex> {
        self.context
            .graph_index()
            .map(AsRef::as_ref)
            .context("application has no graph registrations")
    }

    fn materialize(&self, ids: Vec<Arc<str>>) -> Vec<Arc<E>> {
        let Some(store) = self.context.registry.get(E::ENTITY_NAME_STATIC) else {
            return Vec::new();
        };
        ids.into_iter()
            .filter_map(|id| store.get_value(&id))
            .filter_map(|item| crate::item::downcast_any_item_arc::<E>(&item, "EdgeQuery"))
            .collect()
    }

    fn materialize_one(&self, id: &Arc<str>) -> Option<Arc<E>> {
        self.context
            .registry
            .get(E::ENTITY_NAME_STATIC)
            .and_then(|store| store.get_value(id))
            .and_then(|item| crate::item::downcast_any_item_arc::<E>(&item, "EdgeQuery"))
    }

    /// Edge IDs at endpoint A without loading or downcasting edge items.
    pub fn from_ids(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<str>>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?;
        Ok(self
            .graph()?
            .edge_ids_at(E::ENTITY_NAME_STATIC, EndPosition::A, &endpoint))
    }

    /// Number of edges at endpoint A without materializing edge items.
    pub fn count_from(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<usize> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?;
        Ok(self
            .graph()?
            .edge_count_at(E::ENTITY_NAME_STATIC, EndPosition::A, &endpoint))
    }

    /// Whether any edge exists at endpoint A without materializing edge items.
    pub fn exists_from(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<bool> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?;
        Ok(self
            .graph()?
            .has_edge_at(E::ENTITY_NAME_STATIC, EndPosition::A, &endpoint))
    }

    /// Directed-style lookup at endpoint A (`from`).
    pub fn from(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        Ok(self.materialize(self.from_ids(value)?))
    }

    /// Qualified-address spelling for [`Self::from`].
    pub fn from_at(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        self.from(value)
    }

    /// Directed-style lookup at endpoint B (`to`).
    pub fn to(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        Ok(self.materialize(self.to_ids(value)?))
    }

    /// Edge IDs at endpoint B without loading or downcasting edge items.
    pub fn to_ids(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<str>>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(value)?;
        Ok(self
            .graph()?
            .edge_ids_at(E::ENTITY_NAME_STATIC, EndPosition::B, &endpoint))
    }

    /// Number of edges at endpoint B without materializing edge items.
    pub fn count_to(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<usize> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(value)?;
        Ok(self
            .graph()?
            .edge_count_at(E::ENTITY_NAME_STATIC, EndPosition::B, &endpoint))
    }

    /// Whether any edge exists at endpoint B without materializing edge items.
    pub fn exists_to(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<bool> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(value)?;
        Ok(self
            .graph()?
            .has_edge_at(E::ENTITY_NAME_STATIC, EndPosition::B, &endpoint))
    }

    /// Qualified-address spelling for [`Self::to`].
    pub fn to_at(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        self.to(value)
    }

    /// Exact A/B lookup, using the pair projection when one is configured.
    pub fn between(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        Ok(self.materialize(self.between_ids(a, b)?))
    }

    /// Exact A/B edge IDs without loading or downcasting edge items.
    pub fn between_ids(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<str>>> {
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a)?;
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b)?;
        Ok(self
            .graph()?
            .edge_ids_between(E::ENTITY_NAME_STATIC, &a, &b))
    }

    /// Number of exact A/B edges without materializing edge items.
    pub fn count_between(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<usize> {
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a)?;
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b)?;
        Ok(self
            .graph()?
            .edge_count_between(E::ENTITY_NAME_STATIC, &a, &b))
    }

    /// Whether an exact A/B edge exists without materializing edge items.
    pub fn exists_between(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<bool> {
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a)?;
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b)?;
        Ok(self
            .graph()?
            .has_edge_between(E::ENTITY_NAME_STATIC, &a, &b))
    }

    /// The edge ID for a unique exact pair.
    pub fn between_id(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Option<Arc<str>>> {
        if E::PAIR_POLICY != PairPolicy::Unique {
            bail!("{} does not declare unique pairs", E::ENTITY_NAME_STATIC);
        }
        Ok(self.between_ids(a, b)?.into_iter().next())
    }

    /// The materialized edge for a unique exact pair.
    pub fn one_between(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Option<Arc<E>>> {
        Ok(self
            .between_id(a, b)?
            .as_ref()
            .and_then(|id| self.materialize_one(id)))
    }

    /// Exact A/B lookup within one typed edge scope.
    pub fn between_in_scope(
        &self,
        scope: &<E::Scope as EdgeScope>::Value,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        Ok(self.materialize(self.between_ids_in_scope(scope, a, b)?))
    }

    /// Exact scoped A/B edge IDs without loading or downcasting edge items.
    pub fn between_ids_in_scope(
        &self,
        scope: &<E::Scope as EdgeScope>::Value,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<str>>> {
        let scope = E::Scope::erase(scope)?;
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a)?;
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b)?;
        Ok(self
            .graph()?
            .edge_ids_between_in_scope(E::ENTITY_NAME_STATIC, &a, &b, &scope))
    }

    /// Number of exact scoped A/B edges without materializing edge items.
    pub fn count_between_in_scope(
        &self,
        scope: &<E::Scope as EdgeScope>::Value,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<usize> {
        let scope = E::Scope::erase(scope)?;
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a)?;
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b)?;
        Ok(self
            .graph()?
            .edge_count_between_in_scope(E::ENTITY_NAME_STATIC, &a, &b, &scope))
    }

    /// Whether an exact scoped A/B edge exists without materializing edge items.
    pub fn exists_between_in_scope(
        &self,
        scope: &<E::Scope as EdgeScope>::Value,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<bool> {
        let scope = E::Scope::erase(scope)?;
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a)?;
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b)?;
        Ok(self
            .graph()?
            .has_edge_between_in_scope(E::ENTITY_NAME_STATIC, &a, &b, &scope))
    }

    /// The edge ID for a unique exact pair within one typed scope.
    pub fn between_id_in_scope(
        &self,
        scope: &<E::Scope as EdgeScope>::Value,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Option<Arc<str>>> {
        if E::PAIR_POLICY != PairPolicy::Unique {
            bail!("{} does not declare unique pairs", E::ENTITY_NAME_STATIC);
        }
        Ok(self.between_ids_in_scope(scope, a, b)?.into_iter().next())
    }

    /// The materialized edge for a unique exact pair within one typed scope.
    pub fn one_between_in_scope(
        &self,
        scope: &<E::Scope as EdgeScope>::Value,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Option<Arc<E>>> {
        Ok(self
            .between_id_in_scope(scope, a, b)?
            .as_ref()
            .and_then(|id| self.materialize_one(id)))
    }

    fn watch_at(
        &self,
        position: EndPosition,
        endpoint: &EndpointValue,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<E>, hyphae::CellImmutable>> {
        let selected = self
            .graph()?
            .watch_at(E::ENTITY_NAME_STATIC, position, endpoint)?;
        Ok(crate::item::typed_map_arc_from_any_item(
            selected,
            "EdgeQuery::watch_at",
        ))
    }

    /// Reactive counterpart of [`Self::from`].
    pub fn watch_from(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<E>, hyphae::CellImmutable>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?;
        self.watch_at(EndPosition::A, &endpoint)
    }

    /// Reactive cardinality counterpart of [`Self::count_from`].
    pub fn watch_count_from(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<hyphae::Cell<usize, hyphae::CellImmutable>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?;
        self.graph()?
            .watch_count_at(E::ENTITY_NAME_STATIC, EndPosition::A, &endpoint)
    }

    /// Reactive counterpart of [`Self::to`].
    pub fn watch_to(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<E>, hyphae::CellImmutable>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(value)?;
        self.watch_at(EndPosition::B, &endpoint)
    }

    /// Reactive cardinality counterpart of [`Self::count_to`].
    pub fn watch_count_to(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<hyphae::Cell<usize, hyphae::CellImmutable>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(value)?;
        self.graph()?
            .watch_count_at(E::ENTITY_NAME_STATIC, EndPosition::B, &endpoint)
    }

    /// Reactive counterpart of [`Self::between`].
    pub fn watch_between(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<E>, hyphae::CellImmutable>> {
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a)?;
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b)?;
        let selected = self.graph()?.watch_between(E::ENTITY_NAME_STATIC, &a, &b)?;
        Ok(crate::item::typed_map_arc_from_any_item(
            selected,
            "EdgeQuery::watch_between",
        ))
    }

    /// Reactive cardinality counterpart of [`Self::count_between`].
    pub fn watch_count_between(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<hyphae::Cell<usize, hyphae::CellImmutable>> {
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a)?;
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b)?;
        self.graph()?
            .watch_count_between(E::ENTITY_NAME_STATIC, &a, &b)
    }

    /// Qualified-address spelling for [`Self::watch_from`].
    pub fn watch_from_at(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<E>, hyphae::CellImmutable>> {
        self.watch_from(value)
    }

    /// Qualified-address spelling for [`Self::watch_to`].
    pub fn watch_to_at(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<E>, hyphae::CellImmutable>> {
        self.watch_to(value)
    }
}

impl<E> EdgeQuery<'_, E>
where
    E: GraphEdge,
    E::Ends: TypedEdgeEnds<B = <E::Ends as TypedEdgeEnds>::A>,
{
    /// Undirected-style lookup across both endpoint positions.
    pub fn incident(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?;
        let graph = self.graph()?;
        let mut ids = graph.edge_ids_at(E::ENTITY_NAME_STATIC, EndPosition::A, &endpoint);
        ids.extend(graph.edge_ids_at(E::ENTITY_NAME_STATIC, EndPosition::B, &endpoint));
        ids.sort();
        ids.dedup();
        Ok(self.materialize(ids))
    }

    /// Reactive undirected-style lookup across both endpoint positions.
    pub fn watch_incident(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<E>, hyphae::CellImmutable>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?;
        let selected = self
            .graph()?
            .watch_incident(E::ENTITY_NAME_STATIC, &endpoint)?;
        Ok(crate::item::typed_map_arc_from_any_item(
            selected,
            "EdgeQuery::watch_incident",
        ))
    }
}

impl crate::server::MykoServerContext {
    /// Select a registered graph edge type for typed one-hop operations.
    #[must_use]
    pub const fn edges<E>(&self) -> EdgeQuery<'_, E>
    where
        E: GraphEdge,
        E::Ends: TypedEdgeEnds,
    {
        EdgeQuery::new(self)
    }

    /// Start a bounded traversal over one registered edge type.
    #[must_use]
    pub const fn traverse<E>(&self) -> TraversalBuilder<'_, E>
    where
        E: GraphEdge,
        E::Ends: TypedEdgeEnds,
    {
        TraversalBuilder::new(self)
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::redundant_clone,
        clippy::too_many_lines
    )]
    use std::sync::Arc;

    use super::EdgeIds;
    use crate::prelude::*;
    use crate::{
        search::SearchIndex,
        server::{HandlerRegistry, MykoServerRuntime, PersisterRouter, RelationshipManager},
        store::StoreRegistry,
    };

    #[myko_category]
    pub struct TagTarget;

    mod article {
        use std::sync::Arc;

        use super::TagTarget;
        use crate::prelude::*;

        #[myko_in(TagTarget)]
        #[myko_item]
        pub struct Article {
            pub title: Arc<str>,
        }
    }
    use article::{Article, ArticleId};

    mod tag {
        use std::sync::Arc;

        use crate::prelude::*;

        #[myko_item]
        pub struct Tag {
            pub name: Arc<str>,
        }
    }
    use tag::{Tag, TagId};

    mod graph_scope {
        use crate::prelude::*;

        #[myko_item]
        pub struct GraphScope {
            pub name: String,
        }
    }
    use graph_scope::{GraphScope, GraphScopeId};

    mod assignment {
        use super::{Tag, TagId, TagTarget};
        use crate::prelude::*;

        #[myko_item]
        pub struct TagAssignment {
            #[belongs_to(Tag)]
            pub tag_id: TagId,
            pub target: EntityRef,
        }

        #[myko_edge]
        impl GraphEdge for TagAssignment {
            type Ends = Directed<ConcreteEndpoint<Tag>, CategoryEndpoint<TagTarget>>;

            fn ends(&self) -> (TagId, EntityRef) {
                (self.tag_id.clone(), self.target.clone())
            }

            const PAIR_POLICY: PairPolicy = PairPolicy::Unique;
        }
    }
    use assignment::{ConnectTagAssignment, EnsureTagAssignment, TagAssignment, TagAssignmentId};

    mod scoped_assignment {
        use super::{Article, ArticleId, GraphScope, GraphScopeId, Tag, TagId};
        use crate::prelude::*;

        #[myko_item]
        pub struct ScopedTagAssignment {
            pub scope_id: GraphScopeId,
            pub tag_id: TagId,
            pub article_id: ArticleId,
        }

        #[myko_edge]
        impl GraphEdge for ScopedTagAssignment {
            type Ends = Directed<ConcreteEndpoint<Tag>, ConcreteEndpoint<Article>>;
            type Scope = ConcreteScope<GraphScope>;

            fn ends(&self) -> (TagId, ArticleId) {
                (self.tag_id.clone(), self.article_id.clone())
            }

            fn scope(&self) -> Option<GraphScopeId> {
                Some(self.scope_id.clone())
            }

            const PAIR_POLICY: PairPolicy = PairPolicy::Unique;
        }
    }
    use scoped_assignment::{
        EnsureScopedTagAssignment, ScopedTagAssignment, ScopedTagAssignmentId,
    };

    mod restricted_edge {
        use super::{Tag, TagId, TagTarget};
        use crate::prelude::*;

        #[myko_item]
        pub struct RestrictedTagAssignment {
            pub tag_id: TagId,
            pub target: EntityRef,
        }

        #[myko_edge]
        impl GraphEdge for RestrictedTagAssignment {
            type Ends = Directed<ConcreteEndpoint<Tag>, CategoryEndpoint<TagTarget>>;

            fn ends(&self) -> (TagId, EntityRef) {
                (self.tag_id.clone(), self.target.clone())
            }

            const ADJACENCY: AdjacencyPolicy = AdjacencyPolicy::Eager;
            const A_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::RestrictEndpointDelete;
        }
    }

    mod retained_edge {
        use super::{Tag, TagId, TagTarget};
        use crate::prelude::*;

        #[myko_item]
        pub struct RetainedTagAssignment {
            pub tag_id: TagId,
            pub target: EntityRef,
        }

        #[myko_edge]
        impl GraphEdge for RetainedTagAssignment {
            type Ends = Directed<ConcreteEndpoint<Tag>, CategoryEndpoint<TagTarget>>;

            fn ends(&self) -> (TagId, EntityRef) {
                (self.tag_id.clone(), self.target.clone())
            }

            const A_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::RetainDangling;
        }
    }
    mod forward_indexed_edge {
        use super::{Article, ArticleId, Tag, TagId};
        use crate::prelude::*;

        #[myko_item]
        pub struct ForwardIndexedAssignment {
            pub tag_id: TagId,
            pub article_id: ArticleId,
        }

        #[myko_edge]
        impl GraphEdge for ForwardIndexedAssignment {
            type Ends = Directed<ConcreteEndpoint<Tag>, ConcreteEndpoint<Article>>;

            fn ends(&self) -> (TagId, ArticleId) {
                (self.tag_id.clone(), self.article_id.clone())
            }

            const A_ADJACENCY: AdjacencyPolicy = AdjacencyPolicy::Eager;
        }
    }
    use forward_indexed_edge::{
        ForwardIndexedAssignment, ForwardIndexedAssignmentGraphBetween,
        ForwardIndexedAssignmentGraphCountBetween, ForwardIndexedAssignmentGraphCountFrom,
        ForwardIndexedAssignmentGraphCountTo, ForwardIndexedAssignmentGraphExistsBetween,
        ForwardIndexedAssignmentGraphFrom, ForwardIndexedAssignmentGraphTo,
        ForwardIndexedAssignmentId,
    };
    use restricted_edge::{RestrictedTagAssignment, RestrictedTagAssignmentId};
    use retained_edge::{RetainedTagAssignment, RetainedTagAssignmentId};

    fn context() -> crate::server::MykoServerContext {
        crate::server::MykoServerContext::new(
            Uuid::new_v4(),
            Arc::new(StoreRegistry::new()),
            Arc::new(HandlerRegistry::new()),
            Arc::new(RelationshipManager::new()),
            Arc::new(PersisterRouter::default()),
            Arc::new(SearchIndex::new()),
            MykoServerRuntime {
                peer_clients: Arc::new(dashmap::DashMap::new()),
                event_sink: None,
                history_replay: None,
            },
        )
    }

    #[test]
    fn macros_emit_separate_graph_registrations() {
        fn accepts_target<T: InCategory<TagTarget>>() {}
        accepts_target::<Article>();

        let catalog = GraphSchemaCatalog::collect("myko");
        assert!(
            catalog
                .entity_categories
                .iter()
                .any(|entry| entry.name == "TagTarget")
        );
        assert!(catalog.item_categories.iter().any(|entry| {
            entry.item_type == "Article" && entry.entity_category_id.ends_with("::TagTarget")
        }));

        let edge = catalog
            .edges
            .iter()
            .find(|entry| entry.edge_type == "TagAssignment")
            .copied()
            .expect("TagAssignment graph registration");
        assert_eq!(edge.shape, EdgeShapeKind::Directed);
        assert_eq!(edge.pair_policy, PairPolicy::Unique);
        assert_eq!(edge.adjacency, AdjacencyPolicy::DemandDriven);
        assert_eq!(
            edge.endpoint_adjacency(),
            [AdjacencyPolicy::DemandDriven; 2]
        );
        assert!(edge.validate.is_none());

        let forward = catalog
            .edges
            .iter()
            .find(|entry| entry.edge_type == "ForwardIndexedAssignment")
            .copied()
            .expect("forward-indexed graph registration");
        assert_eq!(forward.adjacency, AdjacencyPolicy::DemandDriven);
        assert_eq!(
            forward.endpoint_adjacency(),
            [AdjacencyPolicy::Eager, AdjacencyPolicy::DemandDriven]
        );

        let handlers = HandlerRegistry::new();
        assert!(
            handlers
                .query("ForwardIndexedAssignmentGraphFrom")
                .is_some_and(|query| query.window_cell_factory.is_some())
        );
        assert!(handlers.query("ForwardIndexedAssignmentGraphTo").is_some());
        assert!(
            handlers
                .query("ForwardIndexedAssignmentGraphBetween")
                .is_some()
        );
        assert!(
            handlers
                .report("ForwardIndexedAssignmentGraphCountFrom")
                .is_some()
        );
        assert!(
            handlers
                .report("ForwardIndexedAssignmentGraphExistsBetween")
                .is_some()
        );
    }

    #[test]
    fn one_sided_adjacency_indexes_only_the_hot_end() {
        let context = context();
        let tag = Tag {
            name: "forward".into(),
            id: TagId::from("tag-forward"),
        };
        let article = Article {
            title: "One-sided projection".into(),
            id: ArticleId::from("article-forward"),
        };
        let edge = ForwardIndexedAssignment {
            tag_id: tag.id.clone(),
            article_id: article.id.clone(),
            id: ForwardIndexedAssignmentId::from("forward-edge"),
        };
        assert!(context.set(&tag).is_ok());
        assert!(context.set(&article).is_ok());
        assert!(context.set(&edge).is_ok());

        let graph = context.graph_index().expect("graph index");
        {
            let state = graph
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let projected = state
                .edge_types
                .get("ForwardIndexedAssignment")
                .expect("projected edge state");
            assert_eq!(projected.a.len(), 1);
            assert_eq!(projected.a_entities.len(), 1);
            assert!(matches!(projected.a.values().next(), Some(EdgeIds::One(_))));
            assert!(matches!(
                projected.a_entities.values().next(),
                Some(EdgeIds::One(_))
            ));
            assert!(projected.b.is_empty());
            assert!(projected.b_entities.is_empty());
            assert_eq!(projected.edges.len(), 1);
        }

        let query = context.edges::<ForwardIndexedAssignment>();
        assert_eq!(
            query.from_ids(&tag.id).expect("eager A lookup"),
            vec![edge.id()]
        );
        assert_eq!(
            query.to_ids(&article.id).expect("demand B lookup"),
            vec![edge.id()]
        );
        assert_eq!(
            query
                .between_ids(&tag.id, &article.id)
                .expect("one-sided pair lookup"),
            vec![edge.id()]
        );
        assert_eq!(
            context
                .traverse::<ForwardIndexedAssignment>()
                .start(tag.id.clone())
                .max_depth(1)
                .max_nodes(10)
                .execute()
                .expect("projected forward traversal")
                .nodes,
            vec![EntityRef::from(&article)]
        );
        assert_eq!(
            context
                .traverse::<ForwardIndexedAssignment>()
                .start_to(article.id.clone())
                .direction(Direction::Reverse)
                .max_depth(1)
                .max_nodes(10)
                .execute()
                .expect("demand reverse traversal")
                .nodes,
            vec![EntityRef::from(&tag)]
        );
        assert_eq!(
            graph
                .endpoint_delete_plan(&EntityRef::from(&tag))
                .expect("indexed A delete plan")
                .cascade_edges
                .len(),
            1
        );
        assert_eq!(
            graph
                .endpoint_delete_plan(&EntityRef::from(&article))
                .expect("demand B delete plan")
                .cascade_edges
                .len(),
            1
        );
    }

    #[test]
    fn eager_watch_is_seeded_and_tracks_edges_moving_in_and_out() {
        let _serial = crate::test_util::scheduler_test_serial();
        let context = context();
        let watched_tag = Tag {
            name: "watched".into(),
            id: TagId::from("tag-watched"),
        };
        let other_tag = Tag {
            name: "other".into(),
            id: TagId::from("tag-other"),
        };
        let article = Article {
            title: "Reactive adjacency".into(),
            id: ArticleId::from("article-watched"),
        };
        let edge = ForwardIndexedAssignment {
            tag_id: watched_tag.id.clone(),
            article_id: article.id.clone(),
            id: ForwardIndexedAssignmentId::from("forward-watch-edge"),
        };
        assert!(context.set(&watched_tag).is_ok());
        assert!(context.set(&other_tag).is_ok());
        assert!(context.set(&article).is_ok());
        assert!(context.set(&edge).is_ok());

        let watched = context
            .edges::<ForwardIndexedAssignment>()
            .watch_from(&watched_tag.id)
            .expect("index-seeded watch");
        assert_eq!(
            watched.snapshot(),
            vec![(edge.id(), Arc::new(edge.clone()))]
        );

        let moved_away = ForwardIndexedAssignment {
            tag_id: other_tag.id.clone(),
            ..edge.clone()
        };
        assert!(context.set(&moved_away).is_ok());
        assert!(watched.snapshot().is_empty());

        assert!(context.set(&edge).is_ok());
        assert_eq!(
            watched.snapshot(),
            vec![(edge.id(), Arc::new(edge.clone()))]
        );

        assert!(context.del(&edge).is_ok());
        assert!(watched.snapshot().is_empty());
    }

    #[test]
    fn eager_window_watch_selects_only_the_page_and_tracks_page_shifts() {
        let _serial = crate::test_util::scheduler_test_serial();
        let context = context();
        let tag = Tag {
            name: "windowed".into(),
            id: TagId::from("tag-windowed"),
        };
        assert!(context.set(&tag).is_ok());
        for id in ["b", "c", "d"] {
            let article = Article {
                title: id.into(),
                id: ArticleId::from(format!("article-{id}")),
            };
            let edge = ForwardIndexedAssignment {
                tag_id: tag.id.clone(),
                article_id: article.id.clone(),
                id: ForwardIndexedAssignmentId::from(id),
            };
            assert!(context.set(&article).is_ok());
            assert!(context.set(&edge).is_ok());
        }

        let endpoint =
            <ConcreteEndpoint<Tag> as EndpointSpec>::erase(&tag.id).expect("erase tag endpoint");
        let graph = context.graph_index().expect("graph index");
        let source = graph
            .watch_window_at(
                ForwardIndexedAssignment::ENTITY_NAME_STATIC,
                EndPosition::A,
                &endpoint,
                crate::wire::QueryWindow {
                    offset: 1,
                    limit: 1,
                },
            )
            .expect("window watch")
            .expect("eager endpoint pushdown");
        let initial = source.snapshots().get();
        assert_eq!(initial.total_count, 3);
        assert_eq!(initial.entries.len(), 1);
        assert_eq!(initial.entries[0].0.as_ref(), "c");

        let first_article = Article {
            title: "a".into(),
            id: ArticleId::from("article-a"),
        };
        let first_edge = ForwardIndexedAssignment {
            tag_id: tag.id.clone(),
            article_id: first_article.id.clone(),
            id: ForwardIndexedAssignmentId::from("a"),
        };
        assert!(context.set(&first_article).is_ok());
        assert!(context.set(&first_edge).is_ok());
        let shifted = source.snapshots().get();
        assert_eq!(shifted.total_count, 4);
        assert_eq!(shifted.entries.len(), 1);
        assert_eq!(shifted.entries[0].0.as_ref(), "b");

        source.set_window(Some(crate::wire::QueryWindow {
            offset: 2,
            limit: 1,
        }));
        let moved = source.snapshots().get();
        assert_eq!(moved.entries.len(), 1);
        assert_eq!(moved.entries[0].0.as_ref(), "c");

        let demand = graph
            .watch_window_at(
                ForwardIndexedAssignment::ENTITY_NAME_STATIC,
                EndPosition::B,
                &<ConcreteEndpoint<Article> as EndpointSpec>::erase(&first_article.id)
                    .expect("erase article endpoint"),
                crate::wire::QueryWindow {
                    offset: 0,
                    limit: 1,
                },
            )
            .expect("demand-driven fallback");
        assert!(demand.is_none());
    }

    #[test]
    fn generated_graph_queries_use_the_live_indexed_path() {
        let _serial = crate::test_util::scheduler_test_serial();
        let context = context();
        let tag = Tag {
            name: "query".into(),
            id: TagId::from("tag-query"),
        };
        let other_tag = Tag {
            name: "other".into(),
            id: TagId::from("tag-query-other"),
        };
        let article = Article {
            title: "Generated query".into(),
            id: ArticleId::from("article-query"),
        };
        let edge = ForwardIndexedAssignment {
            tag_id: tag.id.clone(),
            article_id: article.id.clone(),
            id: ForwardIndexedAssignmentId::from("generated-query-edge"),
        };
        assert!(context.set(&tag).is_ok());
        assert!(context.set(&other_tag).is_ok());
        assert!(context.set(&article).is_ok());
        assert!(context.set(&edge).is_ok());

        let request = || {
            Arc::new(crate::request::RequestContext::from_client(
                Uuid::new_v4().to_string().into(),
                "graph-test".into(),
                context.host_id,
            ))
        };
        let from_request = request();
        let handlers = HandlerRegistry::new();
        let from_handler = handlers
            .query("ForwardIndexedAssignmentGraphFrom")
            .expect("generated graph query handler");
        let encoded = serde_json::to_value(crate::query::QueryRequest::with_tx(
            ForwardIndexedAssignmentGraphFrom::new(tag.id.clone()),
            from_request.tx.clone(),
        ))
        .expect("serialize generated graph query");
        let parsed = (from_handler.parse)(encoded).expect("parse generated graph query");
        let from = (from_handler.cell_factory)(
            parsed,
            context.registry.clone(),
            from_request,
            Some(Arc::new(context.clone())),
        )
        .expect("dispatch generated graph query");
        let to = context.query_map_by_str(
            ForwardIndexedAssignmentGraphTo::new(article.id.clone()),
            request(),
        );
        let between = context.query_map_by_str(
            ForwardIndexedAssignmentGraphBetween::new(tag.id.clone(), article.id.clone()),
            request(),
        );
        assert_eq!(from.snapshot().len(), 1);
        assert_eq!(to.snapshot().len(), 1);
        assert_eq!(between.snapshot().len(), 1);

        let moved = ForwardIndexedAssignment {
            tag_id: other_tag.id.clone(),
            ..edge.clone()
        };
        assert!(context.set(&moved).is_ok());
        assert!(from.snapshot().is_empty());
        assert_eq!(to.snapshot().len(), 1);
        assert!(between.snapshot().is_empty());
    }

    #[test]
    fn generated_graph_client_helpers_preserve_endpoint_and_item_types() {
        let from: fn(&MykoClient, &TagId) -> QueryMapWatch<ForwardIndexedAssignment> =
            MykoClient::watch_graph_from::<ForwardIndexedAssignment>;
        let to: fn(&MykoClient, &ArticleId) -> QueryMapWatch<ForwardIndexedAssignment> =
            MykoClient::watch_graph_to::<ForwardIndexedAssignment>;
        let between: fn(
            &MykoClient,
            &TagId,
            &ArticleId,
        ) -> QueryMapWatch<ForwardIndexedAssignment> =
            MykoClient::watch_graph_between::<ForwardIndexedAssignment>;
        let count_from: fn(
            &MykoClient,
            &TagId,
        ) -> hyphae::Cell<Option<usize>, hyphae::CellImmutable> =
            MykoClient::watch_graph_count_from::<ForwardIndexedAssignment>;
        let count_to: fn(
            &MykoClient,
            &ArticleId,
        ) -> hyphae::Cell<Option<usize>, hyphae::CellImmutable> =
            MykoClient::watch_graph_count_to::<ForwardIndexedAssignment>;
        let count_between: fn(
            &MykoClient,
            &TagId,
            &ArticleId,
        ) -> hyphae::Cell<Option<usize>, hyphae::CellImmutable> =
            MykoClient::watch_graph_count_between::<ForwardIndexedAssignment>;
        let exists_between: fn(
            &MykoClient,
            &TagId,
            &ArticleId,
        ) -> hyphae::Cell<Option<bool>, hyphae::CellImmutable> =
            MykoClient::watch_graph_exists_between::<ForwardIndexedAssignment>;
        std::hint::black_box((
            from,
            to,
            between,
            count_from,
            count_to,
            count_between,
            exists_between,
        ));
    }

    #[test]
    fn generated_graph_aggregates_track_canonical_endpoint_changes() {
        let _serial = crate::test_util::scheduler_test_serial();
        let context = context();
        let tag = Tag {
            name: "aggregate".into(),
            id: TagId::from("tag-aggregate"),
        };
        let other_tag = Tag {
            name: "aggregate-other".into(),
            id: TagId::from("tag-aggregate-other"),
        };
        let article = Article {
            title: "Aggregate report".into(),
            id: ArticleId::from("article-aggregate"),
        };
        let edge = ForwardIndexedAssignment {
            tag_id: tag.id.clone(),
            article_id: article.id.clone(),
            id: ForwardIndexedAssignmentId::from("aggregate-edge"),
        };
        assert!(context.set(&tag).is_ok());
        assert!(context.set(&other_tag).is_ok());
        assert!(context.set(&article).is_ok());
        assert!(context.set(&edge).is_ok());

        let request = || {
            Arc::new(crate::request::RequestContext::from_client(
                Uuid::new_v4().to_string().into(),
                "graph-aggregate-test".into(),
                context.host_id,
            ))
        };
        let count_from = context.report(
            ForwardIndexedAssignmentGraphCountFrom {
                endpoint: tag.id.clone(),
            },
            request(),
        );
        let count_to = context.report(
            ForwardIndexedAssignmentGraphCountTo {
                endpoint: article.id.clone(),
            },
            request(),
        );
        let count_between = context.report(
            ForwardIndexedAssignmentGraphCountBetween {
                a: tag.id.clone(),
                b: article.id.clone(),
            },
            request(),
        );
        let exists_between = context.report(
            ForwardIndexedAssignmentGraphExistsBetween {
                a: tag.id.clone(),
                b: article.id.clone(),
            },
            request(),
        );
        assert_eq!(*count_from.get(), 1);
        assert_eq!(*count_to.get(), 1);
        assert_eq!(*count_between.get(), 1);
        assert!(*exists_between.get());

        let moved = ForwardIndexedAssignment {
            tag_id: other_tag.id.clone(),
            ..edge.clone()
        };
        assert!(context.set(&moved).is_ok());
        assert_eq!(*count_from.get(), 0);
        assert_eq!(*count_to.get(), 1);
        assert_eq!(*count_between.get(), 0);
        assert!(!*exists_between.get());

        assert!(context.del(&moved).is_ok());
        assert_eq!(*count_to.get(), 0);
    }

    #[test]
    fn generated_graph_mutations_are_authoritative_and_idempotent() {
        let _serial = crate::test_util::scheduler_test_serial();
        let context = context();
        let tag = Tag {
            name: "mutations".into(),
            id: TagId::from("tag-mutations"),
        };
        let article = Article {
            title: "Mutation helpers".into(),
            id: ArticleId::from("article-mutations"),
        };
        assert!(context.set(&tag).is_ok());
        assert!(context.set(&article).is_ok());

        let command_context = |command_id: &str| {
            let request = Arc::new(crate::request::RequestContext::from_client(
                Uuid::new_v4().to_string().into(),
                "graph-mutation-test".into(),
                context.host_id,
            ));
            CommandContext::new(command_id.into(), request, Arc::new(context.clone()))
        };
        let edge = TagAssignment {
            tag_id: tag.id.clone(),
            target: EntityRef::from(&article),
            id: TagAssignmentId::from("edge-mutations"),
        };
        ConnectTagAssignment { edge: edge.clone() }
            .execute(command_context("ConnectTagAssignment"))
            .expect("connect edge");

        let competing = TagAssignment {
            id: TagAssignmentId::from("edge-competing"),
            ..edge.clone()
        };
        let ensured = EnsureTagAssignment { edge: competing }
            .execute(command_context("EnsureTagAssignment"))
            .expect("ensure existing edge");
        assert_eq!(ensured.id, edge.id);
        assert!(!ensured.created);
        assert_eq!(
            context
                .edges::<TagAssignment>()
                .count_between(&tag.id, &EntityRef::from(&article))
                .expect("count unique pair"),
            1
        );

        let concurrent_tag = Tag {
            name: "concurrent mutations".into(),
            id: TagId::from("tag-mutations-concurrent"),
        };
        let concurrent_article = Article {
            title: "Concurrent mutation helpers".into(),
            id: ArticleId::from("article-mutations-concurrent"),
        };
        assert!(context.set(&concurrent_tag).is_ok());
        assert!(context.set(&concurrent_article).is_ok());
        let context = Arc::new(context);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        // Both workers must reach the barrier before either handle is joined.
        #[allow(clippy::needless_collect)]
        let joins = (0..2)
            .map(|writer| {
                let context = context.clone();
                let barrier = barrier.clone();
                let tag_id = concurrent_tag.id.clone();
                let target = EntityRef::from(&concurrent_article);
                std::thread::spawn(move || {
                    let request = Arc::new(crate::request::RequestContext::from_client(
                        Uuid::new_v4().to_string().into(),
                        format!("graph-ensure-writer-{writer}").into(),
                        context.host_id,
                    ));
                    let command_context =
                        CommandContext::new("EnsureTagAssignment".into(), request, context.clone());
                    barrier.wait();
                    EnsureTagAssignment {
                        edge: TagAssignment {
                            tag_id,
                            target,
                            id: TagAssignmentId::from(format!("ensure-winner-{writer}")),
                        },
                    }
                    .execute(command_context)
                    .expect("concurrent ensure")
                })
            })
            .collect::<Vec<_>>();
        let results = joins
            .into_iter()
            .map(|join| join.join().expect("ensure writer"))
            .collect::<Vec<_>>();
        assert_eq!(results[0].id, results[1].id);
        assert_eq!(results.iter().filter(|result| result.created).count(), 1);
        assert_eq!(
            context
                .edges::<TagAssignment>()
                .count_between(&concurrent_tag.id, &EntityRef::from(&concurrent_article))
                .expect("count concurrently ensured pair"),
            1
        );
    }

    #[test]
    fn edge_extraction_preserves_the_ordinary_item_shape() {
        let edge_item = TagAssignment {
            tag_id: TagId::from("tag-1"),
            target: EntityRef::new("Article", "article-1"),
            id: TagAssignmentId::from("assignment-1"),
        };
        let registration = GraphSchemaCatalog::collect("myko")
            .edges
            .into_iter()
            .find(|entry| entry.edge_type == "TagAssignment")
            .expect("TagAssignment graph registration");

        let endpoints = (registration.extract)(&edge_item).expect("typed endpoint extraction");
        assert_eq!(endpoints.a.entity, EntityRef::new("Tag", "tag-1"));
        assert_eq!(endpoints.b.entity, EntityRef::new("Article", "article-1"));
        assert!(endpoints.a.qualifier.is_none());
        assert!(endpoints.b.qualifier.is_none());

        let value = serde_json::to_value(&edge_item).expect("serialize ordinary edge item");
        assert_eq!(value["id"], "assignment-1");
        assert_eq!(value["tagId"], "tag-1");
        assert_eq!(value["target"]["entityType"], "Article");
    }

    #[test]
    fn edge_ids_stay_inline_until_parallel_history_requires_a_set() {
        let first: Arc<str> = "edge-1".into();
        let second: Arc<str> = "edge-2".into();
        let mut ids = EdgeIds::One(first.clone());

        ids.insert(first.clone());
        assert_eq!(ids.ids(), vec![first.clone()]);
        ids.insert(second.clone());
        assert_eq!(ids.ids(), vec![first.clone(), second.clone()]);
        assert!(!ids.remove(&first));
        assert_eq!(ids.ids(), vec![second.clone()]);
        assert!(ids.remove(&second));
    }

    #[test]
    fn scoped_pair_projection_distinguishes_identical_endpoints() {
        let context = context();
        let tag = Tag {
            name: "scoped".into(),
            id: TagId::from("tag-scoped"),
        };
        let article = Article {
            title: "Scoped graph".into(),
            id: ArticleId::from("article-scoped"),
        };
        let scopes = [
            GraphScope {
                name: "one".to_string(),
                id: GraphScopeId::from("scope-1"),
            },
            GraphScope {
                name: "two".to_string(),
                id: GraphScopeId::from("scope-2"),
            },
        ];
        assert!(context.set(&tag).is_ok());
        assert!(context.set(&article).is_ok());
        assert!(context.batch_set(&scopes).is_ok());
        let assignments = [
            ScopedTagAssignment {
                scope_id: scopes[0].id.clone(),
                tag_id: tag.id.clone(),
                article_id: article.id.clone(),
                id: ScopedTagAssignmentId::from("scoped-edge-1"),
            },
            ScopedTagAssignment {
                scope_id: scopes[1].id.clone(),
                tag_id: tag.id.clone(),
                article_id: article.id.clone(),
                id: ScopedTagAssignmentId::from("scoped-edge-2"),
            },
        ];
        assert!(context.batch_set(&assignments).is_ok());

        let ensure_context = CommandContext::new(
            "EnsureScopedTagAssignment".into(),
            Arc::new(crate::request::RequestContext::from_client(
                Uuid::new_v4().to_string().into(),
                "scoped-graph-mutation-test".into(),
                context.host_id,
            )),
            Arc::new(context.clone()),
        );
        let ensured = EnsureScopedTagAssignment {
            edge: ScopedTagAssignment {
                id: ScopedTagAssignmentId::from("scoped-edge-competing"),
                ..assignments[0].clone()
            },
        }
        .execute(ensure_context)
        .expect("ensure within candidate scope");
        assert_eq!(ensured.id, assignments[0].id);
        assert!(!ensured.created);

        let query = context.edges::<ScopedTagAssignment>();
        assert_eq!(
            query
                .between(&tag.id, &article.id)
                .expect("all scopes")
                .len(),
            2
        );
        assert_eq!(
            query
                .between_id_in_scope(&scopes[0].id, &tag.id, &article.id)
                .expect("first scoped pair"),
            Some(assignments[0].id())
        );
        assert_eq!(
            query
                .count_between_in_scope(&scopes[1].id, &tag.id, &article.id)
                .expect("second scoped count"),
            1
        );
        assert!(
            query
                .exists_between_in_scope(&scopes[1].id, &tag.id, &article.id)
                .expect("second scoped existence")
        );
        assert!(
            !query
                .exists_between_in_scope(
                    &GraphScopeId::from("scope-missing"),
                    &tag.id,
                    &article.id,
                )
                .expect("missing scoped pair")
        );
    }

    #[test]
    fn graph_runtime_enforces_and_queries_registered_edges() {
        let _serial = crate::test_util::scheduler_test_serial();
        let context = context();
        let tag = Tag {
            name: "rust".into(),
            id: TagId::from("tag-1"),
        };
        let article = Article {
            title: "Graph design".into(),
            id: ArticleId::from("article-1"),
        };
        let moved_article = Article {
            title: "Moved edge".into(),
            id: ArticleId::from("article-2"),
        };
        assert!(context.set(&tag).is_ok());
        assert!(context.set(&article).is_ok());
        assert!(context.set(&moved_article).is_ok());

        let assignment = TagAssignment {
            tag_id: tag.id.clone(),
            target: EntityRef::from(&article),
            id: TagAssignmentId::from("assignment-1"),
        };
        assert!(context.set(&assignment).is_ok());

        let query = context.edges::<TagAssignment>();
        assert_eq!(
            query.from_ids(&tag.id).expect("from IDs"),
            vec![assignment.id()]
        );
        assert_eq!(query.count_from(&tag.id).expect("from count"), 1);
        assert!(query.exists_from(&tag.id).expect("from existence"));
        assert_eq!(
            query
                .count_to(&EntityRef::from(&article))
                .expect("to count"),
            1
        );
        assert!(
            query
                .exists_to(&EntityRef::from(&article))
                .expect("to existence")
        );
        assert_eq!(
            query
                .between_ids(&tag.id, &EntityRef::from(&article))
                .expect("between IDs"),
            vec![assignment.id()]
        );
        assert_eq!(
            query
                .between_id(&tag.id, &EntityRef::from(&article))
                .expect("unique pair ID"),
            Some(assignment.id())
        );
        assert_eq!(
            query
                .count_between(&tag.id, &EntityRef::from(&article))
                .expect("between count"),
            1
        );
        assert!(
            query
                .exists_between(&tag.id, &EntityRef::from(&article))
                .expect("between existence")
        );
        assert_eq!(
            query
                .one_between(&tag.id, &EntityRef::from(&article))
                .expect("unique pair item"),
            Some(Arc::new(assignment.clone()))
        );

        let from = context
            .edges::<TagAssignment>()
            .from(&tag.id)
            .expect("from lookup");
        assert_eq!(from.as_slice(), &[Arc::new(assignment.clone())]);
        assert_eq!(
            context
                .edges::<TagAssignment>()
                .between(&tag.id, &EntityRef::from(&article))
                .expect("between lookup")
                .len(),
            1
        );

        let watched = context
            .edges::<TagAssignment>()
            .watch_to(&EntityRef::from(&article))
            .expect("watch target");
        assert_eq!(watched.snapshot().len(), 1);
        let traversal = context
            .traverse::<TagAssignment>()
            .start(tag.id.clone())
            .max_depth(2)
            .max_nodes(10)
            .execute()
            .expect("bounded traversal");
        assert_eq!(traversal.nodes, vec![EntityRef::from(&article)]);
        assert_eq!(traversal.edge_ids, vec![assignment.id()]);
        assert_eq!(
            context
                .graph_index()
                .expect("graph index")
                .diagnostics()
                .adjacency_entries,
            0,
            "demand-driven edges do not allocate adjacency buckets"
        );
        assert_eq!(
            context
                .graph_index()
                .expect("graph index")
                .diagnostics()
                .pair_entries,
            1
        );

        let moved = TagAssignment {
            target: EntityRef::from(&moved_article),
            ..assignment.clone()
        };
        assert!(context.set(&moved).is_ok());
        assert!(watched.snapshot().is_empty());
        assert!(
            context
                .edges::<TagAssignment>()
                .between(&tag.id, &EntityRef::from(&article))
                .expect("old pair removed")
                .is_empty()
        );
        assert!(
            !context
                .edges::<TagAssignment>()
                .exists_between(&tag.id, &EntityRef::from(&article))
                .expect("old pair absence")
        );
        assert_eq!(
            context
                .edges::<TagAssignment>()
                .between(&tag.id, &EntityRef::from(&moved_article))
                .expect("new pair installed"),
            vec![Arc::new(moved.clone())]
        );

        let duplicate = TagAssignment {
            id: TagAssignmentId::from("assignment-2"),
            ..moved.clone()
        };
        let error = context.set(&duplicate).expect_err("unique pair rejected");
        assert!(error.message.contains("already occupied"));
        assert_eq!(
            context
                .graph_index()
                .expect("graph index")
                .diagnostics()
                .uniqueness_rejections,
            1
        );

        assert!(context.del(&tag).is_ok());
        assert!(
            context
                .edges::<TagAssignment>()
                .from(&tag.id)
                .expect("empty after delete")
                .is_empty()
        );
        assert!(watched.snapshot().is_empty());
        assert!(
            context
                .registry
                .get("TagAssignment")
                .is_some_and(|store| store.snapshot().is_empty()),
            "endpoint deletion cascades the canonical edge item"
        );
        assert!(
            context
                .causal_diagnostics()
                .duplicate_transitions_suppressed
                >= 1,
            "belongs_to and graph cascades converge through exact-transition suppression"
        );
    }

    #[test]
    fn authoritative_edges_reject_missing_or_wrong_category_endpoints() {
        let context = context();
        let tag = Tag {
            name: "rust".into(),
            id: TagId::from("tag-1"),
        };
        assert!(context.set(&tag).is_ok());
        let missing = TagAssignment {
            tag_id: tag.id,
            target: EntityRef::new("Tag", "tag-1"),
            id: TagAssignmentId::from("assignment-invalid"),
        };
        let error = context
            .set(&missing)
            .expect_err("category mismatch rejected");
        assert!(error.message.contains("rejects entity type Tag"));
        assert!(
            context
                .registry
                .get("TagAssignment")
                .is_none_or(|store| store.snapshot().is_empty())
        );
    }

    #[test]
    fn unique_pairs_are_reserved_across_batches_and_concurrent_writers() {
        let context = Arc::new(context());
        let tag = Tag {
            name: "rust".into(),
            id: TagId::from("tag-concurrent"),
        };
        let article = Article {
            title: "Concurrency".into(),
            id: ArticleId::from("article-concurrent"),
        };
        assert!(context.set(&tag).is_ok());
        assert!(context.set(&article).is_ok());
        let first = TagAssignment {
            tag_id: tag.id.clone(),
            target: EntityRef::from(&article),
            id: TagAssignmentId::from("assignment-batch-a"),
        };
        let second = TagAssignment {
            id: TagAssignmentId::from("assignment-batch-b"),
            ..first.clone()
        };
        assert!(context.batch_set(&[first.clone(), second.clone()]).is_err());
        assert!(
            context
                .registry
                .get("TagAssignment")
                .is_none_or(|store| store.snapshot().is_empty())
        );

        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut joins = Vec::new();
        for edge in [first, second] {
            let context = context.clone();
            let barrier = barrier.clone();
            joins.push(std::thread::spawn(move || {
                barrier.wait();
                context.set(&edge).is_ok()
            }));
        }
        barrier.wait();
        let accepted = joins
            .into_iter()
            .map(|join| join.join().expect("writer thread"))
            .filter(|accepted| *accepted)
            .count();
        assert_eq!(accepted, 1);
        assert_eq!(
            context
                .registry
                .get("TagAssignment")
                .map_or(0, |store| store.snapshot().len()),
            1
        );
    }

    #[test]
    fn import_mode_retains_dangling_canonical_history_with_diagnostics() {
        let context = context();
        let applied = context
            .apply_events_immediate(vec![crate::wire::MEvent {
                item: serde_json::json!({
                    "id": "imported-dangling",
                    "tagId": "missing-tag",
                    "target": { "entityType": "Article", "id": "missing-article" }
                }),
                change_type: crate::wire::MEventType::SET,
                item_type: "TagAssignment".into(),
                created_at: "2026-08-14T00:00:00Z".into(),
                tx: "import-tx".into(),
                source_id: Some("history".into()),
            }])
            .expect("import retains canonical edge");
        assert_eq!(applied, 1);
        assert_eq!(
            context
                .registry
                .get("TagAssignment")
                .map_or(0, |store| store.snapshot().len()),
            1
        );
        assert_eq!(
            context
                .graph_index()
                .expect("graph index")
                .diagnostics()
                .invalid_mutations,
            1
        );
    }

    #[test]
    fn endpoint_delete_policies_restrict_or_retain_canonical_edges() {
        let restricted_context = context();
        let tag = Tag {
            name: "restricted".into(),
            id: TagId::from("tag-restricted"),
        };
        let article = Article {
            title: "Restricted".into(),
            id: ArticleId::from("article-restricted"),
        };
        assert!(restricted_context.set(&tag).is_ok());
        assert!(restricted_context.set(&article).is_ok());
        assert!(
            restricted_context
                .set(&RestrictedTagAssignment {
                    tag_id: tag.id.clone(),
                    target: EntityRef::from(&article),
                    id: RestrictedTagAssignmentId::from("restricted-edge"),
                })
                .is_ok()
        );
        let error = restricted_context
            .del(&tag)
            .expect_err("incident restrict edge blocks endpoint deletion");
        assert!(error.message.contains("cannot delete"));
        assert!(
            restricted_context
                .registry
                .get("Tag")
                .is_some_and(|store| store.get_value(&tag.id()).is_some())
        );
        assert!(restricted_context.del(&article).is_ok());
        assert!(
            restricted_context
                .registry
                .get("RestrictedTagAssignment")
                .is_some_and(|store| store.snapshot().is_empty()),
            "the eager B-role incidence cascades without scanning"
        );

        let retained_context = context();
        let retained_tag = Tag {
            name: "retained".into(),
            id: TagId::from("tag-retained"),
        };
        let retained_article = Article {
            title: "Retained".into(),
            id: ArticleId::from("article-retained"),
        };
        assert!(retained_context.set(&retained_tag).is_ok());
        assert!(retained_context.set(&retained_article).is_ok());
        let retained = RetainedTagAssignment {
            tag_id: retained_tag.id.clone(),
            target: EntityRef::from(&retained_article),
            id: RetainedTagAssignmentId::from("retained-edge"),
        };
        assert!(retained_context.set(&retained).is_ok());
        assert!(retained_context.del(&retained_tag).is_ok());
        assert_eq!(
            retained_context
                .edges::<RetainedTagAssignment>()
                .from(&retained_tag.id)
                .expect("dangling edge remains queryable"),
            vec![Arc::new(retained)]
        );
        assert!(retained_context.del(&retained_article).is_ok());
        assert!(
            retained_context
                .registry
                .get("RetainedTagAssignment")
                .is_some_and(|store| store.snapshot().is_empty()),
            "the demand-driven B-role policy retains its canonical scan fallback"
        );
    }
}
