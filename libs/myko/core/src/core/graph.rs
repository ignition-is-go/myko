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
    type Value;

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
    T::Id: Clone + Into<Arc<str>>,
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
    T::Id: Clone + Into<Arc<str>>,
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
    const ADJACENCY: AdjacencyPolicy = AdjacencyPolicy::DemandDriven;
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

#[derive(Default)]
struct EdgeTypeState {
    generation: u64,
    edges: HashMap<Arc<str>, IndexedEdge>,
    a: HashMap<EndpointValue, BTreeSet<Arc<str>>>,
    b: HashMap<EndpointValue, BTreeSet<Arc<str>>>,
    a_entities: HashMap<EntityRef, BTreeSet<Arc<str>>>,
    b_entities: HashMap<EntityRef, BTreeSet<Arc<str>>>,
    pairs: HashMap<EdgePairKey, BTreeSet<Arc<str>>>,
}

#[derive(Default)]
struct GraphState {
    generation: u64,
    edge_types: HashMap<&'static str, EdgeTypeState>,
    invalid_mutations: u64,
    uniqueness_rejections: u64,
    failed: bool,
}

/// Coherent in-memory projection over all registered edge item stores.
///
/// The runtime is only constructed when graph registrations exist, preserving
/// the pre-graph mutation fast path for applications that do not opt in.
pub struct GraphIndex {
    registrations: HashMap<&'static str, &'static EdgeRegistration>,
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
        let (a, b) = if registration.shape == EdgeShapeKind::Undirected && endpoints.b < endpoints.a
        {
            (endpoints.b.clone(), endpoints.a.clone())
        } else {
            (endpoints.a.clone(), endpoints.b.clone())
        };
        EdgePairKey { scope, a, b }
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
                    && let Some(existing_id) = existing_ids
                        .iter()
                        .find(|id| id.as_ref() != candidate.id().as_ref())
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
        if registration.pair_policy == PairPolicy::Unique
            || registration.pair_projection == PairProjectionPolicy::Eager
        {
            let key = Self::pair_key(registration, &edge.endpoints, edge.scope.clone());
            if let Some(ids) = state.pairs.get_mut(&key) {
                ids.remove(&edge.id);
                if ids.is_empty() {
                    state.pairs.remove(&key);
                }
            }
        }
    }

    fn insert_indexed(
        registration: &EdgeRegistration,
        state: &mut EdgeTypeState,
        edge: IndexedEdge,
    ) {
        if registration.adjacency == AdjacencyPolicy::Eager {
            state
                .a
                .entry(edge.endpoints.a.clone())
                .or_default()
                .insert(edge.id.clone());
            state
                .b
                .entry(edge.endpoints.b.clone())
                .or_default()
                .insert(edge.id.clone());
            state
                .a_entities
                .entry(edge.endpoints.a.entity.clone())
                .or_default()
                .insert(edge.id.clone());
            state
                .b_entities
                .entry(edge.endpoints.b.entity.clone())
                .or_default()
                .insert(edge.id.clone());
        }
        if registration.pair_policy == PairPolicy::Unique
            || registration.pair_projection == PairProjectionPolicy::Eager
        {
            state
                .pairs
                .entry(Self::pair_key(
                    registration,
                    &edge.endpoints,
                    edge.scope.clone(),
                ))
                .or_default()
                .insert(edge.id.clone());
        }
        if registration.adjacency == AdjacencyPolicy::Eager {
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
            Self::insert_indexed(registration, edge_type, indexed);
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
    pub fn endpoint_delete_plan(&self, endpoint: &EntityRef) -> Result<EndpointDeletePlan> {
        let mut cascade = HashSet::new();
        for (edge_type, registration) in &self.registrations {
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
                    if &value.entity != endpoint {
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
        if registration.adjacency == AdjacencyPolicy::DemandDriven {
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
        map.get(endpoint)
            .map_or_else(Vec::new, |ids| ids.iter().cloned().collect())
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
        if registration.adjacency == AdjacencyPolicy::DemandDriven {
            return self.registry.get(edge_type).map_or_else(Vec::new, |store| {
                store
                    .snapshot()
                    .into_iter()
                    .filter_map(|(_, item)| {
                        let endpoints = (registration.extract)(item.as_ref()).ok()?;
                        (endpoints.a == *a && endpoints.b == *b).then(|| item.id())
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
        let (small, other) = match (edges.a.get(a), edges.b.get(b)) {
            (Some(a_ids), Some(b_ids)) if a_ids.len() <= b_ids.len() => (a_ids, b_ids),
            (Some(a_ids), Some(b_ids)) => (b_ids, a_ids),
            _ => return Vec::new(),
        };
        small
            .iter()
            .filter(|id| other.contains(*id))
            .cloned()
            .collect()
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
        if registration.adjacency == AdjacencyPolicy::DemandDriven {
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
        let shape = registration.shape;
        let mut candidates = BTreeSet::new();
        if (direction != Direction::Reverse || shape == EdgeShapeKind::Undirected)
            && let Some(ids) = edges.a_entities.get(node)
        {
            candidates.extend(ids.iter().cloned());
        }
        if (direction != Direction::Forward || shape == EdgeShapeKind::Undirected)
            && let Some(ids) = edges.b_entities.get(node)
        {
            candidates.extend(ids.iter().cloned());
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

    /// Directed-style lookup at endpoint A (`from`).
    pub fn from(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?;
        Ok(self.materialize(self.graph()?.edge_ids_at(
            E::ENTITY_NAME_STATIC,
            EndPosition::A,
            &endpoint,
        )))
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
        let endpoint = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(value)?;
        Ok(self.materialize(self.graph()?.edge_ids_at(
            E::ENTITY_NAME_STATIC,
            EndPosition::B,
            &endpoint,
        )))
    }

    /// Qualified-address spelling for [`Self::to`].
    pub fn to_at(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        self.to(value)
    }

    /// Exact A/B lookup, implemented by intersecting the narrower incidence set.
    pub fn between(
        &self,
        a: &<<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::Value,
        b: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<Vec<Arc<E>>> {
        let a = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(a)?;
        let b = <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(b)?;
        Ok(self.materialize(
            self.graph()?
                .edge_ids_between(E::ENTITY_NAME_STATIC, &a, &b),
        ))
    }

    fn watch_at(
        &self,
        position: EndPosition,
        endpoint: EndpointValue,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<E>, hyphae::CellImmutable>> {
        use hyphae::{MapQuery, SelectExt};

        let registration = self
            .graph()?
            .registration(E::ENTITY_NAME_STATIC)
            .context("edge type is not registered")?;
        let store = self.context.registry.get_or_create(E::ENTITY_NAME_STATIC);
        let selected = MapQuery::materialize((*store).clone().select(move |item| {
            (registration.extract)(item.as_ref()).is_ok_and(|ends| match position {
                EndPosition::A => ends.a == endpoint,
                EndPosition::B => ends.b == endpoint,
            })
        }));
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
        self.watch_at(
            EndPosition::A,
            <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?,
        )
    }

    /// Reactive counterpart of [`Self::to`].
    pub fn watch_to(
        &self,
        value: &<<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::Value,
    ) -> Result<hyphae::CellMap<Arc<str>, Arc<E>, hyphae::CellImmutable>> {
        self.watch_at(
            EndPosition::B,
            <<E::Ends as TypedEdgeEnds>::B as EndpointSpec>::erase(value)?,
        )
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
        use hyphae::{MapQuery, SelectExt};

        let endpoint = <<E::Ends as TypedEdgeEnds>::A as EndpointSpec>::erase(value)?;
        let registration = self
            .graph()?
            .registration(E::ENTITY_NAME_STATIC)
            .context("edge type is not registered")?;
        let store = self.context.registry.get_or_create(E::ENTITY_NAME_STATIC);
        let selected = MapQuery::materialize((*store).clone().select(move |item| {
            (registration.extract)(item.as_ref())
                .is_ok_and(|ends| ends.a == endpoint || ends.b == endpoint)
        }));
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
    use assignment::{TagAssignment, TagAssignmentId};

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
        assert!(edge.validate.is_none());
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
    fn graph_runtime_enforces_and_queries_registered_edges() {
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
    }
}
