//! Transport-neutral typed item contracts for Myko 7.
//!
//! This crate deliberately has no networking or runtime dependency. Domain
//! crates define entities with [`myko_item`], federation logs carry
//! [`ItemMutation`] values, and projections recover typed current state.

#![forbid(unsafe_code)]

extern crate self as myko_items;

use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
    marker::PhantomData,
    sync::Arc,
};

pub use myko_items_macros::{myko_command, myko_item, myko_service, myko_subtype};
pub use serde;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

#[cfg(test)]
mod subtype_tests {
    use super::myko_subtype;

    #[myko_subtype(derive(Eq))]
    struct ExampleSubtype {
        field_name: String,
    }

    #[myko_subtype(derive(Eq))]
    enum ExampleVariant {
        NamedValue,
    }

    #[test]
    fn subtype_owns_value_derives_and_wire_casing() {
        let value = ExampleSubtype {
            field_name: "value".to_owned(),
        };
        let encoded_struct = serde_json::to_value(&value);
        assert!(encoded_struct.is_ok());
        if let Ok(encoded_struct) = encoded_struct {
            assert_eq!(encoded_struct, serde_json::json!({"fieldName": "value"}));
        }
        let encoded_variant = serde_json::to_value(ExampleVariant::NamedValue);
        assert!(encoded_variant.is_ok());
        if let Ok(encoded_variant) = encoded_variant {
            assert_eq!(encoded_variant, serde_json::json!("NamedValue"));
        }
    }
}

/// Static placement metadata declared by an item schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemScope {
    /// The item does not itself declare a federation scope boundary.
    Unscoped,
    /// Each item is the root of a federation scope.
    Root,
    /// The item is scoped by the named parent item type.
    ScopedBy(&'static str),
}

/// Stable textual identifier generated for a [`MykoItem`].
pub trait ItemId:
    Clone + Debug + Eq + Ord + AsRef<str> + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

/// Generated static identity of a typed service.
///
/// Application code carries the service type. This value is exposed only for
/// persistence and transport adapters that must serialize that type identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceTypeId(&'static str);

impl ServiceTypeId {
    #[doc(hidden)]
    #[must_use]
    pub const fn new(value: &'static str) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<str> for ServiceTypeId {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl fmt::Display for ServiceTypeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0, formatter)
    }
}

impl From<ServiceTypeId> for String {
    fn from(value: ServiceTypeId) -> Self {
        value.0.to_owned()
    }
}

impl From<ServiceTypeId> for Arc<str> {
    fn from(value: ServiceTypeId) -> Self {
        Self::from(value.0)
    }
}

/// Typed atomicity and replication boundary selected by an application.
pub trait MykoService: Send + Sync + 'static {
    /// Item modules grouped into this service.
    type Items;

    /// Generated stable identity used only by persistence and wire envelopes.
    const SERVICE_ID: ServiceTypeId;
}

/// A typed record in the Myko property graph.
pub trait MykoItem:
    Clone + Debug + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Id: ItemId;
    /// Service that owns this item module's authoritative mutations.
    type Service: MykoService;
    /// Entity type defining this item's immediate application-scope family.
    ///
    /// Root and unscoped items use themselves. Items declared with
    /// `scoped_by` use the named parent entity, which may belong to another
    /// service.
    type Scope: MykoItem;
    /// Generated query returning every current item in one scope.
    type GetAllQuery: ItemQuery<Item = Self, Output = Vec<Self>>;
    /// Generated query returning one item by its typed ID.
    type GetByIdQuery: ItemQuery<Item = Self, Output = Option<Self>>;
    /// Generated query returning selected items by typed ID.
    type GetByIdsQuery: ItemQuery<Item = Self, Output = Vec<Self>>;

    /// Generated wire identity of the typed owning service.
    const SERVICE_ID: ServiceTypeId = <Self::Service as MykoService>::SERVICE_ID;
    /// Stable wire name for this item schema.
    const ITEM_TYPE: &'static str;
    /// Schema version encoded into every mutation.
    const SCHEMA_VERSION: u32 = 1;
    /// Static federation placement declared by the entity.
    const SCOPE: ItemScope = ItemScope::Unscoped;

    /// Returns this item's stable typed identifier.
    fn id(&self) -> &Self::Id;
}

/// A stable reference to any Myko item in the application property graph.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EntityRef {
    pub service_id: Arc<str>,
    pub item_type: Arc<str>,
    pub id: Arc<str>,
}

impl EntityRef {
    #[must_use]
    pub fn new(
        service_id: impl Into<Arc<str>>,
        item_type: impl Into<Arc<str>>,
        id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            service_id: service_id.into(),
            item_type: item_type.into(),
            id: id.into(),
        }
    }
}

impl<T: MykoItem> From<&T> for EntityRef {
    fn from(item: &T) -> Self {
        Self::new(T::SERVICE_ID, T::ITEM_TYPE, item.id().as_ref())
    }
}

/// One typed endpoint declaration for a graph edge.
pub trait EndpointSpec: Send + Sync + 'static {
    type Value: Clone + Debug + Send + Sync + 'static;

    fn erase(value: &Self::Value) -> EntityRef;
}

/// An endpoint whose value is the generated ID of one concrete item type.
pub struct ConcreteEndpoint<T>(PhantomData<T>);

impl<T: MykoItem> EndpointSpec for ConcreteEndpoint<T> {
    type Value = T::Id;

    fn erase(value: &Self::Value) -> EntityRef {
        EntityRef::new(T::SERVICE_ID, T::ITEM_TYPE, value.as_ref())
    }
}

/// Static endpoint metadata for a graph edge.
pub trait EdgeEnds: Send + Sync + 'static {
    type Values;

    fn erase(values: &Self::Values) -> (EntityRef, EntityRef);
}

/// The endpoint specifications exposed by typed graph queries.
pub trait TypedEdgeEnds: EdgeEnds {
    type A: EndpointSpec;
    type B: EndpointSpec;
}

/// A directed edge from endpoint A to endpoint B.
pub struct Directed<A, B>(PhantomData<(A, B)>);

impl<A: EndpointSpec, B: EndpointSpec> EdgeEnds for Directed<A, B> {
    type Values = (A::Value, B::Value);

    fn erase(values: &Self::Values) -> (EntityRef, EntityRef) {
        (A::erase(&values.0), B::erase(&values.1))
    }
}

impl<A: EndpointSpec, B: EndpointSpec> TypedEdgeEnds for Directed<A, B> {
    type A = A;
    type B = B;
}

/// An undirected edge whose canonical storage still retains A/B positions.
pub struct Undirected<A, B>(PhantomData<(A, B)>);

impl<A: EndpointSpec, B: EndpointSpec> EdgeEnds for Undirected<A, B> {
    type Values = (A::Value, B::Value);

    fn erase(values: &Self::Values) -> (EntityRef, EntityRef) {
        (A::erase(&values.0), B::erase(&values.1))
    }
}

impl<A: EndpointSpec, B: EndpointSpec> TypedEdgeEnds for Undirected<A, B> {
    type A = A;
    type B = B;
}

/// An ordinary Myko item carrying typed relationship endpoints.
///
/// Edge values use the same command, persistence, federation, and reactive
/// projection path as every other item; this trait adds only graph metadata.
pub trait GraphEdge: MykoItem {
    type Ends: EdgeEnds;

    fn ends(&self) -> <Self::Ends as EdgeEnds>::Values;
}

/// Generated stable identity and item ownership for one application operation.
pub trait MykoOperation: Send + Sync + 'static {
    /// Stable wire identity generated from the Rust operation type.
    const OPERATION_ID: &'static str;
}

/// Typed wire contract for an application command.
pub trait MykoCommandContract:
    MykoOperation + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Output: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Service: MykoService;
    /// Static application-scope family used to type-check nested commands.
    ///
    /// The concrete scope value remains runtime data because commands commonly
    /// derive its typed entity ID from the command body or serving node.
    type Scope: MykoItem;

    /// Stable service identity generated by `#[myko_command]`.
    const SERVICE_ID: ServiceTypeId = <Self::Service as MykoService>::SERVICE_ID;
    /// Item module directly owned by this command, when mutation is item-limited.
    const ITEM_TYPE: Option<&'static str> = None;
    const COMMAND_TYPE: &'static str = Self::OPERATION_ID;
}

/// A transport-level typed command declaration.
///
/// Execution is deliberately not defined in this transport-neutral crate.
/// `myko_app::CommandHandler` adds the application handler contract and its
/// sealed, framework-owned capability context.
pub trait MykoCommand: MykoCommandContract {}

/// How an item changes in an immutable command batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOperation {
    Set,
    Delete,
}

/// A schema-identified item mutation suitable for durable logs and transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemMutation {
    pub service_id: String,
    pub item_type: String,
    pub item_id: String,
    pub schema_version: u32,
    pub operation: MutationOperation,
    pub payload: Option<Vec<u8>>,
}

impl ItemMutation {
    /// Encodes a typed item replacement.
    ///
    /// # Errors
    ///
    /// Returns an error if the item cannot be serialized.
    pub fn set<T: MykoItem>(item: &T) -> Result<Self, ItemError> {
        Ok(Self {
            service_id: T::SERVICE_ID.as_str().to_owned(),
            item_type: T::ITEM_TYPE.to_owned(),
            item_id: item.id().as_ref().to_owned(),
            schema_version: T::SCHEMA_VERSION,
            operation: MutationOperation::Set,
            payload: Some(serde_json::to_vec(item)?),
        })
    }

    /// Encodes deletion of one typed item.
    #[must_use]
    pub fn delete<T: MykoItem>(id: &T::Id) -> Self {
        Self {
            service_id: T::SERVICE_ID.as_str().to_owned(),
            item_type: T::ITEM_TYPE.to_owned(),
            item_id: id.as_ref().to_owned(),
            schema_version: T::SCHEMA_VERSION,
            operation: MutationOperation::Delete,
            payload: None,
        }
    }

    /// Returns whether this mutation belongs to `T`'s exact schema version.
    #[must_use]
    pub fn is<T: MykoItem>(&self) -> bool {
        self.service_id == T::SERVICE_ID.as_str()
            && self.item_type == T::ITEM_TYPE
            && self.schema_version == T::SCHEMA_VERSION
    }

    /// Validates transport-level invariants without needing the concrete item
    /// schema to be linked into the current process.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty identity, version zero, or a payload whose
    /// presence does not match the mutation operation.
    pub const fn validate_envelope(&self) -> Result<(), ItemError> {
        if self.service_id.is_empty() {
            return Err(ItemError::EmptyServiceId);
        }
        if self.item_type.is_empty() {
            return Err(ItemError::EmptyItemType);
        }
        if self.item_id.is_empty() {
            return Err(ItemError::EmptyItemId);
        }
        if self.schema_version == 0 {
            return Err(ItemError::InvalidSchemaVersion);
        }
        match (self.operation, self.payload.is_some()) {
            (MutationOperation::Set, false) => Err(ItemError::MissingPayload),
            (MutationOperation::Delete, true) => Err(ItemError::UnexpectedPayload),
            (MutationOperation::Set | MutationOperation::Delete, _) => Ok(()),
        }
    }

    /// Decodes and validates a typed set mutation.
    ///
    /// # Errors
    ///
    /// Returns an error for a schema mismatch, non-set operation, absent or
    /// malformed payload, or an ID mismatch between envelope and item.
    pub fn decode_set<T: MykoItem>(&self) -> Result<T, ItemError> {
        self.require_schema::<T>()?;
        if self.operation != MutationOperation::Set {
            return Err(ItemError::UnexpectedOperation(self.operation));
        }
        let payload = self.payload.as_deref().ok_or(ItemError::MissingPayload)?;
        let item: T = serde_json::from_slice(payload)?;
        if item.id().as_ref() != self.item_id {
            return Err(ItemError::IdentifierMismatch {
                envelope: self.item_id.clone(),
                payload: item.id().as_ref().to_owned(),
            });
        }
        Ok(item)
    }

    fn require_schema<T: MykoItem>(&self) -> Result<(), ItemError> {
        if self.service_id != T::SERVICE_ID.as_str()
            || self.item_type != T::ITEM_TYPE
            || self.schema_version != T::SCHEMA_VERSION
        {
            return Err(ItemError::SchemaMismatch {
                expected_service: T::SERVICE_ID,
                expected_type: T::ITEM_TYPE,
                expected_version: T::SCHEMA_VERSION,
                actual_service: self.service_id.clone(),
                actual_type: self.item_type.clone(),
                actual_version: self.schema_version,
            });
        }
        Ok(())
    }
}

/// Failure to encode, validate, or materialize an item mutation.
#[derive(Debug, Error)]
pub enum ItemError {
    #[error("item payload serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error(
        "item schema mismatch: expected {expected_service}/{expected_type}@{expected_version}, got {actual_service}/{actual_type}@{actual_version}"
    )]
    SchemaMismatch {
        expected_service: ServiceTypeId,
        expected_type: &'static str,
        expected_version: u32,
        actual_service: String,
        actual_type: String,
        actual_version: u32,
    },
    #[error("expected an item set mutation, got {0:?}")]
    UnexpectedOperation(MutationOperation),
    #[error("item set mutation has no payload")]
    MissingPayload,
    #[error("item delete mutation unexpectedly has a payload")]
    UnexpectedPayload,
    #[error("item mutation has an empty service ID")]
    EmptyServiceId,
    #[error("item mutation has an empty item type")]
    EmptyItemType,
    #[error("item mutation has an empty item ID")]
    EmptyItemId,
    #[error("item mutation schema version must be greater than zero")]
    InvalidSchemaVersion,
    #[error("item ID mismatch: envelope has {envelope}, payload has {payload}")]
    IdentifierMismatch { envelope: String, payload: String },
}

/// Current typed state reconstructed from immutable item mutations.
#[derive(Debug, Clone)]
pub struct ItemProjection<T: MykoItem> {
    items: BTreeMap<String, ProjectedItem<T>>,
    next_revision: u64,
}

#[derive(Debug, Clone)]
struct ProjectedItem<T> {
    value: T,
    first_changed_at: u64,
    last_changed_at: u64,
    change_index: u32,
}

impl<T: MykoItem> Default for ItemProjection<T> {
    fn default() -> Self {
        Self {
            items: BTreeMap::new(),
            next_revision: 1,
        }
    }
}

impl<T: MykoItem> ItemProjection<T> {
    /// Applies a mutation if it targets `T`, returning whether it was applied.
    ///
    /// # Errors
    ///
    /// Returns an error when a matching mutation is malformed.
    pub fn apply(&mut self, mutation: &ItemMutation) -> Result<bool, ItemError> {
        let revision = self.next_revision;
        self.apply_at(mutation, revision)
    }

    /// Applies a mutation at an authoritative monotonic revision.
    ///
    /// Revisions are projection metadata rather than part of the application
    /// item schema. A durable runtime normally supplies its log position here,
    /// allowing typed queries to order current items without decoding event
    /// envelopes or embedding transport cursors in domain values.
    ///
    /// # Errors
    ///
    /// Returns an error when a matching mutation is malformed.
    pub fn apply_at(&mut self, mutation: &ItemMutation, revision: u64) -> Result<bool, ItemError> {
        self.apply_at_order(mutation, revision, 0)
    }

    /// Applies a mutation at a revision and its index within that atomic batch.
    ///
    /// # Errors
    ///
    /// Returns an error when a matching mutation is malformed.
    pub fn apply_at_order(
        &mut self,
        mutation: &ItemMutation,
        revision: u64,
        change_index: u32,
    ) -> Result<bool, ItemError> {
        if mutation.service_id != T::SERVICE_ID.as_str() || mutation.item_type != T::ITEM_TYPE {
            return Ok(false);
        }
        mutation.require_schema::<T>()?;
        match mutation.operation {
            MutationOperation::Set => {
                let item = mutation.decode_set::<T>()?;
                let first_changed_at = self
                    .items
                    .get(&mutation.item_id)
                    .map_or(revision, |existing| existing.first_changed_at);
                self.items.insert(
                    mutation.item_id.clone(),
                    ProjectedItem {
                        value: item,
                        first_changed_at,
                        last_changed_at: revision,
                        change_index,
                    },
                );
            }
            MutationOperation::Delete => {
                if mutation.payload.is_some() {
                    return Err(ItemError::UnexpectedPayload);
                }
                self.items.remove(&mutation.item_id);
            }
        }
        self.next_revision = self.next_revision.max(revision.saturating_add(1));
        Ok(true)
    }

    /// Gets one current item by typed ID.
    #[must_use]
    pub fn get(&self, id: &T::Id) -> Option<&T> {
        self.items.get(id.as_ref()).map(|item| &item.value)
    }

    /// Iterates current items in stable ID order.
    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.items.values().map(|item| &item.value)
    }

    /// Returns current items ordered by their last authoritative change.
    ///
    /// Items changed by the same atomic revision are ordered by stable item ID.
    #[must_use]
    pub fn values_by_last_change(&self) -> Vec<&T> {
        let mut items = self.items.iter().collect::<Vec<_>>();
        items.sort_unstable_by(|(left_id, left), (right_id, right)| {
            left.last_changed_at
                .cmp(&right.last_changed_at)
                .then_with(|| left.change_index.cmp(&right.change_index))
                .then_with(|| left_id.cmp(right_id))
        });
        items.into_iter().map(|(_, item)| &item.value).collect()
    }

    /// Iterates current values with their authoritative ordering metadata.
    ///
    /// The iterator itself is in stable item-ID order. Consumers may sort by
    /// the returned revision and change index when reconstructing a composite
    /// view from more than one typed projection.
    pub fn values_with_change_metadata(&self) -> impl Iterator<Item = (&T, u64, u32)> {
        self.items
            .values()
            .map(|item| (&item.value, item.last_changed_at, item.change_index))
    }

    /// Iterates current values with their first and latest authoritative revisions.
    ///
    /// The first revision remains stable across replacements, so application
    /// queues and timelines can retain creation order without copying journal
    /// positions into domain entities.
    pub fn values_with_lifecycle_metadata(&self) -> impl Iterator<Item = (&T, u64, u64, u32)> {
        self.items.values().map(|item| {
            (
                &item.value,
                item.first_changed_at,
                item.last_changed_at,
                item.change_index,
            )
        })
    }

    /// Returns the authoritative revision that first created one current item.
    #[must_use]
    pub fn first_changed_at(&self, id: &T::Id) -> Option<u64> {
        self.items
            .get(id.as_ref())
            .map(|item| item.first_changed_at)
    }

    /// Returns the authoritative revision that last changed one current item.
    #[must_use]
    pub fn last_changed_at(&self, id: &T::Id) -> Option<u64> {
        self.items.get(id.as_ref()).map(|item| item.last_changed_at)
    }

    /// Executes a generated typed item query.
    pub fn query<Q>(&self, query: Q) -> Q::Output
    where
        Q: ItemQuery<Item = T>,
    {
        query.execute(self)
    }
}

/// Application-defined query handler over one typed item projection.
///
/// The stable ID and serializable parameters let a Myko node register this
/// handler once and expose it through any compatible peer transport. The
/// output bounds also make the result directly usable as a Hyphae cell value
/// without making this schema crate depend on Hyphae itself.
pub trait ItemQuery:
    MykoOperation + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type Item: MykoItem;
    type Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Stable application wire identity for this query handler.
    const QUERY_ID: &'static str = Self::OPERATION_ID;

    fn execute(self, projection: &ItemProjection<Self::Item>) -> Self::Output;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[myko_service(Project)]
    pub struct ProjectService;

    #[myko_item(service = ProjectService, scope_root)]
    pub struct Project {
        pub title: String,
    }

    #[myko_service(Task)]
    pub struct TaskService;

    #[myko_item(service = TaskService, scoped_by = Project)]
    pub struct Task {
        pub title: String,
    }

    #[test]
    fn macro_mutation_and_generated_queries_are_typed_end_to_end() {
        fn require_project_scope<I: MykoItem<Scope = Project>>() {}
        require_project_scope::<Task>();
        assert_eq!(Task::SERVICE_ID, TaskService::SERVICE_ID);
        assert_eq!(Task::SCOPE, ItemScope::ScopedBy(Project::ITEM_TYPE));

        let project = Project {
            id: ProjectId::from("project-1"),
            title: "Forrest".to_owned(),
        };
        let mutation = ItemMutation::set(&project);
        assert!(mutation.is_ok());
        assert_eq!(Project::SERVICE_ID, ProjectService::SERVICE_ID);
        assert!(matches!(
            mutation.as_ref(),
            Ok(mutation) if mutation.service_id == ProjectService::SERVICE_ID.as_str()
        ));
        let mut projection = ItemProjection::<Project>::default();
        assert!(matches!(
            mutation.and_then(|mutation| projection.apply(&mutation)),
            Ok(true)
        ));
        assert_eq!(projection.query(GetAllProjects), vec![project.clone()]);
        assert_eq!(
            projection.query(GetProjectById {
                id: ProjectId::from("project-1"),
            }),
            Some(project)
        );
    }

    #[test]
    fn known_item_type_with_unknown_version_is_not_silently_ignored() {
        let project = Project {
            id: ProjectId::from("project-1"),
            title: "Forrest".to_owned(),
        };
        let mutation = ItemMutation::set(&project).map(|mut mutation| {
            mutation.schema_version = 2;
            mutation
        });
        let mut projection = ItemProjection::<Project>::default();
        assert!(matches!(
            mutation.and_then(|mutation| projection.apply(&mutation)),
            Err(ItemError::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn item_service_is_part_of_the_exact_schema_contract() {
        let project = Project {
            id: ProjectId::from("project-1"),
            title: "Forrest".to_owned(),
        };
        let Ok(mut mutation) = ItemMutation::set(&project) else {
            return;
        };
        mutation.service_id = "other".to_owned();
        assert!(!mutation.is::<Project>());
        assert!(matches!(
            mutation.decode_set::<Project>(),
            Err(ItemError::SchemaMismatch {
                expected_service,
                ..
            }) if expected_service == Project::SERVICE_ID
        ));
        mutation.service_id.clear();
        assert!(matches!(
            mutation.validate_envelope(),
            Err(ItemError::EmptyServiceId)
        ));
    }

    #[test]
    fn projection_retains_authoritative_revision_and_atomic_batch_order() {
        let later_id = ProjectId::from("z-project");
        let earlier_id = ProjectId::from("a-project");
        let later = Project {
            id: later_id.clone(),
            title: "first in batch".to_owned(),
        };
        let earlier = Project {
            id: earlier_id,
            title: "second in batch".to_owned(),
        };
        let newest = Project {
            id: ProjectId::from("m-project"),
            title: "next revision".to_owned(),
        };
        let mut projection = ItemProjection::<Project>::default();
        let later = ItemMutation::set(&later);
        let earlier = ItemMutation::set(&earlier);
        let newest = ItemMutation::set(&newest);
        assert!(later.is_ok() && earlier.is_ok() && newest.is_ok());
        let (Ok(later), Ok(earlier), Ok(newest)) = (later, earlier, newest) else {
            return;
        };
        assert!(projection.apply_at_order(&later, 7, 0).is_ok());
        assert!(projection.apply_at_order(&earlier, 7, 1).is_ok());
        assert!(projection.apply_at_order(&newest, 8, 0).is_ok());

        let titles = projection
            .values_by_last_change()
            .into_iter()
            .map(|project| project.title.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            titles,
            ["first in batch", "second in batch", "next revision"]
        );
        assert_eq!(projection.last_changed_at(&later_id), Some(7));
        let replacement = Project {
            id: later_id.clone(),
            title: "changed later".to_owned(),
        };
        let Ok(replacement) = ItemMutation::set(&replacement) else {
            return;
        };
        assert!(projection.apply_at_order(&replacement, 9, 0).is_ok());
        assert_eq!(projection.first_changed_at(&later_id), Some(7));
        assert_eq!(projection.last_changed_at(&later_id), Some(9));
    }
}
