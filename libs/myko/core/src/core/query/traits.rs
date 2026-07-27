//! Query trait definitions.

use std::{fmt::Debug, sync::Arc};

use hyphae::{CellImmutable, MapQuery};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::{
    super::item::{AnyItem, Eventable},
    context::QueryContext,
    request::QueryRequest,
};
use crate::{
    cache::CacheKey,
    client::MykoClient,
    common::{with_id::WithId, with_transaction::WithTransaction},
    core::query::{QueryBuildContext, cell::FilteredCellMap},
    prelude::WithTypedId,
    wire::WrappedQuery,
};

// ─────────────────────────────────────────────────────────────────────────────
// Core Query Traits
// ─────────────────────────────────────────────────────────────────────────────

pub trait QueryId {
    fn query_id(&self) -> Arc<str>;
}

pub trait QueryIdStatic {
    fn query_id_static() -> Arc<str>;
}

pub trait QueryItemType {
    type Item: WithTypedId + std::fmt::Debug + PartialEq + Send + Sync;
    fn query_item_type(&self) -> Arc<str>;
    fn query_item_type_static() -> Arc<str>;
}

/// Implementing QueryHandler for a MykoQuery is required to define the logic for filtering entities based on the query.
///
/// It requires one function: test_entity which takes a `QueryHandlerContext<Self>` and returns a `bool`.
/// This answers the question of whether an entity should be included in the query results.
///
/// If `true`, updates to this query will be calculated, and the item will be added or updated as appropriate.
///
/// If `false`, updates to this query will be calculated, and the item will be removed if it exists.
///
/// Any deduplication of changes to this query are handled upstream in the handler logic.
pub trait QueryHandler: QueryItemType + Sized {
    /// Per-entity membership predicate.
    ///
    /// Return `true` when an item should be included in the query result.
    fn test_entity(ctx: QueryTestContext<Self>) -> bool
    where
        Self: Send + Sync + 'static;

    /// Optional set-wise reactive builder for complex many-to-many joins.
    ///
    /// When implemented, this is preferred by the runtime over per-item
    /// `test_entity` evaluation and should return a reactive map plan that
    /// the runtime materializes once at the registration boundary. Returning
    /// `impl MapQuery<...>` lets impls compose `inner_join`, `project_map`,
    /// `select_cell`, etc. without forcing intermediate `CellMap` allocations.
    /// Concrete `CellMap`/`FilteredCellMap` values still satisfy the bound
    /// via the blanket impl on `ReactiveMap`, so simple impls returning a
    /// pre-built map continue to work unchanged.
    fn build_view(_ctx: QueryBuildArgs<Self>) -> Option<impl MapQuery<Arc<str>, Arc<dyn AnyItem>>>
    where
        Self: Send + Sync + 'static,
    {
        None::<FilteredCellMap>
    }
}

pub struct QueryTestContext<TQuery: QueryItemType> {
    pub item: Arc<TQuery::Item>,
    pub query: Arc<TQuery>,
    pub query_context: Arc<QueryContext>,
}

impl<TQuery: QueryItemType> QueryTestContext<TQuery> {
    pub fn map_bool<F>(self, predicate: F) -> bool
    where
        F: Fn(QueryTestContext<TQuery>) -> bool,
    {
        predicate(self)
    }
}

pub struct QueryBuildArgs<TQuery: QueryItemType> {
    pub query: Arc<TQuery>,
    pub query_context: QueryBuildContext,
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryParams - Marker trait for query parameter structs (inner type)
// ─────────────────────────────────────────────────────────────────────────────

/// Marker trait for query parameter structs.
///
/// This is implemented by the user-defined query struct (e.g., `GetServersByIds`).
/// It combines identity traits without requiring transaction metadata.
///
/// The full `Query` trait is implemented on `QueryRequest<Q>` where `Q: QueryParams`.
pub trait QueryParams:
    CacheKey
    + Serialize
    + DeserializeOwned
    + Clone
    + Send
    + Sync
    + QueryId
    + QueryIdStatic
    + QueryItemType
    + QueryHandler
    + std::fmt::Debug
    + 'static
{
}

// Blanket impl for any type that satisfies the bounds
impl<T> QueryParams for T where
    T: Serialize
        + CacheKey
        + DeserializeOwned
        + Clone
        + Send
        + Sync
        + QueryId
        + QueryIdStatic
        + QueryItemType
        + QueryHandler
        + std::fmt::Debug
        + 'static
{
}

// ─────────────────────────────────────────────────────────────────────────────
// Query - Full trait implemented on QueryRequest<Q>
// ─────────────────────────────────────────────────────────────────────────────

/// Full query trait implemented on `QueryRequest<Q>`.
///
/// This provides the `watch` method for client-side subscriptions.
/// For server-side registration, use `Q::register()` on the params type.
pub trait Query:
    Serialize
    + DeserializeOwned
    + Send
    + Sync
    + QueryId
    + QueryIdStatic
    + QueryItemType
    + QueryHandler
    + WithTransaction
    + AnyQuery
    + 'static
{
    /// The inner query params type
    type Params: QueryParams;

    fn watch(
        &self,
        client: &MykoClient,
    ) -> hyphae::Cell<Vec<Arc<<Self as QueryItemType>::Item>>, CellImmutable>;
}

// Blanket impl of Query for QueryRequest<Q>
impl<Q: QueryParams + Clone> Query for QueryRequest<Q>
where
    Q::Item:
        Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    type Params = Q;

    fn watch(
        &self,
        client: &MykoClient,
    ) -> hyphae::Cell<Vec<Arc<<Self as QueryItemType>::Item>>, CellImmutable> {
        client.watch_query::<Q>(self)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AnyQuery - Type-erased query trait
// ─────────────────────────────────────────────────────────────────────────────

/// Type-erased query trait for dynamic dispatch.
/// All queries implement this via the `#[myko_query]` macro.
pub trait AnyQuery: WithTransaction + QueryId + Debug + Send + Sync + 'static {
    /// Returns the item type this query targets (e.g., "Server", "Client").
    fn query_item_type(&self) -> Arc<str>;

    /// Serialize this query to a JSON Value.
    fn to_value(&self) -> Value;
}

// Conversion from Arc<dyn AnyQuery> to WrappedQuery
impl From<&dyn AnyQuery> for WrappedQuery {
    fn from(query: &dyn AnyQuery) -> Self {
        WrappedQuery {
            query: query.to_value(),
            query_id: query.query_id(),
            query_item_type: query.query_item_type(),
            window: None,
        }
    }
}

impl From<Arc<dyn AnyQuery>> for WrappedQuery {
    fn from(query: Arc<dyn AnyQuery>) -> Self {
        WrappedQuery::from(query.as_ref())
    }
}

impl From<&Arc<dyn AnyQuery>> for WrappedQuery {
    fn from(query: &Arc<dyn AnyQuery>) -> Self {
        WrappedQuery::from(query.as_ref())
    }
}
