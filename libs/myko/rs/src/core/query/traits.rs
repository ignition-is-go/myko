//! Query trait definitions.

use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    client::MykoClient,
    common::{with_id::WithId, with_transaction::WithTransaction},
    registry::{
        item::{AnyItem, Eventable},
        query::AnyQuery,
    },
};

use super::{context::MykoServerCtx, request::QueryRequest};

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
    type Item: Send + Sync;
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
    fn test_entity(ctx: QueryHandlerCtx<Self>) -> bool;
}

pub struct QueryHandlerCtx<TQuery: QueryItemType> {
    pub item: Arc<TQuery::Item>,
    pub query: Arc<TQuery>,
    pub server_ctx: Arc<MykoServerCtx>,
}

#[derive(Debug)]
pub struct QueryHandlerCtxAny {
    pub item: Arc<dyn AnyItem>,
    pub query: Arc<dyn AnyQuery>,
    pub ctx: Arc<MykoServerCtx>,
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
    Serialize
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
    ) -> impl tokio_stream::Stream<Item = Vec<<Self as QueryItemType>::Item>>;
}

// Blanket impl of Query for QueryRequest<Q>
impl<Q: QueryParams + Clone> Query for QueryRequest<Q>
where
    Q::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    type Params = Q;

    fn watch(
        &self,
        client: &MykoClient,
    ) -> impl tokio_stream::Stream<Item = Vec<<Self as QueryItemType>::Item>> {
        client.watch_query::<Q>(self)
    }
}
