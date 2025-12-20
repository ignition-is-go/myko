use crate::{
    actors::query::query_manager::RegisterQueryData,
    client::MykoClient,
    common::{with_id::WithId, with_transaction::WithTransaction},
    item::Eventable,
    parsers::query::{AnyQuery, CapturedQueryParser, MykoQueryParser},
    prelude::AnyItem,
    server::MykoServerCtx,
};
use chrono::Utc;
use log::error;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{any::Any, sync::Arc};
use ts_rs::TS;
use uuid::Uuid;

inventory::collect!(QueryRegistration);

/// Type alias for the query factory function pointer.
/// Returns the data needed to register a query (closure + parser).
pub type QueryFactoryFn = fn() -> RegisterQueryData;

// ─────────────────────────────────────────────────────────────────────────────
// QueryRequest<Q> - Generic wrapper that adds tx/created_at to any query params
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps query parameters with transaction metadata.
///
/// This type adds `tx` (transaction ID) and `created_at` timestamp to any query
/// parameters struct. Uses `#[serde(flatten)]` to serialize as a flat structure.
///
/// # Example
///
/// ```ignore
/// // Query params (what user defines):
/// #[myko_query(Server)]
/// pub struct GetServersByIds {
///     pub ids: Vec<Arc<str>>,
/// }
///
/// // Create a request:
/// let request = QueryRequest::new(GetServersByIds { ids: vec![...] });
///
/// // Serializes to: { "tx": "...", "createdAt": "...", "ids": [...] }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest<Q> {
    pub tx: Arc<str>,
    pub created_at: Arc<str>,
    #[serde(flatten)]
    pub query: Q,
}

impl<Q> QueryRequest<Q> {
    /// Create a new query request with auto-generated tx and timestamp.
    pub fn new(query: Q) -> Self {
        Self {
            tx: Uuid::new_v4().to_string().into(),
            created_at: Utc::now().to_rfc3339().into(),
            query,
        }
    }

    /// Create a new query request with a specific tx.
    /// Used by reports to share their tx with query subscriptions.
    pub fn with_tx(query: Q, tx: Arc<str>) -> Self {
        Self {
            tx,
            created_at: Utc::now().to_rfc3339().into(),
            query,
        }
    }
}

impl<Q: Default> Default for QueryRequest<Q> {
    fn default() -> Self {
        Self::new(Q::default())
    }
}

/// Convert query params directly into a QueryRequest.
/// This only works for types that implement QueryParams (actual query param structs),
/// not for QueryRequest itself, which avoids ambiguity with From<&QueryRequest<Q>>.
impl<Q: QueryParams> From<Q> for QueryRequest<Q> {
    fn from(query: Q) -> Self {
        Self::new(query)
    }
}

/// Convert a reference to a QueryRequest into an owned QueryRequest by cloning.
impl<Q: Clone> From<&QueryRequest<Q>> for QueryRequest<Q> {
    fn from(request: &QueryRequest<Q>) -> Self {
        request.clone()
    }
}

impl<Q: Send + Sync + 'static> WithTransaction for QueryRequest<Q> {
    fn tx_id(&self) -> Arc<str> {
        self.tx.clone()
    }
}

impl<Q: QueryId> QueryId for QueryRequest<Q> {
    fn query_id(&self) -> Arc<str> {
        self.query.query_id()
    }
}

impl<Q: QueryIdStatic> QueryIdStatic for QueryRequest<Q> {
    fn query_id_static() -> Arc<str> {
        Q::query_id_static()
    }
}

impl<Q: QueryItemType> QueryItemType for QueryRequest<Q> {
    type Item = Q::Item;

    fn query_item_type(&self) -> Arc<str> {
        self.query.query_item_type()
    }

    fn query_item_type_static() -> Arc<str> {
        Q::query_item_type_static()
    }
}

impl<Q: QueryHandler + Clone> QueryHandler for QueryRequest<Q> {
    fn test_entity(ctx: QueryHandlerCtx<Self>) -> bool {
        // Delegate to inner query's test_entity
        // We need to create a QueryHandlerCtx for the inner type
        Q::test_entity(QueryHandlerCtx {
            item: ctx.item,
            query: Arc::new(ctx.query.query.clone()),
            server_ctx: ctx.server_ctx,
        })
    }
}

impl<Q: QueryId + QueryItemType + Serialize + std::fmt::Debug + Send + Sync + 'static> AnyQuery
    for QueryRequest<Q>
{
    fn query_item_type(&self) -> Arc<str> {
        QueryItemType::query_item_type(self)
    }

    fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("QueryRequest should serialize to JSON")
    }
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

pub struct QueryRegistration {
    pub query_id: &'static str,
    pub query_item_type: &'static str,
    pub crate_name: &'static str,
    /// Factory function that creates the registration data (closure + parser)
    pub factory: QueryFactoryFn,
}

impl std::fmt::Debug for QueryRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRegistration")
            .field("query_id", &self.query_id)
            .field("query_item_type", &self.query_item_type)
            .field("crate_name", &self.crate_name)
            .finish()
    }
}

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

/// implementing QueryHandler for a MykoQuery is required to define the logic for filtering entities based on the query.
///
/// it requires one function: test_entity which takes a `QueryHandlerContext<Self>` and returns a `bool`.
/// this answers the question of whether an entity should be included in the query results.
///
/// if `true`, updates to this query will be calculated, and the item will be added or updated as appropriate.
///
/// if `false`, updates to this query will be calculated, and the item will be removed if it exists.
///
/// any deduplication of changes to this query are handled upstream in the handler logic
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

/// Extension trait for QueryParams to provide a factory function.
///
/// This is implemented for all QueryParams types via blanket impl.
pub trait QueryFactory: QueryParams {
    /// Create the registration data (closure + parser) for this query type.
    fn create_registration() -> RegisterQueryData;
}

impl<Q: QueryParams> QueryFactory for Q
where
    Q::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    fn create_registration() -> RegisterQueryData {
        // The closure works with QueryRequest<Q> internally
        let closure = Arc::new(|ctx: QueryHandlerCtxAny| -> bool {
            let item_ref: Arc<dyn Any + Send + Sync> = ctx.item;
            let query_ref: Arc<dyn Any + Send + Sync> = ctx.query;

            let item = item_ref.downcast::<Q::Item>();
            let query = query_ref.downcast::<QueryRequest<Q>>();

            if query.is_err() {
                error!(
                    "Query did not correctly downcast in closure: {}",
                    Q::query_id_static()
                );
                return false;
            }

            let query = query.expect("Query downcast should be valid");

            if item.is_err() {
                error!(
                    "Item did not downcast: {} in {}",
                    Q::query_item_type_static(),
                    Q::query_id_static(),
                );
                return false;
            }

            let item = item.expect("Item downcast should be valid");

            <QueryRequest<Q> as QueryHandler>::test_entity(QueryHandlerCtx::<QueryRequest<Q>> {
                server_ctx: ctx.ctx,
                item: item.clone(),
                query: query.clone(),
            })
        });

        let parser: Arc<dyn MykoQueryParser> = Arc::new(CapturedQueryParser::<QueryRequest<Q>>::new());

        RegisterQueryData {
            query_id: Q::query_id_static(),
            query_item_type: Q::query_item_type_static(),
            closure,
            parser,
        }
    }
}
