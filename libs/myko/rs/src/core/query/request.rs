//! QueryRequest wrapper type.

use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use uuid::Uuid;

use super::traits::{
    AnyQuery, QueryHandler, QueryId, QueryIdStatic, QueryItemType, QueryParams, QueryTestCellCtx,
};
use crate::common::with_transaction::WithTransaction;
#[cfg(not(target_arch = "wasm32"))]
use hypha::{Cell, CellImmutable};

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

impl<Q: QueryHandler + Clone + Send + Sync + 'static> QueryHandler for QueryRequest<Q> {
    #[cfg(not(target_arch = "wasm32"))]
    fn test_entity(ctx: QueryTestCellCtx<Self>) -> Cell<bool, CellImmutable> {
        Q::test_entity(QueryTestCellCtx {
            item: ctx.item,
            query: Arc::new(ctx.query.query.clone()),
            query_context: ctx.query_context,
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
