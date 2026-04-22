//! QueryRequest wrapper type.

use std::sync::Arc;

use crate::TS;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(not(target_arch = "wasm32"))]
use super::traits::QueryBuildCellCtx;
use super::traits::{
    AnyQuery, QueryHandler, QueryId, QueryIdStatic, QueryItemType, QueryParams, QueryTestCtx,
};
use crate::common::with_transaction::WithTransaction;
#[cfg(not(target_arch = "wasm32"))]
use crate::core::query::cell::FilteredCellMap;

#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest<Q> {
    pub tx: Arc<str>,
    #[serde(default = "default_created_at")]
    pub created_at: Arc<str>,
    #[serde(flatten)]
    pub query: Q,
}

fn default_created_at() -> Arc<str> {
    Utc::now().to_rfc3339().into()
}

impl<Q> QueryRequest<Q> {
    pub fn new(query: Q) -> Self {
        Self {
            tx: Uuid::new_v4().to_string().into(),
            created_at: default_created_at(),
            query,
        }
    }

    pub fn with_tx(query: Q, tx: Arc<str>) -> Self {
        Self {
            tx,
            created_at: default_created_at(),
            query,
        }
    }
}

impl<Q: Default> Default for QueryRequest<Q> {
    fn default() -> Self {
        Self::new(Q::default())
    }
}

impl<Q: QueryParams> From<Q> for QueryRequest<Q> {
    fn from(query: Q) -> Self {
        Self::new(query)
    }
}

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
    fn test_entity(ctx: QueryTestCtx<Self>) -> bool {
        Q::test_entity(QueryTestCtx {
            item: ctx.item,
            query: Arc::new(ctx.query.query.clone()),
            query_context: ctx.query_context,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn build_view(ctx: QueryBuildCellCtx<Self>) -> Option<FilteredCellMap> {
        Q::build_view(QueryBuildCellCtx {
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
