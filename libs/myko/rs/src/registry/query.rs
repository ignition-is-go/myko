use crate::{
    api::query::WrappedQuery,
    prelude::WithTransaction,
    query::{Query, QueryId},
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{fmt::Debug, sync::Arc};

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

pub trait MykoQueryParser: Send + Sync + 'static {
    fn parse(&self, value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;
}

pub struct CapturedQueryParser<T> {
    phantom: std::marker::PhantomData<T>,
}

impl<T: Query + Send + Sync> Default for CapturedQueryParser<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Query + Send + Sync> CapturedQueryParser<T> {
    pub fn new() -> Self {
        Self {
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T: DeserializeOwned + AnyQuery> MykoQueryParser for CapturedQueryParser<T> {
    fn parse(&self, value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error> {
        let item = serde_json::from_value::<T>(value)?;
        Ok(Arc::new(item))
    }
}
