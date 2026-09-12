//! Query outputs keep durable values and their lifecycle in one publication.

use std::{collections::BTreeMap, sync::Arc};

use hyphae::MapQuery;

use super::FilteredCellMap;
use crate::item::AnyItem;

/// Canonical keyed rows after query item type erasure.
pub type QueryRows = BTreeMap<Arc<str>, Arc<dyn AnyItem>>;

/// A process-local reactive query map, without durable lifecycle metadata.
pub type LocalQueryMap<T, K = Arc<str>> = hyphae::CellMap<K, Arc<T>, hyphae::CellImmutable>;

/// Materialized query output. A retained result cannot become a raw local map.
#[derive(Clone)]
pub enum QueryValue {
    LocalMap(FilteredCellMap),
    #[cfg(not(target_arch = "wasm32"))]
    RetainedPublication(myko_federation::LiveSubscription<QueryRows>),
}

impl QueryValue {
    /// Read coherent rows, rejecting an absent or stale durable result.
    ///
    /// # Errors
    /// Returns an error when a durable dependency is not current.
    pub fn read_current(&self) -> Result<QueryRows, String> {
        match self {
            Self::LocalMap(map) => Ok(map.snapshot().into_iter().collect()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::RetainedPublication(publication) => {
                let state = publication.current();
                if state.liveness != myko_federation::SubscriptionLiveness::Current {
                    return Err(format!("query is not current: {:?}", state.liveness));
                }
                state
                    .value
                    .ok_or_else(|| "query has no current value".to_owned())
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// # Errors
    /// Rejects local maps, which have no durable publication or cursor.
    pub fn into_retained(self) -> Result<myko_federation::LiveSubscription<QueryRows>, String> {
        match self {
            Self::RetainedPublication(publication) => Ok(publication),
            Self::LocalMap(_) => Err("local query has no retained publication".to_owned()),
        }
    }

    /// # Errors
    /// Rejects retained output because a raw map cannot represent its lifecycle.
    pub fn into_local_map(self) -> Result<FilteredCellMap, String> {
        match self {
            Self::LocalMap(map) => Ok(map),
            #[cfg(not(target_arch = "wasm32"))]
            Self::RetainedPublication(_) => {
                Err("retained query output cannot be converted to a local map".to_owned())
            }
        }
    }
}

pub enum WeakQueryValue {
    LocalMap(hyphae::WeakCellMap<Arc<str>, Arc<dyn AnyItem>>),
    #[cfg(not(target_arch = "wasm32"))]
    RetainedPublication(myko_federation::WeakLiveSubscription<QueryRows>),
}

impl WeakQueryValue {
    pub(crate) fn new(value: &QueryValue) -> Self {
        match value {
            QueryValue::LocalMap(map) => Self::LocalMap(map.downgrade()),
            #[cfg(not(target_arch = "wasm32"))]
            QueryValue::RetainedPublication(publication) => {
                Self::RetainedPublication(publication.downgrade())
            }
        }
    }

    pub(crate) fn upgrade(&self) -> Option<QueryValue> {
        match self {
            Self::LocalMap(map) => map.upgrade().map(|map| QueryValue::LocalMap(map.lock())),
            #[cfg(not(target_arch = "wasm32"))]
            Self::RetainedPublication(publication) => {
                publication.upgrade().map(QueryValue::RetainedPublication)
            }
        }
    }
}

/// Query output accepted without discarding durable dependency metadata.
pub trait QueryBuildOutput {
    fn materialize_query(self) -> QueryValue;
}

impl QueryBuildOutput for QueryValue {
    fn materialize_query(self) -> Self {
        self
    }
}

impl<Q> QueryBuildOutput for Q
where
    Q: MapQuery<Key = Arc<str>, Value = Arc<dyn AnyItem>>,
{
    fn materialize_query(self) -> QueryValue {
        QueryValue::LocalMap(self.materialize())
    }
}

/// A query derived from a coherent durable source publication.
#[cfg(not(target_arch = "wasm32"))]
pub struct RetainedQuery<T: AnyItem + hyphae::CellValue> {
    publication: myko_federation::LiveSubscription<std::collections::BTreeMap<Arc<str>, Arc<T>>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T: AnyItem + hyphae::CellValue> RetainedQuery<T> {
    #[must_use]
    pub const fn new(
        publication: myko_federation::LiveSubscription<
            std::collections::BTreeMap<Arc<str>, Arc<T>>,
        >,
    ) -> Self {
        Self { publication }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T: AnyItem + hyphae::CellValue> QueryBuildOutput for RetainedQuery<T> {
    fn materialize_query(self) -> QueryValue {
        QueryValue::RetainedPublication(self.publication.map_value(|rows| {
            rows.iter()
                .map(|(key, value)| {
                    let erased: Arc<dyn AnyItem> = value.clone();
                    (Arc::clone(key), erased)
                })
                .collect()
        }))
    }
}
