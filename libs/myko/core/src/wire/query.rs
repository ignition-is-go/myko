use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use hyphae::MapDiff;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{item::ErasedWrappedItem, shared::value_with_tx};
use crate::{
    TS,
    core::{
        item::AnyItem,
        query::{QueryId, QueryItemType},
    },
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<QueryChange>,

    pub deletes: Vec<Arc<str>>,

    pub upserts: Vec<ErasedWrappedItem>,

    pub sequence: u64,

    pub tx: Arc<str>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_count: Option<usize>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<QueryWindow>,
}

/// Custom Deserialize for `QueryResponse`: deserializes using Value-based items,
/// then parses each through the item registry to produce Arc<dyn AnyItem>.
/// Falls back to a no-op `AnyItem` wrapper when running on the client side (WASM)
/// or when the item type is unknown.
impl<'de> Deserialize<'de> for QueryResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = ClientQueryResponse::deserialize(deserializer)?;
        Ok(Self {
            changes: raw
                .changes
                .into_iter()
                .map(|c| match c {
                    ClientQueryChange::Upsert { item } => QueryChange::Upsert {
                        item: ErasedWrappedItem {
                            item: Arc::new(ValueItem {
                                value: item.item,
                                item_type: item.item_type.clone(),
                            }),
                            item_type: item.item_type,
                        },
                    },
                    ClientQueryChange::Delete { id } => QueryChange::Delete { id },
                    ClientQueryChange::WindowOrder {
                        ids,
                        total_count,
                        window,
                    } => QueryChange::WindowOrder {
                        ids,
                        total_count,
                        window,
                    },
                })
                .collect(),
            deletes: raw.deletes,
            upserts: raw
                .upserts
                .into_iter()
                .map(|item| ErasedWrappedItem {
                    item: Arc::new(ValueItem {
                        value: item.item,
                        item_type: item.item_type.clone(),
                    }),
                    item_type: item.item_type,
                })
                .collect(),
            sequence: raw.sequence,
            tx: raw.tx,
            total_count: raw.total_count,
            window: raw.window,
        })
    }
}

/// Thin `AnyItem` wrapper around a `serde_json::Value`, used when deserializing
/// `QueryResponse` on the client side where concrete entity types are unavailable.
#[derive(Debug, Clone)]
struct ValueItem {
    value: Value,
    item_type: Arc<str>,
}

impl Serialize for ValueItem {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.value.serialize(serializer)
    }
}

impl crate::common::with_id::WithId for ValueItem {
    fn id(&self) -> Arc<str> {
        self.value
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .into()
    }
}

impl AnyItem for ValueItem {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn entity_type(&self) -> &'static str {
        // NOTE(ts): leak is acceptable here because entity types are a fixed set
        Box::leak(self.item_type.to_string().into_boxed_str())
    }
    fn equals(&self, other: &dyn AnyItem) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|typed| self.value == typed.value)
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum QueryChange {
    Upsert {
        item: ErasedWrappedItem,
    },
    Delete {
        id: Arc<str>,
    },
    WindowOrder {
        ids: Vec<Arc<str>>,
        total_count: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        window: Option<QueryWindow>,
    },
}

pub struct QueryResult<T> {
    pub deletes: Vec<String>,
    pub upserts: Vec<T>,
    pub sequence: u64,
    pub tx: String,
}

impl<T> QueryResult<T> {
    #[must_use]
    pub const fn new(tx: String, upserts: Vec<T>) -> Self {
        Self {
            deletes: vec![],
            upserts,
            sequence: 0,
            tx,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn collect_query_change(
    diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>,
    upserts: &mut Vec<ErasedWrappedItem>,
    deletes: &mut Vec<Arc<str>>,
    changes: &mut Vec<QueryChange>,
) {
    match diff {
        MapDiff::Initial { entries } => {
            for (_, item) in entries {
                let wrapped = ErasedWrappedItem {
                    item: item.clone(),
                    item_type: item.entity_type().into(),
                };
                changes.push(QueryChange::Upsert {
                    item: wrapped.clone(),
                });
                upserts.push(wrapped);
            }
        }
        MapDiff::Insert { value, .. } => {
            let wrapped = ErasedWrappedItem {
                item: value.clone(),
                item_type: value.entity_type().into(),
            };
            changes.push(QueryChange::Upsert {
                item: wrapped.clone(),
            });
            upserts.push(wrapped);
        }
        MapDiff::Update { new_value, .. } => {
            let wrapped = ErasedWrappedItem {
                item: new_value.clone(),
                item_type: new_value.entity_type().into(),
            };
            changes.push(QueryChange::Upsert {
                item: wrapped.clone(),
            });
            upserts.push(wrapped);
        }
        MapDiff::Remove { key, .. } => {
            deletes.push(key.clone());
            changes.push(QueryChange::Delete { id: key.clone() });
        }
        MapDiff::Batch { changes: batch } => {
            for change in batch {
                collect_query_change(change, upserts, deletes, changes);
            }
        }
    }
}

impl QueryResponse {
    #[must_use]
    pub fn new(tx: Arc<str>, _result: Vec<Value>) -> Self {
        Self {
            changes: vec![],
            sequence: 0,
            upserts: vec![],
            deletes: vec![],
            tx,
            total_count: None,
            window: None,
        }
    }

    /// Create a `QueryResponse` from a `MapDiff`.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn from_diff(
        diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>,
        tx: Arc<str>,
        sequence: u64,
    ) -> Self {
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        let mut changes = Vec::new();
        collect_query_change(diff, &mut upserts, &mut deletes, &mut changes);
        Self {
            tx,
            sequence,
            changes,
            upserts,
            deletes,
            total_count: None,
            window: None,
        }
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

impl QueryResponse {
    #[must_use]
    pub fn get_tx(&self) -> Arc<str> {
        self.tx.clone()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueryWindow {
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueryWindowUpdate {
    pub tx: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<QueryWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WrappedQuery {
    pub query: Value,
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<QueryWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    pub tx: String,
    pub query_id: String,
    pub message: String,
}

impl QueryError {
    pub fn new(
        tx: impl Into<String>,
        query_id: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            tx: tx.into(),
            query_id: query_id.into(),
            message: message.into(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Client-side (inbound) deserialization types
// ─────────────────────────────────────────────────────────────────────────────

/// Client-side query response for deserialization. Uses `WrappedItem` (JSON values)
/// instead of the server-side `ErasedWrappedItem` (type-erased `AnyItem`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientQueryResponse {
    #[serde(default)]
    pub changes: Vec<ClientQueryChange>,

    pub deletes: Vec<Arc<str>>,

    pub upserts: Vec<super::item::WrappedItem>,

    pub sequence: u64,

    pub tx: Arc<str>,

    #[serde(default)]
    pub total_count: Option<usize>,

    #[serde(default)]
    pub window: Option<QueryWindow>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ClientQueryChange {
    Upsert {
        item: super::item::WrappedItem,
    },
    Delete {
        id: Arc<str>,
    },
    WindowOrder {
        ids: Vec<Arc<str>>,
        total_count: usize,
        #[serde(default)]
        window: Option<QueryWindow>,
    },
}

///
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn wrap_query<Q: QueryId + QueryItemType + Serialize + Clone>(
    tx: Arc<str>,
    query: &Q,
) -> Result<WrappedQuery, serde_json::Error> {
    Ok(WrappedQuery {
        query: value_with_tx(tx, query)?,
        query_id: query.query_id(),
        query_item_type: query.query_item_type(),
        window: None,
    })
}
