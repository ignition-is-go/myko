use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use hypha::MapDiff;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

use super::{item::WrappedItem, shared::value_with_tx};
#[cfg(not(target_arch = "wasm32"))]
use crate::core::item::AnyItem;
use crate::core::query::{QueryId, QueryItemType};

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<QueryChange>,

    pub deletes: Vec<Arc<str>>,

    pub upserts: Vec<WrappedItem<Value>>,

    pub sequence: u64,

    pub tx: Arc<str>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_count: Option<usize>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<QueryWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum QueryChange {
    Upsert {
        item: WrappedItem<Value>,
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
    pub fn new(tx: String, upserts: Vec<T>) -> QueryResult<T> {
        QueryResult {
            deletes: vec![],
            upserts,
            sequence: 0,
            tx,
        }
    }
}

impl QueryResponse {
    pub fn new(tx: Arc<str>, _result: Vec<Value>) -> QueryResponse {
        QueryResponse {
            changes: vec![],
            sequence: 0,
            upserts: vec![],
            deletes: vec![],
            tx,
            total_count: None,
            window: None,
        }
    }

    /// Create a QueryResponse from a MapDiff.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_diff(
        diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>,
        tx: Arc<str>,
        sequence: u64,
    ) -> QueryResponse {
        fn push_change(
            diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>,
            upserts: &mut Vec<WrappedItem<Value>>,
            deletes: &mut Vec<Arc<str>>,
            changes: &mut Vec<QueryChange>,
        ) {
            match diff {
                MapDiff::Initial { entries } => {
                    for (_, item) in entries {
                        let wrapped = WrappedItem {
                            item: item.to_value(),
                            item_type: item.entity_type().into(),
                        };
                        changes.push(QueryChange::Upsert {
                            item: wrapped.clone(),
                        });
                        upserts.push(wrapped);
                    }
                }
                MapDiff::Insert { key: _, value } => {
                    let wrapped = WrappedItem {
                        item: value.to_value(),
                        item_type: value.entity_type().into(),
                    };
                    changes.push(QueryChange::Upsert {
                        item: wrapped.clone(),
                    });
                    upserts.push(wrapped);
                }
                MapDiff::Update {
                    key: _,
                    old_value: _,
                    new_value,
                } => {
                    let wrapped = WrappedItem {
                        item: new_value.to_value(),
                        item_type: new_value.entity_type().into(),
                    };
                    changes.push(QueryChange::Upsert {
                        item: wrapped.clone(),
                    });
                    upserts.push(wrapped);
                }
                MapDiff::Remove { key, old_value: _ } => {
                    deletes.push(key.clone());
                    changes.push(QueryChange::Delete { id: key.clone() });
                }
                MapDiff::Batch { changes: batch } => {
                    for change in batch {
                        push_change(change, upserts, deletes, changes);
                    }
                }
            }
        }

        match diff {
            MapDiff::Initial { entries } => {
                let upserts: Vec<WrappedItem<Value>> = entries
                    .iter()
                    .map(|(_, item)| WrappedItem {
                        item: item.to_value(),
                        item_type: item.entity_type().into(),
                    })
                    .collect();
                let changes = upserts
                    .iter()
                    .cloned()
                    .map(|item| QueryChange::Upsert { item })
                    .collect();
                QueryResponse {
                    tx,
                    sequence,
                    changes,
                    upserts,
                    deletes: vec![],
                    total_count: None,
                    window: None,
                }
            }
            MapDiff::Insert { key: _, value } => {
                let upserts = vec![WrappedItem {
                    item: value.to_value(),
                    item_type: value.entity_type().into(),
                }];
                let changes = upserts
                    .iter()
                    .cloned()
                    .map(|item| QueryChange::Upsert { item })
                    .collect();
                QueryResponse {
                    tx,
                    sequence,
                    changes,
                    upserts,
                    deletes: vec![],
                    total_count: None,
                    window: None,
                }
            }
            MapDiff::Update {
                key: _,
                old_value: _,
                new_value,
            } => {
                let upserts = vec![WrappedItem {
                    item: new_value.to_value(),
                    item_type: new_value.entity_type().into(),
                }];
                let changes = upserts
                    .iter()
                    .cloned()
                    .map(|item| QueryChange::Upsert { item })
                    .collect();
                QueryResponse {
                    tx,
                    sequence,
                    changes,
                    upserts,
                    deletes: vec![],
                    total_count: None,
                    window: None,
                }
            }
            MapDiff::Remove { key, old_value: _ } => QueryResponse {
                tx,
                sequence,
                changes: vec![QueryChange::Delete { id: key.clone() }],
                upserts: vec![],
                deletes: vec![key.clone()],
                total_count: None,
                window: None,
            },
            MapDiff::Batch { .. } => {
                let mut upserts = Vec::new();
                let mut deletes = Vec::new();
                let mut changes = Vec::new();
                push_change(diff, &mut upserts, &mut deletes, &mut changes);
                QueryResponse {
                    tx,
                    sequence,
                    changes,
                    upserts,
                    deletes,
                    total_count: None,
                    window: None,
                }
            }
        }
    }

    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

impl QueryResponse {
    pub fn get_tx(&self) -> Arc<str> {
        self.tx.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
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
