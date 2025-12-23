use std::sync::Arc;

use hypha::MapDiff;
use serde::{Deserialize, Serialize, de::Error};
use serde_json::Value;
use ts_rs::TS;

use crate::core::{item::AnyItem, query::{QueryId, QueryItemType}};

use super::item::WrappedItem;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub deletes: Vec<Arc<str>>,

    pub upserts: Vec<WrappedItem<Value>>,

    pub sequence: u64,

    pub tx: Arc<str>,
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
            sequence: 0,
            upserts: vec![],
            deletes: vec![],
            tx,
        }
    }

    /// Create a QueryResponse from a MapDiff.
    pub fn from_diff(
        diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>,
        tx: Arc<str>,
        sequence: u64,
    ) -> QueryResponse {
        match diff {
            MapDiff::Initial { entries } => {
                let upserts = entries
                    .iter()
                    .map(|(_, item)| WrappedItem {
                        item: item.to_value(),
                        item_type: item.entity_type().into(),
                    })
                    .collect();
                QueryResponse { tx, sequence, upserts, deletes: vec![] }
            }
            MapDiff::Insert { key: _, value } => {
                let upserts = vec![WrappedItem {
                    item: value.to_value(),
                    item_type: value.entity_type().into(),
                }];
                QueryResponse { tx, sequence, upserts, deletes: vec![] }
            }
            MapDiff::Update { key: _, old_value: _, new_value } => {
                let upserts = vec![WrappedItem {
                    item: new_value.to_value(),
                    item_type: new_value.entity_type().into(),
                }];
                QueryResponse { tx, sequence, upserts, deletes: vec![] }
            }
            MapDiff::Remove { key, old_value: _ } => {
                QueryResponse { tx, sequence, upserts: vec![], deletes: vec![key.clone()] }
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
pub struct WrappedQuery {
    pub query: Value,
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
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
    let mut json = serde_json::to_value(query.clone())?;

    let obj_mut = json.as_object_mut();

    if obj_mut.is_none() {
        return Err(serde_json::Error::custom("Could not convert to object"));
    }

    let obj = obj_mut.unwrap();

    obj.insert("tx".to_string(), tx.to_string().into());

    Ok(WrappedQuery {
        query: json,
        query_id: query.query_id(),
        query_item_type: query.query_item_type(),
    })
}
