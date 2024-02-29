use std::any::Any;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use wasm_bindgen::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchId {
    pub tx: String,
    pub item_id: String,
    pub item_type: String,
}

impl WatchId {
    pub fn new(tx: String, item_id: String, item_type: String) -> WatchId {
        WatchId {
            tx,
            item_id,
            item_type,
        }
    }

    pub fn item_type(&self) -> String {
        self.item_type.clone()
    }

    pub fn from_json(s: Value) -> Result<WatchId, serde_json::Error> {
        serde_json::from_value(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Watch {
    pub tx: String,
    pub query: String,
    pub item_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JMESPathQuery {
    query: String,

    item_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "queryId", content = "query")]
pub enum Query {
    #[serde(rename = "watchId")]
    WatchId(WatchId),
    #[serde(rename = "watch")]
    Watch(Watch),
}

#[wasm_bindgen]
pub fn make_watch_id(tx: String, item_id: String, item_type: String) -> String {
    Query::WatchId(WatchId::new(tx, item_id, item_type))
        .to_string()
        .unwrap()
}

#[wasm_bindgen]
pub fn make_watch(tx: String, query: String, item_type: String) -> String {
    let q = serde_json::from_str(query.as_str()).unwrap();

    println!("Query: {:?}", q);
    Query::Watch(Watch {
        tx,
        query: q,
        item_type,
    })
    .to_string()
    .unwrap()
}

impl Query {
    pub fn from_json(s: Value) -> Result<Query, serde_json::Error> {
        serde_json::from_value(s)
    }

    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_str(s: &str) -> Result<Query, serde_json::Error> {
        let s = remove_whitespace(s);
        serde_json::from_str(s.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[wasm_bindgen]
pub struct QueryResponse {
    #[serde(rename = "tx")]
    tx: String,

    #[serde(rename = "result")]
    result: Vec<Value>,
}

impl QueryResponse {
    pub fn new(tx: String, result: Vec<Value>) -> QueryResponse {
        QueryResponse { tx, result }
    }

    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[wasm_bindgen]
impl QueryResponse {
    #[wasm_bindgen(getter, js_name = "tx")]
    pub fn get_tx(&self) -> String {
        self.tx.clone()
    }

    #[wasm_bindgen(getter, js_name = "result")]
    pub fn get_item(&self) -> String {
        json!(self.result.clone()).to_string()
    }
}

pub fn remove_whitespace(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}
