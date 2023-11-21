use serde::{Deserialize, Serialize};
use serde_json::Value;
use wasm_bindgen::prelude::*;

use crate::utils::remove_whitespace;

#[wasm_bindgen]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MQuery {
    #[serde(rename = "queryId")]
    query_id: String,
    #[serde(rename = "queryItemType")]
    query_item_type: String,

    query: Value,
}

impl MQuery {
    pub fn from_str(s: &str) -> Result<MQuery, serde_json::Error> {
        serde_json::from_str(&remove_whitespace(s))
    }
    pub fn query_json(&self) -> Value {
        self.query.clone()
    }
}

#[wasm_bindgen]
impl MQuery {
    #[wasm_bindgen(getter, js_name = "queryId")]
    pub fn query_id(&self) -> String {
        self.query_id.clone()
    }

    #[wasm_bindgen(getter, js_name = "queryItemType")]
    pub fn query_item_type(&self) -> String {
        self.query_item_type.clone()
    }
}

#[wasm_bindgen]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchId {
    tx: String,
    item_id: String,
}

#[wasm_bindgen]
impl WatchId {
    #[wasm_bindgen(constructor)]
    pub fn new(tx: String, item_id: String) -> WatchId {
        WatchId { tx, item_id }
    }

    #[wasm_bindgen(getter)]
    pub fn tx(&self) -> String {
        self.tx.clone()
    }

    #[wasm_bindgen(getter, js_name = "itemId")]
    pub fn item_id(&self) -> String {
        self.item_id.clone()
    }

    #[wasm_bindgen(getter, js_name = "queryJson")]
    pub fn query_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

impl WatchId {
    pub fn from_json(s: Value) -> Result<WatchId, serde_json::Error> {
        serde_json::from_value(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllQueries {
    WatchId(WatchId),
}

impl AllQueries {
    pub fn from_json(s: Value) -> Result<AllQueries, serde_json::Error> {
        serde_json::from_value(s)
    }
}
