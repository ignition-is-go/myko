use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use wasm_bindgen::prelude::*;

use crate::utils::set_panic_hook;

#[wasm_bindgen]
extern "C" {
    // Use `js_namespace` here to bind `console.log(..)` instead of just
    // `log(..)`
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);

    // The `console.log` is quite polymorphic, so we can bind it with multiple
    // signatures. Note that we need to use `js_name` to ensure we always call
    // `log` in JS.
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log_u32(a: u32);

    // Multiple arguments too!
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log_many(a: &str, b: &str);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[serde(tag = "queryId", content = "query")]
pub enum Query {
    #[serde(rename = "watchId")]
    WatchId(WatchId),
    #[serde(rename = "watch")]
    Watch(Watch),
}

#[wasm_bindgen]
pub fn make_watch_id(tx: String, item_id: String, item_type: String) -> String {
    set_panic_hook();
    Query::WatchId(WatchId::new(tx, item_id, item_type))
        .to_string()
        .unwrap()
}

#[wasm_bindgen]
pub fn make_watch(tx: String, query: String, item_type: String) -> String {
    set_panic_hook();

    let q = serde_json::from_str(format!("{:?}", query).as_str()).unwrap();

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
