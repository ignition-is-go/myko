use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{client::MykoClient, item::WrappedItem};

pub trait MykoQuery<T> {
    fn watch(&self, client: MykoClient) -> impl tokio_stream::Stream<Item = Vec<T>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub deletes: Vec<String>,

    pub upserts: Vec<WrappedItem<Value>>,

    pub sequence: u64,

    pub tx: String,
}

impl QueryResponse {
    pub fn new(tx: String, result: Vec<Value>) -> QueryResponse {
        QueryResponse {
            sequence: 0,
            upserts: vec![],
            deletes: vec![],
            tx,
        }
    }

    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

impl QueryResponse {
    pub fn get_tx(&self) -> String {
        "".to_string()
    }

    // #[wasm_bindgen(getter, js_name = "result")]
    // pub fn get_item(&self) -> String {
    //     json!(self.result.clone()).to_string()
    // }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WrappedQuery {
    pub query: Value,
    pub query_id: String,
    pub query_item_type: String,
}
