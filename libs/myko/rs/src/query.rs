use serde::{ser::Error, Deserialize, Serialize};
use serde_json::Value;

use crate::{client::MykoClient, item::WrappedItem};

pub trait MykoQuery {
    type Item;

    fn watch(&self, client: &MykoClient) -> impl tokio_stream::Stream<Item = Vec<Self::Item>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub deletes: Vec<String>,

    pub upserts: Vec<WrappedItem<Value>>,

    pub sequence: u64,

    pub tx: String,
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
    pub fn new(tx: String, _result: Vec<Value>) -> QueryResponse {
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
        self.tx.clone()
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    pub tx: String,
    pub message: String,
}

pub trait QueryId {
    fn query_id(&self) -> String;
}

pub trait QueryItemType {
    fn query_item_type(&self) -> String;
}

pub fn wrap_query<Q: QueryId + QueryItemType + Serialize + Clone>(
    tx: String,
    query: Q,
) -> Result<WrappedQuery, serde_json::Error> {
    let mut json = serde_json::to_value(query.clone())?;

    let obj_mut = json.as_object_mut();

    if obj_mut.is_none() {
        return Err(serde_json::Error::custom("Could not convert to object"));
    }

    let obj = obj_mut.unwrap();

    obj.insert("tx".to_string(), tx.into());

    Ok(WrappedQuery {
        query: json,
        query_id: query.query_id(),
        query_item_type: query.query_item_type(),
    })
}

pub trait QueryHandler<Q: MykoQuery> {
    fn handle_query(
        &self,
        query: Q,
        tx: String,
    ) -> impl tokio_stream::Stream<Item = QueryResult<Q::Item>>;
}
