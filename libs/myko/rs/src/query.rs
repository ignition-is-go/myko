use chrono::ParseResult;
use log::{debug, error};
use serde::{Deserialize, Serialize, de::DeserializeOwned, ser::Error};
use serde_json::Value;
use std::{any::Any, sync::Arc};

use crate::{
    actors::{
        query::query_manager::{QueryManagerMsg, RegisterQueryData},
        server::MykoServerCtx,
    },
    client::MykoClient,
    common::any_parser::{CapturedTypeParser, MykoAnyParser},
    item::{Eventable, WrappedItem},
    server::MykoServer,
};

pub trait MykoQuery:
    Serialize
    + DeserializeOwned
    + Send
    + Sync
    + QueryId
    + QueryItemType
    + QueryClosure<<Self as MykoQuery>::Item>
{
    type Item: Eventable;

    fn watch(&self, client: &MykoClient) -> impl tokio_stream::Stream<Item = Vec<Self::Item>>;

    fn register(server: &Arc<MykoServer>) -> Result<(), anyhow::Error> {
        let closure = Arc::new(
            |item: Arc<dyn Any>, ctx: Arc<MykoServerCtx>, query: Arc<dyn Any>| -> bool {
                let item = item.downcast_ref::<Self::Item>();
                let query = query.downcast_ref::<Arc<Self>>();

                if let (Some(item), Some(query)) = (item, query) {
                    <Self as QueryClosure<Self::Item>>::test_entity(item, ctx, query.clone())
                } else {
                    false
                }
            },
        );

        let parser = Arc::new(CapturedTypeParser::<Self>::new());

        match server
            .server
            .send_message(crate::actors::server::ServerMsg::QueryManagerMsg(
                QueryManagerMsg::RegisterQuery(RegisterQueryData {
                    query_id: Self::query_id_static(),
                    query_item_type: Self::query_item_type_static(),
                    closure,
                    parser,
                }),
            )) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to register query: {}", err);
            }
        };
        Ok(())
    }
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WrappedQuery {
    pub query: Value,
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    pub tx: String,
    pub message: String,
}

pub trait QueryId: Any {
    fn query_id(&self) -> Arc<str>;
    fn query_id_static() -> Arc<str>;
}

pub trait QueryItemType: Any {
    fn query_item_type(&self) -> Arc<str>;
    fn query_item_type_static() -> Arc<str>;
}

pub fn wrap_query<Q: QueryId + QueryItemType + Serialize + Clone>(
    tx: String,
    query: &Q,
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

pub trait QueryClosure<T: Eventable> {
    fn test_entity(item: &T, ctx: Arc<MykoServerCtx>, query: Arc<Self>) -> bool;
}
