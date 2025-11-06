use log::debug;
use ractor::Actor;
use serde_json::Value;
use std::{any::Any, marker::PhantomData, sync::Arc};

use crate::{
    entities::server::Server,
    event::MEventType,
    item::Eventable,
    query::{self, MykoQuery},
};

pub struct QueryHandler;

pub struct QueryHandlerArgs {
    pub query_id: Arc<str>,
}

pub struct QueryHandlerState {
    pub query_id: Arc<str>,
}

#[derive(Debug, Clone)]
pub enum ProcessUpdateData {
    Set(Arc<dyn Any + Send + Sync>),
    Del(Arc<str>),
}

pub enum QueryHandlerMsg {
    ProcessUpdate(ProcessUpdateData),
}

impl QueryHandler {
    pub fn new(args: QueryHandlerArgs) -> Self {
        QueryHandler
    }
}

impl Actor for QueryHandler {
    type Arguments = QueryHandlerArgs;

    type State = QueryHandlerState;

    type Msg = QueryHandlerMsg;

    async fn pre_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("Creating Handler for query {}", args.query_id);

        Ok(QueryHandlerState {
            query_id: args.query_id,
        })
    }
}

trait QueryProcessor {
    fn process_query(&self, query_json: Value) -> Result<(), anyhow::Error>;
}

struct MykoQueryProcessor<T> {
    phantom: PhantomData<T>,
}

impl<T: MykoQuery> QueryProcessor for MykoQueryProcessor<T> {
    fn process_query(&self, query_json: Value) -> Result<(), anyhow::Error> {
        let query: T = serde_json::from_value(query_json)?;

        Ok(())
    }
}
