use std::{any::Any, collections::HashMap, hash::Hash, sync::Arc};

use log::{debug, warn};
use ractor::{Actor, ActorRef};
use serde_json::Value;

use crate::{
    actors::{
        query::query_handler::{
            ProcessUpdateData, QueryHandler, QueryHandlerArgs, QueryHandlerMsg,
        },
        server::{MykoServerCtx, ServerMsg},
    },
    event::MEventType,
    item::{self, Eventable},
};

pub struct QueryManager;

pub type QueryClosure =
    Arc<dyn Fn(Arc<dyn Any>, Arc<MykoServerCtx>, Arc<dyn Any>) -> bool + Send + Sync>;

pub struct QueryManagerArgs {
    pub ctx: Arc<MykoServerCtx>,
    pub server: ActorRef<ServerMsg>,
}

pub struct QueryManagerState {
    ctx: Arc<MykoServerCtx>,
    server: ActorRef<ServerMsg>,
    // by query_item_type and then query_id
    handlers: HashMap<Arc<str>, HashMap<Arc<str>, ActorRef<QueryHandlerMsg>>>,
}

pub struct RegisterQueryData {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub closure: QueryClosure,
}

pub enum QueryManagerMsg {
    RegisterQuery(RegisterQueryData),
    ProcessUpdate(ProcessUpdateData, Arc<str>),
}

impl Actor for QueryManager {
    type Msg = QueryManagerMsg;
    type Arguments = QueryManagerArgs;
    type State = QueryManagerState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(QueryManagerState {
            ctx: args.ctx,
            server: args.server,
            handlers: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            QueryManagerMsg::RegisterQuery(data) => {
                match Actor::spawn(
                    None,
                    QueryHandler,
                    QueryHandlerArgs {
                        query_id: data.query_id,
                    },
                )
                .await
                {
                    Ok((handler, _handler_join_handle)) => {
                        assert_default_for_key(&mut state.handlers, &data.query_item_type);

                        let item_handlers = state
                            .handlers
                            .get_mut(&data.query_item_type)
                            .expect("Default Value asserted");

                        item_handlers.insert(data.query_item_type, handler);
                    }
                    Err(err) => {
                        log::error!("Failed to spawn query handler: {}", err);
                    }
                };
                Ok(())
            }
            QueryManagerMsg::ProcessUpdate(update_data, item_type) => {
                //
                let item_handlers = state.handlers.get(&item_type);

                match item_handlers {
                    Some(handlers) => {
                        for (key, handler) in handlers.iter() {
                            match handler
                                .send_message(QueryHandlerMsg::ProcessUpdate(update_data.clone()))
                            {
                                Ok(_) => (),
                                Err(err) => {
                                    log::error!(
                                        "Failed to process update for [{}: {}]: {}",
                                        key,
                                        item_type,
                                        err
                                    );
                                }
                            };
                        }
                    }
                    None => {
                        warn!("No Query handlers registered for item type {:?}", item_type);
                    }
                }

                Ok(())
            }
        }
    }
}

fn assert_default_for_key<K: Hash + Eq + Clone, V: Default>(hash_map: &mut HashMap<K, V>, key: &K) {
    match hash_map.contains_key(key) {
        true => (),
        false => {
            hash_map.insert(key.clone(), V::default());
        }
    }
}
