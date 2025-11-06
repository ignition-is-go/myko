use std::{any::Any, collections::HashMap, sync::Arc};

use log::{debug, error, warn};
use ractor::{Actor, ActorRef};

use crate::{
    actors::{
        query::{
            common::{ProcessUpdateData, StartQueryData},
            query_handler::{QueryHandler, QueryHandlerArgs, QueryHandlerMsg},
        },
        server::{MykoServerCtx, ServerMsg},
    },
    common::any_parser::MykoAnyParser,
    query::WrappedQuery,
    utils::assert_default_for_key,
};

pub struct QueryManager;

pub type QueryClosureType =
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
    // by query_id
    parsers: HashMap<Arc<str>, Arc<dyn MykoAnyParser>>,
}

pub struct RegisterQueryData {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub closure: QueryClosureType,
    pub parser: Arc<dyn MykoAnyParser>,
}

pub enum QueryManagerMsg {
    RegisterQuery(RegisterQueryData),
    ProcessUpdate(ProcessUpdateData, Arc<str>),
    StartQuery(WrappedQuery),
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
            parsers: HashMap::new(),
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
                if state.parsers.contains_key(&data.query_id) {
                    error!("Parser already registered for query ID {}", data.query_id);
                }

                state.parsers.insert(data.query_id.clone(), data.parser);

                match Actor::spawn(
                    None,
                    QueryHandler,
                    QueryHandlerArgs {
                        query_id: data.query_id.clone(),
                        closure: data.closure,
                        ctx: state.ctx.clone(),
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

                        item_handlers.insert(data.query_id.clone(), handler);
                    }
                    Err(err) => {
                        log::error!("Failed to spawn query handler: {}", err);
                    }
                };
                Ok(())
            }
            QueryManagerMsg::StartQuery(data) => {
                debug!("Starting query with ID {}", data.query_id);

                let handler = state
                    .handlers
                    .get(&data.query_item_type)
                    .and_then(|m| m.get(&data.query_id));

                let parser = state.parsers.get(&data.query_id);

                if parser.is_none() {
                    error!(
                        "No parser found for query ID {}: {:?}",
                        data.query_id,
                        state.parsers.keys()
                    );
                    return Ok(());
                }

                if handler.is_none() {
                    error!(
                        "No handler found for query ID {}: {:?}",
                        data.query_id, state.handlers
                    );

                    return Ok(());
                }

                let parser = parser.unwrap();
                let handler = handler.unwrap();

                let parsed_query = parser.parse(data.query);

                if let Err(err) = parsed_query {
                    error!("Failed to parse query: {}", err);
                    return Ok(());
                }

                let parsed_query = parsed_query.unwrap();

                if let Err(err) = handler.send_message(QueryHandlerMsg::StartQuery(parsed_query)) {
                    error!("Failed to start query: {}", err);
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
