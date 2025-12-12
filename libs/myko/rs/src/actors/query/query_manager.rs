use std::{collections::HashMap, sync::Arc};

use futures_signals::signal_map::MutableSignalMap;
use log::{error, trace, warn};
use ractor::{Actor, ActorRef, RpcReplyPort};

use crate::{
    actors::{
        event::event_manager::EventManagerMsg,
        query::{
            common::ProcessUpdateData,
            query_handler::{QueryHandler, QueryHandlerArgs, QueryHandlerMsg},
        },
        server::ServerMsg,
    },
    api::query::WrappedQuery,
    parsers::query::MykoQueryParser,
    prelude::AnyItem,
    query::QueryHandlerCtxAny,
    server::MykoServerCtx,
    utils::assert_default_for_key,
};

pub struct QueryManager;

pub type QueryClosureType = Arc<dyn Fn(QueryHandlerCtxAny) -> bool + Send + Sync>;

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
    parsers: HashMap<Arc<str>, Arc<dyn MykoQueryParser>>,
    /// Set after EventManager is spawned (breaks circular dependency)
    event_manager: Option<ActorRef<EventManagerMsg>>,
}

pub struct RegisterQueryData {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub closure: QueryClosureType,
    pub parser: Arc<dyn MykoQueryParser>,
}

pub enum QueryManagerMsg {
    RegisterQuery(RegisterQueryData),
    ProcessUpdate(ProcessUpdateData, Arc<str>),
    /// Batch of updates for a single item type - more efficient for high throughput
    ProcessBatch(Vec<ProcessUpdateData>, Arc<str>),
    StartQuery(
        WrappedQuery,
        RpcReplyPort<MutableSignalMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ),
    /// One-shot query that returns current state without creating a subscription
    QuerySnapshot(
        WrappedQuery,
        RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ),
    /// Cancel an active query subscription by transaction ID
    CancelQuery(Arc<str>),
    /// Set EventManager reference (breaks circular dependency at startup)
    SetEventManager(ActorRef<EventManagerMsg>),
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
            event_manager: None, // Set later via SetEventManager
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            QueryManagerMsg::RegisterQuery(data) => {
                if state.parsers.contains_key(&data.query_id) {
                    error!("Parser already registered for query ID {}", data.query_id);
                }

                let event_manager = match &state.event_manager {
                    Some(em) => em.clone(),
                    None => {
                        error!("Cannot register query before EventManager is set");
                        return Ok(());
                    }
                };

                state.parsers.insert(data.query_id.clone(), data.parser);

                match Actor::spawn(
                    None,
                    QueryHandler,
                    QueryHandlerArgs {
                        query_id: data.query_id.clone(),
                        closure: data.closure,
                        ctx: state.ctx.clone(),
                        server: state.server.clone(),
                        event_manager,
                        query_item_type: data.query_item_type.clone(),
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
            QueryManagerMsg::StartQuery(data, reply) => {
                trace!("Starting query with ID {}", data.query_id);

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

                if let Err(err) =
                    handler.send_message(QueryHandlerMsg::StartQuery(parsed_query, reply))
                {
                    error!("Failed to start query: {}", err);
                };

                Ok(())
            }
            QueryManagerMsg::ProcessUpdate(update_data, item_type) => {
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
            QueryManagerMsg::ProcessBatch(batch, item_type) => {
                // Batch processing - send entire batch to each handler for efficient processing
                let item_handlers = state.handlers.get(&item_type);

                match item_handlers {
                    Some(handlers) => {
                        for (key, handler) in handlers.iter() {
                            match handler.send_message(QueryHandlerMsg::ProcessBatch(batch.clone()))
                            {
                                Ok(_) => (),
                                Err(err) => {
                                    log::error!(
                                        "Failed to process batch for [{}: {}]: {}",
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
            QueryManagerMsg::QuerySnapshot(data, reply) => {
                trace!("Query snapshot with ID {}", data.query_id);

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

                if let Err(err) =
                    handler.send_message(QueryHandlerMsg::QuerySnapshot(parsed_query, reply))
                {
                    error!("Failed to get query snapshot: {}", err);
                };

                Ok(())
            }
            QueryManagerMsg::CancelQuery(tx) => {
                trace!("Cancelling query with tx {}", tx);
                // Broadcast cancel to all handlers - they check if they own this tx
                for handlers in state.handlers.values() {
                    for handler in handlers.values() {
                        if let Err(err) = handler.send_message(QueryHandlerMsg::CancelQuery(tx.clone())) {
                            error!("Failed to send cancel to handler: {}", err);
                        }
                    }
                }
                Ok(())
            }
            QueryManagerMsg::SetEventManager(event_manager) => {
                log::info!("QueryManager: EventManager reference set");
                state.event_manager = Some(event_manager);
                Ok(())
            }
        }
    }
}
