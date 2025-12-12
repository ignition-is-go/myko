use std::{collections::HashMap, sync::Arc};

use futures_signals::signal_map::MutableSignalMap;
use log::{error, trace, warn};
use ractor::{Actor, ActorRef, RpcReplyPort};
use tokio::sync::mpsc;

use crate::{
    actors::{
        event::event_manager::EventManagerMsg,
        query::{
            common::{ProcessUpdateData, QueryStreamUpdate},
            query_handler::{QueryHandler, QueryHandlerArgs, QueryHandlerMsg},
        },
        server::ServerMsg,
    },
    api::query::WrappedQuery,
    parsers::query::{AnyQuery, MykoQueryParser},
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
    /// One-shot query using a typed query object (no serialization).
    /// For internal actor use - skips JSON parsing overhead.
    QuerySnapshot(
        Arc<dyn AnyQuery>,
        RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ),
    /// Watch a query using a typed query object (no serialization).
    /// For internal actor use - skips JSON parsing overhead.
    WatchQuery(
        Arc<dyn AnyQuery>,
        RpcReplyPort<mpsc::UnboundedReceiver<QueryStreamUpdate>>,
    ),
    /// One-shot query from a WrappedQuery (with JSON parsing).
    /// For external/WebSocket use.
    WrappedQuerySnapshot(
        WrappedQuery,
        RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ),
    /// Watch a query from a WrappedQuery (with JSON parsing).
    /// For external/WebSocket use.
    WatchWrappedQuery(
        WrappedQuery,
        RpcReplyPort<mpsc::UnboundedReceiver<QueryStreamUpdate>>,
    ),
    /// Parse a WrappedQuery into a typed Arc<dyn AnyQuery>.
    /// Useful when you receive a WrappedQuery but need the typed version.
    ParseQuery(WrappedQuery, RpcReplyPort<Option<Arc<dyn AnyQuery>>>),
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
            QueryManagerMsg::QuerySnapshot(query, reply) => {
                Self::handle_query_snapshot(state, query, reply);
                Ok(())
            }
            QueryManagerMsg::WatchQuery(query, reply) => {
                Self::handle_watch_query(state, query, reply);
                Ok(())
            }
            QueryManagerMsg::WrappedQuerySnapshot(data, reply) => {
                // Parse and delegate to QuerySnapshot
                trace!("Wrapped query snapshot with ID {}", data.query_id);

                let parsed = Self::parse_query(state, &data);
                match parsed {
                    Some(query) => {
                        Self::handle_query_snapshot(state, query, reply);
                    }
                    None => {
                        error!("Failed to parse query {}", data.query_id);
                    }
                }

                Ok(())
            }
            QueryManagerMsg::WatchWrappedQuery(data, reply) => {
                // Parse and delegate to WatchQuery
                trace!("Wrapped watch query with ID {}", data.query_id);

                let parsed = Self::parse_query(state, &data);
                match parsed {
                    Some(query) => {
                        Self::handle_watch_query(state, query, reply);
                    }
                    None => {
                        error!("Failed to parse query {}", data.query_id);
                    }
                }

                Ok(())
            }
            QueryManagerMsg::ParseQuery(data, reply) => {
                // Parse a WrappedQuery into Arc<dyn AnyQuery>
                let parser = state.parsers.get(&data.query_id);

                match parser {
                    Some(parser) => {
                        match parser.parse(data.query) {
                            Ok(parsed) => {
                                let _ = reply.send(Some(parsed));
                            }
                            Err(err) => {
                                error!("Failed to parse query {}: {}", data.query_id, err);
                                let _ = reply.send(None);
                            }
                        }
                    }
                    None => {
                        error!(
                            "No parser found for query ID {}: {:?}",
                            data.query_id,
                            state.parsers.keys()
                        );
                        let _ = reply.send(None);
                    }
                }

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
                state.event_manager = Some(event_manager);
                Ok(())
            }
        }
    }
}

impl QueryManager {
    /// Parse a WrappedQuery into Arc<dyn AnyQuery> using the registered parser
    fn parse_query(state: &QueryManagerState, data: &WrappedQuery) -> Option<Arc<dyn AnyQuery>> {
        let parser = state.parsers.get(&data.query_id)?;
        match parser.parse(data.query.clone()) {
            Ok(parsed) => Some(parsed),
            Err(err) => {
                error!("Failed to parse query {}: {}", data.query_id, err);
                None
            }
        }
    }

    /// Handle a query snapshot request
    fn handle_query_snapshot(
        state: &QueryManagerState,
        query: Arc<dyn AnyQuery>,
        reply: RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ) {
        let query_id = query.query_id();
        let query_item_type = query.query_item_type();
        trace!("Query snapshot with ID {}", query_id);

        let handler = state
            .handlers
            .get(&query_item_type)
            .and_then(|m| m.get(&query_id));

        if handler.is_none() {
            error!(
                "No handler found for query ID {}: {:?}",
                query_id, state.handlers
            );
            return;
        }

        let handler = handler.unwrap();

        if let Err(err) = handler.send_message(QueryHandlerMsg::QuerySnapshot(query, reply)) {
            error!("Failed to get query snapshot: {}", err);
        };
    }

    /// Handle a watch query request
    fn handle_watch_query(
        state: &QueryManagerState,
        query: Arc<dyn AnyQuery>,
        reply: RpcReplyPort<mpsc::UnboundedReceiver<QueryStreamUpdate>>,
    ) {
        let query_id = query.query_id();
        let query_item_type = query.query_item_type();
        trace!("Watch query with ID {}", query_id);

        let handler = state
            .handlers
            .get(&query_item_type)
            .and_then(|m| m.get(&query_id));

        if handler.is_none() {
            error!(
                "No handler found for query ID {}: {:?}",
                query_id, state.handlers
            );
            return;
        }

        let handler = handler.unwrap();

        if let Err(err) = handler.send_message(QueryHandlerMsg::WatchQuery(query, reply)) {
            error!("Failed to start watch query: {}", err);
        };
    }
}
