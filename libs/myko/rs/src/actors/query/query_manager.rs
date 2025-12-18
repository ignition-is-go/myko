//! Query manager for reactive query execution.
//!
//! The [`QueryManager`] coordinates query execution across all entity types,
//! providing reactive query results that update automatically as data changes.

use std::{collections::HashMap, sync::Arc};

use crossbeam::channel as crossbeam_channel;
use log::{error, trace, warn};

use crate::{
    actors::{
        event::event_manager::EventManagerMsg,
        query::{
            common::{ProcessUpdateData, QueryResultSink, QueryStreamUpdate},
            query_handler::{QueryHandler, QueryHandlerArgs, QueryHandlerMsg},
        },
        server::ServerMsg,
    },
    api::query::WrappedQuery,
    parsers::query::{AnyQuery, MykoQueryParser},
    prelude::AnyItem,
    query::QueryHandlerCtxAny,
    runtime::{Actor, ActorRef, RpcReplyPort},
    server::MykoServerCtx,
    utils::assert_default_for_key,
};

pub type QueryClosureType = Arc<dyn Fn(QueryHandlerCtxAny) -> bool + Send + Sync>;

pub struct QueryManagerArgs {
    pub ctx: Arc<MykoServerCtx>,
    pub server: ActorRef<ServerMsg>,
}

pub struct QueryManager {
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
        RpcReplyPort<crossbeam_channel::Receiver<QueryStreamUpdate>>,
    ),
    /// One-shot query from a WrappedQuery (with JSON parsing).
    /// For external/WebSocket use.
    WrappedQuerySnapshot(
        WrappedQuery,
        RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ),
    /// Watch a query from a WrappedQuery (with JSON parsing).
    /// For external/WebSocket use - returns a channel.
    WatchWrappedQuery(
        WrappedQuery,
        RpcReplyPort<crossbeam_channel::Receiver<QueryStreamUpdate>>,
    ),
    /// Watch a query with a custom sink for direct result forwarding.
    /// For WebSocket use - no intermediate channel needed.
    WatchWrappedQueryWithSink(WrappedQuery, Box<dyn QueryResultSink>),
    /// Parse a WrappedQuery into a typed Arc<dyn AnyQuery>.
    /// Useful when you receive a WrappedQuery but need the typed version.
    ParseQuery(WrappedQuery, RpcReplyPort<Option<Arc<dyn AnyQuery>>>),
    /// Cancel an active query subscription by transaction ID
    CancelQuery(Arc<str>),
    /// Set EventManager reference (breaks circular dependency at startup)
    SetEventManager(ActorRef<EventManagerMsg>),
}

impl std::fmt::Debug for QueryManagerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryManagerMsg::RegisterQuery(data) => {
                write!(f, "RegisterQuery({})", data.query_id)
            }
            QueryManagerMsg::ProcessUpdate(_, item_type) => {
                write!(f, "ProcessUpdate({})", item_type)
            }
            QueryManagerMsg::ProcessBatch(updates, item_type) => {
                write!(f, "ProcessBatch({} updates, {})", updates.len(), item_type)
            }
            QueryManagerMsg::QuerySnapshot(_, _) => write!(f, "QuerySnapshot"),
            QueryManagerMsg::WatchQuery(_, _) => write!(f, "WatchQuery"),
            QueryManagerMsg::WrappedQuerySnapshot(q, _) => {
                write!(f, "WrappedQuerySnapshot({})", q.query_id)
            }
            QueryManagerMsg::WatchWrappedQuery(q, _) => {
                write!(f, "WatchWrappedQuery({})", q.query_id)
            }
            QueryManagerMsg::WatchWrappedQueryWithSink(q, _) => {
                write!(f, "WatchWrappedQueryWithSink({})", q.query_id)
            }
            QueryManagerMsg::ParseQuery(q, _) => {
                write!(f, "ParseQuery({})", q.query_id)
            }
            QueryManagerMsg::CancelQuery(tx) => write!(f, "CancelQuery({})", tx),
            QueryManagerMsg::SetEventManager(_) => write!(f, "SetEventManager"),
        }
    }
}

impl QueryManager {

    fn handle_register_query(&mut self, data: RegisterQueryData) {
        if self.parsers.contains_key(&data.query_id) {
            trace!("Query {} already registered, skipping", data.query_id);
            return;
        }

        let event_manager = match &self.event_manager {
            Some(em) => em.clone(),
            None => {
                error!("Cannot register query before EventManager is set");
                return;
            }
        };

        self.parsers.insert(data.query_id.clone(), data.parser);

        let handle = QueryHandler::spawn(QueryHandlerArgs {
            query_id: data.query_id.clone(),
            closure: data.closure,
            ctx: self.ctx.clone(),
            server: self.server.clone(),
            event_manager,
            query_item_type: data.query_item_type.clone(),
        });

        assert_default_for_key(&mut self.handlers, &data.query_item_type);

        let item_handlers = self
            .handlers
            .get_mut(&data.query_item_type)
            .expect("Default Value asserted");

        item_handlers.insert(data.query_id.clone(), handle.actor_ref());
    }

    fn handle_process_update(&self, update_data: ProcessUpdateData, item_type: Arc<str>) {
        let item_handlers = self.handlers.get(&item_type);

        match item_handlers {
            Some(handlers) => {
                for (key, handler) in handlers.iter() {
                    match handler.send_message(QueryHandlerMsg::ProcessUpdate(update_data.clone()))
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
    }

    fn handle_process_batch(&self, batch: Vec<ProcessUpdateData>, item_type: Arc<str>) {
        let item_handlers = self.handlers.get(&item_type);

        match item_handlers {
            Some(handlers) => {
                for (key, handler) in handlers.iter() {
                    match handler.send_message(QueryHandlerMsg::ProcessBatch(batch.clone())) {
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
    }

    /// Parse a WrappedQuery into Arc<dyn AnyQuery> using the registered parser
    fn parse_query(&self, data: &WrappedQuery) -> Result<Arc<dyn AnyQuery>, String> {
        let parser = self.parsers.get(&data.query_id).ok_or_else(|| {
            format!("No parser registered for query {}", data.query_id)
        })?;
        parser.parse(data.query.clone()).map_err(|err| {
            format!("Failed to parse query {}: {}", data.query_id, err)
        })
    }

    fn handle_query_snapshot(
        &self,
        query: Arc<dyn AnyQuery>,
        reply: RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ) {
        let query_id = query.query_id();
        let query_item_type = query.query_item_type();
        trace!("Query snapshot with ID {}", query_id);

        let handler = self
            .handlers
            .get(&query_item_type)
            .and_then(|m| m.get(&query_id));

        if handler.is_none() {
            error!(
                "No handler found for query ID {}: {:?}",
                query_id, self.handlers
            );
            return;
        }

        let handler = handler.unwrap();

        if let Err(err) = handler.send_message(QueryHandlerMsg::QuerySnapshot(query, reply)) {
            error!("Failed to get query snapshot: {}", err);
        };
    }

    fn handle_watch_query(
        &self,
        query: Arc<dyn AnyQuery>,
        reply: RpcReplyPort<crossbeam_channel::Receiver<QueryStreamUpdate>>,
    ) {
        let query_id = query.query_id();
        let query_item_type = query.query_item_type();
        trace!("Watch query with ID {}", query_id);

        let handler = self
            .handlers
            .get(&query_item_type)
            .and_then(|m| m.get(&query_id));

        if handler.is_none() {
            error!(
                "No handler found for query ID {}: {:?}",
                query_id, self.handlers
            );
            return;
        }

        let handler = handler.unwrap();

        if let Err(err) = handler.send_message(QueryHandlerMsg::WatchQuery(query, reply)) {
            error!("Failed to start watch query: {}", err);
        };
    }

    fn handle_wrapped_query_snapshot(
        &self,
        data: WrappedQuery,
        reply: RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ) {
        trace!("Wrapped query snapshot with ID {}", data.query_id);

        match self.parse_query(&data) {
            Ok(query) => {
                self.handle_query_snapshot(query, reply);
            }
            Err(err) => {
                error!("{}", err);
            }
        }
    }

    fn handle_watch_wrapped_query(
        &self,
        data: WrappedQuery,
        reply: RpcReplyPort<crossbeam_channel::Receiver<QueryStreamUpdate>>,
    ) {
        trace!("Wrapped watch query with ID {}", data.query_id);

        match self.parse_query(&data) {
            Ok(query) => {
                self.handle_watch_query(query, reply);
            }
            Err(err) => {
                error!("{}", err);
            }
        }
    }

    fn handle_watch_wrapped_query_with_sink(
        &self,
        data: WrappedQuery,
        mut sink: Box<dyn QueryResultSink>,
    ) {
        trace!("Wrapped watch query with sink for ID {}", data.query_id);

        match self.parse_query(&data) {
            Ok(query) => {
                let query_id = query.query_id();
                let query_item_type = query.query_item_type();

                let handler = self
                    .handlers
                    .get(&query_item_type)
                    .and_then(|m| m.get(&query_id));

                if handler.is_none() {
                    let err_msg = format!("No handler found for query {}", query_id);
                    error!("{}", err_msg);
                    sink.push(QueryStreamUpdate::Error(err_msg));
                    return;
                }

                let handler = handler.unwrap();

                if let Err(err) =
                    handler.send_message(QueryHandlerMsg::WatchQueryWithSink(query, sink))
                {
                    error!("Failed to start watch query with sink: {}", err);
                }
            }
            Err(err_msg) => {
                error!("{}", err_msg);
                sink.push(QueryStreamUpdate::Error(err_msg));
            }
        }
    }

    fn handle_parse_query(
        &self,
        data: WrappedQuery,
        reply: RpcReplyPort<Option<Arc<dyn AnyQuery>>>,
    ) {
        let parser = self.parsers.get(&data.query_id);

        match parser {
            Some(parser) => match parser.parse(data.query) {
                Ok(parsed) => {
                    let _ = reply.send(Some(parsed));
                }
                Err(err) => {
                    error!("Failed to parse query {}: {}", data.query_id, err);
                    let _ = reply.send(None);
                }
            },
            None => {
                error!(
                    "No parser found for query ID {}: {:?}",
                    data.query_id,
                    self.parsers.keys()
                );
                let _ = reply.send(None);
            }
        }
    }

    fn handle_cancel_query(&self, tx: Arc<str>) {
        trace!("Cancelling query with tx {}", tx);
        // Broadcast cancel to all handlers - they check if they own this tx
        for handlers in self.handlers.values() {
            for handler in handlers.values() {
                if let Err(err) = handler.send_message(QueryHandlerMsg::CancelQuery(tx.clone())) {
                    error!("Failed to send cancel to handler: {}", err);
                }
            }
        }
    }
}

impl Actor for QueryManager {
    type Msg = QueryManagerMsg;
    type Args = QueryManagerArgs;

    fn new(args: Self::Args, _myself: ActorRef<Self::Msg>) -> Self {
        Self {
            ctx: args.ctx,
            server: args.server,
            handlers: HashMap::new(),
            parsers: HashMap::new(),
            event_manager: None,
        }
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            QueryManagerMsg::RegisterQuery(data) => {
                self.handle_register_query(data);
            }
            QueryManagerMsg::ProcessUpdate(update_data, item_type) => {
                self.handle_process_update(update_data, item_type);
            }
            QueryManagerMsg::ProcessBatch(batch, item_type) => {
                self.handle_process_batch(batch, item_type);
            }
            QueryManagerMsg::QuerySnapshot(query, reply) => {
                self.handle_query_snapshot(query, reply);
            }
            QueryManagerMsg::WatchQuery(query, reply) => {
                self.handle_watch_query(query, reply);
            }
            QueryManagerMsg::WrappedQuerySnapshot(data, reply) => {
                self.handle_wrapped_query_snapshot(data, reply);
            }
            QueryManagerMsg::WatchWrappedQuery(data, reply) => {
                self.handle_watch_wrapped_query(data, reply);
            }
            QueryManagerMsg::WatchWrappedQueryWithSink(data, sink) => {
                self.handle_watch_wrapped_query_with_sink(data, sink);
            }
            QueryManagerMsg::ParseQuery(data, reply) => {
                self.handle_parse_query(data, reply);
            }
            QueryManagerMsg::CancelQuery(tx) => {
                self.handle_cancel_query(tx);
            }
            QueryManagerMsg::SetEventManager(event_manager) => {
                use crate::query::QueryRegistration;

                self.event_manager = Some(event_manager);

                // Register all queries from inventory now that EventManager is available
                let mut count = 0;
                for registration in inventory::iter::<QueryRegistration> {
                    let data = (registration.factory)();
                    self.handle_register_query(data);
                    count += 1;
                }
                log::debug!("QueryManager: registered {} queries from inventory", count);
            }
        }
    }
}
