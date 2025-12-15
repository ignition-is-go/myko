//! Query handler for a specific query type.
//!
//! Each QueryHandler manages subscriptions for a single query type.
//! Supports both channel-based subscriptions (internal) and direct WebSocket forwarding (external).

use crate::{
    actors::{
        event::{event_handler::EventHandlerMessage, event_manager::EventManagerMsg},
        query::{
            common::{ChannelSink, ProcessUpdateData, QueryResultSink, QueryStreamUpdate},
            query_manager::QueryClosureType,
        },
        server::ServerMsg,
    },
    parsers::query::AnyQuery,
    prelude::AnyItem,
    query::QueryHandlerCtxAny,
    runtime::{Actor, ActorHandle, ActorRef, RpcReplyPort},
    server::MykoServerCtx,
};
use crossbeam::channel as crossbeam_channel;
use log::{debug, error, trace};
use std::{collections::HashMap, sync::Arc};

pub struct QueryHandlerArgs {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub closure: QueryClosureType,
    pub ctx: Arc<MykoServerCtx>,
    pub server: ActorRef<ServerMsg>,
    pub event_manager: ActorRef<EventManagerMsg>,
}

/// Tracks a streaming query subscription with a generic sink
struct WatchSubscription {
    sink: Box<dyn QueryResultSink>,
    closure: QueryClosureType,
    ctx: Arc<MykoServerCtx>,
    query: Arc<dyn AnyQuery>,
}

pub struct QueryHandler {
    query_item_type: Arc<str>,
    closure: QueryClosureType,
    /// Active streaming query subscriptions (by tx id)
    watchers: HashMap<Arc<str>, WatchSubscription>,
    ctx: Arc<MykoServerCtx>,
    event_manager: ActorRef<EventManagerMsg>,
}

pub enum QueryHandlerMsg {
    ProcessUpdate(ProcessUpdateData),
    /// Batch of updates - forwarded to watchers for efficient processing
    ProcessBatch(Vec<ProcessUpdateData>),
    /// One-shot query that returns current state without creating a subscription
    QuerySnapshot(
        Arc<dyn AnyQuery>,
        RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ),
    /// Watch a query and receive updates via a crossbeam channel.
    /// Returns a receiver that emits QueryStreamUpdate messages.
    /// Used for internal actor-to-actor subscriptions.
    WatchQuery(
        Arc<dyn AnyQuery>,
        RpcReplyPort<crossbeam_channel::Receiver<QueryStreamUpdate>>,
    ),
    /// Watch a query with a custom sink for results.
    /// Used for direct WebSocket forwarding without intermediate channels.
    WatchQueryWithSink(Arc<dyn AnyQuery>, Box<dyn QueryResultSink>),
    /// Cancel a query subscription by transaction ID
    CancelQuery(Arc<str>),
}

impl std::fmt::Debug for QueryHandlerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryHandlerMsg::ProcessUpdate(_) => write!(f, "ProcessUpdate"),
            QueryHandlerMsg::ProcessBatch(updates) => {
                write!(f, "ProcessBatch({} updates)", updates.len())
            }
            QueryHandlerMsg::QuerySnapshot(_, _) => write!(f, "QuerySnapshot"),
            QueryHandlerMsg::WatchQuery(_, _) => write!(f, "WatchQuery"),
            QueryHandlerMsg::WatchQueryWithSink(_, _) => write!(f, "WatchQueryWithSink"),
            QueryHandlerMsg::CancelQuery(tx) => write!(f, "CancelQuery({})", tx),
        }
    }
}

impl QueryHandler {
    /// Create a new QueryHandler with the given arguments.
    pub fn new(args: QueryHandlerArgs) -> Self {
        trace!("Creating Handler for query {}", args.query_id);

        Self {
            closure: args.closure,
            watchers: HashMap::new(),
            ctx: args.ctx,
            event_manager: args.event_manager,
            query_item_type: args.query_item_type,
        }
    }

    /// Spawn the QueryHandler on a dedicated thread.
    pub fn spawn(args: QueryHandlerArgs) -> ActorHandle<QueryHandlerMsg> {
        let actor = Self::new(args);
        crate::runtime::spawn::spawn(actor)
    }

    fn handle_process_update(&mut self, data: ProcessUpdateData) {
        self.notify_watchers(&data);
    }

    fn handle_process_batch(&mut self, batch: Vec<ProcessUpdateData>) {
        for data in &batch {
            self.notify_watchers(data);
        }
    }

    fn handle_query_snapshot(
        &self,
        query: Arc<dyn AnyQuery>,
        reply: RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ) {
        // One-shot query: get current state, filter, and return immediately
        let handler = match self.event_manager.call(|r| {
            EventManagerMsg::GetEventHandler(self.query_item_type.clone(), r)
        }) {
            Ok(h) => h,
            Err(err) => {
                error!("Failed to get event handler: {}", err);
                return;
            }
        };

        let mut snapshot = match handler.call(EventHandlerMessage::GetState) {
            Ok(s) => s,
            Err(err) => {
                error!("Failed to get state: {}", err);
                return;
            }
        };

        snapshot.retain(|_k, v| {
            self.closure.clone()(QueryHandlerCtxAny {
                ctx: self.ctx.clone(),
                item: v.clone(),
                query: query.clone(),
            })
        });

        let _ = reply.send(snapshot);
    }

    fn handle_watch_query(
        &mut self,
        query: Arc<dyn AnyQuery>,
        reply: RpcReplyPort<crossbeam_channel::Receiver<QueryStreamUpdate>>,
    ) {
        // Create bounded channel for updates with backpressure
        // 256 items should handle normal update bursts while preventing memory exhaustion
        let (sender, receiver) = crossbeam_channel::bounded(256);
        let sink = Box::new(ChannelSink::new(sender));

        // Delegate to the generic sink handler
        if self.subscribe_with_sink(query, sink) {
            let _ = reply.send(receiver);
        }
    }

    fn handle_watch_query_with_sink(
        &mut self,
        query: Arc<dyn AnyQuery>,
        sink: Box<dyn QueryResultSink>,
    ) {
        self.subscribe_with_sink(query, sink);
    }

    /// Internal method to subscribe with any sink implementation.
    /// Returns true if subscription was created successfully.
    fn subscribe_with_sink(
        &mut self,
        query: Arc<dyn AnyQuery>,
        mut sink: Box<dyn QueryResultSink>,
    ) -> bool {
        // Get initial state
        let handler = match self.event_manager.call(|r| {
            EventManagerMsg::GetEventHandler(self.query_item_type.clone(), r)
        }) {
            Ok(h) => h,
            Err(err) => {
                error!("Failed to get event handler: {}", err);
                return false;
            }
        };

        let mut initial_state = match handler.call(EventHandlerMessage::GetState) {
            Ok(s) => s,
            Err(err) => {
                error!("Failed to get initial state: {}", err);
                return false;
            }
        };

        initial_state.retain(|_k, v| {
            self.closure.clone()(QueryHandlerCtxAny {
                ctx: self.ctx.clone(),
                item: v.clone(),
                query: query.clone(),
            })
        });

        let tx: Arc<str> = query.tx_id();

        // Send initial state through the sink
        if !sink.push(QueryStreamUpdate::Initial(initial_state)) {
            error!("Failed to send initial state to watcher");
            return false;
        }

        // Store the watcher
        self.watchers.insert(
            tx.clone(),
            WatchSubscription {
                sink,
                closure: self.closure.clone(),
                ctx: self.ctx.clone(),
                query,
            },
        );

        debug!("Created watch subscription with tx {}", tx);
        true
    }

    fn handle_cancel_query(&mut self, tx: Arc<str>) {
        if self.watchers.remove(&tx).is_some() {
            trace!("Cancelled watch subscription for tx {}", tx);
        }
    }

    /// Notify all streaming watchers about an update.
    /// Checks if each watcher's query matches the update before sending.
    fn notify_watchers(&mut self, data: &ProcessUpdateData) {
        // Collect dead watchers to remove
        let mut dead_watchers = Vec::new();

        for (tx, watcher) in self.watchers.iter_mut() {
            let update = match data {
                ProcessUpdateData::Del(id) => {
                    // Always send deletes - watcher will ignore if not tracking this ID
                    Some(QueryStreamUpdate::Remove(id.clone()))
                }
                ProcessUpdateData::Set(item) => {
                    // Check if item matches query
                    let matches = (watcher.closure)(QueryHandlerCtxAny {
                        ctx: watcher.ctx.clone(),
                        item: item.clone(),
                        query: watcher.query.clone(),
                    });

                    if matches {
                        Some(QueryStreamUpdate::Upsert(item.id(), item.clone()))
                    } else {
                        // Item doesn't match - send remove in case it was previously matching
                        Some(QueryStreamUpdate::Remove(item.id()))
                    }
                }
            };

            if let Some(update) = update
                && !watcher.sink.push(update)
            {
                // Sink closed - mark for removal
                dead_watchers.push(tx.clone());
            }
        }

        // Remove dead watchers
        for tx in dead_watchers {
            debug!("Removing dead watcher with tx {}", tx);
            self.watchers.remove(&tx);
        }
    }
}

impl Actor for QueryHandler {
    type Msg = QueryHandlerMsg;

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            QueryHandlerMsg::ProcessUpdate(data) => {
                self.handle_process_update(data);
            }
            QueryHandlerMsg::ProcessBatch(batch) => {
                self.handle_process_batch(batch);
            }
            QueryHandlerMsg::QuerySnapshot(query, reply) => {
                self.handle_query_snapshot(query, reply);
            }
            QueryHandlerMsg::WatchQuery(query, reply) => {
                self.handle_watch_query(query, reply);
            }
            QueryHandlerMsg::WatchQueryWithSink(query, sink) => {
                self.handle_watch_query_with_sink(query, sink);
            }
            QueryHandlerMsg::CancelQuery(tx) => {
                self.handle_cancel_query(tx);
            }
        }
    }
}
