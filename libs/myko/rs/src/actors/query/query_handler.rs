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
    runtime::{Actor, ActorRef, RpcReplyPort},
    server::MykoServerCtx,
};
use log::{debug, error, trace};
use std::{
    collections::HashMap,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

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
    /// Lineage string for debugging (e.g., "client → GetTargetById → GetTargetsByIds")
    lineage: Option<Arc<str>>,
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
    /// Watch a query and receive updates via a flume channel.
    /// Returns a receiver that emits QueryStreamUpdate messages.
    /// Used for internal actor-to-actor subscriptions.
    /// The optional lineage string is for debugging subscription chains.
    WatchQuery(
        Arc<dyn AnyQuery>,
        RpcReplyPort<flume::Receiver<QueryStreamUpdate>>,
        Option<Arc<str>>, // lineage
    ),
    /// Watch a query with a custom sink for results.
    /// Used for direct WebSocket forwarding without intermediate channels.
    /// The optional lineage string is for debugging subscription chains.
    WatchQueryWithSink(Arc<dyn AnyQuery>, Box<dyn QueryResultSink>, Option<Arc<str>>),
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
            QueryHandlerMsg::WatchQuery(_, _, lineage) => {
                write!(f, "WatchQuery(lineage={:?})", lineage)
            }
            QueryHandlerMsg::WatchQueryWithSink(_, _, lineage) => {
                write!(f, "WatchQueryWithSink(lineage={:?})", lineage)
            }
            QueryHandlerMsg::CancelQuery(tx) => write!(f, "CancelQuery({})", tx),
        }
    }
}

impl QueryHandler {
    /// Create a new QueryHandler with the given arguments.
    fn create(args: QueryHandlerArgs) -> Self {
        trace!("Creating Handler for query {}", args.query_id);

        Self {
            closure: args.closure,
            watchers: HashMap::new(),
            ctx: args.ctx,
            event_manager: args.event_manager,
            query_item_type: args.query_item_type,
        }
    }

    /// Safely call the query closure with panic catching.
    /// Returns Ok(matches) if successful, Err(panic_message) if the closure panicked.
    fn call_closure_safe(
        closure: &QueryClosureType,
        ctx: &Arc<MykoServerCtx>,
        item: &Arc<dyn AnyItem>,
        query: &Arc<dyn AnyQuery>,
    ) -> Result<bool, String> {
        let closure = closure.clone();
        let ctx = ctx.clone();
        let item = item.clone();
        let query = query.clone();

        let result = catch_unwind(AssertUnwindSafe(move || {
            closure(QueryHandlerCtxAny { ctx, item, query })
        }));

        match result {
            Ok(matches) => Ok(matches),
            Err(panic_payload) => {
                let panic_msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                Err(panic_msg)
            }
        }
    }

    fn handle_process_update(&mut self, data: ProcessUpdateData) {
        let id = match &data {
            ProcessUpdateData::Set(item) => item.id(),
            ProcessUpdateData::Del(id) => id.clone(),
        };
        log::debug!(
            "[QueryHandler {}] ProcessUpdate id={}, watchers={}",
            self.query_item_type,
            id,
            self.watchers.len()
        );
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
        let start = std::time::Instant::now();
        let handler = match self.event_manager.call(|r| {
            EventManagerMsg::GetEventHandler(self.query_item_type.clone(), r)
        }) {
            Ok(h) => h,
            Err(err) => {
                error!("Failed to get event handler: {}", err);
                return;
            }
        };
        let get_handler_time = start.elapsed();

        let mut snapshot = match handler.call(EventHandlerMessage::GetState) {
            Ok(s) => s,
            Err(err) => {
                error!("Failed to get state: {}", err);
                return;
            }
        };
        let get_state_time = start.elapsed();

        // Filter with panic catching
        let closure = &self.closure;
        let ctx = &self.ctx;
        snapshot.retain(|_k, v| {
            match Self::call_closure_safe(closure, ctx, v, &query) {
                Ok(matches) => matches,
                Err(panic_msg) => {
                    error!("Query closure panicked during snapshot: {}", panic_msg);
                    false // Exclude item on panic
                }
            }
        });

        let total_time = start.elapsed();
        if total_time.as_millis() > 50 {
            debug!(
                "Slow query snapshot [{}]: get_handler={:?}, get_state={:?}, total={:?}",
                query.query_id(),
                get_handler_time,
                get_state_time - get_handler_time,
                total_time
            );
        }

        let _ = reply.send(snapshot);
    }

    fn handle_watch_query(
        &mut self,
        query: Arc<dyn AnyQuery>,
        reply: RpcReplyPort<flume::Receiver<QueryStreamUpdate>>,
        lineage: Option<Arc<str>>,
    ) {
        // Create bounded channel for updates with backpressure
        // 256 items should handle normal update bursts while preventing memory exhaustion
        let (sender, receiver) = flume::bounded(256);
        let sink = Box::new(ChannelSink::new(sender));

        // Delegate to the generic sink handler
        if self.subscribe_with_sink(query, sink, lineage) {
            let _ = reply.send(receiver);
        }
    }

    fn handle_watch_query_with_sink(
        &mut self,
        query: Arc<dyn AnyQuery>,
        sink: Box<dyn QueryResultSink>,
        lineage: Option<Arc<str>>,
    ) {
        self.subscribe_with_sink(query, sink, lineage);
    }

    /// Internal method to subscribe with any sink implementation.
    /// Returns true if subscription was created successfully.
    fn subscribe_with_sink(
        &mut self,
        query: Arc<dyn AnyQuery>,
        mut sink: Box<dyn QueryResultSink>,
        lineage: Option<Arc<str>>,
    ) -> bool {
        // Get initial state
        let handler = match self.event_manager.call(|r| {
            EventManagerMsg::GetEventHandler(self.query_item_type.clone(), r)
        }) {
            Ok(h) => h,
            Err(err) => {
                error!("Failed to get event handler: {}", err);
                sink.push(QueryStreamUpdate::Error(format!(
                    "Failed to get event handler: {}",
                    err
                )));
                return false;
            }
        };

        let mut initial_state = match handler.call(EventHandlerMessage::GetState) {
            Ok(s) => s,
            Err(err) => {
                error!("Failed to get initial state: {}", err);
                sink.push(QueryStreamUpdate::Error(format!(
                    "Failed to get initial state: {}",
                    err
                )));
                return false;
            }
        };

        // Filter with panic catching
        let closure = &self.closure;
        let ctx = &self.ctx;
        let mut panic_error: Option<String> = None;
        initial_state.retain(|_k, v| {
            if panic_error.is_some() {
                return false; // Skip remaining items after first panic
            }
            match Self::call_closure_safe(closure, ctx, v, &query) {
                Ok(matches) => matches,
                Err(panic_msg) => {
                    error!("Query closure panicked during subscription: {}", panic_msg);
                    panic_error = Some(panic_msg);
                    false
                }
            }
        });

        // If there was a panic, send error and fail subscription
        if let Some(panic_msg) = panic_error {
            sink.push(QueryStreamUpdate::Error(format!(
                "Query handler panicked: {}",
                panic_msg
            )));
            return false;
        }

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
                lineage: lineage.clone(),
            },
        );

        log::debug!(
            "[QueryHandler {}] Created subscription tx={}, total_watchers={}, lineage={}",
            self.query_item_type,
            tx,
            self.watchers.len(),
            lineage.as_deref().unwrap_or("(external)")
        );
        true
    }

    fn handle_cancel_query(&mut self, tx: Arc<str>) {
        if let Some(watcher) = self.watchers.remove(&tx) {
            log::debug!(
                "[QueryHandler {}] Cancelled subscription tx={}, remaining_watchers={}, lineage={}",
                self.query_item_type,
                tx,
                self.watchers.len(),
                watcher.lineage.as_deref().unwrap_or("(external)")
            );
        } else {
            log::warn!(
                "[QueryHandler {}] CancelQuery for unknown tx={}, watchers={}",
                self.query_item_type,
                tx,
                self.watchers.len()
            );
        }
    }

    /// Notify all streaming watchers about an update.
    /// Checks if each watcher's query matches the update before sending.
    fn notify_watchers(&mut self, data: &ProcessUpdateData) {
        // Collect dead watchers and errored watchers to remove
        let mut dead_watchers = Vec::new();

        for (tx, watcher) in self.watchers.iter_mut() {
            let update = match data {
                ProcessUpdateData::Del(id) => {
                    // Always send deletes - watcher will ignore if not tracking this ID
                    Some(QueryStreamUpdate::Remove(id.clone()))
                }
                ProcessUpdateData::Set(item) => {
                    // Check if item matches query with panic catching
                    match Self::call_closure_safe(
                        &watcher.closure,
                        &watcher.ctx,
                        item,
                        &watcher.query,
                    ) {
                        Ok(matches) => {
                            if matches {
                                log::trace!(
                                    "[QueryHandler] Upsert matches tx={} item={}",
                                    tx,
                                    item.id()
                                );
                                Some(QueryStreamUpdate::Upsert(item.id(), item.clone()))
                            } else {
                                // Item doesn't match - send remove in case it was previously matching
                                Some(QueryStreamUpdate::Remove(item.id()))
                            }
                        }
                        Err(panic_msg) => {
                            error!(
                                "Query closure panicked during notify (tx={}): {}",
                                tx, panic_msg
                            );
                            // Send error and mark for removal
                            watcher.sink.push(QueryStreamUpdate::Error(format!(
                                "Query handler panicked: {}",
                                panic_msg
                            )));
                            dead_watchers.push(tx.clone());
                            continue;
                        }
                    }
                }
            };

            if let Some(ref upd) = update {
                let pushed = watcher.sink.push(upd.clone());
                if !pushed {
                    log::warn!(
                        "[QueryHandler] sink.push failed for tx={}",
                        tx
                    );
                    dead_watchers.push(tx.clone());
                }
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
    type Args = QueryHandlerArgs;

    fn new(args: Self::Args, _myself: ActorRef<Self::Msg>) -> Self {
        Self::create(args)
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            QueryHandlerMsg::ProcessUpdate(data) => {
                self.handle_process_update(data);
            }
            QueryHandlerMsg::ProcessBatch(batch) => {
                self.handle_process_batch(batch);
            }
            QueryHandlerMsg::QuerySnapshot(query, reply) => {
                debug!("[QH:{}] Received QuerySnapshot", query.query_id());
                self.handle_query_snapshot(query, reply);
            }
            QueryHandlerMsg::WatchQuery(query, reply, lineage) => {
                self.handle_watch_query(query, reply, lineage);
            }
            QueryHandlerMsg::WatchQueryWithSink(query, sink, lineage) => {
                self.handle_watch_query_with_sink(query, sink, lineage);
            }
            QueryHandlerMsg::CancelQuery(tx) => {
                self.handle_cancel_query(tx);
            }
        }
    }
}
