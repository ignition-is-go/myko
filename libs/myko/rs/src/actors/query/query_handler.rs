use crate::{
    actors::{
        event::{event_handler::EventHandlerMessage, event_manager::EventManagerMsg},
        query::{
            common::{ProcessUpdateData, QueryStreamUpdate},
            query_manager::QueryClosureType,
            query_runner::{QueryRunner, QueryRunnerArgs, QueryRunnerMsg},
        },
        server::ServerMsg,
    },
    parsers::query::AnyQuery,
    prelude::AnyItem,
    query::QueryHandlerCtxAny,
    server::MykoServerCtx,
};
use futures_signals::signal_map::MutableSignalMap;
use log::{debug, error, trace};
use ractor::{Actor, ActorRef, RpcReplyPort};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;

pub struct QueryHandler;

pub struct QueryHandlerArgs {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub closure: QueryClosureType,
    pub ctx: Arc<MykoServerCtx>,
    pub server: ActorRef<ServerMsg>,
    pub event_manager: ActorRef<EventManagerMsg>,
}

/// Tracks a streaming query subscription
struct WatchSubscription {
    sender: mpsc::UnboundedSender<QueryStreamUpdate>,
    closure: QueryClosureType,
    ctx: Arc<MykoServerCtx>,
    query: Arc<dyn AnyQuery>,
}

pub struct QueryHandlerState {
    query_item_type: Arc<str>,
    closure: QueryClosureType,
    runners: HashMap<Arc<str>, ActorRef<QueryRunnerMsg>>,
    /// Active streaming query subscriptions (by tx id)
    watchers: HashMap<Arc<str>, WatchSubscription>,
    ctx: Arc<MykoServerCtx>,
    event_manager: ActorRef<EventManagerMsg>,
}

pub enum QueryHandlerMsg {
    ProcessUpdate(ProcessUpdateData),
    /// Batch of updates - forwarded to runners for efficient processing
    ProcessBatch(Vec<ProcessUpdateData>),
    StartQuery(
        Arc<dyn AnyQuery>,
        RpcReplyPort<MutableSignalMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ),
    /// One-shot query that returns current state without creating a subscription
    QuerySnapshot(
        Arc<dyn AnyQuery>,
        RpcReplyPort<std::collections::BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>,
    ),
    /// Watch a query and receive updates via a channel.
    /// Returns an unbounded receiver that emits QueryStreamUpdate messages.
    WatchQuery(
        Arc<dyn AnyQuery>,
        RpcReplyPort<mpsc::UnboundedReceiver<QueryStreamUpdate>>,
    ),
    /// Cancel a query subscription by transaction ID
    CancelQuery(Arc<str>),
}

impl Actor for QueryHandler {
    type Arguments = QueryHandlerArgs;

    type State = QueryHandlerState;

    type Msg = QueryHandlerMsg;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        trace!("Creating Handler for query {}", args.query_id);

        Ok(QueryHandlerState {
            closure: args.closure,
            runners: HashMap::new(),
            watchers: HashMap::new(),
            ctx: args.ctx,
            event_manager: args.event_manager,
            query_item_type: args.query_item_type,
        })
    }

    async fn handle(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            QueryHandlerMsg::StartQuery(query, reply) => {
                let handler = ractor::call!(
                    state.event_manager.clone(),
                    EventManagerMsg::GetEventHandler,
                    state.query_item_type.clone()
                )?;

                let mut initial_state = ractor::call!(handler, EventHandlerMessage::GetState)?;

                initial_state.retain(|_k, v| {
                    state.closure.clone()(QueryHandlerCtxAny {
                        ctx: state.ctx.clone(),
                        item: v.clone(),
                        query: query.clone(),
                    })
                });

                let tx: Arc<str> = query.tx_id();

                match Actor::spawn(
                    None,
                    QueryRunner,
                    QueryRunnerArgs {
                        initial_state,
                        query,
                        closure: state.closure.clone(),
                        ctx: state.ctx.clone(),
                    },
                )
                .await
                {
                    Ok((runner, _runner_handle)) => {
                        state.runners.insert(tx, runner.clone());

                        let response_signal = ractor::call!(runner, QueryRunnerMsg::GetState)?;

                        reply.send(response_signal)?;
                    }
                    Err(err) => {
                        error!("Failed to spawn query runner: {}", err);
                    }
                };
                //
            }
            QueryHandlerMsg::ProcessUpdate(data) => {
                for runner in state.runners.values() {
                    if let Err(err) =
                        runner.send_message(QueryRunnerMsg::ProcessUpdate(data.clone()))
                    {
                        error!("Failed to send update to runner: {}", err);
                    };
                }

                // Also notify streaming watchers
                Self::notify_watchers(state, &data);
            }
            QueryHandlerMsg::ProcessBatch(batch) => {
                // Forward batch to all runners for efficient processing
                for runner in state.runners.values() {
                    if let Err(err) =
                        runner.send_message(QueryRunnerMsg::ProcessBatch(batch.clone()))
                    {
                        error!("Failed to send batch to runner: {}", err);
                    };
                }

                // Also notify streaming watchers for each update in batch
                for data in &batch {
                    Self::notify_watchers(state, data);
                }
            }
            QueryHandlerMsg::QuerySnapshot(query, reply) => {
                // One-shot query: get current state, filter, and return immediately
                let handler = ractor::call!(
                    state.event_manager.clone(),
                    EventManagerMsg::GetEventHandler,
                    state.query_item_type.clone()
                )?;

                let mut snapshot = ractor::call!(handler, EventHandlerMessage::GetState)?;

                snapshot.retain(|_k, v| {
                    state.closure.clone()(QueryHandlerCtxAny {
                        ctx: state.ctx.clone(),
                        item: v.clone(),
                        query: query.clone(),
                    })
                });

                reply.send(snapshot)?;
            }
            QueryHandlerMsg::WatchQuery(query, reply) => {
                // Get initial state
                let handler = ractor::call!(
                    state.event_manager.clone(),
                    EventManagerMsg::GetEventHandler,
                    state.query_item_type.clone()
                )?;

                let mut initial_state = ractor::call!(handler, EventHandlerMessage::GetState)?;

                initial_state.retain(|_k, v| {
                    state.closure.clone()(QueryHandlerCtxAny {
                        ctx: state.ctx.clone(),
                        item: v.clone(),
                        query: query.clone(),
                    })
                });

                let tx: Arc<str> = query.tx_id();

                // Create channel for updates
                let (sender, receiver) = mpsc::unbounded_channel();

                // Send initial state
                if sender
                    .send(QueryStreamUpdate::Initial(initial_state.clone()))
                    .is_err()
                {
                    error!("Failed to send initial state to watcher");
                    return Ok(());
                }

                // Store the watcher
                state.watchers.insert(
                    tx.clone(),
                    WatchSubscription {
                        sender,
                        closure: state.closure.clone(),
                        ctx: state.ctx.clone(),
                        query,
                    },
                );

                debug!("Created watch subscription with tx {}", tx);
                reply.send(receiver)?;
            }
            QueryHandlerMsg::CancelQuery(tx) => {
                // Remove and stop the runner for this tx if we own it
                if let Some(runner) = state.runners.remove(&tx) {
                    trace!("Cancelling query runner for tx {}", tx);
                    runner.stop(Some("Query cancelled by client".to_string()));
                }
                // Also remove any watcher for this tx
                if state.watchers.remove(&tx).is_some() {
                    trace!("Cancelled watch subscription for tx {}", tx);
                }
            }
        }

        Ok(())
    }
}

impl QueryHandler {
    /// Notify all streaming watchers about an update.
    /// Checks if each watcher's query matches the update before sending.
    fn notify_watchers(state: &mut QueryHandlerState, data: &ProcessUpdateData) {
        // Collect dead watchers to remove
        let mut dead_watchers = Vec::new();

        for (tx, watcher) in state.watchers.iter() {
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
                && watcher.sender.send(update).is_err()
            {
                // Channel closed - mark for removal
                dead_watchers.push(tx.clone());
            }
        }

        // Remove dead watchers
        for tx in dead_watchers {
            debug!("Removing dead watcher with tx {}", tx);
            state.watchers.remove(&tx);
        }
    }
}
