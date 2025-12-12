use crate::{
    actors::query::{common::ProcessUpdateData, query_manager::QueryClosureType},
    prelude::{AnyItem, AnyQuery},
    query::QueryHandlerCtxAny,
    server::MykoServerCtx,
};
use futures_signals::signal_map::{MutableBTreeMap, MutableSignalMap};
use log::trace;
use ractor::{Actor, RpcReplyPort};
use std::{collections::BTreeMap, sync::Arc};

pub struct QueryRunner;

pub struct QueryRunnerArgs {
    pub initial_state: BTreeMap<Arc<str>, Arc<dyn AnyItem>>,
    pub query: Arc<dyn AnyQuery>,
    pub closure: QueryClosureType,
    pub ctx: Arc<MykoServerCtx>,
}

pub struct QueryRunnerState {
    state: MutableBTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>,
    closure: QueryClosureType,
    ctx: Arc<MykoServerCtx>,
    query: Arc<dyn AnyQuery>,
}

#[derive(Debug)]
pub enum QueryRunnerMsg {
    ProcessUpdate(ProcessUpdateData),
    /// Batch of updates - processed with single lock acquisition for better throughput
    ProcessBatch(Vec<ProcessUpdateData>),
    GetState(RpcReplyPort<MutableSignalMap<Arc<str>, Arc<dyn AnyItem + 'static>>>),
    /// Get a snapshot of current entries (for one-shot queries)
    GetSnapshot(RpcReplyPort<BTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>>),
}

impl Actor for QueryRunner {
    type State = QueryRunnerState;
    type Msg = QueryRunnerMsg;
    type Arguments = QueryRunnerArgs;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        // debug!(
        //     "Initial state for query {}:\n {:?}\n",
        //     args.query.query_id(),
        //     args.initial_state,
        // );
        let sig = MutableBTreeMap::with_values(args.initial_state.clone());

        Ok(QueryRunnerState {
            state: sig,
            closure: args.closure,
            ctx: args.ctx,
            query: args.query,
        })
    }

    async fn handle(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        trace!(
            "Runner Processing message: {}: {:?}",
            state.query.query_id(),
            message
        );
        match message {
            QueryRunnerMsg::ProcessUpdate(data) => {
                // Single update - process immediately
                Self::process_single_update(state, data);
            }
            QueryRunnerMsg::ProcessBatch(batch) => {
                // Batch update - acquire lock once for all updates (performance optimization)
                if batch.is_empty() {
                    return Ok(());
                }

                // Pre-compute matches before acquiring lock
                let mut to_insert: Vec<(Arc<str>, Arc<dyn AnyItem>)> = Vec::with_capacity(batch.len());
                let mut to_remove: Vec<Arc<str>> = Vec::with_capacity(batch.len());

                let closure = &state.closure;
                let ctx = &state.ctx;
                let query = &state.query;

                for data in batch {
                    match data {
                        ProcessUpdateData::Del(id) => {
                            to_remove.push(id);
                        }
                        ProcessUpdateData::Set(item) => {
                            let matches = closure(QueryHandlerCtxAny {
                                ctx: ctx.clone(),
                                item: item.clone(),
                                query: query.clone(),
                            });

                            if matches {
                                to_insert.push((item.id(), item));
                            } else {
                                to_remove.push(item.id());
                            }
                        }
                    }
                }

                // Single lock acquisition for all operations
                {
                    let mut lock = state.state.lock_mut();
                    for id in to_remove {
                        lock.remove(&id);
                    }
                    for (id, item) in to_insert {
                        lock.insert_cloned(id, item);
                    }
                }

                trace!(
                    "Runner [{}] Processed batch",
                    state.query.query_id()
                );
            }
            QueryRunnerMsg::GetState(reply) => {
                reply.send(state.state.signal_map_cloned())?;
            }
            QueryRunnerMsg::GetSnapshot(reply) => {
                let snapshot = state.state.lock_ref().clone();
                reply.send(snapshot)?;
            }
        }
        Ok(())
    }
}

impl QueryRunner {
    /// Process a single update (used by ProcessUpdate message)
    fn process_single_update(state: &mut QueryRunnerState, data: ProcessUpdateData) {
        match data {
            ProcessUpdateData::Del(id) => {
                state.state.lock_mut().remove(&id);
                trace!(
                    "Runner [{}] Removed item with id: {} - Deleted",
                    state.query.query_id(),
                    id
                );
            }
            ProcessUpdateData::Set(item) => {
                trace!(
                    "Runner [{}] Setting item with id: {}",
                    state.query.query_id(),
                    item.id()
                );
                let closure = state.closure.clone();
                let matches = closure(QueryHandlerCtxAny {
                    ctx: state.ctx.clone(),
                    item: item.clone(),
                    query: state.query.clone(),
                });

                if matches {
                    state
                        .state
                        .lock_mut()
                        .insert_cloned(item.id(), item.clone());
                    trace!(
                        "Runner [{}] Inserted item with id: {}",
                        state.query.query_id(),
                        item.id()
                    );
                } else {
                    state.state.lock_mut().remove(&item.id());
                    trace!(
                        "Runner [{}] Removed item with id: {} - No Longer Matches",
                        state.query.query_id(),
                        item.id()
                    );
                }
            }
        }
    }
}
