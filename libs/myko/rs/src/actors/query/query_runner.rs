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
    pub tx: Arc<str>,
    pub ctx: Arc<MykoServerCtx>,
}

pub struct QueryRunnerState {
    state: MutableBTreeMap<Arc<str>, Arc<dyn AnyItem + 'static>>,
    tx: Arc<str>,
    closure: QueryClosureType,
    ctx: Arc<MykoServerCtx>,
    query: Arc<dyn AnyQuery>,
}

#[derive(Debug)]
pub enum QueryRunnerMsg {
    ProcessUpdate(ProcessUpdateData),
    GetState(RpcReplyPort<MutableSignalMap<Arc<str>, Arc<dyn AnyItem + 'static>>>),
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
            tx: args.tx,
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
                let _tx = state.tx.clone();

                match data {
                    ProcessUpdateData::Del(id) => {
                        // somehow emit changes here
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
            QueryRunnerMsg::GetState(reply) => {
                reply.send(state.state.signal_map_cloned())?;
            }
        }
        Ok(())
    }
}
