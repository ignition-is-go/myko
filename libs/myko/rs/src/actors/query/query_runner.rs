use crate::{
    actors::query::{common::ProcessUpdateData, query_manager::QueryClosureType},
    prelude::{AnyItem, AnyQuery},
    query::QueryHandlerCtxAny,
    server::MykoServerCtx,
};
use log::debug;
use ractor::Actor;
use std::{collections::HashMap, sync::Arc};

pub struct QueryRunner;

pub struct QueryRunnerArgs {
    pub initial_state: HashMap<Arc<str>, Arc<dyn AnyItem>>,
    pub query: Arc<dyn AnyQuery>,
    pub closure: QueryClosureType,
    pub tx: Arc<str>,
    pub ctx: Arc<MykoServerCtx>,
}

pub struct QueryRunnerState {
    state: HashMap<Arc<str>, Arc<dyn AnyItem>>,
    tx: Arc<str>,
    closure: QueryClosureType,
    ctx: Arc<MykoServerCtx>,
    query: Arc<dyn AnyQuery>,
}

#[derive(Debug)]
pub enum QueryRunnerMsg {
    ProcessUpdate(ProcessUpdateData),
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

        Ok(QueryRunnerState {
            state: args.initial_state,
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
        debug!(
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
                        state.state.remove(&id);
                        debug!(
                            "Runner [{}] Removed item with id: {} - Deleted",
                            state.query.query_id(),
                            id
                        );
                    }
                    ProcessUpdateData::Set(item) => {
                        debug!(
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
                            state.state.insert(item.id(), item.clone());
                            debug!(
                                "Runner [{}] Inserted item with id: {}",
                                state.query.query_id(),
                                item.id()
                            );
                        } else {
                            state.state.remove(&item.id());
                            debug!(
                                "Runner [{}] Removed item with id: {} - No Longer Matches",
                                state.query.query_id(),
                                item.id()
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
