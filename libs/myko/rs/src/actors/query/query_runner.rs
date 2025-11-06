use crate::actors::{
    query::{common::ProcessUpdateData, query_manager::QueryClosureType},
    server::MykoServerCtx,
};
use log::debug;
use ractor::Actor;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct QueryRunner;

pub struct QueryRunnerArgs {
    pub initial_state: HashMap<Arc<str>, Arc<dyn Any + Send + Sync>>,
    pub query: Arc<dyn Any + Send + Sync>,
    pub closure: QueryClosureType,
    pub tx: Arc<str>,
    pub ctx: Arc<MykoServerCtx>,
}

pub struct QueryRunnerState {
    state: HashMap<Arc<str>, Arc<dyn Any + Send + Sync>>,
    tx: Arc<str>,
    closure: QueryClosureType,
    ctx: Arc<MykoServerCtx>,
    query: Arc<dyn Any + Send + Sync>,
}

pub enum QueryRunnerMsg {
    ProcessUpdate(ProcessUpdateData),
}

impl Actor for QueryRunner {
    type State = QueryRunnerState;
    type Msg = QueryRunnerMsg;
    type Arguments = QueryRunnerArgs;

    async fn pre_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("QueryRunner pre_start: {}: {:?}", args.tx, args.query);

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
        myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            QueryRunnerMsg::ProcessUpdate(data) => {
                debug!("Runner processing update");
                match data {
                    ProcessUpdateData::Del(id) => {}
                    ProcessUpdateData::Set(item) => {
                        let closure = state.closure.clone();

                        let matches = closure(item, state.ctx.clone(), state.query.clone());

                        debug!("Matches: {:?}", matches);
                    }
                }
            }
        }
        Ok(())
    }
}
