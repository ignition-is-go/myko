use log::{debug, error};
use ractor::{Actor, ActorRef};
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    actors::{
        query::{
            common::ProcessUpdateData,
            query_manager::QueryClosureType,
            query_runner::{QueryRunner, QueryRunnerArgs, QueryRunnerMsg},
        },
        server::MykoServerCtx,
    },
    common::any_parser::MykoAnyParser,
};

pub struct QueryHandler;

pub struct QueryHandlerArgs {
    pub query_id: Arc<str>,
    pub closure: QueryClosureType,
    pub ctx: Arc<MykoServerCtx>,
}

pub struct QueryHandlerState {
    query_id: Arc<str>,
    closure: QueryClosureType,
    runners: HashMap<Arc<str>, ActorRef<QueryRunnerMsg>>,
    ctx: Arc<MykoServerCtx>,
}

pub enum QueryHandlerMsg {
    ProcessUpdate(ProcessUpdateData),
    StartQuery(Arc<dyn Any + Send + Sync>),
}

impl Actor for QueryHandler {
    type Arguments = QueryHandlerArgs;

    type State = QueryHandlerState;

    type Msg = QueryHandlerMsg;

    async fn pre_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("Creating Handler for query {}", args.query_id);

        Ok(QueryHandlerState {
            query_id: args.query_id,
            closure: args.closure,
            runners: HashMap::new(),
            ctx: args.ctx,
        })
    }

    async fn handle(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            QueryHandlerMsg::StartQuery(query) => {
                debug!("Starting query {}", state.query_id);

                let tx: Arc<str> = "tx-id".into();

                match Actor::spawn(
                    None,
                    QueryRunner,
                    QueryRunnerArgs {
                        // TODO: this real
                        initial_state: HashMap::new(),
                        query,
                        tx: tx.clone(),
                        closure: state.closure.clone(),
                        ctx: state.ctx.clone(),
                    },
                )
                .await
                {
                    Ok((runner, runner_handle)) => {
                        state.runners.insert(tx, runner);
                    }
                    Err(err) => {
                        error!("Failed to spawn query runner: {}", err);
                    }
                };
                //
            }
            QueryHandlerMsg::ProcessUpdate(data) => {
                debug!("Processing update in {} runners", state.runners.len());
                for runner in state.runners.values() {
                    if let Err(err) =
                        runner.send_message(QueryRunnerMsg::ProcessUpdate(data.clone()))
                    {
                        error!("Failed to send update to runner: {}", err);
                    };
                }
            }
        }

        Ok(())
    }
}
