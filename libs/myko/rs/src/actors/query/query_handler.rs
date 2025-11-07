use crate::{
    actors::{
        event::{event_handler::EventHandlerMessage, event_manager::EventManagerMsg},
        query::{
            common::ProcessUpdateData,
            query_manager::QueryClosureType,
            query_runner::{QueryRunner, QueryRunnerArgs, QueryRunnerMsg},
        },
        server::ServerMsg,
    },
    parsers::query::AnyQuery,
    query::QueryHandlerCtxAny,
    server::MykoServerCtx,
};
use log::{debug, error};
use ractor::{Actor, ActorRef};
use std::{collections::HashMap, sync::Arc};

pub struct QueryHandler;

pub struct QueryHandlerArgs {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub closure: QueryClosureType,
    pub ctx: Arc<MykoServerCtx>,
    pub server: ActorRef<ServerMsg>,
    pub event_manager: ActorRef<EventManagerMsg>,
}

pub struct QueryHandlerState {
    query_item_type: Arc<str>,
    query_id: Arc<str>,
    closure: QueryClosureType,
    runners: HashMap<Arc<str>, ActorRef<QueryRunnerMsg>>,
    ctx: Arc<MykoServerCtx>,
    event_manager: ActorRef<EventManagerMsg>,
}

pub enum QueryHandlerMsg {
    ProcessUpdate(ProcessUpdateData),
    StartQuery(Arc<dyn AnyQuery>),
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
        debug!("Creating Handler for query {}", args.query_id);

        Ok(QueryHandlerState {
            query_id: args.query_id,
            closure: args.closure,
            runners: HashMap::new(),
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
            QueryHandlerMsg::StartQuery(query) => {
                debug!("Starting query {}", state.query_id);

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
                        tx: tx.clone(),
                        closure: state.closure.clone(),
                        ctx: state.ctx.clone(),
                    },
                )
                .await
                {
                    Ok((runner, _runner_handle)) => {
                        state.runners.insert(tx, runner);
                    }
                    Err(err) => {
                        error!("Failed to spawn query runner: {}", err);
                    }
                };
                //
            }
            QueryHandlerMsg::ProcessUpdate(data) => {
                if state.runners.len() > 0 {
                    debug!("Processing update in {} runners", state.runners.len());
                }
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
