use log::{error, info};
use ractor::{Actor, ActorRef};

use crate::{
    actors::{
        query::query_manager::{self, QueryManagerMsg},
        repo_manager::RepoManagerMsg,
    },
    item::Eventable,
    message::MykoMessage,
    query::QueryClosure,
};

pub struct MessageHandler;

pub struct MessageHandlerArgs {
    pub repo_manager: ActorRef<RepoManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
}

pub struct MessageHandlerState {
    repo_manager: ActorRef<RepoManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
}

pub enum MessageHandlerMsg {
    ProcessText(String),
}

impl Actor for MessageHandler {
    type Msg = MessageHandlerMsg;

    type State = MessageHandlerState;

    type Arguments = MessageHandlerArgs;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        Ok(MessageHandlerState {
            repo_manager: args.repo_manager,
            query_manager: args.query_manager,
        })
    }

    async fn handle(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            MessageHandlerMsg::ProcessText(text) => {
                let m = text;
                let myko_message = serde_json::from_str::<MykoMessage<()>>(&m).unwrap();
                match myko_message {
                    MykoMessage::Event(event) => {
                        match state
                            .repo_manager
                            .send_message(RepoManagerMsg::ProcessEvent(event, true))
                        {
                            Ok(_) => Ok(()),
                            Err(err) => {
                                error!("Failed to forward message to RepoManager: {}", err);
                                Ok(())
                            }
                        }
                    }
                    MykoMessage::Query(query) => {
                        info!("Received query: {:?}", query);

                        if let Err(err) = state
                            .query_manager
                            .send_message(QueryManagerMsg::StartQuery(query))
                        {
                            error!("Failed to forward message to QueryManager: {}", err);
                        }

                        Ok(())
                    }
                    _ => {
                        error!("Unknown message type: {:?}", myko_message);
                        Ok(())
                    }
                }
            }
        }
    }
}
