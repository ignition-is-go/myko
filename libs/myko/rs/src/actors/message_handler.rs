use log::{debug, error, info};
use ractor::{Actor, ActorRef};

use crate::{
    actors::{
        event::{common::ProcessEventData, event_manager::EventManagerMsg},
        query::query_manager::QueryManagerMsg,
    },
    message::MykoMessage,
};

use super::event::common::PersistEvent;

pub struct MessageHandler;

pub struct MessageHandlerArgs {
    pub event_manager: ActorRef<EventManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
}

pub struct MessageHandlerState {
    event_manager: ActorRef<EventManagerMsg>,
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
            event_manager: args.event_manager,
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
                        state
                            .event_manager
                            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                                event,
                                persist: PersistEvent::Persist,
                            }))?;
                        Ok(())
                    }
                    MykoMessage::Query(query) => {
                        debug!("Received query: {:?}", query);

                        state
                            .query_manager
                            .send_message(QueryManagerMsg::StartQuery(query))?;

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
