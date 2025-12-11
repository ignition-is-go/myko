use super::event::common::PersistEvent;
use crate::{
    actors::{
        event::{common::ProcessEventData, event_manager::EventManagerMsg},
        query::query_manager::QueryManagerMsg,
        server::ServerMsg,
        ws::websocket_server::{SendToClientData, WebSocketServerMsg},
    },
    api::query::QueryResponse,
    item::WrappedItem,
    message::MykoMessage,
};
use futures_signals::signal_map::SignalMapExt;
use log::{debug, error, trace};
use ractor::{Actor, ActorRef, cast};
use std::sync::Arc;

pub struct MessageHandler;

pub struct MessageHandlerArgs {
    pub event_manager: ActorRef<EventManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub server: ActorRef<ServerMsg>,
}

pub struct MessageHandlerState {
    event_manager: ActorRef<EventManagerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    server: ActorRef<ServerMsg>,
}

pub struct ProcessTextData {
    pub client_id: Arc<str>,
    pub text: String,
}

pub enum MessageHandlerMsg {
    ProcessText(ProcessTextData),
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
            server: args.server,
        })
    }

    async fn handle(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            MessageHandlerMsg::ProcessText(ProcessTextData { client_id, text }) => {
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
                        trace!("Received query: {:?}", query);
                        let item_type_clone = query.query_item_type.clone();

                        let sig =
                            ractor::call!(state.query_manager, QueryManagerMsg::StartQuery, query)?;

                        let mut sequence = 0_u64;

                        let server_ref = state.server.clone();

                        tokio::spawn(sig.for_each(move |v| {
                            debug!("Map Diff in MessageHandler: {:?}", v);

                            let update: QueryResponse = match v {
                                futures_signals::signal_map::MapDiff::Replace { entries } => {
                                    sequence = 0;
                                    let items =
                                        entries.iter().map(|f| f.1.to_value()).collect::<Vec<_>>();

                                    let upserts = items
                                        .iter()
                                        .cloned()
                                        .map(|item| {

                                            WrappedItem {
                                                item,
                                                item_type: item_type_clone.clone(),
                                            }
                                        })
                                        .collect::<Vec<_>>();
                                    QueryResponse {
                                        sequence,
                                        deletes: vec![],
                                        upserts,
                                        tx: "faketx".into(),
                                    }
                                }
                                futures_signals::signal_map::MapDiff::Insert { key: _, value }
                                | futures_signals::signal_map::MapDiff::Update { key: _, value } => {
                                    let upserts = vec![WrappedItem {
                                        item: value.to_value(),
                                        item_type: item_type_clone.clone(),
                                    }];

                                    sequence += 1;

                                    QueryResponse {
                                        deletes: vec![],
                                        upserts,
                                        tx: "faketx".into(),
                                        sequence,
                                    }
                                }

                                futures_signals::signal_map::MapDiff::Remove { key } => {
                                    sequence += 1;

                                    let deletes = vec![key];

                                    QueryResponse {
                                        deletes,
                                        upserts: vec![],
                                        tx: "faketx".into(),
                                        sequence,
                                    }
                                }
                                futures_signals::signal_map::MapDiff::Clear {} => {
                                    sequence = 0;

                                    QueryResponse {
                                        deletes: vec![],
                                        upserts: vec![],
                                        tx: "faketx".into(),
                                        sequence,
                                    }
                                }
                            };

                            if let Err(e) = cast!(
                                server_ref,
                                ServerMsg::WebSocketServerMsg(WebSocketServerMsg::SendToClient(
                                    SendToClientData {
                                        client_id: client_id.clone(),
                                        message: MykoMessage::QueryResponse(update),
                                    }
                                ))
                            ) {
                                error!("Failed to send query response: {}", e);
                            };

                            async {}
                        }));

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
