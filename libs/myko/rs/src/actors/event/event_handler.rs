use crate::{
    actors::{
        event::event_manager::EventManagerMsg,
        kafka::{
            common::KafkaSharedConfig,
            consumer::{KafkaConsumer, KafkaConsumerArgs},
            producer::{KafkaProducer, KafkaProducerArgs, KafkaProducerMsg, ProduceEventData},
        },
        query::{common::ProcessUpdateData, query_manager::QueryManagerMsg},
        server::ServerMsg,
    },
    event::MEvent,
    parsers::item::MykoItemParser,
    prelude::AnyItem,
    server::MykoServerCtx,
};
use log::{debug, error};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::{collections::HashMap, sync::Arc};

pub struct EventHandler;

pub struct EventHandlerState {
    entity_name: Arc<str>,
    server: ActorRef<ServerMsg>,
    ctx: Arc<MykoServerCtx>,
    kafka_producer: Option<ActorRef<KafkaProducerMsg>>,
    parser: Arc<dyn MykoItemParser>,
    state: HashMap<Arc<str>, Arc<dyn AnyItem>>,
}

pub enum EventHandlerMessage {
    ProcessEvent(MEvent, bool), // bool for persist
    Init(KafkaSharedConfig),
    PersisterCaughtUp,
}

pub struct EventHandlerArgs {
    pub entity_name: Arc<str>,
    pub server: ActorRef<ServerMsg>,
    pub ctx: Arc<MykoServerCtx>,
    pub parser: Arc<dyn MykoItemParser>,
}

impl Actor for EventHandler {
    type Msg = EventHandlerMessage;

    type State = EventHandlerState;

    type Arguments = EventHandlerArgs;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        let EventHandlerArgs {
            entity_name,
            server,
            ctx,
            parser,
        } = args;

        debug!("Creating Repo: {}", entity_name);

        Ok(EventHandlerState {
            entity_name: entity_name,
            server,
            ctx,
            parser,
            kafka_producer: None,
            state: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            EventHandlerMessage::Init(conf) => {
                let entity_name = state.entity_name.clone();
                debug!("{}: Init", entity_name);

                Actor::spawn(
                    None,
                    KafkaConsumer,
                    KafkaConsumerArgs {
                        topic: entity_name.clone(),
                        shared_conf: conf,
                        repo_ref: myself.clone(),
                        ctx: state.ctx.clone(),
                    },
                )
                .await?;

                let (producer_ref, _producer_handle) = Actor::spawn(
                    None,
                    KafkaProducer,
                    KafkaProducerArgs {
                        shared_conf: conf,
                        topic: entity_name.clone(),
                    },
                )
                .await?;

                state.kafka_producer = Some(producer_ref);

                Ok(())
            }
            EventHandlerMessage::PersisterCaughtUp => {
                debug!(
                    "{} Init Complete: {} entities",
                    state.entity_name,
                    state.state.len()
                );
                match state.server.send_message(ServerMsg::RepoManagerMsg(
                    EventManagerMsg::RepoInitComplete(state.entity_name.clone()),
                )) {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        error!("Failed to notify repo manager: {}", err);
                        Err(ActorProcessingErr::from(String::from(
                            "failed to notify repo manager of topic caught up",
                        )))
                    }
                }
            }
            EventHandlerMessage::ProcessEvent(event, persist) => {
                let item_json = event.item_json();
                let change_type = event.change_type();

                let item = match state.parser.parse(item_json) {
                    Ok(item) => item,
                    Err(err) => {
                        error!("Failed to parse item: {}", err);
                        return Ok(());
                    }
                };

                if let Some(kafka_producer) = &state.kafka_producer {
                    if persist {
                        let mut event = event.clone();
                        event.source_id = Some(state.ctx.host_id.to_string());

                        let produce_res = kafka_producer.send_message(
                            KafkaProducerMsg::ProduceEvent(ProduceEventData {
                                event: event,
                                key: item.id().clone(),
                            }),
                        );

                        match produce_res {
                            Ok(_) => (),
                            Err(err) => {
                                error!("Failed to produce event: {}", err);
                            }
                        }
                    }
                }

                match change_type {
                    crate::event::MEventType::DEL => {
                        state.state.remove(&item.id());

                        if let Err(err) = state.server.send_message(ServerMsg::QueryManagerMsg(
                            QueryManagerMsg::ProcessUpdate(
                                ProcessUpdateData::Del(item.id().clone()),
                                state.entity_name.clone(),
                            ),
                        )) {
                            error!("Failed to send message to query manager: {}", err);
                        };
                    }
                    crate::event::MEventType::SET => {
                        state.state.insert(item.id(), item.clone());

                        if let Err(err) = state.server.send_message(ServerMsg::QueryManagerMsg(
                            QueryManagerMsg::ProcessUpdate(
                                ProcessUpdateData::Set(item.clone()),
                                state.entity_name.clone(),
                            ),
                        )) {
                            error!("Failed to send message to query manager: {}", err);
                        };
                    }
                }

                Ok(())
            }
        }
    }
}
