use crate::{
    actors::{
        kafka::{
            common::KafkaSharedConfig,
            consumer::{KafkaConsumer, KafkaConsumerArgs},
            producer::{KafkaProducer, KafkaProducerArgs, KafkaProducerMsg, ProduceEventData},
        },
        query::{common::ProcessUpdateData, query_manager::QueryManagerMsg},
        repo_manager::RepoManagerMsg,
        server::{MykoServerCtx, ServerMsg},
    },
    event::MEvent,
    item::MykoEntityController,
};
use log::{debug, error};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::{any::TypeId, sync::Arc};

pub struct Repo;

pub struct RepoState {
    entity_name: Arc<str>,
    server: ActorRef<ServerMsg>,
    ctx: Arc<MykoServerCtx>,
    kafka_producer: Option<ActorRef<KafkaProducerMsg>>,
    entity_controller: Box<dyn MykoEntityController>,
}

pub enum RepoMsg {
    ProcessEvent(MEvent, bool), // bool for persist
    Init(KafkaSharedConfig),
    PersisterCaughtUp,
}

pub struct RepoArgs {
    pub entity_name: Arc<str>,
    pub server: ActorRef<ServerMsg>,
    pub ctx: Arc<MykoServerCtx>,
    pub store: Box<dyn MykoEntityController>,
}

impl Actor for Repo {
    type Msg = RepoMsg;

    type State = RepoState;

    type Arguments = RepoArgs;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        let RepoArgs {
            entity_name,
            server,
            ctx,
            store,
        } = args;

        debug!("Creating Repo: {}", entity_name);

        Ok(RepoState {
            entity_name: entity_name,
            server,
            entity_controller: store,
            ctx,
            kafka_producer: None,
        })
    }

    async fn handle(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            RepoMsg::Init(conf) => {
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
            RepoMsg::PersisterCaughtUp => {
                debug!(
                    "{} Init Complete: {} entities",
                    state.entity_name,
                    state.entity_controller.len()
                );
                match state.server.send_message(ServerMsg::RepoManagerMsg(
                    RepoManagerMsg::RepoInitComplete(state.entity_name.clone()),
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
            RepoMsg::ProcessEvent(event, persist) => {
                let key = event.item_id();

                if key.is_none() {
                    error!("Event item ID is missing");
                    return Ok(());
                }

                let key = key.expect("None should be handled");

                if let Some(kafka_producer) = &state.kafka_producer {
                    if persist {
                        let mut event = event.clone();
                        event.source_id = Some(state.ctx.host_id.to_string());

                        let produce_res = kafka_producer.send_message(
                            KafkaProducerMsg::ProduceEvent(ProduceEventData {
                                event: event,
                                key: key.clone(),
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

                let item_json = event.item_json();
                let change_type = event.change_type();

                match change_type {
                    crate::event::MEventType::DEL => {
                        state.entity_controller.del(&key);

                        if let Err(err) = state.server.send_message(ServerMsg::QueryManagerMsg(
                            QueryManagerMsg::ProcessUpdate(
                                ProcessUpdateData::Del(key.clone()),
                                state.entity_name.clone(),
                            ),
                        )) {
                            error!("Failed to send message to query manager: {}", err);
                        };
                    }
                    crate::event::MEventType::SET => {
                        match state.entity_controller.set(key, item_json) {
                            Ok(item) => {
                                if let Err(err) = state.server.send_message(
                                    ServerMsg::QueryManagerMsg(QueryManagerMsg::ProcessUpdate(
                                        ProcessUpdateData::Set(item.clone()),
                                        state.entity_name.clone(),
                                    )),
                                ) {
                                    error!("Failed to send message to query manager: {}", err);
                                };
                            }
                            Err(err) => {
                                error!("Failed to insert item: {}", err);
                            }
                        };
                    }
                }

                Ok(())
            }
        }
    }
}
