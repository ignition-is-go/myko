use crate::{
    actors::{
        kafka::{
            common::KafkaSharedConfig,
            consumer::{KafkaConsumer, KafkaConsumerArgs},
            producer::{KafkaProducer, KafkaProducerArgs, KafkaProducerMsg, ProduceEventData},
        },
        repo_manager::RepoManagerMsg,
        server::{ServerCtx, ServerMsg},
    },
    event::MEvent,
    item::BaseItem,
};
use log::{debug, error};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use rdkafka::types::RDKafkaApiKey;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

pub struct Repo;

pub struct RepoState {
    store: HashMap<Arc<str>, Value>,
    entity_name: Arc<str>,
    server: ActorRef<ServerMsg>,
    ctx: Arc<ServerCtx>,
    kafka_producer: Option<ActorRef<KafkaProducerMsg>>,
}

pub enum RepoMsg {
    ProcessEvent(MEvent, bool), // bool for persist
    Init(KafkaSharedConfig),
    PersisterCaughtUp,
}

pub struct RepoArgs {
    pub entity_name: Arc<str>,
    pub server: ActorRef<ServerMsg>,
    pub ctx: Arc<ServerCtx>,
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
        } = args;

        debug!("Creating Repo: {}", entity_name);

        Ok(RepoState {
            entity_name: entity_name,
            server,
            store: HashMap::new(),
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
            RepoMsg::ProcessEvent(event, persist) => {
                let base_item: BaseItem = match event.clone().try_into() {
                    Ok(base_item) => base_item,
                    Err(err) => {
                        error!("Failed to convert event to BaseItem: {}", err);
                        return Err(ActorProcessingErr::from(err));
                    }
                };

                if let Some(kafka_producer) = &state.kafka_producer {
                    let key = base_item.id.to_string();

                    if persist {
                        let mut event = event.clone();
                        event.source_id = Some(state.ctx.host_id.to_string());

                        let produce_res = kafka_producer.send_message(
                            KafkaProducerMsg::ProduceEvent(ProduceEventData { event: event, key }),
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

                let id: Arc<str> = base_item.id.into();

                match change_type {
                    crate::event::MEventType::DEL => {
                        state.store.remove(&id);
                    }
                    _ => {
                        state.store.insert(id, item_json);
                    }
                }

                Ok(())
            }
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
                        ctx: state.ctx.clone(),
                        repo_ref: myself.clone(),
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
                    state.store.len()
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
        }
    }
}
