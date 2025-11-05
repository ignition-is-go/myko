use crate::{
    actors::{
        kafka_common::KafkaSharedConfig,
        kafka_consumer::{KafkaConsumer, KafkaConsumerArgs},
        repo_manager::RepoManagerMsg,
        server::ServerMsg,
    },
    event::MEvent,
    item::BaseItem,
};
use log::{debug, error};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

pub struct Repo;

pub struct RepoState {
    store: HashMap<Arc<str>, Value>,
    entity_name: Arc<str>,
    server: ActorRef<ServerMsg>,
}

pub enum RepoMsg {
    ProcessEvent(MEvent),
    Init(KafkaSharedConfig),
    PersisterCaughtUp,
}

pub struct RepoArgs {
    pub entity_name: Arc<str>,
    pub server: ActorRef<ServerMsg>,
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
        } = args;

        debug!("Creating Repo: {}", entity_name);

        Ok(RepoState {
            entity_name: entity_name,
            server,
            store: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            RepoMsg::ProcessEvent(event) => {
                let item_json = event.item_json();
                let change_type = event.change_type();

                let base_item: BaseItem = match event.try_into() {
                    Ok(base_item) => base_item,
                    Err(err) => {
                        error!("Failed to convert event to BaseItem: {}", err);
                        return Err(ActorProcessingErr::from(err));
                    }
                };

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
                    },
                )
                .await?;

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
