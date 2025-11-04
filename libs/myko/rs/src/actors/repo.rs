use std::{collections::HashMap, sync::Arc};

use log::{debug, error, info};
use ractor::{Actor, ActorProcessingErr};
use serde_json::Value;

use crate::{
    actors::{
        kafka_common::KafkaSharedConfig,
        kafka_consumer::{KafkaConsumer, KafkaConsumerArgs},
        repo_manager::{RepoManagerMsg, assert_repo_manager},
    },
    event::MEvent,
    item::BaseItem,
};
pub struct Repo;

#[derive(Default)]
pub struct RepoState {
    store: HashMap<Arc<str>, Value>,
    entity_name: Arc<str>,
}

pub enum RepoMsg {
    ProcessEvent(MEvent),
    Init(KafkaSharedConfig),
    PersisterCaughtUp,
}

pub struct RepoArgs {
    pub entity_name: Arc<str>,
}

impl Actor for Repo {
    type Msg = RepoMsg;

    type State = RepoState;

    type Arguments = RepoArgs;

    async fn pre_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        let RepoArgs { entity_name } = args;
        let manager = assert_repo_manager().await;

        info!("Initializing: {}", entity_name);
        manager.send_message(RepoManagerMsg::RegisterRepo(
            entity_name.clone(),
            myself.clone(),
        ))?;

        Ok(RepoState {
            entity_name: entity_name,
            ..Default::default()
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
                match assert_repo_manager().await.send_message(
                    RepoManagerMsg::NotifyRepoInitComplete(state.entity_name.clone()),
                ) {
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
