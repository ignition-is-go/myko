use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use log::{debug, info, warn};
use ractor::{Actor, ActorCell, ActorProcessingErr, ActorRef};

use crate::{
    actors::{common::REPO_MANAGER_NAME, kafka_common::KafkaSharedConfig, repo::RepoMsg},
    event::MEvent,
};

pub struct RepoManager;

#[derive(Default)]
pub struct RepoManagerState {
    repos: HashMap<Arc<str>, ActorRef<RepoMsg>>,
    left_to_init: HashSet<Arc<str>>,
}

pub enum RepoManagerMsg {
    RegisterRepo(Arc<str>, ActorRef<RepoMsg>),
    InitAll(KafkaSharedConfig),
    NotifyRepoInitComplete(Arc<str>),
    ProcessEvent(MEvent),
}

impl Actor for RepoManager {
    type Msg = RepoManagerMsg;

    type State = RepoManagerState;

    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("Initializing RepoManager");
        Ok(RepoManagerState::default())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            RepoManagerMsg::RegisterRepo(entity_name, actor_ref) => {
                state.left_to_init.insert(entity_name.clone());
                if state.repos.contains_key(&entity_name) {
                    return Err(ActorProcessingErr::from(String::from(
                        "Entity already exists",
                    )));
                }
                state.repos.insert(entity_name.clone(), actor_ref);
                info!("Registered repository: {}", entity_name);
                Ok(())
            }
            RepoManagerMsg::ProcessEvent(event) => {
                let entity_type: Arc<str> = event.item_type().into();

                let repo_ref = state.repos.get(&entity_type);
                if repo_ref.is_none() {
                    warn!("No repository found for event type: {}", event.item_type());
                }
                repo_ref
                    .unwrap()
                    .send_message(RepoMsg::ProcessEvent(event))?;
                Ok(())
            }
            RepoManagerMsg::NotifyRepoInitComplete(entity_name) => {
                state.left_to_init.remove(&entity_name);
                let left_to_init = state.left_to_init.len();
                let total_repos = state.repos.len();
                let total_init = total_repos - left_to_init;
                info!(
                    "Repository init complete: ({}/{}) {} [{}]",
                    total_init,
                    total_repos,
                    entity_name,
                    state
                        .left_to_init
                        .iter()
                        .take(3)
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if state.left_to_init.is_empty() {
                    info!("All repositories initialized");
                };
                Ok(())
            }
            RepoManagerMsg::InitAll(config) => {
                for (_, repo_ref) in &state.repos {
                    let _ = repo_ref.send_message(RepoMsg::Init(config.clone()));
                }
                Ok(())
            }
        }
    }
}

pub async fn assert_repo_manager() -> ActorCell {
    let existing = ractor::registry::where_is(String::from(REPO_MANAGER_NAME));
    match existing {
        Some(actor) => return actor,
        None => Actor::spawn(Some(String::from(REPO_MANAGER_NAME)), RepoManager, ())
            .await
            .unwrap()
            .0
            .get_cell(),
    }
}

pub async fn init_all(
    config: KafkaSharedConfig,
) -> Result<(), ractor::MessagingErr<RepoManagerMsg>> {
    assert_repo_manager()
        .await
        .send_message(RepoManagerMsg::InitAll(config))?;
    Ok(())
}
