use crate::{
    actors::{
        kafka::common::KafkaSharedConfig,
        repo::{Repo, RepoArgs, RepoMsg},
        server::{MykoServerCtx, ServerMsg},
    },
    event::MEvent,
    item::MykoEntityController,
};
use log::{debug, error, info, warn};
use ractor::{Actor, ActorRef};
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct RepoManager;

pub struct RepoManagerState {
    repos: HashMap<Arc<str>, ActorRef<RepoMsg>>,
    left_to_init: HashSet<Arc<str>>,
    server: ActorRef<ServerMsg>,
    ctx: Arc<MykoServerCtx>,
}

pub enum RepoManagerMsg {
    RegisterRepo(Arc<str>, Box<dyn MykoEntityController>, TypeId),
    InitAll(KafkaSharedConfig),
    RepoInitComplete(Arc<str>),
    ProcessEvent(MEvent, bool), //bool for persist
}

pub struct RepoManagerArgs {
    pub server: ActorRef<ServerMsg>,
    pub ctx: Arc<MykoServerCtx>,
}

impl Actor for RepoManager {
    type Msg = RepoManagerMsg;

    type State = RepoManagerState;

    type Arguments = RepoManagerArgs;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("Creating RepoManager");
        Ok(RepoManagerState {
            left_to_init: HashSet::new(),
            repos: HashMap::new(),
            server: args.server,
            ctx: args.ctx,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            RepoManagerMsg::RegisterRepo(entity_name, store, type_id) => {
                debug!("Registering repository: {}", entity_name);

                state.left_to_init.insert(entity_name.clone());
                if state.repos.contains_key(&entity_name) {
                    error!("Entity already exists");
                    return Ok(());
                }

                let (repo_ref, _repo_handle) = match Actor::spawn(
                    None,
                    Repo,
                    RepoArgs {
                        server: state.server.clone(),
                        entity_name: entity_name.clone(),
                        ctx: state.ctx.clone(),
                        store,
                        type_id,
                    },
                )
                .await
                {
                    Ok((repo_ref, repo_handle)) => (repo_ref, repo_handle),
                    Err(err) => {
                        error!("Failed to spawn repository: {}", err);
                        return Err(err.into());
                    }
                };

                state.repos.insert(entity_name.clone(), repo_ref);
                info!("Registered repository: {}", entity_name);
                Ok(())
            }
            RepoManagerMsg::ProcessEvent(event, persist) => {
                let entity_type: Arc<str> = event.item_type().into();

                let repo_ref = state.repos.get(&entity_type);

                match repo_ref {
                    Some(repo) => {
                        repo.send_message(RepoMsg::ProcessEvent(event, persist))?;
                    }
                    None => {
                        warn!("No repository found for event type: {}", event.item_type());
                    }
                }

                Ok(())
            }
            RepoManagerMsg::RepoInitComplete(entity_name) => {
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
                    if let Err(err) = state.server.send_message(ServerMsg::AllInitComplete) {
                        error!("Failed to send AllModulesRegistered message: {}", err);
                    }
                };
                Ok(())
            }
            RepoManagerMsg::InitAll(config) => {
                info!("Initializing all repositories: {}", state.repos.len());
                for (_, repo_ref) in &state.repos {
                    let _ = repo_ref.send_message(RepoMsg::Init(config.clone()));
                }
                Ok(())
            }
        }
    }
}
