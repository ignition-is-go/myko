use super::common::ProcessEventData;
use crate::{
    actors::{
        event::event_handler::{EventHandler, EventHandlerArgs, EventHandlerMessage},
        kafka::common::KafkaSharedConfig,
        server::ServerMsg,
    },
    parsers::item::MykoItemParser,
    server::MykoServerCtx,
};
use log::{debug, error, info, warn};
use ractor::{Actor, ActorRef, RpcReplyPort};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct EventManager;

pub struct EventManagerState {
    handlers: HashMap<Arc<str>, ActorRef<EventHandlerMessage>>,
    left_to_init: HashSet<Arc<str>>,
    server: ActorRef<ServerMsg>,
    ctx: Arc<MykoServerCtx>,
}

pub enum EventManagerMsg {
    RegisterRepo(Arc<str>, Arc<dyn MykoItemParser>),
    InitAll(KafkaSharedConfig),
    RepoInitComplete(Arc<str>),
    ProcessEvent(ProcessEventData), //bool for persist
    GetEventHandler(Arc<str>, RpcReplyPort<ActorRef<EventHandlerMessage>>),
}

pub struct EventManagerArgs {
    pub server: ActorRef<ServerMsg>,
    pub ctx: Arc<MykoServerCtx>,
}

impl Actor for EventManager {
    type Msg = EventManagerMsg;

    type State = EventManagerState;

    type Arguments = EventManagerArgs;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("Creating RepoManager");
        Ok(EventManagerState {
            left_to_init: HashSet::new(),
            handlers: HashMap::new(),
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
            EventManagerMsg::RegisterRepo(entity_name, parser) => {
                debug!("Registering repository: {}", entity_name);

                state.left_to_init.insert(entity_name.clone());
                if state.handlers.contains_key(&entity_name) {
                    error!("Entity already exists");
                    return Ok(());
                }

                let (handler_ref, _) = match Actor::spawn(
                    None,
                    EventHandler,
                    EventHandlerArgs {
                        server: state.server.clone(),
                        entity_name: entity_name.clone(),
                        ctx: state.ctx.clone(),
                        parser,
                    },
                )
                .await
                {
                    Ok((handler_ref, handler_handle)) => (handler_ref, handler_handle),
                    Err(err) => {
                        error!("Failed to spawn repository: {}", err);
                        return Err(err.into());
                    }
                };

                state.handlers.insert(entity_name.clone(), handler_ref);
                info!("Registered repository: {}", entity_name);
                Ok(())
            }

            EventManagerMsg::RepoInitComplete(entity_name) => {
                state.left_to_init.remove(&entity_name);
                let left_to_init = state.left_to_init.len();
                let total_repos = state.handlers.len();
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
            EventManagerMsg::InitAll(config) => {
                info!("Initializing all repositories: {}", state.handlers.len());
                for handler in state.handlers.values() {
                    let _ = handler.send_message(EventHandlerMessage::Init(config));
                }
                Ok(())
            }
            EventManagerMsg::ProcessEvent(data) => {
                let entity_type: Arc<str> = data.event.item_type().into();

                let handler = state.handlers.get(&entity_type);

                match handler {
                    Some(handler) => {
                        handler.send_message(EventHandlerMessage::ProcessEvent(data))?;
                    }
                    None => {
                        warn!(
                            "No repository found for event type: {}",
                            data.event.item_type()
                        );
                    }
                }

                Ok(())
            }
            EventManagerMsg::GetEventHandler(entity_name, reply) => {
                let handler = state
                    .handlers
                    .get(&entity_name)
                    .ok_or(anyhow::Error::msg("Handler not found"))?;

                if let Err(err) = reply.send(handler.clone()) {
                    error!("Failed to reply with Event Handler: {}", err)
                };
                Ok(())
            }
        }
    }
}
