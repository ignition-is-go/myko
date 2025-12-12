use super::{common::ProcessEventData, EventBus};
use crate::{
    actors::{
        event::event_handler::{EventHandler, EventHandlerArgs, EventHandlerMessage},
        kafka::common::KafkaSharedConfig,
        query::query_manager::QueryManagerMsg,
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
    /// Direct reference to QueryManager for EventHandlers
    query_manager: ActorRef<QueryManagerMsg>,
    /// Reference back to ourselves for EventHandlers to report init completion
    myself: ActorRef<EventManagerMsg>,
    ctx: Arc<MykoServerCtx>,
    /// Event bus for high-throughput broadcast to sagas (optional for backwards compat)
    event_bus: Option<EventBus>,
}

pub enum EventManagerMsg {
    RegisterRepo(Arc<str>, Arc<dyn MykoItemParser>),
    /// Initialize all repositories. When kafka_config is None, runs in-memory only.
    InitAll(Option<KafkaSharedConfig>),
    RepoInitComplete(Arc<str>),
    ProcessEvent(ProcessEventData), //bool for persist
    GetEventHandler(Arc<str>, RpcReplyPort<ActorRef<EventHandlerMessage>>),
    /// Set the EventBus for high-throughput event broadcasting to sagas
    SetEventBus(EventBus),
    /// Get counts for all entity types
    GetAllCounts(RpcReplyPort<Vec<(Arc<str>, usize)>>),

    // Internal query messages for RelationshipManager (tuple variants for ractor::call!)
    /// Query items of a specific entity type by field value (entity_type, field, value, reply)
    QueryByField(Arc<str>, String, String, RpcReplyPort<Vec<serde_json::Value>>),
    /// Query items where an array field contains a value (entity_type, field, value, reply)
    QueryArrayContains(Arc<str>, String, String, RpcReplyPort<Vec<serde_json::Value>>),
    /// Get items by IDs from a specific entity type (entity_type, ids, reply)
    GetByIds(Arc<str>, Vec<Arc<str>>, RpcReplyPort<Vec<serde_json::Value>>),
    /// Get all items of a specific entity type (entity_type, reply)
    GetAllItems(Arc<str>, RpcReplyPort<Vec<serde_json::Value>>),
}

pub struct EventManagerArgs {
    pub server: ActorRef<ServerMsg>,
    /// Direct reference to QueryManager for EventHandlers
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub ctx: Arc<MykoServerCtx>,
}

impl Actor for EventManager {
    type Msg = EventManagerMsg;

    type State = EventManagerState;

    type Arguments = EventManagerArgs;

    async fn pre_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        debug!("Creating RepoManager");
        Ok(EventManagerState {
            left_to_init: HashSet::new(),
            handlers: HashMap::new(),
            server: args.server,
            query_manager: args.query_manager,
            myself,
            ctx: args.ctx,
            event_bus: None,
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
                        entity_name: entity_name.clone(),
                        // Direct references - bypasses Server routing
                        event_manager: state.myself.clone(),
                        query_manager: state.query_manager.clone(),
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
                    let _ = handler.send_message(EventHandlerMessage::Init(config.clone()));
                }
                Ok(())
            }
            EventManagerMsg::ProcessEvent(data) => {
                let entity_type: Arc<str> = data.event.item_type().into();

                // Publish event to EventBus for saga subscribers (high-throughput path)
                if let Some(event_bus) = &state.event_bus {
                    event_bus.publish(data.event.clone());
                }

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
            EventManagerMsg::SetEventBus(event_bus) => {
                info!("EventManager: EventBus configured for saga broadcast");
                state.event_bus = Some(event_bus);
                Ok(())
            }
            EventManagerMsg::GetAllCounts(reply) => {
                let mut counts: Vec<(Arc<str>, usize)> = Vec::with_capacity(state.handlers.len());

                for (entity_name, handler) in &state.handlers {
                    match ractor::call!(handler, EventHandlerMessage::GetState) {
                        Ok(entity_state) => {
                            counts.push((entity_name.clone(), entity_state.len()));
                        }
                        Err(err) => {
                            error!("Failed to get state for {}: {}", entity_name, err);
                            counts.push((entity_name.clone(), 0));
                        }
                    }
                }

                // Sort by entity name for consistent output
                counts.sort_by(|a, b| a.0.cmp(&b.0));

                if let Err(err) = reply.send(counts) {
                    error!("Failed to reply with entity counts: {:?}", err);
                }
                Ok(())
            }
            EventManagerMsg::QueryByField(entity_type, field, value, reply) => {
                if let Some(handler) = state.handlers.get(&entity_type) {
                    // Forward to the specific EventHandler
                    if let Err(err) = handler.send_message(EventHandlerMessage::QueryByField {
                        field,
                        value,
                        reply,
                    }) {
                        error!("Failed to forward QueryByField to {}: {}", entity_type, err);
                    }
                } else {
                    warn!("No handler found for entity type: {}", entity_type);
                    if let Err(err) = reply.send(vec![]) {
                        error!("Failed to send empty response: {}", err);
                    }
                }
                Ok(())
            }
            EventManagerMsg::QueryArrayContains(entity_type, field, value, reply) => {
                if let Some(handler) = state.handlers.get(&entity_type) {
                    if let Err(err) = handler.send_message(EventHandlerMessage::QueryArrayContains {
                        field,
                        value,
                        reply,
                    }) {
                        error!(
                            "Failed to forward QueryArrayContains to {}: {}",
                            entity_type, err
                        );
                    }
                } else {
                    warn!("No handler found for entity type: {}", entity_type);
                    if let Err(err) = reply.send(vec![]) {
                        error!("Failed to send empty response: {}", err);
                    }
                }
                Ok(())
            }
            EventManagerMsg::GetByIds(entity_type, ids, reply) => {
                if let Some(handler) = state.handlers.get(&entity_type) {
                    if let Err(err) =
                        handler.send_message(EventHandlerMessage::GetByIds { ids, reply })
                    {
                        error!("Failed to forward GetByIds to {}: {}", entity_type, err);
                    }
                } else {
                    warn!("No handler found for entity type: {}", entity_type);
                    if let Err(err) = reply.send(vec![]) {
                        error!("Failed to send empty response: {}", err);
                    }
                }
                Ok(())
            }
            EventManagerMsg::GetAllItems(entity_type, reply) => {
                if let Some(handler) = state.handlers.get(&entity_type) {
                    if let Err(err) = handler.send_message(EventHandlerMessage::GetAllItems(reply))
                    {
                        error!("Failed to forward GetAllItems to {}: {}", entity_type, err);
                    }
                } else {
                    warn!("No handler found for entity type: {}", entity_type);
                    if let Err(err) = reply.send(vec![]) {
                        error!("Failed to send empty response: {}", err);
                    }
                }
                Ok(())
            }
        }
    }
}
