//! Central event coordinator for the Myko actor system.
//!
//! The [`EventManager`] is responsible for:
//! - Managing [`EventHandler`] actors, one per entity type
//! - Routing incoming events to the appropriate handler
//! - Tracking initialization progress across all entity types
//! - Broadcasting events to the [`EventBus`] for saga subscribers
//! - Providing internal query interface for [`RelationshipManager`](crate::actors::relationship::RelationshipManager)
//!
//! # Performance Note
//!
//! The EventManager implements the "direct actor reference" optimization pattern.
//! Rather than routing all messages through the Server actor (creating a bottleneck),
//! EventHandlers hold direct references to QueryManager for update notifications.
//! See `libs/myko/rs/OPTIMIZATION.md` for details.

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
use log::{error, info, trace, warn};
use ractor::{Actor, ActorRef, RpcReplyPort};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Central coordinator for entity event handling.
///
/// EventManager is spawned once per server and manages the lifecycle of all
/// [`EventHandler`] actors. It handles:
///
/// - **Registration**: Spawns EventHandlers when entity types are registered via
///   [`RegisterRepo`](EventManagerMsg::RegisterRepo)
/// - **Routing**: Dispatches events to the correct handler based on `item_type`
/// - **Initialization**: Tracks which handlers have caught up with Kafka and signals
///   when all are ready
/// - **Internal queries**: Provides query interface used by RelationshipManager
///   for cascade operations
pub struct EventManager;

/// Internal state for the EventManager actor.
pub struct EventManagerState {
    /// Map of entity type name → EventHandler actor reference
    handlers: HashMap<Arc<str>, ActorRef<EventHandlerMessage>>,
    /// Entity types that haven't finished Kafka catchup yet
    left_to_init: HashSet<Arc<str>>,
    /// Reference to Server for signaling AllInitComplete
    server: ActorRef<ServerMsg>,
    /// Direct reference to QueryManager for EventHandlers (bypasses Server routing)
    query_manager: ActorRef<QueryManagerMsg>,
    /// Reference back to ourselves for EventHandlers to report init completion
    myself: ActorRef<EventManagerMsg>,
    /// Server context with host ID and config
    ctx: Arc<MykoServerCtx>,
    /// Event bus for high-throughput broadcast to sagas (optional for backwards compat)
    event_bus: Option<EventBus>,
}

/// Messages handled by the EventManager actor.
pub enum EventManagerMsg {
    /// Register a new entity type with its parser.
    /// Spawns an [`EventHandler`] actor for this type.
    RegisterRepo(Arc<str>, Arc<dyn MykoItemParser>),

    /// Initialize all registered handlers.
    /// When `kafka_config` is `Some`, handlers spawn Kafka consumers to replay history.
    /// When `None`, handlers run in-memory only and signal ready immediately.
    InitAll(Option<KafkaSharedConfig>),

    /// Notification from an EventHandler that it has caught up with Kafka.
    /// Once all handlers report, EventManager sends `AllInitComplete` to Server.
    RepoInitComplete(Arc<str>),

    /// Process an incoming event (SET or DEL).
    /// Routes to appropriate EventHandler and publishes to EventBus.
    ProcessEvent(ProcessEventData),

    /// Get an EventHandler actor reference by entity type name.
    /// Used by CommandManager to look up handlers for commands.
    GetEventHandler(Arc<str>, RpcReplyPort<ActorRef<EventHandlerMessage>>),

    /// Configure the EventBus for high-throughput event broadcasting to sagas.
    /// Must be called before events are processed if saga support is needed.
    SetEventBus(EventBus),

    /// Get entity counts for all registered types. Used for diagnostics.
    GetAllCounts(RpcReplyPort<Vec<(Arc<str>, usize)>>),

    // ─────────────────────────────────────────────────────────────────────────
    // Internal query messages for RelationshipManager cascade operations.
    // These use tuple variants for compatibility with ractor::call!() macro.
    // ─────────────────────────────────────────────────────────────────────────

    /// Query items by field equality. (entity_type, field_name, value, reply)
    /// Used by RelationshipManager to find children by foreign key.
    QueryByField(Arc<str>, String, String, RpcReplyPort<Vec<serde_json::Value>>),

    /// Query items where array field contains value. (entity_type, field_name, value, reply)
    /// Used by RelationshipManager to find parents that own a child.
    QueryArrayContains(Arc<str>, String, String, RpcReplyPort<Vec<serde_json::Value>>),

    /// Get items by their IDs. (entity_type, ids, reply)
    /// Used by RelationshipManager to fetch items for cascade deletion.
    GetByIds(Arc<str>, Vec<Arc<str>>, RpcReplyPort<Vec<serde_json::Value>>),

    /// Get all items of an entity type. (entity_type, reply)
    /// Used by RelationshipManager for orphan cleanup on startup.
    GetAllItems(Arc<str>, RpcReplyPort<Vec<serde_json::Value>>),
}

/// Arguments for spawning the EventManager actor.
pub struct EventManagerArgs {
    /// Reference to Server actor for lifecycle notifications
    pub server: ActorRef<ServerMsg>,
    /// Direct reference to QueryManager for EventHandlers (performance optimization)
    pub query_manager: ActorRef<QueryManagerMsg>,
    /// Server context with host ID and configuration
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
                trace!("Registering repository: {}", entity_name);

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
            EventManagerMsg::ProcessEvent(mut data) => {
                let entity_type: Arc<str> = data.event.item_type().into();

                // Set source_id to host_id if not already set (local events)
                // Peer-replicated events will already have source_id set
                if data.event.source_id.is_none() {
                    data.event.source_id = Some(state.ctx.host_id.to_string());
                }

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
