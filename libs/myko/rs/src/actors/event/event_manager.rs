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
        kafka::{
            common::KafkaSharedConfig,
            shared_consumer::{SharedConsumerHandle, SharedKafkaConsumerArgs, spawn_shared_kafka_consumer},
            shared_producer::{SharedKafkaProducer, SharedKafkaProducerArgs, SharedKafkaProducerMsg},
        },
        query::query_manager::QueryManagerMsg,
        relationship::RelationshipManagerMsg,
        server::ServerMsg,
    },
    parsers::item::MykoItemParser,
    prelude::AnyItem,
    relationship::iter_client_id_registrations,
    runtime::{Actor, ActorRef, RpcReplyPort},
    server::MykoServerCtx,
};
use log::{error, info, trace, warn};
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
pub struct EventManager {
    /// Map of entity type name → EventHandler actor reference
    handlers: HashMap<Arc<str>, ActorRef<EventHandlerMessage>>,
    /// Entity types that haven't finished Kafka catchup yet
    left_to_init: HashSet<Arc<str>>,
    /// Reference to Server for signaling AllInitComplete
    server: ActorRef<ServerMsg>,
    /// Direct reference to QueryManager for EventHandlers (bypasses Server routing)
    query_manager: ActorRef<QueryManagerMsg>,
    /// Reference to ourselves for passing to EventHandlers
    myself: ActorRef<EventManagerMsg>,
    /// Server context with host ID and config
    ctx: Arc<MykoServerCtx>,
    /// Event bus for high-throughput broadcast to sagas (optional for backwards compat)
    event_bus: Option<EventBus>,
    /// Map of entity type → client_id field name (camelCase) for auto-population
    client_id_fields: HashMap<String, String>,
    /// Optional reference to RelationshipManager for forwarding events
    relationship_manager: Option<ActorRef<RelationshipManagerMsg>>,
    /// Shared Kafka producer (single connection for all entity types)
    shared_kafka_producer: Option<ActorRef<SharedKafkaProducerMsg>>,
    /// Shared Kafka consumer handle for registering topic handlers
    shared_consumer_handle: Option<SharedConsumerHandle>,
}

/// Messages handled by the EventManager actor.
pub enum EventManagerMsg {
    // NOTE: Debug impl below (manual due to non-Debug fields)
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

    /// Set the RelationshipManager for receiving events with parsed items.
    /// Events are forwarded to RelationshipManager after processing.
    SetRelationshipManager(ActorRef<RelationshipManagerMsg>),

    /// Get entity counts for all registered types. Used for diagnostics.
    GetAllCounts(RpcReplyPort<Vec<(Arc<str>, usize)>>),

    // ─────────────────────────────────────────────────────────────────────────
    // Internal query messages for RelationshipManager cascade operations.
    // These use tuple variants for compatibility with ractor::call!() macro.
    // Return Arc<dyn AnyItem> to allow including parsed items in cascade events.
    // ─────────────────────────────────────────────────────────────────────────

    /// Query items by field equality. (entity_type, field_name, value, reply)
    /// Used by RelationshipManager to find children by foreign key.
    QueryByField(Arc<str>, String, String, RpcReplyPort<Vec<Arc<dyn AnyItem>>>),

    /// Query items where array field contains value. (entity_type, field_name, value, reply)
    /// Used by RelationshipManager to find parents that own a child.
    QueryArrayContains(Arc<str>, String, String, RpcReplyPort<Vec<Arc<dyn AnyItem>>>),

    /// Get items by their IDs. (entity_type, ids, reply)
    /// Used by RelationshipManager to fetch items for cascade deletion.
    GetByIds(Arc<str>, Vec<Arc<str>>, RpcReplyPort<Vec<Arc<dyn AnyItem>>>),

    /// Get all items of an entity type. (entity_type, reply)
    /// Used by RelationshipManager for orphan cleanup on startup.
    GetAllItems(Arc<str>, RpcReplyPort<Vec<Arc<dyn AnyItem>>>),

    /// Set the real server reference (called after Server actor is spawned)
    SetServer(ActorRef<ServerMsg>),
}

impl std::fmt::Debug for EventManagerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventManagerMsg::RegisterRepo(name, _) => {
                write!(f, "RegisterRepo({})", name)
            }
            EventManagerMsg::InitAll(_) => write!(f, "InitAll"),
            EventManagerMsg::RepoInitComplete(name) => write!(f, "RepoInitComplete({})", name),
            EventManagerMsg::ProcessEvent(_) => write!(f, "ProcessEvent"),
            EventManagerMsg::GetEventHandler(name, _) => {
                write!(f, "GetEventHandler({})", name)
            }
            EventManagerMsg::SetEventBus(_) => write!(f, "SetEventBus"),
            EventManagerMsg::SetRelationshipManager(_) => write!(f, "SetRelationshipManager"),
            EventManagerMsg::GetAllCounts(_) => write!(f, "GetAllCounts"),
            EventManagerMsg::QueryByField(entity, field, value, _) => {
                write!(f, "QueryByField({}, {}, {})", entity, field, value)
            }
            EventManagerMsg::QueryArrayContains(entity, field, value, _) => {
                write!(f, "QueryArrayContains({}, {}, {})", entity, field, value)
            }
            EventManagerMsg::GetByIds(entity, ids, _) => {
                write!(f, "GetByIds({}, {} ids)", entity, ids.len())
            }
            EventManagerMsg::GetAllItems(entity, _) => write!(f, "GetAllItems({})", entity),
            EventManagerMsg::SetServer(_) => write!(f, "SetServer"),
        }
    }
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

impl EventManager {
    fn handle_register_repo(&mut self, entity_name: Arc<str>, parser: Arc<dyn MykoItemParser>) {
        trace!("Registering repository: {}", entity_name);

        self.left_to_init.insert(entity_name.clone());
        if self.handlers.contains_key(&entity_name) {
            trace!("Entity {} already registered, skipping", entity_name);
            return;
        }

        let handle = EventHandler::spawn(EventHandlerArgs {
            entity_name: entity_name.clone(),
            event_manager: self.myself.clone(),
            query_manager: self.query_manager.clone(),
            ctx: self.ctx.clone(),
            parser,
        });

        self.handlers.insert(entity_name, handle.actor_ref());
    }

    fn handle_repo_init_complete(&mut self, entity_name: Arc<str>) {
        self.left_to_init.remove(&entity_name);
        let left_to_init = self.left_to_init.len();
        let total_repos = self.handlers.len();
        let total_init = total_repos - left_to_init;
        info!(
            "Repository init complete: ({}/{}) {} [{}]",
            total_init,
            total_repos,
            entity_name,
            self.left_to_init
                .iter()
                .take(3)
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        if self.left_to_init.is_empty() {
            info!("All repositories initialized");
            if let Err(err) = self.server.send_message(ServerMsg::AllInitComplete) {
                error!("Failed to send AllModulesRegistered message: {}", err);
            }
        };
    }

    fn handle_init_all(&mut self, config: Option<KafkaSharedConfig>) {
        info!("Initializing all repositories: {}", self.handlers.len());

        // Create shared Kafka actors if Kafka is configured
        if let Some(ref shared_conf) = config {
            // Spawn shared producer (single Kafka connection for all entity types)
            let producer_handle = SharedKafkaProducer::spawn(SharedKafkaProducerArgs {
                shared_conf: shared_conf.clone(),
            });
            self.shared_kafka_producer = Some(producer_handle.actor_ref());

            // Create shared consumer handle and spawn the consumer
            let (consumer_handle, handler_rx) = SharedConsumerHandle::new();
            spawn_shared_kafka_consumer(SharedKafkaConsumerArgs {
                shared_conf: shared_conf.clone(),
                ctx: self.ctx.clone(),
                handler_rx,
            });
            self.shared_consumer_handle = Some(consumer_handle);

            info!("Shared Kafka producer and consumer created");
        }

        // Send init message to all handlers with shared Kafka references
        for handler in self.handlers.values() {
            let _ = handler.send_message(EventHandlerMessage::Init {
                config: config.clone(),
                shared_producer: self.shared_kafka_producer.clone(),
                shared_consumer_handle: self.shared_consumer_handle.clone(),
            });
        }
    }

    fn handle_process_event(&self, mut data: ProcessEventData) {
        let entity_type: Arc<str> = data.event.item_type().into();

        // Set source_id to host_id if not already set (local events)
        // Peer-replicated events will already have source_id set
        if data.event.source_id.is_none() {
            data.event.source_id = Some(self.ctx.host_id.to_string());
        }

        // Auto-populate client_id field if entity has one and client_id is available
        if let Some(field_name) = self.client_id_fields.get(entity_type.as_ref())
            && let Some(client_id) = &data.client_id
            && let serde_json::Value::Object(ref mut obj) = data.event.item
        {
            // Only set if not already set (allows explicit override)
            if !obj.contains_key(field_name)
                || obj.get(field_name) == Some(&serde_json::Value::Null)
            {
                obj.insert(
                    field_name.clone(),
                    serde_json::Value::String(client_id.to_string()),
                );
                trace!(
                    "EventManager: Auto-populated {} on {} with client_id {}",
                    field_name,
                    entity_type,
                    client_id
                );
            }
        }

        // Publish event to EventBus for saga subscribers (high-throughput path)
        if let Some(event_bus) = &self.event_bus {
            event_bus.publish(data.event.clone());
        }

        // Forward to RelationshipManager for cascade handling (with parsed item)
        if let Some(relationship_manager) = &self.relationship_manager {
            let _ =
                relationship_manager.send_message(RelationshipManagerMsg::ProcessEvent(data.clone()));
        }

        let handler = self.handlers.get(&entity_type);

        match handler {
            Some(handler) => {
                if let Err(err) = handler.send_message(EventHandlerMessage::ProcessEvent(data)) {
                    error!("Failed to send event to handler: {}", err);
                }
            }
            None => {
                warn!(
                    "No repository found for event type: {}",
                    data.event.item_type()
                );
            }
        }
    }

    fn handle_get_event_handler(
        &self,
        entity_name: Arc<str>,
        reply: RpcReplyPort<ActorRef<EventHandlerMessage>>,
    ) {
        match self.handlers.get(&entity_name) {
            Some(handler) => {
                if let Err(err) = reply.send(handler.clone()) {
                    error!("Failed to reply with Event Handler: {:?}", err);
                }
            }
            None => {
                error!("Handler not found for entity: {}", entity_name);
            }
        }
    }

    fn handle_get_all_counts(&self, reply: RpcReplyPort<Vec<(Arc<str>, usize)>>) {
        let mut counts: Vec<(Arc<str>, usize)> = Vec::with_capacity(self.handlers.len());

        for (entity_name, handler) in &self.handlers {
            match handler.call(EventHandlerMessage::GetState) {
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
    }

    fn handle_query_by_field(
        &self,
        entity_type: Arc<str>,
        field: String,
        value: String,
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    ) {
        if let Some(handler) = self.handlers.get(&entity_type) {
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
                error!("Failed to send empty response: {:?}", err);
            }
        }
    }

    fn handle_query_array_contains(
        &self,
        entity_type: Arc<str>,
        field: String,
        value: String,
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    ) {
        if let Some(handler) = self.handlers.get(&entity_type) {
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
                error!("Failed to send empty response: {:?}", err);
            }
        }
    }

    fn handle_get_by_ids(
        &self,
        entity_type: Arc<str>,
        ids: Vec<Arc<str>>,
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    ) {
        if let Some(handler) = self.handlers.get(&entity_type) {
            if let Err(err) = handler.send_message(EventHandlerMessage::GetByIds { ids, reply }) {
                error!("Failed to forward GetByIds to {}: {}", entity_type, err);
            }
        } else {
            warn!("No handler found for entity type: {}", entity_type);
            if let Err(err) = reply.send(vec![]) {
                error!("Failed to send empty response: {:?}", err);
            }
        }
    }

    fn handle_get_all_items(
        &self,
        entity_type: Arc<str>,
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    ) {
        if let Some(handler) = self.handlers.get(&entity_type) {
            if let Err(err) = handler.send_message(EventHandlerMessage::GetAllItems(reply)) {
                error!("Failed to forward GetAllItems to {}: {}", entity_type, err);
            }
        } else {
            warn!("No handler found for entity type: {}", entity_type);
            if let Err(err) = reply.send(vec![]) {
                error!("Failed to send empty response: {:?}", err);
            }
        }
    }
}

impl Actor for EventManager {
    type Msg = EventManagerMsg;
    type Args = EventManagerArgs;

    fn new(args: Self::Args, myself: ActorRef<Self::Msg>) -> Self {
        use crate::item::ItemRegistration;

        // Build lookup map for entities with #[myko_client_id] fields
        let client_id_fields: HashMap<String, String> = iter_client_id_registrations()
            .map(|reg| (reg.entity_type.to_string(), reg.field_name_json.to_string()))
            .collect();

        if !client_id_fields.is_empty() {
            info!(
                "EventManager: {} entity types have client_id fields: {:?}",
                client_id_fields.len(),
                client_id_fields.keys().collect::<Vec<_>>()
            );
        }

        let mut manager = Self {
            left_to_init: HashSet::new(),
            handlers: HashMap::new(),
            server: args.server,
            query_manager: args.query_manager,
            myself,
            ctx: args.ctx,
            event_bus: None,
            client_id_fields,
            relationship_manager: None,
            shared_kafka_producer: None,
            shared_consumer_handle: None,
        };

        // Register all entities from inventory
        let mut count = 0;
        for registration in inventory::iter::<ItemRegistration> {
            let data = (registration.factory)();
            manager.handle_register_repo(data.entity_type, data.parser);
            count += 1;
        }
        log::debug!("EventManager: registered {} entities from inventory", count);

        manager
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            EventManagerMsg::RegisterRepo(entity_name, parser) => {
                self.handle_register_repo(entity_name, parser);
            }
            EventManagerMsg::RepoInitComplete(entity_name) => {
                self.handle_repo_init_complete(entity_name);
            }
            EventManagerMsg::InitAll(config) => {
                self.handle_init_all(config);
            }
            EventManagerMsg::ProcessEvent(data) => {
                self.handle_process_event(data);
            }
            EventManagerMsg::GetEventHandler(entity_name, reply) => {
                self.handle_get_event_handler(entity_name, reply);
            }
            EventManagerMsg::SetEventBus(event_bus) => {
                info!("EventManager: EventBus configured for saga broadcast");
                self.event_bus = Some(event_bus);
            }
            EventManagerMsg::SetRelationshipManager(relationship_manager) => {
                info!("EventManager: RelationshipManager configured for cascade forwarding");
                self.relationship_manager = Some(relationship_manager);
            }
            EventManagerMsg::GetAllCounts(reply) => {
                self.handle_get_all_counts(reply);
            }
            EventManagerMsg::QueryByField(entity_type, field, value, reply) => {
                self.handle_query_by_field(entity_type, field, value, reply);
            }
            EventManagerMsg::QueryArrayContains(entity_type, field, value, reply) => {
                self.handle_query_array_contains(entity_type, field, value, reply);
            }
            EventManagerMsg::GetByIds(entity_type, ids, reply) => {
                self.handle_get_by_ids(entity_type, ids, reply);
            }
            EventManagerMsg::GetAllItems(entity_type, reply) => {
                self.handle_get_all_items(entity_type, reply);
            }
            EventManagerMsg::SetServer(server) => {
                self.server = server;
            }
        }
    }
}
