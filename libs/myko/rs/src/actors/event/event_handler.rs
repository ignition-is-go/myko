//! Per-entity-type event handler actor.
//!
//! Each [`EventHandler`] is responsible for a single entity type and:
//! - Maintains in-memory state of all entities of that type
//! - Persists events to Kafka (when configured)
//! - Parses incoming JSON into strongly-typed items
//! - Notifies [`QueryManager`](crate::actors::query::query_manager::QueryManager) of updates
//! - Provides internal query interface for [`RelationshipManager`](crate::actors::relationship::RelationshipManager)
//!
//! # Initialization
//!
//! When [`Init`](EventHandlerMessage::Init) is called:
//!
//! 1. **With Kafka**: Spawns a [`KafkaConsumer`](crate::actors::kafka::consumer::KafkaConsumer)
//!    and [`KafkaProducer`](crate::actors::kafka::producer::KafkaProducer). The consumer replays
//!    all events from the topic to rebuild in-memory state.
//!
//! 2. **In-memory only**: Immediately signals [`PersisterCaughtUp`](EventHandlerMessage::PersisterCaughtUp)
//!    since there's no history to replay.
//!
//! # Performance
//!
//! EventHandler sends updates directly to QueryManager (bypassing Server) for lower latency.
//! This is part of the "direct actor reference" optimization pattern.

use super::common::ProcessEventData;
use crate::{
    actors::{
        event::{common::PersistEvent, event_manager::EventManagerMsg},
        kafka::{
            common::KafkaSharedConfig,
            consumer::{spawn_kafka_consumer, KafkaConsumerArgs},
            producer::{KafkaProducer, KafkaProducerArgs, KafkaProducerMsg, ProduceEventData},
        },
        query::{common::ProcessUpdateData, query_manager::QueryManagerMsg},
    },
    parsers::item::MykoItemParser,
    prelude::AnyItem,
    runtime::{Actor, ActorHandle, ActorRef, RpcReplyPort},
    server::MykoServerCtx,
};
use log::{error, trace};
use std::{collections::BTreeMap, sync::Arc};

/// Actor that handles events for a single entity type.
///
/// One EventHandler exists per registered entity type (e.g., `Target`, `Scene`, `Binding`).
/// It maintains the authoritative in-memory state for that type and handles persistence.
pub struct EventHandler {
    /// Name of the entity type this handler manages (e.g., "Target")
    entity_name: Arc<str>,
    /// Direct reference to EventManager for reporting init completion
    event_manager: ActorRef<EventManagerMsg>,
    /// Direct reference to QueryManager for update routing (bypasses Server bottleneck)
    query_manager: ActorRef<QueryManagerMsg>,
    /// Server context with host ID
    ctx: Arc<MykoServerCtx>,
    /// Kafka producer for persisting events (None in in-memory mode)
    kafka_producer: Option<ActorRef<KafkaProducerMsg>>,
    /// Parser for converting JSON to strongly-typed items
    parser: Arc<dyn MykoItemParser>,
    /// In-memory store of all items, keyed by ID
    store: BTreeMap<Arc<str>, Arc<dyn AnyItem>>,
    /// Self-reference for sending messages to ourselves
    myself: Option<ActorRef<EventHandlerMessage>>,
}

/// Messages handled by the EventHandler actor.
pub enum EventHandlerMessage {
    /// Process an incoming event (SET or DEL).
    /// Updates in-memory store, persists to Kafka if configured, notifies QueryManager.
    ProcessEvent(ProcessEventData),

    /// Initialize the handler with optional Kafka configuration.
    /// - `Some(config)`: Spawn Kafka consumer/producer, replay history
    /// - `None`: In-memory only, signal ready immediately
    Init(Option<KafkaSharedConfig>),

    /// Notification from KafkaConsumer that it has caught up with the topic.
    /// Triggers [`RepoInitComplete`](crate::actors::event::event_manager::EventManagerMsg::RepoInitComplete)
    /// notification to EventManager.
    PersisterCaughtUp,

    /// Get the full in-memory state for this entity type.
    /// Used for debugging and diagnostics.
    GetState(RpcReplyPort<BTreeMap<Arc<str>, Arc<dyn AnyItem>>>),

    // ─────────────────────────────────────────────────────────────────────────
    // Internal query messages for RelationshipManager cascade operations.
    // Field names are JSON camelCase (e.g., "scopeId" not "scope_id").
    // ─────────────────────────────────────────────────────────────────────────

    /// Query items by field equality. Used by RelationshipManager to find
    /// children with a specific foreign key value.
    QueryByField {
        /// JSON field name (camelCase)
        field: String,
        /// Value to match against
        value: String,
        /// Reply channel for matching items (includes parsed items for cascade events)
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    },

    /// Query items where an array field contains a specific value.
    /// Used by RelationshipManager to find parents that own a specific child.
    QueryArrayContains {
        /// JSON field name (camelCase) pointing to an array
        field: String,
        /// Value to search for in the array
        value: String,
        /// Reply channel for matching items (includes parsed items for cascade events)
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    },

    /// Get items by their IDs.
    /// Used by RelationshipManager for batch fetches during cascade operations.
    GetByIds {
        /// List of item IDs to fetch
        ids: Vec<Arc<str>>,
        /// Reply channel for found items (includes parsed items for cascade events)
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    },

    /// Get all items of this entity type.
    /// Used by RelationshipManager for orphan cleanup on startup.
    GetAllItems(RpcReplyPort<Vec<Arc<dyn AnyItem>>>),

    /// Set self-reference (called immediately after spawn)
    SetMyself(ActorRef<EventHandlerMessage>),

    /// Set Kafka producer reference (called after Kafka producer is spawned)
    SetKafkaProducer(ActorRef<KafkaProducerMsg>),
}

impl std::fmt::Debug for EventHandlerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventHandlerMessage::ProcessEvent(_) => write!(f, "ProcessEvent"),
            EventHandlerMessage::Init(_) => write!(f, "Init"),
            EventHandlerMessage::PersisterCaughtUp => write!(f, "PersisterCaughtUp"),
            EventHandlerMessage::GetState(_) => write!(f, "GetState"),
            EventHandlerMessage::QueryByField { field, value, .. } => {
                write!(f, "QueryByField({}, {})", field, value)
            }
            EventHandlerMessage::QueryArrayContains { field, value, .. } => {
                write!(f, "QueryArrayContains({}, {})", field, value)
            }
            EventHandlerMessage::GetByIds { ids, .. } => {
                write!(f, "GetByIds({} ids)", ids.len())
            }
            EventHandlerMessage::GetAllItems(_) => write!(f, "GetAllItems"),
            EventHandlerMessage::SetMyself(_) => write!(f, "SetMyself"),
            EventHandlerMessage::SetKafkaProducer(_) => write!(f, "SetKafkaProducer"),
        }
    }
}

/// Arguments for spawning an EventHandler actor.
pub struct EventHandlerArgs {
    /// Name of the entity type to handle
    pub entity_name: Arc<str>,
    /// Direct reference to EventManager for init callbacks
    pub event_manager: ActorRef<EventManagerMsg>,
    /// Direct reference to QueryManager for update routing (performance optimization)
    pub query_manager: ActorRef<QueryManagerMsg>,
    /// Server context with host ID and configuration
    pub ctx: Arc<MykoServerCtx>,
    /// Parser for converting JSON to strongly-typed items
    pub parser: Arc<dyn MykoItemParser>,
}

impl EventHandler {
    /// Create a new EventHandler with the given arguments.
    pub fn new(args: EventHandlerArgs) -> Self {
        trace!("Creating Repo: {}", args.entity_name);

        Self {
            entity_name: args.entity_name,
            event_manager: args.event_manager,
            query_manager: args.query_manager,
            ctx: args.ctx,
            parser: args.parser,
            kafka_producer: None,
            store: BTreeMap::new(),
            myself: None,
        }
    }

    /// Spawn the EventHandler on a dedicated thread.
    pub fn spawn(args: EventHandlerArgs) -> ActorHandle<EventHandlerMessage> {
        let actor = Self::new(args);
        let handle = crate::runtime::spawn::spawn(actor);

        // Set self-reference
        let actor_ref = handle.actor_ref();
        let _ = actor_ref.send(EventHandlerMessage::SetMyself(actor_ref.clone()));

        handle
    }

    fn handle_init(&mut self, conf: Option<KafkaSharedConfig>) {
        let entity_name = self.entity_name.clone();
        trace!("{}: Init", entity_name);

        match conf {
            Some(conf) => {
                // With Kafka: spawn consumer and producer
                let myself = self.myself.clone();
                let ctx = self.ctx.clone();
                let entity_name_clone = entity_name.clone();

                // Spawn Kafka consumer (background thread with tokio runtime)
                if let Some(myself_ref) = &myself {
                    spawn_kafka_consumer(KafkaConsumerArgs {
                        topic: entity_name_clone.clone(),
                        shared_conf: conf.clone(),
                        repo_ref: myself_ref.clone(),
                        ctx: ctx.clone(),
                    });
                }

                // Spawn Kafka producer on a background thread (it creates its own tokio runtime)
                let myself_for_producer = myself.clone();
                std::thread::spawn(move || {
                    let producer_handle = KafkaProducer::spawn(KafkaProducerArgs {
                        shared_conf: conf,
                        topic: entity_name_clone,
                    });

                    if let Some(myself_ref) = myself_for_producer {
                        let _ = myself_ref
                            .send(EventHandlerMessage::SetKafkaProducer(producer_handle.actor_ref()));
                    }
                });
            }
            None => {
                // In-memory mode: signal caught up immediately
                if let Some(myself) = &self.myself {
                    let _ = myself.send(EventHandlerMessage::PersisterCaughtUp);
                }
            }
        }
    }

    fn handle_persister_caught_up(&self) {
        trace!(
            "{} Init Complete: {} entities",
            self.entity_name,
            self.store.len()
        );

        if let Err(err) = self
            .event_manager
            .send(EventManagerMsg::RepoInitComplete(self.entity_name.clone()))
        {
            error!("Failed to notify repo manager: {}", err);
        }
    }

    fn handle_process_event(&mut self, data: ProcessEventData) {
        let change_type = data.event.change_type();

        // Use pre-parsed item if available (local events), otherwise parse from JSON
        let item = match data.parsed_item {
            Some(item) => item,
            None => {
                let item_json = data.event.item_json();
                match self.parser.parse(item_json.clone()) {
                    Ok(item) => item,
                    Err(err) => {
                        error!(
                            "{}: Failed to parse item: {} \n {:?}",
                            self.entity_name, err, item_json
                        );
                        return;
                    }
                }
            }
        };

        if let Some(kafka_producer) = &self.kafka_producer
            && let PersistEvent::Persist = data.persist
        {
            let mut event = data.event.clone();
            event.source_id = Some(self.ctx.host_id.to_string());

            let produce_res = kafka_producer.send(KafkaProducerMsg::ProduceEvent(
                ProduceEventData {
                    event,
                    key: item.id().clone(),
                },
            ));

            if let Err(err) = produce_res {
                error!("Failed to produce event: {}", err);
            }
        }

        match change_type {
            crate::event::MEventType::DEL => {
                trace!(
                    "{}: Processing DEL for item {}",
                    self.entity_name,
                    item.id()
                );
                self.store.remove(&item.id());

                // Direct send to QueryManager (bypasses Server actor)
                if let Err(err) = self.query_manager.send(QueryManagerMsg::ProcessUpdate(
                    ProcessUpdateData::Del(item.id().clone()),
                    self.entity_name.clone(),
                )) {
                    error!("Failed to send message to query manager: {}", err);
                };
            }
            crate::event::MEventType::SET => {
                self.store.insert(item.id(), item.clone());

                // Direct send to QueryManager (bypasses Server actor)
                if let Err(err) = self.query_manager.send(QueryManagerMsg::ProcessUpdate(
                    ProcessUpdateData::Set(item.clone()),
                    self.entity_name.clone(),
                )) {
                    error!("Failed to send message to query manager: {}", err);
                };
            }
        }
    }

    fn handle_get_state(&self, reply: RpcReplyPort<BTreeMap<Arc<str>, Arc<dyn AnyItem>>>) {
        if let Err(err) = reply.send(self.store.clone()) {
            error!("Unable to reply with store state: {:?}", err);
        };
    }

    fn handle_query_by_field(
        &self,
        field: String,
        value: String,
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    ) {
        let results: Vec<Arc<dyn AnyItem>> = self
            .store
            .values()
            .filter(|item| {
                let json = item.to_value();
                // Check if the field matches the value
                if let Some(field_value) = json.get(&field) {
                    // Compare as string (handles both string and numeric values)
                    match field_value {
                        serde_json::Value::String(s) => s == &value,
                        serde_json::Value::Number(n) => n.to_string() == value,
                        _ => field_value.to_string().trim_matches('"') == value,
                    }
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        if let Err(err) = reply.send(results) {
            error!("Unable to reply with query results: {:?}", err);
        }
    }

    fn handle_query_array_contains(
        &self,
        field: String,
        value: String,
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    ) {
        let results: Vec<Arc<dyn AnyItem>> = self
            .store
            .values()
            .filter(|item| {
                let json = item.to_value();
                // Check if the array field contains the value
                if let Some(serde_json::Value::Array(arr)) = json.get(&field) {
                    arr.iter().any(|v| match v {
                        serde_json::Value::String(s) => s == &value,
                        serde_json::Value::Number(n) => n.to_string() == value,
                        _ => v.to_string().trim_matches('"') == value,
                    })
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        if let Err(err) = reply.send(results) {
            error!("Unable to reply with query results: {:?}", err);
        }
    }

    fn handle_get_by_ids(
        &self,
        ids: Vec<Arc<str>>,
        reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>,
    ) {
        let results: Vec<Arc<dyn AnyItem>> = ids
            .iter()
            .filter_map(|id| self.store.get(id).cloned())
            .collect();

        if let Err(err) = reply.send(results) {
            error!("Unable to reply with items: {:?}", err);
        }
    }

    fn handle_get_all_items(&self, reply: RpcReplyPort<Vec<Arc<dyn AnyItem>>>) {
        let results: Vec<Arc<dyn AnyItem>> = self.store.values().cloned().collect();

        if let Err(err) = reply.send(results) {
            error!("Unable to reply with all items: {:?}", err);
        }
    }
}

impl Actor for EventHandler {
    type Msg = EventHandlerMessage;

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            EventHandlerMessage::Init(conf) => {
                self.handle_init(conf);
            }
            EventHandlerMessage::PersisterCaughtUp => {
                self.handle_persister_caught_up();
            }
            EventHandlerMessage::ProcessEvent(data) => {
                self.handle_process_event(data);
            }
            EventHandlerMessage::GetState(reply) => {
                self.handle_get_state(reply);
            }
            EventHandlerMessage::QueryByField { field, value, reply } => {
                self.handle_query_by_field(field, value, reply);
            }
            EventHandlerMessage::QueryArrayContains { field, value, reply } => {
                self.handle_query_array_contains(field, value, reply);
            }
            EventHandlerMessage::GetByIds { ids, reply } => {
                self.handle_get_by_ids(ids, reply);
            }
            EventHandlerMessage::GetAllItems(reply) => {
                self.handle_get_all_items(reply);
            }
            EventHandlerMessage::SetMyself(myself) => {
                self.myself = Some(myself);
            }
            EventHandlerMessage::SetKafkaProducer(producer) => {
                self.kafka_producer = Some(producer);
            }
        }
    }
}
