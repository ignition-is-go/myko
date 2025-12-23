//! Kafka producer and consumer for the cell-based server.
//!
//! # Architecture
//!
//! ```text
//! WebSocket ──► CellServerCtx ──► StoreRegistry (Reduce)
//!                   │
//!                   └──► KafkaProducer ──► Kafka ──► other servers
//!
//! KafkaConsumer ──► (filter source_id != host_id) ──► StoreRegistry (Reduce only)
//! ```
//!
//! Events from WebSocket are processed by CellServerCtx:
//! 1. Reduce: update local store
//! 2. Relationships: process cascades
//! 3. Persist: produce to Kafka
//!
//! Events from Kafka consumer:
//! - Filtered: skip events where source_id == host_id (our own events)
//! - Non-self events: Parse → Reduce only (no relationships, no persist)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{error, info, trace, warn};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::event::{MEvent, MEventType};
use crate::store::StoreRegistry;

use super::HandlerRegistry;

/// Kafka configuration.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Kafka bootstrap servers (e.g., ["localhost:9092"])
    pub bootstrap_servers: Vec<String>,
}

impl KafkaConfig {
    /// Create config from KAFKA_BROKERS environment variable.
    pub fn from_env() -> Option<Self> {
        std::env::var("KAFKA_BROKERS").ok().map(|brokers| Self {
            bootstrap_servers: brokers.split(',').map(|s| s.trim().to_string()).collect(),
        })
    }
}

/// Event to be produced to Kafka.
struct ProduceEvent {
    event: MEvent,
}

/// Handle to the Kafka producer, shareable across connections.
///
/// This is a lightweight handle that sends events to the producer
/// via a channel. It is `Clone + Send + Sync`.
#[derive(Clone)]
pub struct KafkaProducerHandle {
    sender: mpsc::UnboundedSender<ProduceEvent>,
    host_id: Uuid,
}

impl KafkaProducerHandle {
    /// Produce an event to Kafka.
    ///
    /// The event's source_id will be set to this server's host_id.
    pub fn produce(&self, mut event: MEvent) {
        // Set source_id to our host_id for deduplication
        if event.source_id.is_none() {
            event.source_id = Some(self.host_id.to_string());
        }

        if let Err(e) = self.sender.send(ProduceEvent { event }) {
            error!("Failed to send event to Kafka producer: {}", e);
        }
    }

    /// Get the host ID associated with this producer.
    pub fn host_id(&self) -> Uuid {
        self.host_id
    }
}

/// Message sent through per-topic channel to the producer.
struct ProduceTask {
    event_json: String,
    key: String,
}

/// Internal state for the Kafka producer.
struct ProducerState {
    /// The rdkafka producer
    producer: FutureProducer,
    /// Admin client for topic creation
    admin_client: AdminClient<rdkafka::client::DefaultClientContext>,
    /// Topics that have been created
    created_topics: HashSet<String>,
    /// Per-topic sender channels
    topic_senders: HashMap<String, mpsc::UnboundedSender<ProduceTask>>,
    /// Runtime handle for spawning per-topic tasks
    rt_handle: tokio::runtime::Handle,
}

impl ProducerState {
    /// Ensure a topic exists, creating it if necessary.
    async fn ensure_topic(&mut self, topic: &str) {
        if self.created_topics.contains(topic) {
            return;
        }

        let topic_owned = topic.to_string();
        match self
            .admin_client
            .create_topics(
                &[NewTopic {
                    num_partitions: 1,
                    replication: TopicReplication::Fixed(3),
                    config: vec![("retention.ms", "-1"), ("cleanup.policy", "compact")],
                    name: &topic_owned,
                }],
                &AdminOptions::new(),
            )
            .await
        {
            Ok(_) => trace!("Created Kafka topic: {}", topic_owned),
            Err(err) => trace!("Topic creation result for {}: {:?}", topic_owned, err),
        }

        self.created_topics.insert(topic.to_string());
    }

    /// Get or create a per-topic sender channel.
    fn get_or_create_sender(&mut self, topic: &str) -> mpsc::UnboundedSender<ProduceTask> {
        if let Some(sender) = self.topic_senders.get(topic) {
            return sender.clone();
        }

        let (tx, mut rx) = mpsc::unbounded_channel::<ProduceTask>();
        let producer = self.producer.clone();
        let topic_name = topic.to_string();

        self.rt_handle.spawn(async move {
            while let Some(task) = rx.recv().await {
                let result = producer
                    .send(
                        rdkafka::producer::FutureRecord {
                            topic: &topic_name,
                            partition: None,
                            payload: Some(&task.event_json),
                            key: Some(&task.key),
                            timestamp: None,
                            headers: None,
                        },
                        Timeout::Never,
                    )
                    .await;

                match result {
                    Ok(_) => trace!("Produced event to Kafka topic {}", topic_name),
                    Err(err) => error!("{}: Failed to produce event: {}", topic_name, err.0),
                }
            }
        });

        self.topic_senders.insert(topic.to_string(), tx.clone());
        tx
    }

    /// Produce an event to Kafka.
    async fn produce(&mut self, event: MEvent) {
        let topic = event.item_type.clone();

        // Get the item ID for the Kafka key
        let key = event
            .item
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let event_json = match serde_json::to_string(&event) {
            Ok(s) => s,
            Err(err) => {
                error!("Failed to serialize event: {}", err);
                return;
            }
        };

        // Ensure topic exists
        self.ensure_topic(&topic).await;

        // Get sender and queue the event
        let sender = self.get_or_create_sender(&topic);
        if let Err(err) = sender.send(ProduceTask { event_json, key }) {
            error!("Failed to queue event for {}: {}", topic, err);
        }
    }
}

/// Kafka producer for the cell-based server.
///
/// Runs in its own thread with its own tokio runtime, independent of the main runtime.
/// Use `handle()` to get a shareable handle for producing events.
pub struct CellKafkaProducer {
    /// Handle for producing events
    handle: KafkaProducerHandle,
    // Note: We don't store the thread handle or runtime here.
    // The thread owns its runtime and runs until the channel closes.
    // This avoids panics when dropping from an async context.
}

impl CellKafkaProducer {
    /// Create a new Kafka producer.
    pub fn new(config: &KafkaConfig, host_id: Uuid) -> Result<Self, rdkafka::error::KafkaError> {
        let bootstrap_servers = config.bootstrap_servers.join(",");

        // Create channel for receiving events
        let (tx, mut rx) = mpsc::unbounded_channel::<ProduceEvent>();

        let handle = KafkaProducerHandle {
            sender: tx,
            host_id,
        };

        info!(
            "CellKafkaProducer created with bootstrap servers: {}",
            bootstrap_servers
        );

        // Spawn dedicated thread with its own runtime
        // The thread owns everything - when the channel closes, it exits and cleans up
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime for Kafka producer");

            let rt_handle = rt.handle().clone();

            rt.block_on(async move {
                let producer: FutureProducer = match ClientConfig::new()
                    .set("bootstrap.servers", &bootstrap_servers)
                    .set("allow.auto.create.topics", "true")
                    .create()
                {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Failed to create Kafka producer: {}", e);
                        return;
                    }
                };

                let admin_client: AdminClient<_> = match AdminClient::from_config(
                    ClientConfig::new().set("bootstrap.servers", &bootstrap_servers),
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to create Kafka admin client: {}", e);
                        return;
                    }
                };

                let mut state = ProducerState {
                    producer,
                    admin_client,
                    created_topics: HashSet::new(),
                    topic_senders: HashMap::new(),
                    rt_handle,
                };

                while let Some(produce_event) = rx.recv().await {
                    state.produce(produce_event.event).await;
                }
            });
        });

        Ok(Self { handle })
    }

    /// Get a shareable handle for producing events.
    pub fn handle(&self) -> KafkaProducerHandle {
        self.handle.clone()
    }
}

/// Message sent to the consumer thread.
enum ConsumerMessage {
    /// Register a topic for consumption
    RegisterTopic(String),
    /// Signal that initial topic registration is complete
    InitialRegistrationComplete,
}

/// Shared state for tracking catch-up status.
#[derive(Debug)]
pub struct CatchUpStatus {
    /// Whether all initial topics have caught up
    caught_up: AtomicBool,
}

impl CatchUpStatus {
    fn new() -> Self {
        Self {
            caught_up: AtomicBool::new(false),
        }
    }

    /// Check if all initial topics have caught up.
    pub fn is_caught_up(&self) -> bool {
        self.caught_up.load(Ordering::SeqCst)
    }

    /// Block until caught up or timeout.
    ///
    /// Returns true if caught up, false if timed out.
    pub fn wait_until_caught_up(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(50);

        while !self.is_caught_up() {
            if start.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(poll_interval);
        }
        true
    }
}

/// Kafka consumer for the cell-based server.
///
/// Spawns a background thread that:
/// 1. Subscribes to all registered entity topics
/// 2. Reads events from Kafka
/// 3. Filters out events from this server (source_id == host_id)
/// 4. Forwards non-self events to EventProcessor
pub struct CellKafkaConsumer {
    /// Sender for registering new topics
    topic_sender: mpsc::UnboundedSender<ConsumerMessage>,
    /// Catch-up status (shared with consumer thread)
    catch_up_status: Arc<CatchUpStatus>,
    /// Handle to the consumer thread
    _handle: std::thread::JoinHandle<()>,
}

impl CellKafkaConsumer {
    /// Start a new Kafka consumer.
    ///
    /// The consumer runs in a background thread and processes events from
    /// all registered topics, forwarding non-self events to stores.
    ///
    /// Kafka events only go through Parse → Reduce (no relationships, no persist).
    pub fn start(
        config: &KafkaConfig,
        host_id: Uuid,
        handler_registry: Arc<HandlerRegistry>,
        registry: Arc<StoreRegistry>,
    ) -> Result<Self, rdkafka::error::KafkaError> {
        let (topic_sender, mut topic_receiver) = mpsc::unbounded_channel::<ConsumerMessage>();
        let catch_up_status = Arc::new(CatchUpStatus::new());
        let catch_up_status_clone = catch_up_status.clone();

        let bootstrap_servers = config.bootstrap_servers.join(",");
        let host_id_string = host_id.to_string();

        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create tokio runtime for Kafka consumer");

            rt.block_on(async move {
                // Create consumer with unique group ID
                let consumer: StreamConsumer = match ClientConfig::new()
                    .set("group.id", format!("myko-cell-{}", uuid::Uuid::new_v4()))
                    .set("bootstrap.servers", &bootstrap_servers)
                    .set("auto.offset.reset", "earliest")
                    .set("allow.auto.create.topics", "true")
                    .create()
                {
                    Ok(c) => c,
                    Err(err) => {
                        error!("Failed to create Kafka consumer: {}", err);
                        return;
                    }
                };

                // Create admin client for topic creation
                let admin_client: AdminClient<_> = match AdminClient::from_config(
                    ClientConfig::new().set("bootstrap.servers", &bootstrap_servers),
                ) {
                    Ok(c) => c,
                    Err(err) => {
                        error!("Failed to create Kafka admin client: {}", err);
                        return;
                    }
                };

                info!(
                    "CellKafkaConsumer started with bootstrap servers: {}",
                    bootstrap_servers
                );

                let mut subscribed_topics: Vec<String> = Vec::new();
                let mut high_water_marks: HashMap<String, i64> = HashMap::new();
                let mut caught_up: HashSet<String> = HashSet::new();
                // Topics registered during initial registration phase
                let mut initial_topics: HashSet<String> = HashSet::new();
                // Whether we're still in initial registration phase
                let mut initial_registration_done = false;

                // Helper to check and signal catch-up
                let check_all_caught_up = |initial_topics: &HashSet<String>,
                                           caught_up: &HashSet<String>,
                                           initial_done: bool,
                                           status: &CatchUpStatus| {
                    if !initial_done {
                        return;
                    }
                    if initial_topics.is_empty() {
                        // No topics registered, consider caught up
                        if !status.is_caught_up() {
                            info!("No Kafka topics registered, marking as caught up");
                            status.caught_up.store(true, Ordering::SeqCst);
                        }
                        return;
                    }
                    // Check if all initial topics are caught up
                    let all_caught_up = initial_topics.iter().all(|t| caught_up.contains(t));
                    if all_caught_up && !status.is_caught_up() {
                        info!(
                            "All {} initial Kafka topics caught up",
                            initial_topics.len()
                        );
                        status.caught_up.store(true, Ordering::SeqCst);
                    }
                };

                loop {
                    // Check for new topic registrations
                    while let Ok(msg) = topic_receiver.try_recv() {
                        match msg {
                            ConsumerMessage::RegisterTopic(topic) => {
                                if subscribed_topics.contains(&topic) {
                                    continue;
                                }

                                info!("Consumer registering topic: {}", topic);

                                // Track as initial topic if registration not done
                                if !initial_registration_done {
                                    initial_topics.insert(topic.clone());
                                }

                                // Create topic if needed
                                let _ = admin_client
                                    .create_topics(
                                        &[NewTopic {
                                            num_partitions: 1,
                                            replication: TopicReplication::Fixed(3),
                                            config: vec![
                                                ("retention.ms", "-1"),
                                                ("cleanup.policy", "compact"),
                                            ],
                                            name: &topic,
                                        }],
                                        &AdminOptions::new(),
                                    )
                                    .await;

                                subscribed_topics.push(topic.clone());

                                // Resubscribe with updated topic list
                                let topic_refs: Vec<&str> =
                                    subscribed_topics.iter().map(|s| s.as_str()).collect();
                                if let Err(err) = consumer.subscribe(&topic_refs) {
                                    error!("Failed to subscribe to topics: {}", err);
                                } else {
                                    trace!("Subscribed to {} topics", subscribed_topics.len());
                                }

                                // Fetch high water mark
                                match consumer.fetch_watermarks(
                                    &topic,
                                    0,
                                    Timeout::After(Duration::from_secs(5)),
                                ) {
                                    Ok((low, high)) => {
                                        high_water_marks.insert(topic.clone(), high);
                                        info!("{}: Watermarks low={} high={}", topic, low, high);

                                        if high == 0 {
                                            info!("{}: Empty topic, caught up immediately", topic);
                                            caught_up.insert(topic);
                                            check_all_caught_up(
                                                &initial_topics,
                                                &caught_up,
                                                initial_registration_done,
                                                &catch_up_status_clone,
                                            );
                                        }
                                    }
                                    Err(err) => {
                                        warn!("{}: Failed to fetch watermarks: {}", topic, err);
                                        // Assume empty
                                        high_water_marks.insert(topic.clone(), 0);
                                        caught_up.insert(topic);
                                        check_all_caught_up(
                                            &initial_topics,
                                            &caught_up,
                                            initial_registration_done,
                                            &catch_up_status_clone,
                                        );
                                    }
                                }
                            }
                            ConsumerMessage::InitialRegistrationComplete => {
                                info!(
                                    "Initial Kafka topic registration complete ({} topics)",
                                    initial_topics.len()
                                );
                                initial_registration_done = true;
                                check_all_caught_up(
                                    &initial_topics,
                                    &caught_up,
                                    initial_registration_done,
                                    &catch_up_status_clone,
                                );
                            }
                        }
                    }

                    // If no topics yet, sleep briefly
                    if subscribed_topics.is_empty() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }

                    // Poll for messages
                    match tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await {
                        Ok(Ok(message)) => {
                            let topic = message.topic().to_string();
                            let offset = message.offset();

                            let payload = match message.payload() {
                                Some(p) => p,
                                None => {
                                    error!("Received message with no payload on topic {}", topic);
                                    continue;
                                }
                            };

                            let str = match std::str::from_utf8(payload) {
                                Ok(s) => s,
                                Err(err) => {
                                    error!("Error decoding message payload: {}", err);
                                    continue;
                                }
                            };

                            match MEvent::from_str_trim(str) {
                                Ok(event) => {
                                    // Skip our own events
                                    let is_my_event = event
                                        .source_id
                                        .as_ref()
                                        .is_some_and(|id| id == &host_id_string);

                                    if !is_my_event {
                                        trace!(
                                            "Processing Kafka event: {} {:?} (from {})",
                                            event.item_type,
                                            event.change_type(),
                                            event.source_id.as_deref().unwrap_or("unknown")
                                        );

                                        // Parse → Reduce (no relationships, no persist)
                                        match event.change_type {
                                            MEventType::SET => {
                                                if let Some(parse) =
                                                    handler_registry.get_item_parser(&event.item_type)
                                                {
                                                    match parse(event.item.clone()) {
                                                        Ok(item) => {
                                                            let store = registry.get_or_create(item.entity_type());
                                                            store.insert(item.id(), item);
                                                        }
                                                        Err(e) => {
                                                            error!(
                                                                "Failed to parse {}: {}",
                                                                event.item_type, e
                                                            );
                                                        }
                                                    }
                                                } else {
                                                    warn!(
                                                        "No parser for entity type: {}",
                                                        event.item_type
                                                    );
                                                }
                                            }
                                            MEventType::DEL => {
                                                // For DEL, just need the ID
                                                if let Some(id) =
                                                    event.item.get("id").and_then(|v| v.as_str())
                                                {
                                                    let store = registry.get_or_create(&event.item_type);
                                                    store.remove(&id.into());
                                                } else {
                                                    error!(
                                                        "DEL event missing id field: {:?}",
                                                        event.item
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!("Invalid message on {}: {}", topic, err);
                                }
                            }

                            // Check if caught up
                            if !caught_up.contains(&topic) {
                                let high_water = high_water_marks.get(&topic).copied().unwrap_or(0);
                                if offset >= high_water - 1 || high_water == 0 {
                                    info!("{}: Caught up at offset {}/{}", topic, offset, high_water);
                                    caught_up.insert(topic.clone());
                                    check_all_caught_up(
                                        &initial_topics,
                                        &caught_up,
                                        initial_registration_done,
                                        &catch_up_status_clone,
                                    );
                                }
                            }
                        }
                        Ok(Err(err)) => {
                            error!("Error receiving message: {:?}", err);
                        }
                        Err(_) => {
                            // Timeout - continue
                        }
                    }
                }
            });
        });

        Ok(Self {
            topic_sender,
            catch_up_status,
            _handle: handle,
        })
    }

    /// Register a topic for consumption.
    ///
    /// Call this for each entity type to consume events for.
    /// The topic name is the entity type name (e.g., "Target", "Scene").
    pub fn register_topic(&self, entity_type: &str) {
        let _ = self
            .topic_sender
            .send(ConsumerMessage::RegisterTopic(entity_type.to_string()));
    }

    /// Register multiple topics at once.
    pub fn register_topics(&self, entity_types: &[&str]) {
        for entity_type in entity_types {
            self.register_topic(entity_type);
        }
    }

    /// Signal that initial topic registration is complete.
    ///
    /// After calling this, topics registered previously become "initial topics"
    /// and the consumer will track when all of them are caught up.
    pub fn finish_initial_registration(&self) {
        let _ = self
            .topic_sender
            .send(ConsumerMessage::InitialRegistrationComplete);
    }

    /// Get the catch-up status.
    pub fn catch_up_status(&self) -> Arc<CatchUpStatus> {
        self.catch_up_status.clone()
    }

    /// Check if all initial topics have caught up.
    pub fn is_caught_up(&self) -> bool {
        self.catch_up_status.is_caught_up()
    }

    /// Block until all initial topics are caught up or timeout.
    ///
    /// Returns true if caught up, false if timed out.
    pub fn wait_until_caught_up(&self, timeout: Duration) -> bool {
        self.catch_up_status.wait_until_caught_up(timeout)
    }
}
