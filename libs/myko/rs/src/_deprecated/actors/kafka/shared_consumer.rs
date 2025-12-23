//! Shared Kafka consumer for all entity types.
//!
//! Instead of spawning one consumer per entity type, this shared consumer
//! subscribes to all entity topics and routes messages to the appropriate
//! EventHandler, drastically reducing file descriptor usage.

use crate::{
    actors::{
        event::{
            common::{PersistEvent, ProcessEventData},
            event_handler::EventHandlerMessage,
        },
        kafka::common::KafkaSharedConfig,
    },
    event::MEvent,
    runtime::ActorRef,
    server::MykoServerCtx,
};
use log::{error, info, trace, warn};
use rdkafka::{
    ClientConfig, Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
    consumer::{Consumer, StreamConsumer},
    util::Timeout,
};
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

/// Message to register a new topic handler with the shared consumer
pub struct RegisterTopicHandler {
    pub topic: Arc<str>,
    pub handler: ActorRef<EventHandlerMessage>,
}

/// Arguments for spawning the SharedKafkaConsumer
pub struct SharedKafkaConsumerArgs {
    pub shared_conf: KafkaSharedConfig,
    pub ctx: Arc<MykoServerCtx>,
    /// Receiver for topic handler registrations
    pub handler_rx: mpsc::Receiver<RegisterTopicHandler>,
}

/// Spawn a shared Kafka consumer that handles all entity topics.
///
/// This is a long-running background task that:
/// 1. Listens for topic handler registrations
/// 2. Subscribes to registered topics
/// 3. Routes messages to appropriate EventHandlers
/// 4. Tracks high water marks per topic for catch-up signaling
pub fn spawn_shared_kafka_consumer(args: SharedKafkaConsumerArgs) {
    let SharedKafkaConsumerArgs {
        shared_conf,
        ctx,
        handler_rx,
    } = args;

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime for shared consumer");

        rt.block_on(async move {
            let bootstrap_servers = shared_conf.bootstrap_servers.join(",");

            // Create consumer with unique group ID
            let consumer: StreamConsumer = ClientConfig::new()
                .set("group.id", format!("myko-shared-{}", uuid::Uuid::new_v4()))
                .set("bootstrap.servers", &bootstrap_servers)
                .set("auto.offset.reset", "earliest")
                .set("allow.auto.create.topics", "true")
                .create()
                .expect("Failed to create shared Kafka consumer");

            // Create admin client for topic creation
            let admin_client: AdminClient<_> = AdminClient::from_config(
                ClientConfig::new().set("bootstrap.servers", &bootstrap_servers),
            )
            .expect("Failed to create Kafka admin client");

            info!("SharedKafkaConsumer started with bootstrap servers: {}", bootstrap_servers);

            // Map of topic -> handler
            let mut handlers: HashMap<Arc<str>, ActorRef<EventHandlerMessage>> = HashMap::new();
            // Track which topics we've subscribed to
            let mut subscribed_topics: Vec<String> = Vec::new();
            // Track high water marks per topic
            let mut high_water_marks: HashMap<Arc<str>, i64> = HashMap::new();
            // Track current offsets per topic (for progress reporting)
            let mut current_offsets: HashMap<Arc<str>, i64> = HashMap::new();
            // Track whether each topic has signaled caught up
            let mut caught_up: HashMap<Arc<str>, bool> = HashMap::new();
            // Track messages processed per topic
            let mut messages_processed: HashMap<Arc<str>, u64> = HashMap::new();
            // For periodic progress logging
            let mut last_progress_log = Instant::now();
            let progress_log_interval = Duration::from_secs(5);

            let host_id_string = ctx.host_id.to_string();

            loop {
                // Periodic progress logging
                if last_progress_log.elapsed() >= progress_log_interval {
                    let total_topics = handlers.len();
                    let caught_up_count = caught_up.values().filter(|&&v| v).count();
                    let pending_count = total_topics - caught_up_count;

                    if pending_count > 0 {
                        // Calculate total progress
                        let mut total_remaining: i64 = 0;
                        let mut slowest_topics: Vec<(String, i64, i64, u64)> = Vec::new();

                        for (topic, &high_water) in &high_water_marks {
                            if !caught_up.get(topic).copied().unwrap_or(true) {
                                let current = current_offsets.get(topic).copied().unwrap_or(0);
                                let remaining = (high_water - current).max(0);
                                let processed = messages_processed.get(topic).copied().unwrap_or(0);
                                total_remaining += remaining;
                                slowest_topics.push((topic.to_string(), current, high_water, processed));
                            }
                        }

                        // Sort by remaining (descending)
                        slowest_topics.sort_by(|a, b| (b.2 - b.1).cmp(&(a.2 - a.1)));

                        info!(
                            "Kafka init progress: {}/{} topics caught up, {} messages remaining",
                            caught_up_count, total_topics, total_remaining
                        );

                        // Show top 5 slowest topics
                        for (topic, current, high_water, processed) in slowest_topics.iter().take(5) {
                            let pct = if *high_water > 0 {
                                ((*current as f64 / *high_water as f64) * 100.0) as u32
                            } else {
                                100
                            };
                            info!(
                                "  {} : {}/{} ({}%) - {} processed",
                                topic, current, high_water, pct, processed
                            );
                        }
                    }

                    last_progress_log = Instant::now();
                }

                // Check for new handler registrations (non-blocking)
                while let Ok(registration) = handler_rx.try_recv() {
                    let topic = registration.topic.clone();
                    info!("Registering handler for topic: {}", topic);

                    // Create topic if needed
                    let _ = admin_client
                        .create_topics(
                            &[NewTopic {
                                num_partitions: 1,
                                replication: TopicReplication::Fixed(3),
                                config: vec![("retention.ms", "-1"), ("cleanup.policy", "compact")],
                                name: &topic,
                            }],
                            &AdminOptions::new(),
                        )
                        .await;

                    handlers.insert(topic.clone(), registration.handler);
                    caught_up.insert(topic.clone(), false);

                    // Add to subscription list if not already there
                    let topic_string = topic.to_string();
                    if !subscribed_topics.contains(&topic_string) {
                        subscribed_topics.push(topic_string);

                        // Resubscribe with updated topic list
                        let topic_refs: Vec<&str> = subscribed_topics.iter().map(|s| s.as_str()).collect();
                        if let Err(err) = consumer.subscribe(&topic_refs) {
                            error!("Failed to subscribe to topics: {}", err);
                        } else {
                            trace!("Subscribed to {} topics", subscribed_topics.len());
                        }

                        // Fetch high water mark for the new topic
                        match consumer.fetch_watermarks(&topic, 0, Timeout::After(Duration::from_secs(5))) {
                            Ok((low_water, high_water)) => {
                                high_water_marks.insert(topic.clone(), high_water);
                                info!("{}: Watermarks low={} high={}", topic, low_water, high_water);

                                // If topic is empty, signal caught up immediately
                                if high_water == 0 {
                                    info!("{}: Empty topic, caught up immediately", topic);
                                    if let Some(handler) = handlers.get(&topic) {
                                        let _ = handler.send(EventHandlerMessage::PersisterCaughtUp);
                                    }
                                    caught_up.insert(topic, true);
                                }
                            }
                            Err(err) => {
                                warn!("{}: Failed to fetch watermarks: {}", topic, err);
                                // Assume empty topic
                                high_water_marks.insert(topic.clone(), 0);
                                if let Some(handler) = handlers.get(&topic) {
                                    let _ = handler.send(EventHandlerMessage::PersisterCaughtUp);
                                }
                                caught_up.insert(topic, true);
                            }
                        }
                    }
                }

                // If no topics registered yet, sleep briefly and continue
                if handlers.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }

                // Poll for messages with timeout
                match tokio::time::timeout(
                    Duration::from_millis(100),
                    consumer.recv(),
                ).await {
                    Ok(Ok(message)) => {
                        let topic = Arc::<str>::from(message.topic());
                        let offset = message.offset();

                        // Track current offset and message count
                        current_offsets.insert(topic.clone(), offset);
                        *messages_processed.entry(topic.clone()).or_insert(0) += 1;

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
                                let my_event = event
                                    .source_id
                                    .clone()
                                    .is_some_and(|id| id == host_id_string);

                                if !my_event
                                    && let Some(handler) = handlers.get(&topic)
                                {
                                    let _ = handler.send(EventHandlerMessage::ProcessEvent(
                                        ProcessEventData {
                                            event,
                                            persist: PersistEvent::NoPersist,
                                            parsed_item: None,
                                            client_id: None,
                                        },
                                    ));
                                }
                            }
                            Err(err) => {
                                error!("Invalid message received on {}: {}", topic, err);
                            }
                        }

                        // Check if this topic has caught up
                        if !caught_up.get(&topic).copied().unwrap_or(true) {
                            let high_water = high_water_marks.get(&topic).copied().unwrap_or(0);
                            if offset >= high_water - 1 || high_water == 0 {
                                info!("{}: Caught up at offset {}/{}", topic, offset, high_water);
                                if let Some(handler) = handlers.get(&topic) {
                                    let _ = handler.send(EventHandlerMessage::PersisterCaughtUp);
                                }
                                caught_up.insert(topic, true);
                            }
                        }
                    }
                    Ok(Err(err)) => {
                        error!("Error receiving message: {:?}", err);
                    }
                    Err(_) => {
                        // Timeout - continue to check for new registrations
                    }
                }
            }
        });
    });
}

/// Handle for registering topics with the shared consumer
#[derive(Clone)]
pub struct SharedConsumerHandle {
    sender: mpsc::Sender<RegisterTopicHandler>,
}

impl SharedConsumerHandle {
    /// Create a new handle and receiver pair
    pub fn new() -> (Self, mpsc::Receiver<RegisterTopicHandler>) {
        let (sender, receiver) = mpsc::channel();
        (Self { sender }, receiver)
    }

    /// Register a topic handler
    pub fn register(&self, topic: Arc<str>, handler: ActorRef<EventHandlerMessage>) {
        let _ = self.sender.send(RegisterTopicHandler { topic, handler });
    }
}

impl Default for SharedConsumerHandle {
    fn default() -> Self {
        Self::new().0
    }
}
