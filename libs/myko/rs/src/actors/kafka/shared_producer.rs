//! Shared Kafka producer actor for all entity types.
//!
//! Instead of spawning one producer per entity type, this shared producer
//! handles all topics with a single Kafka connection, drastically reducing
//! file descriptor usage.
//!
//! Ordering guarantee: Messages to the same topic are sent sequentially.
//! Different topics can send in parallel.

use crate::{
    actors::kafka::common::KafkaSharedConfig,
    event::MEvent,
    runtime::{Actor, ActorHandle},
};
use log::{error, info, trace};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
    producer::FutureProducer,
    util::Timeout,
};
use std::{collections::{HashMap, HashSet}, sync::Arc};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct SharedProduceEventData {
    pub topic: Arc<str>,
    pub event: MEvent,
    pub key: Arc<str>,
}

#[derive(Debug)]
pub enum SharedKafkaProducerMsg {
    /// Produce an event to a specific topic
    ProduceEvent(SharedProduceEventData),
    /// Ensure a topic exists (called during entity registration)
    EnsureTopic(Arc<str>),
}

pub struct SharedKafkaProducerArgs {
    pub shared_conf: KafkaSharedConfig,
}

/// Message sent through per-topic channel
struct TopicSendTask {
    event_str: String,
    key: String,
}

/// Shared Kafka producer actor.
///
/// Uses a single Kafka connection for all entity types, with lazy topic creation.
/// Maintains per-topic ordering by using dedicated channels for each topic.
pub struct SharedKafkaProducer {
    producer: FutureProducer,
    admin_client: AdminClient<rdkafka::client::DefaultClientContext>,
    /// Runtime for async operations - must be kept alive
    _rt: tokio::runtime::Runtime,
    /// Handle for spawning tasks
    rt_handle: tokio::runtime::Handle,
    /// Topics that have been created (to avoid redundant create calls)
    created_topics: HashSet<Arc<str>>,
    /// Per-topic channels for ordered sends
    topic_senders: HashMap<Arc<str>, mpsc::UnboundedSender<TopicSendTask>>,
}

impl SharedKafkaProducer {
    /// Create a new SharedKafkaProducer (blocking - creates Kafka connections).
    pub fn new(args: SharedKafkaProducerArgs) -> Self {
        // Use multi-threaded runtime so per-topic sender tasks run in background
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        let bootstrap_servers = args.shared_conf.bootstrap_servers.join(",");

        // Create producer
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("allow.auto.create.topics", "true")
            .create()
            .expect("Failed to create shared Kafka producer");

        // Create admin client for topic management
        let admin_client: AdminClient<_> = AdminClient::from_config(
            ClientConfig::new().set("bootstrap.servers", &bootstrap_servers),
        )
        .expect("Failed to create Kafka admin client");

        info!("SharedKafkaProducer created with bootstrap servers: {}", bootstrap_servers);

        let rt_handle = rt.handle().clone();
        Self {
            producer,
            admin_client,
            _rt: rt,
            rt_handle,
            created_topics: HashSet::new(),
            topic_senders: HashMap::new(),
        }
    }

    /// Spawn a SharedKafkaProducer on a dedicated thread.
    pub fn spawn(args: SharedKafkaProducerArgs) -> ActorHandle<SharedKafkaProducerMsg> {
        let actor = Self::new(args);
        crate::runtime::spawn::spawn(actor)
    }

    fn ensure_topic(&mut self, topic: Arc<str>) {
        if self.created_topics.contains(&topic) {
            return;
        }

        let topic_clone = topic.clone();
        let admin_client = &self.admin_client;
        self.rt_handle.block_on(async {
            match admin_client
                .create_topics(
                    &[NewTopic {
                        num_partitions: 1,
                        replication: TopicReplication::Fixed(3),
                        config: vec![("retention.ms", "-1"), ("cleanup.policy", "compact")],
                        name: &topic_clone,
                    }],
                    &AdminOptions::new(),
                )
                .await
            {
                Ok(_) => {
                    trace!("Created Kafka topic: {}", topic_clone);
                }
                Err(err) => {
                    // Topic might already exist, which is fine
                    trace!("Topic creation result for {}: {:?}", topic_clone, err);
                }
            }
        });

        self.created_topics.insert(topic);
    }

    /// Get or create a per-topic sender channel.
    /// Each topic gets a dedicated background task that sends messages sequentially.
    fn get_or_create_topic_sender(&mut self, topic: Arc<str>) -> mpsc::UnboundedSender<TopicSendTask> {
        if let Some(sender) = self.topic_senders.get(&topic) {
            return sender.clone();
        }

        // Create new channel and spawn background sender task for this topic
        let (tx, mut rx) = mpsc::unbounded_channel::<TopicSendTask>();
        let producer = self.producer.clone();
        let topic_name = topic.clone();

        self.rt_handle.spawn(async move {
            while let Some(task) = rx.recv().await {
                let send_res = producer
                    .send(
                        rdkafka::producer::FutureRecord {
                            topic: &topic_name,
                            partition: None,
                            payload: Some(&task.event_str),
                            key: Some(&task.key),
                            timestamp: None,
                            headers: None,
                        },
                        Timeout::Never,
                    )
                    .await;

                match send_res {
                    Ok(_) => {
                        trace!("Persisted event to Kafka topic {}", topic_name);
                    }
                    Err(err) => {
                        error!("{}: Failed to produce event: {}", topic_name, err.0);
                    }
                }
            }
        });

        self.topic_senders.insert(topic, tx.clone());
        tx
    }

    fn produce_event(&mut self, data: SharedProduceEventData) {
        // Ensure topic exists (blocking, but only on first use)
        self.ensure_topic(data.topic.clone());

        let event_str = match serde_json::to_string(&data.event) {
            Ok(s) => s,
            Err(err) => {
                error!("{}: Failed to serialize event: {}", data.topic, err);
                return;
            }
        };

        // Get per-topic sender (creates background task if needed)
        let sender = self.get_or_create_topic_sender(data.topic.clone());

        // Send to per-topic channel - maintains ordering within topic
        // Returns immediately, actual send happens in background task
        if let Err(err) = sender.send(TopicSendTask {
            event_str,
            key: data.key.to_string(),
        }) {
            error!("{}: Failed to queue event: {}", data.topic, err);
        }
    }
}

impl Actor for SharedKafkaProducer {
    type Msg = SharedKafkaProducerMsg;

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            SharedKafkaProducerMsg::ProduceEvent(data) => {
                self.produce_event(data);
            }
            SharedKafkaProducerMsg::EnsureTopic(topic) => {
                self.ensure_topic(topic);
            }
        }
    }
}
