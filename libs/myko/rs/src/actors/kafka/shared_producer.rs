//! Shared Kafka producer actor for all entity types.
//!
//! Instead of spawning one producer per entity type, this shared producer
//! handles all topics with a single Kafka connection, drastically reducing
//! file descriptor usage.

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
use std::{collections::HashSet, sync::Arc};

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

/// Shared Kafka producer actor.
///
/// Uses a single Kafka connection for all entity types, with lazy topic creation.
pub struct SharedKafkaProducer {
    producer: FutureProducer,
    admin_client: AdminClient<rdkafka::client::DefaultClientContext>,
    rt: tokio::runtime::Runtime,
    /// Topics that have been created (to avoid redundant create calls)
    created_topics: HashSet<Arc<str>>,
}

impl SharedKafkaProducer {
    /// Create a new SharedKafkaProducer (blocking - creates Kafka connections).
    pub fn new(args: SharedKafkaProducerArgs) -> Self {
        // Use multi-threaded runtime so spawned send tasks run in background
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

        Self {
            producer,
            admin_client,
            rt,
            created_topics: HashSet::new(),
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
        self.rt.block_on(async {
            match self
                .admin_client
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

    fn produce_event(&mut self, data: SharedProduceEventData) {
        // Ensure topic exists
        self.ensure_topic(data.topic.clone());

        let event_str = match serde_json::to_string(&data.event) {
            Ok(s) => s,
            Err(err) => {
                error!("{}: Failed to serialize event: {}", data.topic, err);
                return;
            }
        };

        let key = data.key.to_string();
        let topic = data.topic.clone();
        let event = data.event;

        // Fire-and-forget: spawn the send as a background task
        // This allows the actor to process its mailbox without blocking on Kafka
        let producer = self.producer.clone();
        self.rt.spawn(async move {
            let send_res = producer
                .send(
                    rdkafka::producer::FutureRecord {
                        topic: &topic,
                        partition: None,
                        payload: Some(&event_str),
                        key: Some(&key),
                        timestamp: None,
                        headers: None,
                    },
                    Timeout::Never,
                )
                .await;

            match send_res {
                Ok(_) => {
                    trace!(
                        "Persisted {:?} {} to Kafka topic {}",
                        event.change_type,
                        event.item_type,
                        topic
                    );
                }
                Err(err) => {
                    error!("{}: Failed to produce event: {}", topic, err.0);
                }
            }
        });
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
