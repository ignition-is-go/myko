//! Kafka producer actor for persisting events to a topic.
//!
//! The KafkaProducer handles async Kafka writes in a sync actor context
//! by running a tokio runtime on its dedicated thread.

use crate::{
    actors::kafka::common::KafkaSharedConfig,
    event::MEvent,
    runtime::{Actor, ActorRef},
};
use log::{error, trace};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
    producer::FutureProducer,
    util::Timeout,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct ProduceEventData {
    pub event: MEvent,
    pub key: Arc<str>,
}

#[derive(Debug)]
pub enum KafkaProducerMsg {
    ProduceEvent(ProduceEventData),
}

pub struct KafkaProducerArgs {
    pub topic: Arc<str>,
    pub shared_conf: KafkaSharedConfig,
}

/// Kafka producer actor.
///
/// Uses a dedicated thread with tokio runtime for async Kafka operations.
pub struct KafkaProducer {
    producer: FutureProducer,
    topic: Arc<str>,
    rt: tokio::runtime::Runtime,
}

impl KafkaProducer {
    /// Create a new KafkaProducer (blocking - creates Kafka connections).
    fn create(args: KafkaProducerArgs) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        let KafkaProducerArgs { topic, shared_conf } = args;

        // Create producer
        let producer: FutureProducer = match ClientConfig::new()
            .set("bootstrap.servers", shared_conf.bootstrap_servers.join(","))
            .set("allow.auto.create.topics", "true")
            .create()
        {
            Ok(producer) => producer,
            Err(err) => {
                panic!("{}: Failed to create Kafka producer: {}", topic, err);
            }
        };

        // Create topic if it doesn't exist (blocking)
        rt.block_on(async {
            let admin_client = match AdminClient::from_config(ClientConfig::new().set(
                "bootstrap.servers",
                shared_conf.bootstrap_servers.join(", "),
            )) {
                Ok(admin_client) => admin_client,
                Err(err) => {
                    panic!("{}: Failed to create Kafka admin client: {}", topic, err);
                }
            };

            match admin_client
                .create_topics(
                    &[NewTopic {
                        num_partitions: 1,
                        replication: TopicReplication::Fixed(3),
                        config: vec![("retention.ms", "-1"), ("cleanup.policy", "compact")],
                        name: &topic.clone(),
                    }],
                    &AdminOptions::new(),
                )
                .await
            {
                Ok(_) => {
                    trace!("{}: Created Kafka topic", topic);
                }
                Err(err) => {
                    panic!("{}: Failed to create Kafka topic: {}", topic, err);
                }
            };
        });

        Self { producer, topic, rt }
    }
}

impl Actor for KafkaProducer {
    type Msg = KafkaProducerMsg;
    type Args = KafkaProducerArgs;

    fn new(args: Self::Args, _myself: ActorRef<Self::Msg>) -> Self {
        Self::create(args)
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            KafkaProducerMsg::ProduceEvent(data) => {
                let event_str = match serde_json::to_string(&data.event) {
                    Ok(s) => s,
                    Err(err) => {
                        error!("{}: Failed to serialize event: {}", self.topic, err);
                        return;
                    }
                };

                let key = data.key.to_string();
                let topic = self.topic.clone();
                let event = data.event;

                // Send asynchronously using our runtime
                let producer = self.producer.clone();
                self.rt.block_on(async move {
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
    }
}

