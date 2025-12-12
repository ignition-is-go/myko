use crate::{actors::kafka::common::KafkaSharedConfig, event::MEvent};
use log::{error, trace};
use ractor::{Actor, ActorRef};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
    producer::FutureProducer,
    util::Timeout,
};
use std::sync::Arc;

pub struct KafkaProducer;

pub struct ProduceEventData {
    pub event: MEvent,
    pub key: Arc<str>,
}

pub enum KafkaProducerMsg {
    ProduceEvent(ProduceEventData),
}

pub struct KafkaProducerState {
    producer: FutureProducer,
    topic: Arc<str>,
}

pub struct KafkaProducerArgs {
    pub topic: Arc<str>,
    pub shared_conf: KafkaSharedConfig,
}

impl Actor for KafkaProducer {
    type Msg = KafkaProducerMsg;

    type State = KafkaProducerState;

    type Arguments = KafkaProducerArgs;
    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        let KafkaProducerArgs { topic, shared_conf } = args;

        let producer: FutureProducer = match ClientConfig::new()
            .set("bootstrap.servers", shared_conf.bootstrap_servers.join(","))
            .set("allow.auto.create.topics", "true")
            .create()
        {
            Ok(consumer) => consumer,
            Err(err) => {
                panic!("{}: Failed to create Kafka consumer: {}", topic, err);
            }
        };

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

        Ok(KafkaProducerState { producer, topic })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ractor::ActorProcessingErr> {
        match message {
            KafkaProducerMsg::ProduceEvent(data) => {
                let event_str = serde_json::to_string(&data.event);
                let key = data.key.clone().to_string();

                match event_str {
                    Ok(event_str) => {
                        let send_res = state
                            .producer
                            .send(
                                rdkafka::producer::FutureRecord {
                                    topic: &state.topic,
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
                                    data.event.change_type,
                                    data.event.item_type,
                                    state.topic
                                );
                            }
                            Err(err) => {
                                error!("{}: Failed to produce event: {}", state.topic, err.0);
                            }
                        }
                    }
                    Err(err) => {
                        error!("{}: Failed to serialize event: {}", state.topic, err);
                    }
                }
                Ok(())
            }
        }
    }
}
