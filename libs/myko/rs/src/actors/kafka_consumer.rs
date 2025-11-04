use crate::{
    actors::{kafka_common::KafkaSharedConfig, repo::RepoMsg},
    event::MEvent,
};
use log::{debug, error};
use ractor::{Actor, ActorRef};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    util::Timeout,
};
use std::{sync::Arc, time::Duration};

pub struct KafkaConsumer;

pub enum KafkaConsumerMsg {}

pub struct KafkaConsumerState {}

pub struct KafkaConsumerArgs {
    pub topic: Arc<str>,
    pub shared_conf: KafkaSharedConfig,
    pub repo_ref: ActorRef<RepoMsg>,
}

impl Actor for KafkaConsumer {
    type Msg = KafkaConsumerMsg;

    type State = KafkaConsumerState;

    type Arguments = KafkaConsumerArgs;
    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        let KafkaConsumerArgs {
            topic,
            shared_conf,
            repo_ref,
        } = args;

        let consumer: StreamConsumer = match ClientConfig::new()
            .set("group.id", uuid::Uuid::new_v4().to_string())
            .set("bootstrap.servers", shared_conf.bootstrap_servers.join(","))
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "true")
            .create()
        {
            Ok(consumer) => consumer,
            Err(err) => {
                error!("Failed to create Kafka consumer: {}", err);
                return Err(ractor::ActorProcessingErr::from(String::from(
                    "Could not create Kafka consumer",
                )));
            }
        };

        let watermarks =
            consumer.fetch_watermarks(&topic, 0, Timeout::After(Duration::from_secs(5)));

        if let Err(err) = watermarks {
            error!("Failed to fetch watermarks for topic {}: {}", topic, err);
            return Err(ractor::ActorProcessingErr::from(String::from(
                "Could not fetch watermarks",
            )));
        }

        let (_, high_water) = watermarks.expect("it has the watermarks");

        match consumer.subscribe(&[&topic.clone()]) {
            Ok(_) => {
                debug!("Consumer Subscribed to topic {}", topic);
            }
            Err(err) => {
                error!("Failed to subscribe to topic: {}", err);
                return Err(ractor::ActorProcessingErr::from(String::from(
                    "Could not subscribe to topic",
                )));
            }
        };

        let repo_ref_clone = repo_ref.clone();

        tokio::spawn(async move {
            loop {
                match consumer.recv().await {
                    Ok(message) => {
                        let offset = message.offset();

                        let str = match message.payload() {
                            Some(payload) => std::str::from_utf8(payload),
                            None => {
                                error!("Received message with no payload");
                                continue;
                            }
                        };

                        if let Err(err) = str {
                            error!("Error decoding message payload: {}", err);
                            continue;
                        }

                        let str = str.expect("not utf=8");

                        let event = MEvent::from_str_trim(str);

                        match event {
                            Ok(event) => {
                                match repo_ref.send_message(RepoMsg::ProcessEvent(event)) {
                                    Ok(_) => {}
                                    Err(err) => {
                                        error!("Error sending event message: {}", err);
                                    }
                                };
                            }
                            Err(err) => {
                                error!("Invalid message received: {}", err);
                            }
                        }

                        let caught_up = offset == high_water - 1 || high_water == 0;
                        if caught_up {
                            match repo_ref.send_message(RepoMsg::PersisterCaughtUp) {
                                Ok(_) => {}
                                Err(err) => error!("Error sending caught up message: {}", err),
                            };
                        }
                    }
                    Err(e) => error!("Error receiving message: {:?}", e),
                }
            }
        });

        if high_water == 0 {
            debug!("High water is 0, caught up immediately");
            match repo_ref_clone.send_message(RepoMsg::PersisterCaughtUp) {
                Ok(_) => {}
                Err(err) => error!("Error sending caught up message: {}", err),
            };
        }

        Ok(KafkaConsumerState {})
    }
}
