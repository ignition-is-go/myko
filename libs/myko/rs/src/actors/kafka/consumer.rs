use crate::{
    actors::{
        event::{
            common::{PersistEvent, ProcessEventData},
            event_handler::EventHandlerMessage,
        },
        kafka::common::KafkaSharedConfig,
    },
    event::MEvent,
    server::MykoServerCtx,
};
use log::{debug, error};
use ractor::{Actor, ActorRef};
use rdkafka::{
    ClientConfig, Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
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
    pub repo_ref: ActorRef<EventHandlerMessage>,
    pub ctx: Arc<MykoServerCtx>,
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
            ctx,
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
                debug!("{}: Created Kafka topic", topic);
            }
            Err(err) => {
                panic!("{}: Failed to create Kafka topic: {}", topic, err);
            }
        };

        match consumer.subscribe(&[&topic.clone()]) {
            Ok(_) => {
                debug!("{}: Consumer Subscribed", topic);
            }
            Err(err) => {
                panic!("{}: Failed to subscribe: {}", topic, err);
            }
        };

        let watermarks =
            consumer.fetch_watermarks(&topic, 0, Timeout::After(Duration::from_secs(5)));

        if let Err(err) = watermarks {
            panic!("{}: failed to fetch watermarks: {}", topic, err);
        }

        let (_, high_water) = watermarks.expect("it has the watermarks");

        let repo_ref_clone = repo_ref.clone();

        let host_id_string = ctx.host_id.to_string();

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
                                let my_event = event
                                    .source_id
                                    .clone()
                                    .is_some_and(|id| id == host_id_string);

                                if !my_event {
                                    match repo_ref.send_message(EventHandlerMessage::ProcessEvent(
                                        ProcessEventData {
                                            event,
                                            persist: PersistEvent::NoPersist,
                                        },
                                    )) {
                                        Ok(_) => {}
                                        Err(err) => {
                                            error!("Error sending event message: {}", err);
                                        }
                                    };
                                } else {
                                    // debug!(
                                    //     "Not Processing Events from this server - already processed"
                                    // )
                                }
                            }
                            Err(err) => {
                                error!("Invalid message received: {}", err);
                            }
                        }

                        let caught_up = offset == high_water - 1 || high_water == 0;
                        if caught_up {
                            match repo_ref.send_message(EventHandlerMessage::PersisterCaughtUp) {
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
            debug!("{}: High water is 0, caught up immediately", topic);
            match repo_ref_clone.send_message(EventHandlerMessage::PersisterCaughtUp) {
                Ok(_) => {}
                Err(err) => error!("Error sending caught up message: {}", err),
            };
        }

        Ok(KafkaConsumerState {})
    }
}
