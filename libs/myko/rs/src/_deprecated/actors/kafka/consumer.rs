//! Kafka consumer actor for replaying events from a topic.
//!
//! The KafkaConsumer is spawned per entity type and:
//! - Connects to Kafka and subscribes to the entity's topic
//! - Replays all events from the beginning to rebuild state
//! - Signals PersisterCaughtUp when it reaches the high water mark
//! - Continues polling for new events from other servers

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
use log::{error, trace};
use rdkafka::{
    ClientConfig, Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::FromClientConfig,
    consumer::{Consumer, StreamConsumer},
    util::Timeout,
};
use std::{sync::Arc, time::Duration};

/// Arguments for spawning a KafkaConsumer.
pub struct KafkaConsumerArgs {
    pub topic: Arc<str>,
    pub shared_conf: KafkaSharedConfig,
    pub repo_ref: ActorRef<EventHandlerMessage>,
    pub ctx: Arc<MykoServerCtx>,
}

/// Spawn a Kafka consumer that replays events and signals when caught up.
///
/// This is a long-running background task, not a typical actor.
/// It spawns a tokio runtime internally for async Kafka operations.
pub fn spawn_kafka_consumer(args: KafkaConsumerArgs) {
    let KafkaConsumerArgs {
        topic,
        shared_conf,
        repo_ref,
        ctx,
    } = args;

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        rt.block_on(async move {
            // Create consumer
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

            // Create admin client for topic creation
            let admin_client = match AdminClient::from_config(ClientConfig::new().set(
                "bootstrap.servers",
                shared_conf.bootstrap_servers.join(", "),
            )) {
                Ok(admin_client) => admin_client,
                Err(err) => {
                    panic!("{}: Failed to create Kafka admin client: {}", topic, err);
                }
            };

            // Create topic if it doesn't exist
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

            // Subscribe to topic
            match consumer.subscribe(&[&topic.clone()]) {
                Ok(_) => {
                    trace!("{}: Consumer subscribed", topic);
                }
                Err(err) => {
                    panic!("{}: Failed to subscribe: {}", topic, err);
                }
            };

            // Get high water mark to know when we've caught up
            let watermarks =
                consumer.fetch_watermarks(&topic, 0, Timeout::After(Duration::from_secs(5)));

            if let Err(err) = watermarks {
                panic!("{}: failed to fetch watermarks: {}", topic, err);
            }

            let (_, high_water) = watermarks.expect("it has the watermarks");

            let host_id_string = ctx.host_id.to_string();

            // If topic is empty, signal caught up immediately
            if high_water == 0 {
                trace!("{}: High water is 0, caught up immediately", topic);
                if let Err(err) = repo_ref.send_message(EventHandlerMessage::PersisterCaughtUp) {
                    error!("Error sending caught up message: {}", err);
                }
            }

            // Poll for messages
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

                        let str = str.expect("not utf-8");

                        let event = MEvent::from_str_trim(str);

                        match event {
                            Ok(event) => {
                                let my_event = event
                                    .source_id
                                    .clone()
                                    .is_some_and(|id| id == host_id_string);

                                if !my_event
                                    && let Err(err) =
                                        repo_ref.send_message(EventHandlerMessage::ProcessEvent(
                                            ProcessEventData {
                                                event,
                                                persist: PersistEvent::NoPersist,
                                                parsed_item: None, // Kafka events need parsing
                                                client_id: None,   // Kafka events have no client
                                            },
                                        ))
                                {
                                    error!("Error sending event message: {}", err);
                                }
                            }
                            Err(err) => {
                                error!("Invalid message received: {}", err);
                            }
                        }

                        let caught_up = offset == high_water - 1 || high_water == 0;
                        if caught_up
                            && let Err(err) =
                                repo_ref.send_message(EventHandlerMessage::PersisterCaughtUp)
                        {
                            error!("Error sending caught up message: {}", err);
                        }
                    }
                    Err(e) => error!("Error receiving message: {:?}", e),
                }
            }
        });
    });
}

