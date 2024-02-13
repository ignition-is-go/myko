use futures_util::stream::StreamExt;
use myko_wasm::event::MEvent;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::{ClientConfig, FromClientConfig};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct KafkaClient {
    brokers: String,
    producer: FutureProducer,
    topic: &'static str,
}

impl KafkaClient {
    pub async fn new(brokers: &str, topic: &'static str) -> KafkaClient {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .expect("Producer creation error");

        let mut client_config = ClientConfig::new();

        client_config.set("bootstrap.servers", brokers);

        let admin =
            AdminClient::from_config(&client_config).expect("Could not create admin client");

        admin
            .create_topics(
                vec![&NewTopic::new(topic, 1, TopicReplication::Fixed(3))].into_iter(),
                &AdminOptions::default(),
            )
            .await
            .expect("Error Creating Topic");

        KafkaClient {
            producer,
            brokers: brokers.to_string(),
            topic,
        }
    }

    pub async fn append_event(&self, event: &MEvent) {
        let event_data = serde_json::to_string(event).expect("Event serialization failed");
        let record = FutureRecord::<String, String>::to(self.topic).payload(&event_data);
        let _ = self
            .producer
            .send::<String, String, Timeout>(record, Timeout::Never)
            .await;
        // Error handling and confirmation of event append can be added here.
    }

    pub async fn consume_events(&self, from_kafka_tx: mpsc::Sender<MEvent>) {
        let consumer: StreamConsumer =
            create_kafka_consumer(&self.brokers, Uuid::new_v4().to_string().as_str());

        // Start consuming in a separate async task

        let topic = self.topic;
        tokio::spawn(async move {
            consume_events(consumer, from_kafka_tx, topic).await;
        });
    }
}

async fn consume_events(
    consumer: StreamConsumer,
    from_kafka_tx: mpsc::Sender<MEvent>,
    topic: &'static str,
) {
    println!("Kafka consumer started for {}", topic);
    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");

    let mut message_stream = consumer.stream();

    while let Some(message) = message_stream.next().await {
        match message {
            Ok(m) => {
                if let Some(payload) = m.payload() {
                    let payload_str = std::str::from_utf8(payload).unwrap();

                    let event = match MEvent::from_str(payload_str) {
                        Ok(event) => event,
                        Err(_e) => {
                            continue;
                        }
                    };

                    from_kafka_tx.send(event).await.unwrap();

                    // Process the message here
                }

                // Committing offsets to Kafka
                consumer
                    .commit_message(&m, rdkafka::consumer::CommitMode::Async)
                    .unwrap();
            }
            Err(e) => eprintln!("Kafka error: {}", e),
        }
    }
}

fn create_kafka_consumer(brokers: &str, group_id: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed")
}
