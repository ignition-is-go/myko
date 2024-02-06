use futures_util::{stream::StreamExt, SinkExt};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc},
};
use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};

use crate::{
    event::MEvent,
    module::Module,
    query::{AllQueries, QueryResponse},
};

#[derive(PartialEq)]
enum StartupState {
    Off,
    HasModules,
    Bound,
}

pub struct Server {
    startup_state: StartupState,
    config: ServerConfig,
}

pub struct ServerConfig {
    kafka_brokers: String,
}

impl Server {
    pub async fn new(config: ServerConfig) -> Server {
        Server {
            startup_state: StartupState::Off,
            config,
        }
    }

    pub fn add_modules(mut self, modules: Vec<impl Module>) -> Server {
        if self.startup_state != StartupState::Off {
            panic!("Cannot add modules after startup");
        }

        self.startup_state = StartupState::HasModules;

        self
    }

    pub async fn start(mut self) {
        if self.startup_state != StartupState::HasModules {
            panic!("Cannot start without modules");
        }

        // let (to_kafka_tx, mut to_kafka_rx) = mpsc::channel::<MEvent>(100);
        let (all_events_tx, _) = broadcast::channel::<MEvent>(100);

        let mut port = 5156;
        let max_port = 5255;

        // let kafka = KafkaClient::new(&self.config.kafka_brokers).await;

        // tokio::spawn(async move {
        //     kafka.consume_events(from_kafka_tx).await;
        //     while let Some(message) = to_kafka_rx.recv().await {
        //         kafka.append_event(&message).await;
        //     }
        // });

        loop {
            let address = format!("0.0.0.0:{}", port);
            match TcpListener::bind(&address).await {
                Ok(listener) => {
                    println!("WebSocket server listening on {}", address);
                    while let Ok((stream, _)) = listener.accept().await {
                        tokio::spawn(handle_connection(stream, all_events_tx.clone()));
                    }
                    self.startup_state = StartupState::Bound;
                    break; // Exit loop if successfully bound
                }
                Err(e) => {
                    println!("Failed to bind to port {}: {}", port, e);
                    port += 1;
                    if port > max_port {
                        eprintln!("Exceeded maximum port limit");
                        return;
                    }
                }
            }
        }
    }
}

// pub async fn main() {
//     let kafka = KafkaClient::new("b0:9094", "check").await;
//     let (from_kafka_tx, mut from_kafka_rx) = mpsc::channel::<MEvent>(100);

//     let kakfa_event_sender = all_events_tx.clone();
//     let mut all_event_receiver = all_events_tx.subscribe();

//     tokio::spawn(async move {
//         while let Ok(event) = all_event_receiver.recv().await {
//             let _item_type = event.item_type();
//             let _item_json = event.item_json();

//             // find the repo with the matching item_type

//             // call repo.process_event(event)
//         }
//     });

//     tokio::spawn(async move {
//         while let Some(msg) = from_kafka_rx.recv().await {
//             match kakfa_event_sender.send(msg.clone()) {
//                 Ok(_) => {}
//                 Err(e) => {
//                     println!("Failed to send event on broadcast channel: {:?}", e);
//                 }
//             };
//         }
//     });
// }

async fn handle_connection(
    stream: tokio::net::TcpStream,
    all_events: broadcast::Sender<MEvent>,
    // entites: Arc<Mutex<HashMap<String, HashMap<String, Value>>>>,
) {
    println!("New WebSocket connection");
    let ws_stream = accept_async(stream)
        .await
        .expect("Error during WebSocket handshake");
    let (mut ws_write, mut ws_read) = ws_stream.split();

    let (to_ws_tx, mut to_ws_rx) = mpsc::channel::<Message>(100);

    tokio::spawn(async move {
        while let Some(message) = to_ws_rx.recv().await {
            if let Err(_) = ws_write.send(message).await {
                println!("Failed to send message to WebSocket");
                break;
            }
        }
    });

    while let Some(message) = ws_read.next().await {
        match message {
            Ok(message) => {
                println!("Received message from WebSocket: {:?}", message);
                if message.is_text() {
                    let text = match message.to_text() {
                        Ok(t) => t,
                        Err(e) => {
                            println!("Failed to convert message into text: {}", e);
                            continue;
                        }
                    };

                    match MEvent::from_str(text) {
                        Ok(event) => {
                            todo!("Process Event to all modules, and continue");
                        }
                        Err(_e) => {}
                    };

                    match AllQueries::from_str(text) {
                        Ok(query) => {
                            println!("Received query: {:?}", query);
                            todo!("Handle Query, and continue");
                            // (self.handle_query(query, to_ws_tx.clone(), &all_events)).await;
                        }
                        Err(_e) => {
                            println!("Failed to parse query: {}", _e);
                        }
                    };
                }
            }
            Err(e) => {
                println!("Failed to receive message from WebSocket: {}", e);
                break;
            }
        }
    }
}

async fn handle_query(
    query: AllQueries,
    to_ws: mpsc::Sender<Message>,
    all_events: &broadcast::Sender<MEvent>,
    // entites: Arc<Mutex<HashMap<String, HashMap<String, Value>>>>,
) {
    println!("handle_query, {:?}", query);

    match query {
        AllQueries::WatchId(query) => {
            let mut rx = all_events.subscribe();

            // let entity_map = entites.lock().await;

            // if let Some(existing) = entity_map.get(&query.item_type) {
            //     if let Some(existing) = existing.get(&query.item_id) {
            //         let response = QueryResponse::new(query.tx.clone(), vec![existing.clone()]);

            //         let reply = Message::Text(response.to_string().unwrap());

            //         to_ws.send(reply).await.unwrap();
            //     }
            // }

            tokio::spawn(async move {
                while let Ok(event) = rx.recv().await {
                    if event.item_type() != query.item_type {
                        continue;
                    }

                    let event_json = event.item_json();

                    let response = QueryResponse::new(query.tx.clone(), vec![event_json.clone()]);

                    let reply = Message::Text(response.to_string().unwrap());

                    if let Err(_) = to_ws.send(reply).await {
                        break;
                    }
                }
            });
        }
    }
}
