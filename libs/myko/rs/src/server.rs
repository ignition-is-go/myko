use std::sync::Arc;

use futures_util::{stream::StreamExt, SinkExt};
use myko_wasm::{
    event::MEvent,
    query::{Query, QueryResponse},
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc, Mutex},
};
use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};

use crate::module::Module;

#[derive(PartialEq)]
enum StartupState {
    Off,
    HasModules,
    Bound,
}

pub struct Server {
    startup_state: StartupState,
    config: ServerConfig,
    modules: Arc<Mutex<Vec<Box<dyn Module + Send>>>>,
}

pub struct ServerConfig {
    // kafka_brokers: String,
}

impl Server {
    pub fn new(config: ServerConfig) -> Server {
        Server {
            startup_state: StartupState::Off,
            config,
            modules: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn add_modules(mut self, modules: Vec<Box<dyn Module + Send>>) -> Server {
        if self.startup_state != StartupState::Off {
            panic!("Cannot add modules after startup");
        }

        self.modules = Arc::new(Mutex::new(modules));

        self.startup_state = StartupState::HasModules;

        self
    }

    pub async fn start(mut self) {
        if self.startup_state != StartupState::HasModules {
            panic!("Cannot start without modules");
        }

        // let (to_kafka_tx, mut to_kafka_rx) = mpsc::channel::<MEvent>(100);

        let mut port = 5156;
        let max_port = 5255;

        // let kafka = KafkaClient::new(&self.config.kafka_brokers).await;

        // tokio::spawn(async move {
        //     kafka.consume_events(from_kafka_tx).await;
        //     while let Some(message) = to_kafka_rx.recv().await {
        //         kafka.append_event(&message).await;
        //     }
        // });

        // let all_events = all_events_tx.clone();
        // let modules = self.modules.clone();
        // tokio::spawn(async move {
        //     let mut modules = modules.lock().await;

        //     for module in modules.iter_mut() {
        //         module.start(all_events.subscribe()).await;
        //     }
        // });

        loop {
            let address = format!("0.0.0.0:{}", port);
            match TcpListener::bind(&address).await {
                Ok(listener) => {
                    println!("WebSocket server listening on {}", address);
                    while let Ok((stream, _)) = listener.accept().await {
                        let r = self.modules.clone();
                        tokio::spawn(handle_connection(stream, r));
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
    // all_events: broadcast::Sender<MEvent>,
    modules: Arc<Mutex<Vec<Box<dyn Module + Send>>>>,
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
                // println!("Received message from WebSocket: {:?}", message);
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
                            // println!("Received event: {:?}", event);

                            let mut modules = modules.lock().await;

                            for module in modules.iter_mut() {
                                module.process_event(event.clone()).await;
                            }

                            continue;
                            // todo!("Process Event to all modules, and continue");
                        }
                        Err(_e) => {}
                    };

                    match Query::from_str(text) {
                        Ok(query) => {
                            println!("Received query: {:?}", query);

                            let mut modules = modules.lock().await;

                            for module in modules.iter_mut() {
                                match module.handle_query(query.clone()).await {
                                    Some(mut rx) => {
                                        let tx_clone = to_ws_tx.clone();

                                        tokio::spawn(async move {
                                            while let Some(response) = rx.recv().await {
                                                let response_str = match response.to_string() {
                                                    Ok(s) => s,
                                                    Err(e) => {
                                                        println!("Failed to convert response to string: {}", e);
                                                        continue;
                                                    }
                                                };

                                                if let Err(_) = tx_clone
                                                    .clone()
                                                    .send(Message::from(response_str))
                                                    .await
                                                {
                                                    break;
                                                }
                                            }
                                        });
                                    }
                                    None => {}
                                };
                            }

                            continue;
                            // todo!("Handle Query, and continue");
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

fn handle_query(
    query: Query,
    to_ws: mpsc::Sender<Message>,
    all_events: &broadcast::Sender<MEvent>,
    // entites: Arc<Mutex<HashMap<String, HashMap<String, Value>>>>,
) {
    println!("handle_query, {:?}", query);

    match query {
        Query::WatchId(query) => {
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
        Query::Watch(query) => {
            let mut rx = all_events.subscribe();

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
