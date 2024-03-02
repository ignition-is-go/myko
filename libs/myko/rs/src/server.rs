use std::{collections::HashMap, sync::Arc};

use futures_util::{stream::StreamExt, SinkExt};
use myko_wasm::{event::MEvent, query::Query};
use tokio::{
    net::TcpListener,
    sync::{
        broadcast,
        mpsc::{self, Receiver},
        Mutex,
    },
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
    modules_map: Arc<Mutex<HashMap<String, Box<dyn Module + Send>>>>,
}

pub struct ServerConfig {
    pub kafka_brokers: &'static [&'static str],
}

impl Server {
    pub fn new(config: ServerConfig) -> Server {
        Server {
            startup_state: StartupState::Off,
            config,
            modules_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add_modules(mut self, mut modules: Vec<Box<dyn Module + Send>>) -> Server {
        if self.startup_state != StartupState::Off {
            panic!("Cannot add modules after startup");
        }

        let (from_kafka_tx, from_kafka_rx) = mpsc::channel::<MEvent>(100);

        for module in modules.iter_mut() {
            module
                .start_kafka(self.config.kafka_brokers, from_kafka_tx.clone())
                .await;
        }

        self.modules_map = Arc::new(Mutex::new(
            modules.into_iter().map(|m| (m.entity_name(), m)).collect(),
        ));

        self.startup_state = StartupState::HasModules;

        listen_kafka(from_kafka_rx, self.modules_map.clone());

        self
    }

    pub async fn start(mut self) {
        if self.startup_state != StartupState::HasModules
            || self.modules_map.lock().await.is_empty()
        {
            panic!("Cannot start without modules");
        }

        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<Message>(100);

        let mut port = 5156;
        let max_port = 5255;

        loop {
            let address = format!("0.0.0.0:{}", port);

            match TcpListener::bind(&address).await {
                Ok(listener) => {
                    println!("WebSocket server listening on {}", address);
                    while let Ok((stream, _)) = listener.accept().await {
                        let r = self.modules_map.clone();
                        tokio::spawn(handle_connection(
                            stream,
                            r,
                            broadcast_tx.clone(),
                            broadcast_tx.subscribe(),
                        ));
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

fn listen_kafka(
    mut from_kafka_rx: Receiver<MEvent>,
    modules: Arc<Mutex<HashMap<String, Box<dyn Module + Send>>>>,
) {
    tokio::spawn(async move {
        while let Some(event) = from_kafka_rx.recv().await {
            let mut modules = modules.lock().await;

            let module = modules.get_mut(&event.item_type());

            match module {
                Some(module) => {
                    module.process_event(event.clone(), false).await;
                }
                None => {
                    println!("No module found for event type: {}", event.item_type());
                }
            }
        }
    });
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    modules: Arc<Mutex<HashMap<String, Box<dyn Module + Send>>>>,
    broadcast_tx: broadcast::Sender<Message>,
    mut broadcast_rx: broadcast::Receiver<Message>,
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

    let broadcast_ws_tx = to_ws_tx.clone();

    tokio::spawn(async move {
        while let Ok(message) = broadcast_rx.recv().await {
            if let Err(_) = broadcast_ws_tx.send(message).await {
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
                            let mut modules = modules.lock().await;

                            let module = modules.get_mut(&event.item_type());

                            match module {
                                Some(module) => {
                                    module.process_event(event.clone(), true).await;
                                }
                                None => {
                                    println!(
                                        "No module found for event type: {}",
                                        event.item_type()
                                    );
                                }
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

                            let itemType = match query.clone() {
                                Query::Watch(q) => q.item_type.clone(),
                                Query::WatchId(q) => q.item_type.clone(),
                            };

                            let module = modules.get_mut(&itemType);

                            if module.is_none() {
                                println!("No module found for item type: {}", itemType);
                                continue;
                            }

                            let module = module.unwrap();

                            match module.handle_query(query.clone()).await {
                                Some(mut rx) => {
                                    let tx_clone = to_ws_tx.clone();

                                    tokio::spawn(async move {
                                        while let Some(response) = rx.recv().await {
                                            let response_str = match response.to_string() {
                                                Ok(s) => s,
                                                Err(e) => {
                                                    println!(
                                                        "Failed to convert response to string: {}",
                                                        e
                                                    );
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

                            continue;
                        }
                        Err(_e) => {
                            println!("Failed to parse query: {}", _e);
                        }
                    };

                    println!("Received other message, broadcasting to all connections");
                    broadcast_tx.send(message).unwrap();
                }
            }
            Err(e) => {
                println!("Failed to receive message from WebSocket: {}", e);
                break;
            }
        }
    }
}
