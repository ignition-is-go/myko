use futures_util::{SinkExt, StreamExt};
use myko_wasm::event::MEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug)]
struct Context {
    client_id: Option<String>,
}

impl Context {
    pub fn new() -> Self {
        Self { client_id: None }
    }
}

#[derive(Debug)]
pub struct MykoClient {
    address: Option<String>,
    send_tx: Option<tokio::sync::mpsc::Sender<Message>>,
    recv_rx: Option<tokio::sync::mpsc::Receiver<Message>>,
    send_task: Option<tokio::task::JoinHandle<()>>,
    recv_task: Option<tokio::task::JoinHandle<()>>,
    event_tx: Option<tokio::sync::mpsc::Sender<Message>>,
    context: Option<Arc<Mutex<Context>>>,
}

impl MykoClient {
    pub const fn new() -> MykoClient {
        MykoClient {
            address: None,
            send_tx: None,
            recv_rx: None,
            send_task: None,
            recv_task: None,
            event_tx: None,
            context: None,
        }
    }

    fn init(&mut self) {
        match self.context {
            None => {
                self.context.replace(Arc::new(Mutex::new(Context::new())));
            }
            Some(_) => {}
        }
    }

    pub async fn send_event(&mut self, event: MEvent) {
        match &self.event_tx {
            Some(tx) => {
                tx.send(Message::Text(serde_json::to_string(&event).unwrap()))
                    .await
                    .expect("Could not send event");
            }
            None => {}
        }
    }

    pub async fn set_address(&mut self, addr: &str) -> bool {
        self.init();

        println!("Previous Address: {:?}", self.address);
        println!("Setting Address: {:?}", addr);

        let prev_addr = self.address.clone();

        if addr != prev_addr.unwrap_or_default() {
            self.disconnect().await;
            self.address = Some(addr.to_string());
            return self.connect().await;
        }

        return self.get_connection_status().await;
    }

    pub async fn get_address(&self) -> Option<String> {
        self.address.clone()
    }

    pub async fn get_connection_status(&self) -> bool {
        self.send_tx.is_some() && self.recv_rx.is_some()
    }

    async fn disconnect(&mut self) {
        if self.send_tx.is_none() || self.recv_rx.is_none() {
            return;
        }

        println!("Disconnecting");
        match &self.send_task {
            Some(task) => {
                task.abort();
                println!("Aborting Send");
            }
            None => {
                println!("No Send Task to Aport");
            }
        }

        match &self.recv_task {
            Some(task) => {
                task.abort();
                println!("Aborting Recv");
            }
            None => {
                println!("No Recv Task to Aport");
            }
        }

        self.send_tx = None;
        self.recv_rx = None;
        self.address = None;
        println!("Disconnected");
    }

    async fn connect(&mut self) -> bool {
        self.init();
        let addr = self.address.clone().unwrap();

        println!("Connecting to: {:?}", &addr);

        let ws_stream = match connect_async(&addr).await {
            Ok((ws_stream, _)) => ws_stream,
            Err(e) => {
                println!("Could not connect: {:?}", e);
                return false;
            }
        };

        let (mut write, mut read) = ws_stream.split();

        println!("Connected: {:?}", &addr);

        let (send_tx, mut send_rx) = tokio::sync::mpsc::channel(1);
        let (recv_tx, recv_rx) = tokio::sync::mpsc::channel::<Message>(1);

        self.send_tx = Some(send_tx);
        self.recv_rx = Some(recv_rx);

        let send_task = tokio::spawn(async move {
            loop {
                let msg = match send_rx.recv().await {
                    Some(msg) => msg,
                    None => {
                        println!("Could not receive message");
                        break;
                    }
                };
                write.send(msg).await.expect("Could not send message");
            }
        });

        let ctx = self.context.clone().expect("Context Not Initialized");

        let recv_task = tokio::spawn(async move {
            loop {
                let msg = read
                    .next()
                    .await
                    .expect("Could Not Read Message")
                    .expect("Could Not Read Message");

                process_message(ctx.clone(), msg.clone()).await;
                recv_tx.send(msg).await.unwrap();
            }
        });

        self.send_task = Some(send_task);
        self.recv_task = Some(recv_task);
        return true;
    }
}

async fn process_message(context: Arc<Mutex<Context>>, message: Message) {
    match message {
        Message::Text(content) => {
            let d = serde_json::from_str::<TextMessage>(content.as_str());

            let data = d.expect("did not parse data").data;

            let command = serde_json::from_value::<Command>(data.to_owned());

            process_command(command, &context).await;
        }
        _ => {}
    }
}

async fn process_command(
    command: Result<Command, serde_json::Error>,
    context: &Arc<Mutex<Context>>,
) {
    match command {
        Ok(command) => match command {
            Command::SetId(set_id) => {
                let mut ctx = context.lock().await;
                ctx.client_id = Some(set_id.client_id.to_string().trim().to_string());
            }
        },
        Err(e) => {
            println!("Could not parse command: {:?}", e);
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TextMessage {
    data: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetId {
    client_id: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "commandId", content = "command")]
enum Command {
    #[serde(rename = "client:setId")]
    SetId(SetId),
}
