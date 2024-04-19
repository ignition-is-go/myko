use futures_util::{SinkExt, StreamExt};
use myko_wasm::event::{MEvent, MykoMessage};
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
    context: Option<Arc<Mutex<Context>>>,
    downstream_out: Option<tokio::sync::broadcast::Sender<Value>>,
}

// #[derive(Clone)]
// pub struct ConnectionInfo {
//     pub address: String,
//     pub client_id: String,
// }

// #[derive(Clone)]
// pub enum ConnectionStatus {
//     Connected(ConnectionInfo),
//     Disconnected,
// }

// pub struct MykoClientProxy {
//     send: tokio::sync::mpsc::Sender<MykoMessage>,
//     recv: tokio::sync::broadcast::Receiver<Value>,
//     address_change: tokio::sync::mpsc::Sender<&'static str>,
//     connection_status: tokio::sync::broadcast::Receiver<ConnectionStatus>,
// }

// impl Clone for MykoClientProxy {
//     fn clone(&self) -> Self {
//         Self {
//             send: self.send.clone(),
//             recv: self.recv.resubscribe(),
//             address_change: self.address_change.clone(),
//             connection_status: self.connection_status.resubscribe(),
//         }
//     }
// }

// impl MykoClientProxy {
//     pub async fn send_event(&self, event: MEvent) {
//         self.send
//             .send(MykoMessage::Event(event))
//             .await
//             .expect("Could not send event");
//     }

//     pub fn get_messages(&self) -> tokio::sync::broadcast::Receiver<Value> {
//         self.recv.resubscribe()
//     }

//     pub async fn set_address(&self, addr: &'static str) {
//         self.address_change
//             .send(addr)
//             .await
//             .expect("Could not send address change");
//     }
// }

impl MykoClient {
    pub const fn new() -> MykoClient {
        MykoClient {
            address: None,
            send_tx: None,
            recv_rx: None,
            send_task: None,
            recv_task: None,
            context: None,
            downstream_out: None,
        }
    }

    fn init(&mut self) {
        match self.context {
            None => {
                self.context.replace(Arc::new(Mutex::new(Context::new())));
            }
            Some(_) => {}
        };
        match self.downstream_out {
            None => {
                let (tx, rx) = tokio::sync::broadcast::channel(1);
                self.downstream_out = Some(tx);
            }
            Some(_) => {}
        };
    }

    pub async fn send_event(&mut self, event: MEvent) {
        match &self.send_tx {
            Some(tx) => {
                let msg = MykoMessage::Event(event);
                let msg_str = serde_json::to_string(&msg).unwrap();
                tx.send(Message::Text(msg_str.clone()))
                    .await
                    .expect("Could not send event");
            }
            None => {
                println!("Event Tx Not Initialized");
            }
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

    pub fn get_messages(&self) -> tokio::sync::broadcast::Receiver<Value> {
        self.downstream_out
            .clone()
            .expect("downtream not available")
            .subscribe()
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

    pub async fn get_client_id(&self) -> Option<String> {
        match &self.context {
            Some(ctx) => {
                let ctx = ctx.lock().await;
                ctx.client_id.clone()
            }
            None => None,
        }
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
                println!("Sending Message: {:?}", msg);
                write.send(msg).await.expect("Could not send message");
            }
        });

        let ctx = self.context.clone().expect("Context Not Initialized");

        let downstream_out = match &self.downstream_out {
            Some(tx) => tx.clone(),
            None => {
                panic!("Downstream Pub Not Initialized");
            }
        };

        let recv_task = tokio::spawn(async move {
            loop {
                let loop_clone = downstream_out.clone();

                let msg = read
                    .next()
                    .await
                    .expect("Could Not Read Message")
                    .expect("Could Not Read Message");

                process_message(ctx.clone(), msg.clone(), loop_clone).await;
                recv_tx.send(msg).await.unwrap();
            }
        });

        self.send_task = Some(send_task);
        self.recv_task = Some(recv_task);
        return true;
    }
}

async fn process_message(
    context: Arc<Mutex<Context>>,
    message: Message,
    downstream_out: tokio::sync::broadcast::Sender<Value>,
) {
    match message {
        Message::Text(content) => {
            let d = serde_json::from_str::<TextMessage>(content.as_str());

            let data = d.expect("did not parse data").data;
            let _ = downstream_out.send(data.clone());

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
                ctx.client_id = Some(set_id.client_id);
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

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SetId {
    client_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "commandId", content = "command")]
enum Command {
    #[serde(rename = "client:setId")]
    SetId(SetId),
}
