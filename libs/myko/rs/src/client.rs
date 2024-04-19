use futures_util::{SinkExt, StreamExt};
use myko_wasm::event::{MEvent, MykoMessage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{
    broadcast::{Receiver, Sender},
    Mutex,
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Clone)]
pub struct ConnectionInfo {
    pub address: String,
    pub client_id: String,
}

#[derive(Clone)]
pub enum ConnectionStatus {
    Client(ConnectionInfo),
    Connected(String),
    Disconnected,
}

#[derive(Clone)]
pub struct MykoClient {
    send_tx: tokio::sync::broadcast::Sender<Outgoing>,
    recv_tx: tokio::sync::broadcast::Sender<Incoming>,
    address_change: tokio::sync::mpsc::Sender<String>,
    connection_status: Arc<Mutex<ConnectionStatus>>,
}

#[derive(Clone, Debug)]
pub struct Outgoing(pub Value);

#[derive(Clone, Debug)]
pub struct Incoming(pub Value);

impl Default for MykoClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MykoClient {
    pub fn new() -> MykoClient {
        let (self_send_tx, _) = tokio::sync::broadcast::channel::<Outgoing>(100);
        let (self_recv_tx, _) = tokio::sync::broadcast::channel::<Incoming>(100);
        let (address_change, mut address_change_rx) = tokio::sync::mpsc::channel::<String>(1);
        let connection = Arc::new(Mutex::new(ConnectionStatus::Disconnected));

        let self_connection = connection.clone();
        let send_tx = self_send_tx.clone();
        let recv_tx = self_recv_tx.clone();

        tokio::spawn(async move {
            let mut send_task: Option<tokio::task::JoinHandle<()>> = None;
            let mut recv_task: Option<tokio::task::JoinHandle<()>> = None;

            while let Some(addr) = address_change_rx.recv().await {
                let connection_state = {
                    let connection = self_connection.lock().await;
                    connection.clone()
                };

                match connection_state {
                    ConnectionStatus::Disconnected => {
                        println!("SEND_TX_SUB");
                        if let Ok((send_handle, recv_handle)) = connect(
                            addr.to_string(),
                            send_tx.subscribe(),
                            recv_tx.clone(),
                            self_connection.clone(),
                        )
                        .await
                        {
                            send_task = Some(send_handle);
                            recv_task = Some(recv_handle);
                        }
                        let mut con = self_connection.lock().await;
                        *con = ConnectionStatus::Connected(addr);
                    }
                    ConnectionStatus::Connected(address) => {
                        if address == addr {
                            return;
                        }
                        if let Some(task) = &send_task {
                            task.abort();
                        }
                        if let Some(task) = &recv_task {
                            task.abort();
                        }
                        println!("SEND_TX_SUB");

                        if let Ok((send_handle, recv_handle)) = connect(
                            addr.to_string(),
                            send_tx.subscribe(),
                            recv_tx.clone(),
                            self_connection.clone(),
                        )
                        .await
                        {
                            send_task = Some(send_handle);
                            recv_task = Some(recv_handle);
                        }
                    }
                    ConnectionStatus::Client(info) => {
                        if info.address == addr {
                            return;
                        }
                        let mut con = self_connection.lock().await;
                        *con = ConnectionStatus::Disconnected;

                        drop(con);
                        if let Some(task) = &send_task {
                            task.abort();
                        }
                        if let Some(task) = &recv_task {
                            task.abort();
                        }
                        println!("SEND_TX_SUB");

                        if let Ok((send_handle, recv_handle)) = connect(
                            addr.to_string(),
                            send_tx.subscribe(),
                            recv_tx.clone(),
                            self_connection.clone(),
                        )
                        .await
                        {
                            send_task = Some(send_handle);
                            recv_task = Some(recv_handle);
                        }
                    }
                }
            }
        });

        MykoClient {
            send_tx: self_send_tx,
            recv_tx: self_recv_tx,
            address_change,
            connection_status: connection,
        }
    }

    pub async fn send_event(&self, event: MEvent) {
        let msg = MykoMessage::Event(event);

        let val = serde_json::to_value(msg).expect("Could not serialize message");

        match self.send_tx.send(Outgoing(val)) {
            Ok(num) => {
                println!("Sent message: {:?}", num);
            }
            Err(e) => {
                println!("Could not send event: {:?}", e);
            }
        }
    }

    pub fn get_messages(&self) -> tokio::sync::broadcast::Receiver<Incoming> {
        self.recv_tx.subscribe()
    }

    pub async fn set_address(&self, addr: String) {
        match self.address_change.send(addr).await {
            Ok(_) => {}
            Err(e) => {
                println!("Could not send address change: {:?}", e);
            }
        }
    }

    pub async fn get_connection_status(&self) -> ConnectionStatus {
        let status = self.connection_status.lock().await;
        status.clone()
    }

    pub async fn get_client_id(&self) -> Option<String> {
        let status = self.connection_status.lock().await;
        match &*status {
            ConnectionStatus::Client(info) => Some(info.client_id.clone()),
            _ => None,
        }
    }
}

async fn connect(
    addr: String,
    mut send: Receiver<Outgoing>,
    recv: Sender<Incoming>,
    conn: Arc<Mutex<ConnectionStatus>>,
) -> Result<(tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>), String> {
    let ws_stream = match connect_async(&addr).await {
        Ok((ws_stream, _)) => ws_stream,
        Err(_) => {
            return Err("Could not connect".to_string());
        }
    };

    let (mut write, mut read) = ws_stream.split();

    println!("Connected: {:?}", &addr);

    let send_task = tokio::spawn(async move {
        loop {
            let msg = match send.try_recv() {
                Ok(msg) => {
                    println!("Received Message for Sending on WS: {:?}", msg.0);
                    msg
                }
                Err(_) => {
                    continue;
                }
            };
            println!("Sending Message to WS");

            let msg_str = serde_json::to_string(&msg.0).expect("Could not serialize message");

            match write.send(Message::Text(msg_str)).await {
                Ok(_) => {
                    println!("Sent Message to WS");
                }
                Err(e) => {
                    println!("Could not write message to ws: {:?}", e);
                }
            }
        }
    });

    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = read.next().await {
            process_message(conn.clone(), msg.clone(), recv.clone()).await;

            let msg = serde_json::from_str::<Value>(msg.to_string().as_str())
                .expect("Could not parse message");

            match recv.send(Incoming(msg)) {
                Ok(_) => {}
                Err(e) => {
                    println!("No Downstream Listeners: {:?}", e);
                }
            }
        }
    });

    Ok((send_task, recv_task))
}
async fn process_message(
    connection: Arc<Mutex<ConnectionStatus>>,
    message: Message,
    downstream_out: tokio::sync::broadcast::Sender<Incoming>,
) {
    if let Message::Text(content) = message {
        let d = serde_json::from_str::<TextMessage>(content.as_str());

        let data = d.expect("did not parse data").data;
        let _ = downstream_out.send(Incoming(data.clone()));

        let command = serde_json::from_value::<Command>(data.to_owned());

        process_command(command, connection).await;
    }
}

async fn process_command(
    command: Result<Command, serde_json::Error>,
    connection: Arc<Mutex<ConnectionStatus>>,
) {
    match command {
        Ok(command) => match command {
            Command::SetId(set_id) => {
                let con_state = connection.lock().await.clone();

                let mut connection = connection.lock().await;

                match con_state {
                    ConnectionStatus::Disconnected => {
                        unreachable!("Received SetId Command, but not connected");
                    }
                    ConnectionStatus::Connected(addr) => {
                        println!("Received Client Id: {:?}", set_id.client_id);
                        *connection = ConnectionStatus::Client(ConnectionInfo {
                            address: addr,
                            client_id: set_id.client_id,
                        });
                    }
                    ConnectionStatus::Client(info) => {
                        println!("Received New Client Id: {:?}", set_id.client_id);
                        *connection = ConnectionStatus::Client(ConnectionInfo {
                            address: info.address,
                            client_id: set_id.client_id,
                        });
                    }
                }
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
