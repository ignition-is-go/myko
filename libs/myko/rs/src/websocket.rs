use futures_util::{SinkExt, StreamExt};
use std::{sync::Arc, time::Duration};

use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tungstenite::Message;

#[derive(Clone)]
pub enum SocketConnectionStatus {
    Disconnected,
    Connecting(String, CancellationToken),
    Connected(String, CancellationToken),
}

pub struct AutoReconnectSocket {
    status: Arc<Mutex<SocketConnectionStatus>>,
    pub status_tx: tokio::sync::broadcast::Sender<SocketConnectionStatus>,
    pub incoming: tokio::sync::broadcast::Sender<Message>,
    pub outgoing: tokio::sync::broadcast::Sender<Message>,
}

impl Default for AutoReconnectSocket {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoReconnectSocket {
    pub fn new() -> Self {
        Self {
            status: Arc::new(Mutex::new(SocketConnectionStatus::Disconnected)),
            status_tx: tokio::sync::broadcast::channel(1).0,
            incoming: tokio::sync::broadcast::channel(1).0,
            outgoing: tokio::sync::broadcast::channel(1).0,
        }
    }

    pub async fn set_addr(&self, addr: String) {
        let prev_status = self.status.lock().await.clone();

        match prev_status {
            SocketConnectionStatus::Connected(current_addr, current_reconnect) => {
                if current_addr == addr {
                    return;
                }

                current_reconnect.cancel();

                if self
                    .status_tx
                    .send(SocketConnectionStatus::Disconnected)
                    .is_err()
                {
                    println!("Could not send status update");
                }

                *self.status.lock().await = SocketConnectionStatus::Disconnected;

                self.build(addr).await;
            }

            SocketConnectionStatus::Connecting(current_addr, current_reconnect) => {
                if current_addr == addr {
                    return;
                }

                current_reconnect.cancel();

                if self
                    .status_tx
                    .send(SocketConnectionStatus::Disconnected)
                    .is_err()
                {
                    println!("Could not send status update");
                }
                *self.status.lock().await = SocketConnectionStatus::Disconnected;

                self.build(addr).await;
            }
            SocketConnectionStatus::Disconnected => {
                self.build(addr).await;
            }
        }
    }

    pub async fn build(&self, addr: String) {
        match self.status.lock().await.clone() {
            SocketConnectionStatus::Connected(_, _token) => {
                unreachable!("Should not be building when already connected");
            }
            SocketConnectionStatus::Connecting(_, _token) => {
                unreachable!("Should not be building when already connected");
            }
            SocketConnectionStatus::Disconnected => (),
        }

        let reconnect_token = CancellationToken::new();

        let s = SocketConnectionStatus::Connecting(addr.clone(), reconnect_token.clone());

        if self.status_tx.send(s.clone()).is_err() {
            println!("Could not send status update");
        }
        *self.status.lock().await = s;

        let send = self.outgoing.clone();
        let recv = self.incoming.clone();

        let status_sender = self.status_tx.clone();
        let status = self.status.clone();

        tokio::spawn(async move {
            loop {
                let ws_stream = match connect_async(&addr).await {
                    Ok((ws_stream, _)) => ws_stream,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                let s = SocketConnectionStatus::Connected(addr.clone(), reconnect_token.clone());

                if status_sender.send(s.clone()).is_err() {
                    println!("Could not send status update:128");
                }
                *status.lock().await = s;

                let (mut write, mut read) = ws_stream.split();
                let interior_cancel = CancellationToken::new();

                let int_send_cancel = interior_cancel.clone();
                let rec_send_cancel = reconnect_token.clone();

                if reconnect_token.is_cancelled() {
                    break;
                }

                let mut local_send = send.subscribe();

                let write_handle = tokio::spawn(async move {
                    loop {
                        if int_send_cancel.is_cancelled() || rec_send_cancel.is_cancelled() {
                            break;
                        }

                        let msg = match local_send.try_recv() {
                            Ok(msg) => msg,
                            Err(_) => {
                                continue;
                            }
                        };

                        match write.send(msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                int_send_cancel.cancel();
                                println!("Websocket write failed: {:?}", e);
                            }
                        }
                    }
                });

                let rec_read_cancel = reconnect_token.clone();
                let int_read_cancel = interior_cancel.clone();

                let local_recv = recv.clone();

                let read_handle = tokio::spawn(async move {
                    while let (Some(Ok(msg)), false, false) = (
                        read.next().await,
                        rec_read_cancel.is_cancelled(),
                        int_read_cancel.is_cancelled(),
                    ) {
                        match local_recv.send(msg) {
                            Ok(_num) => {
                                // println!("Sent Message Downstream to {} Listeners", num);
                            }
                            Err(_e) => {
                                println!("No Downstream Listeners");
                            }
                        }
                    }

                    println!("Websocket Read Failed");
                    int_read_cancel.cancel();
                });

                write_handle.await.expect("msg write failed");
                read_handle.await.expect("msg read failed");

                println!("Read/Write Exited - Reconnecting in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}
