use futures_signals::signal::Mutable;
use futures_util::{future::select_all, SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;
use tungstenite::Message;

#[derive(Clone, Debug)]
pub enum SocketConnectionStatus {
    Disconnected,
    Connecting(String, CancellationToken),
    Connected(String, CancellationToken),
}

pub struct AutoReconnectSocket {
    pub status: Mutable<SocketConnectionStatus>,
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
            status: Mutable::new(SocketConnectionStatus::Disconnected),
            incoming: tokio::sync::broadcast::channel(1000).0,
            outgoing: tokio::sync::broadcast::channel(1000).0,
        }
    }

    pub fn set_addr(&self, addr: Option<String>) {
        let lock = self.status.lock_ref();
        let s = lock.clone();
        drop(lock);

        match s {
            SocketConnectionStatus::Connected(current_addr, teardown)
            | SocketConnectionStatus::Connecting(current_addr, teardown) => {
                if Some(current_addr) == addr {
                    return;
                }

                teardown.cancel();

                self.status.set(SocketConnectionStatus::Disconnected);
                if let Some(addr) = addr {
                    self.build(addr);
                }
            }

            SocketConnectionStatus::Disconnected => {
                if let Some(addr) = addr {
                    self.build(addr);
                }
            }
        }
    }

    pub fn build(&self, addr: String) {
        println!("Building Connection to {}", addr);

        let lock = self.status.lock_ref();
        let s = lock.clone();
        drop(lock);

        match s {
            SocketConnectionStatus::Connected(_, _token) => {
                unreachable!("Should not be building when already connected");
            }
            SocketConnectionStatus::Connecting(_, _token) => {
                unreachable!("Should not be building when already connected");
            }
            SocketConnectionStatus::Disconnected => (),
        }

        let teardown = CancellationToken::new();

        let send = self.outgoing.clone();
        let recv = self.incoming.clone();

        let status = self.status.clone();

        tokio::spawn(async move {
            loop {
                println!("Connecting to {}", addr);
                let ws_stream = match connect_async(&addr).await {
                    Ok((ws_stream, _)) => ws_stream,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                println!("Connected to {}", addr);

                let (mut write, mut read) = ws_stream.split();
                let interior_cancel = CancellationToken::new();

                let int_send_cancel = interior_cancel.clone();
                let rec_send_cancel = teardown.clone();

                if teardown.is_cancelled() {
                    break;
                }

                let mut local_send = send.subscribe();
                let write_handle = tokio::spawn(async move {
                    loop {
                        if int_send_cancel.is_cancelled() || rec_send_cancel.is_cancelled() {
                            eprintln!("Exiting Write Loop");
                            break;
                        }

                        let msg = match local_send.recv().await {
                            Ok(msg) => msg,
                            Err(e) => {
                                eprintln!("Error receiving message to send: {:?}", e);
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
                    println!("Websocket Write Loop Exited");
                });

                let rec_read_cancel = teardown.clone();
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

                let s = SocketConnectionStatus::Connected(addr.clone(), teardown.clone());

                status.set(s);

                let _ = select_all(vec![write_handle, read_handle]).await;

                println!("Read and/or Write Exited - Reconnecting in 1s");

                let s = SocketConnectionStatus::Disconnected;

                status.set(s);

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}
