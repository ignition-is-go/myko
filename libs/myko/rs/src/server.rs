use crate::actors::server::{Server, ServerArgs, ServerMsg};
use anyhow::bail;
use ractor::{Actor, ActorRef};
use std::sync::Arc;
use tokio::sync::Notify;

pub struct MykoServer {
    pub(crate) server: ActorRef<ServerMsg>,
    notify_end: Arc<Notify>,
}

impl MykoServer {
    pub async fn init(args: MykoServerArgs) -> Result<Arc<MykoServer>, anyhow::Error> {
        let notify = Arc::new(Notify::new());
        match Actor::spawn(None, Server, args).await {
            Err(err) => {
                bail!("Failed to start MykoServer: {}", err);
            }

            Ok((server, server_handle)) => {
                let n_clone = notify.clone();
                tokio::spawn(async move {
                    let _ = server_handle.await;
                    n_clone.notify_waiters();
                });

                Ok(Arc::new(MykoServer {
                    server,
                    notify_end: notify.clone(),
                }))
            }
        }
    }

    pub async fn start(&self) -> Result<(), anyhow::Error> {
        self.server.send_message(ServerMsg::InitAllModules)?;

        self.notify_end.notified().await;

        Ok(())
    }
}

pub type MykoServerArgs = ServerArgs;
