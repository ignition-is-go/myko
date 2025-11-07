use crate::{
    actors::server::{Server, ServerArgs, ServerMsg},
    item::Eventable,
    query::Query,
};
use anyhow::bail;
use ractor::{Actor, ActorRef};
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

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

                let server = Arc::new(MykoServer {
                    server,
                    notify_end: notify.clone(),
                });

                crate::entities::server::Server::register(&server)?;
                crate::entities::server::GetConnectedServer::register(&server)?;
                crate::entities::server::GetPeerServers::register(&server)?;

                crate::entities::client::Client::register(&server)?;
                crate::entities::client::GetClientsByServerId::register(&server)?;

                Ok(server)
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

#[derive(Debug)]
pub struct MykoServerCtx {
    pub host_id: Uuid,
}
