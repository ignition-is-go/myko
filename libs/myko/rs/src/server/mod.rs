use crate::{
    actors::{
        command::command_manager::CommandManagerMsg,
        event::event_manager::EventManagerMsg,
        query::query_manager::QueryManagerMsg,
        report::report_manager::ReportManagerMsg,
        server::{Server, ServerArgs, ServerMsg},
    },
    item::Eventable,
    query::Query,
};
use anyhow::bail;
use ractor::{Actor, ActorRef};
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

/// References to the internal manager actors (useful for benchmarking/testing)
pub struct ManagerRefs {
    pub event_manager: ActorRef<EventManagerMsg>,
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub report_manager: ActorRef<ReportManagerMsg>,
    pub command_manager: ActorRef<CommandManagerMsg>,
}

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

    /// Get direct references to the internal manager actors.
    /// Useful for benchmarking and testing where you need direct actor access.
    pub async fn get_managers(&self) -> Result<ManagerRefs, anyhow::Error> {
        let (event_manager, query_manager, report_manager, command_manager) =
            ractor::call!(self.server, ServerMsg::GetManagers)?;

        Ok(ManagerRefs {
            event_manager,
            query_manager,
            report_manager,
            command_manager,
        })
    }

    /// Initialize all modules without blocking.
    /// For in-memory mode (kafka_config: None), this signals caught up immediately.
    /// For Kafka mode, this starts consumers and waits for them to catch up.
    pub fn init_modules(&self) -> Result<(), anyhow::Error> {
        self.server.send_message(ServerMsg::InitAllModules)?;
        Ok(())
    }
}

pub type MykoServerArgs = ServerArgs;

#[derive(Debug)]
pub struct MykoServerCtx {
    pub host_id: Uuid,
}
