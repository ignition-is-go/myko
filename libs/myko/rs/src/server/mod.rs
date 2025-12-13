use crate::{
    actors::{
        command::command_manager::CommandManagerMsg,
        event::event_manager::EventManagerMsg,
        query::query_manager::QueryManagerMsg,
        report::report_manager::ReportManagerMsg,
        search::SearchManagerMsg,
        server::{Server, ServerArgs, ServerMsg},
    },
    item::Eventable,
    query::Query,
    report::Report,
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
    /// Direct manager references for registration (bypasses Server routing)
    pub(crate) event_manager: ActorRef<EventManagerMsg>,
    pub(crate) query_manager: ActorRef<QueryManagerMsg>,
    pub(crate) report_manager: ActorRef<ReportManagerMsg>,
    pub(crate) command_manager: ActorRef<CommandManagerMsg>,
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

                // Get direct manager references for registration (bypasses Server routing)
                let (event_manager, query_manager, report_manager, command_manager) =
                    ractor::call!(server, ServerMsg::GetManagers)?;

                let server = Arc::new(MykoServer {
                    server,
                    notify_end: notify.clone(),
                    event_manager,
                    query_manager,
                    report_manager,
                    command_manager,
                });

                crate::entities::server::Server::register(&server)?;
                crate::entities::server::GetConnectedServer::register(&server)?;
                crate::entities::server::GetPeerServers::register(&server)?;

                crate::entities::client::Client::register(&server)?;

                // Register EntitySearch report
                crate::search::EntitySearch::register(&server)?;

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
    pub fn get_managers(&self) -> ManagerRefs {
        ManagerRefs {
            event_manager: self.event_manager.clone(),
            query_manager: self.query_manager.clone(),
            report_manager: self.report_manager.clone(),
            command_manager: self.command_manager.clone(),
        }
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

/// Server context shared across actors.
///
/// Contains server identity and shared resources like the EventBus.
pub struct MykoServerCtx {
    /// Unique identifier for this server instance
    pub host_id: Uuid,
    /// Event bus for high-throughput event distribution (set during server startup)
    pub event_bus: std::sync::OnceLock<crate::actors::event::EventBus>,
    /// Search manager for full-text search (set during server startup)
    pub search_manager: std::sync::OnceLock<ActorRef<SearchManagerMsg>>,
}

impl std::fmt::Debug for MykoServerCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MykoServerCtx")
            .field("host_id", &self.host_id)
            .field("event_bus", &self.event_bus.get().map(|_| "EventBus"))
            .field(
                "search_manager",
                &self.search_manager.get().map(|_| "SearchManager"),
            )
            .finish()
    }
}

impl MykoServerCtx {
    /// Search for entities matching a query string.
    ///
    /// Returns matching entity IDs (up to `limit` results).
    /// Returns empty Vec if no SearchManager is available or entity type is not indexed.
    pub async fn search(
        &self,
        entity_type: &str,
        query: &str,
        limit: usize,
    ) -> Vec<Arc<str>> {
        let Some(search_manager) = self.search_manager.get() else {
            return vec![];
        };

        match ractor::call!(
            search_manager,
            SearchManagerMsg::Search,
            entity_type.to_string(),
            query.to_string(),
            limit
        ) {
            Ok(ids) => ids,
            Err(e) => {
                log::error!("Search call failed: {}", e);
                vec![]
            }
        }
    }
}
