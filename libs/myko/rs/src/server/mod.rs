use crate::{
    actors::{
        command::command_manager::CommandManagerMsg,
        event::event_manager::EventManagerMsg,
        message_handler::MessageHandlerMsg,
        query::query_manager::QueryManagerMsg,
        report::report_manager::ReportManagerMsg,
        search::SearchManagerMsg,
        server::{Server, ServerArgs, ServerMsg},
    },
    runtime::{Actor, ActorRef},
    sync_client::SyncClient,
};
use anyhow::bail;
use std::sync::Arc;
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
    /// Direct manager references for registration (bypasses Server routing)
    pub(crate) event_manager: ActorRef<EventManagerMsg>,
    pub(crate) query_manager: ActorRef<QueryManagerMsg>,
    pub(crate) report_manager: ActorRef<ReportManagerMsg>,
    pub(crate) command_manager: ActorRef<CommandManagerMsg>,
}

impl MykoServer {
    pub fn init(args: MykoServerArgs) -> Result<Arc<MykoServer>, anyhow::Error> {
        let handle = Server::spawn(args);
        let server = handle.actor_ref();

        // Get direct manager references for registration (bypasses Server routing)
        let (event_manager, query_manager, report_manager, command_manager) =
            match server.call(ServerMsg::GetManagers) {
                Ok(refs) => refs,
                Err(err) => {
                    bail!("Failed to get manager refs: {}", err);
                }
            };

        let server = Arc::new(MykoServer {
            server,
            event_manager,
            query_manager,
            report_manager,
            command_manager,
        });

        Ok(server)
    }

    pub fn start(&self) -> Result<(), anyhow::Error> {
        // All entities, queries, and reports are auto-registered in their manager constructors
        self.server.send_message(ServerMsg::InitAllModules)?;
        Ok(())
    }

    /// Block until the server stops.
    /// This is typically not needed - the server runs on its own threads.
    pub fn wait(&self) {
        // Server actors run on dedicated threads, so we just park this thread
        // The server will run indefinitely until the process exits
        std::thread::park();
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
    /// Query manager for subscriptions (set during server startup)
    pub query_manager: std::sync::OnceLock<ActorRef<QueryManagerMsg>>,
    /// Report manager for sub-report subscriptions (set during server startup)
    pub report_manager: std::sync::OnceLock<ActorRef<ReportManagerMsg>>,
    /// Sync client for distributed timing (set during server startup if sync server available)
    pub sync_client: std::sync::OnceLock<Arc<SyncClient>>,
    /// Message handler for windback cache updates (set during server startup)
    pub message_handler: std::sync::OnceLock<ActorRef<MessageHandlerMsg>>,
    /// Shared tokio runtime handle for async operations
    /// This is used by actors that need to run async code from sync contexts
    pub tokio_handle: tokio::runtime::Handle,
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
            .field(
                "query_manager",
                &self.query_manager.get().map(|_| "QueryManager"),
            )
            .field(
                "report_manager",
                &self.report_manager.get().map(|_| "ReportManager"),
            )
            .field(
                "sync_client",
                &self.sync_client.get().map(|_| "SyncClient"),
            )
            .field(
                "message_handler",
                &self.message_handler.get().map(|_| "MessageHandler"),
            )
            .finish()
    }
}

impl MykoServerCtx {
    /// Search for entities matching a query string.
    ///
    /// Returns matching entity IDs (up to `limit` results).
    /// Returns empty Vec if no SearchManager is available or entity type is not indexed.
    pub fn search(&self, entity_type: &str, query: &str, limit: usize) -> Vec<Arc<str>> {
        let Some(search_manager) = self.search_manager.get() else {
            return vec![];
        };

        match search_manager.call(|r| {
            SearchManagerMsg::Search(entity_type.to_string(), query.to_string(), limit, r)
        }) {
            Ok(ids) => ids,
            Err(e) => {
                log::error!("Search call failed: {}", e);
                vec![]
            }
        }
    }

    /// Update the windback cache for a client.
    ///
    /// Called by windback commands (SetClientWindbackTime, ClearClientWindbackTime)
    /// after successfully updating the Client entity.
    pub fn update_windback(&self, client_id: Arc<str>, windback: Option<Arc<str>>) {
        if let Some(message_handler) = self.message_handler.get()
            && let Err(e) = message_handler.send_message(MessageHandlerMsg::UpdateWindback {
                client_id,
                windback,
            })
        {
            log::error!("Failed to update windback cache: {}", e);
        }
    }
}
