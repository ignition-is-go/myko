//! Server actor - the root coordinator for a Myko server instance.
//!
//! The [`Server`] actor is responsible for:
//! - Spawning and wiring all manager actors during startup
//! - Coordinating the initialization sequence
//! - Managing server lifecycle events (start, init, shutdown)
//!
//! # Actor Hierarchy
//!
//! ```text
//! Server (lifecycle coordinator)
//!    ├── WebSocketServer (accepts connections)
//!    ├── QueryManager (reactive query execution)
//!    ├── EventManager (event routing + persistence)
//!    │      └── EventHandler (per entity type)
//!    ├── ReportManager (computed reports)
//!    ├── CommandManager (command execution)
//!    ├── SagaManager (event stream processors)
//!    ├── RelationshipManager (cascade operations)
//!    ├── SearchManager (full-text search indexing)
//!    └── MessageHandler (WebSocket message routing)
//! ```

use crate::{
    actors::{
        command::command_manager::{CommandManager, CommandManagerArgs, CommandManagerMsg},
        event::{
            common::{PersistEvent, ProcessEventData},
            event_manager::{EventManager, EventManagerArgs, EventManagerMsg},
            EventBus,
        },
        kafka::common::KafkaSharedConfig,
        message_handler::{MessageHandler, MessageHandlerArgs, MessageHandlerMsg},
        peer::{PeerManager, PeerManagerArgs, PeerManagerMsg},
        query::query_manager::{QueryManager, QueryManagerArgs, QueryManagerMsg},
        relationship::{RelationshipManager, RelationshipManagerArgs, RelationshipManagerMsg},
        report::report_manager::{ReportManager, ReportManagerArgs, ReportManagerMsg},
        saga::{SagaManager, SagaManagerArgs, SagaManagerMsg},
        search::{SearchManager, SearchManagerArgs, SearchManagerMsg},
        ws::websocket_server::{WebSocketServer, WebSocketServerArgs, WebSocketServerMsg},
    },
    api::query::WrappedQuery,
    entities::server::{GetServersByQuery, PartialServer},
    event::{MEvent, MEventType},
    query::QueryRequest,
    runtime::{Actor, ActorRef, RpcReplyPort},
    saga::SagaContext,
    server::MykoServerCtx,
};
use chrono::Utc;
use log::{error, info, trace};
use std::sync::Arc;
use uuid::Uuid;

/// Arguments for spawning the Server actor.
#[derive(Debug, Clone)]
pub struct ServerArgs {
    /// Address to bind to (e.g., "0.0.0.0")
    pub bind_addr: String,
    /// Path for WebSocket endpoint (e.g., "/myko")
    pub bind_path: String,
    /// Port to listen on
    pub bind_port: u16,
    /// Kafka configuration. When `None`, runs in-memory only (useful for testing/benchmarks).
    pub kafka_config: Option<KafkaSharedConfig>,
    /// Public address for this server (used in Server entity and peer discovery)
    pub public_host_address: String,
}

/// Server messages - lifecycle only.
pub enum ServerMsg {
    /// Start the server (currently just logs startup)
    Start,

    /// Initialize all modules - triggers Kafka consumers to replay history
    InitAllModules,

    /// All EventHandlers have caught up with Kafka.
    AllInitComplete,

    /// Get direct references to manager actors.
    #[allow(clippy::type_complexity)]
    GetManagers(
        RpcReplyPort<(
            ActorRef<EventManagerMsg>,
            ActorRef<QueryManagerMsg>,
            ActorRef<ReportManagerMsg>,
            ActorRef<CommandManagerMsg>,
        )>,
    ),
}

impl std::fmt::Debug for ServerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerMsg::Start => write!(f, "Start"),
            ServerMsg::InitAllModules => write!(f, "InitAllModules"),
            ServerMsg::AllInitComplete => write!(f, "AllInitComplete"),
            ServerMsg::GetManagers(_) => write!(f, "GetManagers"),
        }
    }
}

/// Root coordinator actor for a Myko server instance.
pub struct Server {
    /// Shared tokio runtime for async operations (kept alive for server lifetime)
    #[allow(dead_code)]
    tokio_runtime: tokio::runtime::Runtime,
    /// EventManager for event routing and persistence
    repo_manager: ActorRef<EventManagerMsg>,
    /// WebSocket server for accepting client connections
    web_socket_server: ActorRef<WebSocketServerMsg>,
    /// MessageHandler for routing WebSocket messages to appropriate managers
    message_handler: ActorRef<MessageHandlerMsg>,
    /// QueryManager for reactive query execution
    query_manager: ActorRef<QueryManagerMsg>,
    /// ReportManager for computed reports
    report_manager: ActorRef<ReportManagerMsg>,
    /// CommandManager for command execution
    command_manager: ActorRef<CommandManagerMsg>,
    /// SagaManager for event stream processors
    saga_manager: ActorRef<SagaManagerMsg>,
    /// RelationshipManager for cascade operations
    relationship_manager: ActorRef<RelationshipManagerMsg>,
    /// PeerManager for peer discovery and federation
    peer_manager: ActorRef<PeerManagerMsg>,
    /// SearchManager for full-text search
    #[allow(dead_code)]
    search_manager: ActorRef<SearchManagerMsg>,
    /// Server context with host ID
    ctx: Arc<MykoServerCtx>,
    /// Original arguments for reference during lifecycle events
    args: ServerArgs,
}

impl Server {
    /// Create a new Server with all child actors spawned.
    fn create(args: ServerArgs, myself: ActorRef<ServerMsg>) -> Self {
        // Create shared tokio runtime for all async operations
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create shared tokio runtime");

        let ctx = Arc::new(MykoServerCtx {
            host_id: Uuid::new_v4(),
            event_bus: std::sync::OnceLock::new(),
            search_manager: std::sync::OnceLock::new(),
            tokio_handle: tokio_runtime.handle().clone(),
        });

        let server_id: Arc<str> = ctx.host_id.to_string().into();

        // Use the real server reference for child actors
        let server_ref = myself;

        // 1. Spawn WebSocketServer
        let web_socket_server = WebSocketServer::spawn(WebSocketServerArgs {
            port: args.bind_port,
            server_id: server_id.clone(),
        })
        .actor_ref();

        // 2. Spawn QueryManager
        let query_manager = QueryManager::spawn(QueryManagerArgs {
            ctx: ctx.clone(),
            server: server_ref.clone(),
        })
        .actor_ref();

        // 3. Spawn EventManager (needs query_manager)
        let event_manager = EventManager::spawn(EventManagerArgs {
            server: server_ref.clone(),
            query_manager: query_manager.clone(),
            ctx: ctx.clone(),
        })
        .actor_ref();

        // Wire EventManager reference to QueryManager (breaks circular dependency)
        if let Err(err) =
            query_manager.send_message(QueryManagerMsg::SetEventManager(event_manager.clone()))
        {
            error!("Failed to set EventManager in QueryManager: {}", err);
        }

        // 4. Spawn ReportManager (needs query_manager)
        let report_manager = ReportManager::spawn(ReportManagerArgs {
            ctx: ctx.clone(),
            query_manager: query_manager.clone(),
        })
        .actor_ref();

        // 5. Spawn CommandManager (needs event_manager, query_manager, report_manager)
        let command_manager = CommandManager::spawn(CommandManagerArgs {
            ctx: ctx.clone(),
            event_manager: event_manager.clone(),
            query_manager: query_manager.clone(),
            report_manager: report_manager.clone(),
        })
        .actor_ref();

        // Create EventBus for high-throughput event distribution to sagas
        let event_bus = EventBus::new();

        // Store EventBus in server context for reports
        let _ = ctx.event_bus.set(event_bus.clone());

        // Create SagaContext for saga handlers
        let saga_ctx = Arc::new(SagaContext::new(
            ctx.clone(),
            event_manager.clone(),
            command_manager.clone(),
            query_manager.clone(),
            event_bus.clone(),
        ));

        // 6. Spawn SagaManager
        let saga_manager = SagaManager::spawn(SagaManagerArgs {
            ctx: saga_ctx,
            command_manager: command_manager.clone(),
        })
        .actor_ref();

        // Wire EventBus to EventManager for high-throughput saga broadcasting
        if let Err(err) =
            event_manager.send_message(EventManagerMsg::SetEventBus(event_bus.clone()))
        {
            error!("Failed to set EventBus in EventManager: {}", err);
        }

        // 7. Spawn SearchManager for full-text search indexing
        let search_manager = SearchManager::spawn(SearchManagerArgs {
            ctx: ctx.clone(),
            event_manager: event_manager.clone(),
        })
        .actor_ref();

        // Store SearchManager in context for reports to access
        let _ = ctx.search_manager.set(search_manager.clone());

        // 8. Spawn RelationshipManager for cascade operations
        let relationship_manager = RelationshipManager::spawn(RelationshipManagerArgs {
            ctx: ctx.clone(),
            event_manager: event_manager.clone(),
        })
        .actor_ref();

        // Wire RelationshipManager to receive events directly from EventManager
        if let Err(err) = event_manager
            .send_message(EventManagerMsg::SetRelationshipManager(relationship_manager.clone()))
        {
            error!("Failed to set RelationshipManager in EventManager: {}", err);
        }

        // 9. Spawn MessageHandler with direct references to all managers + ws_server
        let message_handler = MessageHandler::spawn(MessageHandlerArgs {
            event_manager: event_manager.clone(),
            query_manager: query_manager.clone(),
            report_manager: report_manager.clone(),
            command_manager: command_manager.clone(),
            ws_server: web_socket_server.clone(),
            host_id: ctx.host_id,
            tokio_handle: ctx.tokio_handle.clone(),
        })
        .actor_ref();

        // 10. Spawn PeerManager for peer discovery and federation
        let peer_manager = PeerManager::spawn(PeerManagerArgs {
            ctx: ctx.clone(),
            host_address: args.public_host_address.clone(),
            host_port: args.bind_port,
            query_manager: query_manager.clone(),
            command_manager: command_manager.clone(),
        })
        .actor_ref();

        Self {
            tokio_runtime,
            repo_manager: event_manager,
            web_socket_server,
            message_handler,
            query_manager,
            report_manager,
            command_manager,
            saga_manager,
            relationship_manager,
            peer_manager,
            search_manager,
            ctx,
            args,
        }
    }

    fn handle_init_all_modules(&self) {
        if let Err(err) = self
            .repo_manager
            .send_message(EventManagerMsg::InitAll(self.args.kafka_config.clone()))
        {
            error!("Failed to send message to RepoManager: {}", err);
        };
    }

    fn handle_all_init_complete(&self) {
        // Establish relationships (orphan cleanup, ensure-for initialization)
        if let Err(err) = self
            .relationship_manager
            .call(RelationshipManagerMsg::EstablishRelations)
        {
            error!("Failed to establish relationships: {}", err);
        }

        // Start all registered sagas
        if let Err(err) = self.saga_manager.send_message(SagaManagerMsg::StartAll) {
            error!("Failed to start SagaManager: {}", err);
        }

        // Populate search indices with existing entities
        if let Err(err) = self
            .search_manager
            .send_message(SearchManagerMsg::PopulateAll)
        {
            error!("Failed to populate search indices: {}", err);
        }

        // Start WebSocket server and wait for it to be ready
        let (ready_tx, ready_rx) = crossbeam::channel::bounded(1);
        if let Err(err) = self
            .web_socket_server
            .send_message(WebSocketServerMsg::Start {
                message_handler: self.message_handler.clone(),
                reply: ready_tx,
            })
        {
            error!("Failed to send Start to WebSocketServer: {}", err);
            return;
        }

        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("WebSocket server failed to start: {}", e);
                return;
            }
            Err(err) => {
                error!("Failed to receive WebSocket ready signal: {:?}", err);
                return;
            }
        }

        // Clean up stale servers with the same address and port
        // Create the query params (without tx/created_at - those are in QueryRequest)
        let query_params = GetServersByQuery(PartialServer {
            address: Some(self.args.public_host_address.clone()),
            port: Some(self.args.bind_port),
            ..Default::default()
        });

        // Wrap with QueryRequest to add tx and created_at
        let query_request = QueryRequest::new(query_params);
        let query_value = match serde_json::to_value(&query_request) {
            Ok(v) => v,
            Err(err) => {
                error!("Failed to serialize cleanup query: {}", err);
                return;
            }
        };

        let wrapped = WrappedQuery {
            query: query_value,
            query_id: "GetServersByQuery".into(),
            query_item_type: "Server".into(),
        };

        match self
            .query_manager
            .call(|r| QueryManagerMsg::WrappedQuerySnapshot(wrapped, r))
        {
            Ok(stale_servers) => {
                for (id, server) in stale_servers {
                    trace!("Cleaning up stale server: {}", id);
                    let event = MEvent {
                        item: server.to_value(),
                        change_type: MEventType::DEL,
                        item_type: "Server".to_string(),
                        created_at: Utc::now().to_rfc3339(),
                        tx: Uuid::new_v4().to_string(),
                        source_id: None,
                        options: None,
                    };
                    if let Err(err) =
                        self.repo_manager
                            .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                                event,
                                persist: PersistEvent::Persist,
                                parsed_item: None,
                                client_id: None,
                            }))
                    {
                        error!("Failed to delete stale server {}: {}", id, err);
                    }
                }
            }
            Err(err) => {
                error!("Failed to query for stale servers: {}", err);
            }
        }

        // NOW publish our Server entity
        let s = crate::entities::server::Server {
            id: self.ctx.host_id.to_string().into(),
            address: self.args.public_host_address.to_string(),
            hash: uuid::Uuid::new_v4().to_string().into(),
            port: self.args.bind_port,
            started_at: Utc::now().to_rfc3339(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        };

        info!("Server {} listening on {}:{}", s.id, s.address, s.port);

        let event = MEvent::from_item(&s, MEventType::SET, Uuid::new_v4().to_string());

        if let Err(err) =
            self.repo_manager
                .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                    event,
                    persist: PersistEvent::Persist,
                    parsed_item: None,
                    client_id: None,
                }))
        {
            error!("Failed to send message to RepoManager: {}", err);
        }

        // Start PeerManager for peer discovery (after Server entity is published)
        if let Err(err) = self.peer_manager.send_message(PeerManagerMsg::Start) {
            error!("Failed to start PeerManager: {}", err);
        }
    }

    #[allow(clippy::type_complexity)]
    fn handle_get_managers(
        &self,
        reply: RpcReplyPort<(
            ActorRef<EventManagerMsg>,
            ActorRef<QueryManagerMsg>,
            ActorRef<ReportManagerMsg>,
            ActorRef<CommandManagerMsg>,
        )>,
    ) {
        if let Err(err) = reply.send((
            self.repo_manager.clone(),
            self.query_manager.clone(),
            self.report_manager.clone(),
            self.command_manager.clone(),
        )) {
            error!("Failed to reply with manager refs: {:?}", err);
        }
    }
}

impl Actor for Server {
    type Msg = ServerMsg;
    type Args = ServerArgs;

    fn new(args: Self::Args, myself: ActorRef<Self::Msg>) -> Self {
        Self::create(args, myself)
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            ServerMsg::Start => {}
            ServerMsg::InitAllModules => {
                self.handle_init_all_modules();
            }
            ServerMsg::AllInitComplete => {
                self.handle_all_init_complete();
            }
            ServerMsg::GetManagers(reply) => {
                self.handle_get_managers(reply);
            }
        }
    }
}
