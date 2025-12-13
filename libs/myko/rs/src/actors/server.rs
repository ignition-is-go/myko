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
//!
//! # Startup Sequence
//!
//! 1. **`pre_start`**: Spawn all actors with direct references (no Server routing)
//! 2. **`Start`**: Log startup message
//! 3. **`InitAllModules`**: Trigger Kafka consumers to replay history
//! 4. **`AllInitComplete`** (from EventManager when all handlers ready):
//!    - Clean up stale Server records with same address:port
//!    - Publish this server's `Server` entity
//!    - Establish relationships (orphan cleanup, ensure-for)
//!    - Start all registered sagas
//!    - Start WebSocket server to accept connections
//!
//! # Performance Note
//!
//! The Server actor is **lifecycle-only** - it does not route messages between
//! other actors. All managers hold direct [`ActorRef`] to each other, eliminating
//! the Server as a bottleneck. See `libs/myko/rs/OPTIMIZATION.md` for details.

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
    api::query::wrap_query,
    entities::server::{GetServersByQuery, PartialServer},
    event::{MEvent, MEventType},
    saga::SagaContext,
    server::MykoServerCtx,
};
use chrono::Utc;
use log::{error, info, trace};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use uuid::Uuid;

/// Root coordinator actor for a Myko server instance.
///
/// This is the top-level actor that bootstraps all other actors and manages
/// the server lifecycle. It is intentionally minimal to avoid being a bottleneck.
pub struct Server;

/// Internal state for the Server actor.
///
/// Holds references to all spawned child actors.
pub struct ServerState {
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
    /// SearchManager for full-text search (kept alive, accessed via ctx.search_manager)
    #[allow(dead_code)]
    search_manager: ActorRef<SearchManagerMsg>,
    /// Server context with host ID
    ctx: Arc<MykoServerCtx>,
    /// Original arguments for reference during lifecycle events
    args: ServerArgs,
}

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
///
/// **Note**: Message routing between actors has been eliminated for performance.
/// All managers hold direct [`ActorRef`] to each other.
pub enum ServerMsg {
    /// Start the server (currently just logs startup)
    Start,

    /// Initialize all modules - triggers Kafka consumers to replay history
    InitAllModules,

    /// All EventHandlers have caught up with Kafka.
    /// Triggers: stale server cleanup, Server entity publish, relationship establishment,
    /// saga startup, and WebSocket server start.
    AllInitComplete,

    /// Get direct references to manager actors.
    /// Useful for benchmarking and testing.
    #[allow(clippy::type_complexity)]
    GetManagers(
        ractor::RpcReplyPort<(
            ActorRef<EventManagerMsg>,
            ActorRef<QueryManagerMsg>,
            ActorRef<ReportManagerMsg>,
            ActorRef<CommandManagerMsg>,
        )>,
    ),
}

impl Actor for Server {
    type State = ServerState;
    type Arguments = ServerArgs;
    type Msg = ServerMsg;

    async fn pre_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        let ctx = Arc::new(MykoServerCtx {
            host_id: Uuid::new_v4(),
            event_bus: std::sync::OnceLock::new(),
            search_manager: std::sync::OnceLock::new(),
        });

        // Spawn order optimized for direct references (no Server routing):
        // 1. WebSocketServer (no deps, receives message_handler in Start msg)
        // 2. QueryManager (no deps)
        // 3. EventManager (needs query_manager)
        // 4. ReportManager (needs query_manager)
        // 5. CommandManager (needs event_manager, query_manager, report_manager)
        // 6. SagaManager (needs command_manager)
        // 7. MessageHandler (needs all managers + ws_server)

        let server_id: Arc<str> = ctx.host_id.to_string().into();

        let web_socket_server = match Actor::spawn(
            None,
            WebSocketServer,
            WebSocketServerArgs {
                port: args.bind_port,
                server_id: server_id.clone(),
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn WebSocketServer actor: {}", err);
                return Err(err.into());
            }
        };

        let query_manager = match Actor::spawn(
            None,
            QueryManager,
            QueryManagerArgs {
                ctx: ctx.clone(),
                server: myself.clone(),
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn QueryManager actor: {}", err);
                return Err(err.into());
            }
        };

        let event_manager = match Actor::spawn(
            None,
            EventManager,
            EventManagerArgs {
                server: myself.clone(),
                query_manager: query_manager.clone(),
                ctx: ctx.clone(),
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn RepoManager actor: {}", err);
                return Err(err.into());
            }
        };

        // Wire EventManager reference to QueryManager (breaks circular dependency)
        if let Err(err) = query_manager.send_message(QueryManagerMsg::SetEventManager(event_manager.clone())) {
            error!("Failed to set EventManager in QueryManager: {}", err);
        }

        let report_manager = match Actor::spawn(
            None,
            ReportManager,
            ReportManagerArgs {
                ctx: ctx.clone(),
                query_manager: query_manager.clone(),
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn ReportManager actor: {}", err);
                return Err(err.into());
            }
        };

        let command_manager = match Actor::spawn(
            None,
            CommandManager,
            CommandManagerArgs {
                ctx: ctx.clone(),
                event_manager: event_manager.clone(),
                query_manager: query_manager.clone(),
                report_manager: report_manager.clone(),
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn CommandManager actor: {}", err);
                return Err(err.into());
            }
        };

        // Create EventBus for high-throughput event distribution to sagas
        let event_bus = EventBus::new();

        // Store EventBus in server context for reports (e.g., ServerEventLog)
        let _ = ctx.event_bus.set(event_bus.clone());

        // Create SagaContext for saga handlers
        let saga_ctx = Arc::new(SagaContext::new(
            ctx.clone(),
            event_manager.clone(),
            command_manager.clone(),
            query_manager.clone(),
            event_bus.clone(),
        ));

        let saga_manager = match Actor::spawn(
            None,
            SagaManager,
            SagaManagerArgs {
                ctx: saga_ctx,
                command_manager: command_manager.clone(),
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn SagaManager actor: {}", err);
                return Err(err.into());
            }
        };

        // Wire EventBus to EventManager for high-throughput saga broadcasting
        if let Err(err) = event_manager.send_message(EventManagerMsg::SetEventBus(event_bus.clone())) {
            error!("Failed to set EventBus in EventManager: {}", err);
        }

        // Spawn SearchManager for full-text search indexing
        let search_manager = match Actor::spawn(
            None,
            SearchManager,
            SearchManagerArgs {
                ctx: ctx.clone(),
                event_manager: event_manager.clone(),
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn SearchManager actor: {}", err);
                return Err(err.into());
            }
        };

        // Store SearchManager in context for reports to access
        let _ = ctx.search_manager.set(search_manager.clone());

        // Spawn RelationshipManager for cascade operations
        let relationship_manager = match Actor::spawn(
            None,
            RelationshipManager,
            RelationshipManagerArgs {
                ctx: ctx.clone(),
                event_manager: event_manager.clone(),
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn RelationshipManager actor: {}", err);
                return Err(err.into());
            }
        };

        // Wire RelationshipManager to receive events directly from EventManager
        // (includes parsed items for efficient cascade handling)
        if let Err(err) =
            event_manager.send_message(EventManagerMsg::SetRelationshipManager(relationship_manager.clone()))
        {
            error!("Failed to set RelationshipManager in EventManager: {}", err);
        }

        // MessageHandler with direct references to all managers + ws_server
        let message_handler = match Actor::spawn(
            None,
            MessageHandler,
            MessageHandlerArgs {
                event_manager: event_manager.clone(),
                query_manager: query_manager.clone(),
                report_manager: report_manager.clone(),
                command_manager: command_manager.clone(),
                ws_server: web_socket_server.clone(),
                host_id: ctx.host_id,
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn MessageHandler actor: {}", err);
                return Err(err.into());
            }
        };

        // PeerManager for peer discovery and federation
        let peer_manager = match Actor::spawn(
            None,
            PeerManager,
            PeerManagerArgs {
                ctx: ctx.clone(),
                host_address: args.public_host_address.clone(),
                host_port: args.bind_port,
                query_manager: query_manager.clone(),
                command_manager: command_manager.clone(),
            },
        )
        .await
        {
            Ok((a, _h)) => a,
            Err(err) => {
                error!("Failed to spawn PeerManager actor: {}", err);
                return Err(err.into());
            }
        };

        Ok(ServerState {
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
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // NOTE: Server actor is now lifecycle-only. Routing eliminated for performance.
        // All managers communicate directly via ActorRef.
        match message {
            ServerMsg::Start => {}
            ServerMsg::InitAllModules => {
                if let Err(err) = state
                    .repo_manager
                    .send_message(EventManagerMsg::InitAll(state.args.kafka_config.clone()))
                {
                    error!("Failed to send message to RepoManager: {}", err);
                };
            }
            ServerMsg::AllInitComplete => {
                // Establish relationships (orphan cleanup, ensure-for initialization)
                if let Err(err) = ractor::call!(
                    state.relationship_manager,
                    RelationshipManagerMsg::EstablishRelations
                ) {
                    error!("Failed to establish relationships: {}", err);
                }

                // Start all registered sagas
                if let Err(err) = state.saga_manager.send_message(SagaManagerMsg::StartAll) {
                    error!("Failed to start SagaManager: {}", err);
                }

                // Populate search indices with existing entities
                if let Err(err) = state
                    .search_manager
                    .send_message(SearchManagerMsg::PopulateAll)
                {
                    error!("Failed to populate search indices: {}", err);
                }

                // Start WebSocket server FIRST and wait for it to be ready
                // This ensures we can accept connections before advertising ourselves
                let (ready_sender, ready_receiver) = ractor::concurrency::oneshot();
                if let Err(err) = state
                    .web_socket_server
                    .send_message(WebSocketServerMsg::Start {
                        message_handler: state.message_handler.clone(),
                        reply: ready_sender.into(),
                    })
                {
                    error!("Failed to send Start to WebSocketServer: {}", err);
                    return Ok(());
                }

                match ready_receiver.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        error!("WebSocket server failed to start: {}", e);
                        return Ok(());
                    }
                    Err(err) => {
                        error!("Failed to receive WebSocket ready signal: {:?}", err);
                        return Ok(());
                    }
                }

                // Clean up stale servers with the same address and port
                let query = GetServersByQuery {
                    partial: PartialServer {
                        address: Some(state.args.public_host_address.clone()),
                        port: Some(state.args.bind_port),
                        ..Default::default()
                    },
                    tx: Uuid::new_v4().to_string().into(),
                    created_at: Utc::now().to_rfc3339().into(),
                };

                let wrapped = match wrap_query(Uuid::new_v4().to_string().into(), &query) {
                    Ok(w) => w,
                    Err(err) => {
                        error!("Failed to wrap cleanup query: {}", err);
                        return Ok(());
                    }
                };

                match ractor::call!(state.query_manager, QueryManagerMsg::WrappedQuerySnapshot, wrapped) {
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
                            if let Err(err) = state.repo_manager.send_message(
                                EventManagerMsg::ProcessEvent(ProcessEventData {
                                    event,
                                    persist: PersistEvent::Persist,
                                    parsed_item: None,
                                    client_id: None, // Server internal events have no client
                                }),
                            ) {
                                error!("Failed to delete stale server {}: {}", id, err);
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to query for stale servers: {}", err);
                    }
                }

                // NOW publish our Server entity - WebSocket is ready to accept connections
                let s = crate::entities::server::Server {
                    id: state.ctx.host_id.to_string().into(),
                    address: state.args.public_host_address.to_string(),
                    hash: uuid::Uuid::new_v4().to_string().into(),
                    port: state.args.bind_port,
                    started_at: Utc::now().to_rfc3339(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                };

                info!("Server {} listening on {}:{}", s.id, s.address, s.port);

                let event = MEvent::from_item(&s, MEventType::SET, Uuid::new_v4().to_string());

                if let Err(err) = state
                    .repo_manager
                    .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                        event,
                        persist: PersistEvent::Persist,
                        parsed_item: None, // Server startup event, needs parsing
                        client_id: None,   // Server internal events have no client
                    }))
                {
                    error!("Failed to send message to RepoManager: {}", err);
                }

                // Start PeerManager for peer discovery (after Server entity is published)
                if let Err(err) = state.peer_manager.send_message(PeerManagerMsg::Start) {
                    error!("Failed to start PeerManager: {}", err);
                }
            }
            ServerMsg::GetManagers(reply) => {
                if let Err(err) = reply.send((
                    state.repo_manager.clone(),
                    state.query_manager.clone(),
                    state.report_manager.clone(),
                    state.command_manager.clone(),
                )) {
                    error!("Failed to reply with manager refs: {:?}", err);
                }
            }
        };
        Ok(())
    }
}
