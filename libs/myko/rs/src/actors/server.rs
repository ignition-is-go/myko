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
        query::query_manager::{QueryManager, QueryManagerArgs, QueryManagerMsg},
        report::report_manager::{ReportManager, ReportManagerArgs, ReportManagerMsg},
        saga::{SagaManager, SagaManagerArgs, SagaManagerMsg},
        ws::websocket_server::{WebSocketServer, WebSocketServerArgs, WebSocketServerMsg},
    },
    api::query::wrap_query,
    entities::server::{GetServersByQuery, PartialServer},
    event::{MEvent, MEventType},
    saga::SagaContext,
    server::MykoServerCtx,
};
use chrono::Utc;
use log::{error, info};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use uuid::Uuid;

pub struct Server;

pub struct ServerState {
    repo_manager: ActorRef<EventManagerMsg>,
    web_socket_server: ActorRef<WebSocketServerMsg>,
    message_handler: ActorRef<MessageHandlerMsg>,
    query_manager: ActorRef<QueryManagerMsg>,
    report_manager: ActorRef<ReportManagerMsg>,
    command_manager: ActorRef<CommandManagerMsg>,
    saga_manager: ActorRef<SagaManagerMsg>,
    ctx: Arc<MykoServerCtx>,
    args: ServerArgs,
}

pub struct ServerArgs {
    pub bind_addr: String,
    pub bind_path: String,
    pub bind_port: u16,
    /// Kafka configuration. When None, the server runs in-memory only (useful for testing/benchmarks).
    pub kafka_config: Option<KafkaSharedConfig>,
    pub public_host_address: String,
}

/// Server messages - lifecycle only (routing eliminated for performance)
pub enum ServerMsg {
    Start,
    InitAllModules,
    AllInitComplete,
    /// Get direct references to manager actors (useful for benchmarking/testing)
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

        Ok(ServerState {
            repo_manager: event_manager,
            web_socket_server,
            message_handler,
            query_manager,
            report_manager,
            command_manager,
            saga_manager,
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
            ServerMsg::Start => {
                info!("Starting Server!");
            }
            ServerMsg::InitAllModules => {
                if let Err(err) = state
                    .repo_manager
                    .send_message(EventManagerMsg::InitAll(state.args.kafka_config.clone()))
                {
                    error!("Failed to send message to RepoManager: {}", err);
                };
            }
            ServerMsg::AllInitComplete => {
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

                match ractor::call!(state.query_manager, QueryManagerMsg::QuerySnapshot, wrapped) {
                    Ok(stale_servers) => {
                        for (id, server) in stale_servers {
                            info!("Cleaning up stale server: {}", id);
                            let event = MEvent {
                                item: server.to_value(),
                                change_type: MEventType::DEL,
                                item_type: "Server".to_string(),
                                created_at: Utc::now().to_rfc3339(),
                                tx: Uuid::new_v4().to_string(),
                                source_id: None,
                            };
                            if let Err(err) = state.repo_manager.send_message(
                                EventManagerMsg::ProcessEvent(ProcessEventData {
                                    event,
                                    persist: PersistEvent::Persist,
                                    parsed_item: None,
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

                let s = crate::entities::server::Server {
                    id: state.ctx.host_id.to_string().into(),
                    address: state.args.public_host_address.to_string(),
                    hash: uuid::Uuid::new_v4().to_string().into(),
                    port: state.args.bind_port,
                    started_at: Utc::now().to_rfc3339(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                };

                let event = MEvent::from_item(&s, MEventType::SET, Uuid::new_v4().to_string());

                if let Err(err) = state
                    .repo_manager
                    .send_message(EventManagerMsg::ProcessEvent(ProcessEventData {
                        event,
                        persist: PersistEvent::Persist,
                        parsed_item: None, // Server startup event, needs parsing
                    }))
                {
                    error!("Failed to send message to RepoManager: {}", err);
                }

                // Start all registered sagas
                if let Err(err) = state.saga_manager.send_message(SagaManagerMsg::StartAll) {
                    error!("Failed to start SagaManager: {}", err);
                }

                // Start WebSocket server with MessageHandler (breaks circular dep)
                if let Err(err) = state
                    .web_socket_server
                    .send_message(WebSocketServerMsg::Start {
                        message_handler: state.message_handler.clone(),
                    })
                {
                    error!("Failed to send message to WebSocketServer: {}", err);
                }

                // Log entity counts after initialization
                match ractor::call!(state.repo_manager, EventManagerMsg::GetAllCounts) {
                    Ok(counts) => {
                        let total: usize = counts.iter().map(|(_, c)| c).sum();
                        let counts_str = counts
                            .iter()
                            .map(|(name, count)| format!("{}: {}", name, count))
                            .collect::<Vec<_>>()
                            .join(", ");
                        info!("Entity counts after init: {} total [{}]", total, counts_str);
                    }
                    Err(err) => {
                        error!("Failed to get entity counts: {}", err);
                    }
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
