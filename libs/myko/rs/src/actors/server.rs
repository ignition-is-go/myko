use crate::{
    actors::{
        event::{
            common::{PersistEvent, ProcessEventData},
            event_manager::{EventManager, EventManagerArgs, EventManagerMsg},
        },
        kafka::common::KafkaSharedConfig,
        message_handler::{MessageHandler, MessageHandlerArgs, MessageHandlerMsg},
        query::query_manager::{QueryManager, QueryManagerArgs, QueryManagerMsg},
        report::report_manager::{ReportManager, ReportManagerArgs, ReportManagerMsg},
        ws::websocket_server::{WebSocketServer, WebSocketServerArgs, WebSocketServerMsg},
    },
    event::{MEvent, MEventType},
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
    ctx: Arc<MykoServerCtx>,
    args: ServerArgs,
}

pub struct ServerArgs {
    pub bind_addr: String,
    pub bind_path: String,
    pub bind_port: u16,
    pub kafka_config: KafkaSharedConfig,
    pub public_host_address: String,
}

pub enum ServerMsg {
    Start,
    InitAllModules,
    AllInitComplete,
    RepoManagerMsg(EventManagerMsg),
    WebSocketServerMsg(WebSocketServerMsg),
    MessageHandlerMsg(MessageHandlerMsg),
    QueryManagerMsg(QueryManagerMsg),
    ReportManagerMsg(ReportManagerMsg),
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

        let event_manager = match Actor::spawn(
            None,
            EventManager,
            EventManagerArgs {
                server: myself.clone(),
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

        let query_manager = match Actor::spawn(
            None,
            QueryManager,
            QueryManagerArgs {
                ctx: ctx.clone(),
                server: myself.clone(),
                event_manager: event_manager.clone(),
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

        let message_handler = match Actor::spawn(
            None,
            MessageHandler,
            MessageHandlerArgs {
                event_manager: event_manager.clone(),
                query_manager: query_manager.clone(),
                report_manager: report_manager.clone(),
                server: myself.clone(),
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

        let web_socket_server = match Actor::spawn(
            None,
            WebSocketServer,
            WebSocketServerArgs {
                port: args.bind_port,
                message_handler: message_handler.clone(),
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

        Ok(ServerState {
            repo_manager: event_manager,
            web_socket_server,
            message_handler,
            query_manager,
            report_manager,
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
        match message {
            ServerMsg::MessageHandlerMsg(msg) => {
                if let Err(err) = state.message_handler.send_message(msg) {
                    error!("Failed to send message to MessageHandler: {}", err);
                };
            }
            ServerMsg::RepoManagerMsg(msg) => {
                if let Err(err) = state.repo_manager.send_message(msg) {
                    error!("Failed to send message to RepoManager: {}", err);
                };
            }
            ServerMsg::WebSocketServerMsg(msg) => {
                if let Err(err) = state.web_socket_server.send_message(msg) {
                    error!("Failed to send message to WebSocketServer: {}", err);
                };
            }
            ServerMsg::QueryManagerMsg(msg) => {
                if let Err(err) = state.query_manager.send_message(msg) {
                    error!("Failed to send message to QueryManager: {}", err);
                };
            }
            ServerMsg::ReportManagerMsg(msg) => {
                if let Err(err) = state.report_manager.send_message(msg) {
                    error!("Failed to send message to ReportManager: {}", err);
                };
            }
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
                    }))
                {
                    error!("Failed to send message to RepoManager: {}", err);
                }

                if let Err(err) = state
                    .web_socket_server
                    .send_message(WebSocketServerMsg::Start)
                {
                    error!("Failed to send message to WebSocketServer: {}", err);
                }
            }
        };
        Ok(())
    }
}
