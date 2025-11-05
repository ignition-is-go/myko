use crate::{
    actors::{
        kafka_common::KafkaSharedConfig,
        message_handler::{MessageHandler, MessageHandlerArgs, MessageHandlerMsg},
        repo_manager::{RepoManager, RepoManagerArgs, RepoManagerMsg},
        websocket_server::{WebSocketServer, WebSocketServerArgs, WebSocketServerMsg},
    },
    event::MEvent,
};
use chrono::Utc;
use log::{error, info};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use uuid::Uuid;

pub struct Server;

pub struct ServerCtx {
    pub host_id: Uuid,
}

pub struct ServerState {
    repo_manager: ActorRef<RepoManagerMsg>,
    web_socket_server: ActorRef<WebSocketServerMsg>,
    message_handler: ActorRef<MessageHandlerMsg>,
    ctx: Arc<ServerCtx>,
    args: ServerArgs,
}

pub struct ServerArgs {
    pub bind_addr: &'static str,
    pub bind_path: &'static str,
    pub bind_port: u16,
    pub kafka_config: KafkaSharedConfig,
    pub public_host_address: &'static str,
}

pub enum ServerMsg {
    Start,
    InitAllModules,
    AllInitComplete,
    RepoManagerMsg(RepoManagerMsg),
    WebSocketServerMsg(WebSocketServerMsg),
    MessageHandlerMsg(MessageHandlerMsg),
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
        let ctx = Arc::new(ServerCtx {
            host_id: Uuid::new_v4(),
        });

        let repo_manager = match Actor::spawn(
            None,
            RepoManager,
            RepoManagerArgs {
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

        let message_handler = match Actor::spawn(
            None,
            MessageHandler,
            MessageHandlerArgs {
                repo_manager: repo_manager.clone(),
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
            repo_manager,
            web_socket_server,
            message_handler,
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
            ServerMsg::Start => {
                info!("Starting Server!");
            }
            ServerMsg::InitAllModules => {
                if let Err(err) = state
                    .repo_manager
                    .send_message(RepoManagerMsg::InitAll(state.args.kafka_config.clone()))
                {
                    error!("Failed to send message to RepoManager: {}", err);
                };
            }
            ServerMsg::AllInitComplete => {
                if let Err(err) = state
                    .repo_manager
                    .send_message(RepoManagerMsg::ProcessEvent(MEvent::from_item(
                        &crate::entities::server::Server {
                            id: state.ctx.host_id.to_string(),
                            address: state.args.public_host_address.to_string(),
                            hash: uuid::Uuid::new_v4().to_string(),
                            port: state.args.bind_port,
                            started_at: Utc::now().to_rfc3339(),
                            version: env!("CARGO_PKG_VERSION").to_string(),
                        },
                        crate::event::MEventType::SET,
                        uuid::Uuid::new_v4().to_string(),
                    )))
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
