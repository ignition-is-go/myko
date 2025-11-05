use std::ops::Range;

use log::{error, info};
use ractor::{Actor, ActorProcessingErr, ActorRef};

use crate::actors::{
    kafka_common::KafkaSharedConfig,
    message_handler::{MessageHandler, MessageHandlerArgs, MessageHandlerMsg},
    repo_manager::{RepoManager, RepoManagerArgs, RepoManagerMsg},
    websocket_server::{WebSocketServer, WebSocketServerArgs, WebSocketServerMsg},
};

pub struct Server;

pub struct ServerState {
    repo_manager: ActorRef<RepoManagerMsg>,
    web_socket_server: ActorRef<WebSocketServerMsg>,
    message_handler: ActorRef<MessageHandlerMsg>,
    kafka_config: KafkaSharedConfig,
}

pub struct ServerArgs {
    pub bind_addr: &'static str,
    pub bind_path: &'static str,
    pub bind_port: Range<u16>,
    pub kafka_config: KafkaSharedConfig,
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
        let repo_manager = match Actor::spawn(
            None,
            RepoManager,
            RepoManagerArgs {
                server: myself.clone(),
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
                min_port: args.bind_port.start,
                max_port: args.bind_port.end,
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
            kafka_config: args.kafka_config,
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
                    .send_message(RepoManagerMsg::InitAll(state.kafka_config.clone()))
                {
                    error!("Failed to send message to RepoManager: {}", err);
                };
            }
            ServerMsg::AllInitComplete => {
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
