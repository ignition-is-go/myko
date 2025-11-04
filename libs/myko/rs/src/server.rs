use log::error;
use ractor::Actor;

use crate::{
    actors::{
        common::REPO_MANAGER_NAME,
        kafka_common::KafkaSharedConfig,
        repo_manager::init_all,
        websocket_server::{WebSocketServer, WebSocketServerArgs},
    },
    entities::client::Client,
    item::Eventable,
};

pub struct Server {
    config: ServerConfig,
}

pub struct ServerConfig {
    pub kafka_brokers: &'static [&'static str],
}

impl Server {
    pub fn new(config: ServerConfig) -> Server {
        Server { config: config }
    }

    pub async fn start(&self) {
        let _ = Client::register().await;

        let manager = ractor::registry::where_is(String::from(REPO_MANAGER_NAME));

        if manager.is_none() {
            error!("Repo manager not found - likely no modules have been registered");
            return;
        }

        let _ = init_all(KafkaSharedConfig {
            bootstrap_servers: self.config.kafka_brokers,
        })
        .await;

        let (_, join_handle) = Actor::spawn(
            None,
            WebSocketServer,
            WebSocketServerArgs {
                min_port: 5155,
                max_port: 5158,
            },
        )
        .await
        .expect("Could not start socket server");

        let _ = join_handle.await;
    }
}
