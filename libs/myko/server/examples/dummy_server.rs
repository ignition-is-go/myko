//! In-memory Myko server for client and UI development.
//!
//! Run with:
//! `cargo run -p myko-server --example dummy_server --target-dir target/agent`
//!
//! Override the listener with `MYKO_DEMO_BIND_ADDR` (default: `127.0.0.1:5155`)
//! and its advertised host with `MYKO_DEMO_ADVERTISE_ADDR`.

use std::{env, net::SocketAddr, sync::Arc};

use myko::{
    entities::demo::{DemoStatus, DemoStatusId, DemoTask, DemoTaskId},
    tracing,
};
use myko_server::{MykoServer, NullPersister, peer_registry::PeerRegistryConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _telemetry = myko_server::telemetry::init_from_env();
    let bind_addr = env::var("MYKO_DEMO_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5155".to_owned())
        .parse::<SocketAddr>()?;

    let advertise_address =
        env::var("MYKO_DEMO_ADVERTISE_ADDR").unwrap_or_else(|_| bind_addr.ip().to_string());
    let server = MykoServer::builder()
        .with_bind_addr(bind_addr)
        .with_peer_registry(PeerRegistryConfig {
            address: advertise_address,
            port: bind_addr.port(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        })
        .with_default_persister(Arc::new(NullPersister))
        .after_init(|server| {
            let ctx = server.ctx();
            let statuses = [
                ("demo-status-todo", "Todo", "#718096", "○"),
                ("demo-status-doing", "Doing", "#d69e2e", "◐"),
                ("demo-status-done", "Done", "#38a169", "✓"),
            ];
            for (id, name, color, emoji) in statuses {
                let status = DemoStatus {
                    id: DemoStatusId::from(id),
                    name: name.to_owned(),
                    color: color.to_owned(),
                    emoji: emoji.to_owned(),
                };
                if let Err(error) = ctx.set(&status) {
                    tracing::error!(%error, status_id = id, "failed to seed demo status");
                }
            }

            let tasks = [
                (
                    "demo-task-1",
                    "Start the dummy server",
                    true,
                    "demo-status-done",
                ),
                (
                    "demo-task-2",
                    "Connect the GPUI client",
                    true,
                    "demo-status-done",
                ),
                (
                    "demo-task-3",
                    "Watch live task updates",
                    false,
                    "demo-status-doing",
                ),
            ];
            for (id, title, completed, status_id) in tasks {
                let task = DemoTask {
                    id: DemoTaskId::from(id),
                    title: title.to_owned(),
                    completed,
                    status_id: DemoStatusId::from(status_id),
                };
                if let Err(error) = ctx.set(&task) {
                    tracing::error!(%error, task_id = id, "failed to seed demo task");
                }
            }
        })
        .build();

    server.run().await
}
