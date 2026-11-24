//! `hydrate_snapshot` loads a fixture the way startup loads Postgres.

use std::sync::Arc;

use myko::{
    entities::{
        client::Client,
        server::{Server, ServerId},
    },
    wire::{MEvent, MEventType},
};
use myko_server::MykoServer;

fn client(id: &str, server: &str) -> Client {
    Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from(server)),
        address: None,
        windback: None,
    }
}

#[test]
fn a_snapshot_lands_in_the_stores_without_the_mutation_pipeline() {
    let server = MykoServer::builder().build();
    // Relations are established after the load, which removes a client
    // whose server is absent, so the fixture carries its server.
    let parent = Server {
        id: "server-A".into(),
        version: "1".into(),
        address: "127.0.0.1".into(),
        port: 1,
        started_at: "2026-01-01T00:00:00Z".into(),
    };
    let loaded = server.hydrate_snapshot([
        MEvent::from_item(&parent, MEventType::SET, "tx-0"),
        MEvent::from_item(&client("c1", "server-A"), MEventType::SET, "tx-1"),
        MEvent::from_item(&client("c2", "server-A"), MEventType::SET, "tx-2"),
        MEvent::from_item(&client("gone", "server-A"), MEventType::DEL, "tx-3"),
    ]);

    assert_eq!(loaded.map(|summary| summary.loaded), Ok(3));
    let store = server.ctx().registry.get_or_create("Client");
    let mut ids: Vec<String> = store
        .snapshot()
        .into_iter()
        .map(|(id, _)| id.to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, ["c1", "c2"]);
}
