//! `GetXById` hands on the row the store holds, not a copy of it.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use myko::{
    entities::{
        client::{Client, GetClientById},
        server::ServerId,
    },
    hyphae::Gettable,
    prelude::AnyItem,
    server::{HandlerRegistry, MykoServerContext, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
};
use uuid::Uuid;

#[test]
fn a_by_id_report_returns_the_stored_row() {
    let ctx = MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(myko::search::SearchIndex::new()),
        myko::server::MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    );
    let client = Client {
        id: "c1".into(),
        server_id: ServerId::from(Arc::<str>::from("server-A")),
        address: None,
        windback: None,
    };
    assert!(ctx.set(client).is_ok());
    let stored: Arc<dyn AnyItem> = ctx
        .registry
        .get_or_create("Client")
        .get_value(&Arc::from("c1"))
        .expect("stored row");

    let report = ctx.report(
        GetClientById { id: "c1".into() },
        ctx.new_server_transaction(),
    );
    let output = report.get();
    let row = (*output).as_ref().expect("report row");
    let stored: Arc<dyn std::any::Any + Send + Sync> = stored;
    let row_any: Arc<dyn std::any::Any + Send + Sync> = row.clone();
    assert!(
        Arc::ptr_eq(&stored, &row_any),
        "the report returned a copy of the row"
    );
}
