//! End-to-end test: `In` on a real `#[belongs_to]` field (`Client.server_id`
//! -> `Server`) routes through `BelongsToSourceIndex` and returns correct,
//! reactive results — the spec §4 hard requirement, exercised against
//! production entities rather than the registration.rs-internal test
//! fixtures. Full routing-metrics verification (proving it's index-served,
//! not a scan) is covered separately alongside the rest of the phase-1
//! acceptance criteria.

use std::sync::Arc;

use myko::{
    entities::{
        client::{Client, ClientQuery, GetClientsByQuery},
        server::ServerId,
    },
    query::IdFilter,
    server::{CellServerCtx, HandlerRegistry, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

fn make_ctx() -> CellServerCtx {
    CellServerCtx::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(myko::search::SearchIndex::new()),
        Arc::new(dashmap::DashMap::new()),
        None,
        None,
    )
}

fn insert_client(ctx: &CellServerCtx, id: &str, server_id: &str) {
    let client = Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from(server_id)),
        address: None,
        windback: None,
    };
    let event = MEvent::from_item(&client, MEventType::SET, &format!("tx-{id}"));
    ctx.apply_event_batch(vec![event]).unwrap();
}

fn request(ctx: &CellServerCtx, tx: &str) -> Arc<myko::request::RequestContext> {
    Arc::new(myko::request::RequestContext::from_client(
        Arc::from(tx),
        Arc::from("client-1"),
        ctx.host_id,
    ))
}

/// hyphae's scheduler tick queue is process-wide, so tests in this binary
/// that assert on reactive propagation timing can perturb each other when
/// run concurrently (the default) — see the matching helper (and full
/// rationale) in query_cache_leak_test.rs.
fn scheduler_test_serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn in_filter_on_belongs_to_field_returns_the_union() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");
    insert_client(&ctx, "c3", "server-C"); // not in the In set

    let filter = ClientQuery {
        server_id: Some(IdFilter::In(vec![
            ServerId::from(Arc::<str>::from("server-A")),
            ServerId::from(Arc::<str>::from("server-B")),
        ])),
        ..Default::default()
    };
    let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx-1"));
    assert_eq!(
        cell.snapshot().len(),
        2,
        "must return exactly server-A and server-B's clients"
    );
}

#[test]
fn in_filter_on_belongs_to_field_stays_reactive() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");

    let filter = ClientQuery {
        server_id: Some(IdFilter::In(vec![
            ServerId::from(Arc::<str>::from("server-A")),
            ServerId::from(Arc::<str>::from("server-B")),
        ])),
        ..Default::default()
    };
    let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx-1"));
    assert_eq!(cell.snapshot().len(), 1);

    // A new client under server-B (also in the In set) must appear.
    insert_client(&ctx, "c2", "server-B");
    assert_eq!(cell.snapshot().len(), 2);

    // A new client under a server NOT in the In set must not appear.
    insert_client(&ctx, "c3", "server-C");
    assert_eq!(cell.snapshot().len(), 2);
}

#[test]
fn eq_filter_on_belongs_to_field_still_works() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");

    let filter = ClientQuery {
        server_id: Some(IdFilter::Eq(ServerId::from(Arc::<str>::from("server-A")))),
        ..Default::default()
    };
    let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx-1"));
    assert_eq!(cell.snapshot().len(), 1);
}
