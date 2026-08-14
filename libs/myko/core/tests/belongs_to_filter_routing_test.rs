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
    hyphae::{Gettable, Materialize},
    prelude::AnyItem,
    query::IdFilter,
    server::{HandlerRegistry, MykoServerContext, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

fn make_ctx() -> MykoServerContext {
    MykoServerContext::new(
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
    )
}

fn insert_client(ctx: &MykoServerContext, id: &str, server_id: &str) {
    let client = Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from(server_id)),
        address: None,
        windback: None,
    };
    let event = MEvent::from_item(&client, MEventType::SET, &format!("tx-{id}"));
    assert!(ctx.apply_event_batch(vec![event]).is_ok());
}

fn delete_client(ctx: &MykoServerContext, id: &str, server_id: &str) {
    let client = Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from(server_id)),
        address: None,
        windback: None,
    };
    let event = MEvent::from_item(&client, MEventType::DEL, &format!("tx-delete-{id}"));
    assert!(ctx.apply_event_batch(vec![event]).is_ok());
}

fn request(ctx: &MykoServerContext, tx: &str) -> Arc<myko::request::RequestContext> {
    Arc::new(myko::request::RequestContext::from_client(
        Arc::from(tx),
        Arc::from("client-1"),
        ctx.host_id,
    ))
}

/// hyphae's scheduler tick queue is process-wide, so tests in this binary
/// that assert on reactive propagation timing can perturb each other when
/// run concurrently (the default) — see the matching helper (and full
/// rationale) in `query_cache_leak_test.rs`.
fn scheduler_test_serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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

#[test]
fn retained_filtered_query_items_propagate_deletion() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");

    let filter = ClientQuery {
        server_id: Some(IdFilter::Eq(ServerId::from(Arc::<str>::from("server-A")))),
        ..Default::default()
    };
    let live_request = request(&ctx, "tx-live-delete");
    let untyped = ctx.query_map_untyped(GetClientsByQuery(filter.clone()), live_request.clone());
    let live = ctx.query_map(GetClientsByQuery(filter.clone()), live_request);
    let items = live.items().materialize();
    assert_eq!(items.get().len(), 1);

    delete_client(&ctx, "c1", "server-A");

    let snapshot = ctx.query_snapshot(
        GetClientsByQuery(filter),
        request(&ctx, "tx-snapshot-delete"),
    );
    assert_eq!(
        snapshot.len(),
        0,
        "the store snapshot must observe the delete"
    );
    assert_eq!(
        untyped.snapshot().len(),
        0,
        "the untyped live query must propagate the delete"
    );
    assert_eq!(
        items.get().len(),
        0,
        "the retained filtered live query must propagate the delete"
    );
}

/// Regression: an UPDATE to a non-FK field of an item already in a routed
/// bucket must propagate through the filtered view (rship's dynamic-anchor
/// "value flip doesn't propagate" failure shape, 2026-07-18).
#[test]
fn update_to_non_fk_field_propagates_through_routed_view() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");

    let filter = ClientQuery {
        server_id: Some(IdFilter::Eq(ServerId::from(Arc::<str>::from("server-A")))),
        ..Default::default()
    };
    let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx-1"));
    let snap = cell.snapshot();
    assert_eq!(snap.len(), 1);

    // Update c1 in place: same server_id (FK unchanged), new address.
    let updated = Client {
        id: "c1".into(),
        server_id: ServerId::from(Arc::<str>::from("server-A")),
        address: Some("10.0.0.9".into()),
        windback: None,
    };
    let event = MEvent::from_item(&updated, MEventType::SET, "tx-upd");
    assert!(ctx.apply_event_batch(vec![event]).is_ok());

    let snap = cell.snapshot();
    assert_eq!(snap.len(), 1, "item must remain in the view");
    let item = snap.first().map(|entry| entry.1.clone());
    assert!(item.is_some(), "expected routed item");
    let Some(item) = item else {
        return;
    };
    let any_ref: &dyn std::any::Any = item.as_ref().as_any();
    let client = any_ref.downcast_ref::<Client>();
    assert!(client.is_some(), "downcast Client");
    let Some(client) = client else {
        return;
    };
    assert_eq!(
        client.address.as_deref(),
        Some("10.0.0.9"),
        "update to non-FK field must propagate through the routed filtered view"
    );
}

/// A value-based query pinning `id` must route through the store's per-id
/// cells (primary-key route), stay reactive to later inserts of ids in the
/// set, and let secondary pinned fields narrow the id-selected rows.
#[test]
fn id_filter_routes_through_per_id_cells_and_stays_reactive() {
    use myko::entities::client::ClientId;

    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");

    let filter = ClientQuery {
        id: Some(IdFilter::In(vec![
            ClientId::from(Arc::<str>::from("c1")),
            ClientId::from(Arc::<str>::from("c3")),
        ])),
        ..Default::default()
    };
    let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx-id-1"));
    assert_eq!(cell.snapshot().len(), 1);

    // An id in the set inserted AFTER subscription must appear.
    insert_client(&ctx, "c3", "server-C");
    assert_eq!(cell.snapshot().len(), 2);

    // An id outside the set never appears.
    insert_client(&ctx, "c4", "server-A");
    assert_eq!(cell.snapshot().len(), 2);

    // id + belongs_to combined: id wins the route, server_id narrows.
    let narrowed = ClientQuery {
        id: Some(IdFilter::In(vec![
            ClientId::from(Arc::<str>::from("c1")),
            ClientId::from(Arc::<str>::from("c2")),
        ])),
        server_id: Some(IdFilter::Eq(ServerId::from(Arc::<str>::from("server-B")))),
        ..Default::default()
    };
    let cell = ctx.query_map(GetClientsByQuery(narrowed), request(&ctx, "tx-id-2"));
    let snap = cell.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap.first().map(|entry| entry.0.as_ref()), Some("c2"));
}
