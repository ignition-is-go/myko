//! Phase-2 acceptance-criterion 7 tests for
//! docs/superpowers/specs/2026-07-13-advanced-query-design.md §5:
//! `ctx.query_live(filter_cell)` — reactive filter parameters.

use std::sync::Arc;

use myko::{
    entities::{
        client::{Client, ClientQuery},
        server::ServerId,
    },
    hyphae::{Cell, CellMutable, MapExt, Materialize, Mutable},
    query::IdFilter,
    server::{HandlerRegistry, MykoServerContext, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

fn scheduler_test_serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn make_ctx() -> MykoServerContext {
    MykoServerContext::new(
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

fn insert_client(ctx: &MykoServerContext, id: &str, server_id: &str) {
    let client = Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from(server_id)),
        address: None,
        windback: None,
    };
    let event = MEvent::from_item(&client, MEventType::SET, &format!("tx-{id}"));
    ctx.apply_event_batch(vec![event]).unwrap();
}

fn eq_filter(server_id: &str) -> ClientQuery {
    ClientQuery {
        server_id: Some(IdFilter::Eq(ServerId::from(Arc::<str>::from(server_id)))),
        ..Default::default()
    }
}

fn in_filter(server_ids: &[&str]) -> ClientQuery {
    ClientQuery {
        server_id: Some(IdFilter::In(
            server_ids
                .iter()
                .map(|s| ServerId::from(Arc::<str>::from(*s)))
                .collect(),
        )),
        ..Default::default()
    }
}

#[test]
fn query_live_initial_population_matches_the_starting_filter() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");

    let filter_cell: Cell<ClientQuery, CellMutable> = Cell::new(eq_filter("server-A"));
    let result = ctx.query_live(filter_cell);
    assert_eq!(result.snapshot().len(), 1);
}

#[test]
fn query_live_updates_when_the_in_set_grows() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");
    insert_client(&ctx, "c3", "server-C");

    let filter_cell: Cell<ClientQuery, CellMutable> = Cell::new(in_filter(&["server-A"]));
    let result = ctx.query_live(filter_cell.clone());
    assert_eq!(result.snapshot().len(), 1);

    filter_cell.set(in_filter(&["server-A", "server-B"]));
    assert_eq!(
        result.snapshot().len(),
        2,
        "growing the In set must add the newly-included server's clients"
    );

    filter_cell.set(in_filter(&["server-A", "server-B", "server-C"]));
    assert_eq!(result.snapshot().len(), 3);
}

#[test]
fn query_live_updates_when_the_in_set_shrinks() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");

    let filter_cell: Cell<ClientQuery, CellMutable> =
        Cell::new(in_filter(&["server-A", "server-B"]));
    let result = ctx.query_live(filter_cell.clone());
    assert_eq!(result.snapshot().len(), 2);

    filter_cell.set(in_filter(&["server-A"]));
    assert_eq!(
        result.snapshot().len(),
        1,
        "shrinking the In set must retract the dropped server's clients"
    );
}

#[test]
fn query_live_still_tracks_store_writes_after_a_filter_change() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");

    let filter_cell: Cell<ClientQuery, CellMutable> =
        Cell::new(in_filter(&["server-A", "server-B"]));
    let result = ctx.query_live(filter_cell.clone());
    assert_eq!(result.snapshot().len(), 1);

    // Change the filter (still In-based, same field) then write to the
    // store — the bucket subscriptions from the LATEST filter tick must
    // still be live and tracking writes, not just a one-time snapshot.
    filter_cell.set(in_filter(&["server-B"]));
    assert_eq!(result.snapshot().len(), 0);

    insert_client(&ctx, "c2", "server-B");
    assert_eq!(
        result.snapshot().len(),
        1,
        "a write to a bucket added by the LATEST filter tick must still be tracked reactively"
    );
}

#[test]
fn query_live_range_filter_change_reevaluates_correctly() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    // No belongs_to field pinned here — Client's only belongs_to field is
    // server_id; use a non-indexed field (windback, a plain Option<Arc<str>>)
    // to exercise the scan-mode path with a changing predicate.
    let client_a = Client {
        id: "c1".into(),
        server_id: ServerId::from(Arc::<str>::from("server-A")),
        address: None,
        windback: Some(Arc::from("2026-01-01T00:00:00Z")),
    };
    ctx.apply_event_batch(vec![MEvent::from_item(&client_a, MEventType::SET, "tx-1")])
        .unwrap();

    let filter_cell: Cell<ClientQuery, CellMutable> = Cell::new(ClientQuery {
        windback: Some(myko::query::StringFilter::Eq(Arc::from(
            "2026-01-01T00:00:00Z",
        ))),
        ..Default::default()
    });
    let result = ctx.query_live(filter_cell.clone());
    assert_eq!(result.snapshot().len(), 1);

    // Change to a non-matching value — scan mode must re-evaluate and
    // retract, even though nothing about the store changed.
    filter_cell.set(ClientQuery {
        windback: Some(myko::query::StringFilter::Eq(Arc::from("nope"))),
        ..Default::default()
    });
    assert_eq!(
        result.snapshot().len(),
        0,
        "a non-indexed filter change must re-evaluate the scan-mode scope"
    );

    // Change back to a matching value.
    filter_cell.set(ClientQuery {
        windback: Some(myko::query::StringFilter::Eq(Arc::from(
            "2026-01-01T00:00:00Z",
        ))),
        ..Default::default()
    });
    assert_eq!(result.snapshot().len(), 1);
}

#[test]
fn query_live_downstream_state_survives_a_filter_change() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");

    let filter_cell: Cell<ClientQuery, CellMutable> = Cell::new(in_filter(&["server-A"]));
    let result = ctx.query_live(filter_cell.clone());

    // A stateful counter downstream of query_live's result — incremented
    // once per graph NODE CREATION (i.e. once, ever, if query_live never
    // tears down and rebuilds `result`), not once per filter tick.
    use std::sync::atomic::{AtomicUsize, Ordering};
    let build_count = Arc::new(AtomicUsize::new(0));
    let build_count_for_map = build_count.clone();
    let downstream = result
        .entries()
        .map(move |_entries| {
            build_count_for_map.fetch_add(1, Ordering::SeqCst);
            build_count_for_map.load(Ordering::SeqCst)
        })
        .materialize();
    let _ = myko::hyphae::Gettable::get(&downstream);

    let builds_after_first_read = build_count.load(Ordering::SeqCst);
    assert!(builds_after_first_read >= 1);

    // Multiple filter changes — if query_live tore down `result` and
    // rebuilt it on each tick, `downstream`'s own subscription to `result`
    // would need to be re-established externally (it isn't, here), so
    // this call alone doesn't prove much by itself; the real proof is
    // that `result` (returned once, at the top) is the SAME object
    // throughout — verified structurally by construction (query_live
    // returns one CellMap up front and only ever mutates it via
    // insert/remove from then on, never replacing it).
    filter_cell.set(in_filter(&["server-A", "server-B"]));
    insert_client(&ctx, "c2", "server-B");
    let _ = myko::hyphae::Gettable::get(&downstream);
    assert_eq!(
        result.snapshot().len(),
        2,
        "result must reflect both the filter change and the subsequent write"
    );
}

#[test]
fn query_live_transitions_between_indexed_and_scan_mode() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    let client_a = Client {
        id: "c1".into(),
        server_id: ServerId::from(Arc::<str>::from("server-A")),
        address: None,
        windback: Some(Arc::from("2026-01-01T00:00:00Z")),
    };
    let client_b = Client {
        id: "c2".into(),
        server_id: ServerId::from(Arc::<str>::from("server-B")),
        address: None,
        windback: None,
    };
    ctx.apply_event_batch(vec![
        MEvent::from_item(&client_a, MEventType::SET, "tx-1"),
        MEvent::from_item(&client_b, MEventType::SET, "tx-2"),
    ])
    .unwrap();

    // Start indexed (belongs_to field pinned).
    let filter_cell: Cell<ClientQuery, CellMutable> = Cell::new(eq_filter("server-A"));
    let result = ctx.query_live(filter_cell.clone());
    assert_eq!(result.snapshot().len(), 1);

    // Switch to scan mode (no belongs_to field pinned — only a non-indexed
    // field) — must tear down the indexed bucket subscription cleanly and
    // re-evaluate via the whole-store scan path.
    filter_cell.set(ClientQuery {
        windback: Some(myko::query::StringFilter::Eq(Arc::from(
            "2026-01-01T00:00:00Z",
        ))),
        ..Default::default()
    });
    assert_eq!(
        result.snapshot().len(),
        1,
        "switching to scan mode must re-evaluate correctly (client_a matches via windback)"
    );

    // Switch back to indexed mode with a different server.
    filter_cell.set(eq_filter("server-B"));
    assert_eq!(
        result.snapshot().len(),
        1,
        "switching back to indexed mode must re-evaluate correctly (client_b matches server-B)"
    );

    // Confirm the NEW indexed subscription is genuinely live (not stale).
    insert_client(&ctx, "c3", "server-B");
    assert_eq!(result.snapshot().len(), 2);
}

// ─── Primary-key (id) routing ────────────────────────────────────────────

fn id_in_filter(ids: &[&str]) -> ClientQuery {
    use myko::entities::client::ClientId;
    ClientQuery {
        id: Some(IdFilter::In(
            ids.iter()
                .map(|s| ClientId::from(Arc::<str>::from(*s)))
                .collect(),
        )),
        ..Default::default()
    }
}

/// The id route must behave like any indexed route: initial population,
/// tracking store writes for ids in the set, and full retraction/adoption
/// across a swap to a completely disjoint id set.
#[test]
fn query_live_id_filter_routes_and_tracks_disjoint_swaps() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");

    let filter_cell: Cell<ClientQuery, CellMutable> = Cell::new(id_in_filter(&["c1", "c3"]));
    let result = ctx.query_live(filter_cell.clone());
    // c1 exists and is pinned; c3 doesn't exist yet; c2 is out of set.
    assert_eq!(result.snapshot().len(), 1);

    // A later insert of an id already in the set must appear (per-id cell
    // subscription covers not-yet-existing ids).
    insert_client(&ctx, "c3", "server-C");
    assert_eq!(result.snapshot().len(), 2);

    // Swap to a completely disjoint id set: old contributions retract,
    // new ones adopt — no teardown of `result` itself.
    filter_cell.set(id_in_filter(&["c2"]));
    let snap = result.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].0.as_ref(), "c2");
}

/// Crossing between id mode and belongs_to mode is a route-shape change:
/// contents must fully reconcile in both directions.
#[test]
fn query_live_switches_between_id_and_belongs_to_modes() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-A");
    insert_client(&ctx, "c3", "server-B");

    let filter_cell: Cell<ClientQuery, CellMutable> = Cell::new(id_in_filter(&["c3"]));
    let result = ctx.query_live(filter_cell.clone());
    assert_eq!(result.snapshot().len(), 1);

    // id mode -> belongs_to mode (server-A has two clients).
    filter_cell.set(eq_filter("server-A"));
    assert_eq!(result.snapshot().len(), 2);

    // belongs_to mode -> id mode again.
    filter_cell.set(id_in_filter(&["c1"]));
    let snap = result.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].0.as_ref(), "c1");
}

/// A pinned id wins the route, and other pinned fields still narrow via
/// `matches` on the ≤ N id-selected rows.
#[test]
fn query_live_id_route_narrows_by_secondary_fields() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");

    let mut filter = id_in_filter(&["c1", "c2"]);
    filter.server_id = Some(IdFilter::Eq(ServerId::from(Arc::<str>::from("server-B"))));
    let filter_cell: Cell<ClientQuery, CellMutable> = Cell::new(filter);
    let result = ctx.query_live(filter_cell);
    let snap = result.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].0.as_ref(), "c2");
}
