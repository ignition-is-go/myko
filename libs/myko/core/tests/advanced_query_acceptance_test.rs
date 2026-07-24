//! Phase-1 acceptance-criteria tests for
//! docs/superpowers/specs/2026-07-13-advanced-query-design.md (criteria
//! 2-4; criterion 1's unit coverage lives in filter.rs's own test module,
//! criterion 5 needs the TS codegen pipeline, criterion 6 is a separate
//! `cargo check --target wasm32-unknown-unknown` run, criterion 7 is phase
//! 2 / out of scope here).

#![cfg(feature = "bench")]

use std::sync::Arc;

use myko::{
    bench_entities::{
        BenchCompoundChild, BenchCompoundChildQuery, BenchParentAId, BenchParentBId,
        GetBenchCompoundChildsByQuery,
    },
    entities::{
        client::{Client, ClientQuery, GetClientsByQuery},
        server::ServerId,
    },
    query::{IdFilter, query_runtime_metrics_by_id},
    server::{MykoServerContext, HandlerRegistry, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

/// hyphae's scheduler tick queue is process-wide — serialize against other
/// tests in this binary the same way query_cache_leak_test.rs does.
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

fn insert_compound_child(
    ctx: &MykoServerContext,
    id: &str,
    parent_a: &str,
    parent_b: &str,
    value: i64,
) {
    let child = BenchCompoundChild {
        id: id.into(),
        parent_a_id: BenchParentAId::from(Arc::<str>::from(parent_a)),
        parent_b_id: BenchParentBId::from(Arc::<str>::from(parent_b)),
        value,
    };
    let event = MEvent::from_item(&child, MEventType::SET, &format!("tx-{id}"));
    ctx.apply_event_batch(vec![event]).unwrap();
}

fn request(ctx: &MykoServerContext, tx: &str) -> Arc<myko::request::RequestContext> {
    Arc::new(myko::request::RequestContext::from_client(
        Arc::from(tx),
        Arc::from("client-1"),
        ctx.host_id,
    ))
}

// ─────────────────────────────────────────────────────────────────────────
// Acceptance criterion 2: routing, including a 2-belongs_to compound case.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn compound_two_belongs_to_in_filter_returns_exact_union() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    // Cartesian product of parent_a_id In(A1,A2) x parent_b_id In(B1,B2)
    // is 4 compound keys; only children matching one of those 4 exact
    // (parent_a, parent_b) pairs should appear.
    insert_compound_child(&ctx, "c1", "A1", "B1", 1); // matches
    insert_compound_child(&ctx, "c2", "A2", "B2", 2); // matches
    insert_compound_child(&ctx, "c3", "A1", "B3", 3); // parent_b not in set
    insert_compound_child(&ctx, "c4", "A3", "B1", 4); // parent_a not in set

    let filter = BenchCompoundChildQuery {
        parent_a_id: Some(IdFilter::In(vec![
            BenchParentAId::from(Arc::<str>::from("A1")),
            BenchParentAId::from(Arc::<str>::from("A2")),
        ])),
        parent_b_id: Some(IdFilter::In(vec![
            BenchParentBId::from(Arc::<str>::from("B1")),
            BenchParentBId::from(Arc::<str>::from("B2")),
        ])),
        ..Default::default()
    };
    let cell = ctx.query_map(GetBenchCompoundChildsByQuery(filter), request(&ctx, "tx-1"));
    assert_eq!(
        cell.snapshot().len(),
        2,
        "must return exactly c1 and c2 — the only items matching one of the 4 compound keys"
    );
}

#[test]
fn writes_to_non_matching_belongs_to_buckets_do_not_change_the_result() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");

    let filter = ClientQuery {
        server_id: Some(IdFilter::In(vec![ServerId::from(Arc::<str>::from(
            "server-A",
        ))])),
        ..Default::default()
    };
    let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx-1"));
    assert_eq!(cell.snapshot().len(), 1);

    // Writes to servers outside the In set must not appear — the union
    // only ever subscribes to the buckets it was given, so these writes
    // never reach it structurally, not just "produce no visible change".
    for i in 0..10 {
        insert_client(&ctx, &format!("other-{i}"), &format!("server-{i}"));
    }
    assert_eq!(
        cell.snapshot().len(),
        1,
        "10 writes to non-matching server buckets must not affect the result"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Acceptance criterion 3: reactive tick on mutate-into and mutate-out.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn item_moving_out_of_the_in_set_disappears() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");

    let filter = ClientQuery {
        server_id: Some(IdFilter::In(vec![ServerId::from(Arc::<str>::from(
            "server-A",
        ))])),
        ..Default::default()
    };
    let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx-1"));
    assert_eq!(cell.snapshot().len(), 1);

    // Re-SET the same client id under a server OUTSIDE the In set — an fk
    // mutation, not an insert. Must disappear from the result.
    insert_client(&ctx, "c1", "server-Z");
    assert_eq!(
        cell.snapshot().len(),
        0,
        "an item whose fk mutates OUT of the In set must be removed from the result"
    );
}

#[test]
fn item_moving_into_the_in_set_appears() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-Z"); // starts outside the In set

    let filter = ClientQuery {
        server_id: Some(IdFilter::In(vec![ServerId::from(Arc::<str>::from(
            "server-A",
        ))])),
        ..Default::default()
    };
    let cell = ctx.query_map(GetClientsByQuery(filter), request(&ctx, "tx-1"));
    assert_eq!(cell.snapshot().len(), 0);

    // Re-SET the same client id under a server INSIDE the In set.
    insert_client(&ctx, "c1", "server-A");
    assert_eq!(
        cell.snapshot().len(),
        1,
        "an item whose fk mutates INTO the In set must appear in the result"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Acceptance criterion 4: cache sharing for equivalent advanced queries.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn permuted_in_filters_share_one_query_cell() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");

    let before = query_runtime_metrics_by_id(usize::MAX)
        .into_iter()
        .find(|m| m.query_id.as_ref() == "GetClientsByQuery")
        .map(|m| m.cell_factories_created)
        .unwrap_or(0);

    // Two DIFFERENT call sites, same logical filter, permuted + duplicated
    // In array — must canonicalize to the SAME cache key.
    let filter_a = ClientQuery {
        server_id: Some(IdFilter::In(vec![
            ServerId::from(Arc::<str>::from("server-A")),
            ServerId::from(Arc::<str>::from("server-B")),
        ])),
        ..Default::default()
    };
    let filter_b = ClientQuery {
        server_id: Some(IdFilter::In(vec![
            ServerId::from(Arc::<str>::from("server-B")),
            ServerId::from(Arc::<str>::from("server-A")),
            ServerId::from(Arc::<str>::from("server-B")),
        ])),
        ..Default::default()
    };

    let cell_a = ctx.query_map(GetClientsByQuery(filter_a), request(&ctx, "tx-a"));
    let cell_b = ctx.query_map(GetClientsByQuery(filter_b), request(&ctx, "tx-b"));
    assert_eq!(cell_a.snapshot().len(), 2);
    assert_eq!(cell_b.snapshot().len(), 2);

    let after = query_runtime_metrics_by_id(usize::MAX)
        .into_iter()
        .find(|m| m.query_id.as_ref() == "GetClientsByQuery")
        .map(|m| m.cell_factories_created)
        .unwrap_or(0);

    assert_eq!(
        after - before,
        1,
        "two equivalent (canonicalization-wise) advanced queries from different call sites \
         must share one query cell — exactly one cell_factory invocation, not two"
    );
}

#[test]
fn distinct_filters_do_not_share_a_query_cell() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    insert_client(&ctx, "c1", "server-A");
    insert_client(&ctx, "c2", "server-B");

    let before = query_runtime_metrics_by_id(usize::MAX)
        .into_iter()
        .find(|m| m.query_id.as_ref() == "GetClientsByQuery")
        .map(|m| m.cell_factories_created)
        .unwrap_or(0);

    let filter_a = ClientQuery {
        server_id: Some(IdFilter::Eq(ServerId::from(Arc::<str>::from("server-A")))),
        ..Default::default()
    };
    let filter_c = ClientQuery {
        server_id: Some(IdFilter::Eq(ServerId::from(Arc::<str>::from("server-C")))),
        ..Default::default()
    };
    let _cell_a = ctx.query_map(GetClientsByQuery(filter_a), request(&ctx, "tx-a"));
    let _cell_c = ctx.query_map(GetClientsByQuery(filter_c), request(&ctx, "tx-c"));

    let after = query_runtime_metrics_by_id(usize::MAX)
        .into_iter()
        .find(|m| m.query_id.as_ref() == "GetClientsByQuery")
        .map(|m| m.cell_factories_created)
        .unwrap_or(0);

    assert_eq!(
        after - before,
        2,
        "genuinely distinct filters must NOT collapse onto one cache entry"
    );
}
