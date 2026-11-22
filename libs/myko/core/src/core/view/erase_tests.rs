//! Behaviour and cost of the typed-to-erased step at the view boundary.
//!
//! The cost tests count hyphae cell fanouts (one per scheduled write that
//! reaches the drain) for a single changed row, with views subscribed the way
//! the websocket handler subscribes them: one `cell_factory` call per
//! subscription. Run with `--features profiling -- --test-threads=1 --nocapture`
//! to print the counts.

use std::sync::{Arc, Mutex};

use crate::{
    entities::{
        client::{Client, GetAllClients},
        server::ServerId,
    },
    hyphae::{CellImmutable, CellMap, MapDiff},
    prelude::*,
    server::{HandlerRegistry, MykoServerContext, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
    view::ViewFactory,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

mod views {
    #[allow(clippy::wildcard_imports)]
    use crate::prelude::*;
    use std::sync::Arc;

    use crate::entities::client::{Client, GetAllClients};
    use crate::hyphae::MapValuesExt;

    /// Returns an already-materialized map (the query cache's typed map).
    #[myko_macros::myko_view(Client)]
    pub struct EraseHopMapView {}

    impl ViewHandler for EraseHopMapView {
        fn build_cell(
            ctx: ViewBuildArgs<Self>,
        ) -> impl MapQuery<Key = Arc<str>, Value = Arc<Self::Item>>
        where
            Self: Send + Sync + 'static,
        {
            ctx.view_context.query_map_by_str(GetAllClients {})
        }
    }

    /// Returns an unmaterialized plan, the common shape of real views.
    #[myko_macros::myko_view(Client)]
    pub struct EraseHopPlanView {}

    impl ViewHandler for EraseHopPlanView {
        fn build_cell(
            ctx: ViewBuildArgs<Self>,
        ) -> impl MapQuery<Key = Arc<str>, Value = Arc<Self::Item>>
        where
            Self: Send + Sync + 'static,
        {
            ctx.view_context
                .query_map_by_str(GetAllClients {})
                .filter_map_values(|_, client| (client.windback.is_none()).then(|| client.clone()))
        }
    }

    /// Reads `EraseHopPlanView` through `ctx.view`.
    #[myko_macros::myko_view(Client)]
    pub struct EraseHopReaderView {}

    impl ViewHandler for EraseHopReaderView {
        fn build_cell(
            ctx: ViewBuildArgs<Self>,
        ) -> impl MapQuery<Key = Arc<str>, Value = Arc<Self::Item>>
        where
            Self: Send + Sync + 'static,
        {
            ctx.view_context
                .view(EraseHopPlanView {})
                .filter_map_values(|_, client| Some(client.clone()))
        }
    }
}

use views::{EraseHopMapView, EraseHopPlanView, EraseHopReaderView};

type Erased = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;

fn scheduler_test_serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn make_ctx() -> Arc<MykoServerContext> {
    Arc::new(MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(crate::search::SearchIndex::new()),
        crate::server::MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    ))
}

fn request(ctx: &MykoServerContext, tx: &str) -> Arc<crate::request::RequestContext> {
    Arc::new(crate::request::RequestContext::from_client(
        Arc::from(tx),
        Arc::from("client-1"),
        ctx.host_id,
    ))
}

fn client(id: &str, server_id: &str, address: Option<&str>) -> Client {
    Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from(server_id)),
        address: address.map(Arc::from),
        windback: None,
    }
}

fn set(ctx: &MykoServerContext, item: &Client, tx: &str) {
    let event = MEvent::from_item(item, MEventType::SET, tx);
    assert!(ctx.apply_event_batch(vec![event]).is_ok());
}

fn del(ctx: &MykoServerContext, item: &Client, tx: &str) {
    let event = MEvent::from_item(item, MEventType::DEL, tx);
    assert!(ctx.apply_event_batch(vec![event]).is_ok());
}

/// One websocket-style subscription: a fresh `cell_factory` call.
fn subscribe_ws<V>(ctx: &Arc<MykoServerContext>, view: V, tx: &str) -> Erased
where
    V: ViewFactory + Clone + Send + Sync + 'static,
    V::Item: serde::de::DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    let req = crate::view::ViewRequest::with_tx(view, Arc::from(tx));
    let any_view: Arc<dyn crate::view::AnyView> = Arc::new(req);
    V::cell_factory(
        any_view,
        ctx.registry.clone(),
        request(ctx, tx),
        ctx.clone(),
    )
    .expect("view builds")
}

/// A recorded diff, reduced to what a subscriber can observe.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Initial(Vec<(String, Option<String>)>),
    Insert(String, Option<String>),
    Update(String, Option<String>, Option<String>),
    Remove(String),
}

fn address(item: &Arc<dyn AnyItem>) -> Option<String> {
    item.as_any()
        .downcast_ref::<Client>()
        .and_then(|c| c.address.as_deref().map(str::to_owned))
}

fn record(diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>, out: &mut Vec<Seen>) {
    match diff {
        MapDiff::Initial { entries } => {
            let mut rows: Vec<_> = entries
                .iter()
                .map(|(k, v)| (k.to_string(), address(v)))
                .collect();
            rows.sort();
            out.push(Seen::Initial(rows));
        }
        MapDiff::Insert { key, value } => out.push(Seen::Insert(key.to_string(), address(value))),
        MapDiff::Update {
            key,
            old_value,
            new_value,
        } => out.push(Seen::Update(
            key.to_string(),
            address(old_value),
            address(new_value),
        )),
        MapDiff::Remove { key, .. } => out.push(Seen::Remove(key.to_string())),
        MapDiff::Batch { changes } => changes.iter().for_each(|c| record(c, out)),
    }
}

fn watch(map: &Erased) -> (Arc<Mutex<Vec<Seen>>>, crate::hyphae::SubscriptionGuard) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let guard = map.subscribe_diffs(move |diff| record(diff, &mut sink.lock().unwrap()));
    (seen, guard)
}

fn some(s: &str) -> Option<String> {
    Some(s.to_owned())
}

fn assert_view_contract(ctx: &Arc<MykoServerContext>, map: &Erased) {
    let (seen, guard) = watch(map);
    set(ctx, &client("c2", "s", Some("a2")), "t2");
    set(ctx, &client("c1", "s", Some("a1b")), "t3");
    // Same value again: the view must not re-emit it.
    set(ctx, &client("c1", "s", Some("a1b")), "t4");
    del(ctx, &client("c2", "s", Some("a2")), "t5");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            Seen::Initial(vec![("c1".into(), some("a1"))]),
            Seen::Insert("c2".into(), some("a2")),
            Seen::Update("c1".into(), some("a1"), some("a1b")),
            Seen::Remove("c2".into()),
        ]
    );

    // A late subscriber sees the current rows as one Initial.
    let (late, late_guard) = watch(map);
    assert_eq!(
        *late.lock().unwrap(),
        vec![Seen::Initial(vec![("c1".into(), some("a1b"))])]
    );
    drop((guard, late_guard));
}

#[test]
fn map_shaped_view_keeps_its_diff_contract() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    let map = subscribe_ws(&ctx, EraseHopMapView {}, "sub");
    assert_view_contract(&ctx, &map);
}

#[test]
fn plan_shaped_view_keeps_its_diff_contract() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    let map = subscribe_ws(&ctx, EraseHopPlanView {}, "sub");
    assert_view_contract(&ctx, &map);
}

#[test]
fn view_read_through_ctx_view_keeps_its_diff_contract() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    let map = subscribe_ws(&ctx, EraseHopReaderView {}, "sub");
    assert_view_contract(&ctx, &map);
}

#[test]
fn batched_changes_reach_subscribers_inside_one_batch() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    let map = subscribe_ws(&ctx, EraseHopPlanView {}, "sub");
    let emissions = Arc::new(Mutex::new(Vec::<usize>::new()));
    let sink = emissions.clone();
    let guard = map.subscribe_diffs(move |diff| {
        let n = match diff {
            MapDiff::Batch { changes } => changes.len(),
            _ => 1,
        };
        sink.lock().unwrap().push(n);
    });
    let events = vec![
        MEvent::from_item(&client("c2", "s", Some("a2")), MEventType::SET, "t2"),
        MEvent::from_item(&client("c3", "s", Some("a3")), MEventType::SET, "t2"),
    ];
    assert!(ctx.apply_event_batch(events).is_ok());
    let seen = emissions.lock().unwrap().clone();
    assert_eq!(
        seen.iter().skip(1).sum::<usize>(),
        2,
        "both rows arrive: {seen:?}"
    );
    assert_eq!(map.snapshot().len(), 3);
    drop(guard);
}

#[test]
fn dropping_the_last_handle_tears_the_view_down() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    let map = subscribe_ws(&ctx, EraseHopPlanView {}, "sub");
    let weak = map.downgrade();
    let (seen, guard) = watch(&map);
    drop(guard);
    drop(map);
    assert!(
        weak.upgrade().is_none(),
        "view map outlived its last handle"
    );
    set(&ctx, &client("c1", "s", Some("a1c")), "t2");
    assert_eq!(seen.lock().unwrap().len(), 1, "no diffs after teardown");
}

#[cfg(feature = "profiling")]
mod cost {
    use super::*;
    use crate::hyphae::profiling::{pass, take_report};

    fn fires_for_one_changed_row(ctx: &MykoServerContext, n: usize) -> u64 {
        pass(|| {
            crate::hyphae::batch(|| {
                set(ctx, &client("c1", "s", Some(&format!("addr-{n}"))), "t");
            });
        });
        take_report().expect("pass report").total_fires()
    }

    /// Fanouts for one changed row with `subs` websocket subscriptions to `V`,
    /// minus the same write with no view subscribed.
    fn view_cost<V>(view: V, subs: usize) -> u64
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: serde::de::DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let ctx = make_ctx();
        set(&ctx, &client("c1", "s", Some("a0")), "t0");
        // Keep the query map alive so the baseline includes the query hop.
        let _query = ctx.query_map_by_str(GetAllClients {}, request(&ctx, "base"));
        let _q_guard = _query.subscribe_diffs(|_| {});
        fires_for_one_changed_row(&ctx, 1);
        let base = fires_for_one_changed_row(&ctx, 2);
        let maps: Vec<Erased> = (0..subs)
            .map(|i| subscribe_ws(&ctx, view.clone(), &format!("sub-{i}")))
            .collect();
        let guards: Vec<_> = maps.iter().map(|m| m.subscribe_diffs(|_| {})).collect();
        fires_for_one_changed_row(&ctx, 3);
        let with = fires_for_one_changed_row(&ctx, 4);
        drop((guards, maps));
        with - base
    }

    #[test]
    fn report_fanouts_per_changed_row() {
        let _serial = scheduler_test_serial();
        for subs in [1usize, 4] {
            let map = view_cost(EraseHopMapView {}, subs);
            let plan = view_cost(EraseHopPlanView {}, subs);
            let reader = view_cost(EraseHopReaderView {}, subs);
            println!(
                "fanouts per changed row, {subs} subs: map-view {map} ({:.1}/sub), \
                 plan-view {plan} ({:.1}/sub), ctx.view-reader {reader} ({:.1}/sub)",
                map as f64 / subs as f64,
                plan as f64 / subs as f64,
                reader as f64 / subs as f64,
            );
        }
    }
}
