//! Behaviour and cost of the typed-to-erased step at the view boundary.
//!
//! The cost tests count hyphae cell fanouts (one per scheduled write that
//! reaches the drain) for a single changed row, with views subscribed the way
//! the websocket handler subscribes them: one `cell_factory` call per
//! subscription, and one `subscribe_factory` call per subscription for the
//! erase-on-read path the handler uses now. Run with `--features profiling -- --test-threads=1 --nocapture`
//! to print the counts.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::redundant_clone,
    clippy::unwrap_used
)]

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
    view::ErasedViewMap,
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

    /// Materializes its own typed map, the shape of a hand-written union.
    #[myko_macros::myko_view(Client)]
    pub struct EraseHopUnionView {}

    impl ViewHandler for EraseHopUnionView {
        fn build_cell(
            ctx: ViewBuildArgs<Self>,
        ) -> impl MapQuery<Key = Arc<str>, Value = Arc<Self::Item>>
        where
            Self: Send + Sync + 'static,
        {
            ctx.view_context
                .query_map_by_str(GetAllClients {})
                .filter_map_values(|_, client| Some(client.clone()))
                .materialize()
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

    /// Serves live subscriptions by gating `EraseHopUnionView`, read once
    /// through the shared view cache.
    #[myko_macros::myko_view(Client)]
    pub struct EraseHopGatedView {}

    impl ViewHandler for EraseHopGatedView {
        fn build_cell(
            ctx: ViewBuildArgs<Self>,
        ) -> impl MapQuery<Key = Arc<str>, Value = Arc<Self::Item>>
        where
            Self: Send + Sync + 'static,
        {
            ctx.view_context.view(EraseHopUnionView {})
        }

        fn subscribe(ctx: &ViewBuildArgs<Self>) -> Option<crate::view::ErasedViewMap> {
            let view_ctx = ctx.view_context.clone();
            Some(crate::view::ErasedViewMap::switched(
                crate::hyphae::Cell::new(Some(())).lock(),
                move |()| view_ctx.view_map_untyped(EraseHopUnionView {}).into(),
            ))
        }
    }
}

use views::{EraseHopMapView, EraseHopPlanView, EraseHopReaderView, EraseHopUnionView};

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

/// One subscription the way the websocket handler makes it now.
fn subscribe_live<V>(ctx: &Arc<MykoServerContext>, view: V, tx: &str) -> ErasedViewMap
where
    V: ViewFactory + Clone + Send + Sync + 'static,
    V::Item: serde::de::DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    let req = crate::view::ViewRequest::with_tx(view, Arc::from(tx));
    let any_view: Arc<dyn crate::view::AnyView> = Arc::new(req);
    V::subscribe_factory(
        any_view,
        ctx.registry.clone(),
        request(ctx, tx),
        ctx.clone(),
    )
    .expect("view builds")
}

/// Both subscription paths for one view.
fn both_paths<V>(ctx: &Arc<MykoServerContext>, view: V) -> [ErasedViewMap; 2]
where
    V: ViewFactory + Clone + Send + Sync + 'static,
    V::Item: serde::de::DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    [
        subscribe_ws(ctx, view.clone(), "sub-cell").into(),
        subscribe_live(ctx, view, "sub-live"),
    ]
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

fn watch(map: &ErasedViewMap) -> (Arc<Mutex<Vec<Seen>>>, crate::hyphae::SubscriptionGuard) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let guard = map.subscribe_diffs(move |diff| record(diff, &mut sink.lock().unwrap()));
    (seen, guard)
}

fn assert_view_contract(ctx: &Arc<MykoServerContext>, map: &ErasedViewMap) {
    let (seen, guard) = watch(map);
    set(ctx, &client("c2", "s", Some("a2")), "t2");
    set(ctx, &client("c1", "s", Some("a1b")), "t3");
    // Same value again: the view must not re-emit it.
    set(ctx, &client("c1", "s", Some("a1b")), "t4");
    del(ctx, &client("c2", "s", Some("a2")), "t5");
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            Seen::Initial(vec![("c1".into(), Some("a1".to_owned()))]),
            Seen::Insert("c2".into(), Some("a2".to_owned())),
            Seen::Update("c1".into(), Some("a1".to_owned()), Some("a1b".to_owned())),
            Seen::Remove("c2".into()),
        ]
    );

    // A late subscriber sees the current rows as one Initial.
    let (late, late_guard) = watch(map);
    assert_eq!(
        *late.lock().unwrap(),
        vec![Seen::Initial(vec![("c1".into(), Some("a1b".to_owned()))])]
    );
    drop((guard, late_guard));
}

#[test]
fn map_shaped_view_keeps_its_diff_contract() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    let map = subscribe_ws(&ctx, EraseHopMapView {}, "sub");
    assert_view_contract(&ctx, &map.into());
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    assert_view_contract(&ctx, &subscribe_live(&ctx, EraseHopMapView {}, "sub"));
}

#[test]
fn plan_shaped_view_keeps_its_diff_contract() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    let map = subscribe_ws(&ctx, EraseHopPlanView {}, "sub");
    assert_view_contract(&ctx, &map.into());
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    assert_view_contract(&ctx, &subscribe_live(&ctx, EraseHopPlanView {}, "sub"));
}

#[test]
fn view_read_through_ctx_view_keeps_its_diff_contract() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    let map = subscribe_ws(&ctx, EraseHopReaderView {}, "sub");
    assert_view_contract(&ctx, &map.into());
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    assert_view_contract(&ctx, &subscribe_live(&ctx, EraseHopReaderView {}, "sub"));
}

#[test]
fn union_shaped_view_keeps_its_diff_contract() {
    let _serial = scheduler_test_serial();
    for live in [false, true] {
        let ctx = make_ctx();
        set(&ctx, &client("c1", "s", Some("a1")), "t1");
        let map = if live {
            subscribe_live(&ctx, EraseHopUnionView {}, "sub")
        } else {
            subscribe_ws(&ctx, EraseHopUnionView {}, "sub").into()
        };
        assert_view_contract(&ctx, &map);
    }
}

#[test]
fn both_paths_deliver_identical_diff_sequences() {
    let _serial = scheduler_test_serial();
    for union in [false, true] {
        let ctx = make_ctx();
        set(&ctx, &client("c1", "s", Some("a1")), "t1");
        let view_paths = if union {
            both_paths(&ctx, EraseHopUnionView {})
        } else {
            both_paths(&ctx, EraseHopMapView {})
        };
        let watched: Vec<_> = view_paths.iter().map(watch).collect();
        set(&ctx, &client("c2", "s", Some("a2")), "t2");
        set(&ctx, &client("c1", "s", Some("a1b")), "t3");
        set(&ctx, &client("c1", "s", Some("a1b")), "t4");
        let events = vec![
            MEvent::from_item(&client("c3", "s", Some("a3")), MEventType::SET, "t5"),
            MEvent::from_item(&client("c1", "s", Some("a1c")), MEventType::SET, "t5"),
            MEvent::from_item(&client("c3", "s", Some("a3")), MEventType::DEL, "t5"),
        ];
        assert!(ctx.apply_event_batch(events).is_ok());
        del(&ctx, &client("c2", "s", Some("a2")), "t6");
        let cell = watched[0].0.lock().unwrap().clone();
        let live = watched[1].0.lock().unwrap().clone();
        assert_eq!(cell, live);
        let mut a = view_paths[0].snapshot();
        let mut b = view_paths[1].snapshot();
        a.sort_by(|x, y| x.0.cmp(&y.0));
        b.sort_by(|x, y| x.0.cmp(&y.0));
        assert_eq!(a, b);
    }
}

/// The erased mirror drops an `Update` whose value equals the one it stores.
/// Every `CellMap` writer already drops such an `Update` before emitting it,
/// so reading the typed map directly sees the same sequence. Drive a typed
/// map through each writer and compare.
#[test]
fn erased_mirror_drops_nothing_the_typed_map_emits() {
    let _serial = scheduler_test_serial();
    let typed = CellMap::<Arc<str>, Arc<Client>>::new();
    let mirror: ErasedViewMap = super::cell::materialize_erased_view(typed.clone().lock()).into();
    let direct = ErasedViewMap::from_typed(typed.clone().lock());
    let (mirror_seen, _g1) = watch(&mirror);
    let (direct_seen, _g2) = watch(&direct);
    let a = Arc::new(client("c1", "s", Some("a")));
    let b = Arc::new(client("c1", "s", Some("b")));
    let equal_b = Arc::new(client("c1", "s", Some("b")));
    let k: Arc<str> = "c1".into();
    typed.insert(k.clone(), a.clone());
    typed.insert(k.clone(), b.clone());
    typed.insert(k.clone(), equal_b.clone());
    typed.insert_many(vec![(k.clone(), equal_b.clone())]);
    typed.replace_all(vec![(k.clone(), equal_b.clone())]);
    typed.apply_diff_owned(MapDiff::Update {
        key: k.clone(),
        old_value: b.clone(),
        new_value: equal_b.clone(),
    });
    typed.apply_batch(vec![
        MapDiff::Update {
            key: k.clone(),
            old_value: b.clone(),
            new_value: equal_b.clone(),
        },
        MapDiff::Update {
            key: k.clone(),
            old_value: b.clone(),
            new_value: a.clone(),
        },
        MapDiff::Insert {
            key: k.clone(),
            value: a.clone(),
        },
    ]);
    typed.remove(&k);
    typed.apply_diff_owned(MapDiff::Remove {
        key: k.clone(),
        old_value: a.clone(),
    });
    assert_eq!(*mirror_seen.lock().unwrap(), *direct_seen.lock().unwrap());
    assert!(direct_seen.lock().unwrap().len() > 3);
}

#[test]
fn batched_changes_reach_subscribers_inside_one_batch() {
    let _serial = scheduler_test_serial();
    for live in [false, true] {
        let ctx = make_ctx();
        set(&ctx, &client("c1", "s", Some("a1")), "t1");
        let map: ErasedViewMap = if live {
            subscribe_live(&ctx, EraseHopUnionView {}, "sub")
        } else {
            subscribe_ws(&ctx, EraseHopPlanView {}, "sub").into()
        };
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
            seen,
            vec![1, 2],
            "Initial, then both rows in one batch (live={live})"
        );
        assert_eq!(map.snapshot().len(), 3);
        drop(guard);
    }
}

#[test]
fn dropping_the_last_handle_tears_the_view_down() {
    let _serial = scheduler_test_serial();
    let ctx = make_ctx();
    set(&ctx, &client("c1", "s", Some("a1")), "t1");
    let map = subscribe_ws(&ctx, EraseHopPlanView {}, "sub");
    let weak = map.downgrade();
    let (seen, guard) = watch(&map.clone().into());
    drop(guard);
    drop(map);
    assert!(
        weak.upgrade().is_none(),
        "view map outlived its last handle"
    );
    set(&ctx, &client("c1", "s", Some("a1c")), "t2");
    assert_eq!(seen.lock().unwrap().len(), 1, "no diffs after teardown");
}

#[test]
fn dropping_the_last_live_subscription_tears_the_view_down() {
    let _serial = scheduler_test_serial();
    for union in [false, true] {
        let ctx = make_ctx();
        set(&ctx, &client("c1", "s", Some("a1")), "t1");
        let map = if union {
            subscribe_live(&ctx, EraseHopUnionView {}, "sub")
        } else {
            subscribe_live(&ctx, EraseHopPlanView {}, "sub")
        };
        let (seen, guard) = watch(&map);
        let (late, late_guard) = watch(&map);
        drop(map);
        drop(guard);
        set(&ctx, &client("c1", "s", Some("a1b")), "t2");
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "dropped subscriber got no diffs"
        );
        assert_eq!(
            late.lock().unwrap().len(),
            2,
            "remaining subscriber still live"
        );
        drop(late_guard);
        set(&ctx, &client("c1", "s", Some("a1c")), "t3");
        assert_eq!(
            late.lock().unwrap().len(),
            2,
            "no diffs after the last one left"
        );
    }
}

#[cfg(feature = "profiling")]
mod cost {
    use super::views::EraseHopGatedView;
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
    /// minus the same write with no view subscribed. `live` subscribes through
    /// `subscribe_factory`, otherwise through `cell_factory`.
    fn view_cost<V>(view: &V, subs: usize, live: bool) -> u64
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: serde::de::DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let ctx = make_ctx();
        set(&ctx, &client("c1", "s", Some("a0")), "t0");
        // Keep the query map alive so the baseline includes the query hop.
        let query = ctx.query_map_by_str(GetAllClients {}, request(&ctx, "base"));
        let _q_guard = query.subscribe_diffs(|_| {});
        fires_for_one_changed_row(&ctx, 1);
        let base = fires_for_one_changed_row(&ctx, 2);
        let maps: Vec<ErasedViewMap> = (0..subs)
            .map(|i| {
                let tx = format!("sub-{i}");
                if live {
                    subscribe_live(&ctx, view.clone(), &tx)
                } else {
                    subscribe_ws(&ctx, view.clone(), &tx).into()
                }
            })
            .collect();
        let guards: Vec<_> = maps.iter().map(|m| m.subscribe_diffs(|_| {})).collect();
        fires_for_one_changed_row(&ctx, 3);
        let with = fires_for_one_changed_row(&ctx, 4);
        drop((guards, maps));
        with.checked_sub(base)
            .expect("fanouts with subscribers are at least the baseline")
    }

    #[test]
    fn report_fanouts_per_changed_row() {
        let _serial = scheduler_test_serial();
        let mut gated_costs = Vec::new();
        for live in [false, true] {
            let path = if live {
                "subscribe_factory"
            } else {
                "cell_factory"
            };
            for subs in [1usize, 4] {
                let per_sub = |n: u64| {
                    f64::from(u32::try_from(n).expect("fanout count fits u32"))
                        / f64::from(u32::try_from(subs).expect("subscriber count fits u32"))
                };
                let map = view_cost(&EraseHopMapView {}, subs, live);
                let union = view_cost(&EraseHopUnionView {}, subs, live);
                let plan = view_cost(&EraseHopPlanView {}, subs, live);
                let reader = view_cost(&EraseHopReaderView {}, subs, live);
                let gated = view_cost(&EraseHopGatedView {}, subs, live);
                println!(
                    "{path}: fanouts per changed row, {subs} subs: map-view {map} ({:.1}/sub), \
                     union-view {union} ({:.1}/sub), plan-view {plan} ({:.1}/sub), \
                     ctx.view-reader {reader} ({:.1}/sub), gated {gated} ({:.1}/sub)",
                    per_sub(map),
                    per_sub(union),
                    per_sub(plan),
                    per_sub(reader),
                    per_sub(gated),
                );
                if live {
                    assert_eq!(map, 0, "a map-rooted view subscribes with no erase write");
                    gated_costs.push(gated);
                }
            }
        }
        // The shared map's own writes are paid once, so each further gated
        // subscriber adds none.
        assert_eq!(
            gated_costs[0], gated_costs[1],
            "a gated subscriber costs no write"
        );
    }
}
