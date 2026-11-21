//! `#[indexed]`, `#[indexed(each)]` and `#[index(a, b)]`: a query with
//! equality filters on indexed fields routes through the index, live and
//! as a snapshot, and follows rows that move between keys.

use std::sync::Arc;

// The macros name `crate::` inside myko's own package, tests included.
pub use myko::*;

use myko::{
    query::{ContainsFilter, IdFilter, KeyFilter, NumericFilter, QueryRoute, StringFilter},
    request::RequestContext,
    server::{HandlerRegistry, MykoServerContext, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
};
use uuid::Uuid;

pub mod value {
    use myko::{TS, serde};

    /// A JSON-like value, as rship's `BindingValue`: no `Eq` or `Ord`,
    /// because it holds floats, so it is keyed by its JSON text.
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, TS)]
    #[serde(crate = "myko::serde")]
    pub enum IxValue {
        Number(f64),
        Text(String),
    }

    myko::impl_index_key_serde!(IxValue);
    myko::impl_filterable_by_key!(IxValue);
}

pub mod node_value {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::value::IxValue;

    /// `BindingNodeValue` by `(anchor_id, value)`.
    #[myko_item]
    #[index(anchor_id, value)]
    pub struct IxNodeValue {
        pub anchor_id: Arc<str>,
        pub value: IxValue,
    }
}

pub mod target {
    use std::sync::Arc;

    use myko::prelude::*;

    /// `Target` by `service_id`, and by an element of its `children`.
    #[myko_item]
    pub struct IxTarget {
        #[indexed]
        pub service_id: Arc<str>,
        #[indexed(each)]
        pub children: Vec<IxTargetId>,
    }
}

pub mod control {
    use std::sync::Arc;

    use myko::prelude::*;

    /// `ControlValue` by `(control_id, session_id)`.
    #[myko_item]
    #[index(control_id, session_id)]
    pub struct IxControlValue {
        pub control_id: Arc<str>,
        pub session_id: Arc<str>,
        pub level: i64,
    }
}

use control::{GetIxControlValuesByQuery, IxControlValue, IxControlValueQuery};
use node_value::{GetIxNodeValuesByQuery, IxNodeValue, IxNodeValueQuery};
use target::{GetIxTargetsByQuery, IxTarget, IxTargetId, IxTargetQuery};
use value::IxValue;

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

fn request(ctx: &MykoServerContext) -> Arc<RequestContext> {
    Arc::new(RequestContext::from_client(
        Arc::from("tx"),
        Arc::from("client"),
        ctx.host_id,
    ))
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn sorted(ids: impl IntoIterator<Item = Arc<str>>) -> Vec<String> {
    let mut ids: Vec<String> = ids.into_iter().map(|id| id.to_string()).collect();
    ids.sort();
    ids
}

fn routed_fields(route: Option<QueryRoute>) -> Option<&'static [&'static str]> {
    match route? {
        QueryRoute::BelongsTo(route) => Some(route.field_names),
        QueryRoute::Ids(_) => None,
    }
}

fn target(id: &str, service: &str, children: &[&str]) -> IxTarget {
    IxTarget {
        id: id.into(),
        service_id: service.into(),
        children: children
            .iter()
            .map(|child| IxTargetId::from(*child))
            .collect(),
    }
}

fn node_value(id: &str, anchor: &str, value: IxValue) -> IxNodeValue {
    IxNodeValue {
        id: id.into(),
        anchor_id: anchor.into(),
        value,
    }
}

fn control_value(id: &str, control: &str, session: &str) -> IxControlValue {
    IxControlValue {
        id: id.into(),
        control_id: control.into(),
        session_id: session.into(),
        level: 0,
    }
}

/// Insert, a move between keys, and a delete, seen by a live query and by
/// snapshots of the same query.
fn check_lifecycle<Q, T>(
    ctx: &MykoServerContext,
    query: impl Fn() -> Q,
    live: &myko::query::FilteredCellMap,
    snapshot: impl Fn(Q) -> Vec<Arc<str>>,
    write: impl Fn(&str, bool) -> T,
    remove: impl Fn(&str),
) {
    let _ = ctx;
    write("a", true);
    write("b", true);
    write("c", false);
    assert_eq!(
        sorted(live.snapshot().into_iter().map(|(id, _)| id)),
        ["a", "b"]
    );
    assert_eq!(sorted(snapshot(query())), ["a", "b"]);

    write("b", false);
    write("c", true);
    assert_eq!(
        sorted(live.snapshot().into_iter().map(|(id, _)| id)),
        ["a", "c"]
    );
    assert_eq!(sorted(snapshot(query())), ["a", "c"]);

    remove("a");
    assert_eq!(sorted(live.snapshot().into_iter().map(|(id, _)| id)), ["c"]);
    assert_eq!(sorted(snapshot(query())), ["c"]);
}

#[test]
fn target_by_service_id() {
    let _serial = serial();
    let ctx = make_ctx();
    let query = || IxTargetQuery {
        service_id: Some(StringFilter::Eq("service-1".into())),
        ..Default::default()
    };
    assert_eq!(
        routed_fields(query().query_route()),
        Some(&["service_id"][..])
    );
    let live = ctx.query_map_untyped(GetIxTargetsByQuery(query()), request(&ctx));
    check_lifecycle(
        &ctx,
        query,
        &live,
        |query| {
            ctx.query_snapshot(GetIxTargetsByQuery(query), request(&ctx))
                .iter()
                .map(|row| Arc::<str>::from(row.id.clone()))
                .collect()
        },
        |id, matching| {
            let service = if matching { "service-1" } else { "service-2" };
            assert!(ctx.set(target(id, service, &[])).is_ok());
        },
        |id| assert!(ctx.del(target(id, "service-1", &[])).is_ok()),
    );
}

#[test]
fn target_by_an_element_of_its_children() {
    let _serial = serial();
    let ctx = make_ctx();
    let query = || IxTargetQuery {
        children: Some(ContainsFilter::Eq(IxTargetId::from("child-x"))),
        ..Default::default()
    };
    assert_eq!(
        routed_fields(query().query_route()),
        Some(&["children[]"][..])
    );
    let live = ctx.query_map_untyped(GetIxTargetsByQuery(query()), request(&ctx));
    check_lifecycle(
        &ctx,
        query,
        &live,
        |query| {
            ctx.query_snapshot(GetIxTargetsByQuery(query), request(&ctx))
                .iter()
                .map(|row| Arc::<str>::from(row.id.clone()))
                .collect()
        },
        |id, matching| {
            let children: &[&str] = if matching {
                &["child-w", "child-x"]
            } else {
                &["child-w"]
            };
            assert!(ctx.set(target(id, "service-1", children)).is_ok());
        },
        |id| assert!(ctx.del(target(id, "service-1", &[])).is_ok()),
    );
}

#[test]
fn a_row_filed_under_two_keys_of_one_query_appears_once() {
    let _serial = serial();
    let ctx = make_ctx();
    let query = || IxTargetQuery {
        children: Some(ContainsFilter::In(vec![
            IxTargetId::from("child-x"),
            IxTargetId::from("child-y"),
        ])),
        ..Default::default()
    };
    let live = ctx.query_map_untyped(GetIxTargetsByQuery(query()), request(&ctx));
    assert!(ctx.set(target("t1", "s", &["child-x", "child-y"])).is_ok());
    assert_eq!(live.snapshot().len(), 1);
    assert_eq!(
        ctx.query_snapshot(GetIxTargetsByQuery(query()), request(&ctx))
            .len(),
        1
    );
    // Dropping one of its two keys keeps it in the query.
    assert!(ctx.set(target("t1", "s", &["child-y"])).is_ok());
    assert_eq!(live.snapshot().len(), 1);
    assert!(ctx.set(target("t1", "s", &[])).is_ok());
    assert_eq!(live.snapshot().len(), 0);
}

#[test]
fn node_value_by_anchor_and_value() {
    let _serial = serial();
    let ctx = make_ctx();
    let query = || IxNodeValueQuery {
        anchor_id: Some(StringFilter::Eq("anchor-1".into())),
        value: Some(KeyFilter::Eq(IxValue::Number(0.5))),
        ..Default::default()
    };
    assert_eq!(
        routed_fields(query().query_route()),
        Some(&["anchor_id", "value"][..])
    );
    let live = ctx.query_map_untyped(GetIxNodeValuesByQuery(query()), request(&ctx));
    check_lifecycle(
        &ctx,
        query,
        &live,
        |query| {
            ctx.query_snapshot(GetIxNodeValuesByQuery(query), request(&ctx))
                .iter()
                .map(|row| Arc::<str>::from(row.id.clone()))
                .collect()
        },
        |id, matching| {
            let value = if matching {
                IxValue::Number(0.5)
            } else {
                IxValue::Text("0.5".into())
            };
            assert!(ctx.set(node_value(id, "anchor-1", value)).is_ok());
        },
        |id| {
            assert!(
                ctx.del(node_value(id, "anchor-1", IxValue::Number(0.5)))
                    .is_ok()
            );
        },
    );
}

#[test]
fn control_value_by_control_and_session() {
    let _serial = serial();
    let ctx = make_ctx();
    let query = || IxControlValueQuery {
        control_id: Some(StringFilter::Eq("control-1".into())),
        session_id: Some(StringFilter::Eq("session-1".into())),
        ..Default::default()
    };
    assert_eq!(
        routed_fields(query().query_route()),
        Some(&["control_id", "session_id"][..])
    );
    let live = ctx.query_map_untyped(GetIxControlValuesByQuery(query()), request(&ctx));
    check_lifecycle(
        &ctx,
        query,
        &live,
        |query| {
            ctx.query_snapshot(GetIxControlValuesByQuery(query), request(&ctx))
                .iter()
                .map(|row| Arc::<str>::from(row.id.clone()))
                .collect()
        },
        |id, matching| {
            let session = if matching { "session-1" } else { "session-2" };
            assert!(ctx.set(control_value(id, "control-1", session)).is_ok());
        },
        |id| {
            assert!(ctx.del(control_value(id, "control-1", "session-1")).is_ok());
        },
    );
}

#[test]
fn an_unindexed_filter_is_not_routed_and_a_partial_compound_key_is_not_either() {
    let only_control = IxControlValueQuery {
        control_id: Some(StringFilter::Eq("control-1".into())),
        ..Default::default()
    };
    assert_eq!(routed_fields(only_control.query_route()), None);
    let substring = IxTargetQuery {
        service_id: Some(StringFilter::Contains("service".into())),
        ..Default::default()
    };
    assert_eq!(routed_fields(substring.query_route()), None);
    let by_id = IxTargetQuery {
        id: Some(IdFilter::Eq(IxTargetId::from("t1"))),
        service_id: Some(StringFilter::Eq("service-1".into())),
        ..Default::default()
    };
    assert!(matches!(by_id.query_route(), Some(QueryRoute::Ids(_))));
}

#[test]
fn a_row_whose_unrouted_field_stops_matching_leaves_a_routed_query() {
    let _serial = serial();
    let ctx = make_ctx();
    let query = || IxControlValueQuery {
        control_id: Some(StringFilter::Eq("control-1".into())),
        session_id: Some(StringFilter::Eq("session-1".into())),
        level: Some(NumericFilter::Eq(0)),
        ..Default::default()
    };
    let live = ctx.query_map_untyped(GetIxControlValuesByQuery(query()), request(&ctx));
    let mut row = control_value("v1", "control-1", "session-1");
    assert!(ctx.set(row.clone()).is_ok());
    assert_eq!(live.snapshot().len(), 1);

    // The route's keys are unchanged; only the residual field changes.
    row.level = 1;
    assert!(ctx.set(row.clone()).is_ok());
    assert_eq!(
        live.snapshot().len(),
        0,
        "the row no longer matches the filter"
    );
    row.level = 0;
    assert!(ctx.set(row).is_ok());
    assert_eq!(live.snapshot().len(), 1);
}

#[test]
fn a_row_that_changes_key_and_stops_matching_leaves_a_union_route() {
    let _serial = serial();
    let ctx = make_ctx();
    let query = IxControlValueQuery {
        control_id: Some(StringFilter::Eq("control-1".into())),
        session_id: Some(StringFilter::In(vec![
            "session-1".into(),
            "session-2".into(),
        ])),
        level: Some(NumericFilter::Eq(0)),
        ..Default::default()
    };
    let live = ctx.query_map_untyped(GetIxControlValuesByQuery(query), request(&ctx));
    for round in 0..20 {
        let id = format!("v{round}");
        assert!(
            ctx.set(control_value(&id, "control-1", "session-1"))
                .is_ok()
        );
        let mut moved = control_value(&id, "control-1", "session-2");
        moved.level = 1;
        assert!(ctx.set(moved).is_ok());
        assert!(
            live.snapshot().iter().all(|(row, _)| row.as_ref() != id),
            "round {round}: a row that moved key and stopped matching stayed"
        );
    }
}

#[test]
fn an_indexed_route_keeps_a_row_with_its_new_value_and_snapshots_agree() {
    let _serial = serial();
    let ctx = make_ctx();
    let query = || IxControlValueQuery {
        control_id: Some(StringFilter::Eq("control-1".into())),
        session_id: Some(StringFilter::Eq("session-1".into())),
        level: Some(NumericFilter::In(vec![0, 2])),
        ..Default::default()
    };
    let live = ctx.query_map_untyped(GetIxControlValuesByQuery(query()), request(&ctx));
    let levels = |rows: Vec<i64>| rows;
    let mut row = control_value("v1", "control-1", "session-1");
    for (level, expected) in [(0, vec![0]), (1, vec![]), (2, vec![2]), (0, vec![0])] {
        row.level = level;
        assert!(ctx.set(row.clone()).is_ok());
        let live_levels: Vec<i64> = live
            .snapshot()
            .iter()
            .filter_map(|(_, row)| row.as_any().downcast_ref::<IxControlValue>())
            .map(|row| row.level)
            .collect();
        assert_eq!(levels(live_levels), expected, "live, level {level}");
        let snapshot: Vec<i64> = ctx
            .query_snapshot(GetIxControlValuesByQuery(query()), request(&ctx))
            .iter()
            .map(|row| row.level)
            .collect();
        assert_eq!(snapshot, expected, "snapshot, level {level}");
    }
}
