//! Report and view families looked up at a changing key.

#![cfg(feature = "bench")]

use std::sync::Arc;

use myko::{
    bench_entities::{BenchCategoryNames, BenchItem, BenchItemsInCategory},
    hyphae::{Cell, Gettable, LookupExt, MapExt, Materialize, Mutable},
    search::SearchIndex,
    server::{
        HandlerRegistry, MykoServerContext, MykoServerRuntime, RelationshipManager,
        persister::PersisterRouter,
    },
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn make_ctx() -> MykoServerContext {
    MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(SearchIndex::new()),
        MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    )
}

fn insert(ctx: &MykoServerContext, id: &str, category: &str) {
    let item = BenchItem {
        id: id.into(),
        name: id.to_string(),
        category: category.to_string(),
        value: 0,
    };
    let event = MEvent::from_item(&item, MEventType::SET, &format!("tx-{id}"));
    assert!(ctx.apply_event_batch(vec![event]).is_ok());
}

fn names(ids: &[&str]) -> Arc<Vec<String>> {
    Arc::new(ids.iter().map(|id| (*id).to_string()).collect())
}

fn seed(ctx: &MykoServerContext) {
    insert(ctx, "a1", "alpha");
    insert(ctx, "a2", "alpha");
    insert(ctx, "b1", "beta");
}

#[test]
fn a_report_family_follows_a_changing_key() {
    let _serial = serial();
    let ctx = make_ctx();
    seed(&ctx);
    let family = ctx.family::<BenchCategoryNames>(ctx.new_server_transaction());

    let key = Cell::new(Some(BenchCategoryNames {
        category: "alpha".into(),
    }));
    let names_at_key = key.clone().lock().lookup(family);
    assert_eq!(names_at_key.get(), Some(names(&["a1", "a2"])));

    key.set(Some(BenchCategoryNames {
        category: "beta".into(),
    }));
    assert_eq!(names_at_key.get(), Some(names(&["b1"])));

    insert(&ctx, "b2", "beta");
    assert_eq!(names_at_key.get(), Some(names(&["b1", "b2"])));

    // The old key's row is no longer what the reader follows.
    insert(&ctx, "a3", "alpha");
    assert_eq!(names_at_key.get(), Some(names(&["b1", "b2"])));

    key.set(None);
    assert_eq!(names_at_key.get(), None);
}

#[test]
fn readers_of_one_report_share_its_row() {
    let _serial = serial();
    let ctx = make_ctx();
    seed(&ctx);
    let request = ctx.new_server_transaction();
    let alpha = || BenchCategoryNames {
        category: "alpha".into(),
    };

    let first = ctx.report(alpha(), request.clone());
    let family = ctx.family::<BenchCategoryNames>(request);
    let second = Cell::new(alpha()).lock().lookup(family.clone());
    assert_eq!(family.len(), 1);
    assert_eq!(ctx.report_rows(), 1);
    assert_eq!(first.get(), second.get());

    drop(first);
    drop(second);
    myko::hyphae::flush();
    assert_eq!(family.len(), 0);
}

#[test]
fn a_view_family_follows_a_changing_key() {
    let _serial = serial();
    let ctx = make_ctx();
    seed(&ctx);
    let family = ctx.family::<BenchItemsInCategory>(ctx.new_server_transaction());

    let key = Cell::new(BenchItemsInCategory {
        category: "alpha".into(),
    });
    let items = key.clone().lock().lookup(family);
    let ids = items.keys().map(|keys| {
        let mut ids: Vec<String> = keys.iter().map(ToString::to_string).collect();
        ids.sort();
        ids
    });
    let ids = ids.materialize();
    assert_eq!(*ids.get(), vec!["a1", "a2"]);

    key.set(BenchItemsInCategory {
        category: "beta".into(),
    });
    assert_eq!(*ids.get(), vec!["b1"]);

    insert(&ctx, "b2", "beta");
    insert(&ctx, "a3", "alpha");
    assert_eq!(*ids.get(), vec!["b1", "b2"]);
}
