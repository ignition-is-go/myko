//! A `belongs_to` route built while another thread drains the root domain.
//!
//! The route's index follows the store through diffs that the scheduler
//! queues behind that drain, so a write can return before the index sees it.
//! A route built then must still hold the write, and the queued diffs must
//! not move it back when they arrive.

#![allow(clippy::expect_used)]

use std::sync::{Arc, Mutex, mpsc};

use myko::{
    entities::{
        client::{Client, ClientId, ClientQuery, GetClientsByQuery},
        server::ServerId,
    },
    hyphae::{Cell, MapDiff, Mutable, Signal, Watchable},
    query::IdFilter,
    server::{HandlerRegistry, MykoServerContext, RelationshipManager, persister::PersisterRouter},
    store::StoreRegistry,
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

fn client(id: &str, address: &str) -> Client {
    Client {
        id: id.into(),
        server_id: ServerId::from(Arc::<str>::from("server-A")),
        address: Some(address.into()),
        windback: None,
    }
}

fn routed(
    ctx: &MykoServerContext,
) -> myko::hyphae::CellMap<ClientId, Arc<Client>, myko::hyphae::CellImmutable> {
    ctx.query_map(
        GetClientsByQuery(ClientQuery {
            server_id: Some(IdFilter::In(vec![ServerId::from(Arc::<str>::from(
                "server-A",
            ))])),
            ..Default::default()
        }),
        ctx.new_server_transaction(),
    )
}

/// Each client id with its address, sorted by id.
fn rows(ctx: &MykoServerContext) -> Vec<(String, String)> {
    rows_of(&routed(ctx))
}

fn rows_of(
    map: &myko::hyphae::CellMap<ClientId, Arc<Client>, myko::hyphae::CellImmutable>,
) -> Vec<(String, String)> {
    let mut rows: Vec<_> = map
        .items_snapshot()
        .iter()
        .map(|c| {
            (
                c.id.to_string(),
                c.address.as_deref().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    rows.sort();
    rows
}

/// Run `during` while another thread holds a root-domain drain open, so the
/// store diffs it produces stay queued until it returns.
fn while_another_thread_drains<R>(during: impl FnOnce() -> R) -> R {
    // NOTE(ts): The root drain is process-wide, so two tests holding one at
    // once would each queue behind the other.
    static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());
    let _serial = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let cell = Cell::new(0_u32);
        let ready_tx = Mutex::new(ready_tx);
        let go_rx = Mutex::new(go_rx);
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(v) = signal
                && **v == 1
            {
                ready_tx.lock().expect("lock").send(()).expect("ready");
                go_rx.lock().expect("lock").recv().expect("go");
            }
        });
        // NOTE(ts): A drain another test left running may run the callback on
        // its own thread after this batch returns; it parks that drain
        // instead, so the subscription must outlive the batch.
        myko::hyphae::batch(|| cell.set(1));
        done_rx.recv().expect("done");
        drop(guard);
    });
    ready_rx.recv().expect("holder draining");
    let result = during();
    go_tx.send(()).expect("go");
    done_tx.send(()).expect("done");
    holder.join().expect("holder");
    result
}

fn pair(id: &str, address: &str) -> (String, String) {
    (id.to_owned(), address.to_owned())
}

#[test]
fn a_route_built_during_a_drain_holds_an_insert_that_returned() {
    let ctx = make_ctx();
    ctx.set(&client("c1", "a")).expect("set c1");
    assert_eq!(rows(&ctx), vec![pair("c1", "a")], "builds the index");

    let (map, seen) = while_another_thread_drains(|| {
        ctx.set(&client("c2", "a")).expect("set c2");
        let map = routed(&ctx);
        let seen = rows_of(&map);
        (map, seen)
    });
    ctx.settle();

    assert_eq!(seen, vec![pair("c1", "a"), pair("c2", "a")]);
    assert_eq!(rows_of(&map), vec![pair("c1", "a"), pair("c2", "a")]);
}

#[test]
fn queued_diffs_do_not_move_a_route_back_to_an_older_row() {
    let ctx = make_ctx();
    ctx.set(&client("c1", "a")).expect("set c1");
    ctx.set(&client("gone", "a")).expect("set gone");
    assert_eq!(rows(&ctx).len(), 2, "builds the index");

    let addresses = Arc::new(Mutex::new(Vec::new()));
    let (map, _guard, seen) = while_another_thread_drains(|| {
        ctx.set(&client("c1", "b")).expect("update c1");
        ctx.set(&client("c1", "c")).expect("update c1 again");
        ctx.del(&client("gone", "a")).expect("delete gone");
        let map = routed(&ctx);
        let seen = rows_of(&map);
        let log = Arc::clone(&addresses);
        let guard = map.subscribe_diffs(move |diff| {
            let mut log = log.lock().expect("lock");
            let mut record = |diff: &MapDiff<ClientId, Arc<Client>>| match diff {
                MapDiff::Insert { value, .. }
                | MapDiff::Update {
                    new_value: value, ..
                } => {
                    log.push(format!(
                        "{}={}",
                        value.id,
                        value.address.as_deref().unwrap_or_default()
                    ));
                }
                MapDiff::Remove { key, .. } => log.push(format!("-{key}")),
                MapDiff::Initial { .. } | MapDiff::Batch { .. } => {}
            };
            match diff {
                MapDiff::Batch { changes } => changes.iter().for_each(&mut record),
                other => record(other),
            }
        });
        (map, guard, seen)
    });
    ctx.settle();

    assert_eq!(seen, vec![pair("c1", "c")]);
    assert_eq!(rows_of(&map), vec![pair("c1", "c")]);
    let delivered = addresses.lock().expect("lock").clone();
    assert!(
        delivered.iter().all(|row| row == "c1=c" || row == "-c1"),
        "a subscriber saw an older row: {delivered:?}"
    );

    ctx.set(&client("c1", "d")).expect("update after the drain");
    ctx.settle();
    assert_eq!(
        rows_of(&map),
        vec![pair("c1", "d")],
        "later writes still arrive"
    );
}
