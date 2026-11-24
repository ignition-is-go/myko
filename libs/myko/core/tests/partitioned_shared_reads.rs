//! A partition reading shared data.
//!
//! Shared data, and everything myko derives from it and keeps for later
//! readers (stores, indexes, index buckets), lives in the root domain,
//! whichever context asks for it first. A partition reads it through the
//! one-way hand-off from root, and what the partition derives from it lives
//! in the partition. The tests build the same routed reads in every order a
//! server can meet them and check that each reader keeps receiving root's
//! writes.

#![allow(clippy::expect_used, clippy::panic)]

use std::{
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

// The macros name `crate::` inside myko's own package, tests included.
pub use myko::*;
use myko::{
    hyphae::scheduler::Domain,
    prelude::Eventable,
    server::{
        HandlerRegistry, MykoServerContext, MykoServerRuntime, PartitionKey, PartitionSchema,
        RelationshipManager, persister::PersisterRouter,
    },
    store::StoreRegistry,
};
use parking_lot::Mutex;
use uuid::Uuid;

const DEADLINE: Duration = Duration::from_secs(10);

/// Root's tick is shared by the process, so the tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

pub mod space {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared. Setting a space opens the partition named by its id.
    #[myko_item]
    pub struct PsrSpace {
        pub label: Arc<str>,
    }
}

pub mod shelf {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared parent.
    #[myko_item]
    pub struct PsrShelf {
        pub label: Arc<str>,
    }
}

pub mod goods {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::shelf::{PsrShelf, PsrShelfId};

    /// Shared child, read through its `belongs_to` index.
    #[myko_item]
    pub struct PsrItem {
        #[belongs_to(PsrShelf)]
        pub shelf_id: PsrShelfId,
        pub label: Arc<str>,
    }
}

use goods::{GetPsrItemsByQuery, PsrItem, PsrItemId, PsrItemQuery};
use shelf::PsrShelfId;
use space::PsrSpace;

struct Fixture {
    root: MykoServerContext,
    a: MykoServerContext,
    b: MykoServerContext,
}

fn fixture() -> Fixture {
    let root = MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(myko::search::SearchIndex::new()),
        MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    )
    .with_partitions(PartitionSchema::new().lifecycle::<PsrSpace>());
    for id in ["a", "b"] {
        root.set(&PsrSpace {
            id: id.into(),
            label: id.into(),
        })
        .expect("space");
    }
    let a = root.partition(&PartitionKey::new("a")).expect("space a");
    let b = root.partition(&PartitionKey::new("b")).expect("space b");
    Fixture { root, a, b }
}

fn on_shelf(shelf: &str) -> GetPsrItemsByQuery {
    GetPsrItemsByQuery(PsrItemQuery {
        shelf_id: Some(myko::query::IdFilter::Eq(PsrShelfId(shelf.into()))),
        ..Default::default()
    })
}

/// A live routed read of the items on `shelf`, held open.
struct Reader {
    rows: myko::hyphae::CellMap<PsrItemId, Arc<PsrItem>, myko::hyphae::CellImmutable>,
}

impl Reader {
    fn open(ctx: &MykoServerContext, shelf: &str) -> Self {
        Self {
            rows: ctx.query_map(on_shelf(shelf), ctx.new_server_transaction()),
        }
    }

    /// Whether the read shows `count` rows before the deadline.
    fn shows(&self, count: usize) -> bool {
        let start = Instant::now();
        while start.elapsed() < DEADLINE {
            if self.rows.snapshot().len() == count {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        false
    }
}

fn put(root: &MykoServerContext, id: &str, shelf: &str) {
    root.set(&PsrItem {
        id: id.into(),
        shelf_id: PsrShelfId(shelf.into()),
        label: id.into(),
    })
    .expect("shared item");
}

/// Root writes one item onto each reader's shelf, and every reader shows it.
fn assert_every_reader_follows_root(root: &MykoServerContext, readers: &[(&str, &Reader)]) {
    for (shelf, _) in readers {
        put(root, &format!("on-{shelf}"), shelf);
    }
    for (shelf, reader) in readers {
        assert!(
            reader.shows(1),
            "the reader of shelf {shelf} shows root's write"
        );
    }
    for (shelf, _) in readers {
        put(root, &format!("also-on-{shelf}"), shelf);
    }
    for (shelf, reader) in readers {
        assert!(
            reader.shows(2),
            "the reader of shelf {shelf} keeps following root"
        );
    }
}

#[test]
fn a_partition_that_reads_a_shared_index_first_does_not_own_it() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let in_a = Reader::open(&fixture.a, "x");
    let in_b = Reader::open(&fixture.b, "y");
    let in_root = Reader::open(&fixture.root, "z");
    assert_every_reader_follows_root(
        &fixture.root,
        &[("x", &in_a), ("y", &in_b), ("z", &in_root)],
    );
}

#[test]
fn a_partition_reads_a_shared_index_root_built_first() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let in_root = Reader::open(&fixture.root, "z");
    let in_a = Reader::open(&fixture.a, "x");
    let in_b = Reader::open(&fixture.b, "y");
    assert_every_reader_follows_root(
        &fixture.root,
        &[("x", &in_a), ("y", &in_b), ("z", &in_root)],
    );
}

#[test]
fn two_partitions_share_one_bucket_of_a_shared_index() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let in_a = Reader::open(&fixture.a, "x");
    let in_b = Reader::open(&fixture.b, "x");
    assert_every_reader_follows_root(&fixture.root, &[("x", &in_a)]);
    assert!(in_b.shows(2), "the second partition reads the same bucket");
}

#[test]
fn partitions_reading_a_shared_index_at_once_each_follow_root() {
    let _serial = SERIAL.lock();
    for _ in 0..20 {
        let fixture = fixture();
        let start = Arc::new(Barrier::new(2));
        let open = |ctx: MykoServerContext, shelf: &'static str| {
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                Reader::open(&ctx, shelf)
            })
        };
        let a = open(fixture.a.clone(), "x");
        let b = open(fixture.b.clone(), "y");
        let in_a = a.join().expect("reader in a");
        let in_b = b.join().expect("reader in b");
        assert_every_reader_follows_root(&fixture.root, &[("x", &in_a), ("y", &in_b)]);
    }
}

#[test]
fn a_one_shot_read_from_a_partition_leaves_the_shared_index_in_root() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    put(&fixture.root, "first", "x");
    let request = fixture.a.new_server_transaction();
    assert_eq!(
        fixture.a.query_snapshot(on_shelf("x"), request).len(),
        1,
        "a one-shot read from a partition sees the shared row"
    );
    let in_root = Reader::open(&fixture.root, "y");
    let in_b = Reader::open(&fixture.b, "x");
    put(&fixture.root, "second", "x");
    put(&fixture.root, "third", "y");
    assert!(in_b.shows(2), "a partition follows root's writes");
    assert!(in_root.shows(1), "root follows its own writes");
}

#[test]
fn shared_state_is_born_in_root_whoever_asks_first() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let store = fixture.a.in_domain(|| {
        fixture
            .a
            .registry
            .get_or_create(PsrItem::ENTITY_NAME_STATIC)
    });
    assert!(
        store.as_ref().domain().is_root(),
        "a shared store first asked for from a partition lives in root"
    );
    assert!(
        fixture.a.domain() != Domain::root(),
        "the partition has a domain of its own"
    );
}

#[test]
fn a_partition_wave_takes_a_one_shot_routed_read_of_shared_data() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let reader = fixture.a.clone();
    let log = Arc::clone(&seen);
    let live = fixture
        .a
        .query_map(on_shelf("x"), fixture.a.new_server_transaction());
    let _guard = live.subscribe_diffs(move |_| {
        let rows = reader.query_snapshot(on_shelf("x"), reader.new_server_transaction());
        log.lock().push(rows.len());
    });
    put(&fixture.root, "first", "x");
    fixture.a.domain().flush();
    assert_eq!(
        seen.lock().last().copied(),
        Some(1),
        "a read taken inside the partition's wave sees the shared row"
    );
}
