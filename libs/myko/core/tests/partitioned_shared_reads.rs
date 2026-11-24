//! A partition reading shared data.
//!
//! Shared stores live in the root domain and exist from the moment a server
//! declares its partitions. Everything a partition derives from them, the
//! indexes it routes reads through included, is the partition's own and
//! lives in its domain, fed by the one-way hand-off from root. A partition
//! never builds root state, so it can open any read at any time, from
//! inside its own wave too. The tests build the same reads in every order a
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

pub mod note {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared, and read by nothing until a partition's wave first asks.
    #[myko_item]
    pub struct PsrNote {
        pub label: Arc<str>,
    }
}

pub mod late {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::shelf::{PsrShelf, PsrShelfId};

    /// Shared child no reader routes through until a partition's wave asks.
    #[myko_item]
    pub struct PsrTag {
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
fn a_partition_reading_a_shared_route_first_leaves_root_reads_working() {
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
fn two_partitions_reading_one_shared_route_each_follow_root() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let in_a = Reader::open(&fixture.a, "x");
    let in_b = Reader::open(&fixture.b, "x");
    assert_every_reader_follows_root(&fixture.root, &[("x", &in_a)]);
    assert!(in_b.shows(2), "the second partition follows root too");
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
fn a_one_shot_read_from_a_partition_leaves_later_reads_following_root() {
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
fn shared_stores_live_in_root_whoever_asks_first() {
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

#[test]
fn a_partition_wave_opens_a_routed_read_of_shared_data_no_one_read_before() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    put(&fixture.root, "waiting", "fresh");
    let opened: Arc<Mutex<Vec<Reader>>> = Arc::new(Mutex::new(Vec::new()));
    let reader = fixture.a.clone();
    let sink = Arc::clone(&opened);
    let trigger = fixture
        .a
        .query_map(on_shelf("x"), fixture.a.new_server_transaction());
    let _guard = trigger.subscribe_diffs(move |_| {
        let mut opened = sink.lock();
        if opened.is_empty() && Domain::propagating().is_some() {
            opened.push(Reader::open(&reader, "fresh"));
        }
    });
    put(&fixture.root, "trigger", "x");
    fixture.a.domain().flush();
    let in_root = Reader::open(&fixture.root, "fresh");
    put(&fixture.root, "later", "fresh");
    let in_wave = opened.lock().pop().expect("the wave opened a read");
    assert!(
        in_wave.shows(2),
        "a read opened inside the wave follows root"
    );
    assert!(
        in_root.shows(2),
        "root's own read of the same rows is unaffected"
    );
}

/// Run `open` once, inside a wave of `ctx`'s partition, and hand back what it
/// built. The wave is the first diff root's write of a trigger row brings.
fn open_in_wave<R: Send + 'static>(
    fixture: &Fixture,
    open: impl Fn(&MykoServerContext) -> R + Send + Sync + 'static,
) -> R {
    let opened: Arc<Mutex<Option<R>>> = Arc::new(Mutex::new(None));
    let reader = fixture.a.clone();
    let sink = Arc::clone(&opened);
    let trigger = fixture
        .a
        .query_map(on_shelf("trigger"), fixture.a.new_server_transaction());
    let _guard = trigger.subscribe_diffs(move |_| {
        let mut opened = sink.lock();
        if opened.is_none() && Domain::propagating().is_some_and(|domain| !domain.is_root()) {
            *opened = Some(open(&reader));
        }
    });
    put(&fixture.root, "trigger", "trigger");
    fixture.a.domain().flush();
    let built = opened.lock().take();
    built.expect("the partition's wave opened the read")
}

/// Whether `len` reads `count` before the deadline.
fn shows(len: impl Fn() -> usize, count: usize) -> bool {
    let start = Instant::now();
    while start.elapsed() < DEADLINE {
        if len() == count {
            return true;
        }
        thread::sleep(Duration::from_millis(1));
    }
    false
}

#[test]
fn a_partition_wave_reads_a_shared_type_nothing_read_before() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let notes = open_in_wave(&fixture, |ctx| {
        ctx.query_map(note::GetAllPsrNotes {}, ctx.new_server_transaction())
    });
    fixture
        .root
        .set(&note::PsrNote {
            id: "n".into(),
            label: "n".into(),
        })
        .expect("shared note");
    assert!(
        shows(|| notes.snapshot().len(), 1),
        "a read the wave opened over an untouched shared type follows root"
    );
}

#[test]
fn a_partition_wave_routes_a_shared_read_through_an_index_nothing_built() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let tags = open_in_wave(&fixture, |ctx| {
        ctx.query_map(
            late::GetPsrTagsByQuery(late::PsrTagQuery {
                shelf_id: Some(myko::query::IdFilter::Eq(PsrShelfId("x".into()))),
                ..Default::default()
            }),
            ctx.new_server_transaction(),
        )
    });
    let in_root = fixture.root.query_map(
        late::GetPsrTagsByQuery(late::PsrTagQuery {
            shelf_id: Some(myko::query::IdFilter::Eq(PsrShelfId("x".into()))),
            ..Default::default()
        }),
        fixture.root.new_server_transaction(),
    );
    for id in ["t1", "t2"] {
        fixture
            .root
            .set(&late::PsrTag {
                id: id.into(),
                shelf_id: PsrShelfId("x".into()),
                label: id.into(),
            })
            .expect("shared tag");
    }
    assert!(
        shows(|| tags.snapshot().len(), 2),
        "a routed read the wave opened first follows root"
    );
    assert!(
        shows(|| in_root.snapshot().len(), 2),
        "root's own routed read is unaffected"
    );
}
