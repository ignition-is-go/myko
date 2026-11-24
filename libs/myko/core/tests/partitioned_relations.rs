//! Relationships across the partition boundary.
//!
//! A shared parent's delete reaches its partitioned children in every
//! partition, once in each, there. A relation between two partitioned types
//! runs inside one partition. A relation whose derived writes would carry
//! partition data back into shared data, or across partitions, is refused
//! when the schema is built. What an admitted write derives is applied, even
//! after its partition stopped admitting writes.

#![allow(clippy::expect_used, clippy::panic)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

// The macros name `crate::` inside myko's own package, tests included.
pub use myko::*;
use myko::{
    prelude::{AnyItem, Eventable},
    server::{
        HandlerRegistry, MutationValidatorRegistration, MykoServerContext, MykoServerRuntime,
        PartitionKey, PartitionSchema, PersistError, ProposedState, RelationshipManager,
        persister::{Persister, PersisterRouter},
    },
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use parking_lot::{Condvar, Mutex};
use uuid::Uuid;

/// Root's tick is shared by the process, so the tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

pub mod room {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared. Setting a room opens the partition named by its id.
    #[myko_item]
    pub struct PrlRoom {
        pub label: Arc<str>,
    }
}

pub mod folder {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared parent of partitioned notes.
    #[myko_item]
    pub struct PrlFolder {
        pub label: Arc<str>,
    }
}

pub mod note {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::folder::{PrlFolder, PrlFolderId};

    /// Partitioned by its room; belongs to a shared folder.
    #[myko_item]
    pub struct PrlNote {
        #[myko_partition_key]
        pub room: Arc<str>,
        #[belongs_to(PrlFolder)]
        pub folder_id: PrlFolderId,
    }
}

pub mod line {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::note::{PrlNote, PrlNoteId};

    /// Partitioned by its room; belongs to a partitioned note.
    #[myko_item]
    pub struct PrlLine {
        #[myko_partition_key]
        pub room: Arc<str>,
        #[belongs_to(PrlNote)]
        pub note_id: PrlNoteId,
    }
}

pub mod cursor {
    use myko::prelude::*;

    use super::room::{PrlRoom, PrlRoomId};

    /// Partitioned by the room it is ensured for.
    #[myko_item]
    pub struct PrlCursor {
        #[ensure_for(PrlRoom)]
        pub room_id: PrlRoomId,
    }
}

pub mod shelf {
    use myko::prelude::*;

    use super::book::{PrlBook, PrlBookId};

    /// Shared, owning books. Refused once books are partitioned.
    #[myko_item]
    pub struct PrlShelf {
        #[owns_many(PrlBook)]
        pub books: Vec<PrlBookId>,
    }
}

pub mod book {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared, except where a schema partitions it.
    #[myko_item]
    pub struct PrlBook {
        pub room: Arc<str>,
    }
}

pub mod tag {
    use myko::prelude::*;

    use super::book::{PrlBook, PrlBookId};

    /// Shared child of a book. Refused once books are partitioned.
    #[myko_item]
    pub struct PrlTag {
        #[belongs_to(PrlBook)]
        pub book_id: PrlBookId,
    }
}

pub mod memo {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Partitioned by its room, and searchable.
    #[myko_item]
    pub struct PrlMemo {
        #[myko_partition_key]
        pub room: Arc<str>,
        #[searchable]
        pub text: Arc<str>,
    }
}

use book::PrlBook;
use cursor::PrlCursor;
use folder::{PrlFolder, PrlFolderId};
use line::PrlLine;
use note::{PrlNote, PrlNoteId};
use room::PrlRoom;

/// A persister that keeps every event it is given, in order.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<MEvent>>>);

impl Persister for Log {
    fn persist(&self, event: MEvent) -> Result<(), PersistError> {
        self.0.lock().push(event);
        Ok(())
    }
}

impl Log {
    /// `(change, id)` of every logged event of `entity_type`.
    fn of(&self, entity_type: &str) -> Vec<(MEventType, String)> {
        self.0
            .lock()
            .iter()
            .filter(|event| event.item_type.as_ref() == entity_type)
            .map(|event| {
                (
                    event.change_type,
                    event
                        .item
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                )
            })
            .collect()
    }
}

fn schema() -> PartitionSchema {
    PartitionSchema::new()
        .lifecycle::<PrlRoom>()
        .partition_by::<PrlCursor>(|cursor| PartitionKey::new(cursor.room_id.0.clone()))
}

struct Fixture {
    log: Log,
    root: MykoServerContext,
    a: MykoServerContext,
    b: MykoServerContext,
}

fn root_logging(log: &Log) -> MykoServerContext {
    let mut persisters = PersisterRouter::default();
    persisters.set_default(Some(Arc::new(log.clone())));
    MykoServerContext::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(persisters),
        Arc::new(myko::search::SearchIndex::new()),
        MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    )
}

/// Rooms `a` and `b`, and a shared folder `f`.
fn fixture() -> Fixture {
    let log = Log::default();
    let root = root_logging(&log).with_partitions(schema());
    for id in ["a", "b"] {
        root.set(&room(id)).expect("room");
    }
    root.set(&PrlFolder {
        id: "f".into(),
        label: "f".into(),
    })
    .expect("folder");
    let a = root.partition(&PartitionKey::new("a")).expect("room a");
    let b = root.partition(&PartitionKey::new("b")).expect("room b");
    settle(&a);
    settle(&b);
    Fixture { log, root, a, b }
}

fn room(id: &str) -> PrlRoom {
    PrlRoom {
        id: id.into(),
        label: id.into(),
    }
}

fn folder(id: &str) -> PrlFolder {
    PrlFolder {
        id: id.into(),
        label: id.into(),
    }
}

fn note(room: &str, id: &str) -> PrlNote {
    PrlNote {
        id: id.into(),
        room: room.into(),
        folder_id: PrlFolderId("f".into()),
    }
}

fn line(room: &str, id: &str, note: &str) -> PrlLine {
    PrlLine {
        id: id.into(),
        room: room.into(),
        note_id: PrlNoteId(note.into()),
    }
}

fn settle(ctx: &MykoServerContext) {
    ctx.domain().flush();
}

fn has(ctx: &MykoServerContext, entity_type: &str, id: &str) -> bool {
    ctx.registry
        .get_or_create(entity_type)
        .get_value(&Arc::from(id))
        .is_some()
}

fn cursors(ctx: &MykoServerContext) -> Vec<String> {
    let mut rooms: Vec<String> = ctx
        .registry
        .get_or_create(PrlCursor::ENTITY_NAME_STATIC)
        .snapshot()
        .into_iter()
        .filter_map(|(_, row)| {
            AnyItem::as_any(row.as_ref())
                .downcast_ref::<PrlCursor>()
                .map(|cursor| cursor.room_id.0.to_string())
        })
        .collect();
    rooms.sort();
    rooms
}

/// Parks whoever passes it until it is dropped.
#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

const DEADLINE: Duration = Duration::from_secs(10);

impl Gate {
    fn pass(&self) {
        let mut state = self.state.lock();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            if self.changed.wait_for(&mut state, DEADLINE).timed_out() {
                break;
            }
        }
    }

    fn open(&self) {
        self.state.lock().1 = true;
        self.changed.notify_all();
    }

    fn parked(&self) -> bool {
        let state = self.state.lock();
        state.0 && !state.1
    }
}

struct Parked(Arc<Gate>);

impl Drop for Parked {
    fn drop(&mut self) {
        self.0.open();
    }
}

/// Park `ctx`'s worker: work handed to the partition waits behind it.
fn park_worker(ctx: &MykoServerContext) -> Parked {
    let gate = Arc::new(Gate::default());
    let parked = Arc::clone(&gate);
    ctx.domain().hand_off(move || parked.pass());
    let mut state = gate.state.lock();
    while !state.0 {
        assert!(
            !gate.changed.wait_for(&mut state, DEADLINE).timed_out(),
            "the partition's worker never parked"
        );
    }
    drop(state);
    Parked(gate)
}

#[test]
fn deleting_a_shared_parent_deletes_its_children_in_every_partition_once() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    fixture.root.set(&note("a", "n-a")).expect("note in a");
    fixture.root.set(&note("b", "n-b")).expect("note in b");
    settle(&fixture.a);
    settle(&fixture.b);

    let parked = park_worker(&fixture.a);
    fixture.root.del(&folder("f")).expect("delete folder");
    settle(&fixture.b);
    assert!(
        !has(&fixture.b, PrlNote::ENTITY_NAME_STATIC, "n-b"),
        "b deleted its child while a was parked"
    );
    assert!(parked.0.parked(), "a was parked throughout");
    assert!(
        has(&fixture.a, PrlNote::ENTITY_NAME_STATIC, "n-a"),
        "a's child waits behind its parked worker"
    );
    drop(parked);
    settle(&fixture.a);
    assert!(!has(&fixture.a, PrlNote::ENTITY_NAME_STATIC, "n-a"));

    let notes = fixture.log.of(PrlNote::ENTITY_NAME_STATIC);
    for id in ["n-a", "n-b"] {
        assert_eq!(
            notes
                .iter()
                .filter(|(change, logged)| *change == MEventType::DEL && logged == id)
                .count(),
            1,
            "{id}'s delete is logged once: {notes:?}"
        );
    }
}

#[test]
fn a_partitioned_parent_cascades_inside_its_partition() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    fixture.root.set(&note("a", "n-a")).expect("note in a");
    fixture
        .root
        .set(&line("a", "l-a", "n-a"))
        .expect("line in a");
    // Names a note of another partition: outside the relation's reach.
    fixture
        .root
        .set(&line("b", "l-b", "n-a"))
        .expect("line in b");
    settle(&fixture.a);
    settle(&fixture.b);

    fixture.a.del(&note("a", "n-a")).expect("delete note in a");
    settle(&fixture.a);
    settle(&fixture.b);
    assert!(!has(&fixture.a, PrlLine::ENTITY_NAME_STATIC, "l-a"));
    assert!(
        has(&fixture.b, PrlLine::ENTITY_NAME_STATIC, "l-b"),
        "a cascade in a never writes b"
    );
    assert_eq!(
        fixture.log.of(PrlLine::ENTITY_NAME_STATIC),
        [
            (MEventType::SET, "l-a".to_owned()),
            (MEventType::SET, "l-b".to_owned()),
            (MEventType::DEL, "l-a".to_owned()),
        ]
    );
}

#[test]
fn each_partition_ensures_its_own_rows_for_a_shared_dependency() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    assert_eq!(cursors(&fixture.a), ["a"]);
    assert_eq!(cursors(&fixture.b), ["b"]);
    assert!(
        fixture
            .root
            .registry
            .get(PrlCursor::ENTITY_NAME_STATIC)
            .is_none(),
        "root holds no ensured partition rows"
    );
    // Writing the room again ensures nothing new.
    fixture.root.set(&room("a")).expect("room again");
    settle(&fixture.a);
    assert_eq!(cursors(&fixture.a), ["a"]);
    assert_eq!(
        fixture
            .log
            .of(PrlCursor::ENTITY_NAME_STATIC)
            .iter()
            .filter(|(change, _)| *change == MEventType::SET)
            .count(),
        2,
        "one cursor per partition is created and logged"
    );
}

/// Set while a test wants a cursor's creation refused by validation.
static REFUSE_CURSORS: AtomicBool = AtomicBool::new(false);

fn refuse_cursors(state: &ProposedState<'_>, _reversal: bool) -> Result<(), String> {
    let cursor = state
        .upserts
        .iter()
        .any(|item| AnyItem::as_any(item.as_ref()).is::<PrlCursor>());
    if cursor && REFUSE_CURSORS.load(Ordering::SeqCst) {
        Err("cursors refused".into())
    } else {
        Ok(())
    }
}

myko::inventory::submit! {
    MutationValidatorRegistration {
        validate_reversal_deletes: false,
        entity_types: &[PrlCursor::ENTITY_NAME_STATIC],
        validate: refuse_cursors,
    }
}

struct Refusing;

impl Refusing {
    fn begin() -> Self {
        REFUSE_CURSORS.store(true, Ordering::SeqCst);
        Self
    }
}

impl Drop for Refusing {
    fn drop(&mut self) {
        REFUSE_CURSORS.store(false, Ordering::SeqCst);
    }
}

#[test]
fn a_refused_derived_write_fails_its_partition_and_refuses_later_writes() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let refusing = Refusing::begin();
    fixture.root.set(&room("c")).expect("room c");
    let c = fixture
        .root
        .partition(&PartitionKey::new("c"))
        .expect("room c");
    settle(&c);
    drop(refusing);
    assert!(cursors(&c).is_empty(), "the refused cursor is not applied");
    let key = PartitionKey::new("c");
    let failure = fixture
        .root
        .partition_failure(&key)
        .expect("c reports its failure");
    assert!(failure.message.contains("cursors refused"), "{failure}");
    let refused = fixture
        .root
        .set(&note("c", "n-c"))
        .expect_err("a failed partition admits no write");
    assert!(
        refused.message.contains("partition c failed")
            && refused.message.contains("cursors refused"),
        "{refused}"
    );
    assert!(
        fixture
            .root
            .partition_failure(&PartitionKey::new("a"))
            .is_none(),
        "another partition is untouched"
    );
    fixture
        .root
        .set(&note("a", "n-a"))
        .expect("a still admits writes");
}

#[test]
fn a_write_derived_after_its_partition_closes_is_applied_and_logged() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    fixture.root.set(&note("a", "n-a")).expect("note in a");
    settle(&fixture.a);

    let parked = park_worker(&fixture.a);
    fixture.root.del(&folder("f")).expect("delete folder");
    fixture.root.del(&room("a")).expect("close a");
    assert!(
        fixture.root.partition(&PartitionKey::new("a")).is_none(),
        "a admits no more writes"
    );
    drop(parked);
    settle(&fixture.a);
    assert!(
        fixture
            .log
            .of(PrlNote::ENTITY_NAME_STATIC)
            .contains(&(MEventType::DEL, "n-a".to_owned())),
        "the cascade the folder's delete derived in a is logged"
    );
}

#[test]
fn hydrated_children_are_reached_by_a_live_cascade() {
    let _serial = SERIAL.lock();
    let log = Log::default();
    let root = root_logging(&log).with_partitions(schema());
    let events = [
        MEvent::from_item(&room("a"), MEventType::SET, "log"),
        MEvent::from_item(&room("b"), MEventType::SET, "log"),
        MEvent::from_item(&folder("f"), MEventType::SET, "log"),
        MEvent::from_item(&note("a", "n-a"), MEventType::SET, "log"),
        MEvent::from_item(&note("b", "n-b"), MEventType::SET, "log"),
    ];
    root.replay(events).expect("replay");
    let a = root.partition(&PartitionKey::new("a")).expect("room a");
    let b = root.partition(&PartitionKey::new("b")).expect("room b");
    assert!(has(&a, PrlNote::ENTITY_NAME_STATIC, "n-a"));
    assert!(has(&b, PrlNote::ENTITY_NAME_STATIC, "n-b"));

    root.del(&folder("f")).expect("delete folder");
    settle(&a);
    settle(&b);
    assert!(!has(&a, PrlNote::ENTITY_NAME_STATIC, "n-a"));
    assert!(!has(&b, PrlNote::ENTITY_NAME_STATIC, "n-b"));
}

fn refusal(schema: PartitionSchema) -> String {
    let log = Log::default();
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(root_logging(&log).with_partitions(schema));
    }))
    .expect_err("the schema is refused");
    refused
        .downcast_ref::<String>()
        .cloned()
        .expect("the refusal says why")
}

#[test]
fn a_schema_whose_relations_flow_back_or_across_is_refused() {
    let _serial = SERIAL.lock();
    let refused = refusal(
        PartitionSchema::new().partition_by::<PrlBook>(|book| PartitionKey::new(book.room.clone())),
    );
    assert!(
        refused.contains("PrlTag belongs to PrlBook"),
        "a partitioned parent of shared children: {refused}"
    );
    assert!(
        refused.contains("PrlShelf (shared) owns many PrlBook (partitioned)"),
        "a shared owner of partitioned children: {refused}"
    );
    assert!(
        !refused.contains("PrlNote") && !refused.contains("PrlCursor"),
        "relations into partitions are allowed: {refused}"
    );
}

#[test]
fn an_ensured_shared_type_cannot_depend_on_a_partitioned_type() {
    let _serial = SERIAL.lock();
    let refused = refusal(
        PartitionSchema::new().partition_by::<PrlRoom>(|room| PartitionKey::new(room.id.clone())),
    );
    assert!(
        refused.contains("PrlCursor is ensured for partitioned PrlRoom"),
        "{refused}"
    );
}

fn notes_in_folder(ctx: &MykoServerContext, folder: &str) -> usize {
    let query = note::GetPrlNotesByQuery(note::PrlNoteQuery {
        folder_id: Some(myko::query::IdFilter::Eq(PrlFolderId(folder.into()))),
        ..Default::default()
    });
    let live = ctx
        .query_map(query.clone(), ctx.new_server_transaction())
        .snapshot()
        .len();
    let once = ctx
        .query_snapshot(query, ctx.new_server_transaction())
        .len();
    assert_eq!(live, once, "a one-shot read agrees with the live query");
    live
}

#[test]
fn a_belongs_to_query_reads_its_own_partitions_rows() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    fixture.root.set(&note("a", "n-a")).expect("note in a");
    fixture.root.set(&note("b", "n-b")).expect("note in b");
    fixture
        .root
        .set(&note("b", "n-b2"))
        .expect("second note in b");
    settle(&fixture.a);
    settle(&fixture.b);
    assert_eq!(
        [
            notes_in_folder(&fixture.a, "f"),
            notes_in_folder(&fixture.b, "f")
        ],
        [1, 2],
        "each partition's routed query reads its own store"
    );
}

#[test]
fn setting_a_lifecycle_row_whose_ensure_for_creates_partition_rows_succeeds() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    fixture
        .root
        .set(&room("c"))
        .expect("the partition is open before what the write derives");
    let c = fixture
        .root
        .partition(&PartitionKey::new("c"))
        .expect("room c");
    settle(&c);
    assert_eq!(cursors(&c), ["c"]);
}

#[test]
fn an_ensured_partition_row_is_ensured_again_once_deleted() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let cursor = PrlCursor {
        id: cursor::PrlCursorId(
            fixture
                .a
                .registry
                .get_or_create(PrlCursor::ENTITY_NAME_STATIC)
                .snapshot()
                .into_iter()
                .next()
                .expect("a's cursor")
                .0,
        ),
        room_id: room::PrlRoomId("a".into()),
    };
    fixture.root.del(&cursor).expect("delete a's cursor");
    settle(&fixture.a);
    assert!(cursors(&fixture.a).is_empty());
    fixture.root.set(&room("a")).expect("room again");
    settle(&fixture.a);
    assert_eq!(
        cursors(&fixture.a),
        ["a"],
        "the first cursor's reservation was released when it landed in a"
    );
}

#[cfg(feature = "search")]
#[test]
fn a_search_finds_only_its_own_partitions_rows() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    for (room, id) in [("a", "m-a"), ("b", "m-b")] {
        fixture
            .root
            .set(&memo::PrlMemo {
                id: id.into(),
                room: room.into(),
                text: "harbour lights".into(),
            })
            .expect("memo");
    }
    settle(&fixture.a);
    settle(&fixture.b);
    let found = |ctx: &MykoServerContext| -> Vec<String> {
        ctx.search_index()
            .search(memo::PrlMemo::ENTITY_NAME_STATIC, "harbour", 10)
            .iter()
            .map(ToString::to_string)
            .collect()
    };
    assert_eq!(found(&fixture.a), ["m-a"]);
    assert_eq!(found(&fixture.b), ["m-b"]);
}

#[test]
fn settling_waits_for_every_admitted_write_in_every_partition() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    fixture.root.set(&note("a", "n-a")).expect("note in a");
    fixture.root.set(&note("b", "n-b")).expect("note in b");
    fixture.root.settle();
    let parked = park_worker(&fixture.a);
    // Deleting the shared parent derives in each partition, behind root.
    fixture.root.del(&folder("f")).expect("delete folder");
    let (settled_tx, settled) = std::sync::mpsc::channel();
    let root = fixture.root.clone();
    let settling = std::thread::spawn(move || {
        root.settle();
        assert!(settled_tx.send(()).is_ok());
    });
    assert!(parked.0.parked(), "a is parked");
    assert!(
        settled.try_recv().is_err(),
        "settling returned while a's writes waited behind its parked worker"
    );
    drop(parked);
    settled
        .recv_timeout(DEADLINE)
        .expect("settling returns once a is released");
    assert!(settling.join().is_ok());
    for (ctx, id) in [(&fixture.a, "n-a"), (&fixture.b, "n-b")] {
        assert!(
            !has(ctx, PrlNote::ENTITY_NAME_STATIC, id),
            "the cascade's delete of {id} is applied"
        );
    }
    assert_eq!(
        fixture
            .log
            .of(PrlNote::ENTITY_NAME_STATIC)
            .iter()
            .filter(|(change, _)| *change == MEventType::DEL)
            .count(),
        2
    );
}

fn lines_of(ctx: &MykoServerContext, note: &str) -> usize {
    let query = line::GetPrlLinesByQuery(line::PrlLineQuery {
        note_id: Some(myko::query::IdFilter::Eq(PrlNoteId(note.into()))),
        ..Default::default()
    });
    ctx.query_snapshot(query, ctx.new_server_transaction())
        .len()
}

#[test]
fn a_one_shot_read_during_a_partitions_open_batch_sees_the_row_written_in_it() {
    let _serial = SERIAL.lock();
    let fixture = Arc::new(fixture());
    fixture.root.set(&note("a", "n1")).expect("note");
    settle(&fixture.a);
    assert_eq!(lines_of(&fixture.a, "n1"), 0, "the index is built empty");

    // The row is stored, but its diff waits for the batch to close, so the
    // index has not seen it while the writer is parked.
    let gate = Arc::new(Gate::default());
    let writer = {
        let (fixture, gate) = (Arc::clone(&fixture), Arc::clone(&gate));
        std::thread::spawn(move || {
            fixture.a.domain().batch(|| {
                let written = fixture.a.set(&line("a", "l1", "n1"));
                gate.pass();
                written
            })
        })
    };
    let deadline = std::time::Instant::now() + DEADLINE;
    while !gate.parked() {
        assert!(
            std::time::Instant::now() < deadline,
            "the writer never parked"
        );
        std::thread::yield_now();
    }
    let seen = lines_of(&fixture.a, "n1");
    gate.open();
    writer.join().expect("writer").expect("line");
    assert_eq!(
        seen, 1,
        "a read while a's batch is open falls back to the store"
    );
}

/// The domain the last registered memo source was built in.
static MEMO_SOURCE_BUILT_IN: Mutex<Option<hyphae::scheduler::Domain>> = Mutex::new(None);

myko::inventory::submit! {
    myko::query::EntityQuerySourceRegistration {
        entity_type: memo::PrlMemo::ENTITY_NAME_STATIC,
        build: |_, _| {
            *MEMO_SOURCE_BUILT_IN.lock() = Some(hyphae::scheduler::Domain::current());
            None
        },
    }
}

#[test]
fn a_one_shot_read_builds_its_source_in_its_partition() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    drop(
        fixture
            .a
            .query_snapshot(memo::GetAllPrlMemos {}, fixture.a.new_server_transaction()),
    );
    assert_eq!(
        MEMO_SOURCE_BUILT_IN.lock().take(),
        Some(fixture.a.domain()),
        "the source is built in a's domain, not the caller's"
    );
}

#[test]
fn a_panic_in_a_partitions_propagation_fails_it_and_refuses_later_writes() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    fixture
        .a
        .domain()
        .hand_off(|| panic!("a partition's wave broke"));
    settle(&fixture.a);
    let key = PartitionKey::new("a");
    let failure = fixture
        .root
        .partition_failure(&key)
        .expect("a reports its failure");
    assert!(
        failure.message.contains("a partition's wave broke"),
        "{failure}"
    );
    assert!(fixture.a.domain().is_closed(), "the failed partition's domain stops");
    let refused = fixture
        .root
        .set(&note("a", "after"))
        .expect_err("a failed partition admits no write");
    assert!(refused.message.contains("partition a failed"), "{refused}");
    assert!(
        fixture.root.partition_failure(&PartitionKey::new("b")).is_none(),
        "another partition is untouched"
    );
    fixture
        .root
        .set(&note("b", "n-b"))
        .expect("b still admits writes");
}
