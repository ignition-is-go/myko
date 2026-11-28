//! A route that names its dependencies re-resolves only the rows an input
//! write can move, not every row of its type.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub use myko::*;
use myko::{
    prelude::{AnyItem, Eventable},
    server::{
        HandlerRegistry, MykoServerContext, MykoServerRuntime, PartitionKey, PartitionSchema,
        RelationshipManager, RouteInput, persister::PersisterRouter,
    },
    store::StoreRegistry,
};
use parking_lot::Mutex;
use uuid::Uuid;

static SERIAL: Mutex<()> = Mutex::new(());
/// Resolutions of a row so far; the resolver counts every call.
static RESOLVED: AtomicUsize = AtomicUsize::new(0);

pub mod room {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared. Setting a room opens the partition named by its id.
    #[myko_item]
    pub struct PrdRoom {
        pub label: Arc<str>,
    }
}

pub mod assign {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared. Places the row with the same id in `room`.
    #[myko_item]
    pub struct PrdAssign {
        pub room: Arc<str>,
    }
}

pub mod row {
    use myko::prelude::*;

    /// Routed by the assignment that shares its id.
    #[myko_item]
    pub struct PrdRow {
        pub level: i64,
    }
}

use assign::PrdAssign;
use room::PrdRoom;
use row::PrdRow;

const ROWS: usize = 50;

fn dependency(id: &str) -> Arc<str> {
    Arc::from(format!("assign:{id}"))
}

fn schema() -> PartitionSchema {
    PartitionSchema::new()
        .lifecycle::<PrdRoom>()
        .route_with_dependencies::<PrdRow>(
            vec![RouteInput::of::<PrdAssign>(|assign| {
                vec![dependency(&assign.id)]
            })],
            |row| vec![dependency(&row.id)],
            |root, row| {
                RESOLVED.fetch_add(1, Ordering::SeqCst);
                let assign = root
                    .registry
                    .get(PrdAssign::ENTITY_NAME_STATIC)?
                    .get_value(&row.id.0)?;
                let assign = AnyItem::as_any(assign.as_ref()).downcast_ref::<PrdAssign>()?;
                Some(PartitionKey::new(assign.room.clone()))
            },
        )
}

fn root() -> MykoServerContext {
    MykoServerContext::new(
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
    .with_partitions(schema())
}

fn has(ctx: &MykoServerContext, id: &str) -> bool {
    ctx.registry
        .get_or_create(PrdRow::ENTITY_NAME_STATIC)
        .get_value(&Arc::from(id))
        .is_some()
}

#[test]
fn an_input_write_re_resolves_only_the_rows_that_read_it() {
    let _serial = SERIAL.lock();
    let root = root();
    for id in ["a", "b"] {
        root.set(&PrdRoom {
            id: id.into(),
            label: id.into(),
        })
        .expect("room");
    }
    for index in 0..ROWS {
        let id = format!("r{index}");
        root.set(&PrdAssign {
            id: id.clone().into(),
            room: "a".into(),
        })
        .expect("assign");
        root.set(&PrdRow {
            id: id.into(),
            level: 0,
        })
        .expect("row");
    }
    root.settle();
    let a = root.partition(&PartitionKey::new("a")).expect("room a");
    let b = root.partition(&PartitionKey::new("b")).expect("room b");
    assert!(has(&a, "r0") && has(&a, "r1"));

    let before = RESOLVED.load(Ordering::SeqCst);
    root.set(&PrdAssign {
        id: "r0".into(),
        room: "b".into(),
    })
    .expect("move r0");
    root.settle();
    let resolutions = RESOLVED.load(Ordering::SeqCst) - before;

    assert!(
        has(&b, "r0") && !has(&a, "r0"),
        "r0 followed its assignment to b"
    );
    assert!(has(&a, "r1"), "r1 stayed in a");
    assert_eq!(
        resolutions, 1,
        "only r0 reads the assignment that changed, not all {ROWS} rows"
    );
}
