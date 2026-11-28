//! Graph edges across the partition boundary.
//!
//! Each partition projects its own partitioned edge types; root projects the
//! shared ones. Deleting a shared endpoint cascades the partitioned edges
//! that reach it, in each partition. An edge whose endpoint deletes would
//! carry data backwards is refused when the schema is built.

#![allow(clippy::expect_used, clippy::panic)]

use std::sync::Arc;

// The macros name `crate::` inside myko's own package, tests included.
pub use myko::*;
use myko::{
    prelude::Eventable,
    server::{MykoServerContext, PartitionKey, PartitionSchema},
};
use parking_lot::Mutex;

/// Root's tick is shared by the process, so the tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

pub mod board {
    use myko::prelude::*;

    /// Shared. Setting a board opens the partition named by its id.
    #[myko_item]
    pub struct PgBoard {}
}

pub mod label {
    use myko::prelude::*;

    /// Shared endpoint of partitioned edges.
    #[myko_item]
    pub struct PgLabel {}
}

pub mod card {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Partitioned by its board.
    #[myko_item]
    pub struct PgCard {
        #[myko_partition_key]
        pub board: Arc<str>,
    }
}

pub mod labelling {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::{
        card::{PgCard, PgCardId},
        label::{PgLabel, PgLabelId},
    };

    /// Partitioned edge from a shared label to a partitioned card.
    #[myko_item]
    pub struct PgLabelling {
        #[myko_partition_key]
        pub board: Arc<str>,
        pub label_id: PgLabelId,
        pub card_id: PgCardId,
    }

    #[myko_edge]
    impl GraphEdge for PgLabelling {
        type Ends = Directed<ConcreteEndpoint<PgLabel>, ConcreteEndpoint<PgCard>>;

        fn ends(&self) -> (PgLabelId, PgCardId) {
            (self.label_id.clone(), self.card_id.clone())
        }
    }
}

pub mod stuck_on {
    use std::sync::Arc;

    use myko::{
        graph::{ConcreteEndpoint, EndpointSpec},
        prelude::*,
    };

    use super::{
        label::{PgLabel, PgLabelId},
        stuck::PgStuck,
    };

    /// The stickings of a label, read through the graph.
    #[myko_query(PgStuck)]
    pub struct GetPgStucksOnLabel {
        pub label_id: PgLabelId,
    }

    impl QueryHandler for GetPgStucksOnLabel {
        fn test_entity(ctx: QueryTestContext<Self>) -> bool {
            ctx.item.label_id == ctx.query.label_id
        }

        fn build_view(
            ctx: myko::query::QueryBuildArgs<Self>,
        ) -> Option<impl MapQuery<Key = Arc<str>, Value = Arc<dyn myko::item::AnyItem>>> {
            let endpoint =
                <ConcreteEndpoint<PgLabel> as EndpointSpec>::erase(&ctx.query.label_id).ok()?;
            ctx.query_context
                .graph_watch_many_at::<PgStuck>(myko::graph::EndPosition::A, &[endpoint])
                .ok()
        }
    }
}

pub mod sticker {
    use std::sync::Arc;

    use myko::prelude::*;

    /// Shared, except where a schema partitions it.
    #[myko_item]
    pub struct PgSticker {
        pub board: Arc<str>,
    }
}

pub mod stuck {
    use std::sync::Arc;

    use myko::prelude::*;

    use super::{
        label::{PgLabel, PgLabelId},
        sticker::{PgSticker, PgStickerId},
    };

    /// Shared edge to a sticker; refused once stickers are partitioned.
    #[myko_item]
    pub struct PgStuck {
        pub board: Arc<str>,
        pub label_id: PgLabelId,
        pub sticker_id: PgStickerId,
    }

    #[myko_edge]
    impl GraphEdge for PgStuck {
        type Ends = Directed<ConcreteEndpoint<PgLabel>, ConcreteEndpoint<PgSticker>>;

        fn ends(&self) -> (PgLabelId, PgStickerId) {
            (self.label_id.clone(), self.sticker_id.clone())
        }

        const A_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::RestrictEndpointDelete;
        const B_DELETE: EndpointDeletePolicy = EndpointDeletePolicy::RetainDangling;
    }
}

use board::PgBoard;
use card::{PgCard, PgCardId};
use label::{PgLabel, PgLabelId};
use labelling::PgLabelling;
use sticker::PgSticker;
use stuck::PgStuck;

fn schema() -> PartitionSchema {
    PartitionSchema::new().lifecycle::<PgBoard>()
}

fn root(schema: PartitionSchema) -> MykoServerContext {
    myko::server::MykoServerContext::new(
        uuid::Uuid::new_v4(),
        Arc::new(myko::store::StoreRegistry::new()),
        Arc::new(myko::server::HandlerRegistry::new()),
        Arc::new(myko::server::RelationshipManager::new()),
        Arc::new(myko::server::persister::PersisterRouter::default()),
        Arc::new(myko::search::SearchIndex::new()),
        myko::server::MykoServerRuntime {
            peer_clients: Arc::new(dashmap::DashMap::new()),
            event_sink: None,
            history_replay: None,
        },
    )
    .with_partitions(schema)
}

struct Fixture {
    root: MykoServerContext,
    a: MykoServerContext,
    b: MykoServerContext,
}

/// Boards `a` and `b`, label `l`, and in each board a card labelled `l`.
fn fixture() -> Fixture {
    let root = root(schema());
    for id in ["a", "b"] {
        root.set(&PgBoard { id: id.into() }).expect("board");
    }
    root.set(&PgLabel { id: "l".into() }).expect("label");
    for board in ["a", "b"] {
        root.set(&PgCard {
            id: format!("card-{board}").into(),
            board: board.into(),
        })
        .expect("card");
        root.set(&labelling(board)).expect("labelling");
    }
    let a = root.partition(&PartitionKey::new("a")).expect("board a");
    let b = root.partition(&PartitionKey::new("b")).expect("board b");
    root.settle();
    Fixture { root, a, b }
}

fn labelling(board: &str) -> PgLabelling {
    PgLabelling {
        id: format!("labelling-{board}").into(),
        board: board.into(),
        label_id: PgLabelId("l".into()),
        card_id: PgCardId(format!("card-{board}").into()),
    }
}

fn labelled(ctx: &MykoServerContext) -> Vec<Arc<str>> {
    ctx.edges::<PgLabelling>()
        .from_ids(&PgLabelId("l".into()))
        .expect("labellings from l")
}

#[test]
fn each_partition_projects_its_own_edges() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    assert_eq!(labelled(&fixture.a), [Arc::<str>::from("labelling-a")]);
    assert_eq!(labelled(&fixture.b), [Arc::<str>::from("labelling-b")]);
    assert!(
        fixture.root.graph_index().is_none_or(|graph| graph
            .registration(PgLabelling::ENTITY_NAME_STATIC)
            .is_none()),
        "root projects no partitioned edge type"
    );
}

#[test]
fn deleting_a_shared_endpoint_cascades_its_edges_in_every_partition() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    fixture
        .root
        .del(&PgLabel { id: "l".into() })
        .expect("delete label");
    fixture.root.settle();
    for ctx in [&fixture.a, &fixture.b] {
        assert!(labelled(ctx).is_empty(), "the projection lost the edge");
        assert!(
            ctx.registry
                .get_or_create(PgLabelling::ENTITY_NAME_STATIC)
                .snapshot()
                .is_empty(),
            "the cascade deleted the edge row"
        );
    }
}

fn refusal(schema: PartitionSchema) -> String {
    let _serial = SERIAL.lock();
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(root(schema))))
        .expect_err("the schema was accepted");
    refused
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default()
}

#[test]
fn a_shared_edge_naming_a_partitioned_endpoint_is_refused() {
    let refused = refusal(
        schema().partition_by::<PgSticker>(|sticker| PartitionKey::new(sticker.board.clone())),
    );
    assert!(
        refused.contains("shared edge PgStuck names partitioned endpoint PgSticker"),
        "{refused}"
    );
}

#[test]
fn a_partitioned_edge_restricting_a_shared_endpoint_is_refused() {
    let refused = refusal(
        schema()
            .partition_by::<PgSticker>(|sticker| PartitionKey::new(sticker.board.clone()))
            .partition_by::<PgStuck>(|stuck| PartitionKey::new(stuck.board.clone())),
    );
    assert!(
        refused.contains("partitioned edge PgStuck restricts deleting a shared endpoint"),
        "{refused}"
    );
}

#[test]
fn a_saga_reads_a_partitioned_events_row_in_its_partition() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let saga = myko::saga::SagaContext::new(uuid::Uuid::new_v4(), fixture.root.registry.clone())
        .with_server(fixture.root.clone());
    let card = PgCard {
        id: "card-b".into(),
        board: "b".into(),
    };
    let event = myko::wire::MEvent::from_item(&card, myko::wire::MEventType::SET, "test");
    let held = saga.for_event(&event).expect("b is open");
    assert!(
        myko::prelude::RegistryScoped::registry(&held)
            .get(PgCard::ENTITY_NAME_STATIC)
            .and_then(|store| store.get_value(&Arc::from("card-b")))
            .is_some(),
        "the saga reads the card where b holds it"
    );

    fixture
        .root
        .del(&PgBoard { id: "b".into() })
        .expect("close b");
    assert!(
        matches!(
            saga.for_event(&event),
            Err(myko::server::PartitionError::NotOpen(key)) if key.as_str() == "b"
        ),
        "an event of a closed partition is refused, not read from root"
    );
}

#[test]
fn reading_a_partitioned_type_from_root_outside_checked_panics() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    let refused = fixture.root.checked(|| {
        drop(
            fixture
                .root
                .registry
                .get_or_create(PgCard::ENTITY_NAME_STATIC),
        );
    });
    assert!(
        refused.is_err(),
        "inside checked the read is refused with an error"
    );
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(
            fixture
                .root
                .registry
                .get_or_create(PgCard::ENTITY_NAME_STATIC),
        );
    }))
    .expect_err("outside checked the read panics");
    let message = panicked
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(
        message.contains("PgCard was read from the root context"),
        "{message}"
    );
}

#[test]
fn root_reads_the_shared_edges_root_writes() {
    let _serial = SERIAL.lock();
    let fixture = fixture();
    fixture
        .root
        .set_edge_apply_mode::<stuck::PgStuck>(myko::graph::EdgeApplyMode::Observe)
        .expect("observe");
    fixture
        .root
        .set(&sticker::PgSticker {
            id: "s".into(),
            board: "a".into(),
        })
        .expect("sticker");
    fixture
        .root
        .set(&stuck::PgStuck {
            id: "stuck".into(),
            board: "a".into(),
            label_id: label::PgLabelId("l".into()),
            sticker_id: sticker::PgStickerId("s".into()),
        })
        .expect("stuck");
    fixture.root.settle();
    let rows = fixture.root.query_map(
        stuck_on::GetPgStucksOnLabel {
            label_id: label::PgLabelId("l".into()),
        },
        fixture.root.new_server_transaction(),
    );
    assert_eq!(
        rows.snapshot().len(),
        1,
        "root's graph read holds the shared edge root wrote"
    );
}

#[test]
fn root_reads_the_shared_edges_a_replayed_log_holds() {
    use myko::wire::{MEvent, MEventType};
    let _serial = SERIAL.lock();
    let root = root(schema());
    let events = [
        MEvent::from_item(&PgBoard { id: "a".into() }, MEventType::SET, "log"),
        MEvent::from_item(&PgLabel { id: "l".into() }, MEventType::SET, "log"),
        MEvent::from_item(
            &PgSticker {
                id: "s".into(),
                board: "a".into(),
            },
            MEventType::SET,
            "log",
        ),
        MEvent::from_item(
            &PgStuck {
                id: "stuck".into(),
                board: "a".into(),
                label_id: PgLabelId("l".into()),
                sticker_id: sticker::PgStickerId("s".into()),
            },
            MEventType::SET,
            "log",
        ),
    ];
    root.replay(events).expect("replay");
    root.settle();
    let rows = root.query_map(
        stuck_on::GetPgStucksOnLabel {
            label_id: PgLabelId("l".into()),
        },
        root.new_server_transaction(),
    );
    assert_eq!(
        rows.snapshot().len(),
        1,
        "root's graph read holds the shared edge the log replayed"
    );
}

#[test]
fn root_reads_the_shared_edges_a_loaded_snapshot_holds() {
    use myko::wire::{MEvent, MEventType};
    let _serial = SERIAL.lock();
    let root = root(schema());
    let mut loading = root.load_log();
    for event in [
        MEvent::from_item(
            &PgStuck {
                id: "stuck".into(),
                board: "a".into(),
                label_id: PgLabelId("l".into()),
                sticker_id: sticker::PgStickerId("s".into()),
            },
            MEventType::SET,
            "log",
        ),
        MEvent::from_item(&PgBoard { id: "a".into() }, MEventType::SET, "log"),
        MEvent::from_item(&PgLabel { id: "l".into() }, MEventType::SET, "log"),
        MEvent::from_item(
            &PgSticker {
                id: "s".into(),
                board: "a".into(),
            },
            MEventType::SET,
            "log",
        ),
    ] {
        loading.push(&event);
    }
    loading.finish().expect("load");
    root.settle();
    let rows = root.query_map(
        stuck_on::GetPgStucksOnLabel {
            label_id: PgLabelId("l".into()),
        },
        root.new_server_transaction(),
    );
    assert_eq!(
        rows.snapshot().len(),
        1,
        "root's graph read holds the shared edge the snapshot loaded"
    );
}
