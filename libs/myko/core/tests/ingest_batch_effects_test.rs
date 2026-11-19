//! A wire batch whose later events depend on its earlier ones.
//!
//! The websocket reader applies a batch inside `hyphae::submit`, so the whole
//! batch is one event set and every store read made while applying it sees
//! batch is one event set and no subscriber hears of any of it until the set
//! settles. Relationship cascades and the graph projection run while the set
//! is still being built, and they read the stores. They get the right answer
//! because a store holds a write from the moment it is made; only the diff is
//! held back. Each scenario here applies one batch twice, once the way a
//! command does (the reduce settles before the effects run) and once the way
//! the websocket reader does, and expects the same end state.

#![allow(clippy::expect_used)]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub use myko::*;
use myko::{
    prelude::*,
    search::SearchIndex,
    server::{
        HandlerRegistry, MykoServerContext, MykoServerRuntime, RelationshipManager,
        persister::PersisterRouter,
    },
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

mod folder {
    use myko::prelude::*;
    #[myko_item]
    pub struct IngestFolder {
        #[belongs_to(IngestFolder)]
        pub parent_id: IngestFolderId,
    }
}
use folder::IngestFolder;

mod member {
    use myko::prelude::*;
    #[myko_item]
    pub struct IngestMember {
        pub name: String,
    }
}
use member::{IngestMember, IngestMemberId};

mod group {
    use myko::prelude::*;

    use super::IngestMemberId;
    #[myko_item]
    pub struct IngestGroup {
        #[owns_many(IngestMember)]
        pub member_ids: Vec<IngestMemberId>,
    }
}
use group::IngestGroup;

mod owner {
    use myko::prelude::*;
    #[myko_item]
    pub struct IngestOwner {
        pub name: String,
    }
}
use owner::{IngestOwner, IngestOwnerId};

mod status {
    use myko::prelude::*;

    use super::{IngestOwner, IngestOwnerId};
    #[myko_item]
    pub struct IngestStatus {
        #[ensure_for(IngestOwner)]
        pub owner_id: IngestOwnerId,
    }
}
use status::IngestStatus;

mod label {
    use myko::prelude::*;
    #[myko_item]
    pub struct IngestLabel {
        pub name: String,
    }
}
use label::{IngestLabel, IngestLabelId};

mod doc {
    use myko::prelude::*;
    #[myko_item]
    pub struct IngestDoc {
        pub name: String,
    }
}
use doc::{IngestDoc, IngestDocId};

mod labeling {
    use myko::prelude::*;

    use super::{IngestDoc, IngestDocId, IngestLabel, IngestLabelId};
    #[myko_item]
    pub struct IngestLabeling {
        pub label_id: IngestLabelId,
        pub doc_id: IngestDocId,
    }

    #[myko_edge]
    impl GraphEdge for IngestLabeling {
        type Ends = Directed<ConcreteEndpoint<IngestLabel>, ConcreteEndpoint<IngestDoc>>;

        fn ends(&self) -> (IngestLabelId, IngestDocId) {
            (self.label_id.clone(), self.doc_id.clone())
        }

        const ADJACENCY: AdjacencyPolicy = AdjacencyPolicy::Eager;
    }
}
use labeling::IngestLabeling;

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

fn folder(id: &str, parent: &str) -> IngestFolder {
    IngestFolder {
        id: id.into(),
        parent_id: parent.into(),
    }
}

fn member(id: &str) -> IngestMember {
    IngestMember {
        id: id.into(),
        name: id.into(),
    }
}

fn group(id: &str, members: &[&str]) -> IngestGroup {
    IngestGroup {
        id: id.into(),
        member_ids: members.iter().map(|id| IngestMemberId::from(*id)).collect(),
    }
}

fn owner(id: &str) -> IngestOwner {
    IngestOwner {
        id: id.into(),
        name: id.into(),
    }
}

fn status(id: &str, owner: &str) -> IngestStatus {
    IngestStatus {
        id: id.into(),
        owner_id: owner.into(),
    }
}

fn label(id: &str) -> IngestLabel {
    IngestLabel {
        id: id.into(),
        name: id.into(),
    }
}

fn doc(id: &str) -> IngestDoc {
    IngestDoc {
        id: id.into(),
        name: id.into(),
    }
}

fn labeling(id: &str, label: &str, doc: &str) -> IngestLabeling {
    IngestLabeling {
        id: id.into(),
        label_id: label.into(),
        doc_id: doc.into(),
    }
}

fn set(item: &impl Eventable) -> MEvent {
    MEvent::from_item(item, MEventType::SET, "ingest-test")
}

fn del(item: &impl Eventable) -> MEvent {
    MEvent::from_item(item, MEventType::DEL, "ingest-test")
}

/// How the batch reaches the stores.
#[derive(Clone, Copy, Debug)]
enum Ingest {
    /// Outside any event set, as a command emits: each store reduce settles
    /// before the effects that follow it read the stores.
    Settled,
    /// As the websocket reader does it: the whole batch is one event set.
    OneEventSet,
}

/// What a client could read back once everything has settled.
#[derive(Debug, PartialEq, Eq)]
struct EndState {
    folders: Vec<String>,
    members: Vec<String>,
    groups: Vec<String>,
    owners: Vec<String>,
    /// The owner of each status row. Ensured rows get random ids.
    status_owners: Vec<String>,
    labels: Vec<String>,
    docs: Vec<String>,
    labelings: Vec<String>,
    graph_edges: usize,
    graph_edges_after_rebuild: usize,
}

fn ids(ctx: &MykoServerContext, entity_type: &str) -> Vec<String> {
    let mut ids: Vec<String> = ctx
        .registry
        .get(entity_type)
        .map_or_else(Vec::new, |store| {
            store
                .snapshot()
                .into_iter()
                .map(|(id, _)| id.to_string())
                .collect()
        });
    ids.sort();
    ids
}

fn end_state(ctx: &MykoServerContext) -> EndState {
    let mut status_owners: Vec<String> =
        ctx.registry
            .get("IngestStatus")
            .map_or_else(Vec::new, |store| {
                store
                    .snapshot()
                    .into_iter()
                    .filter_map(|(_, item)| {
                        item.as_any()
                            .downcast_ref::<IngestStatus>()
                            .map(|status| status.owner_id.to_string())
                    })
                    .collect()
            });
    status_owners.sort();
    let graph = ctx.graph_index().expect("graph index");
    let graph_edges = graph.diagnostics().edge_count;
    graph.rebuild().expect("rebuild graph from the stores");
    EndState {
        folders: ids(ctx, "IngestFolder"),
        members: ids(ctx, "IngestMember"),
        groups: ids(ctx, "IngestGroup"),
        owners: ids(ctx, "IngestOwner"),
        status_owners,
        labels: ids(ctx, "IngestLabel"),
        docs: ids(ctx, "IngestDoc"),
        labelings: ids(ctx, "IngestLabeling"),
        graph_edges,
        graph_edges_after_rebuild: graph.diagnostics().edge_count,
    }
}

fn run(seed: &[MEvent], batch: &[MEvent], ingest: Ingest) -> EndState {
    let ctx = make_ctx();
    for event in seed {
        ctx.apply_event(event.clone()).expect("seed event");
    }
    match ingest {
        Ingest::Settled => {
            ctx.apply_event_batch(batch.to_vec()).expect("batch");
        }
        Ingest::OneEventSet => {
            hyphae::submit(&[], None, || ctx.apply_event_batch(batch.to_vec())).expect("batch");
        }
    }
    hyphae::flush();
    end_state(&ctx)
}

struct Scenario {
    name: &'static str,
    seed: Vec<MEvent>,
    batch: Vec<MEvent>,
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "independent sets",
            seed: vec![],
            batch: vec![set(&label("l1")), set(&doc("d1")), set(&folder("f1", ""))],
        },
        Scenario {
            name: "parent deleted in the batch that adds its child",
            seed: vec![set(&folder("root", ""))],
            batch: vec![set(&folder("child", "root")), del(&folder("root", ""))],
        },
        Scenario {
            name: "group deleted in the batch that adds its member",
            seed: vec![],
            batch: vec![
                set(&member("m1")),
                set(&group("g1", &["m1"])),
                del(&group("g1", &["m1"])),
            ],
        },
        Scenario {
            name: "owner set and deleted in one batch",
            seed: vec![],
            batch: vec![set(&owner("o1")), del(&owner("o1"))],
        },
        Scenario {
            name: "owner and its status set in one batch",
            seed: vec![],
            batch: vec![set(&owner("o1")), set(&status("given", "o1"))],
        },
        Scenario {
            name: "edge and both endpoints set in one batch",
            seed: vec![],
            batch: vec![
                set(&label("l1")),
                set(&doc("d1")),
                set(&labeling("e1", "l1", "d1")),
            ],
        },
        Scenario {
            name: "edge set and deleted in one batch",
            seed: vec![set(&label("l1")), set(&doc("d1"))],
            batch: vec![
                set(&labeling("e1", "l1", "d1")),
                del(&labeling("e1", "l1", "d1")),
            ],
        },
        Scenario {
            name: "endpoint deleted in the batch that adds its edge",
            seed: vec![set(&label("l1")), set(&doc("d1"))],
            batch: vec![set(&labeling("e1", "l1", "d1")), del(&label("l1"))],
        },
    ]
}

fn scenario(name: &str) -> Scenario {
    scenarios()
        .into_iter()
        .find(|scenario| scenario.name == name)
        .expect("scenario")
}

#[test]
fn a_settled_batch_runs_the_cascades_of_its_own_events() {
    let settled = |name: &str| {
        let scenario = scenario(name);
        run(&scenario.seed, &scenario.batch, Ingest::Settled)
    };
    assert!(
        settled("parent deleted in the batch that adds its child")
            .folders
            .is_empty()
    );
    assert!(
        settled("group deleted in the batch that adds its member")
            .members
            .is_empty()
    );
    assert!(
        settled("owner set and deleted in one batch")
            .status_owners
            .is_empty()
    );
    assert_eq!(
        settled("owner and its status set in one batch").status_owners,
        ["o1"]
    );
    assert_eq!(
        settled("edge and both endpoints set in one batch").graph_edges,
        1
    );
    assert_eq!(settled("edge set and deleted in one batch").graph_edges, 0);
    let cascaded = settled("endpoint deleted in the batch that adds its edge");
    assert!(cascaded.labelings.is_empty());
    assert_eq!(cascaded.graph_edges, 0);
}

#[test]
fn a_batch_in_one_event_set_ends_where_a_settled_batch_does() {
    let mismatches: Vec<String> = scenarios()
        .into_iter()
        .filter_map(|scenario| {
            let settled = run(&scenario.seed, &scenario.batch, Ingest::Settled);
            let one_set = run(&scenario.seed, &scenario.batch, Ingest::OneEventSet);
            (one_set != settled).then(|| {
                format!(
                    "{}\n  settled: {settled:?}\n  one event set: {one_set:?}",
                    scenario.name
                )
            })
        })
        .collect();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn a_store_holds_a_write_before_its_event_set_settles() {
    let ctx = make_ctx();
    let store = ctx.registry.get_or_create("IngestFolder");
    let diffs = Arc::new(AtomicUsize::new(0));
    let counted = diffs.clone();
    let _guard = store.subscribe_diffs(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    let before = diffs.load(Ordering::SeqCst);

    let (stored, delivered_inside) = hyphae::submit(&[], None, || {
        ctx.apply_event(set(&folder("f1", ""))).expect("event");
        (
            store.get_value(&Arc::<str>::from("f1")).is_some(),
            diffs.load(Ordering::SeqCst) - before,
        )
    });
    hyphae::flush();

    assert!(stored, "the store is read by the effects of later events");
    assert_eq!(delivered_inside, 0, "the diff waits for the event set");
    assert_eq!(diffs.load(Ordering::SeqCst) - before, 1);
}
