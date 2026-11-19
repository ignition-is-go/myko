//! Two threads that emit graph-coordinated entities at the same time.
//!
//! The graph authority covers the endpoint check, the store reduce, the graph
//! projection and the queueing of the diffs of one mutation. It is released
//! before the diffs are delivered and before the mutation's cascades run, so
//! another emitter can get in between a deleted endpoint and the deletion of
//! its edges. Whatever the interleaving, the stores and the projection must
//! agree afterwards, no edge may outlive an endpoint, and a subscriber that
//! replays a store's diffs must hold what the store holds.

#![allow(clippy::expect_used)]

use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier, Mutex, PoisonError, mpsc},
    time::Duration,
};

pub use myko::*;
use myko::{
    hyphae::{MapDiff, SubscriptionGuard},
    prelude::{AnyItem, Eventable},
    search::SearchIndex,
    server::{
        HandlerRegistry, MykoServerContext, MykoServerRuntime, RelationshipManager,
        persister::PersisterRouter,
    },
    store::StoreRegistry,
    wire::{MEvent, MEventType},
};
use uuid::Uuid;

mod label {
    use myko::prelude::*;
    #[myko_item]
    pub struct RaceLabel {
        pub name: String,
    }
}
use label::{RaceLabel, RaceLabelId};

mod doc {
    use myko::prelude::*;
    #[myko_item]
    pub struct RaceDoc {
        pub name: String,
    }
}
use doc::{RaceDoc, RaceDocId};

mod note {
    use myko::prelude::*;
    /// Not coordinated by the graph or by any relationship.
    #[myko_item]
    pub struct RaceNote {
        pub name: String,
    }
}
use note::RaceNote;

mod labeling {
    use myko::prelude::*;

    use super::{RaceDoc, RaceDocId, RaceLabel, RaceLabelId};
    #[myko_item]
    pub struct RaceLabeling {
        pub label_id: RaceLabelId,
        pub doc_id: RaceDocId,
    }

    #[myko_edge]
    impl GraphEdge for RaceLabeling {
        type Ends = Directed<ConcreteEndpoint<RaceLabel>, ConcreteEndpoint<RaceDoc>>;

        fn ends(&self) -> (RaceLabelId, RaceDocId) {
            (self.label_id.clone(), self.doc_id.clone())
        }

        const ADJACENCY: AdjacencyPolicy = AdjacencyPolicy::Eager;
    }
}
use labeling::RaceLabeling;

const ROUNDS: usize = 300;
const EDGES_PER_ROUND: usize = 4;

fn make_ctx() -> Arc<MykoServerContext> {
    Arc::new(MykoServerContext::new(
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
    ))
}

fn label(round: usize) -> RaceLabel {
    RaceLabel {
        id: format!("label-{round}").into(),
        name: "label".into(),
    }
}

fn doc(round: usize) -> RaceDoc {
    RaceDoc {
        id: format!("doc-{round}").into(),
        name: "doc".into(),
    }
}

fn other_doc(round: usize) -> RaceDoc {
    RaceDoc {
        id: format!("other-doc-{round}").into(),
        name: "doc".into(),
    }
}

/// The round's label tied to both of its docs, half the edges each.
fn split_edges(round: usize) -> Vec<RaceLabeling> {
    (0..EDGES_PER_ROUND)
        .map(|edge| RaceLabeling {
            id: format!("edge-{round}-{edge}").into(),
            label_id: label(round).id,
            doc_id: if edge % 2 == 0 {
                doc(round).id
            } else {
                other_doc(round).id
            },
        })
        .collect()
}

fn edges(round: usize) -> Vec<RaceLabeling> {
    (0..EDGES_PER_ROUND)
        .map(|edge| RaceLabeling {
            id: format!("edge-{round}-{edge}").into(),
            label_id: label(round).id,
            doc_id: doc(round).id,
        })
        .collect()
}

/// Whether a round starts with its edges already in place.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Start {
    Endpoints,
    EndpointsAndEdges,
    EndpointsAndSplitEdges,
}

/// Run `first` and `second` on two threads released together, once per
/// round, and fail instead of hanging if the two ever wait on each other.
fn race_rounds(
    ctx: &Arc<MykoServerContext>,
    start_with: Start,
    first: impl Fn(&MykoServerContext, usize) + Send + Sync + 'static,
    second: impl Fn(&MykoServerContext, usize) + Send + Sync + 'static,
) {
    let (done_tx, done_rx) = mpsc::channel();
    let ctx = ctx.clone();
    std::thread::spawn(move || {
        for round in 0..ROUNDS {
            ctx.set(&label(round)).expect("label");
            ctx.set(&doc(round)).expect("doc");
            match start_with {
                Start::Endpoints => {}
                Start::EndpointsAndEdges => ctx.batch_set(&edges(round)).expect("edges"),
                Start::EndpointsAndSplitEdges => {
                    ctx.set(&other_doc(round)).expect("other doc");
                    ctx.batch_set(&split_edges(round)).expect("edges");
                }
            }
            let start = Barrier::new(2);
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    start.wait();
                    first(&ctx, round);
                });
                scope.spawn(|| {
                    start.wait();
                    second(&ctx, round);
                });
            });
        }
        let _ = done_tx.send(());
    });
    done_rx
        .recv_timeout(Duration::from_mins(2))
        .expect("the two emitters finish every round");
    hyphae::flush();
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

/// Edges in the store whose label or doc is not.
fn dangling_edges(ctx: &MykoServerContext) -> Vec<String> {
    let labels = ids(ctx, "RaceLabel");
    let docs = ids(ctx, "RaceDoc");
    let mut dangling: Vec<String> =
        ctx.registry
            .get("RaceLabeling")
            .map_or_else(Vec::new, |store| {
                store
                    .snapshot()
                    .into_iter()
                    .filter_map(|(id, item)| {
                        let edge = item.as_any().downcast_ref::<RaceLabeling>()?;
                        let live = labels.contains(&edge.label_id.to_string())
                            && docs.contains(&edge.doc_id.to_string());
                        (!live).then(|| id.to_string())
                    })
                    .collect()
            });
    dangling.sort();
    dangling
}

/// The projection's sizes now, and after rebuilding it from the stores.
fn projection_and_rebuild(ctx: &MykoServerContext) -> ([usize; 3], [usize; 3]) {
    let graph = ctx.graph_index().expect("graph index");
    let sizes = || {
        let diagnostics = graph.diagnostics();
        [
            diagnostics.edge_count,
            diagnostics.adjacency_entries,
            diagnostics.pair_entries,
        ]
    };
    let projected = sizes();
    graph.rebuild().expect("rebuild the graph from the stores");
    (projected, sizes())
}

/// What a subscriber of one store was told about each id, in order:
/// `+` insert, `~` update, `-` remove.
struct Mirror {
    entity_type: &'static str,
    told: Arc<Mutex<BTreeMap<String, String>>>,
    _guard: SubscriptionGuard,
}

fn record(diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>, told: &mut BTreeMap<String, String>) {
    let mut tell = |id: &str, what: char| told.entry(id.to_owned()).or_default().push(what);
    match diff {
        // A snapshot replaces whatever the subscriber held.
        MapDiff::Initial { entries } => {
            let held: Vec<String> = told
                .iter()
                .filter(|(_, told)| !told.ends_with('-'))
                .map(|(id, _)| id.clone())
                .collect();
            for id in held {
                told.entry(id).or_default().push('-');
            }
            for (id, _) in entries {
                told.entry(id.to_string()).or_default().push('+');
            }
        }
        MapDiff::Insert { key, .. } => tell(key, '+'),
        MapDiff::Update { key, .. } => tell(key, '~'),
        MapDiff::Remove { key, .. } => tell(key, '-'),
        MapDiff::Batch { changes } => {
            for change in changes {
                record(change, told);
            }
        }
    }
}

fn mirror_edges(ctx: &MykoServerContext) -> Mirror {
    mirror(ctx, "RaceLabeling")
}

fn mirror(ctx: &MykoServerContext, entity_type: &'static str) -> Mirror {
    let told = Arc::new(Mutex::new(BTreeMap::new()));
    let recorded = told.clone();
    let guard = ctx
        .registry
        .get_or_create(entity_type)
        .subscribe_diffs(move |diff| {
            record(
                diff,
                &mut recorded.lock().unwrap_or_else(PoisonError::into_inner),
            );
        });
    Mirror {
        entity_type,
        told,
        _guard: guard,
    }
}

/// Ids for which replaying what the subscriber was told does not give what
/// the store holds, each with what it was told.
fn replay_disagreements(ctx: &MykoServerContext, mirror: &Mirror) -> Vec<String> {
    let stored = ids(ctx, mirror.entity_type);
    mirror
        .told
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|(id, told)| {
            let held = !told.ends_with('-');
            held != stored.contains(id)
        })
        .map(|(id, told)| format!("{id} told {told}, stored {}", stored.contains(id)))
        .collect()
}

fn assert_consistent(ctx: &MykoServerContext, mirror: &Mirror) {
    assert_eq!(
        replay_disagreements(ctx, mirror),
        Vec::<String>::new(),
        "replayed diffs against the store"
    );
    assert_eq!(dangling_edges(ctx), Vec::<String>::new());
    let (projected, rebuilt) = projection_and_rebuild(ctx);
    assert_eq!(projected, rebuilt, "projection against a rebuild");
    assert_eq!(projected[0], ids(ctx, "RaceLabeling").len());
}

#[test]
fn an_endpoint_delete_racing_edge_sets_leaves_no_edge_behind() {
    let ctx = make_ctx();
    let mirror = mirror_edges(&ctx);
    race_rounds(
        &ctx,
        Start::Endpoints,
        |ctx, round| ctx.del(&label(round)).expect("delete label"),
        |ctx, round| {
            for edge in edges(round) {
                // Refused once the label is gone.
                let _ = ctx.set(&edge);
            }
        },
    );

    assert_eq!(ids(&ctx, "RaceLabel"), Vec::<String>::new());
    assert_eq!(ids(&ctx, "RaceLabeling"), Vec::<String>::new());
    assert_eq!(ids(&ctx, "RaceDoc").len(), ROUNDS);
    assert_consistent(&ctx, &mirror);
}

#[test]
fn both_endpoints_deleted_at_once_leave_no_edge_behind() {
    let ctx = make_ctx();
    let mirror = mirror_edges(&ctx);
    race_rounds(
        &ctx,
        Start::EndpointsAndEdges,
        |ctx, round| {
            let _ = ctx.del(&label(round));
        },
        |ctx, round| {
            let _ = ctx.del(&doc(round));
        },
    );

    assert_eq!(ids(&ctx, "RaceLabel"), Vec::<String>::new());
    assert_eq!(ids(&ctx, "RaceDoc"), Vec::<String>::new());
    assert_eq!(ids(&ctx, "RaceLabeling"), Vec::<String>::new());
    assert_consistent(&ctx, &mirror);
}

#[test]
#[ignore = "myko lv-2afe: a cascade whose edge is already gone drops the rest of its batch"]
fn two_endpoint_deletes_that_share_some_edges_leave_no_edge_behind() {
    let ctx = make_ctx();
    let mirror = mirror_edges(&ctx);
    race_rounds(
        &ctx,
        Start::EndpointsAndSplitEdges,
        |ctx, round| {
            let _ = ctx.del(&label(round));
        },
        |ctx, round| {
            let _ = ctx.del(&doc(round));
        },
    );

    assert_eq!(ids(&ctx, "RaceLabel"), Vec::<String>::new());
    assert_eq!(ids(&ctx, "RaceDoc").len(), ROUNDS);
    assert_eq!(dangling_edges(&ctx), Vec::<String>::new());
    assert_consistent(&ctx, &mirror);
}

#[test]
#[ignore = "myko lv-2afe: a cascade whose edge is already gone fails the endpoint delete"]
fn an_endpoint_delete_reports_success_when_another_delete_took_its_edges() {
    let ctx = make_ctx();
    let refused = Arc::new(Mutex::new(Vec::new()));
    let (by_label, by_doc) = (refused.clone(), refused.clone());
    race_rounds(
        &ctx,
        Start::EndpointsAndEdges,
        move |ctx, round| {
            if let Err(error) = ctx.del(&label(round)) {
                by_label
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(error.to_string());
            }
        },
        move |ctx, round| {
            if let Err(error) = ctx.del(&doc(round)) {
                by_doc
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(error.to_string());
            }
        },
    );

    let mut refused = refused
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    refused.dedup();
    assert_eq!(refused, Vec::<String>::new());
}

#[test]
fn edge_batches_racing_edge_deletes_agree_with_the_projection() {
    let ctx = make_ctx();
    let mirror = mirror_edges(&ctx);
    race_rounds(
        &ctx,
        Start::EndpointsAndEdges,
        |ctx, round| {
            for _ in 0..3 {
                ctx.batch_set(&edges(round)).expect("edges");
            }
        },
        |ctx, round| {
            for edge in edges(round) {
                // Refused when the edge is not there to delete.
                let _ = ctx.del(&edge);
            }
        },
    );

    assert_eq!(ids(&ctx, "RaceLabel").len(), ROUNDS);
    assert_consistent(&ctx, &mirror);
}

/// One websocket reader's batch: a single event set that is not waited on.
fn ingest<T: Eventable>(ctx: &MykoServerContext, change: MEventType, items: &[T]) {
    let events = items
        .iter()
        .map(|item| MEvent::from_item(item, change, "race-test"))
        .collect();
    let _ = hyphae::submit(&[], None, || ctx.apply_event_batch(events));
}

// Each reader queues its event set when the set closes, which is after it
// released the authority. A store tells its subscribers what it holds when
// the set is applied, so the order the sets arrive in does not matter.
#[test]
fn wire_edge_batches_racing_wire_edge_deletes_agree_with_the_projection() {
    let ctx = make_ctx();
    let mirror = mirror_edges(&ctx);
    race_rounds(
        &ctx,
        Start::EndpointsAndEdges,
        |ctx, round| {
            for _ in 0..3 {
                ingest(ctx, MEventType::SET, &edges(round));
            }
        },
        |ctx, round| {
            for edge in edges(round) {
                ingest(ctx, MEventType::DEL, &[edge]);
            }
        },
    );

    assert_eq!(ids(&ctx, "RaceLabel").len(), ROUNDS);
    assert_consistent(&ctx, &mirror);
}

fn notes(round: usize) -> Vec<RaceNote> {
    (0..EDGES_PER_ROUND)
        .map(|note| RaceNote {
            id: format!("note-{round}-{note}").into(),
            name: "note".into(),
        })
        .collect()
}

// Nothing orders two writers of a type the graph does not coordinate. The
// store's diffs still follow its data (myko lv-fb7b).
#[test]
fn wire_batches_racing_wire_deletes_of_a_plain_type_agree_with_a_subscriber() {
    let ctx = make_ctx();
    let mirror = mirror(&ctx, "RaceNote");
    race_rounds(
        &ctx,
        Start::Endpoints,
        |ctx, round| {
            for _ in 0..3 {
                ingest(ctx, MEventType::SET, &notes(round));
            }
        },
        |ctx, round| {
            for note in notes(round) {
                ingest(ctx, MEventType::DEL, &[note]);
            }
        },
    );

    assert_eq!(
        replay_disagreements(&ctx, &mirror),
        Vec::<String>::new(),
        "replayed diffs against the store"
    );
}
