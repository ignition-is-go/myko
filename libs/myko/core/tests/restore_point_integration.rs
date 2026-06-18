//! End-to-end integration tests for restore points.
//!
//! These drive the real `RestoreToPoint` command against a live in-memory server
//! context — the entity store, the relationship adjacency (`walk_tree`), the FK-closure
//! scoping of deleted-since entities, and the converge → `apply_event_batch` apply path.
//! History is supplied by a mock `HistoryReplayProvider`, so the tests are hermetic (no
//! Postgres) yet exercise the same code the Postgres-backed provider feeds.
//!
//! Fixture entities (`BenchProject` ← `BenchScene` ← `BenchCue`, a 3-level `belongs_to`
//! chain) live in `bench_entities`, so these tests require `--features bench`.

#![cfg(feature = "bench")]

use std::sync::Arc;

use myko::{
    bench_entities::{BenchCue, BenchProject, BenchScene},
    entities::restore_point::{RestorePoint, RestorePointId, RestoreToPoint},
    prelude::*,
    search::SearchIndex,
    server::{
        CellServerCtx, HandlerRegistry, HistoryReplayProvider, RelationshipManager, RestoreChange,
        persister::PersisterRouter,
    },
    store::StoreRegistry,
};
use uuid::Uuid;

/// A `HistoryReplayProvider` that returns a fixed `restore_changes_since` result — the
/// "what changed since the restore point, and its value then" set the Postgres provider
/// would compute from the durable log.
struct MockHistory {
    changes: Vec<RestoreChange>,
}

impl HistoryReplayProvider for MockHistory {
    fn restore_changes_since(
        &self,
        _until: &str,
        _handler_registry: &HandlerRegistry,
    ) -> Result<Vec<RestoreChange>, String> {
        Ok(self.changes.clone())
    }
}

fn make_ctx(history: MockHistory) -> Arc<CellServerCtx> {
    Arc::new(CellServerCtx::new(
        Uuid::new_v4(),
        Arc::new(StoreRegistry::new()),
        Arc::new(HandlerRegistry::new()),
        Arc::new(RelationshipManager::new()),
        Arc::new(PersisterRouter::default()),
        Arc::new(SearchIndex::new()),
        Arc::new(dashmap::DashMap::new()),
        None,
        Some(Arc::new(history)),
    ))
}

/// Seed `item` into the live store as a SET (current state).
fn seed<T>(ctx: &CellServerCtx, item: &T)
where
    T: Eventable + serde::Serialize + Clone + 'static,
{
    let event = MEvent::from_item(item, MEventType::SET, "tx-seed");
    ctx.apply_event_batch(vec![event]).unwrap();
}

fn seed_restore_point(ctx: &CellServerCtx, id: &str, root_id: &str, at: &str) -> RestorePointId {
    let rp = RestorePoint {
        id: id.into(),
        name: "before edits".to_string(),
        description: None,
        root_type: Arc::from("BenchProject"),
        root_id: Arc::from(root_id),
        at_timestamp: at.to_string(),
    };
    seed(ctx, &rp);
    id.into()
}

fn exists(ctx: &CellServerCtx, entity_type: &str, id: &str) -> bool {
    ctx.registry
        .get(entity_type)
        .is_some_and(|store| store.contains_key(&Arc::<str>::from(id)))
}

fn name_of(ctx: &CellServerCtx, entity_type: &str, id: &str) -> Option<String> {
    let store = ctx.registry.get(entity_type)?;
    let item = store.get_value(&Arc::<str>::from(id))?;
    item.to_value()
        .get("name")
        .and_then(|v| v.as_str())
        .map(String::from)
}

fn run_restore(ctx: &Arc<CellServerCtx>, id: RestorePointId) -> usize {
    let req = ctx.new_server_transaction();
    let cmd_ctx = CommandContext::new(Arc::from("RestoreToPoint"), req, ctx.clone());
    RestoreToPoint { id }.execute(cmd_ctx).expect("restore failed")
}

/// The canonical user scenario: make A (restore point), then modify a scene, delete a cue,
/// and create a new scene+cue. Restoring to A must revert the modification, resurrect the
/// deleted cue, and remove everything created since — in one forward transaction.
#[test]
fn restore_reverts_resurrects_and_removes() {
    // Value of each changed entity AT the restore point.
    let s1_then = BenchScene {
        id: "s1".into(),
        project_id: "p1".into(),
        name: "original".into(),
    };
    let c1_then = BenchCue {
        id: "c1".into(),
        scene_id: "s1".into(),
        name: "orig-cue".into(),
    };

    let ctx = make_ctx(MockHistory {
        changes: vec![
            // s1 was modified after the point → revert to "original".
            ("BenchScene".into(), "s1".into(), Some(s1_then.to_value())),
            // c1 was deleted after the point → resurrect (its parent s1 is also restored).
            ("BenchCue".into(), "c1".into(), Some(c1_then.to_value())),
            // s2 + c2 were created after the point → remove (value at the point: absent).
            ("BenchScene".into(), "s2".into(), None),
            ("BenchCue".into(), "c2".into(), None),
        ],
    });

    // Current live state: p1 (unchanged), s1 (renamed), s2 + c2 (new). c1 is gone.
    seed(&ctx, &BenchProject { id: "p1".into(), name: "proj".into() });
    seed(
        &ctx,
        &BenchScene { id: "s1".into(), project_id: "p1".into(), name: "changed".into() },
    );
    seed(
        &ctx,
        &BenchScene { id: "s2".into(), project_id: "p1".into(), name: "new-scene".into() },
    );
    seed(
        &ctx,
        &BenchCue { id: "c2".into(), scene_id: "s2".into(), name: "new-cue".into() },
    );

    let rp = seed_restore_point(&ctx, "rp1", "p1", "2026-06-17T00:00:00Z");

    let applied = run_restore(&ctx, rp);

    // The bug was "0 changes applied" — assert we actually did work.
    assert!(applied > 0, "restore should apply at least one event, got {applied}");

    // Modification reverted.
    assert_eq!(name_of(&ctx, "BenchScene", "s1").as_deref(), Some("original"));
    // Deleted cue resurrected with its captured value.
    assert!(exists(&ctx, "BenchCue", "c1"), "c1 should be resurrected");
    assert_eq!(name_of(&ctx, "BenchCue", "c1").as_deref(), Some("orig-cue"));
    // Created-since entities removed.
    assert!(!exists(&ctx, "BenchScene", "s2"), "s2 created since the point should be removed");
    assert!(!exists(&ctx, "BenchCue", "c2"), "c2 created since the point should be removed");
    // Untouched root survives.
    assert!(exists(&ctx, "BenchProject", "p1"), "p1 was never changed and must remain");
}

/// Restoring to a point with no changes since is a clean no-op: the converge plan is empty
/// and live state is untouched.
#[test]
fn restore_with_no_changes_is_noop() {
    let ctx = make_ctx(MockHistory { changes: vec![] });

    seed(&ctx, &BenchProject { id: "p1".into(), name: "proj".into() });
    seed(
        &ctx,
        &BenchScene { id: "s1".into(), project_id: "p1".into(), name: "scene".into() },
    );

    let rp = seed_restore_point(&ctx, "rp1", "p1", "2026-06-17T00:00:00Z");
    let applied = run_restore(&ctx, rp);

    assert_eq!(applied, 0, "no changes since → nothing to apply");
    assert!(exists(&ctx, "BenchScene", "s1"));
    assert_eq!(name_of(&ctx, "BenchScene", "s1").as_deref(), Some("scene"));
}

/// A deleted child whose parent was *also* deleted must still be scoped into the subtree
/// via the FK closure (the parent is no longer in the live tree to anchor it directly).
#[test]
fn restore_resurrects_deleted_child_of_deleted_parent() {
    let s1_then = BenchScene {
        id: "s1".into(),
        project_id: "p1".into(),
        name: "scene".into(),
    };
    let c1_then = BenchCue {
        id: "c1".into(),
        scene_id: "s1".into(),
        name: "cue".into(),
    };

    let ctx = make_ctx(MockHistory {
        changes: vec![
            // Both the scene and its cue were deleted after the restore point.
            ("BenchScene".into(), "s1".into(), Some(s1_then.to_value())),
            ("BenchCue".into(), "c1".into(), Some(c1_then.to_value())),
        ],
    });

    // Current live state: only the project remains; the whole scene subtree was deleted.
    seed(&ctx, &BenchProject { id: "p1".into(), name: "proj".into() });

    let rp = seed_restore_point(&ctx, "rp1", "p1", "2026-06-17T00:00:00Z");
    let applied = run_restore(&ctx, rp);

    assert!(applied >= 2, "both s1 and c1 should be resurrected, applied={applied}");
    assert!(exists(&ctx, "BenchScene", "s1"), "deleted scene resurrected");
    assert!(
        exists(&ctx, "BenchCue", "c1"),
        "deleted cue (child of deleted parent) resurrected via FK closure"
    );
}
