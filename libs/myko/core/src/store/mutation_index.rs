//! Per-entity mutation index — the in-memory backbone of "what changed since X".
//!
//! The event log is O(all history); reconstructing state from it on every diff is the
//! cost this avoids. The current state of every live entity is *already* in memory (the
//! stores). This index adds, per entity, only small mutation markers (and a tombstone for
//! deletes), updated at ingest. From it, "changes since an anchor timestamp" is classified
//! entirely in memory — no log reread — and the event log is touched only lazily, with
//! targeted queries, to fetch field-level before-values for the few entities that changed.
//!
//! Markers are RFC3339 `created_at` strings (the same clock `RestorePoint.at_timestamp`
//! uses), compared lexicographically — valid for fixed-offset RFC3339 timestamps.
//!
//! See `docs/superpowers/specs/2026-06-17-restore-points-design.md`.

use std::sync::Arc;

use dashmap::DashMap;
use hyphae::{Cell, CellImmutable, CellMutable, Gettable, Mutable};
use serde_json::Value;

use crate::wire::{MEvent, MEventType};

type Key = (Arc<str>, Arc<str>);

#[derive(Clone)]
struct LiveMarker {
    /// `created_at` of the entity's first SET.
    created_at: Arc<str>,
    /// `created_at` of the entity's most recent event.
    last_at: Arc<str>,
}

#[derive(Clone)]
struct Tombstone {
    /// `created_at` of the entity's first SET (before it was deleted).
    created_at: Arc<str>,
    /// `created_at` of the DEL event.
    deleted_at: Arc<str>,
    /// The entity's value at deletion — lets "removed" rows and their subtree membership
    /// be computed in memory without touching the log.
    last_value: Value,
}

/// How an entity changed relative to an anchor.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChangeKind {
    Added,
    Modified,
}

/// In-memory index of per-entity mutation markers + tombstones.
pub struct MutationIndex {
    live: DashMap<Key, LiveMarker, ahash::RandomState>,
    tombstones: DashMap<Key, Tombstone, ahash::RandomState>,
    /// Monotonic tick bumped when tracked entities change — the reactive "live state
    /// changed" signal that drives `EntityChangesSince`.
    tick: Cell<u64, CellMutable>,
}

impl Default for MutationIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl MutationIndex {
    pub fn new() -> Self {
        Self {
            live: DashMap::with_hasher(ahash::RandomState::new()),
            tombstones: DashMap::with_hasher(ahash::RandomState::new()),
            tick: Cell::new(0).with_name("mutation_index_tick"),
        }
    }

    /// Reactive tick — bump on every change to a tracked entity; subscribe to recompute.
    pub fn tick(&self) -> Cell<u64, CellImmutable> {
        self.tick.clone().lock()
    }

    /// Bump the change tick. Call once per applied batch of tracked mutations.
    pub fn bump(&self) {
        let next = self.tick.get().saturating_add(1);
        self.tick.set(next);
    }

    /// Record an ingested event. Safe to call from every ingest path. Types excluded from
    /// the tree (`exclude_from_tree`) are harmless to record — they just never surface,
    /// since the diff walks the tree, which already skips them.
    pub fn record_event(&self, event: &MEvent) {
        let Some(id) = event.item.get("id").and_then(|v| v.as_str()) else {
            return;
        };
        match event.change_type {
            MEventType::SET => self.record_set(&event.item_type, id, &event.created_at),
            MEventType::DEL => {
                self.record_del(&event.item_type, id, &event.created_at, event.item.clone())
            }
        }
    }

    /// Seed a live entity's markers directly from startup-bootstrap aggregates.
    ///
    /// Unlike [`record_event`](Self::record_event), which sees one event at a time, the
    /// bootstrap knows the entity's *first* SET (`created_at`) and *latest* event
    /// (`last_at`) up front — so it can install the correct markers in one shot. The
    /// incremental path can't reconstruct `created_at` at startup because the snapshot
    /// loads only the latest event per entity, not its full history.
    pub fn seed_live(&self, entity_type: &str, id: &str, created_at: &str, last_at: &str) {
        let key: Key = (entity_type.into(), id.into());
        self.live.insert(
            key,
            LiveMarker {
                created_at: created_at.into(),
                last_at: last_at.into(),
            },
        );
    }

    /// Seed a deleted entity's tombstone directly from startup-bootstrap aggregates.
    ///
    /// `created_at` is the entity's first SET (so it can be tested against a restore-point
    /// anchor); `deleted_at` is its DEL. Without this, a pre-restart deletion would carry
    /// `created_at == deleted_at` and be wrongly excluded from "removed since" diffs.
    pub fn seed_tombstone(
        &self,
        entity_type: &str,
        id: &str,
        created_at: &str,
        deleted_at: &str,
        last_value: Value,
    ) {
        let key: Key = (entity_type.into(), id.into());
        self.tombstones.insert(
            key,
            Tombstone {
                created_at: created_at.into(),
                deleted_at: deleted_at.into(),
                last_value,
            },
        );
    }

    fn record_set(&self, entity_type: &str, id: &str, created_at: &str) {
        let key: Key = (entity_type.into(), id.into());
        // A SET resurrects: drop any tombstone, carrying its original created_at forward.
        let prior_created = self.tombstones.remove(&key).map(|(_, t)| t.created_at);
        self.live
            .entry(key)
            .and_modify(|m| m.last_at = created_at.into())
            .or_insert_with(|| LiveMarker {
                created_at: prior_created.unwrap_or_else(|| created_at.into()),
                last_at: created_at.into(),
            });
    }

    fn record_del(&self, entity_type: &str, id: &str, deleted_at: &str, last_value: Value) {
        let key: Key = (entity_type.into(), id.into());
        let created_at = self
            .live
            .remove(&key)
            .map(|(_, m)| m.created_at)
            .unwrap_or_else(|| deleted_at.into());
        self.tombstones.insert(
            key,
            Tombstone {
                created_at,
                deleted_at: deleted_at.into(),
                last_value,
            },
        );
    }

    /// Classify a currently-live entity relative to `anchor`. `None` if unchanged since.
    pub fn live_change(&self, entity_type: &str, id: &str, anchor: &str) -> Option<ChangeKind> {
        let key: Key = (entity_type.into(), id.into());
        let m = self.live.get(&key)?;
        if m.last_at.as_ref() <= anchor {
            return None; // untouched since the anchor
        }
        Some(if m.created_at.as_ref() > anchor {
            ChangeKind::Added
        } else {
            ChangeKind::Modified
        })
    }

    /// All entities that existed at `anchor` but have since been deleted, with the value
    /// they held at deletion. Caller filters to the relevant subtree.
    pub fn removed_since(&self, anchor: &str) -> Vec<(Arc<str>, Arc<str>, Value)> {
        self.tombstones
            .iter()
            .filter(|e| e.deleted_at.as_ref() > anchor && e.created_at.as_ref() <= anchor)
            .map(|e| (e.key().0.clone(), e.key().1.clone(), e.value().last_value.clone()))
            .collect()
    }

    /// Drop tombstones whose deletion predates `anchor` — they can never appear in a diff
    /// against any restore point at or after `anchor`. Call with the oldest live anchor.
    pub fn prune_tombstones_before(&self, anchor: &str) {
        self.tombstones
            .retain(|_, t| t.deleted_at.as_ref() >= anchor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set(idx: &MutationIndex, ty: &str, id: &str, at: &str) {
        idx.record_event(&MEvent {
            item: json!({ "id": id }),
            change_type: MEventType::SET,
            item_type: ty.to_string(),
            created_at: at.to_string(),
            tx: "t".to_string(),
            source_id: None,
            options: None,
        });
    }

    fn del(idx: &MutationIndex, ty: &str, at: &str, value: Value) {
        idx.record_event(&MEvent {
            item: value,
            change_type: MEventType::DEL,
            item_type: ty.to_string(),
            created_at: at.to_string(),
            tx: "t".to_string(),
            source_id: None,
            options: None,
        });
    }

    #[test]
    fn classifies_added_vs_modified_vs_unchanged() {
        let idx = MutationIndex::new();
        let anchor = "2026-06-17T12:00:00+00:00";

        // created before anchor, untouched since -> unchanged (None)
        set(&idx, "Scene", "old", "2026-06-17T10:00:00+00:00");
        assert_eq!(idx.live_change("Scene", "old", anchor), None);

        // created before anchor, modified after -> Modified
        set(&idx, "Scene", "old", "2026-06-17T13:00:00+00:00");
        assert_eq!(idx.live_change("Scene", "old", anchor), Some(ChangeKind::Modified));

        // created after anchor -> Added
        set(&idx, "Scene", "new", "2026-06-17T13:30:00+00:00");
        assert_eq!(idx.live_change("Scene", "new", anchor), Some(ChangeKind::Added));
    }

    #[test]
    fn deletion_after_anchor_is_removed_and_carries_value() {
        let idx = MutationIndex::new();
        let anchor = "2026-06-17T12:00:00+00:00";

        set(&idx, "Cue", "c1", "2026-06-17T10:00:00+00:00");
        del(&idx, "Cue", "2026-06-17T13:00:00+00:00", json!({ "id": "c1", "name": "x" }));

        // no longer a live change
        assert_eq!(idx.live_change("Cue", "c1", anchor), None);
        // surfaces as removed, with its last value
        let removed = idx.removed_since(anchor);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0.as_ref(), "Cue");
        assert_eq!(removed[0].1.as_ref(), "c1");
        assert_eq!(removed[0].2.get("name").and_then(|v| v.as_str()), Some("x"));
    }

    #[test]
    fn deletion_before_anchor_is_not_removed() {
        let idx = MutationIndex::new();
        set(&idx, "Cue", "c1", "2026-06-17T09:00:00+00:00");
        del(&idx, "Cue", "2026-06-17T10:00:00+00:00", json!({ "id": "c1" }));
        // anchor is AFTER the deletion -> not part of "changes since anchor"
        assert!(idx.removed_since("2026-06-17T12:00:00+00:00").is_empty());
    }

    #[test]
    fn resurrect_clears_tombstone_and_preserves_original_created() {
        let idx = MutationIndex::new();
        let anchor = "2026-06-17T12:00:00+00:00";

        set(&idx, "Cue", "c1", "2026-06-17T09:00:00+00:00");
        del(&idx, "Cue", "2026-06-17T13:00:00+00:00", json!({ "id": "c1" }));
        // re-created after the delete
        set(&idx, "Cue", "c1", "2026-06-17T14:00:00+00:00");

        assert!(idx.removed_since(anchor).is_empty(), "tombstone cleared on resurrect");
        // original created (pre-anchor) preserved -> Modified, not Added
        assert_eq!(idx.live_change("Cue", "c1", anchor), Some(ChangeKind::Modified));
    }

    /// Startup-bootstrap regression: a deletion that happened *before* a restart must still
    /// surface as "removed" afterward. The index is reseeded from aggregates, so a seeded
    /// tombstone carries the entity's first-set `created_at` (<= anchor) and its `deleted_at`
    /// (> anchor) — not `created_at == deleted_at`, which would wrongly exclude it.
    #[test]
    fn seeded_tombstone_from_bootstrap_surfaces_as_removed() {
        let idx = MutationIndex::new();
        let anchor = "2026-06-17T12:00:00+00:00";

        // Reconstructed at startup: created before the restore point, deleted after it.
        idx.seed_tombstone(
            "Cue",
            "c1",
            "2026-06-17T10:00:00+00:00", // first SET (<= anchor)
            "2026-06-17T13:00:00+00:00", // DEL (> anchor)
            json!({ "id": "c1", "name": "x" }),
        );

        let removed = idx.removed_since(anchor);
        assert_eq!(removed.len(), 1, "pre-restart deletion must survive the reseed");
        assert_eq!(removed[0].1.as_ref(), "c1");
        assert_eq!(removed[0].2.get("name").and_then(|v| v.as_str()), Some("x"));
    }

    /// The naive bootstrap bug this fixes: had the tombstone been seeded with
    /// `created_at == deleted_at` (only the latest DEL event, no first-set lookup), the
    /// `created_at <= anchor` test fails and the deletion silently vanishes from the diff.
    #[test]
    fn seeded_tombstone_with_collapsed_created_at_is_the_old_bug() {
        let idx = MutationIndex::new();
        let anchor = "2026-06-17T12:00:00+00:00";
        let deleted_at = "2026-06-17T13:00:00+00:00";

        idx.seed_tombstone("Cue", "c1", deleted_at, deleted_at, json!({ "id": "c1" }));

        assert!(
            idx.removed_since(anchor).is_empty(),
            "collapsed created_at == deleted_at is exactly what dropped pre-restart deletions"
        );
    }

    /// Seeded live markers preserve first-set vs latest-event, so Added/Modified classifies
    /// correctly after a restart (the snapshot otherwise collapses both to the latest event).
    #[test]
    fn seeded_live_marker_classifies_modified_after_restart() {
        let idx = MutationIndex::new();
        let anchor = "2026-06-17T12:00:00+00:00";

        // Created before the anchor, last modified after it.
        idx.seed_live("Scene", "s1", "2026-06-17T09:00:00+00:00", "2026-06-17T13:00:00+00:00");
        assert_eq!(idx.live_change("Scene", "s1", anchor), Some(ChangeKind::Modified));

        // Created entirely after the anchor.
        idx.seed_live("Scene", "s2", "2026-06-17T13:00:00+00:00", "2026-06-17T13:00:00+00:00");
        assert_eq!(idx.live_change("Scene", "s2", anchor), Some(ChangeKind::Added));
    }

    #[test]
    fn prune_drops_old_tombstones() {
        let idx = MutationIndex::new();
        set(&idx, "Cue", "c1", "2026-06-17T08:00:00+00:00");
        del(&idx, "Cue", "2026-06-17T09:00:00+00:00", json!({ "id": "c1" }));
        idx.prune_tombstones_before("2026-06-17T12:00:00+00:00");
        assert!(idx.removed_since("2026-06-17T07:00:00+00:00").is_empty());
    }
}
