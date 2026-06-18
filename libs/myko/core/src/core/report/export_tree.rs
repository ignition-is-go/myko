//! Entity tree export report.
//!
//! BFS walks the relationship graph from a root entity, collecting all
//! descendant entities into a flat `EntityTreeExport` structure.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use chrono::Utc;
use hyphae::{Cell, DedupedExt, MaterializeDefinite, SwitchMapExt};
use myko_macros::{myko_report, myko_report_output};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    TS,
    common::to_value::ToValue,
    relationship::{Relation, iter_relations},
    store::{ChangeKind, StoreRegistry},
};

// ─────────────────────────────────────────────────────────────────────────────
// Output types
// ─────────────────────────────────────────────────────────────────────────────

/// A single exported entity within the tree.
#[myko_report_output]
pub struct ExportedEntity {
    /// The entity type name (e.g., "Scene", "Binding").
    pub entity_type: Arc<str>,
    /// The full serialized entity data.
    pub data: Value,
}

/// The complete tree export containing all entities reachable from the root.
#[myko_report_output]
pub struct EntityTreeExport {
    /// Export format version.
    pub version: u32,
    /// Entity type of the root (e.g., "Project").
    pub root_type: Arc<str>,
    /// ID of the root entity.
    pub root_id: Arc<str>,
    /// ISO 8601 timestamp of when the export was created.
    pub exported_at: String,
    /// All entities in the tree, flattened.
    pub entities: Vec<ExportedEntity>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Report definition
// ─────────────────────────────────────────────────────────────────────────────

/// Export the full entity tree rooted at a given entity.
///
/// Performs a BFS walk over the relationship graph, collecting every
/// reachable descendant. Respects `exclude_from_tree` on `BelongsTo`
/// relations and follows `EnsureFor` on a single-axis basis to prevent
/// Cartesian explosion.
#[myko_report(EntityTreeExport)]
pub struct ExportEntityTree {
    /// Entity type of the root (e.g., "Project").
    pub root_type: Arc<str>,
    /// ID of the root entity.
    pub root_id: Arc<str>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Adjacency map
// ─────────────────────────────────────────────────────────────────────────────

/// Describes how to find children of a given parent entity type.
pub struct ChildRelation {
    /// The child entity type.
    pub child_type: &'static str,
    /// How to discover child IDs from the parent.
    pub kind: ChildKind,
}

pub enum ChildKind {
    /// BelongsTo: scan the child store for entities whose FK matches the parent ID.
    BelongsTo {
        extract_fk: crate::relationship::FkExtractor,
    },
    /// OwnsMany: extract child IDs directly from the parent entity.
    OwnsMany {
        extract_ids: crate::relationship::ArrayExtractor,
    },
    /// EnsureFor: scan the local (ensured) store for entities whose FK matches the parent ID.
    EnsureFor {
        extract_fk: crate::relationship::FkExtractor,
    },
}

/// Build a map from parent entity type -> list of child relations.
///
/// This processes all registered relationships once and inverts them into
/// a lookup table suitable for BFS traversal.
pub fn build_adjacency_map() -> HashMap<&'static str, Vec<ChildRelation>> {
    let mut map: HashMap<&'static str, Vec<ChildRelation>> = HashMap::new();

    for reg in iter_relations() {
        match &reg.relation {
            Relation::BelongsTo {
                local_type,
                foreign_type,
                extract_fk,
                exclude_from_tree,
                ..
            } => {
                if *exclude_from_tree {
                    continue;
                }
                // Parent is foreign_type, child is local_type.
                map.entry(foreign_type).or_default().push(ChildRelation {
                    child_type: local_type,
                    kind: ChildKind::BelongsTo {
                        extract_fk: *extract_fk,
                    },
                });
            }
            Relation::OwnsMany {
                local_type,
                foreign_type,
                extract_ids,
                exclude_from_tree,
                ..
            } => {
                if *exclude_from_tree {
                    continue;
                }
                // Parent is local_type, child is foreign_type.
                map.entry(local_type).or_default().push(ChildRelation {
                    child_type: foreign_type,
                    kind: ChildKind::OwnsMany {
                        extract_ids: *extract_ids,
                    },
                });
            }
            Relation::EnsureFor {
                local_type,
                dependencies,
                exclude_from_tree,
                ..
            } => {
                if *exclude_from_tree {
                    continue;
                }
                // For each dependency, the dependency's foreign_type is a parent
                // that can reach local_type children (single-axis to avoid Cartesian).
                for dep in *dependencies {
                    map.entry(dep.foreign_type)
                        .or_default()
                        .push(ChildRelation {
                            child_type: local_type,
                            kind: ChildKind::EnsureFor {
                                extract_fk: dep.extract_fk,
                            },
                        });
                }
            }
        }
    }

    map
}

// ─────────────────────────────────────────────────────────────────────────────
// BFS walk
// ─────────────────────────────────────────────────────────────────────────────

/// Walk the entity tree via BFS starting from `(root_type, root_id)`.
///
/// Returns all reachable entities (including the root) as `ExportedEntity` values.
pub fn walk_tree(
    root_type: &str,
    root_id: &str,
    registry: &StoreRegistry,
    adjacency: &HashMap<&'static str, Vec<ChildRelation>>,
) -> Vec<ExportedEntity> {
    let mut result = Vec::new();
    let mut visited: HashSet<(Arc<str>, Arc<str>)> = HashSet::new();
    let mut queue: VecDeque<(Arc<str>, Arc<str>)> = VecDeque::new();

    let root_type: Arc<str> = root_type.into();
    let root_id: Arc<str> = root_id.into();

    queue.push_back((root_type.clone(), root_id.clone()));
    visited.insert((root_type, root_id));

    while let Some((entity_type, entity_id)) = queue.pop_front() {
        // Fetch entity from store
        let Some(store) = registry.get(&entity_type) else {
            continue;
        };
        let Some(entity) = store.get_value(&entity_id) else {
            continue;
        };

        // Serialize entity
        result.push(ExportedEntity {
            entity_type: entity_type.clone(),
            data: entity.to_value(),
        });

        // Find children via adjacency map
        let Some(children) = adjacency.get(entity_type.as_ref()) else {
            continue;
        };

        for child_rel in children {
            match &child_rel.kind {
                ChildKind::BelongsTo { extract_fk } => {
                    // Scan child store for entities whose FK matches this entity's ID
                    let Some(child_store) = registry.get(child_rel.child_type) else {
                        continue;
                    };
                    for (child_id, child_item) in child_store.snapshot() {
                        if let Some(fk) = extract_fk(child_item.as_any())
                            && fk == entity_id
                        {
                            let key = (Arc::<str>::from(child_rel.child_type), child_id);
                            if visited.insert(key.clone()) {
                                queue.push_back(key);
                            }
                        }
                    }
                }
                ChildKind::OwnsMany { extract_ids } => {
                    // Extract child IDs directly from the parent entity
                    if let Some(ids) = extract_ids(entity.as_any()) {
                        for child_id in ids {
                            let key = (Arc::<str>::from(child_rel.child_type), child_id);
                            if visited.insert(key.clone()) {
                                queue.push_back(key);
                            }
                        }
                    }
                }
                ChildKind::EnsureFor { extract_fk } => {
                    // Scan the ensured entity store for entities whose FK matches this entity's ID
                    let Some(ensured_store) = registry.get(child_rel.child_type) else {
                        continue;
                    };
                    for (ensured_id, ensured_item) in ensured_store.snapshot() {
                        if let Some(fk) = extract_fk(ensured_item.as_any())
                            && fk == entity_id
                        {
                            let key = (Arc::<str>::from(child_rel.child_type), ensured_id);
                            if visited.insert(key.clone()) {
                                queue.push_back(key);
                            }
                        }
                    }
                }
            }
        }
    }

    result
}

// ─────────────────────────────────────────────────────────────────────────────
// ReportHandler impl
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
impl crate::report::ReportHandler for ExportEntityTree {
    type Output = EntityTreeExport;

    fn compute(
        &self,
        ctx: crate::report::ReportContext,
    ) -> impl MaterializeDefinite<Arc<Self::Output>> {
        let registry = ctx.registry();

        eprintln!(
            "[ExportEntityTree] registry has {} entity types, walking root_type={} root_id={}",
            registry.entity_types().len(),
            self.root_type,
            self.root_id,
        );
        let adjacency = build_adjacency_map();
        let entities = walk_tree(&self.root_type, &self.root_id, &registry, &adjacency);
        eprintln!(
            "[ExportEntityTree] walk_tree found {} entities",
            entities.len()
        );

        Cell::new(Arc::new(EntityTreeExport {
            version: 1,
            root_type: self.root_type.clone(),
            root_id: self.root_id.clone(),
            exported_at: Utc::now().to_rfc3339(),
            entities,
        }))
        .lock()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityChangesSince — "what changed since a restore point"
// ─────────────────────────────────────────────────────────────────────────────
//
// Reconstructs the subtree as of `since_timestamp` (history replay) and diffs it
// against the current subtree, emitting per-entity Added/Removed/Modified changes.
// Entities deleted since the timestamp surface as `Removed`; entities alive at the
// timestamp but since deleted are present in the reconstruction, so the diff is
// computed against the true past state. See
// `docs/superpowers/specs/2026-06-17-restore-points-design.md`.

/// A single changed field on a modified entity.
#[myko_report_output]
pub struct FieldChange {
    pub field: String,
    /// Value at the restore point (None if the field was absent then).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub before: Option<Value>,
    /// Current value (None if the field is absent now).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub after: Option<Value>,
}

/// One entity's change relative to the restore point.
/// How an entity changed relative to the restore point. Serializes to
/// `"added"` / `"removed"` / `"modified"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
#[ts(export)]
pub enum ChangeStatus {
    Added,
    Removed,
    Modified,
}

/// One entity's change relative to the restore point. `fields` is populated only when
/// `status` is `Modified`.
#[myko_report_output]
pub struct EntityChange {
    pub entity_type: Arc<str>,
    pub id: Arc<str>,
    pub status: ChangeStatus,
    #[serde(default)]
    pub fields: Vec<FieldChange>,
}

/// The full changeset for a subtree relative to a past moment.
#[myko_report_output]
pub struct EntityChanges {
    pub root_type: Arc<str>,
    pub root_id: Arc<str>,
    pub since_timestamp: String,
    pub changes: Vec<EntityChange>,
}

/// Report: the changes to the `(root_type, root_id)` subtree since `since_timestamp`.
///
/// Computed entirely in memory from the store's mutation index — no event-log reread, no
/// historical replay. Field-level before/after values (for `modified`) are fetched lazily
/// and separately via a targeted query; this report returns the classified change set
/// (added / modified / removed) and is cheap enough to run on the reactive path.
#[myko_report(EntityChanges)]
pub struct EntityChangesSince {
    pub root_type: Arc<str>,
    pub root_id: Arc<str>,
    /// RFC3339 timestamp (e.g. a restore point's `at_timestamp`).
    pub since_timestamp: String,
}

/// Stable `(entity_type, id)` key for a flattened entity.
fn entity_key(e: &ExportedEntity) -> (Arc<str>, Arc<str>) {
    let id = e
        .data
        .get("id")
        .and_then(|v| v.as_str())
        .map(Arc::<str>::from)
        .unwrap_or_else(|| Arc::from(""));
    (e.entity_type.clone(), id)
}

/// Field-level diff between two entity JSON objects.
fn json_field_changes(before: &Value, after: &Value) -> Vec<FieldChange> {
    let mut changes = Vec::new();
    let empty = serde_json::Map::new();
    let b = before.as_object().unwrap_or(&empty);
    let a = after.as_object().unwrap_or(&empty);

    let mut keys: Vec<&String> = b.keys().chain(a.keys()).collect();
    keys.sort();
    keys.dedup();

    for k in keys {
        // The hash field is derived from content; skip it as diff noise.
        if k == "hash" {
            continue;
        }
        let bv = b.get(k);
        let av = a.get(k);
        if bv != av {
            changes.push(FieldChange {
                field: k.clone(),
                before: bv.cloned(),
                after: av.cloned(),
            });
        }
    }
    changes
}

/// Diff two flattened entity lists into per-entity changes.
pub fn diff_exported(before: &[ExportedEntity], after: &[ExportedEntity]) -> Vec<EntityChange> {
    let before_map: HashMap<(Arc<str>, Arc<str>), &Value> =
        before.iter().map(|e| (entity_key(e), &e.data)).collect();
    let after_map: HashMap<(Arc<str>, Arc<str>), &Value> =
        after.iter().map(|e| (entity_key(e), &e.data)).collect();

    let mut changes = Vec::new();

    // Added / Modified — iterate `after` to preserve BFS (parent-first) order.
    for e in after {
        let key = entity_key(e);
        match before_map.get(&key) {
            None => changes.push(EntityChange {
                entity_type: key.0.clone(),
                id: key.1.clone(),
                status: ChangeStatus::Added,
                fields: Vec::new(),
            }),
            Some(before_data) => {
                let fields = json_field_changes(before_data, &e.data);
                if !fields.is_empty() {
                    changes.push(EntityChange {
                        entity_type: key.0.clone(),
                        id: key.1.clone(),
                        status: ChangeStatus::Modified,
                        fields,
                    });
                }
            }
        }
    }

    // Removed — present at the restore point, absent now.
    for e in before {
        let key = entity_key(e);
        if !after_map.contains_key(&key) {
            changes.push(EntityChange {
                entity_type: key.0.clone(),
                id: key.1.clone(),
                status: ChangeStatus::Removed,
                fields: Vec::new(),
            });
        }
    }

    changes
}

/// Build the change set for a subtree from the in-memory mutation index. Pure, in-memory:
/// no event-log reread, no historical replay.
#[cfg(not(target_arch = "wasm32"))]
fn compute_entity_changes(
    registry: &StoreRegistry,
    adjacency: &HashMap<&'static str, Vec<ChildRelation>>,
    root_type: &str,
    root_id: &str,
    anchor: &str,
) -> Vec<EntityChange> {
    let index = registry.mutation_index();

    // Current subtree (live, in-memory).
    let current = walk_tree(root_type, root_id, registry, adjacency);
    let subtree: HashSet<(Arc<str>, Arc<str>)> = current.iter().map(entity_key).collect();

    let mut changes = Vec::new();

    // Added / modified — classify each live subtree entity via the index.
    for e in &current {
        let (entity_type, id) = entity_key(e);
        if let Some(kind) = index.live_change(&entity_type, &id, anchor) {
            let status = match kind {
                ChangeKind::Added => ChangeStatus::Added,
                ChangeKind::Modified => ChangeStatus::Modified,
            };
            changes.push(EntityChange {
                entity_type,
                id,
                status,
                fields: Vec::new(),
            });
        }
    }

    // Removed — tombstones that belonged to the subtree at the anchor. A deleted entity's
    // ancestry isn't in the live tree, so membership is resolved transitively from its
    // stored value's FK fields (its parent may itself be deleted).
    let removed = index.removed_since(anchor);
    if !removed.is_empty() {
        // local_type -> [(fk_field_json, foreign_type)]
        let mut fk_fields: HashMap<&'static str, Vec<(&'static str, &'static str)>> = HashMap::new();
        for reg in iter_relations() {
            if let Relation::BelongsTo {
                local_type,
                foreign_type,
                fk_field_json,
                ..
            } = &reg.relation
            {
                fk_fields
                    .entry(local_type)
                    .or_default()
                    .push((fk_field_json, foreign_type));
            }
        }

        let mut included = subtree;
        let mut pending = removed;
        loop {
            let mut progressed = false;
            let mut still_pending = Vec::with_capacity(pending.len());
            for (entity_type, id, value) in pending {
                let in_subtree = fk_fields
                    .get(entity_type.as_ref())
                    .map(|fields| {
                        fields.iter().any(|(field, foreign_type)| {
                            value.get(field).and_then(|v| v.as_str()).is_some_and(|pid| {
                                included.contains(&(
                                    Arc::<str>::from(*foreign_type),
                                    Arc::<str>::from(pid),
                                ))
                            })
                        })
                    })
                    .unwrap_or(false);
                if in_subtree {
                    included.insert((entity_type.clone(), id.clone()));
                    changes.push(EntityChange {
                        entity_type,
                        id,
                        status: ChangeStatus::Removed,
                        fields: Vec::new(),
                    });
                    progressed = true;
                } else {
                    still_pending.push((entity_type, id, value));
                }
            }
            pending = still_pending;
            if !progressed || pending.is_empty() {
                break;
            }
        }
    }

    changes
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::report::ReportHandler for EntityChangesSince {
    type Output = EntityChanges;

    fn compute(
        &self,
        ctx: crate::report::ReportContext,
    ) -> impl MaterializeDefinite<Arc<Self::Output>> {
        let registry = ctx.registry();
        let tick = registry.mutation_index().tick();
        let adjacency = build_adjacency_map();
        let root_type = self.root_type.clone();
        let root_id = self.root_id.clone();
        let since = self.since_timestamp.clone();

        // Truly reactive: the anchor (before) is fixed; only the live state (after) moves.
        // Recompute when the mutation index's change tick bumps — i.e. when a tracked
        // entity actually changes — and emit only when the change set differs (deduped).
        // No polling, no event-log reread. High-frequency excluded/buffered types don't
        // bump the tick, so this stays quiet under pulse traffic.
        tick.switch_map(move |_| {
                let changes =
                    compute_entity_changes(&registry, &adjacency, &root_type, &root_id, &since);
                Cell::new(Arc::new(EntityChanges {
                    root_type: root_type.clone(),
                    root_id: root_id.clone(),
                    since_timestamp: since.clone(),
                    changes,
                }))
                .lock()
            })
            .deduped()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RelationshipSchema — the type-level relationship graph
// ─────────────────────────────────────────────────────────────────────────────

/// One parent→child edge in the entity-type relationship graph (the same edges
/// `walk_tree` follows; `exclude_from_tree` relations are omitted).
#[myko_report_output]
pub struct RelationshipEdge {
    pub parent_type: Arc<str>,
    pub child_type: Arc<str>,
    /// `"belongs_to"` | `"owns_many"` | `"ensure_for"`.
    pub kind: String,
}

/// Report: the authoritative type-level relationship structure, derived from the
/// relationship registry. Pure — no store, no DB. This is the exact graph snapshots,
/// restore points, and diffs traverse, so a viewer built on it reflects reality rather
/// than reverse-engineering FK fields client-side.
#[myko_report(Vec<RelationshipEdge>)]
pub struct RelationshipSchema {}

#[cfg(not(target_arch = "wasm32"))]
impl crate::report::ReportHandler for RelationshipSchema {
    type Output = Vec<RelationshipEdge>;

    fn compute(
        &self,
        _ctx: crate::report::ReportContext,
    ) -> impl MaterializeDefinite<Arc<Self::Output>> {
        let adjacency = build_adjacency_map();
        let mut edges = Vec::new();
        for (parent, children) in &adjacency {
            for cr in children {
                let kind = match cr.kind {
                    ChildKind::BelongsTo { .. } => "belongs_to",
                    ChildKind::OwnsMany { .. } => "owns_many",
                    ChildKind::EnsureFor { .. } => "ensure_for",
                };
                edges.push(RelationshipEdge {
                    parent_type: Arc::from(*parent),
                    child_type: Arc::from(cr.child_type),
                    kind: kind.to_string(),
                });
            }
        }
        edges.sort_by(|a, b| {
            (a.parent_type.as_ref(), a.child_type.as_ref())
                .cmp(&(b.parent_type.as_ref(), b.child_type.as_ref()))
        });
        Cell::new(Arc::new(edges)).lock()
    }
}
