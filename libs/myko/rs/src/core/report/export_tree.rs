//! Entity tree export report.
//!
//! BFS walks the relationship graph from a root entity, collecting all
//! descendant entities into a flat `EntityTreeExport` structure.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use chrono::Utc;
use hyphae::{Cell, CellImmutable};

use crate::common::to_value::ToValue;
use myko_macros::{myko_report, myko_report_output};
use serde_json::Value;

use crate::relationship::{Relation, iter_relations};
use crate::store::StoreRegistry;

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
    /// ISO 8601 timestamp — when set, replays events up to this time
    /// into a temporary store and exports from that instead of the live store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub as_of: Option<Arc<str>>,
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
                            && fk == entity_id {
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
                            && fk == entity_id {
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

    fn compute(&self, ctx: crate::report::ReportContext) -> Cell<Arc<Self::Output>, CellImmutable> {
        let registry = if let Some(as_of) = &self.as_of {
            match ctx.replay_store(as_of) {
                Ok(r) => r,
                Err(err) => {
                    eprintln!(
                        "[ExportEntityTree] replay_store FAILED: as_of={} err={}",
                        as_of, err
                    );
                    return Cell::new(Arc::new(EntityTreeExport {
                        version: 1,
                        root_type: self.root_type.clone(),
                        root_id: self.root_id.clone(),
                        exported_at: Utc::now().to_rfc3339(),
                        entities: vec![],
                    }))
                    .lock();
                }
            }
        } else {
            ctx.registry()
        };

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
