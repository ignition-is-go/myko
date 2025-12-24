//! Cell-based RelationshipManager for handling entity relationship cascades.
//!
//! This module handles cascade operations based on relationships registered via
//! `#[belongs_to]`, `#[owns_many]`, and `#[ensure_for]` attribute macros.
//!
//! Uses CellServerCtx for queries and event publishing, keeping this module
//! decoupled from direct store and event processor access.
//!
//! # Relationship Types
//!
//! ## BelongsTo (Foreign Key)
//!
//! A child entity has a foreign key pointing to a parent. When the parent is deleted,
//! all children with matching foreign keys are cascade-deleted.
//!
//! ```rust,ignore
//! #[myko_item]
//! pub struct Binding {
//!     #[belongs_to(Scene)]
//!     pub scope_id: Arc<str>,
//! }
//! ```
//!
//! ## OwnsMany (Parent has array of child IDs)
//!
//! A parent entity owns an array of child IDs. Deleting the parent deletes all children.
//! Deleting a child removes its ID from the parent's array.
//!
//! ```rust,ignore
//! #[myko_item]
//! pub struct Scene {
//!     #[owns_many(BindingNode)]
//!     pub node_ids: Vec<Arc<str>>,
//! }
//! ```
//!
//! ## EnsureFor (Auto-create for combinations)
//!
//! Automatically create one entity for each combination of dependency entities.
//!
//! ```rust,ignore
//! #[myko_item]
//! pub struct BundleStatus {
//!     #[ensure_for(Session)]
//!     pub session_id: Arc<str>,
//!     #[ensure_for(Bundle)]
//!     pub bundle_id: Arc<str>,
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use hypha::Gettable;
use log::{debug, info, trace};

use crate::core::item::AnyItem;
use crate::event::EventOptions;
use crate::relationship::{
    ArrayExtractor, ArrayRemover, EnsureForDependency, EntityFactory, FkExtractor, Relation,
    iter_relations,
};

use super::CellServerCtx;

/// Lookup info for BelongsTo cascades
#[derive(Clone)]
struct BelongsToLookup {
    local_type: &'static str,
    foreign_type: &'static str,
    extract_fk: FkExtractor,
}

/// Lookup info for OwnsMany cascades
#[derive(Clone)]
struct OwnsManyLookup {
    local_type: &'static str,
    foreign_type: &'static str,
    extract_ids: ArrayExtractor,
    remove_id: ArrayRemover,
}

/// Lookup info for EnsureFor cascades
#[derive(Clone)]
struct EnsureForLookup {
    local_type: &'static str,
    dependencies: Vec<EnsureForDependency>,
    make_entity: EntityFactory,
}

/// Cell-based RelationshipManager for handling entity relationship cascades.
///
/// This manager discovers relationships via [`inventory`] at initialization,
/// builds lookup indexes for efficient cascade processing, and provides
/// methods for processing events and establishing relations on startup.
///
/// Unlike the actor-based version, this implementation uses CellServerCtx
/// for queries and event publishing, keeping it decoupled from direct
/// store and event processor access.
pub struct RelationshipManager {
    /// BelongsTo relations indexed by foreign_type (the parent type)
    /// When a parent is deleted, look up children to cascade delete
    belongs_to_by_foreign: HashMap<&'static str, Vec<BelongsToLookup>>,

    /// BelongsTo relations indexed by local_type (the child type)
    /// Used for orphan cleanup on startup
    belongs_to_by_local: HashMap<&'static str, Vec<BelongsToLookup>>,

    /// OwnsMany relations indexed by local_type (the parent type)
    /// When a parent is deleted, delete all owned children
    owns_many_by_local: HashMap<&'static str, Vec<OwnsManyLookup>>,

    /// OwnsMany relations indexed by foreign_type (the child type)
    /// When a child is deleted, update parent arrays
    owns_many_by_foreign: HashMap<&'static str, Vec<OwnsManyLookup>>,

    /// EnsureFor relations indexed by their dependency types
    /// When a dependency entity is created, ensure derived entities exist
    ensure_for_by_dependency: HashMap<&'static str, Vec<EnsureForLookup>>,
}

impl RelationshipManager {
    /// Create a new RelationshipManager with lookup tables built from inventory.
    pub fn new() -> Self {
        info!("RelationshipManager: Initializing from inventory");

        let mut belongs_to_by_foreign: HashMap<&'static str, Vec<BelongsToLookup>> = HashMap::new();
        let mut belongs_to_by_local: HashMap<&'static str, Vec<BelongsToLookup>> = HashMap::new();
        let mut owns_many_by_local: HashMap<&'static str, Vec<OwnsManyLookup>> = HashMap::new();
        let mut owns_many_by_foreign: HashMap<&'static str, Vec<OwnsManyLookup>> = HashMap::new();
        let mut ensure_for_by_dependency: HashMap<&'static str, Vec<EnsureForLookup>> =
            HashMap::new();

        for registration in iter_relations() {
            match &registration.relation {
                Relation::BelongsTo {
                    local_type,
                    foreign_type,
                    extract_fk,
                } => {
                    trace!(
                        "RelationshipManager: Registered BelongsTo {} -> {}",
                        local_type, foreign_type
                    );
                    let lookup = BelongsToLookup {
                        local_type,
                        foreign_type,
                        extract_fk: *extract_fk,
                    };
                    belongs_to_by_foreign
                        .entry(foreign_type)
                        .or_default()
                        .push(lookup.clone());
                    belongs_to_by_local
                        .entry(local_type)
                        .or_default()
                        .push(lookup);
                }
                Relation::OwnsMany {
                    local_type,
                    foreign_type,
                    extract_ids,
                    remove_id,
                } => {
                    trace!(
                        "RelationshipManager: Registered OwnsMany {} ->> {}",
                        local_type, foreign_type
                    );
                    let lookup = OwnsManyLookup {
                        local_type,
                        foreign_type,
                        extract_ids: *extract_ids,
                        remove_id: *remove_id,
                    };
                    owns_many_by_local
                        .entry(local_type)
                        .or_default()
                        .push(lookup.clone());
                    owns_many_by_foreign
                        .entry(foreign_type)
                        .or_default()
                        .push(lookup);
                }
                Relation::EnsureFor {
                    local_type,
                    dependencies,
                    make_entity,
                } => {
                    trace!(
                        "RelationshipManager: Registered EnsureFor {} for {:?}",
                        local_type,
                        dependencies
                            .iter()
                            .map(|d| d.foreign_type)
                            .collect::<Vec<_>>()
                    );
                    let deps: Vec<_> = dependencies.to_vec();

                    // Index by each dependency type
                    for dep in dependencies.iter() {
                        ensure_for_by_dependency
                            .entry(dep.foreign_type)
                            .or_default()
                            .push(EnsureForLookup {
                                local_type,
                                dependencies: deps.clone(),
                                make_entity: *make_entity,
                            });
                    }
                }
            }
        }

        let relation_count =
            belongs_to_by_foreign.len() + owns_many_by_local.len() + ensure_for_by_dependency.len();
        debug!(
            "RelationshipManager: {} relation types indexed",
            relation_count
        );

        Self {
            belongs_to_by_foreign,
            belongs_to_by_local,
            owns_many_by_local,
            owns_many_by_foreign,
            ensure_for_by_dependency,
        }
    }

    /// Forward a SET event for relationship processing.
    ///
    /// Handles EnsureFor: when a dependency entity is created, ensures
    /// all derived entities exist for all combinations.
    pub fn forward_set(&self, item: Arc<dyn AnyItem>, ctx: &CellServerCtx) {
        let item_type = item.entity_type();

        // Handle EnsureFor (dependency created → ensure derived entities exist)
        if self.ensure_for_by_dependency.contains_key(item_type) {
            self.handle_ensure_for(&item, ctx);
        }
    }

    /// Forward a DEL event for relationship processing.
    ///
    /// Handles:
    /// - BelongsTo cascade deletes (parent deleted → delete children)
    /// - OwnsMany parent deletes (parent deleted → delete owned children)
    /// - OwnsMany child deletes (child deleted → update parent arrays)
    pub fn forward_del(&self, item: Arc<dyn AnyItem>, ctx: &CellServerCtx) {
        // Handle BelongsTo cascades (parent deleted → delete children)
        self.handle_belongs_to_cascade(&item, ctx);

        // Handle OwnsMany parent deleted → delete owned children
        self.handle_owns_many_parent_delete(&item, ctx);

        // Handle OwnsMany child deleted → update parent arrays
        self.handle_owns_many_child_delete(&item, ctx);
    }

    /// Establish relations on startup (called after Kafka catchup).
    ///
    /// This performs:
    /// 1. BelongsTo orphan cleanup: Delete children pointing to non-existent parents
    /// 2. OwnsMany orphan cleanup: Delete children not referenced by any parent
    /// 3. EnsureFor initialization: Create missing entities for all dependency combinations
    pub fn establish_relations(&self, ctx: &CellServerCtx) {
        info!("RelationshipManager: Establishing relations on startup");
        info!(
            "RelationshipManager: BelongsTo relations by local: {:?}",
            self.belongs_to_by_local.keys().collect::<Vec<_>>()
        );
        info!(
            "RelationshipManager: OwnsMany relations by local: {:?}",
            self.owns_many_by_local.keys().collect::<Vec<_>>()
        );

        // 1. Orphan cleanup for BelongsTo relationships
        self.cleanup_belongs_to_orphans(ctx);

        // 2. Orphan cleanup for OwnsMany relationships
        self.cleanup_owns_many_orphans(ctx);

        // 3. EnsureFor initialization
        self.initialize_ensure_for(ctx);

        info!("RelationshipManager: Relations established");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Cascade handlers
    // ─────────────────────────────────────────────────────────────────────────────

    /// Handle BelongsTo cascades: when a parent is deleted, delete all children
    fn handle_belongs_to_cascade(&self, item: &Arc<dyn AnyItem>, ctx: &CellServerCtx) {
        let item_type = item.entity_type();
        let Some(lookups) = self.belongs_to_by_foreign.get(item_type) else {
            return;
        };

        let parent_id = item.id();

        for lookup in lookups {
            // Find children whose FK matches the deleted parent ID using extractor
            let children = self.find_children_by_fk(ctx, lookup, &parent_id);

            for child in children {
                trace!(
                    "RelationshipManager: Cascade delete {} {} (parent {} deleted)",
                    lookup.local_type,
                    child.id(),
                    &parent_id
                );
                self.publish_del_cascade(ctx, lookup.local_type, &child.id());
            }
        }
    }

    /// Find children whose FK matches a given parent ID
    fn find_children_by_fk(
        &self,
        ctx: &CellServerCtx,
        lookup: &BelongsToLookup,
        parent_id: &str,
    ) -> Vec<Arc<dyn AnyItem>> {
        let store = ctx.registry.get_or_create(lookup.local_type);
        store
            .entries()
            .get()
            .into_iter()
            .filter(|(_, item)| {
                (lookup.extract_fk)(item.as_any())
                    .map(|fk| fk.as_ref() == parent_id)
                    .unwrap_or(false)
            })
            .map(|(_, item)| item)
            .collect()
    }

    /// Handle OwnsMany parent delete: delete all owned children
    fn handle_owns_many_parent_delete(&self, item: &Arc<dyn AnyItem>, ctx: &CellServerCtx) {
        let item_type = item.entity_type();
        let Some(lookups) = self.owns_many_by_local.get(item_type) else {
            return;
        };

        for lookup in lookups {
            // Extract child IDs using the typed extractor
            let child_ids = match (lookup.extract_ids)(item.as_any()) {
                Some(ids) => ids,
                None => continue,
            };

            if child_ids.is_empty() {
                continue;
            }

            // Delete each child
            for child_id in &child_ids {
                // Check if child exists first
                if self.get_by_id(ctx, lookup.foreign_type, child_id).is_some() {
                    trace!(
                        "RelationshipManager: Cascade delete owned {} {}",
                        lookup.foreign_type, child_id
                    );
                    self.publish_del_cascade(ctx, lookup.foreign_type, child_id);
                }
            }
        }
    }

    /// Handle OwnsMany child delete: remove child ID from parent arrays
    fn handle_owns_many_child_delete(&self, item: &Arc<dyn AnyItem>, ctx: &CellServerCtx) {
        let item_type = item.entity_type();
        let Some(lookups) = self.owns_many_by_foreign.get(item_type) else {
            return;
        };

        let child_id = item.id();

        for lookup in lookups {
            // Find parents that contain this child ID using extract_ids
            let parents = self.find_parents_containing(ctx, lookup, &child_id);

            for parent_item in parents {
                // Use the remove_id extractor to get updated parent as Value
                if let Some(updated_parent) = (lookup.remove_id)(parent_item.as_any(), &child_id) {
                    trace!(
                        "RelationshipManager: Updating {} {} to remove child {}",
                        lookup.local_type,
                        parent_item.id(),
                        child_id
                    );
                    self.publish_set_cascade(ctx, lookup.local_type, updated_parent);
                }
            }
        }
    }

    /// Find parents whose owned array contains a given child ID
    fn find_parents_containing(
        &self,
        ctx: &CellServerCtx,
        lookup: &OwnsManyLookup,
        child_id: &str,
    ) -> Vec<Arc<dyn AnyItem>> {
        let store = ctx.registry.get_or_create(lookup.local_type);
        store
            .entries()
            .get()
            .into_iter()
            .filter(|(_, item)| {
                (lookup.extract_ids)(item.as_any())
                    .map(|ids| ids.iter().any(|id| id.as_ref() == child_id))
                    .unwrap_or(false)
            })
            .map(|(_, item)| item)
            .collect()
    }

    /// Handle EnsureFor: when dependency created, ensure derived entities exist
    fn handle_ensure_for(&self, item: &Arc<dyn AnyItem>, ctx: &CellServerCtx) {
        let item_type = item.entity_type();
        let Some(lookups) = self.ensure_for_by_dependency.get(item_type) else {
            return;
        };

        for lookup in lookups {
            // Get all combinations of dependency entities
            let combinations = self.get_dependency_combinations(ctx, &lookup.dependencies);

            for combo in combinations {
                // Check if derived entity already exists
                let existing = self.find_ensure_for_entity(
                    ctx,
                    lookup.local_type,
                    &lookup.dependencies,
                    &combo,
                );

                if existing.is_none() {
                    // Create the derived entity using the factory
                    let entity = (lookup.make_entity)(&combo);

                    trace!(
                        "RelationshipManager: Creating ensured {} for {:?}",
                        lookup.local_type, combo
                    );

                    self.publish_set_cascade(ctx, lookup.local_type, entity);
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Orphan cleanup
    // ─────────────────────────────────────────────────────────────────────────────

    /// Cleanup orphaned children for BelongsTo relationships
    fn cleanup_belongs_to_orphans(&self, ctx: &CellServerCtx) {
        debug!(
            "RelationshipManager: cleanup_belongs_to_orphans - checking {} child types",
            self.belongs_to_by_local.len()
        );

        for (child_type, lookups) in &self.belongs_to_by_local {
            debug!(
                "RelationshipManager: Checking BelongsTo orphans for child type '{}' ({} lookups)",
                child_type,
                lookups.len()
            );

            for lookup in lookups {
                // Get all parent IDs that exist
                let parents = self.get_all_items(ctx, lookup.foreign_type);
                let parent_ids: HashSet<Arc<str>> = parents.iter().map(|p| p.id()).collect();

                debug!(
                    "RelationshipManager: {} -> {}: Found {} parents in store",
                    child_type,
                    lookup.foreign_type,
                    parents.len()
                );

                // Get all children and find orphans using typed extractor
                let children = self.get_all_items(ctx, child_type);
                debug!(
                    "RelationshipManager: {} -> {}: Found {} children in store",
                    child_type,
                    lookup.foreign_type,
                    children.len()
                );

                let mut orphan_count = 0;
                let mut valid_count = 0;
                let mut no_fk_count = 0;

                for child in &children {
                    // Use the typed extractor to get the FK value
                    if let Some(fk_value) = (lookup.extract_fk)(child.as_any()) {
                        if !parent_ids.contains(&fk_value) {
                            debug!(
                                "RelationshipManager: ORPHAN {} {} has FK '{}' but parent {} not found (have {} parent IDs)",
                                child_type,
                                child.id(),
                                fk_value,
                                lookup.foreign_type,
                                parent_ids.len()
                            );
                            self.publish_del_cascade(ctx, child_type, &child.id());
                            orphan_count += 1;
                        } else {
                            valid_count += 1;
                        }
                    } else {
                        debug!(
                            "RelationshipManager: {} {} - extract_fk returned None",
                            child_type,
                            child.id()
                        );
                        no_fk_count += 1;
                    }
                }

                info!(
                    "RelationshipManager: {} -> {}: {} orphans deleted, {} valid, {} no FK",
                    child_type, lookup.foreign_type, orphan_count, valid_count, no_fk_count
                );
            }
        }
    }

    /// Cleanup orphaned children for OwnsMany relationships
    fn cleanup_owns_many_orphans(&self, ctx: &CellServerCtx) {
        debug!(
            "RelationshipManager: cleanup_owns_many_orphans - checking {} parent types",
            self.owns_many_by_local.len()
        );

        for (parent_type, lookups) in &self.owns_many_by_local {
            debug!(
                "RelationshipManager: Checking OwnsMany orphans for parent type '{}' ({} lookups)",
                parent_type,
                lookups.len()
            );

            for lookup in lookups {
                // Get all child IDs referenced by parents using typed extractors
                let parents = self.get_all_items(ctx, parent_type);
                let mut referenced_ids: HashSet<Arc<str>> = HashSet::new();

                debug!(
                    "RelationshipManager: {} ->> {}: Found {} parents in store",
                    parent_type,
                    lookup.foreign_type,
                    parents.len()
                );

                let mut parents_with_ids = 0;
                let mut parents_no_ids = 0;
                for parent in &parents {
                    if let Some(ids) = (lookup.extract_ids)(parent.as_any()) {
                        if !ids.is_empty() {
                            parents_with_ids += 1;
                        }
                        referenced_ids.extend(ids);
                    } else {
                        parents_no_ids += 1;
                    }
                }

                debug!(
                    "RelationshipManager: {} ->> {}: {} parents have child IDs, {} have no IDs, {} total referenced child IDs",
                    parent_type,
                    lookup.foreign_type,
                    parents_with_ids,
                    parents_no_ids,
                    referenced_ids.len()
                );

                // Get all children and find orphans
                let children = self.get_all_items(ctx, lookup.foreign_type);
                debug!(
                    "RelationshipManager: {} ->> {}: Found {} children in store",
                    parent_type,
                    lookup.foreign_type,
                    children.len()
                );

                let mut orphan_count = 0;
                let mut valid_count = 0;

                for child in children {
                    let child_id = child.id();
                    if !referenced_ids.contains(&child_id) {
                        debug!(
                            "RelationshipManager: ORPHAN {} {} not referenced by any {} (have {} referenced IDs)",
                            lookup.foreign_type,
                            child_id,
                            parent_type,
                            referenced_ids.len()
                        );
                        self.publish_del_cascade(ctx, lookup.foreign_type, &child_id);
                        orphan_count += 1;
                    } else {
                        valid_count += 1;
                    }
                }

                info!(
                    "RelationshipManager: {} ->> {}: {} orphans deleted, {} valid",
                    parent_type, lookup.foreign_type, orphan_count, valid_count
                );
            }
        }
    }

    /// Initialize EnsureFor relationships (create missing derived entities)
    fn initialize_ensure_for(&self, ctx: &CellServerCtx) {
        // Track which local_types we've processed to avoid duplicates
        let mut processed: HashSet<&'static str> = HashSet::new();

        for lookups in self.ensure_for_by_dependency.values() {
            for lookup in lookups {
                if processed.contains(lookup.local_type) {
                    continue;
                }
                processed.insert(lookup.local_type);

                // Get all combinations of dependency entities
                let combinations = self.get_dependency_combinations(ctx, &lookup.dependencies);

                let mut created_count = 0;

                for combo in combinations {
                    // Check if derived entity already exists
                    let existing = self.find_ensure_for_entity(
                        ctx,
                        lookup.local_type,
                        &lookup.dependencies,
                        &combo,
                    );

                    if existing.is_none() {
                        // Create the derived entity using the factory
                        let entity = (lookup.make_entity)(&combo);
                        self.publish_set_cascade(ctx, lookup.local_type, entity);
                        created_count += 1;
                    }
                }

                if created_count > 0 {
                    info!(
                        "RelationshipManager: Created {} {} entities via EnsureFor",
                        created_count, lookup.local_type
                    );
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Query helpers (using CellServerCtx)
    // ─────────────────────────────────────────────────────────────────────────────

    /// Get an entity by ID
    fn get_by_id(
        &self,
        ctx: &CellServerCtx,
        entity_type: &str,
        id: &str,
    ) -> Option<Arc<dyn AnyItem>> {
        let store = ctx.registry.get_or_create(entity_type);
        store.get_value(&id.into())
    }

    /// Get all entities of a type
    fn get_all_items(&self, ctx: &CellServerCtx, entity_type: &str) -> Vec<Arc<dyn AnyItem>> {
        let store = ctx.registry.get_or_create(entity_type);
        store
            .entries()
            .get()
            .into_iter()
            .map(|(_, item)| item)
            .collect()
    }

    /// Get all combinations of dependency entity IDs for EnsureFor
    fn get_dependency_combinations(
        &self,
        ctx: &CellServerCtx,
        dependencies: &[EnsureForDependency],
    ) -> Vec<Vec<Arc<str>>> {
        if dependencies.is_empty() {
            return vec![];
        }

        // Get IDs for each dependency type
        let mut dep_ids: Vec<Vec<Arc<str>>> = Vec::new();

        for dep in dependencies {
            let items = self.get_all_items(ctx, dep.foreign_type);
            let ids: Vec<Arc<str>> = items.iter().map(|item| item.id()).collect();
            dep_ids.push(ids);
        }

        // Compute Cartesian product
        self.cartesian_product(&dep_ids)
    }

    /// Compute Cartesian product of multiple ID sets
    fn cartesian_product(&self, sets: &[Vec<Arc<str>>]) -> Vec<Vec<Arc<str>>> {
        if sets.is_empty() {
            return vec![];
        }

        let mut result = vec![vec![]];

        for set in sets {
            let mut new_result = Vec::new();
            for existing in &result {
                for item in set {
                    let mut new_combo = existing.clone();
                    new_combo.push(item.clone());
                    new_result.push(new_combo);
                }
            }
            result = new_result;
        }

        result
    }

    /// Find an EnsureFor entity matching the given dependency IDs
    fn find_ensure_for_entity(
        &self,
        ctx: &CellServerCtx,
        local_type: &str,
        dependencies: &[EnsureForDependency],
        combo: &[Arc<str>],
    ) -> Option<Arc<dyn AnyItem>> {
        if dependencies.is_empty() || combo.is_empty() {
            return None;
        }

        // Get all entities of the local type and filter by matching all FK values
        let store = ctx.registry.get_or_create(local_type);

        store.entries().get().into_iter().find_map(|(_, item)| {
            // Check if all dependency FKs match the combo values
            let all_match = dependencies
                .iter()
                .zip(combo.iter())
                .all(|(dep, expected_id)| {
                    (dep.extract_fk)(item.as_any())
                        .map(|fk| fk == *expected_id)
                        .unwrap_or(false)
                });

            if all_match { Some(item) } else { None }
        })
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Publishing helpers (using CellServerCtx)
    // ─────────────────────────────────────────────────────────────────────────────

    /// Publish a SET for cascade operations.
    ///
    /// Sets prevent_relationship_updates to avoid infinite loops.
    fn publish_set_cascade(&self, ctx: &CellServerCtx, _entity_type: &str, item: Arc<dyn AnyItem>) {
        let options = EventOptions {
            prevent_relationship_updates: true,
            ..Default::default()
        };
        ctx.set_dyn_with_options(item, Some(options));
    }

    /// Publish a DEL for cascade operations.
    ///
    /// Sets prevent_relationship_updates to avoid infinite loops.
    fn publish_del_cascade(&self, ctx: &CellServerCtx, entity_type: &str, id: &str) {
        let options = EventOptions {
            prevent_relationship_updates: true,
            ..Default::default()
        };

        // Get the entity from the store
        let id_arc: Arc<str> = id.into();
        if let Some(item) = ctx.registry.get_or_create(entity_type).get_value(&id_arc) {
            debug!(
                "RelationshipManager: publish_del_cascade {} {} - entity found, deleting",
                entity_type, id
            );
            ctx.del_dyn_with_options(item, Some(options));
        } else {
            debug!(
                "RelationshipManager: publish_del_cascade {} {} - entity NOT found in store",
                entity_type, id
            );
        }
    }
}

impl Default for RelationshipManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relationship_manager_creation() {
        let manager = RelationshipManager::new();

        // Should have built lookup tables from inventory
        // (actual counts depend on entities linked in test binary)
        // Just verify the manager initializes without panic
        let _ = manager.belongs_to_by_foreign.len();
        let _ = manager.owns_many_by_local.len();
    }

    #[test]
    fn test_cartesian_product() {
        let manager = RelationshipManager::new();

        let sets = vec![
            vec![Arc::from("a"), Arc::from("b")],
            vec![Arc::from("1"), Arc::from("2")],
        ];

        let product = manager.cartesian_product(&sets);

        assert_eq!(product.len(), 4);
        assert!(product.contains(&vec![Arc::from("a"), Arc::from("1")]));
        assert!(product.contains(&vec![Arc::from("a"), Arc::from("2")]));
        assert!(product.contains(&vec![Arc::from("b"), Arc::from("1")]));
        assert!(product.contains(&vec![Arc::from("b"), Arc::from("2")]));
    }

    #[test]
    fn test_cartesian_product_empty() {
        let manager = RelationshipManager::new();

        let sets: Vec<Vec<Arc<str>>> = vec![];
        let product = manager.cartesian_product(&sets);
        assert!(product.is_empty());
    }
}
