//! Cell-based RelationshipManager for handling entity relationship cascades.
//!
//! This module handles cascade operations based on relationships registered via
//! `#[belongs_to]`, `#[owns_many]`, and `#[ensure_for]` attribute macros.
//!
//! Uses MykoServerContext for queries and event publishing, keeping this module
//! decoupled from direct store and event processor access.
//!
//! # Relationship Types
//!
//! ## BelongsTo (Foreign Key)
//!
//! A child entity has a foreign key pointing to a parent. When the parent is deleted,
//! all children with matching foreign keys are cascade-deleted.
//!
//! ```text
//! use myko::prelude::*;
//! use std::sync::Arc;
//!
//! #[myko_item]
//! pub struct Scene {
//!     pub name: String,
//! }
//!
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
//! ```text
//! use myko::prelude::*;
//! use std::sync::Arc;
//!
//! #[myko_item]
//! pub struct BindingNode {
//!     pub name: String,
//! }
//!
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
//! ```text
//! use myko::prelude::*;
//! use std::sync::Arc;
//!
//! #[myko_item]
//! pub struct Session {
//!     pub name: String,
//! }
//!
//! #[myko_item]
//! pub struct Bundle {
//!     pub name: String,
//! }
//!
//! #[myko_item]
//! pub struct BundleStatus {
//!     #[ensure_for(Session)]
//!     pub session_id: Arc<str>,
//!     #[ensure_for(Bundle)]
//!     pub bundle_id: Arc<str>,
//! }
//! ```

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use dashmap::DashMap;
use hyphae::{Gettable, MaterializeDefinite};
use tracing::{debug, info, trace};

use super::{MykoServerContext, persister::PersistError};
use crate::{
    core::item::AnyItem,
    relationship::{
        ArrayExtractor, ArrayRemover, EnsureForDependency, EntityFactory, FkExtractor, Relation,
        iter_relations,
    },
};

/// Lookup info for BelongsTo cascades
#[derive(Clone)]
struct BelongsToLookup {
    id: u64,
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
/// Unlike the actor-based version, this implementation uses MykoServerContext
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

    /// Reverse belongs_to index: lookup_id -> parent_id -> child_ids
    belongs_to_children_by_parent: DashMap<u64, DashMap<Arc<str>, BTreeSet<Arc<str>>>>,

    /// Reverse belongs_to index: lookup_id -> child_id -> parent_id
    belongs_to_parent_by_child: DashMap<u64, DashMap<Arc<str>, Arc<str>>>,
}

impl RelationshipManager {
    /// Create a new RelationshipManager with lookup tables built from inventory.
    pub fn new() -> Self {
        trace!("RelationshipManager: Initializing from inventory");

        let mut belongs_to_by_foreign: HashMap<&'static str, Vec<BelongsToLookup>> = HashMap::new();
        let mut belongs_to_by_local: HashMap<&'static str, Vec<BelongsToLookup>> = HashMap::new();
        let mut owns_many_by_local: HashMap<&'static str, Vec<OwnsManyLookup>> = HashMap::new();
        let mut owns_many_by_foreign: HashMap<&'static str, Vec<OwnsManyLookup>> = HashMap::new();
        let mut ensure_for_by_dependency: HashMap<&'static str, Vec<EnsureForLookup>> =
            HashMap::new();

        let mut next_belongs_to_id = 1u64;
        for registration in iter_relations() {
            match &registration.relation {
                Relation::BelongsTo {
                    local_type,
                    foreign_type,
                    extract_fk,
                    ..
                } => {
                    trace!(
                        "RelationshipManager: Registered BelongsTo {} -> {}",
                        local_type, foreign_type
                    );
                    let lookup = BelongsToLookup {
                        id: next_belongs_to_id,
                        local_type,
                        foreign_type,
                        extract_fk: *extract_fk,
                    };
                    next_belongs_to_id += 1;
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
                    ..
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
                    ..
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
        trace!(
            "RelationshipManager: {} relation types indexed",
            relation_count
        );

        Self {
            belongs_to_by_foreign,
            belongs_to_by_local,
            owns_many_by_local,
            owns_many_by_foreign,
            ensure_for_by_dependency,
            belongs_to_children_by_parent: DashMap::new(),
            belongs_to_parent_by_child: DashMap::new(),
        }
    }

    /// Forward a SET event for relationship processing.
    ///
    /// Handles EnsureFor: when a dependency entity is created, ensures
    /// all derived entities exist for all combinations.
    pub fn forward_set(
        &self,
        item: Arc<dyn AnyItem>,
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        let item_type = item.entity_type();

        if let Some(lookups) = self.belongs_to_by_local.get(item_type) {
            for lookup in lookups {
                self.index_belongs_to_child(lookup, &item);
            }
        }

        // Handle EnsureFor (dependency created → ensure derived entities exist)
        if self.ensure_for_by_dependency.contains_key(item_type) {
            self.handle_ensure_for(&item, ctx)?;
        }

        Ok(())
    }

    /// Forward a DEL event for relationship processing.
    ///
    /// Handles:
    /// - BelongsTo cascade deletes (parent deleted → delete children)
    /// - OwnsMany parent deletes (parent deleted → delete owned children)
    /// - OwnsMany child deletes (child deleted → update parent arrays)
    /// - EnsureFor cascade deletes (dependency deleted → delete derived entities)
    pub fn forward_del(
        &self,
        item: Arc<dyn AnyItem>,
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        // Handle BelongsTo cascades (parent deleted → delete children)
        self.handle_belongs_to_cascade(&item, ctx)?;

        // Handle OwnsMany parent deleted → delete owned children
        self.handle_owns_many_parent_delete(&item, ctx)?;

        // Handle OwnsMany child deleted → update parent arrays
        self.handle_owns_many_child_delete(&item, ctx)?;

        // Handle EnsureFor dependency deleted → delete derived entities
        self.handle_ensure_for_delete(&item, ctx)?;

        if let Some(lookups) = self.belongs_to_by_local.get(item.entity_type()) {
            for lookup in lookups {
                self.remove_belongs_to_child(lookup, &item.id());
            }
        }

        Ok(())
    }

    /// Forward a batch of DEL events for relationship processing.
    ///
    /// Items should all be the same entity type. This keeps cascade deletes grouped
    /// so downstream stores and views can process one wider delete wave instead of
    /// thousands of tiny per-parent cascades.
    pub fn forward_del_batch(
        &self,
        items: &[Arc<dyn AnyItem>],
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        if items.is_empty() {
            return Ok(());
        }

        self.handle_belongs_to_cascade_batch(items, ctx)?;
        self.handle_owns_many_parent_delete_batch(items, ctx)?;
        self.handle_ensure_for_delete_batch(items, ctx)?;

        for item in items {
            self.handle_owns_many_child_delete(item, ctx)?;

            if let Some(lookups) = self.belongs_to_by_local.get(item.entity_type()) {
                for lookup in lookups {
                    self.remove_belongs_to_child(lookup, &item.id());
                }
            }
        }

        Ok(())
    }

    /// Establish relations on startup (called after durable backend catchup).
    ///
    /// This performs:
    /// 1. BelongsTo orphan cleanup: Delete children pointing to non-existent parents
    /// 2. OwnsMany orphan cleanup: Delete children not referenced by any parent
    /// 3. EnsureFor initialization: Create missing entities for all dependency combinations
    pub fn establish_relations(&self, ctx: &MykoServerContext) -> Result<(), PersistError> {
        info!("RelationshipManager: Establishing relations on startup");
        trace!(
            "RelationshipManager: BelongsTo relations by local: {:?}",
            self.belongs_to_by_local.keys().collect::<Vec<_>>()
        );
        debug!(
            "RelationshipManager: OwnsMany relations by local: {:?}",
            self.owns_many_by_local.keys().collect::<Vec<_>>()
        );

        // 1. Orphan cleanup for BelongsTo relationships
        self.cleanup_belongs_to_orphans(ctx)?;

        // 2. Orphan cleanup for OwnsMany relationships
        self.cleanup_owns_many_orphans(ctx)?;

        // 3. EnsureFor initialization
        self.initialize_ensure_for(ctx)?;

        info!("RelationshipManager: Relations established");
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Cascade handlers
    // ─────────────────────────────────────────────────────────────────────────────

    /// Handle BelongsTo cascades: when a parent is deleted, delete all children
    fn handle_belongs_to_cascade(
        &self,
        item: &Arc<dyn AnyItem>,
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        let item_type = item.entity_type();
        let Some(lookups) = self.belongs_to_by_foreign.get(item_type) else {
            return Ok(());
        };

        let parent_id = item.id();

        for lookup in lookups {
            // Find children whose FK matches the deleted parent ID using extractor
            let children = self.find_children_by_fk(ctx, lookup, &parent_id);
            if children.is_empty() {
                continue;
            }

            trace!(
                "RelationshipManager: Cascade delete batch {} count={} (parent {} deleted)",
                lookup.local_type,
                children.len(),
                parent_id
            );
            self.publish_del_cascade_batch(ctx, &children)?;
        }

        Ok(())
    }

    fn handle_belongs_to_cascade_batch(
        &self,
        items: &[Arc<dyn AnyItem>],
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        let Some(first) = items.first() else {
            return Ok(());
        };
        let item_type = first.entity_type();
        let Some(lookups) = self.belongs_to_by_foreign.get(item_type) else {
            return Ok(());
        };

        let parent_ids: Vec<Arc<str>> = items.iter().map(|item| item.id()).collect();

        for lookup in lookups {
            let mut children_by_id: HashMap<Arc<str>, Arc<dyn AnyItem>> = HashMap::new();
            for parent_id in &parent_ids {
                for child in self.find_children_by_fk(ctx, lookup, parent_id) {
                    children_by_id.entry(child.id()).or_insert(child);
                }
            }

            if children_by_id.is_empty() {
                continue;
            }

            let children: Vec<_> = children_by_id.into_values().collect();
            trace!(
                "RelationshipManager: Cascade delete batch {} count={} ({} parents deleted)",
                lookup.local_type,
                children.len(),
                parent_ids.len()
            );
            self.publish_del_cascade_batch(ctx, &children)?;
        }

        Ok(())
    }

    /// Find children whose FK matches a given parent ID
    fn find_children_by_fk(
        &self,
        ctx: &MykoServerContext,
        lookup: &BelongsToLookup,
        parent_id: &str,
    ) -> Vec<Arc<dyn AnyItem>> {
        self.ensure_belongs_to_index_loaded(ctx, lookup);
        if let Some(parent_map) = self.belongs_to_children_by_parent.get(&lookup.id) {
            let store = ctx.registry.get_or_create(lookup.local_type);
            let Some(child_ids) = parent_map.get(parent_id) else {
                return Vec::new();
            };
            return child_ids
                .iter()
                .filter_map(|child_id| store.get_value(child_id))
                .collect();
        }

        let store = ctx.registry.get_or_create(lookup.local_type);
        store
            .entries()
            .materialize()
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

    fn index_belongs_to_child(&self, lookup: &BelongsToLookup, item: &Arc<dyn AnyItem>) {
        let child_id = item.id();
        self.remove_belongs_to_child(lookup, &child_id);

        let Some(parent_id) = (lookup.extract_fk)(item.as_any()) else {
            return;
        };

        self.belongs_to_parent_by_child
            .entry(lookup.id)
            .or_default()
            .insert(child_id.clone(), parent_id.clone());
        self.belongs_to_children_by_parent
            .entry(lookup.id)
            .or_default()
            .entry(parent_id)
            .or_default()
            .insert(child_id);
    }

    fn remove_belongs_to_child(&self, lookup: &BelongsToLookup, child_id: &Arc<str>) {
        let Some(parent_map) = self.belongs_to_parent_by_child.get(&lookup.id) else {
            return;
        };
        let Some((_, parent_id)) = parent_map.remove(child_id) else {
            return;
        };

        let Some(children_by_parent) = self.belongs_to_children_by_parent.get(&lookup.id) else {
            return;
        };
        let should_remove_parent = children_by_parent
            .get_mut(parent_id.as_ref())
            .map(|mut child_ids| {
                child_ids.remove(child_id);
                child_ids.is_empty()
            })
            .unwrap_or(false);

        if should_remove_parent {
            children_by_parent.remove(parent_id.as_ref());
        }
    }

    fn ensure_belongs_to_index_loaded(&self, ctx: &MykoServerContext, lookup: &BelongsToLookup) {
        if self.belongs_to_parent_by_child.contains_key(&lookup.id) {
            return;
        }

        let child_index = DashMap::<Arc<str>, Arc<str>>::new();
        let parent_index = DashMap::<Arc<str>, BTreeSet<Arc<str>>>::new();
        let store = ctx.registry.get_or_create(lookup.local_type);

        for (_, item) in store.snapshot() {
            let Some(parent_id) = (lookup.extract_fk)(item.as_any()) else {
                continue;
            };
            let child_id = item.id();
            child_index.insert(child_id.clone(), parent_id.clone());
            parent_index.entry(parent_id).or_default().insert(child_id);
        }

        let _ = self
            .belongs_to_parent_by_child
            .insert(lookup.id, child_index);
        let _ = self
            .belongs_to_children_by_parent
            .insert(lookup.id, parent_index);
    }

    /// Handle OwnsMany parent delete: delete all owned children
    fn handle_owns_many_parent_delete(
        &self,
        item: &Arc<dyn AnyItem>,
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        let item_type = item.entity_type();
        let Some(lookups) = self.owns_many_by_local.get(item_type) else {
            return Ok(());
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

            let mut children = Vec::new();
            for child_id in &child_ids {
                if self.get_by_id(ctx, lookup.foreign_type, child_id).is_some()
                    && let Some(child) = self.get_by_id(ctx, lookup.foreign_type, child_id)
                {
                    children.push(child);
                }
            }

            if children.is_empty() {
                continue;
            }

            trace!(
                "RelationshipManager: Cascade delete owned batch {} count={}",
                lookup.foreign_type,
                children.len()
            );
            self.publish_del_cascade_batch(ctx, &children)?;
        }

        Ok(())
    }

    fn handle_owns_many_parent_delete_batch(
        &self,
        items: &[Arc<dyn AnyItem>],
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        let Some(first) = items.first() else {
            return Ok(());
        };
        let item_type = first.entity_type();
        let Some(lookups) = self.owns_many_by_local.get(item_type) else {
            return Ok(());
        };

        for lookup in lookups {
            let mut child_ids = BTreeSet::new();
            for item in items {
                if let Some(ids) = (lookup.extract_ids)(item.as_any()) {
                    child_ids.extend(ids);
                }
            }

            if child_ids.is_empty() {
                continue;
            }

            let mut children = Vec::new();
            for child_id in &child_ids {
                if let Some(child) = self.get_by_id(ctx, lookup.foreign_type, child_id) {
                    children.push(child);
                }
            }

            if children.is_empty() {
                continue;
            }

            trace!(
                "RelationshipManager: Cascade delete owned batch {} count={} ({} parents deleted)",
                lookup.foreign_type,
                children.len(),
                items.len()
            );
            self.publish_del_cascade_batch(ctx, &children)?;
        }

        Ok(())
    }

    /// Handle OwnsMany child delete: remove child ID from parent arrays
    fn handle_owns_many_child_delete(
        &self,
        item: &Arc<dyn AnyItem>,
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        let item_type = item.entity_type();
        let Some(lookups) = self.owns_many_by_foreign.get(item_type) else {
            return Ok(());
        };

        let child_id = item.id();

        for lookup in lookups {
            // Find parents that contain this child ID using extract_ids
            let parents = self.find_parents_containing(ctx, lookup, &child_id);
            let mut updates = Vec::new();

            for parent_item in parents {
                // Use the remove_id extractor to get updated parent as Value
                if let Some(updated_parent) = (lookup.remove_id)(parent_item.as_any(), &child_id) {
                    trace!(
                        "RelationshipManager: Updating {} {} to remove child {}",
                        lookup.local_type,
                        parent_item.id(),
                        child_id
                    );
                    updates.push(updated_parent);
                }
            }

            if !updates.is_empty() {
                self.publish_set_cascade_batch(ctx, &updates)?;
            }
        }

        Ok(())
    }

    /// Find parents whose owned array contains a given child ID
    fn find_parents_containing(
        &self,
        ctx: &MykoServerContext,
        lookup: &OwnsManyLookup,
        child_id: &str,
    ) -> Vec<Arc<dyn AnyItem>> {
        let store = ctx.registry.get_or_create(lookup.local_type);
        store
            .entries()
            .materialize()
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
    fn handle_ensure_for(
        &self,
        item: &Arc<dyn AnyItem>,
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        let item_type = item.entity_type();
        let Some(lookups) = self.ensure_for_by_dependency.get(item_type) else {
            return Ok(());
        };

        for lookup in lookups {
            // Get all combinations of dependency entities
            let combinations = self.get_dependency_combinations(ctx, &lookup.dependencies);

            // Snapshot the store once outside the combo loop to avoid
            // re-materializing entries() for every combination
            let store = ctx.registry.get_or_create(lookup.local_type);
            let existing_items = store.snapshot();

            for combo in combinations {
                // Check if derived entity already exists
                let existing =
                    Self::find_ensure_for_entity_in(&existing_items, &lookup.dependencies, &combo);

                if existing.is_none() {
                    // Create the derived entity using the factory
                    let entity = (lookup.make_entity)(&combo);

                    trace!(
                        "RelationshipManager: Creating ensured {} for {:?}",
                        lookup.local_type, combo
                    );

                    self.publish_set_cascade(ctx, lookup.local_type, entity)?;
                }
            }
        }

        Ok(())
    }

    /// Handle EnsureFor: when a dependency entity is deleted, delete the
    /// entities that were auto-created for it — symmetric with
    /// `handle_ensure_for`'s create-if-missing on the SET side. Without
    /// this, `#[ensure_for(X)]`-created entities are orphaned forever once
    /// `X` is deleted (they're never revisited by any other cascade path).
    fn handle_ensure_for_delete(
        &self,
        item: &Arc<dyn AnyItem>,
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        self.handle_ensure_for_delete_batch(std::slice::from_ref(item), ctx)
    }

    fn handle_ensure_for_delete_batch(
        &self,
        items: &[Arc<dyn AnyItem>],
        ctx: &MykoServerContext,
    ) -> Result<(), PersistError> {
        let Some(first) = items.first() else {
            return Ok(());
        };
        let item_type = first.entity_type();
        let Some(lookups) = self.ensure_for_by_dependency.get(item_type) else {
            return Ok(());
        };

        let dep_ids: HashSet<Arc<str>> = items.iter().map(|item| item.id()).collect();

        for lookup in lookups {
            // A lookup can depend on several types (Cartesian-product
            // ensure_for); only the dependency matching the deleted type's
            // extractor is relevant here.
            let Some(dep) = lookup
                .dependencies
                .iter()
                .find(|dep| dep.foreign_type == item_type)
            else {
                continue;
            };

            let orphaned = self.find_ensure_for_children_by_dependency(ctx, lookup, dep, &dep_ids);
            if orphaned.is_empty() {
                continue;
            }

            trace!(
                "RelationshipManager: EnsureFor cascade delete {} count={} ({} {} dependencies deleted)",
                lookup.local_type,
                orphaned.len(),
                dep_ids.len(),
                item_type
            );
            self.publish_del_cascade_batch(ctx, &orphaned)?;
        }

        Ok(())
    }

    /// Scan `lookup.local_type`'s store for ensure_for-derived entities
    /// whose `dep`-extracted FK is one of `dep_ids`. Unlike `belongs_to`,
    /// there is no lazily-built reverse index for `ensure_for` — the
    /// created entity's id is a random UUID (`make_entity` in
    /// `handle_ensure_for`), not derivable from the dependency id, so a
    /// full-store scan is the only option here (same as `belongs_to`'s own
    /// fallback path when its index isn't loaded yet).
    fn find_ensure_for_children_by_dependency(
        &self,
        ctx: &MykoServerContext,
        lookup: &EnsureForLookup,
        dep: &EnsureForDependency,
        dep_ids: &HashSet<Arc<str>>,
    ) -> Vec<Arc<dyn AnyItem>> {
        let store = ctx.registry.get_or_create(lookup.local_type);
        store
            .entries()
            .materialize()
            .get()
            .into_iter()
            .filter(|(_, item)| {
                (dep.extract_fk)(item.as_any())
                    .map(|fk| dep_ids.contains(&fk))
                    .unwrap_or(false)
            })
            .map(|(_, item)| item)
            .collect()
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Orphan cleanup
    // ─────────────────────────────────────────────────────────────────────────────

    /// Cleanup orphaned children for BelongsTo relationships
    /// Boot-time **backstop** sweep for `belongs_to` orphans (children whose FK
    /// points at a parent that no longer exists).
    ///
    /// Runtime orphaning is handled by the transitive DEL cascade
    /// (`Origin::Cascade` + DEL descends — see `MykoServerContext::apply_effects`),
    /// so deleting a parent removes its whole subtree without a restart. This
    /// sweep remains only for the "child written with an FK to a never-existent
    /// parent" case. We deliberately do **not** delete such orphans eagerly on
    /// the child's SET: under out-of-order / eventually-consistent ingestion a
    /// child can legitimately arrive before its parent, so eager deletion would
    /// be data loss. The sweep runs at boot, once ordering has settled.
    fn cleanup_belongs_to_orphans(&self, ctx: &MykoServerContext) -> Result<(), PersistError> {
        trace!(
            "RelationshipManager: cleanup_belongs_to_orphans - checking {} child types",
            self.belongs_to_by_local.len()
        );

        for (child_type, lookups) in &self.belongs_to_by_local {
            trace!(
                "RelationshipManager: Checking BelongsTo orphans for child type '{}' ({} lookups)",
                child_type,
                lookups.len()
            );

            for lookup in lookups {
                // Get all parent IDs that exist
                let parents = self.get_all_items(ctx, lookup.foreign_type);
                let parent_ids: HashSet<Arc<str>> = parents.iter().map(|p| p.id()).collect();

                trace!(
                    "RelationshipManager: {} -> {}: Found {} parents in store",
                    child_type,
                    lookup.foreign_type,
                    parents.len()
                );

                // Get all children and find orphans using typed extractor
                let children = self.get_all_items(ctx, child_type);
                trace!(
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
                            self.publish_del_cascade(ctx, child_type, &child.id())?;
                            orphan_count += 1;
                        } else {
                            valid_count += 1;
                        }
                    } else {
                        trace!(
                            "RelationshipManager: {} {} - extract_fk returned None",
                            child_type,
                            child.id()
                        );
                        no_fk_count += 1;
                    }
                }

                trace!(
                    "RelationshipManager: {} -> {}: {} orphans deleted, {} valid, {} no FK",
                    child_type, lookup.foreign_type, orphan_count, valid_count, no_fk_count
                );
            }
        }

        Ok(())
    }

    /// Cleanup orphaned children for OwnsMany relationships
    fn cleanup_owns_many_orphans(&self, ctx: &MykoServerContext) -> Result<(), PersistError> {
        trace!(
            "RelationshipManager: cleanup_owns_many_orphans - checking {} parent types",
            self.owns_many_by_local.len()
        );

        for (parent_type, lookups) in &self.owns_many_by_local {
            trace!(
                "RelationshipManager: Checking OwnsMany orphans for parent type '{}' ({} lookups)",
                parent_type,
                lookups.len()
            );

            for lookup in lookups {
                // Get all child IDs referenced by parents using typed extractors
                let parents = self.get_all_items(ctx, parent_type);
                let mut referenced_ids: HashSet<Arc<str>> = HashSet::new();

                trace!(
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

                trace!(
                    "RelationshipManager: {} ->> {}: {} parents have child IDs, {} have no IDs, {} total referenced child IDs",
                    parent_type,
                    lookup.foreign_type,
                    parents_with_ids,
                    parents_no_ids,
                    referenced_ids.len()
                );

                // Get all children and find orphans
                let children = self.get_all_items(ctx, lookup.foreign_type);
                trace!(
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
                        self.publish_del_cascade(ctx, lookup.foreign_type, &child_id)?;
                        orphan_count += 1;
                    } else {
                        valid_count += 1;
                    }
                }

                if orphan_count > 0 {
                    info!(
                        "RelationshipManager: {} ->> {}: {} orphans deleted, {} valid",
                        parent_type, lookup.foreign_type, orphan_count, valid_count
                    );
                } else {
                    trace!(
                        "RelationshipManager: {} ->> {}: {} orphans deleted, {} valid",
                        parent_type, lookup.foreign_type, orphan_count, valid_count
                    );
                }
            }
        }

        Ok(())
    }

    /// Initialize EnsureFor relationships (create missing derived entities)
    fn initialize_ensure_for(&self, ctx: &MykoServerContext) -> Result<(), PersistError> {
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

                // Snapshot once outside the combo loop
                let store = ctx.registry.get_or_create(lookup.local_type);
                let existing_items = store.snapshot();

                let mut created_count = 0;

                for combo in combinations {
                    // Check if derived entity already exists
                    let existing = Self::find_ensure_for_entity_in(
                        &existing_items,
                        &lookup.dependencies,
                        &combo,
                    );

                    if existing.is_none() {
                        // Create the derived entity using the factory
                        let entity = (lookup.make_entity)(&combo);
                        self.publish_set_cascade(ctx, lookup.local_type, entity)?;
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

        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Query helpers (using MykoServerContext)
    // ─────────────────────────────────────────────────────────────────────────────

    /// Get an entity by ID
    fn get_by_id(
        &self,
        ctx: &MykoServerContext,
        entity_type: &str,
        id: &str,
    ) -> Option<Arc<dyn AnyItem>> {
        let store = ctx.registry.get_or_create(entity_type);
        store.get_value(&id.into())
    }

    /// Get all entities of a type
    fn get_all_items(&self, ctx: &MykoServerContext, entity_type: &str) -> Vec<Arc<dyn AnyItem>> {
        let store = ctx.registry.get_or_create(entity_type);
        store.snapshot().into_iter().map(|(_, item)| item).collect()
    }

    /// Get all combinations of dependency entity IDs for EnsureFor
    fn get_dependency_combinations(
        &self,
        ctx: &MykoServerContext,
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
    /// from a pre-computed snapshot of existing items.
    fn find_ensure_for_entity_in(
        items: &[(Arc<str>, Arc<dyn AnyItem>)],
        dependencies: &[EnsureForDependency],
        combo: &[Arc<str>],
    ) -> Option<Arc<dyn AnyItem>> {
        if dependencies.is_empty() || combo.is_empty() {
            return None;
        }

        items.iter().find_map(|(_, item)| {
            // Check if all dependency FKs match the combo values
            let all_match = dependencies
                .iter()
                .zip(combo.iter())
                .all(|(dep, expected_id)| {
                    (dep.extract_fk)(item.as_any())
                        .map(|fk| fk == *expected_id)
                        .unwrap_or(false)
                });

            if all_match { Some(item.clone()) } else { None }
        })
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Publishing helpers (using MykoServerContext)
    // ─────────────────────────────────────────────────────────────────────────────

    /// Publish a SET for cascade operations.
    ///
    /// Sets prevent_relationship_updates to avoid infinite loops.
    fn publish_set_cascade(
        &self,
        ctx: &MykoServerContext,
        _entity_type: &str,
        item: Arc<dyn AnyItem>,
    ) -> Result<(), PersistError> {
        // If the item has an empty #[server_owned] field, bake in the current server's ID
        let item = if item.server_owner().is_none() {
            item.bake_server_owner(&ctx.host_id.to_string())
                .unwrap_or(item)
        } else {
            item
        };

        ctx.set_dyn_with_origin(item, super::Origin::Cascade)
    }

    fn publish_set_cascade_batch(
        &self,
        ctx: &MykoServerContext,
        items: &[Arc<dyn AnyItem>],
    ) -> Result<(), PersistError> {
        ctx.batch_set_dyn_with_origin(items, super::Origin::Cascade)
    }

    /// Publish a DEL for cascade operations.
    ///
    /// Sets prevent_relationship_updates to avoid infinite loops.
    fn publish_del_cascade(
        &self,
        ctx: &MykoServerContext,
        entity_type: &str,
        id: &str,
    ) -> Result<(), PersistError> {
        // Get the entity from the store
        let id_arc: Arc<str> = id.into();
        if let Some(item) = ctx.registry.get_or_create(entity_type).get_value(&id_arc) {
            debug!(
                "RelationshipManager: publish_del_cascade {} {} - entity found, deleting",
                entity_type, id
            );
            ctx.del_dyn_with_origin(item, super::Origin::Cascade)?;
        } else {
            trace!(
                "RelationshipManager: publish_del_cascade {} {} - entity NOT found in store",
                entity_type, id
            );
        }

        Ok(())
    }

    fn publish_del_cascade_batch(
        &self,
        ctx: &MykoServerContext,
        items: &[Arc<dyn AnyItem>],
    ) -> Result<(), PersistError> {
        if items.is_empty() {
            return Ok(());
        }

        ctx.batch_del_dyn_with_origin(items, super::Origin::Cascade)
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

#[cfg(test)]
mod cascade_tests {
    //! Transitive relationship cascade (Event Bus Unification, Fix #1).
    //!
    //! Deleting a parent must remove its children, grandchildren, … at runtime
    //! (previously grandchildren survived until the boot-time orphan sweep
    //! because the cascade product's `prevent_relationship_updates` flag was
    //! read as "do not cascade at all"). A cyclic schema must converge, not loop.

    use std::sync::Arc;

    use uuid::Uuid;

    use self::node::CascadeNode;
    use crate::{
        hyphae::{Gettable, MaterializeDefinite},
        search::SearchIndex,
        server::{MykoServerContext, HandlerRegistry, RelationshipManager, persister::PersisterRouter},
        store::StoreRegistry,
        test_util::scheduler_test_serial,
    };

    // `#[myko_item]` re-imports hyphae traits at module scope, so the entity
    // lives in its own module (mirrors `bench_entities::tree`).
    mod node {
        use crate::prelude::*;

        /// Self-referential entity: a node `belongs_to` another node of the same
        /// type, so one type expresses both a multi-level chain and a cycle.
        #[myko_item]
        pub struct CascadeNode {
            #[belongs_to(CascadeNode)]
            pub parent_id: CascadeNodeId,
            pub name: String,
        }
    }

    fn make_ctx() -> (MykoServerContext, Arc<StoreRegistry>) {
        let registry = Arc::new(StoreRegistry::new());
        let ctx = MykoServerContext::new(
            Uuid::new_v4(),
            registry.clone(),
            Arc::new(HandlerRegistry::new()),
            Arc::new(RelationshipManager::new()),
            Arc::new(PersisterRouter::default()),
            Arc::new(SearchIndex::new()),
            Arc::new(dashmap::DashMap::new()),
            None,
            None,
        );
        (ctx, registry)
    }

    fn make_node(id: &str, parent_id: &str) -> CascadeNode {
        CascadeNode {
            id: id.into(),
            parent_id: parent_id.into(),
            name: format!("node-{id}"),
        }
    }

    fn exists(registry: &StoreRegistry, id: &str) -> bool {
        registry
            .get("CascadeNode")
            .and_then(|store| store.get(&Arc::<str>::from(id)).materialize().get())
            .is_some()
    }

    /// A 3-level `belongs_to` chain: deleting the root removes the child *and*
    /// the grandchild at runtime. The grandchild regressed before Fix #1.
    #[test]
    fn del_cascade_descends_to_grandchildren() {
        let _serial = scheduler_test_serial();
        let (ctx, registry) = make_ctx();

        // root <- branch <- leaf
        ctx.set(&make_node("root", "")).unwrap();
        ctx.set(&make_node("branch", "root")).unwrap();
        ctx.set(&make_node("leaf", "branch")).unwrap();

        assert!(exists(&registry, "root"));
        assert!(exists(&registry, "branch"));
        assert!(exists(&registry, "leaf"));

        ctx.del(&make_node("root", "")).unwrap();

        assert!(!exists(&registry, "root"), "root deleted");
        assert!(!exists(&registry, "branch"), "direct child deleted");
        assert!(
            !exists(&registry, "leaf"),
            "grandchild deleted at runtime (Fix #1)"
        );
    }

    /// Regression test: a cascade-triggered recursive `emit_grouped` call
    /// (deleting root cascades to branch, which cascades to leaf) must not
    /// share its reducing `hyphae::batch` window with the batch that
    /// triggered it. `CellMap`'s `diffs_cell` coalesces last-write-wins like
    /// any other cell, so if the recursive call's `store.remove_many` landed
    /// in the *same* still-open window as the top-level reduce, the later
    /// level's diff would silently drop the earlier one on the same
    /// `CascadeNode` store (see the batch-scoping comment on `emit_grouped`).
    #[test]
    fn del_cascade_recursion_does_not_drop_earlier_diffs_in_same_store() {
        let _serial = scheduler_test_serial();
        let (ctx, registry) = make_ctx();

        ctx.set(&make_node("root", "")).unwrap();
        ctx.set(&make_node("branch", "root")).unwrap();
        ctx.set(&make_node("leaf", "branch")).unwrap();
        ctx.set(&make_node("island", "")).unwrap();

        let store = registry.get_or_create("CascadeNode");
        let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_for_closure = seen.clone();
        let _guard = store.subscribe_diffs(move |diff| {
            seen_for_closure.lock().unwrap().push(format!("{diff:?}"));
        });
        // subscribe_diffs replays the current snapshot synchronously on
        // subscribe -- drop that so only diffs from the batch below count.
        seen.lock().unwrap().clear();

        // One wire batch: an unrelated standalone delete (island) alongside
        // root's delete, which cascades to branch then leaf -- three
        // distinct mutations to the *same* CascadeNode store triggered by
        // one top-level call.
        let root_item: Arc<dyn crate::core::item::AnyItem> = Arc::new(make_node("root", ""));
        let island_item: Arc<dyn crate::core::item::AnyItem> = Arc::new(make_node("island", ""));
        ctx.batch_del_dyn(&[root_item, island_item]).unwrap();

        assert!(!exists(&registry, "root"));
        assert!(!exists(&registry, "branch"), "direct child deleted");
        assert!(!exists(&registry, "leaf"), "grandchild deleted");
        assert!(!exists(&registry, "island"));

        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.len(),
            3,
            "expected 3 separate diffs (root+island reduce, branch cascade, leaf cascade), not coalesced: {:?}",
            *seen
        );
    }

    /// A 2-cycle (a.parent = b, b.parent = a): the cascade must converge. The
    /// store-as-visited-set guarantees it — the second visit finds nothing.
    #[test]
    fn del_cascade_terminates_on_cycle() {
        let _serial = scheduler_test_serial();
        let (ctx, registry) = make_ctx();

        ctx.set(&make_node("a", "b")).unwrap();
        ctx.set(&make_node("b", "a")).unwrap();

        ctx.del(&make_node("a", "b")).unwrap();

        assert!(!exists(&registry, "a"), "a deleted");
        assert!(
            !exists(&registry, "b"),
            "b deleted via the cycle, then terminated"
        );
    }
}

#[cfg(test)]
mod ensure_for_cascade_tests {
    //! `#[ensure_for(X)]` delete-side cleanup. Regression coverage for the
    //! orphan-accumulation bug reported against 4.24.2 (rship bead
    //! rship-e3f): `RelationshipManager::forward_del` handled `belongs_to`
    //! cascades and `owns_many` cleanup, but nothing ever revisited
    //! `ensure_for`-created entities when their dependency was deleted —
    //! `DeleteBindingNode` cascaded the node's `belongs_to` value correctly
    //! but left its `#[ensure_for(BindingNode)]` position behind forever
    //! (4,762 orphaned `BindingNodePosition`s accumulated on the rack, 516
    //! on sandbox, before this fix).

    use std::sync::Arc;

    use uuid::Uuid;

    use self::fixtures::{EnsuredStatus, Parent};
    use crate::{
        core::item::AnyItem,
        search::SearchIndex,
        server::{MykoServerContext, HandlerRegistry, RelationshipManager, persister::PersisterRouter},
        store::StoreRegistry,
        test_util::scheduler_test_serial,
    };

    // `#[myko_item]` re-imports hyphae traits at module scope, and two
    // invocations in the same module collide — each entity gets its own
    // submodule (mirrors `bench_entities::tree`/`compound_a`/`compound_b`).
    mod fixtures {
        pub use parent::{Parent, ParentId};
        mod parent {
            use crate::prelude::*;

            #[myko_item]
            pub struct Parent {
                pub name: String,
            }
        }

        pub use ensured_status::EnsuredStatus;
        mod ensured_status {
            use crate::prelude::*;

            use super::{Parent, ParentId};

            #[myko_item]
            pub struct EnsuredStatus {
                #[ensure_for(Parent)]
                pub parent_id: ParentId,
            }
        }
    }

    fn make_ctx() -> (MykoServerContext, Arc<StoreRegistry>) {
        let registry = Arc::new(StoreRegistry::new());
        let ctx = MykoServerContext::new(
            Uuid::new_v4(),
            registry.clone(),
            Arc::new(HandlerRegistry::new()),
            Arc::new(RelationshipManager::new()),
            Arc::new(PersisterRouter::default()),
            Arc::new(SearchIndex::new()),
            Arc::new(dashmap::DashMap::new()),
            None,
            None,
        );
        (ctx, registry)
    }

    fn make_parent(id: &str) -> Parent {
        Parent {
            id: id.into(),
            name: format!("parent-{id}"),
        }
    }

    /// Every `EnsuredStatus` row currently in the store whose
    /// `#[ensure_for(Parent)]` field points at `parent_id`. The created
    /// row's own id is a random UUID (not derivable from `parent_id`), so
    /// this scans rather than looking up by a known id — same reason
    /// `RelationshipManager`'s own delete-side handler has to scan.
    fn ensured_statuses_for(registry: &StoreRegistry, parent_id: &str) -> Vec<Arc<str>> {
        let Some(store) = registry.get("EnsuredStatus") else {
            return Vec::new();
        };
        store
            .snapshot()
            .into_iter()
            .filter(|(_, item)| {
                item.as_any()
                    .downcast_ref::<EnsuredStatus>()
                    .map(|status| status.parent_id.as_ref() == parent_id)
                    .unwrap_or(false)
            })
            .map(|(id, _)| id)
            .collect()
    }

    #[test]
    fn del_of_dependency_deletes_its_ensured_entity() {
        let _serial = scheduler_test_serial();
        let (ctx, registry) = make_ctx();

        ctx.set(&make_parent("p1")).unwrap();
        assert_eq!(
            ensured_statuses_for(&registry, "p1").len(),
            1,
            "ensure_for auto-created exactly one EnsuredStatus for p1"
        );

        ctx.del(&make_parent("p1")).unwrap();

        assert!(
            ensured_statuses_for(&registry, "p1").is_empty(),
            "EnsuredStatus for a deleted dependency must not be orphaned"
        );
    }

    #[test]
    fn batch_del_of_dependencies_deletes_their_ensured_entities() {
        let _serial = scheduler_test_serial();
        let (ctx, registry) = make_ctx();

        ctx.set(&make_parent("p1")).unwrap();
        ctx.set(&make_parent("p2")).unwrap();
        assert_eq!(ensured_statuses_for(&registry, "p1").len(), 1);
        assert_eq!(ensured_statuses_for(&registry, "p2").len(), 1);

        let p1: Arc<dyn AnyItem> = Arc::new(make_parent("p1"));
        let p2: Arc<dyn AnyItem> = Arc::new(make_parent("p2"));
        ctx.batch_del_dyn(&[p1, p2]).unwrap();

        assert!(ensured_statuses_for(&registry, "p1").is_empty());
        assert!(ensured_statuses_for(&registry, "p2").is_empty());
    }

    /// A dependency unrelated to `parent_id` must not lose its own
    /// `EnsuredStatus` — the delete-side cleanup must match by FK, not
    /// wipe every `EnsuredStatus` whenever any `Parent` is deleted.
    #[test]
    fn del_of_one_dependency_does_not_orphan_unrelated_ensured_entities() {
        let _serial = scheduler_test_serial();
        let (ctx, registry) = make_ctx();

        ctx.set(&make_parent("p1")).unwrap();
        ctx.set(&make_parent("p2")).unwrap();
        assert_eq!(ensured_statuses_for(&registry, "p1").len(), 1);
        assert_eq!(ensured_statuses_for(&registry, "p2").len(), 1);

        ctx.del(&make_parent("p1")).unwrap();

        assert!(ensured_statuses_for(&registry, "p1").is_empty());
        assert_eq!(
            ensured_statuses_for(&registry, "p2").len(),
            1,
            "p2's EnsuredStatus must survive p1's deletion"
        );
    }
}
