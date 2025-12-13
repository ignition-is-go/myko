//! RelationshipManager actor for handling entity relationship cascades.
//!
//! This actor subscribes to the [`EventBus`](crate::actors::event::EventBus) and processes
//! cascade operations based on relationships registered via the `#[belongs_to]`, `#[owns_many]`,
//! and `#[ensure_for]` attribute macros.
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
//!     #[belongs_to(Scene)]  // Binding.scope_id → Scene.id
//!     pub scope_id: Arc<str>,
//! }
//! ```
//!
//! **Cascade behavior:**
//! - `Scene` deleted → all `Binding` where `scope_id == scene.id` are deleted
//! - Orphan cleanup on startup: delete `Binding` where `scope_id` points to non-existent `Scene`
//!
//! ## OwnsMany (Parent has array of child IDs)
//!
//! A parent entity owns an array of child IDs. Deleting the parent deletes all children.
//! Deleting a child removes its ID from the parent's array.
//!
//! ```rust,ignore
//! #[myko_item]
//! pub struct Scene {
//!     #[owns_many(BindingNode)]  // Scene.node_ids[] → BindingNode.id
//!     pub node_ids: Vec<Arc<str>>,
//! }
//! ```
//!
//! **Cascade behavior:**
//! - `Scene` deleted → all `BindingNode` in `node_ids` are deleted
//! - `BindingNode` deleted → its ID is removed from parent `Scene.node_ids`
//! - Orphan cleanup on startup: delete `BindingNode` not in any `Scene.node_ids`
//!
//! ## EnsureFor (Auto-create for combinations)
//!
//! Automatically create one entity for each combination of dependency entities.
//! Used for cross-product derived entities like "one status per session per bundle".
//!
//! ```rust,ignore
//! #[myko_item]
//! pub struct BundleStatus {
//!     #[ensure_for(Session)]
//!     pub session_id: Arc<str>,
//!     #[ensure_for(Bundle)]
//!     pub bundle_id: Arc<str>,
//!     #[default_value(false)]
//!     pub armed: bool,
//! }
//! ```
//!
//! **Cascade behavior:**
//! - New `Session` or `Bundle` created → `BundleStatus` entities created for all combinations
//! - On startup: create missing `BundleStatus` for all `Session × Bundle` combinations
//!
//! # Lifecycle
//!
//! 1. **Startup**: `RelationshipManager` is spawned and subscribes to `EventBus`
//! 2. **Kafka catchup**: Wait for all events to be replayed from Kafka
//! 3. **EstablishRelations**: Clean up orphans and initialize EnsureFor entities
//! 4. **Runtime**: Process events and apply cascade logic
//!
//! # Event Filtering
//!
//! The manager only processes events that:
//! - Originate from this server (`source_id == host_id`)
//! - Don't have `prevent_relationship_updates` option set (prevents infinite loops)
//!
//! # Transaction Propagation
//!
//! All cascaded events share the same transaction ID (`tx`) as the triggering event,
//! enabling traceability of related changes.

use crate::{
    actors::event::{
        common::ProcessEventData,
        event_manager::EventManagerMsg,
        EventPublisher,
    },
    event::{EventOptions, MEvent, MEventType},
    prelude::AnyItem,
    relationship::{Relation, RelationRegistration},
    server::MykoServerCtx,
};
use log::{debug, error, info, trace, warn};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

/// Actor that handles entity relationship cascades.
///
/// The RelationshipManager discovers relationships via [`inventory`] at startup,
/// builds lookup indexes for efficient cascade processing, and subscribes to
/// the [`EventBus`](crate::actors::event::EventBus) to receive events.
///
/// See the [module documentation](self) for details on relationship types and behavior.
pub struct RelationshipManager;

/// Internal state for the RelationshipManager actor.
///
/// Contains lookup indexes built from [`RelationRegistration`] entries discovered
/// via [`inventory`]. These indexes enable O(1) lookup of which cascades to apply
/// for any given event.
pub struct RelationshipManagerState {
    ctx: Arc<MykoServerCtx>,
    event_manager: ActorRef<EventManagerMsg>,

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

/// Lookup info for BelongsTo cascades
#[derive(Clone)]
struct BelongsToLookup {
    local_type: &'static str,
    local_key_json: &'static str,
    foreign_type: &'static str,
}

/// Lookup info for OwnsMany cascades
#[derive(Clone)]
struct OwnsManyLookup {
    local_type: &'static str,
    local_key_json: &'static str,
    foreign_type: &'static str,
}

/// Lookup info for EnsureFor cascades
#[derive(Clone)]
struct EnsureForLookup {
    local_type: &'static str,
    dependencies: Vec<(&'static str, &'static str, &'static str)>, // (foreign_type, local_key, local_key_json)
    make_default: fn() -> Value,
}

/// Messages for the RelationshipManager actor.
pub enum RelationshipManagerMsg {
    /// Process an event for potential relationship cascades.
    ///
    /// The manager will check if the event triggers any BelongsTo, OwnsMany,
    /// or EnsureFor cascades based on the event's item type and change type.
    ///
    /// Events are filtered by:
    /// - `source_id`: Only events from this server are processed
    /// - `prevent_relationship_updates`: Events with this flag are skipped
    ///
    /// The `ProcessEventData` includes the parsed item when available, avoiding
    /// re-parsing of JSON for cascade operations.
    ProcessEvent(ProcessEventData),

    /// Establish all relations on startup (called after Kafka catchup).
    ///
    /// This performs:
    /// 1. **BelongsTo orphan cleanup**: Delete children pointing to non-existent parents
    /// 2. **OwnsMany orphan cleanup**: Delete children not referenced by any parent
    /// 3. **EnsureFor initialization**: Create missing entities for all dependency combinations
    ///
    /// The reply port signals completion so the server can proceed with WebSocket setup.
    EstablishRelations(RpcReplyPort<()>),
}

/// Arguments for spawning the RelationshipManager actor.
pub struct RelationshipManagerArgs {
    /// Server context containing host ID for event filtering.
    pub ctx: Arc<MykoServerCtx>,
    /// Reference to EventManager for publishing cascade events and querying entities.
    pub event_manager: ActorRef<EventManagerMsg>,
}

impl Actor for RelationshipManager {
    type Msg = RelationshipManagerMsg;
    type State = RelationshipManagerState;
    type Arguments = RelationshipManagerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        trace!("RelationshipManager: Initializing");

        // Build lookup indexes from inventory registrations
        let mut belongs_to_by_foreign: HashMap<&'static str, Vec<BelongsToLookup>> = HashMap::new();
        let mut belongs_to_by_local: HashMap<&'static str, Vec<BelongsToLookup>> = HashMap::new();
        let mut owns_many_by_local: HashMap<&'static str, Vec<OwnsManyLookup>> = HashMap::new();
        let mut owns_many_by_foreign: HashMap<&'static str, Vec<OwnsManyLookup>> = HashMap::new();
        let mut ensure_for_by_dependency: HashMap<&'static str, Vec<EnsureForLookup>> =
            HashMap::new();

        for registration in inventory::iter::<RelationRegistration> {
            match &registration.relation {
                Relation::BelongsTo {
                    local_type,
                    local_key_json,
                    foreign_type,
                    ..
                } => {
                    trace!(
                        "RelationshipManager: Registered BelongsTo {} -> {} (via {})",
                        local_type, foreign_type, local_key_json
                    );
                    let lookup = BelongsToLookup {
                        local_type,
                        local_key_json,
                        foreign_type,
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
                    local_key,
                    local_key_json,
                    foreign_type,
                } => {
                    trace!(
                        "RelationshipManager: Registered OwnsMany {} ->> {} (via {})",
                        local_type, foreign_type, local_key
                    );
                    let lookup = OwnsManyLookup {
                        local_type,
                        local_key_json,
                        foreign_type,
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
                    make_default,
                } => {
                    trace!(
                        "RelationshipManager: Registered EnsureFor {} for {:?}",
                        local_type,
                        dependencies
                            .iter()
                            .map(|d| d.foreign_type)
                            .collect::<Vec<_>>()
                    );
                    let deps: Vec<_> = dependencies
                        .iter()
                        .map(|d| (d.foreign_type, d.local_key, d.local_key_json))
                        .collect();

                    // Index by each dependency type
                    for dep in dependencies.iter() {
                        ensure_for_by_dependency
                            .entry(dep.foreign_type)
                            .or_default()
                            .push(EnsureForLookup {
                                local_type,
                                dependencies: deps.clone(),
                                make_default: *make_default,
                            });
                    }
                }
            }
        }

        let relation_count = belongs_to_by_foreign.len()
            + owns_many_by_local.len()
            + ensure_for_by_dependency.len();
        debug!(
            "RelationshipManager: {} relation types",
            relation_count
        );

        Ok(RelationshipManagerState {
            ctx: args.ctx,
            event_manager: args.event_manager,
            belongs_to_by_foreign,
            belongs_to_by_local,
            owns_many_by_local,
            owns_many_by_foreign,
            ensure_for_by_dependency,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            RelationshipManagerMsg::ProcessEvent(data) => {
                let event = &data.event;

                // Only process events originating from this server
                if let Some(ref source_id) = event.source_id
                    && source_id != &state.ctx.host_id.to_string()
                {
                    trace!(
                        "RelationshipManager: Skipping event from other server: {}",
                        source_id
                    );
                    return Ok(());
                }

                // Skip if prevent_relationship_updates is set
                if event.prevent_relationship_updates() {
                    trace!(
                        "RelationshipManager: Skipping event with prevent_relationship_updates"
                    );
                    return Ok(());
                }

                let item_type = event.item_type();

                match event.change_type() {
                    MEventType::DEL => {
                        // Handle BelongsTo cascades (parent deleted → delete children)
                        self.handle_belongs_to_cascade(state, event, data.parsed_item.as_ref())
                            .await?;

                        // Handle OwnsMany parent deleted → delete owned children
                        self.handle_owns_many_parent_delete(state, event, data.parsed_item.as_ref())
                            .await?;

                        // Handle OwnsMany child deleted → update parent arrays
                        self.handle_owns_many_child_delete(state, event).await?;
                    }
                    MEventType::SET => {
                        // Handle EnsureFor (dependency created → ensure derived entities exist)
                        if state.ensure_for_by_dependency.contains_key(item_type.as_str()) {
                            self.handle_ensure_for(state, event).await?;
                        }
                    }
                }

                Ok(())
            }
            RelationshipManagerMsg::EstablishRelations(reply) => {
                trace!("RelationshipManager: Establishing relations on startup");

                // 1. Orphan cleanup for BelongsTo relationships (child points to non-existent parent)
                self.cleanup_belongs_to_orphans(state).await?;

                // 2. Orphan cleanup for OwnsMany relationships (child not in any parent's array)
                self.cleanup_owns_many_orphans(state).await?;

                // 3. EnsureFor initialization
                self.initialize_ensure_for(state).await?;

                trace!("RelationshipManager: Relations established");
                if let Err(err) = reply.send(()) {
                    error!(
                        "RelationshipManager: Failed to reply establish complete: {}",
                        err
                    );
                }
                Ok(())
            }
        }
    }
}

impl RelationshipManager {
    /// Handle BelongsTo cascades: when a parent is deleted, delete all children
    async fn handle_belongs_to_cascade(
        &self,
        state: &RelationshipManagerState,
        event: &MEvent,
        _parsed_item: Option<&Arc<dyn AnyItem>>,
    ) -> Result<(), ActorProcessingErr> {
        let item_type = event.item_type();
        let Some(lookups) = state.belongs_to_by_foreign.get(item_type.as_str()) else {
            return Ok(());
        };

        let parent_id = event.item.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let tx = &event.tx;

        for lookup in lookups {
            // Query children by foreign key (returns parsed items)
            let children = self
                .query_by_field(state, lookup.local_type, lookup.local_key_json, parent_id)
                .await;

            for child in children {
                trace!(
                    "RelationshipManager: Cascade delete {} {} (parent {} deleted)",
                    lookup.local_type,
                    child.id(),
                    parent_id
                );
                self.publish_del_with_item(state, lookup.local_type, child, tx)
                    .await;
            }
        }

        Ok(())
    }

    /// Handle OwnsMany parent delete: delete all owned children
    async fn handle_owns_many_parent_delete(
        &self,
        state: &RelationshipManagerState,
        event: &MEvent,
        _parsed_item: Option<&Arc<dyn AnyItem>>,
    ) -> Result<(), ActorProcessingErr> {
        let item_type = event.item_type();
        let Some(lookups) = state.owns_many_by_local.get(item_type.as_str()) else {
            return Ok(());
        };

        let tx = &event.tx;

        for lookup in lookups {
            // Get child IDs from the parent's array field
            let child_ids = event
                .item
                .get(lookup.local_key_json)
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(Arc::from))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            if child_ids.is_empty() {
                continue;
            }

            // Get and delete each child (returns parsed items)
            let children = self
                .get_by_ids(state, lookup.foreign_type, &child_ids)
                .await;
            for child in children {
                trace!(
                    "RelationshipManager: Cascade delete owned {} {}",
                    lookup.foreign_type,
                    child.id()
                );
                self.publish_del_with_item(state, lookup.foreign_type, child, tx)
                    .await;
            }
        }

        Ok(())
    }

    /// Handle OwnsMany child delete: remove child ID from parent arrays
    async fn handle_owns_many_child_delete(
        &self,
        state: &RelationshipManagerState,
        event: &MEvent,
    ) -> Result<(), ActorProcessingErr> {
        let item_type = event.item_type();
        let Some(lookups) = state.owns_many_by_foreign.get(item_type.as_str()) else {
            return Ok(());
        };

        let child_id = event.item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let tx = &event.tx;

        for lookup in lookups {
            // Find parents that contain this child ID in their array
            let parents = self
                .query_array_contains(state, lookup.local_type, lookup.local_key_json, &child_id)
                .await;

            for parent_item in parents {
                // Convert to Value for modification (we can't modify Arc<dyn AnyItem> in place)
                let mut parent = parent_item.to_value();
                if let Some(arr) = parent
                    .get_mut(lookup.local_key_json)
                    .and_then(|v| v.as_array_mut())
                {
                    // Remove the child ID from the array
                    let original_len = arr.len();
                    arr.retain(|v| v.as_str() != Some(&child_id));

                    if arr.len() != original_len {
                        trace!(
                            "RelationshipManager: Updating {} to remove {} from {}",
                            lookup.local_type, child_id, lookup.local_key_json
                        );
                        // Publish updated parent (hash will be recalculated, no parsed item since modified)
                        self.publish_set(state, lookup.local_type, parent, tx)
                            .await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle EnsureFor: when dependency created, ensure derived entities exist
    async fn handle_ensure_for(
        &self,
        state: &RelationshipManagerState,
        event: &MEvent,
    ) -> Result<(), ActorProcessingErr> {
        let item_type = event.item_type();
        let Some(lookups) = state.ensure_for_by_dependency.get(item_type.as_str()) else {
            return Ok(());
        };

        let tx = &event.tx;

        for lookup in lookups {
            // Get all combinations of dependency entities
            let combinations = self.get_dependency_combinations(state, &lookup.dependencies).await;

            for combo in combinations {
                // Check if derived entity already exists
                let existing = self
                    .find_ensure_for_entity(state, lookup.local_type, &lookup.dependencies, &combo)
                    .await;

                if existing.is_none() {
                    // Create the derived entity
                    let mut entity = (lookup.make_default)();

                    // Set the foreign keys
                    if let Some(obj) = entity.as_object_mut() {
                        for ((_, _, local_key_json), dep_id) in
                            lookup.dependencies.iter().zip(combo.iter())
                        {
                            obj.insert((*local_key_json).to_string(), Value::String(dep_id.to_string()));
                        }

                        // Generate a unique ID
                        let id = Uuid::new_v4().to_string();
                        obj.insert("id".to_string(), Value::String(id.clone()));
                        obj.insert("hash".to_string(), Value::String(String::new()));

                        trace!(
                            "RelationshipManager: Creating ensured {} for {:?}",
                            lookup.local_type, combo
                        );

                        self.publish_set(state, lookup.local_type, entity, tx).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Cleanup orphaned children for BelongsTo relationships (child points to non-existent parent)
    async fn cleanup_belongs_to_orphans(
        &self,
        state: &RelationshipManagerState,
    ) -> Result<(), ActorProcessingErr> {
        let tx = Uuid::new_v4().to_string();

        for (child_type, lookups) in &state.belongs_to_by_local {
            for lookup in lookups {
                // Get all parent IDs that exist
                let parents = self.get_all_items(state, lookup.foreign_type).await;
                let parent_ids: HashSet<String> =
                    parents.iter().map(|p| p.id().to_string()).collect();

                // Get all children and find orphans (those pointing to non-existent parents)
                let children = self.get_all_items(state, child_type).await;
                let mut orphan_count = 0;

                for child in children {
                    let child_value = child.to_value();
                    if let Some(fk_value) = child_value
                        .get(lookup.local_key_json)
                        .and_then(|v| v.as_str())
                    {
                        if !parent_ids.contains(fk_value) {
                            trace!(
                                "RelationshipManager: Deleting BelongsTo orphan {} {} (parent {} {} not found)",
                                child_type,
                                child.id(),
                                lookup.foreign_type,
                                fk_value
                            );
                            self.publish_del_with_item(state, child_type, child, &tx)
                                .await;
                            orphan_count += 1;
                        }
                    }
                }

                if orphan_count > 0 {
                    info!(
                        "RelationshipManager: Cleaned up {} orphan {} entities (missing {} parents)",
                        orphan_count, child_type, lookup.foreign_type
                    );
                }
            }
        }

        Ok(())
    }

    /// Cleanup orphaned children for OwnsMany relationships (child not in any parent's array)
    async fn cleanup_owns_many_orphans(
        &self,
        state: &RelationshipManagerState,
    ) -> Result<(), ActorProcessingErr> {
        let tx = Uuid::new_v4().to_string();

        for (parent_type, lookups) in &state.owns_many_by_local {
            for lookup in lookups {
                // Get all child IDs referenced by parents
                let parents = self.get_all_items(state, parent_type).await;
                let mut referenced_ids: HashSet<String> = HashSet::new();

                for parent in &parents {
                    let parent_value = parent.to_value();
                    if let Some(arr) = parent_value
                        .get(lookup.local_key_json)
                        .and_then(|v| v.as_array())
                    {
                        for id in arr.iter().filter_map(|v| v.as_str()) {
                            referenced_ids.insert(id.to_string());
                        }
                    }
                }

                // Get all children and find orphans
                let children = self.get_all_items(state, lookup.foreign_type).await;
                let mut orphan_count = 0;

                for child in children {
                    let child_id = child.id();
                    if !referenced_ids.contains(&child_id.to_string()) {
                        trace!(
                            "RelationshipManager: Deleting OwnsMany orphan {} {}",
                            lookup.foreign_type, child_id
                        );
                        self.publish_del_with_item(state, lookup.foreign_type, child, &tx)
                            .await;
                        orphan_count += 1;
                    }
                }

                if orphan_count > 0 {
                    info!(
                        "RelationshipManager: Cleaned up {} orphan {} entities (OwnsMany)",
                        orphan_count, lookup.foreign_type
                    );
                }
            }
        }

        Ok(())
    }

    /// Initialize EnsureFor relationships (create missing derived entities)
    async fn initialize_ensure_for(
        &self,
        state: &RelationshipManagerState,
    ) -> Result<(), ActorProcessingErr> {
        let tx = Uuid::new_v4().to_string();

        // Get unique EnsureFor lookups (avoid duplicates from multiple dependency indexes)
        let mut processed: HashSet<&'static str> = HashSet::new();

        for lookups in state.ensure_for_by_dependency.values() {
            for lookup in lookups {
                if processed.contains(lookup.local_type) {
                    continue;
                }
                processed.insert(lookup.local_type);

                // Get all combinations of dependency entities
                let combinations = self
                    .get_dependency_combinations(state, &lookup.dependencies)
                    .await;

                let mut created_count = 0;

                for combo in combinations {
                    // Check if derived entity already exists
                    let existing = self
                        .find_ensure_for_entity(
                            state,
                            lookup.local_type,
                            &lookup.dependencies,
                            &combo,
                        )
                        .await;

                    if existing.is_none() {
                        // Create the derived entity
                        let mut entity = (lookup.make_default)();

                        if let Some(obj) = entity.as_object_mut() {
                            for ((_, _, local_key_json), dep_id) in
                                lookup.dependencies.iter().zip(combo.iter())
                            {
                                obj.insert(
                                    (*local_key_json).to_string(),
                                    Value::String(dep_id.to_string()),
                                );
                            }

                            let id = Uuid::new_v4().to_string();
                            obj.insert("id".to_string(), Value::String(id));
                            obj.insert("hash".to_string(), Value::String(String::new()));

                            self.publish_set(state, lookup.local_type, entity, &tx).await;
                            created_count += 1;
                        }
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

    /// Get all combinations of dependency entity IDs for EnsureFor
    async fn get_dependency_combinations(
        &self,
        state: &RelationshipManagerState,
        dependencies: &[(&'static str, &'static str, &'static str)],
    ) -> Vec<Vec<String>> {
        if dependencies.is_empty() {
            return vec![];
        }

        // Get IDs for each dependency type
        let mut dep_ids: Vec<Vec<String>> = Vec::new();

        for (foreign_type, _, _) in dependencies {
            let items = self.get_all_items(state, foreign_type).await;
            let ids: Vec<String> = items.iter().map(|item| item.id().to_string()).collect();
            dep_ids.push(ids);
        }

        // Compute Cartesian product
        self.cartesian_product(&dep_ids)
    }

    /// Compute Cartesian product of multiple ID sets
    fn cartesian_product(&self, sets: &[Vec<String>]) -> Vec<Vec<String>> {
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
    async fn find_ensure_for_entity(
        &self,
        state: &RelationshipManagerState,
        local_type: &str,
        dependencies: &[(&'static str, &'static str, &'static str)],
        combo: &[String],
    ) -> Option<Arc<dyn AnyItem>> {
        // For the first dependency, query by that field
        if dependencies.is_empty() || combo.is_empty() {
            return None;
        }

        let (_, _, first_key_json) = dependencies[0];
        let first_value = &combo[0];

        let candidates = self
            .query_by_field(state, local_type, first_key_json, first_value)
            .await;

        // Filter by remaining dependencies
        for candidate in candidates {
            let candidate_value = candidate.to_value();
            let mut matches = true;
            for (i, (_, _, key_json)) in dependencies.iter().enumerate() {
                if let Some(field_value) = candidate_value.get(*key_json).and_then(|v| v.as_str()) {
                    if field_value != combo[i] {
                        matches = false;
                        break;
                    }
                } else {
                    matches = false;
                    break;
                }
            }
            if matches {
                return Some(candidate);
            }
        }

        None
    }

    // Helper methods for querying via EventManager

    async fn query_by_field(
        &self,
        state: &RelationshipManagerState,
        entity_type: &str,
        field: &str,
        value: &str,
    ) -> Vec<Arc<dyn AnyItem>> {
        match ractor::call!(
            state.event_manager,
            EventManagerMsg::QueryByField,
            Arc::from(entity_type),
            field.to_string(),
            value.to_string()
        ) {
            Ok(results) => results,
            Err(err) => {
                warn!(
                    "RelationshipManager: Failed to query {} by {}: {}",
                    entity_type, field, err
                );
                vec![]
            }
        }
    }

    async fn query_array_contains(
        &self,
        state: &RelationshipManagerState,
        entity_type: &str,
        field: &str,
        value: &str,
    ) -> Vec<Arc<dyn AnyItem>> {
        match ractor::call!(
            state.event_manager,
            EventManagerMsg::QueryArrayContains,
            Arc::from(entity_type),
            field.to_string(),
            value.to_string()
        ) {
            Ok(results) => results,
            Err(err) => {
                warn!(
                    "RelationshipManager: Failed to query {} array contains: {}",
                    entity_type, err
                );
                vec![]
            }
        }
    }

    async fn get_by_ids(
        &self,
        state: &RelationshipManagerState,
        entity_type: &str,
        ids: &[Arc<str>],
    ) -> Vec<Arc<dyn AnyItem>> {
        match ractor::call!(
            state.event_manager,
            EventManagerMsg::GetByIds,
            Arc::from(entity_type),
            ids.to_vec()
        ) {
            Ok(results) => results,
            Err(err) => {
                warn!(
                    "RelationshipManager: Failed to get {} by ids: {}",
                    entity_type, err
                );
                vec![]
            }
        }
    }

    async fn get_all_items(
        &self,
        state: &RelationshipManagerState,
        entity_type: &str,
    ) -> Vec<Arc<dyn AnyItem>> {
        match ractor::call!(
            state.event_manager,
            EventManagerMsg::GetAllItems,
            Arc::from(entity_type)
        ) {
            Ok(results) => results,
            Err(err) => {
                warn!(
                    "RelationshipManager: Failed to get all {}: {}",
                    entity_type, err
                );
                vec![]
            }
        }
    }

    /// Get cascade event options (prevent_relationship_updates to avoid loops)
    fn cascade_options() -> EventOptions {
        EventOptions {
            prevent_relationship_updates: true,
            ..Default::default()
        }
    }

    /// Get an EventPublisher for cascade events
    fn publisher(state: &RelationshipManagerState) -> EventPublisher {
        EventPublisher::new(state.event_manager.clone(), state.ctx.host_id)
    }

    /// Publish a SET event. For modified items where we don't have a parsed representation.
    async fn publish_set(
        &self,
        state: &RelationshipManagerState,
        entity_type: &str,
        item: Value,
        tx: &str,
    ) {
        if let Err(err) = Self::publisher(state).publish_set_value(
            entity_type,
            item,
            tx,
            Some(Self::cascade_options()),
        ) {
            error!(
                "RelationshipManager: Failed to publish SET event: {}",
                err
            );
        }
    }

    /// Publish a DEL event with the parsed item for efficient downstream processing.
    async fn publish_del_with_item(
        &self,
        state: &RelationshipManagerState,
        entity_type: &str,
        item: Arc<dyn AnyItem>,
        tx: &str,
    ) {
        if let Err(err) = Self::publisher(state).publish_del_item(
            entity_type,
            item,
            tx,
            Some(Self::cascade_options()),
        ) {
            error!(
                "RelationshipManager: Failed to publish DEL event: {}",
                err
            );
        }
    }
}
