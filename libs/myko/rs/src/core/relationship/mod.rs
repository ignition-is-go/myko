//! Entity Relationship System for automatic cascade operations.
//!
//! This module defines the relationship types that can be declared on entity fields
//! using the `#[belongs_to]`, `#[owns_many]`, and `#[ensure_for]` attribute macros.
//! Relationships are registered at compile time via [`inventory`] and processed
//! by the [`RelationshipManager`](crate::actors::relationship::RelationshipManager).
//!
//! # Relationship Types
//!
//! | Type | Direction | On Parent DEL | On Child DEL |
//! |------|-----------|---------------|--------------|
//! | [`BelongsTo`](Relation::BelongsTo) | Child → Parent (FK) | Delete children | N/A |
//! | [`OwnsMany`](Relation::OwnsMany) | Parent → Children (array) | Delete children | Update parent array |
//! | [`EnsureFor`](Relation::EnsureFor) | Entity × Dependencies | N/A | N/A (on SET: ensure exists) |
//!
//! # Usage
//!
//! Relationships are declared using attribute macros on `#[myko_item]` entities:
//!
//! ```text
//! use myko_rs::prelude::*;
//! use std::sync::Arc;
//!
//! // BelongsTo: Binding has FK to Scene
//! #[myko_item]
//! pub struct BindingNode {
//!     pub name: String,
//! }
//!
//! #[myko_item]
//! pub struct Binding {
//!     #[belongs_to(Scene)]
//!     pub scope_id: Arc<str>,
//! }
//!
//! // OwnsMany: Scene owns array of BindingNode IDs
//! #[myko_item]
//! pub struct Scene {
//!     pub name: String,
//!     #[owns_many(BindingNode)]
//!     pub node_ids: Vec<Arc<str>>,
//! }
//!
//! // EnsureFor: Create one BundleStatus per Session×Bundle combination
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
//!     #[default_value(false)]
//!     pub armed: bool,
//! }
//! ```
//!
//! # Registration
//!
//! The `#[myko_item]` macro automatically generates [`RelationRegistration`] entries
//! that are collected via [`inventory`]. Use [`iter_relations`] to enumerate all
//! registered relationships at runtime.
//!
//! # Orphan Cleanup
//!
//! On server startup, the [`RelationshipManager`](crate::actors::relationship::RelationshipManager)
//! performs orphan cleanup:
//! - **BelongsTo**: Delete children whose FK points to a non-existent parent
//! - **OwnsMany**: Delete children not referenced in any parent's array

use std::sync::Arc;

use super::item::AnyItem;

/// Type alias for a function that extracts an FK value from an entity
pub type FkExtractor = fn(&dyn std::any::Any) -> Option<Arc<str>>;

/// Type alias for a function that extracts an array of IDs from an entity
pub type ArrayExtractor = fn(&dyn std::any::Any) -> Option<Vec<Arc<str>>>;

/// Type alias for a function that removes an ID from a parent's array and returns the updated entity
pub type ArrayRemover = fn(&dyn std::any::Any, &str) -> Option<Arc<dyn AnyItem>>;

/// Type alias for a function that creates an EnsureFor entity with dependency IDs populated
pub type EntityFactory = fn(&[Arc<str>]) -> Arc<dyn AnyItem>;

/// Represents the different types of entity relationships
#[derive(Clone)]
pub enum Relation {
    /// Child entity has a foreign key pointing to parent.
    /// When parent is DEL'd -> cascade delete all children with matching FK.
    ///
    /// Example: `Binding.scope_id` belongs to `Scene.id`
    /// When Scene is deleted, all Bindings with that scope_id are deleted.
    BelongsTo {
        /// Entity type that has the foreign key (child)
        local_type: &'static str,
        /// Entity type being referenced (parent)
        foreign_type: &'static str,
        /// The camelCase JSON field name of the FK (e.g., "scopeId", "projectId")
        fk_field_json: &'static str,
        /// Function to extract the FK value from a child entity
        extract_fk: FkExtractor,
        /// If true, this child type is excluded from entity tree exports.
        /// The relationship still cascades on delete — only tree traversal is affected.
        exclude_from_tree: bool,
    },

    /// Parent entity owns an array of child IDs.
    /// When parent DEL'd -> cascade delete all referenced children.
    /// When child DEL'd -> remove child ID from parent's array, recalculate hash.
    ///
    /// Example: `Scene.node_ids` owns `BindingNode` entities
    /// When Scene is deleted, all BindingNodes in node_ids are deleted.
    /// When a BindingNode is deleted, it's removed from the owning Scene's node_ids.
    OwnsMany {
        /// Entity type that owns the array (parent)
        local_type: &'static str,
        /// Entity type being owned (child)
        foreign_type: &'static str,
        /// Function to extract owned IDs from a parent entity
        extract_ids: ArrayExtractor,
        /// Function to remove an ID from parent and return updated entity as Value
        remove_id: ArrayRemover,
        /// If true, exclude this child type from entity tree exports.
        exclude_from_tree: bool,
    },

    /// Auto-create entity for each combination of dependencies.
    /// When any dependency is SET -> ensure local entity exists for all combinations.
    ///
    /// Example: `BundleStatus` ensure-for `(Session, Bundle)`
    /// Creates one BundleStatus per Session×Bundle combination.
    EnsureFor {
        /// Entity type to auto-create
        local_type: &'static str,
        /// Dependencies to create Cartesian product from
        dependencies: &'static [EnsureForDependency],
        /// Factory function to create entity with dependency IDs populated.
        /// Takes slice of dependency IDs in same order as dependencies array.
        /// Returns complete entity with id, hash, FK fields, and default values.
        make_entity: EntityFactory,
        /// If true, exclude this entity type from entity tree exports.
        exclude_from_tree: bool,
    },
}

impl std::fmt::Debug for Relation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Relation::BelongsTo {
                local_type,
                foreign_type,
                ..
            } => f
                .debug_struct("BelongsTo")
                .field("local_type", local_type)
                .field("foreign_type", foreign_type)
                .finish(),
            Relation::OwnsMany {
                local_type,
                foreign_type,
                ..
            } => f
                .debug_struct("OwnsMany")
                .field("local_type", local_type)
                .field("foreign_type", foreign_type)
                .finish(),
            Relation::EnsureFor {
                local_type,
                dependencies,
                ..
            } => f
                .debug_struct("EnsureFor")
                .field("local_type", local_type)
                .field("dependencies", dependencies)
                .finish(),
        }
    }
}

/// A dependency for EnsureFor relationships
#[derive(Clone, Copy)]
pub struct EnsureForDependency {
    /// Entity type of the dependency
    pub foreign_type: &'static str,
    /// Function to extract the FK value from the local entity
    pub extract_fk: FkExtractor,
}

impl std::fmt::Debug for EnsureForDependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnsureForDependency")
            .field("foreign_type", &self.foreign_type)
            .finish()
    }
}

/// Registration entry for relationship discovery via inventory
pub struct RelationRegistration {
    pub relation: Relation,
}

// Collect all relationship registrations at compile time
inventory::collect!(RelationRegistration);

/// Iterator over all registered relationships
pub fn iter_relations() -> impl Iterator<Item = &'static RelationRegistration> {
    inventory::iter::<RelationRegistration>()
}

/// Registration for entities that have a client_id field.
/// When present, the server will auto-populate this field with the client_id
/// of the WebSocket connection that sent the event.
///
/// # Example
///
/// ```text
/// use myko_rs::prelude::*;
///
/// #[myko_item]
/// pub struct Instance {
///     #[myko_client_id]
///     pub client_id: Option<String>,
/// }
/// ```
pub struct ClientIdRegistration {
    /// Entity type that has the client_id field
    pub entity_type: &'static str,
    /// Field name in JSON (camelCase)
    pub field_name_json: &'static str,
}

// Collect all client_id registrations at compile time
inventory::collect!(ClientIdRegistration);

/// Iterator over all registered client_id fields
pub fn iter_client_id_registrations() -> impl Iterator<Item = &'static ClientIdRegistration> {
    inventory::iter::<ClientIdRegistration>()
}

/// Registration for a field that should fall back to the entity's own `id` if null/missing.
///
/// When a SET event is processed, if this field is null or absent in the JSON,
/// it will be populated with the entity's `id` value before parsing.
///
/// # Example
///
/// ```text
/// use myko_rs::prelude::*;
///
/// #[myko_item]
/// pub struct Instance {
///     #[fallback_to_id]
///     pub cluster_id: Option<String>,
/// }
/// ```
pub struct FallbackToIdRegistration {
    /// Entity type that has the fallback field
    pub entity_type: &'static str,
    /// Field name in JSON (camelCase)
    pub field_name_json: &'static str,
}

// Collect all fallback_to_id registrations at compile time
inventory::collect!(FallbackToIdRegistration);

/// Iterator over all registered fallback_to_id fields
pub fn iter_fallback_to_id_registrations() -> impl Iterator<Item = &'static FallbackToIdRegistration>
{
    inventory::iter::<FallbackToIdRegistration>()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Dummy extractors for testing
    fn dummy_fk_extractor(_: &dyn std::any::Any) -> Option<Arc<str>> {
        None
    }

    fn dummy_array_extractor(_: &dyn std::any::Any) -> Option<Vec<Arc<str>>> {
        None
    }

    fn dummy_array_remover(_: &dyn std::any::Any, _: &str) -> Option<Arc<dyn AnyItem>> {
        None
    }

    #[test]
    fn test_relation_variants() {
        let belongs_to = Relation::BelongsTo {
            local_type: "Binding",
            foreign_type: "Scene",
            fk_field_json: "scopeId",
            extract_fk: dummy_fk_extractor,
            exclude_from_tree: false,
        };

        let owns_many = Relation::OwnsMany {
            local_type: "Scene",
            foreign_type: "BindingNode",
            extract_ids: dummy_array_extractor,
            remove_id: dummy_array_remover,
            exclude_from_tree: false,
        };

        // Verify we can match on variants
        match belongs_to {
            Relation::BelongsTo { foreign_type, .. } => {
                assert_eq!(foreign_type, "Scene");
            }
            _ => panic!("Expected BelongsTo"),
        }

        match owns_many {
            Relation::OwnsMany { foreign_type, .. } => {
                assert_eq!(foreign_type, "BindingNode");
            }
            _ => panic!("Expected OwnsMany"),
        }
    }
}
