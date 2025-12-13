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
//! ```rust,ignore
//! use myko_rs::prelude::*;
//!
//! // BelongsTo: Binding has FK to Scene
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

use serde_json::Value;

/// Represents the different types of entity relationships
#[derive(Debug, Clone)]
pub enum Relation {
    /// Child entity has a foreign key pointing to parent.
    /// When parent is DEL'd -> cascade delete all children with matching FK.
    ///
    /// Example: `Binding.scope_id` belongs to `Scene.id`
    /// When Scene is deleted, all Bindings with that scope_id are deleted.
    BelongsTo {
        /// Entity type that has the foreign key (child)
        local_type: &'static str,
        /// Field name on local entity holding the FK (snake_case)
        local_key: &'static str,
        /// Field name in JSON (camelCase)
        local_key_json: &'static str,
        /// Entity type being referenced (parent)
        foreign_type: &'static str,
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
        /// Field name on local entity holding child IDs (snake_case)
        local_key: &'static str,
        /// Field name in JSON (camelCase)
        local_key_json: &'static str,
        /// Entity type being owned (child)
        foreign_type: &'static str,
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
        /// Factory function to create default entity value
        make_default: fn() -> Value,
    },
}

/// A dependency for EnsureFor relationships
#[derive(Debug, Clone, Copy)]
pub struct EnsureForDependency {
    /// Entity type of the dependency
    pub foreign_type: &'static str,
    /// Field on local entity pointing to dependency (snake_case)
    pub local_key: &'static str,
    /// Field name in JSON (camelCase)
    pub local_key_json: &'static str,
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
/// ```rust,ignore
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relation_variants() {
        let belongs_to = Relation::BelongsTo {
            local_type: "Binding",
            local_key: "scope_id",
            local_key_json: "scopeId",
            foreign_type: "Scene",
        };

        let owns_many = Relation::OwnsMany {
            local_type: "Scene",
            local_key: "node_ids",
            local_key_json: "nodeIds",
            foreign_type: "BindingNode",
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
