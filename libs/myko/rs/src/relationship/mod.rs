//! Entity Relationship System
//!
//! Provides relationship cascades for Myko entities:
//! - `BelongsTo`: Child has FK to parent. Parent DEL → cascade delete children
//! - `OwnsMany`: Parent has array of child IDs. Parent DEL → delete children. Child DEL → update parent
//! - `EnsureFor`: Auto-create entity for each dependency combination

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
