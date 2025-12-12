//! Relationship management actors for cascade operations.
//!
//! This module provides the RelationshipManager actor which handles:
//! - BelongsTo cascades: Parent DEL → cascade delete children
//! - OwnsMany cascades: Parent DEL → delete children, Child DEL → update parent arrays
//! - EnsureFor: Auto-create entities for each dependency combination

mod relationship_manager;

pub use relationship_manager::{RelationshipManager, RelationshipManagerArgs, RelationshipManagerMsg};
