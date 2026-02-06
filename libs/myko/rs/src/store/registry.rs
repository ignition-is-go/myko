//! StoreRegistry - Central registry for all entity stores
//!
//! Manages one EntityStore per entity type, providing a unified
//! interface for event processing.

use std::sync::Arc;

use dashmap::DashMap;

use super::EntityStore;

/// Central registry holding all entity stores.
///
/// Thread-safe via DashMap. Automatically creates stores on first access.
///
/// # Example
/// ```ignore
/// let registry = StoreRegistry::new();
///
/// // Get or create store for entity type
/// let targets = registry.get_or_create("Target");
/// targets.insert(id, item);
/// ```
pub struct StoreRegistry {
    stores: DashMap<Arc<str>, Arc<EntityStore>>,
}

impl std::fmt::Debug for StoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreRegistry")
            .field("stores", &format!("({} entries)", self.stores.len()))
            .finish()
    }
}

impl StoreRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            stores: DashMap::new(),
        }
    }

    /// Get or create an entity store for the given type.
    ///
    /// Creates a new store if one doesn't exist for this type.
    pub fn get_or_create(&self, entity_type: &str) -> Arc<EntityStore> {
        let key: Arc<str> = entity_type.into();

        self.stores
            .entry(key)
            .or_insert_with(|| Arc::new(EntityStore::new()))
            .clone()
    }

    /// Get an entity store if it exists.
    pub fn get(&self, entity_type: &str) -> Option<Arc<EntityStore>> {
        self.stores.get(entity_type).map(|r| r.clone())
    }

    /// List all registered entity types.
    pub fn entity_types(&self) -> Vec<Arc<str>> {
        self.stores.iter().map(|r| r.key().clone()).collect()
    }

    /// Get the number of registered entity types.
    pub fn len(&self) -> usize {
        self.stores.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }
}

impl Default for StoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_get_or_create() {
        let registry = StoreRegistry::new();

        // First access creates store
        let store1 = registry.get_or_create("Target");
        assert_eq!(registry.len(), 1);

        // Second access returns same store
        let store2 = registry.get_or_create("Target");
        assert!(Arc::ptr_eq(&store1, &store2));

        // Different type creates new store
        let store3 = registry.get_or_create("Scene");
        assert_eq!(registry.len(), 2);
        assert!(!Arc::ptr_eq(&store1, &store3));
    }

    #[test]
    fn test_registry_get() {
        let registry = StoreRegistry::new();

        // Non-existent returns None
        assert!(registry.get("Target").is_none());

        // After creation, returns Some
        registry.get_or_create("Target");
        assert!(registry.get("Target").is_some());
    }

    #[test]
    fn test_registry_entity_types() {
        let registry = StoreRegistry::new();

        registry.get_or_create("Target");
        registry.get_or_create("Scene");
        registry.get_or_create("Binding");

        let types = registry.entity_types();
        assert_eq!(types.len(), 3);
    }
}
