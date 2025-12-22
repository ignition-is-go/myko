//! EntityStore - Reactive entity storage backed by hypha CellMap
//!
//! Provides a thread-safe, reactive store for entities that automatically
//! propagates changes to all subscribers.

use std::sync::Arc;

use hypha::{Cell, CellImmutable, CellMap, Gettable, MapDiff};

use crate::parsers::item::AnyItem;

/// A reactive entity store backed by hypha's CellMap.
///
/// Thread-safe via DashMap internally. Changes propagate automatically
/// to all cell subscribers.
///
/// # Example
/// ```ignore
/// let store = EntityStore::new();
///
/// // Insert an entity
/// store.set(id, item);
///
/// // Subscribe to changes
/// let diffs = store.diffs();
/// diffs.subscribe(|signal| {
///     // Handle insert/update/remove
/// });
/// ```
pub struct EntityStore {
    inner: CellMap<Arc<str>, Arc<dyn AnyItem>>,
}

impl EntityStore {
    /// Create a new empty entity store.
    pub fn new() -> Self {
        Self {
            inner: CellMap::new(),
        }
    }

    /// Insert or update an entity.
    ///
    /// Returns the previous value if one existed.
    pub fn set(&self, id: Arc<str>, item: Arc<dyn AnyItem>) -> Option<Arc<dyn AnyItem>> {
        self.inner.insert(id, item)
    }

    /// Remove an entity by ID.
    ///
    /// Returns the removed value if one existed.
    pub fn remove(&self, id: &Arc<str>) -> Option<Arc<dyn AnyItem>> {
        self.inner.remove(id)
    }

    /// Get an entity by ID (non-reactive).
    pub fn get(&self, id: &Arc<str>) -> Option<Arc<dyn AnyItem>> {
        self.inner.get_value(id)
    }

    /// Get a reactive cell for diffs (Insert/Update/Remove notifications).
    ///
    /// Subscribe to this cell to receive incremental updates.
    pub fn diffs(&self) -> Cell<Option<MapDiff<Arc<str>, Arc<dyn AnyItem>>>, CellImmutable> {
        self.inner.diffs()
    }

    /// Get a reactive cell containing all entries.
    ///
    /// This cell updates whenever any entity changes.
    pub fn entries(&self) -> Cell<Vec<(Arc<str>, Arc<dyn AnyItem>)>, CellImmutable> {
        self.inner.entries()
    }

    /// Get the number of entities in the store.
    pub fn len(&self) -> usize {
        self.inner.len().get()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Check if an entity exists by ID.
    pub fn contains(&self, id: &Arc<str>) -> bool {
        self.inner.contains_key(id)
    }
}

impl Default for EntityStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::to_value::ToValue;
    use crate::common::with_id::WithId;
    use hypha::{Signal, Watchable};
    use serde_json::Value;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Test entity
    #[derive(Debug, Clone)]
    struct TestEntity {
        id: Arc<str>,
        name: String,
    }

    impl WithId for TestEntity {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl ToValue for TestEntity {
        fn to_value(&self) -> Value {
            serde_json::json!({
                "id": self.id.as_ref(),
                "name": self.name
            })
        }
    }

    impl AnyItem for TestEntity {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn test_entity_store_basic() {
        let store = EntityStore::new();

        let entity = Arc::new(TestEntity {
            id: "test-1".into(),
            name: "Test".to_string(),
        }) as Arc<dyn AnyItem>;

        // Insert
        assert!(store.set("test-1".into(), entity.clone()).is_none());
        assert_eq!(store.len(), 1);

        // Get
        let retrieved = store.get(&"test-1".into()).unwrap();
        assert_eq!(retrieved.id().as_ref(), "test-1");

        // Remove
        assert!(store.remove(&"test-1".into()).is_some());
        assert!(store.is_empty());
    }

    #[test]
    fn test_entity_store_diffs() {
        let store = EntityStore::new();
        let count = Arc::new(AtomicUsize::new(0));

        let diffs = store.diffs();
        let count_clone = count.clone();

        let _guard = diffs.subscribe(move |signal| {
            if let Signal::Value(opt) = signal {
                if opt.is_some() {
                    count_clone.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        // Insert triggers diff
        let entity = Arc::new(TestEntity {
            id: "test-1".into(),
            name: "Test".to_string(),
        }) as Arc<dyn AnyItem>;
        store.set("test-1".into(), entity);

        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Update triggers diff
        let entity2 = Arc::new(TestEntity {
            id: "test-1".into(),
            name: "Updated".to_string(),
        }) as Arc<dyn AnyItem>;
        store.set("test-1".into(), entity2);

        assert_eq!(count.load(Ordering::SeqCst), 2);

        // Remove triggers diff
        store.remove(&"test-1".into());
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_entity_store_entries() {
        let store = EntityStore::new();

        let entity1 = Arc::new(TestEntity {
            id: "a".into(),
            name: "A".to_string(),
        }) as Arc<dyn AnyItem>;

        let entity2 = Arc::new(TestEntity {
            id: "b".into(),
            name: "B".to_string(),
        }) as Arc<dyn AnyItem>;

        store.set("a".into(), entity1);
        store.set("b".into(), entity2);

        let entries = store.entries();
        assert_eq!(entries.get().len(), 2);
    }
}
