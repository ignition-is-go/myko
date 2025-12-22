//! Cell-based reactive queries
//!
//! Provides query factory functions that return hypha cells instead of
//! working with the actor-based query system.
//!
//! # Example
//! ```ignore
//! let store = registry.get_or_create("Target");
//!
//! // Query all entities
//! let all = query_all(&store);
//!
//! // Query by IDs
//! let by_ids = query_by_ids(&store, vec!["id-1".into(), "id-2".into()]);
//!
//! // Query with predicate
//! let filtered = query_where(&store, |item| {
//!     // your filter logic
//!     true
//! });
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use hypha::{Cell, CellImmutable, Gettable, MapDiff, MapExt, ScanExt};

use crate::parsers::item::AnyItem;
use crate::store::EntityStore;

/// Create a query cell that returns all entities in the store.
///
/// The cell updates whenever any entity is added, updated, or removed.
pub fn query_all(store: &EntityStore) -> Cell<Vec<Arc<dyn AnyItem>>, CellImmutable> {
    store.entries().map(|entries| {
        entries.iter().map(|(_, item)| item.clone()).collect()
    })
}

/// Create a query cell that filters entities by ID set.
///
/// Returns a cell containing entities whose IDs are in the provided set.
/// The cell updates when matching entities change.
pub fn query_by_ids(
    store: &EntityStore,
    ids: Vec<Arc<str>>,
) -> Cell<Vec<Arc<dyn AnyItem>>, CellImmutable> {
    let id_set: HashSet<Arc<str>> = ids.into_iter().collect();

    // Build initial state from current entries
    let initial: HashMap<Arc<str>, Arc<dyn AnyItem>> = {
        let entries = store.entries();
        let current = entries.get();
        current
            .iter()
            .filter(|(id, _)| id_set.contains(id))
            .map(|(id, item)| (id.clone(), item.clone()))
            .collect()
    };

    // Use scan to maintain HashMap state, then map to Vec
    store
        .diffs()
        .scan(initial, move |state, diff_opt| {
            let mut new_state = state.clone();
            if let Some(diff) = diff_opt {
                match diff {
                    MapDiff::Insert { key, value } => {
                        if id_set.contains(key) {
                            new_state.insert(key.clone(), value.clone());
                        }
                    }
                    MapDiff::Update { key, new_value, .. } => {
                        if id_set.contains(key) {
                            new_state.insert(key.clone(), new_value.clone());
                        }
                    }
                    MapDiff::Remove { key, .. } => {
                        new_state.remove(key);
                    }
                }
            }
            new_state
        })
        .map(|state| state.values().cloned().collect())
}

/// Create a query cell that filters entities by a predicate.
///
/// Returns a cell containing entities that satisfy the predicate.
/// The predicate is re-evaluated when entities change.
pub fn query_where<F>(
    store: &EntityStore,
    predicate: F,
) -> Cell<Vec<Arc<dyn AnyItem>>, CellImmutable>
where
    F: Fn(&Arc<dyn AnyItem>) -> bool + Send + Sync + 'static,
{
    let predicate = Arc::new(predicate);

    // Build initial state from current entries
    let initial: HashMap<Arc<str>, Arc<dyn AnyItem>> = {
        let entries = store.entries();
        let current = entries.get();
        current
            .iter()
            .filter(|(_, item)| predicate(item))
            .map(|(id, item)| (id.clone(), item.clone()))
            .collect()
    };

    let predicate_clone = predicate.clone();

    // Use scan to maintain HashMap state, then map to Vec
    store
        .diffs()
        .scan(initial, move |state, diff_opt| {
            let mut new_state = state.clone();
            if let Some(diff) = diff_opt {
                match diff {
                    MapDiff::Insert { key, value } => {
                        if predicate_clone(value) {
                            new_state.insert(key.clone(), value.clone());
                        } else {
                            new_state.remove(key);
                        }
                    }
                    MapDiff::Update { key, new_value, .. } => {
                        if predicate_clone(new_value) {
                            new_state.insert(key.clone(), new_value.clone());
                        } else {
                            new_state.remove(key);
                        }
                    }
                    MapDiff::Remove { key, .. } => {
                        new_state.remove(key);
                    }
                }
            }
            new_state
        })
        .map(|state| state.values().cloned().collect())
}

/// Create a query cell that returns a single entity by ID.
///
/// Returns `None` if the entity doesn't exist.
pub fn query_by_id(
    store: &EntityStore,
    id: Arc<str>,
) -> Cell<Option<Arc<dyn AnyItem>>, CellImmutable> {
    let id_clone = id.clone();

    // Build initial state
    let initial: Option<Arc<dyn AnyItem>> = store.get(&id);

    // Use scan to track the single entity
    store
        .diffs()
        .scan(initial, move |state, diff_opt| {
            let mut new_state = state.clone();
            if let Some(diff) = diff_opt {
                match diff {
                    MapDiff::Insert { key, value } => {
                        if *key == id_clone {
                            new_state = Some(value.clone());
                        }
                    }
                    MapDiff::Update { key, new_value, .. } => {
                        if *key == id_clone {
                            new_state = Some(new_value.clone());
                        }
                    }
                    MapDiff::Remove { key, .. } => {
                        if *key == id_clone {
                            new_state = None;
                        }
                    }
                }
            }
            new_state
        })
}

/// Create a query cell that counts entities matching a predicate.
pub fn query_count<F>(store: &EntityStore, predicate: F) -> Cell<usize, CellImmutable>
where
    F: Fn(&Arc<dyn AnyItem>) -> bool + Send + Sync + 'static,
{
    query_where(store, predicate).map(|items| items.len())
}

/// Create a query cell that counts all entities in the store.
pub fn query_count_all(store: &EntityStore) -> Cell<usize, CellImmutable> {
    store.entries().map(|entries| entries.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::to_value::ToValue;
    use crate::common::with_id::WithId;
    use hypha::{Gettable, Signal, Watchable};
    use serde_json::Value;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Test entity
    #[derive(Debug, Clone)]
    struct TestEntity {
        id: Arc<str>,
        name: String,
        active: bool,
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
                "name": self.name,
                "active": self.active
            })
        }
    }

    impl AnyItem for TestEntity {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn make_entity(id: &str, name: &str, active: bool) -> Arc<dyn AnyItem> {
        Arc::new(TestEntity {
            id: id.into(),
            name: name.to_string(),
            active,
        }) as Arc<dyn AnyItem>
    }

    #[test]
    fn test_query_all() {
        let store = EntityStore::new();

        store.set("a".into(), make_entity("a", "Alice", true));
        store.set("b".into(), make_entity("b", "Bob", false));

        let all = query_all(&store);
        assert_eq!(all.get().len(), 2);

        // Add another
        store.set("c".into(), make_entity("c", "Charlie", true));
        assert_eq!(all.get().len(), 3);

        // Remove one
        store.remove(&"a".into());
        assert_eq!(all.get().len(), 2);
    }

    #[test]
    fn test_query_by_ids() {
        let store = EntityStore::new();

        store.set("a".into(), make_entity("a", "Alice", true));
        store.set("b".into(), make_entity("b", "Bob", false));
        store.set("c".into(), make_entity("c", "Charlie", true));

        let by_ids = query_by_ids(&store, vec!["a".into(), "c".into()]);
        assert_eq!(by_ids.get().len(), 2);

        // Adding non-matching ID doesn't affect result
        store.set("d".into(), make_entity("d", "Dave", true));
        assert_eq!(by_ids.get().len(), 2);

        // Removing matching ID updates result
        store.remove(&"a".into());
        assert_eq!(by_ids.get().len(), 1);
    }

    #[test]
    fn test_query_where() {
        let store = EntityStore::new();

        store.set("a".into(), make_entity("a", "Alice", true));
        store.set("b".into(), make_entity("b", "Bob", false));
        store.set("c".into(), make_entity("c", "Charlie", true));

        // Query active entities
        let active = query_where(&store, |item| {
            // Downcast to check active field
            if let Some(entity) = item.as_any().downcast_ref::<TestEntity>() {
                entity.active
            } else {
                false
            }
        });

        assert_eq!(active.get().len(), 2);

        // Update Bob to active
        store.set("b".into(), make_entity("b", "Bob", true));
        assert_eq!(active.get().len(), 3);

        // Update Alice to inactive
        store.set("a".into(), make_entity("a", "Alice", false));
        assert_eq!(active.get().len(), 2);
    }

    #[test]
    fn test_query_by_id() {
        let store = EntityStore::new();

        let entity = query_by_id(&store, "a".into());
        assert!(entity.get().is_none());

        // Add the entity
        store.set("a".into(), make_entity("a", "Alice", true));
        assert!(entity.get().is_some());
        assert_eq!(entity.get().unwrap().id().as_ref(), "a");

        // Update the entity
        store.set("a".into(), make_entity("a", "Alice Updated", true));
        assert_eq!(entity.get().unwrap().id().as_ref(), "a");

        // Remove the entity
        store.remove(&"a".into());
        assert!(entity.get().is_none());
    }

    #[test]
    fn test_query_count() {
        let store = EntityStore::new();

        store.set("a".into(), make_entity("a", "Alice", true));
        store.set("b".into(), make_entity("b", "Bob", false));
        store.set("c".into(), make_entity("c", "Charlie", true));

        let active_count = query_count(&store, |item| {
            if let Some(entity) = item.as_any().downcast_ref::<TestEntity>() {
                entity.active
            } else {
                false
            }
        });

        assert_eq!(active_count.get(), 2);

        store.set("b".into(), make_entity("b", "Bob", true));
        assert_eq!(active_count.get(), 3);
    }

    #[test]
    fn test_query_subscriptions() {
        let store = EntityStore::new();
        let all = query_all(&store);

        let update_count = Arc::new(AtomicUsize::new(0));
        let count_clone = update_count.clone();

        let _guard = all.subscribe(move |signal| {
            if let Signal::Value(_) = signal {
                count_clone.fetch_add(1, Ordering::SeqCst);
            }
        });

        // Initial subscription fires
        assert!(update_count.load(Ordering::SeqCst) >= 1);
        let initial = update_count.load(Ordering::SeqCst);

        // Each change should fire subscription
        store.set("a".into(), make_entity("a", "Alice", true));
        assert!(update_count.load(Ordering::SeqCst) > initial);
    }
}
