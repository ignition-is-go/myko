use std::{any::Any, collections::HashMap, hash::Hash, sync::Arc};

use serde_json::Value;

use crate::core::item::AnyItem;

/// Downcast an `Arc<dyn AnyItem>` to a concrete type.
/// Returns `Some(T)` if the downcast succeeds, `None` otherwise.
///
/// # Example
/// ```rust,no_run
/// use std::{any::Any, sync::Arc};
/// use myko_rs::common::{to_value::ToValue, with_id::WithId};
/// use myko_rs::item::AnyItem;
/// use myko_rs::utils::downcast_item;
///
/// #[derive(Clone, Debug, PartialEq)]
/// struct Server {
///     id: Arc<str>,
/// }
///
/// impl WithId for Server {
///     fn id(&self) -> Arc<str> {
///         self.id.clone()
///     }
/// }
///
/// impl ToValue for Server {
///     fn to_value(&self) -> serde_json::Value {
///         serde_json::json!({ "id": self.id.as_ref() })
///     }
/// }
///
/// impl AnyItem for Server {
///     fn as_any(&self) -> &dyn Any {
///         self
///     }
///
///     fn entity_type(&self) -> &'static str {
///         "Server"
///     }
///
///     fn equals(&self, other: &dyn AnyItem) -> bool {
///         other
///             .as_any()
///             .downcast_ref::<Self>()
///             .map(|typed| self == typed)
///             .unwrap_or(false)
///     }
/// }
///
/// let item: Arc<dyn AnyItem> = Arc::new(Server { id: "server-1".into() });
/// if let Some(server) = downcast_item::<Server>(&item) {
///     assert_eq!(server.id.as_ref(), "server-1");
/// }
/// ```
pub fn downcast_item<T: Clone + 'static>(item: &Arc<dyn AnyItem>) -> Option<T> {
    // AnyItem: Any, so we can use downcast_ref on the trait object
    (item.as_ref() as &dyn Any).downcast_ref::<T>().cloned()
}

pub fn mask_filter(filter: &Value, candidate: &Value) -> bool {
    match (filter, candidate) {
        (Value::Object(map_a), Value::Object(map_b)) => {
            for (key, value_a) in map_a {
                match map_b.get(key) {
                    Some(value_b) => {
                        if !mask_filter(value_a, value_b) {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        }
        _ => filter == candidate,
    }
}

pub fn assert_default_for_key<K, V>(hash_map: &mut HashMap<K, V>, key: &K)
where
    K: Hash + Eq + Clone,
    V: Default,
{
    match hash_map.contains_key(key) {
        true => (),
        false => {
            hash_map.insert(key.clone(), V::default());
        }
    }
}
