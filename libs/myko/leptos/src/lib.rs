use std::sync::Arc;

use leptos::prelude::*;
use myko_rs::{common::with_id::WithTypedId, hyphae::IdFor};

/// Initialize the myko-leptos bridge.
///
/// Creates a `MykoClient` with WASM WebSocket transport and stores it in Leptos context.
/// Call this once in your root `App` component.
///
/// `address` should be the myko server WebSocket address (e.g. `"localhost:5155"`).
pub fn provide_myko(address: &str) {
    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::client::MykoClient;
        let client = MykoClient::new();
        client.set_protocol(myko_rs::client::MykoProtocol::JSON);
        client.set_address(Some(address.to_string()));
        provide_context(client);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = address;
}

/// Disconnect the Myko client (clears the address).
pub fn disconnect_myko() {
    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::client::MykoClient;
        let client = expect_context::<MykoClient>();
        client.set_address(None);
    }
}

/// Returns a reactive signal tracking the Myko connection status.
pub fn use_connection_status() -> ReadSignal<bool> {
    let (read, write) = signal(false);

    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::{
            client::{ConnectionStatus, MykoClient},
            hyphae::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();
        let cell = client.connection_status();
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(status) = signal {
                write.set(matches!(&**status, ConnectionStatus::Connected(_)));
            }
        });
        cell.own(guard);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = write;

    read
}

/// Returns a reactive signal of query results that updates when the server pushes data.
///
/// Subscribes to the query via `MykoClient` and updates the signal on each change.
///
/// `Q` is the query type (e.g. `GetAllServers`).
pub fn live_query<Q>(query: Q) -> ReadSignal<Vec<Arc<Q::Item>>>
where
    Q: myko_rs::query::QueryParams + Clone + Send + Sync + 'static,
    Q::Item: myko_rs::core::item::Eventable
        + myko_rs::common::with_id::WithId
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + Send
        + 'static,
{
    let (read, write) = signal(vec![]);

    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::{
            client::MykoClient,
            hyphae::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();
        let cell = client.watch_query(query);
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(items) = signal {
                write.set((**items).to_vec());
            }
        });
        cell.own(guard);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = (query, write);

    read
}

/// Apply a hyphae MapDiff to per-entity Leptos signals.
///
/// Each entity has its own RwSignal — only subscribers to that specific entity
/// are notified on update. The `ids` signal tracks the full ID list for `<For>`
/// iteration and is updated once per diff (not per-entity).
fn apply_diff<T: Clone + Send + Sync + 'static>(
    signals: &StoredValue<std::collections::HashMap<Arc<str>, RwSignal<Option<Arc<T>>>>>,
    ids_write: &WriteSignal<Vec<Arc<str>>>,
    diff: &myko_rs::hyphae::MapDiff<Arc<str>, Arc<T>>,
) {
    use myko_rs::hyphae::MapDiff;
    match diff {
        MapDiff::Initial { entries } => {
            signals.update_value(|map| {
                for (key, value) in entries {
                    match map.entry(key.clone()) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            e.get().set(Some(value.clone()));
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(RwSignal::new(Some(value.clone())));
                        }
                    }
                }
            });
            ids_write.update(|ids| {
                *ids = entries.iter().map(|(k, _)| k.clone()).collect();
            });
        }
        MapDiff::Insert { key, value } => {
            signals.update_value(|map| match map.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(e) => {
                    e.get().set(Some(value.clone()));
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(RwSignal::new(Some(value.clone())));
                }
            });
            ids_write.update(|ids| {
                if !ids.contains(key) {
                    ids.push(key.clone());
                }
            });
        }
        MapDiff::Update { key, new_value, .. } => {
            signals.update_value(|map| {
                if let Some(sig) = map.get(key) {
                    sig.set(Some(new_value.clone()));
                }
            });
            // No ids change on update — entity still exists
        }
        MapDiff::Remove { key, .. } => {
            // Update ids FIRST so <For> unmounts the component before
            // the signal goes None (avoids flash of empty state)
            ids_write.update(|ids| ids.retain(|i| i != key));
            signals.update_value(|map| {
                // Don't set to None — the component is being unmounted via ids.
                // Just clean up the signal from the map.
                map.remove(key);
            });
        }
        MapDiff::Batch { changes } => {
            // Collect ID mutations and apply once to avoid N notifications
            let mut ids_to_add: Vec<Arc<str>> = Vec::new();
            let mut ids_to_remove: Vec<Arc<str>> = Vec::new();

            for change in changes {
                match change {
                    MapDiff::Insert { key, value } => {
                        signals.update_value(|map| match map.entry(key.clone()) {
                            std::collections::hash_map::Entry::Occupied(e) => {
                                e.get().set(Some(value.clone()));
                            }
                            std::collections::hash_map::Entry::Vacant(e) => {
                                e.insert(RwSignal::new(Some(value.clone())));
                            }
                        });
                        ids_to_add.push(key.clone());
                    }
                    MapDiff::Update { key, new_value, .. } => {
                        signals.update_value(|map| {
                            if let Some(sig) = map.get(key) {
                                sig.set(Some(new_value.clone()));
                            }
                        });
                    }
                    MapDiff::Remove { key, .. } => {
                        signals.update_value(|map| {
                            map.remove(key);
                        });
                        ids_to_remove.push(key.clone());
                    }
                    // Nested batch or initial within a batch — recurse
                    other => apply_diff(signals, ids_write, other),
                }
            }

            // Single ids update for the whole batch
            if !ids_to_add.is_empty() || !ids_to_remove.is_empty() {
                ids_write.update(|ids| {
                    ids.retain(|i| !ids_to_remove.contains(i));
                    for id in ids_to_add {
                        if !ids.contains(&id) {
                            ids.push(id);
                        }
                    }
                });
            }
        }
    }
}

/// Fine-grained reactive query — per-entity Leptos signals.
///
/// Unlike `live_query` which returns `ReadSignal<Vec<Item>>` (all subscribers
/// re-render on any entity change), this uses `watch_query_map` to maintain
/// per-entity signals. Only components subscribed to a specific entity
/// re-render when that entity changes.
///
/// Returns a `LiveQueryMap` handle for looking up individual entities.
pub fn live_query_map<Q>(query: Q) -> LiveQueryMap<Q::Item>
where
    Q: myko_rs::query::QueryParams + Clone + Send + Sync + 'static,
    Q::Item: myko_rs::core::item::Eventable
        + WithTypedId
        + serde::de::DeserializeOwned
        + Clone
        + std::fmt::Debug
        + PartialEq
        + Send
        + 'static,
    <Q::Item as WithTypedId>::Id: IdFor<Q::Item, MapKey = Arc<str>>,
{
    #[cfg(target_arch = "wasm32")]
    {
        use std::collections::HashMap;

        use myko_rs::{client::MykoClient, hyphae::Watchable};

        let client = expect_context::<MykoClient>();
        let cell_map = client.watch_query_map(query);

        // Per-entity Leptos signals, keyed by entity ID
        let signals: StoredValue<HashMap<Arc<str>, RwSignal<Option<Arc<Q::Item>>>>> =
            StoredValue::new(HashMap::new());

        // Track entity IDs for list rendering
        let (ids_read, ids_write) = signal(Vec::<Arc<str>>::new());

        // Subscribe to CellMap diffs and update per-entity Leptos signals
        let guard = cell_map.subscribe_diffs(move |diff| {
            apply_diff(&signals, &ids_write, diff);
        });

        // Keep both alive independently — no circular ownership.
        // Arena-managed: drops when the reactive owner (component) disposes.
        let _keepalive_map = StoredValue::new(cell_map);
        let _keepalive_guard = StoredValue::new(guard);

        LiveQueryMap {
            signals,
            ids: ids_read,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = query;
        LiveQueryMap {
            signals: StoredValue::new(std::collections::HashMap::new()),
            ids: signal(vec![]).0,
        }
    }
}

/// Handle for a fine-grained reactive query.
///
/// Provides per-entity signal access and a reactive list of entity IDs.
/// The inner CellMap is kept alive via StoredValue to maintain the diff subscription.
#[derive(Clone, Copy)]
pub struct LiveQueryMap<T: Clone + Send + Sync + 'static> {
    signals: StoredValue<std::collections::HashMap<Arc<str>, RwSignal<Option<Arc<T>>>>>,
    /// Reactive list of entity IDs (for `<For>` iteration)
    pub ids: ReadSignal<Vec<Arc<str>>>,
}

impl<T: Clone + Send + Sync + 'static> LiveQueryMap<T> {
    /// Get a reactive signal for a specific entity by ID.
    ///
    /// Returns `ReadSignal<Option<Arc<T>>>` — None if entity doesn't exist yet.
    /// Safe to call before data arrives — the signal will update when the entity
    /// is inserted via a diff.
    pub fn get(&self, id: &str) -> ReadSignal<Option<Arc<T>>> {
        let id: Arc<str> = id.into();
        // Ensure signal exists (subscribe-before-data pattern)
        self.signals.update_value(|map| {
            map.entry(id.clone()).or_insert_with(|| RwSignal::new(None));
        });
        let sig = self.signals.with_value(|map| map.get(&id).copied());
        sig.map(|s| s.read_only()).unwrap_or_else(|| signal(None).0)
    }
}

/// Send a command to the Myko server and return a reactive signal with the result.
///
/// The returned signal starts as `None` and resolves to `Some(Ok(response))` or
/// `Some(Err(message))` when the server responds.
pub fn send_command<C, R>(cmd: C) -> ReadSignal<Option<Result<R, String>>>
where
    C: serde::Serialize + Clone + myko_rs::core::command::CommandId + 'static,
    R: serde::de::DeserializeOwned + Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
{
    let (read, write) = signal(None);

    #[cfg(target_arch = "wasm32")]
    {
        use myko_rs::{
            client::MykoClient,
            hyphae::{Signal, Watchable},
        };

        let client = expect_context::<MykoClient>();
        let cell = client.send_command::<C, R>(&cmd);
        let guard = cell.subscribe(move |signal| {
            if let Signal::Value(result) = signal {
                write.set((**result).clone());
            }
        });
        cell.own(guard);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = (cmd, write);

    read
}
