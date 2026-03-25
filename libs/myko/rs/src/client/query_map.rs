use std::sync::Arc;

use hyphae::{CellImmutable, CellMap, Gettable, Watchable};
use log::{debug, error, trace};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{ConnectionStatus, MykoClient, QueryRequest};
use crate::{
    common::with_id::{WithId, WithTypedId},
    core::{item::Eventable, query::QueryParams},
    wire::{message::MykoMessage, query::WrappedQuery},
};

impl MykoClient {
    /// Watch a query with per-entity reactive granularity.
    ///
    /// Unlike `watch_query` which returns `Cell<Vec<Arc<Item>>>` (re-notifies
    /// all subscribers on any entity change), this returns a `CellMap` where
    /// each entity has its own cell. Only subscribers to a specific entity
    /// are notified when that entity changes.
    ///
    /// Use this for fine-grained reactivity in UI frameworks.
    pub fn watch_query_map<Q>(
        &self,
        query: impl Into<QueryRequest<Q>>,
    ) -> CellMap<Arc<str>, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + Clone,
        Q::Item: Eventable + WithTypedId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
        <Q::Item as WithTypedId>::Id: hyphae::IdFor<Q::Item, MapKey = Arc<str>>,
    {
        let query: QueryRequest<Q> = query.into();
        let tx: Arc<str> = query.tx.clone();
        let query_id = query.query.query_id();
        let query_item_type = Q::query_item_type_static();

        let query_value = serde_json::to_value(&query).expect("Query should serialize");

        let wrapped = WrappedQuery {
            query: query_value,
            query_id: query_id.clone(),
            query_item_type,
            window: None,
        };

        let map: CellMap<Arc<str>, Arc<Q::Item>> =
            CellMap::new().with_name(format!("query_map:{}", query_id));
        let map_weak = map.downgrade();

        let tx_for_handler = tx.clone();
        let query_id_for_handler = query_id.clone();

        // Register handler for query responses matching this tx
        self.inner.query_handlers.insert(
            tx.clone(),
            Box::new(move |response_value: Value| {
                let Some(map_writer) = map_weak.upgrade() else {
                    return;
                };

                let Ok(response) =
                    serde_json::from_value::<crate::wire::QueryResponse>(response_value)
                else {
                    return;
                };

                if response.tx != tx_for_handler {
                    return;
                }

                // Parse upserts
                let upserts: Vec<_> = response
                    .upserts
                    .into_iter()
                    .filter_map(|item_value| {
                        match serde_json::from_value::<Q::Item>(item_value.item) {
                            Ok(item) => {
                                let item = Arc::new(item);
                                let id = item.id().clone();
                                Some((id, item))
                            }
                            Err(e) => {
                                error!(
                                    "Failed to parse query '{}' upsert as {}: {}",
                                    query_id_for_handler,
                                    std::any::type_name::<Q::Item>(),
                                    e
                                );
                                None
                            }
                        }
                    })
                    .collect();

                if response.sequence == 0 {
                    // Server reset — full state replacement as single Batch diff
                    trace!("Sequence reset: replacing {} map", query_id_for_handler);
                    map_writer.replace_all(upserts);
                } else {
                    // Incremental update
                    if !upserts.is_empty() {
                        map_writer.insert_many(upserts);
                    }
                    if !response.deletes.is_empty() {
                        map_writer.remove_many(response.deletes);
                    }
                }
            }),
        );

        // Build the frame to send (and re-send on reconnect)
        let msg = MykoMessage::Query(wrapped);
        let frame = self
            .encode_message(&msg)
            .expect("Could not serialize message");

        // Subscribe to connection status to re-send on reconnect
        let socket = self.inner.socket.clone();
        let status_cell = self.connection_status();
        let send_query_id = query_id.clone();
        let frame_clone = frame.clone();
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                match &**status {
                    ConnectionStatus::Connected(_) => match socket.send(frame_clone.clone()) {
                        Ok(_) => debug!("Watching query map {send_query_id}"),
                        Err(e) => error!("Could not send query: {e:?}"),
                    },
                    _ => {
                        debug!("Query map {send_query_id} disconnected");
                    }
                }
            }
        });

        // Send immediately if connected
        if let ConnectionStatus::Connected(_) = status_cell.get() {
            match self.inner.socket.send(frame) {
                Ok(_) => debug!("Watching query map {query_id}"),
                Err(e) => error!("Could not send query map: {e:?}"),
            }
        }

        // Own the subscription guard so it lives as long as the map
        map.own(status_guard);
        map.own(super::query_cancel_guard(tx, self.inner.clone()));

        map.lock()
    }
}
