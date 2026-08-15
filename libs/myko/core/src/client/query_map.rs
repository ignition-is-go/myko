use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use hyphae::{Cell, CellImmutable, CellMap, CellMutable, Gettable, Mutable, Watchable};

pub(super) fn apply_incremental_map_update<T: hyphae::CellValue>(
    map: &CellMap<Arc<str>, Arc<T>, CellMutable>,
    deletes: Vec<Arc<str>>,
    upserts: Vec<(Arc<str>, Arc<T>)>,
) {
    // A joined row can be removed and reinserted under the same stable ID in
    // one server batch. Upserts describe the resulting value, so they must win
    // when an ID appears in both legacy wire collections.
    if !deletes.is_empty() {
        map.remove_many(deletes);
    }
    if !upserts.is_empty() {
        map.insert_many(upserts);
    }
}
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::{debug, error, trace};

use super::{
    ConnectionStatus, MykoClient, QueryRequest, WindowedQueryWatch,
    map_response::{MapSequence, decode_map_upserts},
};
use crate::{
    common::with_id::{WithId, WithTypedId},
    core::{item::Eventable, query::QueryParams},
    wire::{ClientQueryChange, QueryWindow, message::MykoMessage, query::WrappedQuery},
};

type WindowQueryState<T> = Arc<Mutex<HashMap<Arc<str>, Arc<T>>>>;

/// Fine-grained query data together with explicit initial-response readiness.
///
/// A newly-created [`CellMap`] immediately publishes an empty snapshot, so its
/// emptiness cannot distinguish "not answered yet" from a successful empty
/// query. `ready` becomes true only after a valid sequence-zero server response.
#[derive(Clone)]
pub struct QueryMapWatch<T: hyphae::CellValue> {
    map: CellMap<Arc<str>, Arc<T>, CellImmutable>,
    ready: Cell<bool, CellImmutable>,
}

impl<T: hyphae::CellValue> QueryMapWatch<T> {
    #[must_use]
    pub const fn map(&self) -> &CellMap<Arc<str>, Arc<T>, CellImmutable> {
        &self.map
    }

    #[must_use]
    pub const fn ready(&self) -> &Cell<bool, CellImmutable> {
        &self.ready
    }

    #[must_use]
    pub fn into_map(self) -> CellMap<Arc<str>, Arc<T>, CellImmutable> {
        self.map
    }
}

impl MykoClient {
    /// Authoritatively create or replace an edge through the ordinary command
    /// protocol.
    pub fn connect_graph<E>(
        &self,
        edge: &E,
    ) -> hyphae::Cell<Option<Result<(), String>>, hyphae::CellImmutable>
    where
        E: crate::graph::GraphClientMutations,
    {
        self.send_command(&E::connect_command(edge))
    }

    /// Authoritatively create or replace a batch of same-type edges in one
    /// bulk mutation.
    pub fn connect_graph_batch<E>(
        &self,
        edges: &[E],
    ) -> hyphae::Cell<Option<Result<usize, String>>, hyphae::CellImmutable>
    where
        E: crate::graph::GraphClientMutations,
    {
        self.send_command(&E::connect_many_command(edges))
    }

    /// Ensure a unique edge pair exists without replacing an existing edge.
    ///
    /// The generated server command retries the indexed pair lookup after a
    /// concurrent uniqueness conflict, making simultaneous ensures converge on
    /// the winning edge ID.
    pub fn ensure_graph<E>(
        &self,
        edge: &E,
    ) -> hyphae::Cell<Option<Result<E::EnsureResult, String>>, hyphae::CellImmutable>
    where
        E: crate::graph::GraphClientMutations,
    {
        self.send_command(&E::ensure_command(edge))
    }

    /// Delete an edge by typed ID through its existing generated delete
    /// command.
    pub fn disconnect_graph<E>(
        &self,
        id: &E::Id,
    ) -> hyphae::Cell<Option<Result<E::DisconnectResult, String>>, hyphae::CellImmutable>
    where
        E: crate::graph::GraphClientMutations,
    {
        self.send_command(&E::disconnect_command(id))
    }

    /// Delete a batch of same-type edges by typed ID.
    pub fn disconnect_graph_batch<E>(
        &self,
        ids: &[E::Id],
    ) -> hyphae::Cell<Option<Result<E::DisconnectManyResult, String>>, hyphae::CellImmutable>
    where
        E: crate::graph::GraphClientMutations,
    {
        self.send_command(&E::disconnect_many_command(ids))
    }

    /// Watch edges at endpoint A through the generated ordinary query.
    ///
    /// The returned state includes an authoritative readiness signal and
    /// shares its decoded map and wire subscription with identical watches.
    pub fn watch_graph_from<E>(
        &self,
        endpoint: &<<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::Value,
    ) -> QueryMapWatch<E>
    where
        E: crate::graph::GraphClientQueries + WithTypedId,
        E::Ends: crate::graph::TypedEdgeEnds,
        <E as WithTypedId>::Id: hyphae::IdFor<E, MapKey = Arc<str>>,
    {
        self.watch_query_map_state(E::from_query(endpoint))
    }

    /// Watch edges at endpoint B through the generated ordinary query.
    pub fn watch_graph_to<E>(
        &self,
        endpoint: &<<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::Value,
    ) -> QueryMapWatch<E>
    where
        E: crate::graph::GraphClientQueries + WithTypedId,
        E::Ends: crate::graph::TypedEdgeEnds,
        <E as WithTypedId>::Id: hyphae::IdFor<E, MapKey = Arc<str>>,
    {
        self.watch_query_map_state(E::to_query(endpoint))
    }

    /// Watch edges matching one exact endpoint pair through the generated
    /// ordinary query.
    pub fn watch_graph_between<E>(
        &self,
        a: &<<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::Value,
        b: &<<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::Value,
    ) -> QueryMapWatch<E>
    where
        E: crate::graph::GraphClientQueries + WithTypedId,
        E::Ends: crate::graph::TypedEdgeEnds,
        <E as WithTypedId>::Id: hyphae::IdFor<E, MapKey = Arc<str>>,
    {
        self.watch_query_map_state(E::between_query(a, b))
    }

    /// Watch one ordered page of edges at endpoint A.
    pub fn watch_graph_from_windowed<E>(
        &self,
        endpoint: &<<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::Value,
        window: QueryWindow,
    ) -> WindowedQueryWatch<E>
    where
        E: crate::graph::GraphClientQueries,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        self.watch_query_windowed(E::from_query(endpoint), window)
    }

    /// Watch one ordered page of edges at endpoint B.
    pub fn watch_graph_to_windowed<E>(
        &self,
        endpoint: &<<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::Value,
        window: QueryWindow,
    ) -> WindowedQueryWatch<E>
    where
        E: crate::graph::GraphClientQueries,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        self.watch_query_windowed(E::to_query(endpoint), window)
    }

    /// Watch one ordered page of edges matching an exact endpoint pair.
    pub fn watch_graph_between_windowed<E>(
        &self,
        a: &<<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::Value,
        b: &<<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::Value,
        window: QueryWindow,
    ) -> WindowedQueryWatch<E>
    where
        E: crate::graph::GraphClientQueries,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        self.watch_query_windowed(E::between_query(a, b), window)
    }

    /// Watch the live number of edges at endpoint A without transferring edge
    /// payloads to the client.
    pub fn watch_graph_count_from<E>(
        &self,
        endpoint: &<<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::Value,
    ) -> Cell<Option<usize>, CellImmutable>
    where
        E: crate::graph::GraphClientAggregates,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        self.watch_report::<E::CountFromReport, usize>(E::count_from_report(endpoint))
    }

    /// Watch the live number of edges at endpoint B without transferring edge
    /// payloads to the client.
    pub fn watch_graph_count_to<E>(
        &self,
        endpoint: &<<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::Value,
    ) -> Cell<Option<usize>, CellImmutable>
    where
        E: crate::graph::GraphClientAggregates,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        self.watch_report::<E::CountToReport, usize>(E::count_to_report(endpoint))
    }

    /// Watch the live number of edges matching one exact endpoint pair.
    pub fn watch_graph_count_between<E>(
        &self,
        a: &<<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::Value,
        b: &<<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::Value,
    ) -> Cell<Option<usize>, CellImmutable>
    where
        E: crate::graph::GraphClientAggregates,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        self.watch_report::<E::CountBetweenReport, usize>(E::count_between_report(a, b))
    }

    /// Watch whether any edge matches one exact endpoint pair.
    pub fn watch_graph_exists_between<E>(
        &self,
        a: &<<E::Ends as crate::graph::TypedEdgeEnds>::A as crate::graph::EndpointSpec>::Value,
        b: &<<E::Ends as crate::graph::TypedEdgeEnds>::B as crate::graph::EndpointSpec>::Value,
    ) -> Cell<Option<bool>, CellImmutable>
    where
        E: crate::graph::GraphClientAggregates,
        E::Ends: crate::graph::TypedEdgeEnds,
    {
        self.watch_report::<E::ExistsBetweenReport, bool>(E::exists_between_report(a, b))
    }

    /// Watch a query with per-entity reactive granularity.
    ///
    /// Unlike `watch_query` which returns `Cell<Vec<Arc<Item>>>` (re-notifies
    /// all subscribers on any entity change), this returns a `CellMap` where
    /// each entity has its own cell. Only subscribers to a specific entity
    /// are notified when that entity changes.
    ///
    /// Use this for fine-grained reactivity in UI frameworks. Identical query
    /// parameters share one decoded map and wire subscription.
    pub fn watch_query_map<Q>(
        &self,
        query: impl Into<QueryRequest<Q>>,
    ) -> CellMap<Arc<str>, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + Clone,
        Q::Item: Eventable + WithTypedId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
        <Q::Item as WithTypedId>::Id: hyphae::IdFor<Q::Item, MapKey = Arc<str>>,
    {
        self.watch_query_map_state(query).into_map()
    }

    /// Watch a query map and retain an explicit initial-response signal.
    #[allow(clippy::too_many_lines)]
    pub fn watch_query_map_state<Q>(
        &self,
        query: impl Into<QueryRequest<Q>>,
    ) -> QueryMapWatch<Q::Item>
    where
        Q: QueryParams + Clone,
        Q::Item: Eventable + WithTypedId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
        <Q::Item as WithTypedId>::Id: hyphae::IdFor<Q::Item, MapKey = Arc<str>>,
    {
        let supplied: QueryRequest<Q> = query.into();
        let query_id = supplied.query.query_id();
        let query_item_type = Q::query_item_type_static();
        let cache_key = format!(
            "query-map:{query_id}:{query_item_type}:{}:{:016x}",
            std::any::type_name::<Q::Item>(),
            supplied.query.cache_key_hash()
        );
        let _cache_gate = self
            .inner
            .map_watch_cache_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((map, ready)) = self.cached_map_watch(&cache_key) {
            debug!("watch_query_map_state: cache hit for {cache_key}");
            return QueryMapWatch { map, ready };
        }
        self.inner.map_watch_cache.remove(&cache_key);

        let query = QueryRequest::with_tx(supplied.query, super::next_subscription_tx());
        let tx: Arc<str> = query.tx.clone();

        let map: CellMap<Arc<str>, Arc<Q::Item>> =
            CellMap::new().with_name(format!("query_map:{query_id}"));
        let map_weak = map.downgrade();
        let ready =
            Cell::<bool, CellMutable>::new(false).with_name(format!("query_map_ready:{query_id}"));
        let ready_weak = ready.downgrade();
        let ready_read = ready.clone().lock();

        let Ok(query_value) = serde_json::to_value(&query) else {
            error!("Could not serialize query map request for {query_id}");
            return QueryMapWatch {
                map: map.lock(),
                ready: ready_read,
            };
        };

        let wrapped = WrappedQuery {
            query: query_value,
            query_id: query_id.clone(),
            query_item_type,
            window: None,
        };

        let tx_for_handler = tx.clone();
        let query_id_for_handler = query_id.clone();
        let sequences = Arc::new(MapSequence::new());
        let sequences_for_handler = Arc::clone(&sequences);

        // Duplicate explicit transaction IDs must not replace another watch;
        // otherwise dropping the older watch could remove the newer handler.
        let handler: super::QueryHandler = Box::new(move |response_value: Value| {
            let Some(map_writer) = map_weak.upgrade() else {
                return;
            };

            let response =
                match serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value) {
                    Ok(response) => response,
                    Err(error) => {
                        error!(
                            "Rejected query '{}' malformed response: {}",
                            query_id_for_handler, error
                        );
                        return;
                    }
                };

            if response.tx != tx_for_handler {
                return;
            }

            // Decode the complete response before advancing its sequence or
            // mutating the map. One malformed row invalidates the whole batch.
            let upserts = match decode_map_upserts::<Q::Item, _>(response.upserts, WithId::id) {
                Ok(upserts) => upserts,
                Err(error) => {
                    error!(
                        "Rejected query '{}' response: invalid {} upsert: {}",
                        query_id_for_handler,
                        std::any::type_name::<Q::Item>(),
                        error
                    );
                    return;
                }
            };

            if !sequences_for_handler.accept(response.sequence) {
                error!(
                    "Rejected query '{}' out-of-order sequence {}",
                    query_id_for_handler, response.sequence
                );
                return;
            }

            let is_initial_response = response.sequence == 0;
            if is_initial_response {
                trace!("Sequence reset: replacing {} map", query_id_for_handler);
                map_writer.replace_all(upserts);
            } else {
                apply_incremental_map_update(&map_writer, response.deletes, upserts);
            }

            if is_initial_response && let Some(ready_writer) = ready_weak.upgrade() {
                ready_writer.set(true);
            }
        });
        if !self.try_register_query_handler(tx.clone(), handler) {
            error!("Refusing duplicate query map transaction {tx}");
            return QueryMapWatch {
                map: map.lock(),
                ready: ready_read,
            };
        }

        // Build the frame to send (and re-send on reconnect)
        let msg = MykoMessage::Query(wrapped);
        let Ok(frame) = self.encode_message(&msg) else {
            error!("Could not encode query map request for {query_id}");
            self.inner.query_handlers.remove(&tx);
            return QueryMapWatch {
                map: map.lock(),
                ready: ready_read,
            };
        };

        // Subscribe to connection status to re-send on reconnect
        let socket = self.inner.socket.clone();
        let ready_for_status = ready.downgrade();
        let sequences_for_status = sequences;
        let status_cell = self.connection_status();
        let send_query_id = query_id;
        let frame_to_send = frame;
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                if let ConnectionStatus::Connected(_) = &**status {
                    match socket.send(frame_to_send.clone()) {
                        Ok(()) => debug!("Watching query map {send_query_id}"),
                        Err(e) => error!("Could not send query: {e:?}"),
                    }
                } else {
                    sequences_for_status.reset_epoch();
                    if let Some(ready_writer) = ready_for_status.upgrade() {
                        ready_writer.set(false);
                    }
                    debug!("Query map {send_query_id} disconnected");
                }
            }
        });

        // Own the subscription guard so it lives as long as the map
        map.own(status_guard);
        map.own(super::query_cancel_guard(tx.clone(), self.inner.clone()));
        map.own(super::retain_cell_guard(ready_read.clone()));
        map.own(super::map_watch_cache_guard(
            cache_key.clone(),
            tx.clone(),
            self.inner.clone(),
        ));

        let watch = QueryMapWatch {
            map: map.lock(),
            ready: ready_read,
        };
        self.cache_map_watch(cache_key, tx, &watch.map, &watch.ready);
        watch
    }

    /// Watch an ordered server window with live total-count and window state.
    ///
    /// Identical query parameters and initial windows share one decoded state
    /// and wire subscription. Use [`WindowedQueryWatch::set_window`] to move
    /// the shared subscription without cancelling and recreating it.
    #[allow(clippy::too_many_lines)]
    pub fn watch_query_windowed<Q>(
        &self,
        query: impl Into<QueryRequest<Q>>,
        initial_window: QueryWindow,
    ) -> WindowedQueryWatch<Q::Item>
    where
        Q: QueryParams + Clone,
        Q::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        let supplied: QueryRequest<Q> = query.into();
        let query_id = supplied.query.query_id();
        let query_item_type = Q::query_item_type_static();
        let cache_key = format!(
            "query-window:{query_id}:{query_item_type}:{}:{:016x}:{}:{}",
            std::any::type_name::<Q::Item>(),
            supplied.query.cache_key_hash(),
            initial_window.offset,
            initial_window.limit
        );
        let _cache_gate = self
            .inner
            .list_watch_cache_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(watch) = self.cached_window_watch(&cache_key) {
            debug!("watch_query_windowed: cache hit for {cache_key}");
            return watch;
        }
        self.inner.list_watch_cache.remove(&cache_key);

        let query = QueryRequest::with_tx(supplied.query, super::next_subscription_tx());
        let tx = query.tx.clone();
        let items = Cell::new(Vec::new()).with_name(format!("query_window:{query_id}"));
        let items_weak = items.downgrade();
        let ready = Cell::new(false).with_name(format!("query_window_ready:{query_id}"));
        let total_count = Cell::new(None).with_name(format!("query_window_total_count:{query_id}"));
        let window = Cell::new(Some(initial_window.clone()))
            .with_name(format!("query_window_state:{query_id}"));
        let page_state = Cell::new(super::WindowedQueryState::new(
            Vec::new(),
            false,
            None,
            Some(initial_window.clone()),
        ))
        .with_name(format!("query_window_page_state:{query_id}"));
        let early_watch = WindowedQueryWatch {
            items: items.clone().lock(),
            ready: ready.clone().lock(),
            total_count: total_count.clone().lock(),
            window: window.clone().lock(),
            state: page_state.clone().lock(),
            tx: tx.clone(),
            client: self.clone(),
        };

        let Ok(query_value) = serde_json::to_value(&query) else {
            error!("Could not serialize windowed query request for {query_id}");
            return early_watch;
        };
        let wrapped = WrappedQuery {
            query: query_value,
            query_id: query_id.clone(),
            query_item_type,
            window: Some(initial_window),
        };
        let state: WindowQueryState<Q::Item> = Arc::default();
        let sequences = Arc::new(MapSequence::new());
        let sequences_for_handler = sequences.clone();
        let tx_for_handler = tx.clone();
        let query_id_for_handler = query_id.clone();
        let ready_for_handler = ready.clone();
        let total_count_for_handler = total_count.clone();
        let window_for_handler = window.clone();
        let page_state_for_handler = page_state.clone();
        let handler: super::QueryHandler = Box::new(move |response_value: Value| {
            let Some(items_writer) = items_weak.upgrade() else {
                return;
            };
            let response =
                match serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value) {
                    Ok(response) => response,
                    Err(error) => {
                        error!(
                            "Rejected windowed query '{}' malformed response: {}",
                            query_id_for_handler, error
                        );
                        return;
                    }
                };
            if response.tx != tx_for_handler {
                return;
            }
            let upserts = match decode_map_upserts::<Q::Item, _>(response.upserts, WithId::id) {
                Ok(upserts) => upserts,
                Err(error) => {
                    error!(
                        "Rejected windowed query '{}' response: invalid {} upsert: {}",
                        query_id_for_handler,
                        std::any::type_name::<Q::Item>(),
                        error
                    );
                    return;
                }
            };
            if !sequences_for_handler.accept(response.sequence) {
                error!(
                    "Rejected windowed query '{}' out-of-order sequence {}",
                    query_id_for_handler, response.sequence
                );
                return;
            }
            let order = response
                .changes
                .into_iter()
                .find_map(|change| match change {
                    ClientQueryChange::WindowOrder { ids, .. } => Some(ids),
                    ClientQueryChange::Upsert { .. } | ClientQueryChange::Delete { .. } => None,
                });
            let is_initial = response.sequence == 0;
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if is_initial {
                state.clear();
            }
            for id in response.deletes {
                state.remove(&id);
            }
            for (id, item) in upserts {
                state.insert(id, item);
            }
            let next_items: Vec<Arc<Q::Item>> = order.map_or_else(
                || {
                    let mut ids: Vec<_> = state.keys().cloned().collect();
                    ids.sort_unstable();
                    ids.into_iter()
                        .filter_map(|id| state.get(&id).cloned())
                        .collect()
                },
                |order| {
                    order
                        .into_iter()
                        .filter_map(|id| state.get(&id).cloned())
                        .collect()
                },
            );
            drop(state);
            let next_ready = ready_for_handler.get() || is_initial;
            let next_page_state = super::WindowedQueryState::new(
                next_items.clone(),
                next_ready,
                response.total_count,
                response.window.clone(),
            );
            items_writer.set(next_items);
            total_count_for_handler.set(response.total_count);
            window_for_handler.set(response.window);
            if is_initial {
                ready_for_handler.set(true);
            }
            page_state_for_handler.set(next_page_state);
        });
        if !self.try_register_query_handler(tx.clone(), handler) {
            error!("Refusing duplicate windowed query transaction {tx}");
            return early_watch;
        }

        let inner = self.inner.clone();
        let ready_for_status = ready.downgrade();
        let page_state_for_status = page_state.downgrade();
        let window_for_status = window.clone().lock();
        let wrapped_for_status = wrapped;
        let status_cell = self.connection_status();
        let status_guard = status_cell.subscribe(move |signal| {
            let hyphae::Signal::Value(status) = signal else {
                return;
            };
            if let ConnectionStatus::Connected(_) = &**status {
                let mut request = wrapped_for_status.clone();
                request.window = window_for_status.get();
                let message = MykoMessage::Query(request);
                match super::encode_protocol(&inner.protocol, &message)
                    .ok_or_else(|| "could not encode query".to_string())
                    .and_then(|frame| inner.socket.send(frame))
                {
                    Ok(()) => debug!("Watching windowed query {query_id}"),
                    Err(error) => error!("Could not send windowed query: {error}"),
                }
            } else {
                sequences.reset_epoch();
                if let Some(writer) = ready_for_status.upgrade() {
                    writer.set(false);
                }
                if let Some(writer) = page_state_for_status.upgrade() {
                    let current = writer.get();
                    writer.set(super::WindowedQueryState::new(
                        current.items,
                        false,
                        current.total_count,
                        current.window,
                    ));
                }
            }
        });

        items.own(status_guard);
        items.own(super::query_cancel_guard(tx.clone(), self.inner.clone()));
        items.own(super::retain_cell_guard(ready.clone().lock()));
        items.own(super::retain_cell_guard(total_count.clone().lock()));
        items.own(super::retain_cell_guard(window.clone().lock()));
        items.own(super::retain_cell_guard(page_state.clone().lock()));
        items.own(super::list_watch_cache_guard(
            cache_key.clone(),
            tx,
            self.inner.clone(),
        ));
        let watch = WindowedQueryWatch {
            items: items.lock(),
            ready: ready.lock(),
            total_count: total_count.lock(),
            window: window.lock(),
            state: page_state.lock(),
            tx: early_watch.tx,
            client: early_watch.client,
        };
        self.cache_window_watch(cache_key, &watch);
        watch
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier, Mutex},
        thread,
    };

    use autosocket::{SocketConnectionStatus, SocketTransport, WsFrame};
    use hyphae::{Cell, CellImmutable, CellMap, CellMutable, Gettable, Mutable};

    use super::apply_incremental_map_update;
    #[cfg(feature = "demo")]
    use crate::entities::demo::GetDemoTasksWithStatus;
    use crate::{
        client::MykoClient,
        entities::client::GetAllClients,
        wire::{QueryWindow, QueryWindowUpdate},
    };

    struct MockTransport {
        status: Cell<SocketConnectionStatus, CellMutable>,
        sent: Mutex<Vec<WsFrame>>,
        incoming_rx: flume::Receiver<WsFrame>,
    }

    impl MockTransport {
        fn new() -> Self {
            let (_incoming_tx, incoming_rx) = flume::unbounded();
            Self {
                status: Cell::new(SocketConnectionStatus::Idle),
                sent: Mutex::new(Vec::new()),
                incoming_rx,
            }
        }

        fn set_status(&self, status: SocketConnectionStatus) {
            self.status.set(status);
        }

        fn sent_frames(&self) -> Vec<WsFrame> {
            self.sent
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl SocketTransport for MockTransport {
        fn set_addr(&self, _addr: Option<String>) {}

        fn close(&self) {
            self.status.set(SocketConnectionStatus::Idle);
        }

        fn intended_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
            self.status.clone().lock()
        }

        fn actual_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable> {
            self.status.clone().lock()
        }

        fn send(&self, frame: WsFrame) -> Result<(), String> {
            self.sent
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(frame);
            Ok(())
        }

        fn read_rx(&self) -> flume::Receiver<WsFrame> {
            self.incoming_rx.clone()
        }
    }

    #[test]
    fn overlapping_delete_and_upsert_keeps_upserted_value() {
        let map = CellMap::<Arc<str>, Arc<String>>::new();
        map.insert("task-1".into(), Arc::new("old".to_owned()));

        apply_incremental_map_update(
            &map,
            vec!["task-1".into()],
            vec![("task-1".into(), Arc::new("new".to_owned()))],
        );

        assert_eq!(
            map.get_value(&Arc::<str>::from("task-1"))
                .as_deref()
                .map(String::as_str),
            Some("new")
        );
    }

    #[test]
    fn reconnect_resends_same_query_and_restores_ready_snapshot() {
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let watch = client.watch_query_map_state(GetAllClients {});

        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        let first_frames = transport
            .sent_frames()
            .into_iter()
            .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query")))
            .collect::<Vec<_>>();
        assert_eq!(first_frames.len(), 1);
        let Some(WsFrame::Text(first_frame)) = first_frames.first() else {
            return;
        };
        let parsed_request = serde_json::from_str::<serde_json::Value>(first_frame);
        assert!(parsed_request.is_ok());
        let Ok(request) = parsed_request else {
            return;
        };
        let request_tx = request
            .get("data")
            .and_then(|data| data.get("query"))
            .and_then(|query| query.get("tx"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        assert!(request_tx.is_some());
        let Some(tx) = request_tx else {
            return;
        };

        let initial = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": &tx,
                "sequence": 0,
                "deletes": [],
                "upserts": [{
                    "item": {
                        "id": "client-1",
                        "serverId": "server-1",
                        "address": null,
                        "windback": null
                    },
                    "itemType": "client"
                }]
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(initial.to_string()));
        assert!(watch.ready().get());
        assert_eq!(watch.map().snapshot().len(), 1);

        transport.set_status(SocketConnectionStatus::Disconnected);
        assert!(!watch.ready().get());
        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));

        let reconnected_frames = transport
            .sent_frames()
            .into_iter()
            .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query")))
            .collect::<Vec<_>>();
        assert_eq!(reconnected_frames.len(), 2);
        assert!(matches!(
            reconnected_frames.as_slice(),
            [WsFrame::Text(first), WsFrame::Text(second)] if first == second
        ));

        let restored = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": &tx,
                "sequence": 0,
                "deletes": [],
                "upserts": [{
                    "item": {
                        "id": "client-2",
                        "serverId": "server-1",
                        "address": null,
                        "windback": null
                    },
                    "itemType": "client"
                }]
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(restored.to_string()));

        assert!(watch.ready().get());
        let snapshot = watch.map().snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot.first().map(|(id, _)| id.as_ref()),
            Some("client-2")
        );
    }

    #[test]
    fn list_watch_readiness_tracks_authoritative_response_epochs() {
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let watch = client.watch_query_state(GetAllClients {});
        assert!(!watch.ready().get());
        assert!(watch.items().get().is_empty());

        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        let frames = transport.sent_frames();
        let Some(WsFrame::Text(frame)) = frames.first() else {
            return;
        };
        let request = serde_json::from_str::<serde_json::Value>(frame);
        assert!(request.is_ok());
        let Ok(request) = request else {
            return;
        };
        let tx = request
            .pointer("/data/query/tx")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        assert!(tx.is_some());
        let Some(tx) = tx else {
            return;
        };

        let empty = serde_json::json!({
            "event": "ws:m:query-response",
            "data": { "tx": &tx, "sequence": 0, "deletes": [], "upserts": [] }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(empty.to_string()));
        assert!(
            watch.ready().get(),
            "an authoritative empty result is ready"
        );
        assert!(watch.items().get().is_empty());

        transport.set_status(SocketConnectionStatus::Disconnected);
        assert!(!watch.ready().get());
        let delayed = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": &tx,
                "sequence": 1,
                "deletes": [],
                "upserts": [{
                    "item": {
                        "id": "stale-client",
                        "serverId": "server-1",
                        "address": null,
                        "windback": null
                    },
                    "itemType": "client"
                }]
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(delayed.to_string()));
        assert!(!watch.ready().get());
        assert!(watch.items().get().is_empty());

        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        let restored = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": &tx,
                "sequence": 0,
                "deletes": [],
                "upserts": [{
                    "item": {
                        "id": "fresh-client",
                        "serverId": "server-1",
                        "address": null,
                        "windback": null
                    },
                    "itemType": "client"
                }]
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(restored.to_string()));
        assert!(watch.ready().get());
        assert_eq!(watch.items().get().len(), 1);
    }

    #[test]
    fn query_list_apis_share_one_wire_subscription() {
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let first = client.watch_query_state(GetAllClients {});
        let second = client.watch_query(GetAllClients {});

        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        let query_frames = transport
            .sent_frames()
            .into_iter()
            .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query\"")))
            .collect::<Vec<_>>();
        assert_eq!(query_frames.len(), 1);
        let Some(WsFrame::Text(frame)) = query_frames.first() else {
            return;
        };
        let request = serde_json::from_str::<serde_json::Value>(frame);
        assert!(request.is_ok());
        let Ok(request) = request else {
            return;
        };
        let tx = request
            .pointer("/data/query/tx")
            .and_then(serde_json::Value::as_str);
        assert!(tx.is_some());
        let Some(tx) = tx else {
            return;
        };

        let initial = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": tx,
                "sequence": 0,
                "deletes": [],
                "upserts": [{
                    "item": {
                        "id": "shared-client",
                        "serverId": "server-1",
                        "address": null,
                        "windback": null
                    },
                    "itemType": "client"
                }]
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(initial.to_string()));
        assert!(first.ready().get());
        assert_eq!(first.items().get().len(), 1);
        assert_eq!(second.get().len(), 1);

        drop(first);
        assert!(transport.sent_frames().iter().all(
            |frame| !matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query-cancel"))
        ));
        drop(second);
        assert_eq!(
            transport
                .sent_frames()
                .iter()
                .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query-cancel")))
                .count(),
            1
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn windowed_query_watch_shares_orders_and_moves_one_live_subscription() {
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let initial_window = QueryWindow {
            offset: 0,
            limit: 1,
        };
        let first = client.watch_query_windowed(GetAllClients {}, initial_window.clone());
        let second = client.watch_query_windowed(GetAllClients {}, initial_window);

        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        let query_frames: Vec<_> = transport
            .sent_frames()
            .into_iter()
            .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query\"")))
            .collect();
        assert_eq!(query_frames.len(), 1);
        let Some(WsFrame::Text(frame)) = query_frames.first() else {
            return;
        };
        let request = serde_json::from_str::<serde_json::Value>(frame);
        assert!(request.is_ok());
        let Ok(request) = request else {
            return;
        };
        assert_eq!(
            request.pointer("/data/window/limit"),
            Some(&serde_json::json!(1))
        );
        let tx = request
            .pointer("/data/query/tx")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        assert!(tx.is_some());
        let Some(tx) = tx else {
            return;
        };

        let initial = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": &tx,
                "sequence": 0,
                "deletes": [],
                "upserts": [{
                    "item": {
                        "id": "client-b",
                        "serverId": "server-1",
                        "address": null,
                        "windback": null
                    },
                    "itemType": "client"
                }],
                "changes": [{
                    "kind": "windowOrder",
                    "ids": ["client-b"],
                    "total_count": 3,
                    "window": { "offset": 0, "limit": 1 }
                }],
                "totalCount": 3,
                "window": { "offset": 0, "limit": 1 }
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(initial.to_string()));
        assert!(first.ready().get());
        assert!(second.ready().get());
        assert!(
            first
                .items()
                .get()
                .first()
                .is_some_and(|item| item.id.as_ref() == "client-b")
        );
        assert_eq!(second.total_count().get(), Some(3));
        assert_eq!(
            first.state().get(),
            crate::client::WindowedQueryState {
                items: first.items().get(),
                ready: true,
                total_count: Some(3),
                window: Some(QueryWindow {
                    offset: 0,
                    limit: 1,
                }),
                page_index: Some(0),
                page_count: Some(3),
                has_previous_page: false,
                has_next_page: true,
            }
        );

        assert_eq!(first.next_page(), Ok(true));
        let window_messages: Vec<QueryWindowUpdate> = transport
            .sent_frames()
            .iter()
            .filter_map(|frame| {
                let WsFrame::Text(text) = frame else {
                    return None;
                };
                let value: serde_json::Value = serde_json::from_str(text).ok()?;
                (value.get("event")?.as_str()? == "ws:m:query-window")
                    .then(|| serde_json::from_value(value.get("data")?.clone()).ok())
                    .flatten()
            })
            .collect();
        assert_eq!(window_messages.len(), 1);
        let Some(window_message) = window_messages.first() else {
            return;
        };
        assert_eq!(window_message.tx, tx);
        assert_eq!(
            window_message.window,
            Some(QueryWindow {
                offset: 1,
                limit: 1
            })
        );

        let moved = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": &tx,
                "sequence": 1,
                "deletes": ["client-b"],
                "upserts": [{
                    "item": {
                        "id": "client-c",
                        "serverId": "server-1",
                        "address": null,
                        "windback": null
                    },
                    "itemType": "client"
                }],
                "changes": [{
                    "kind": "windowOrder",
                    "ids": ["client-c"],
                    "total_count": 3,
                    "window": { "offset": 1, "limit": 1 }
                }],
                "totalCount": 3,
                "window": { "offset": 1, "limit": 1 }
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(moved.to_string()));
        assert!(
            first
                .items()
                .get()
                .first()
                .is_some_and(|item| item.id.as_ref() == "client-c")
        );
        assert_eq!(
            second.window().get(),
            Some(QueryWindow {
                offset: 1,
                limit: 1
            })
        );
        let moved_state = second.state().get();
        assert_eq!(moved_state.page_index, Some(1));
        assert_eq!(moved_state.page_count, Some(3));
        assert!(moved_state.has_previous_page);
        assert!(moved_state.has_next_page);
        assert_eq!(first.last_page(), Ok(true));
        assert_eq!(first.set_page_index(3), Ok(false));
        let last_window_message = transport.sent_frames().iter().rev().find_map(|frame| {
            let WsFrame::Text(text) = frame else {
                return None;
            };
            let value: serde_json::Value = serde_json::from_str(text).ok()?;
            (value.get("event")?.as_str()? == "ws:m:query-window")
                .then(|| {
                    serde_json::from_value::<QueryWindowUpdate>(value.get("data")?.clone()).ok()
                })
                .flatten()
        });
        assert_eq!(
            last_window_message.and_then(|message| message.window),
            Some(QueryWindow {
                offset: 2,
                limit: 1,
            })
        );

        transport.set_status(SocketConnectionStatus::Disconnected);
        assert!(!first.state().get().ready);
        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        let frames = transport.sent_frames();
        let resumed = frames.iter().rev().find_map(|frame| {
            let WsFrame::Text(text) = frame else {
                return None;
            };
            let value: serde_json::Value = serde_json::from_str(text).ok()?;
            (value.get("event")?.as_str()? == "ws:m:query").then_some(value)
        });
        assert_eq!(
            resumed
                .as_ref()
                .and_then(|value| value.pointer("/data/window/offset")),
            Some(&serde_json::json!(1))
        );

        drop(first);
        assert!(transport.sent_frames().iter().all(
            |frame| !matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query-cancel"))
        ));
        drop(second);
        assert_eq!(
            transport
                .sent_frames()
                .iter()
                .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query-cancel")))
                .count(),
            1
        );
    }

    #[cfg(feature = "demo")]
    #[test]
    fn view_list_watch_does_not_treat_the_local_seed_as_ready() {
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let watch = client.watch_view_state(GetDemoTasksWithStatus {});
        assert!(!watch.ready().get());
        assert!(watch.items().get().is_empty());

        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        let frames = transport.sent_frames();
        let Some(WsFrame::Text(frame)) = frames.first() else {
            return;
        };
        let request = serde_json::from_str::<serde_json::Value>(frame);
        assert!(request.is_ok());
        let Ok(request) = request else {
            return;
        };
        let tx = request
            .pointer("/data/view/tx")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        assert!(tx.is_some());
        let Some(tx) = tx else {
            return;
        };

        let empty = serde_json::json!({
            "event": "ws:m:view-response",
            "data": { "tx": &tx, "sequence": 0, "deletes": [], "upserts": [] }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(empty.to_string()));
        assert!(watch.ready().get());

        transport.set_status(SocketConnectionStatus::Disconnected);
        assert!(!watch.ready().get());
    }

    #[cfg(feature = "demo")]
    #[test]
    fn view_list_apis_share_one_wire_subscription() {
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let first = client.watch_view_state(GetDemoTasksWithStatus {});
        let second = client.watch_view(GetDemoTasksWithStatus {});

        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        assert_eq!(
            transport
                .sent_frames()
                .iter()
                .filter(
                    |frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:view\""))
                )
                .count(),
            1
        );

        drop(first);
        assert!(transport.sent_frames().iter().all(
            |frame| !matches!(frame, WsFrame::Text(text) if text.contains("ws:m:view-cancel"))
        ));
        drop(second);
        assert_eq!(
            transport
                .sent_frames()
                .iter()
                .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:view-cancel")))
                .count(),
            1
        );
    }

    #[cfg(feature = "demo")]
    #[test]
    fn view_map_apis_share_one_wire_subscription() {
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let state = client.watch_view_map_state(GetDemoTasksWithStatus {});
        let map = client.watch_view_map(GetDemoTasksWithStatus {});

        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        assert_eq!(
            transport
                .sent_frames()
                .iter()
                .filter(
                    |frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:view\""))
                )
                .count(),
            1
        );

        drop(state);
        assert!(transport.sent_frames().iter().all(
            |frame| !matches!(frame, WsFrame::Text(text) if text.contains("ws:m:view-cancel"))
        ));
        drop(map);
        assert_eq!(
            transport
                .sent_frames()
                .iter()
                .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:view-cancel")))
                .count(),
            1
        );
    }
    #[test]
    fn malformed_initial_snapshot_is_atomic_and_does_not_become_ready() {
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let watch = client.watch_query_map_state(GetAllClients {});
        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        let frames = transport.sent_frames();
        assert!(matches!(frames.first(), Some(WsFrame::Text(_))));
        let Some(WsFrame::Text(frame)) = frames.first() else {
            return;
        };
        let request = serde_json::from_str::<serde_json::Value>(frame);
        assert!(request.is_ok());
        let Ok(request) = request else {
            return;
        };
        let tx = request
            .pointer("/data/query/tx")
            .and_then(serde_json::Value::as_str);
        assert!(tx.is_some());
        let Some(tx) = tx else {
            return;
        };

        let malformed = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": tx,
                "sequence": 0,
                "deletes": [],
                "upserts": [
                    {"item": {"id": "client-1", "serverId": "server-1", "address": null, "windback": null}, "itemType": "client"},
                    {"item": {"malformed": true}, "itemType": "client"}
                ]
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(malformed.to_string()));
        assert!(!watch.ready().get());
        assert!(watch.map().snapshot().is_empty());

        let valid = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": tx,
                "sequence": 0,
                "deletes": [],
                "upserts": [{"item": {"id": "client-2", "serverId": "server-1", "address": null, "windback": null}, "itemType": "client"}]
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(valid.to_string()));
        assert!(watch.ready().get());
        assert_eq!(watch.map().snapshot().len(), 1);
    }

    #[test]
    fn query_map_apis_share_subscription_and_ignore_supplied_transaction_ids() {
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let supplied = crate::query::QueryRequest::with_tx(
            GetAllClients {},
            Arc::<str>::from("caller-supplied-duplicate"),
        );
        let state = client.watch_query_map_state(&supplied);
        let map = client.watch_query_map(&supplied);
        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));

        let frames = transport.sent_frames();
        let txs = frames
            .iter()
            .filter_map(|frame| match frame {
                WsFrame::Text(frame) => serde_json::from_str::<serde_json::Value>(frame).ok(),
                WsFrame::Binary(_) => None,
            })
            .filter_map(|request| {
                request
                    .pointer("/data/query/tx")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        assert_eq!(txs.len(), 1);
        let [tx] = txs.as_slice() else {
            return;
        };
        assert_ne!(tx, "caller-supplied-duplicate");

        let initial = serde_json::json!({
            "event": "ws:m:query-response",
            "data": {
                "tx": tx,
                "sequence": 0,
                "deletes": [],
                "upserts": [{"item": {"id": "client-2", "serverId": "server-1", "address": null, "windback": null}, "itemType": "client"}]
            }
        });
        MykoClient::handle_frame(&client.inner, &WsFrame::Text(initial.to_string()));
        assert!(state.ready().get());
        assert_eq!(state.map().snapshot().len(), 1);
        assert_eq!(map.snapshot().len(), 1);

        drop(state);
        assert!(transport.sent_frames().iter().all(
            |frame| !matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query-cancel"))
        ));
        drop(map);
        assert_eq!(
            transport
                .sent_frames()
                .iter()
                .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query-cancel")))
                .count(),
            1
        );
    }

    #[test]
    fn concurrent_query_map_watch_creation_is_single_flight() {
        const CONSUMERS: usize = 8;
        let transport = Arc::new(MockTransport::new());
        let client = MykoClient::with_transport(transport.clone());
        let barrier = Arc::new(Barrier::new(CONSUMERS));
        let mut handles = Vec::with_capacity(CONSUMERS);
        for _ in 0..CONSUMERS {
            let client = client.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                client.watch_query_map_state(GetAllClients {})
            }));
        }
        let watches = handles
            .into_iter()
            .filter_map(|handle| handle.join().ok())
            .collect::<Vec<_>>();
        assert_eq!(watches.len(), CONSUMERS);

        transport.set_status(SocketConnectionStatus::Connected("ws://test".to_owned()));
        assert_eq!(
            transport
                .sent_frames()
                .iter()
                .filter(
                    |frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query\""))
                )
                .count(),
            1
        );
        drop(watches);
        assert_eq!(
            transport
                .sent_frames()
                .iter()
                .filter(|frame| matches!(frame, WsFrame::Text(text) if text.contains("ws:m:query-cancel")))
                .count(),
            1
        );
    }
}
