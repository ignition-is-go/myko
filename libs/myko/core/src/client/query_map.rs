use std::sync::Arc;

use hyphae::{Cell, CellImmutable, CellMap, CellMutable, Mutable, Watchable};

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
    ConnectionStatus, MykoClient, QueryRequest,
    map_response::{MapSequence, decode_map_upserts},
};
use crate::{
    common::with_id::{WithId, WithTypedId},
    core::{item::Eventable, query::QueryParams},
    wire::{message::MykoMessage, query::WrappedQuery},
};

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
    use crate::{client::MykoClient, entities::client::GetAllClients};

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
