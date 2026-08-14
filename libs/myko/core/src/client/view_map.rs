use std::sync::Arc;

use hyphae::{Cell, CellImmutable, CellMap, CellMutable, Mutable as _, Watchable as _};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::{debug, error, trace};

use super::{
    ConnectionStatus, MykoClient,
    map_response::{MapSequence, decode_map_upserts},
    query_map::apply_incremental_map_update,
};
use crate::{
    common::with_id::WithId,
    core::{
        item::Eventable,
        view::{ViewParams, ViewRequest},
    },
    wire::{message::MykoMessage, wrap_view},
};

/// Fine-grained view data together with explicit initial-response readiness.
pub struct ViewMapWatch<T: hyphae::CellValue> {
    map: CellMap<Arc<str>, Arc<T>, CellImmutable>,
    ready: Cell<bool, CellImmutable>,
}

impl<T: hyphae::CellValue> ViewMapWatch<T> {
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
    /// Watch a view with stable, independently reactive item cells.
    pub fn watch_view_map<V>(
        &self,
        view: impl Into<ViewRequest<V>>,
    ) -> CellMap<Arc<str>, Arc<V::Item>, CellImmutable>
    where
        V: ViewParams + Clone,
        V::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        self.watch_view_map_state(view).into_map()
    }

    /// Watch a fine-grained view map and retain an initial-response signal.
    #[allow(clippy::too_many_lines)]
    pub fn watch_view_map_state<V>(&self, view: impl Into<ViewRequest<V>>) -> ViewMapWatch<V::Item>
    where
        V: ViewParams + Clone,
        V::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        let supplied: ViewRequest<V> = view.into();
        let view = ViewRequest::with_tx(supplied.view, super::next_subscription_tx());
        let tx = view.tx.clone();
        let view_id = view.view.view_id();
        let map: CellMap<Arc<str>, Arc<V::Item>> =
            CellMap::new().with_name(format!("view_map:{view_id}"));
        let map_weak = map.downgrade();
        let ready =
            Cell::<bool, CellMutable>::new(false).with_name(format!("view_map_ready:{view_id}"));
        let ready_weak = ready.downgrade();
        let ready_read = ready.clone().lock();

        let Ok(wrapped) = wrap_view(tx.clone(), &view.view) else {
            error!("Could not serialize view map request for {view_id}");
            return ViewMapWatch {
                map: map.lock(),
                ready: ready_read,
            };
        };
        let Ok(frame) = self.encode_message(&MykoMessage::View(wrapped)) else {
            error!("Could not encode view map request for {view_id}");
            return ViewMapWatch {
                map: map.lock(),
                ready: ready_read,
            };
        };

        let tx_for_handler = tx.clone();
        let view_id_for_handler = view_id.clone();
        let sequences = Arc::new(MapSequence::new());
        let sequences_for_handler = Arc::clone(&sequences);
        let handler: super::QueryHandler = Box::new(move |response_value: Value| {
            let Some(map_writer) = map_weak.upgrade() else {
                return;
            };
            let response =
                match serde_json::from_value::<crate::wire::ClientQueryResponse>(response_value) {
                    Ok(response) => response,
                    Err(error) => {
                        error!(
                            "Rejected view '{}' malformed response: {}",
                            view_id_for_handler, error
                        );
                        return;
                    }
                };
            if response.tx != tx_for_handler {
                return;
            }

            let upserts = match decode_map_upserts::<V::Item, _>(response.upserts, WithId::id) {
                Ok(upserts) => upserts,
                Err(error) => {
                    error!(
                        "Rejected view '{}' response: invalid {} upsert: {}",
                        view_id_for_handler,
                        std::any::type_name::<V::Item>(),
                        error
                    );
                    return;
                }
            };
            if !sequences_for_handler.accept(response.sequence) {
                error!(
                    "Rejected view '{}' out-of-order sequence {}",
                    view_id_for_handler, response.sequence
                );
                return;
            }
            let is_initial_response = response.sequence == 0;
            if is_initial_response {
                trace!("Sequence reset: replacing {} view map", view_id_for_handler);
                map_writer.replace_all(upserts);
            } else {
                apply_incremental_map_update(&map_writer, response.deletes, upserts);
            }
            if is_initial_response && let Some(ready_writer) = ready_weak.upgrade() {
                ready_writer.set(true);
            }
        });
        if !self.try_register_query_handler(tx.clone(), handler) {
            error!("Refusing duplicate view map transaction {tx}");
            return ViewMapWatch {
                map: map.lock(),
                ready: ready_read,
            };
        }

        let socket = self.inner.socket.clone();
        let ready_for_status = ready.downgrade();
        let sequences_for_status = sequences;
        let status_cell = self.connection_status();
        let send_view_id = view_id;
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                if let ConnectionStatus::Connected(_) = &**status {
                    match socket.send(frame.clone()) {
                        Ok(()) => debug!("Watching view map {send_view_id}"),
                        Err(error) => error!("Could not send view: {error:?}"),
                    }
                } else {
                    sequences_for_status.reset_epoch();
                    if let Some(ready_writer) = ready_for_status.upgrade() {
                        ready_writer.set(false);
                    }
                    debug!("View map {send_view_id} disconnected");
                }
            }
        });
        map.own(status_guard);
        map.own(super::view_cancel_guard(tx, self.inner.clone()));
        ViewMapWatch {
            map: map.lock(),
            ready: ready_read,
        }
    }
}
