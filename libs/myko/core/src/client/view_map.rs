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
#[derive(Clone)]
pub struct ViewMapWatch<T: hyphae::CellValue> {
    map: CellMap<Arc<str>, Arc<T>, CellImmutable>,
    ready: Cell<bool, CellImmutable>,
    sampling: Option<Arc<ViewSamplingControl>>,
}

impl<T: hyphae::CellValue> ViewMapWatch<T> {
    /// Update delivery cadence for every handle sharing this view subscription.
    /// The latest rate is retained across reconnects. None restores immediate delivery.
    ///
    /// # Errors
    /// Returns an error if the subscription or client is gone, or sending fails.
    pub fn set_sample_rate(&self, rate: Option<crate::wire::ViewSampleRate>) -> Result<(), String> {
        self.sampling
            .as_ref()
            .ok_or("view subscription is unavailable")?
            .set_rate(rate)
    }

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
    /// Identical view parameters share one decoded map and wire subscription.
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
        let view_id = supplied.view.view_id();
        let cache_key = format!(
            "view-map:{view_id}:{}:{:016x}:{:?}",
            std::any::type_name::<V::Item>(),
            supplied.view.cache_key_hash(),
            supplied.sample_rate
        );
        let _cache_gate = self
            .inner
            .map_watch_cache_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((map, ready, sampling)) = self.cached_map_watch(&cache_key) {
            debug!("watch_view_map_state: cache hit for {cache_key}");
            return ViewMapWatch {
                map,
                ready,
                sampling,
            };
        }
        self.inner.map_watch_cache.remove(&cache_key);

        let view = ViewRequest::with_tx(supplied.view, super::next_subscription_tx());
        let tx = view.tx.clone();
        let map: CellMap<Arc<str>, Arc<V::Item>> =
            CellMap::new().with_name(format!("view_map:{view_id}"));
        let map_weak = map.downgrade();
        let ready =
            Cell::<bool, CellMutable>::new(false).with_name(format!("view_map_ready:{view_id}"));
        let ready_weak = ready.downgrade();
        let ready_read = ready.clone().lock();

        let Ok(mut wrapped) = wrap_view(tx.clone(), &view.view) else {
            error!("Could not serialize view map request for {view_id}");
            return ViewMapWatch {
                map: map.lock(),
                ready: ready_read,
                sampling: None,
            };
        };
        wrapped.sample_rate = supplied.sample_rate;
        let sampling = Arc::new(ViewSamplingControl {
            request: std::sync::Mutex::new(wrapped),
            client: Arc::downgrade(&self.inner),
        });

        let tx_for_handler = tx.clone();
        let view_id_for_handler = view_id.clone();
        let sequences = Arc::new(MapSequence::new());
        let sequences_for_handler = Arc::clone(&sequences);
        let handler: super::QueryHandler = Arc::new(move |response_value: Value| {
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
                sampling: None,
            };
        }

        let sampling_for_status = sampling.clone();
        let ready_for_status = ready.downgrade();
        let sequences_for_status = sequences;
        let status_cell = self.connection_status();
        let send_view_id = view_id;
        let status_guard = status_cell.subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal {
                sequences_for_status.reset_epoch();
                if let Some(ready_writer) = ready_for_status.upgrade() {
                    ready_writer.set(false);
                }
                if let ConnectionStatus::Connected(_) = &**status {
                    match sampling_for_status.subscribe() {
                        Ok(()) => debug!("Watching view map {send_view_id}"),
                        Err(error) => error!("Could not send view: {error:?}"),
                    }
                } else {
                    debug!("View map {send_view_id} disconnected");
                }
            }
        });
        map.own(status_guard);
        map.own(super::view_cancel_guard(tx.clone(), self.inner.clone()));
        map.own(super::retain_cell_guard(ready_read.clone()));
        map.own(super::map_watch_cache_guard(
            cache_key.clone(),
            tx.clone(),
            self.inner.clone(),
        ));
        let watch = ViewMapWatch {
            map: map.lock(),
            ready: ready_read,
            sampling: Some(sampling),
        };
        self.cache_map_watch(
            cache_key,
            tx,
            &watch.map,
            &watch.ready,
            watch.sampling.clone(),
        );
        watch
    }
}

pub(super) struct ViewSamplingControl {
    request: std::sync::Mutex<crate::wire::WrappedView>,
    client: std::sync::Weak<super::MykoClientInner>,
}

// Keep rate updates ordered with reconnect subscription frames through enqueue.
#[allow(clippy::significant_drop_tightening)]
impl ViewSamplingControl {
    fn subscribe(&self) -> Result<(), String> {
        let client = MykoClient {
            inner: self.client.upgrade().ok_or("client has been dropped")?,
        };
        let request = self
            .request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let frame = client.encode_message(&MykoMessage::View(request.clone()))?;
        client
            .inner
            .socket
            .send(frame)
            .map_err(|error| format!("{error:?}"))
    }

    fn set_rate(&self, rate: Option<crate::wire::ViewSampleRate>) -> Result<(), String> {
        use hyphae::Gettable as _;
        let client = MykoClient {
            inner: self.client.upgrade().ok_or("client has been dropped")?,
        };
        let mut request = self
            .request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if request.sample_rate == rate {
            return Ok(());
        }
        request.sample_rate = rate;
        if !matches!(
            client.connection_status().get(),
            ConnectionStatus::Connected(_)
        ) {
            return Ok(());
        }
        let tx = request
            .view
            .get("tx")
            .and_then(Value::as_str)
            .ok_or("view transaction is missing")?
            .to_owned();
        let frame = client.encode_message(&MykoMessage::ViewSampleRate(
            crate::wire::ViewSampleRateUpdate {
                tx,
                sample_rate: rate,
            },
        ))?;
        client
            .inner
            .socket
            .send(frame)
            .map_err(|error| format!("{error:?}"))
    }
}
