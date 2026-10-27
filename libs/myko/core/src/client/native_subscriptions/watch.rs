use super::super::{self as client, QueryWatch, ViewMapWatch, ViewWatch};
use super::{Delivery, NativeSubscriptions, Request, SubscriptionAdmissionError};
use crate::{
    common::with_id::WithId,
    core::item::Eventable,
    query::{QueryParams, QueryRequest},
    view::{ViewParams, ViewRequest},
    wire::{ClientQueryResponse, WrappedQuery, wrap_view},
};
use hyphae::{Cell, CellMap, CellMutable, Mutable as _, SubscriptionGuard};
use serde::de::DeserializeOwned;
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

impl NativeSubscriptions {
    /// Watch a query, reserving bounded startup and retirement capacity before publication.
    ///
    /// # Errors
    /// Returns an error on encoding or capacity failure, without registering a subscription.
    pub fn try_watch_query_state<Q>(
        &self,
        query: impl Into<QueryRequest<Q>>,
    ) -> Result<QueryWatch<Q::Item>, SubscriptionAdmissionError>
    where
        Q: QueryParams + Clone,
        Q::Item: Eventable + WithId + DeserializeOwned + Clone + fmt::Debug + 'static,
    {
        let supplied = query.into();
        let id = supplied.query.query_id();
        let item_type = Q::query_item_type_static();
        let key = format!(
            "native-query:{id}:{item_type}:{}:{:016x}",
            std::any::type_name::<Q::Item>(),
            supplied.query.cache_key_hash()
        );
        let query = QueryRequest::with_tx(supplied.query, client::next_subscription_tx());
        let request = Request::Query(WrappedQuery {
            query: serde_json::to_value(query).map_err(|_| SubscriptionAdmissionError::Encoding)?,
            query_id: id,
            query_item_type: item_type,
            window: None,
        });
        self.list_watch::<Q::Item, Arc<Q::Item>>(request, key, |item| item)
    }

    /// Watch a view with the same bounded startup and retirement policy as query watches.
    ///
    /// # Errors
    /// Returns an error on encoding or capacity failure.
    pub fn try_watch_view_state<V>(
        &self,
        view: impl Into<ViewRequest<V>>,
    ) -> Result<ViewWatch<V::Item>, SubscriptionAdmissionError>
    where
        V: ViewParams + Clone,
        V::Item: Eventable + WithId + DeserializeOwned + Clone + fmt::Debug + 'static,
    {
        let supplied = view.into();
        let key = format!(
            "native-view:{}:{}:{:016x}:{:?}",
            supplied.view.view_id(),
            std::any::type_name::<V::Item>(),
            supplied.view.cache_key_hash(),
            supplied.sample_rate
        );
        let mut wrapped = wrap_view(client::next_subscription_tx(), &supplied.view)
            .map_err(|_| SubscriptionAdmissionError::Encoding)?;
        wrapped.sample_rate = supplied.sample_rate;
        self.list_watch::<V::Item, V::Item>(Request::View(wrapped), key, |item| {
            item.as_ref().clone()
        })
    }

    fn list_watch<T, U>(
        &self,
        request: Request,
        key: String,
        convert: fn(Arc<T>) -> U,
    ) -> Result<client::ListWatch<U>, SubscriptionAdmissionError>
    where
        T: Eventable + WithId + DeserializeOwned + Clone + fmt::Debug + 'static,
        U: hyphae::CellValue,
    {
        let client = self.client()?;
        let cache_guard = client
            .inner
            .list_watch_cache_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(watch) = client.cached_list_watch(&key) {
            return Ok(watch);
        }
        let items = Cell::new(Vec::<U>::new());
        let ready = Cell::<bool, CellMutable>::new(false);
        let weak_items = items.downgrade();
        let weak_ready = ready.downgrade();
        let sequences = Arc::new(client::map_response::MapSequence::new());
        let handler_sequences = sequences.clone();
        let state = Mutex::new(HashMap::<Arc<str>, Arc<T>>::new());
        let delivery: Delivery = Arc::new(move |epoch, value| {
            if !epoch.is_current() {
                return;
            }
            let Some(items) = weak_items.upgrade() else {
                return;
            };
            let Ok(response) = serde_json::from_value::<ClientQueryResponse>(value) else {
                return;
            };
            let Ok(upserts) =
                client::map_response::decode_map_upserts::<T, _>(response.upserts, WithId::id)
            else {
                return;
            };
            if !handler_sequences.accept(response.sequence) {
                return;
            }
            let initial = response.sequence == 0;
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if initial {
                state.clear();
            }
            for id in response.deletes {
                state.remove(&id);
            }
            state.extend(upserts);
            let next = state.values().cloned().map(convert).collect();
            drop(state);
            items.set(next);
            if initial
                && epoch.is_current()
                && let Some(ready) = weak_ready.upgrade()
            {
                ready.set(true);
            }
        });
        let ready_reset = ready.downgrade();
        let reset = Arc::new(move || {
            sequences.reset_epoch();
            if let Some(ready) = ready_reset.upgrade() {
                ready.set(false);
            }
        });
        let lease = self.lease(request, delivery, reset)?;
        let owner = lease.clone();
        let identity = client::next_subscription_tx();
        items.own(SubscriptionGuard::from_callback(move || owner.close()));
        items.own(client::retain_cell_guard(ready.clone().lock()));
        items.own(client::list_watch_cache_guard(
            key.clone(),
            identity.clone(),
            client.inner.clone(),
        ));
        let watch = client::ListWatch {
            items: items.lock(),
            ready: ready.lock(),
        };
        client.cache_list_watch(key, identity, &watch);
        drop(cache_guard);
        lease.start();
        Ok(watch)
    }

    /// Watch an incremental view map. Runtime sampling controls are unavailable on this capability.
    ///
    /// # Errors
    /// Returns an error on encoding or capacity failure.
    pub fn try_watch_view_map_state<V>(
        &self,
        view: impl Into<ViewRequest<V>>,
    ) -> Result<ViewMapWatch<V::Item>, SubscriptionAdmissionError>
    where
        V: ViewParams + Clone,
        V::Item: Eventable + WithId + DeserializeOwned + Clone + fmt::Debug + 'static,
    {
        let supplied = view.into();
        let key = format!(
            "native-view-map:{}:{}:{:016x}:{:?}",
            supplied.view.view_id(),
            std::any::type_name::<V::Item>(),
            supplied.view.cache_key_hash(),
            supplied.sample_rate
        );
        let client = self.client()?;
        let cache_guard = client
            .inner
            .map_watch_cache_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((map, ready, _)) = client.cached_map_watch(&key) {
            return Ok(ViewMapWatch::native(map, ready));
        }
        let mut wrapped = wrap_view(client::next_subscription_tx(), &supplied.view)
            .map_err(|_| SubscriptionAdmissionError::Encoding)?;
        wrapped.sample_rate = supplied.sample_rate;
        let map = CellMap::<Arc<str>, Arc<V::Item>>::new();
        let ready = Cell::<bool, CellMutable>::new(false);
        let weak_map = map.downgrade();
        let weak_ready = ready.downgrade();
        let sequences = Arc::new(client::map_response::MapSequence::new());
        let handler_sequences = sequences.clone();
        let delivery: Delivery = Arc::new(move |epoch, value| {
            if !epoch.is_current() {
                return;
            }
            let Some(map) = weak_map.upgrade() else {
                return;
            };
            let Ok(response) = serde_json::from_value::<ClientQueryResponse>(value) else {
                return;
            };
            let Ok(upserts) = client::map_response::decode_map_upserts::<V::Item, _>(
                response.upserts,
                WithId::id,
            ) else {
                return;
            };
            if !handler_sequences.accept(response.sequence) {
                return;
            }
            let initial = response.sequence == 0;
            if initial {
                map.replace_all(upserts);
            } else {
                client::query_map::apply_incremental_map_update(&map, response.deletes, upserts);
            }
            if initial
                && epoch.is_current()
                && let Some(ready) = weak_ready.upgrade()
            {
                ready.set(true);
            }
        });
        let weak_ready = ready.downgrade();
        let reset = Arc::new(move || {
            sequences.reset_epoch();
            if let Some(ready) = weak_ready.upgrade() {
                ready.set(false);
            }
        });
        let lease = self.lease(Request::View(wrapped), delivery, reset)?;
        let owner = lease.clone();
        let identity = client::next_subscription_tx();
        map.own(SubscriptionGuard::from_callback(move || owner.close()));
        map.own(client::retain_cell_guard(ready.clone().lock()));
        map.own(client::map_watch_cache_guard(
            key.clone(),
            identity.clone(),
            client.inner.clone(),
        ));
        let watch = ViewMapWatch::native(map.lock(), ready.lock());
        client.cache_map_watch(key, identity, watch.map(), watch.ready(), None);
        drop(cache_guard);
        lease.start();
        Ok(watch)
    }
}
