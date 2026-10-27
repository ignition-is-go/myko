use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use serde::de::DeserializeOwned;

use super::{
    Completion, Delivery, DeliveryEpoch, Lease, NativeSubscriptions, Request,
    SubscriptionAdmissionError,
};
use crate::{
    common::with_id::WithId,
    core::item::Eventable,
    query::{QueryParams, QueryRequest},
    wire::WrappedQuery,
};

struct Snapshot<T> {
    epoch: Arc<DeliveryEpoch>,
    items: Vec<Arc<T>>,
}

/// Complete authoritative query snapshots, independent of reactive-cell readiness.
///
/// At most one decoded initial result is retained. Reconnect retires any queued
/// result; the next sequence-zero response replaces it, including an empty result.
/// Multiple complete rebuilds before a receive retain only the latest snapshot.
/// Duplicate IDs use the last upsert, matching the query list-watch contract.
/// Dropping this handle closes its bounded native subscription.
/// The handle retains its client, as live watches do; dropping another client
/// reference does not cancel this receive. Use [`Self::cancel`] for cancellation.
pub struct NativeQueryInitial<T: Send + Sync> {
    slot: Arc<Mutex<Option<Snapshot<T>>>>,
    wake: flume::Receiver<()>,
    pub(super) lease: Arc<Lease>,
    _client: crate::client::MykoClient,
}

impl<T: Send + Sync> NativeQueryInitial<T> {
    /// Take a completed result from the current connection attempt, if available.
    #[must_use]
    pub fn try_snapshot(&self) -> Option<Vec<Arc<T>>> {
        take_snapshot(&self.slot)
    }

    /// Cancel the receive and close its subscription. An admitted startup retains
    /// its reservation until the genuine initial response permits cancellation.
    pub fn cancel(&self) {
        self.lease.close();
    }

    /// Wait for a complete initial result. An authoritative empty vector completes normally.
    ///
    /// # Errors
    /// Returns `ClientClosed` when the subscription retires before a result arrives.
    pub async fn recv(&self) -> Result<Vec<Arc<T>>, SubscriptionAdmissionError> {
        loop {
            if let Some(items) = self.try_snapshot() {
                return Ok(items);
            }
            self.wake
                .recv_async()
                .await
                .map_err(|_| SubscriptionAdmissionError::ClientClosed)?;
        }
    }
}

impl<T: Send + Sync> Drop for NativeQueryInitial<T> {
    fn drop(&mut self) {
        self.lease.close();
    }
}

impl NativeSubscriptions {
    /// Reserve a query whose complete initial responses bypass reactive-cell publication.
    ///
    /// The result slot and wake channel each have capacity one. Startup, reconnect,
    /// and cancellation use the same admission budgets as native live watches.
    ///
    /// # Errors
    /// Returns an error on encoding or capacity failure before starting the query.
    pub fn try_query_initial<Q>(
        &self,
        query: impl Into<QueryRequest<Q>>,
    ) -> Result<NativeQueryInitial<Q::Item>, SubscriptionAdmissionError>
    where
        Q: QueryParams + Clone,
        Q::Item: Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + 'static,
    {
        let client = self.client()?;
        let supplied = query.into();
        let request = Request::Query(WrappedQuery {
            query_id: supplied.query.query_id(),
            query_item_type: Q::query_item_type_static(),
            query: serde_json::to_value(QueryRequest::with_tx(
                supplied.query,
                super::super::next_subscription_tx(),
            ))
            .map_err(|_| SubscriptionAdmissionError::Encoding)?,
            window: None,
        });
        let slot = Arc::new(Mutex::new(None));
        let (sender, wake) = flume::bounded(1);
        let result = slot.clone();
        let delivery: Delivery = Arc::new(move |epoch, response| {
            if response.sequence != 0 || !epoch.is_current() {
                return None;
            }
            let Ok(upserts) = super::super::map_response::decode_map_upserts::<Q::Item, _>(
                response.upserts,
                WithId::id,
            ) else {
                return None;
            };
            let mut items = HashMap::new();
            let mut discarded = Vec::new();
            for (id, item) in upserts {
                if let Some(previous) = items.insert(id, item) {
                    discarded.push(previous);
                }
            }
            Some(replace_snapshot(
                &result,
                Snapshot {
                    epoch,
                    items: items.into_values().collect(),
                },
                discarded,
                sender.clone(),
            ))
        });
        let reset_slot = slot.clone();
        let reset = Arc::new(move || reset_snapshot(&reset_slot));
        let lease = self.lease(request, delivery, reset)?;
        lease.start();
        Ok(NativeQueryInitial {
            slot,
            wake,
            lease,
            _client: client,
        })
    }
}

fn take_snapshot<T>(slot: &Mutex<Option<Snapshot<T>>>) -> Option<Vec<Arc<T>>> {
    let snapshot = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    snapshot
        .filter(|snapshot| snapshot.epoch.is_current())
        .map(|snapshot| snapshot.items)
}

fn replace_snapshot<T: Send + Sync + 'static>(
    slot: &Mutex<Option<Snapshot<T>>>,
    snapshot: Snapshot<T>,
    discarded: Vec<Arc<T>>,
    sender: flume::Sender<()>,
) -> Completion {
    let retired = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .replace(snapshot);
    Box::new(move || {
        drop(retired);
        drop(discarded);
        let _ = sender.try_send(());
    })
}

fn reset_snapshot<T: Send + Sync + 'static>(
    slot: &Mutex<Option<Snapshot<T>>>,
) -> Option<Completion> {
    let retired = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    retired.map(|retired| {
        let completion: Completion = Box::new(move || drop(retired));
        completion
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    struct DropProbe(Box<dyn Fn() + Send + Sync>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            (self.0)();
        }
    }

    #[test]
    fn retired_and_discarded_items_drop_after_slot_and_dispatch_unlock() {
        let slot = Arc::new(Mutex::new(None));
        let dispatch = Arc::new(parking_lot::ReentrantMutex::new(()));
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = || {
            let weak_slot = Arc::downgrade(&slot);
            let dispatch = dispatch.clone();
            let drops = drops.clone();
            Arc::new(DropProbe(Box::new(move || {
                assert!(!dispatch.is_locked());
                if let Some(slot) = weak_slot.upgrade() {
                    assert!(slot.try_lock().is_ok());
                }
                drops.fetch_add(1, Ordering::SeqCst);
            })))
        };
        let snapshot = |item| Snapshot {
            epoch: Arc::new(DeliveryEpoch(AtomicBool::new(true))),
            items: vec![item],
        };
        *slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(snapshot(probe()));
        let (sender, _wake) = flume::bounded(1);
        let guard = dispatch.lock();
        let completion = replace_snapshot(&slot, snapshot(probe()), vec![probe()], sender);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(guard);
        completion();
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        let guard = dispatch.lock();
        let completion = reset_snapshot(&slot);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        drop(guard);
        assert!(completion.is_some());
        if let Some(completion) = completion {
            completion();
        }
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        let stale = snapshot(probe());
        stale.epoch.retire();
        *slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stale);
        assert!(take_snapshot(&slot).is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 4);
    }
}
