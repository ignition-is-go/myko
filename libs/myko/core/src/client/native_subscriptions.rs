use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use hyphae::{SubscriptionGuard, Watchable as _};
use serde_json::Value;

use super::{ConnectionStatus, MykoClient, MykoClientInner};
use crate::wire::{ClientQueryResponse, MykoMessage, WrappedQuery, WrappedView};

mod initial;
mod watch;
pub use initial::NativeQueryInitial;

/// Bounds subscription requests, including dropped watches awaiting server installation or cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeSubscriptionLimits {
    /// Live watches and dropped watches awaiting initial response or cancellation share this limit.
    pub max_subscriptions: usize,
    /// Combined encoded startup and cancellation reservations, using the larger JSON/CBOR size.
    pub max_retained_request_bytes: usize,
    /// Maximum combined encoded startup and cancellation reservation for one subscription.
    pub max_request_bytes: usize,
}

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionAdmissionError {
    InvalidLimits,
    ConfigurationConflict,
    CapacityExceeded,
    RequestTooLarge,
    Encoding,
    ClientClosed,
}

impl fmt::Display for SubscriptionAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "subscription limits must be positive and per-request bytes cannot exceed total bytes",
            Self::ConfigurationConflict => "the client already has different native subscription limits",
            Self::CapacityExceeded => "native subscription capacity is exhausted",
            Self::RequestTooLarge => "native subscription request exceeds its byte limit",
            Self::Encoding => "could not encode native subscription request",
            Self::ClientClosed => "subscription client has closed",
        })
    }
}

impl std::error::Error for SubscriptionAdmissionError {}

/// Fallible, bounded watches. Legacy watches on `MykoClient` retain their existing behavior.
///
/// Dropping an admitted startup retains capacity until its genuine initial response permits
/// cancellation, or its actual connection closes. This supports servers whose pending factories
/// cannot yet be cancelled. Result collections are not covered by the encoded request budget.
#[derive(Clone)]
pub struct NativeSubscriptions {
    scheduler: Arc<Scheduler>,
}

#[derive(Default)]
struct Usage {
    entries: usize,
    bytes: usize,
}

pub(super) struct Scheduler {
    client: Weak<MykoClientInner>,
    limits: NativeSubscriptionLimits,
    usage: Mutex<Usage>,
    dirty: Mutex<VecDeque<Arc<Lease>>>,
    dispatch: Arc<parking_lot::ReentrantMutex<()>>,
}

struct Reservation {
    scheduler: Weak<Scheduler>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(scheduler) = self.scheduler.upgrade() {
            let mut usage = scheduler
                .usage
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            usage.entries = usage.entries.saturating_sub(1);
            usage.bytes = usage.bytes.saturating_sub(self.bytes);
        }
    }
}

enum Request {
    Query(WrappedQuery),
    View(WrappedView),
}

struct Attempt {
    tx: Arc<str>,
    admitted: bool,
    initial: bool,
    epoch: Arc<DeliveryEpoch>,
}

struct DeliveryEpoch(AtomicBool);

impl DeliveryEpoch {
    fn is_current(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    fn retire(&self) {
        self.0.store(false, Ordering::Release);
    }
}

type Completion = Box<dyn FnOnce() + Send>;
type Delivery =
    Arc<dyn Fn(Arc<DeliveryEpoch>, ClientQueryResponse) -> Option<Completion> + Send + Sync>;
type Reset = Arc<dyn Fn() -> Option<Completion> + Send + Sync>;

struct PreparedResponse {
    completion: Option<Completion>,
    cancel: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Owner {
    Preparing,
    Active,
    Closing,
    Retired,
}

struct LeaseState {
    owner: Owner,
    connected: bool,
    dirty: bool,
    in_flight: bool,
    attempt: Option<Attempt>,
    delivery: Option<Delivery>,
    reset: Option<Reset>,
    reservation: Option<Reservation>,
}

struct Lease {
    scheduler: Weak<Scheduler>,
    request: Request,
    state: Mutex<LeaseState>,
    status_guard: Mutex<Option<SubscriptionGuard>>,
    dispatch: Arc<parking_lot::ReentrantMutex<()>>,
}

impl MykoClient {
    /// Configure one shared bounded subscription capability. Identical limits reuse its budget.
    ///
    /// # Errors
    /// Invalid limits or conflicting configuration are rejected. This does not bound legacy watch APIs.
    pub fn native_subscriptions(
        &self,
        limits: NativeSubscriptionLimits,
    ) -> Result<NativeSubscriptions, SubscriptionAdmissionError> {
        if limits.max_subscriptions == 0
            || limits.max_request_bytes == 0
            || limits.max_retained_request_bytes == 0
            || limits.max_request_bytes > limits.max_retained_request_bytes
        {
            return Err(SubscriptionAdmissionError::InvalidLimits);
        }
        let mut configured = self
            .inner
            .native_subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(scheduler) = configured.as_ref() {
            if scheduler.limits != limits {
                return Err(SubscriptionAdmissionError::ConfigurationConflict);
            }
            return Ok(NativeSubscriptions {
                scheduler: scheduler.clone(),
            });
        }
        let scheduler = Arc::new(Scheduler {
            client: Arc::downgrade(&self.inner),
            limits,
            usage: Mutex::new(Usage::default()),
            dirty: Mutex::new(VecDeque::new()),
            dispatch: Arc::new(parking_lot::ReentrantMutex::new(())),
        });
        *configured = Some(scheduler.clone());
        drop(configured);
        Ok(NativeSubscriptions { scheduler })
    }
}

impl NativeSubscriptions {
    /// Retry at most `max_work` dirty requests or cancellations, including rejected attempts.
    /// Call when transport capacity becomes available; no reconnect is required.
    pub fn retry_pending_subscriptions(&self, max_work: usize) -> usize {
        let mut attempted = 0;
        while attempted < max_work {
            let lease = self
                .scheduler
                .dirty
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front();
            let Some(lease) = lease else {
                break;
            };
            lease
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .dirty = false;
            lease.drive();
            attempted = attempted.saturating_add(1);
        }
        attempted
    }

    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.scheduler
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
    }

    #[must_use]
    pub fn retained_request_bytes(&self) -> usize {
        self.scheduler
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bytes
    }

    #[must_use]
    pub fn pending_subscription_count(&self) -> usize {
        self.scheduler
            .dirty
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    fn client(&self) -> Result<MykoClient, SubscriptionAdmissionError> {
        self.scheduler
            .client
            .upgrade()
            .map(|inner| MykoClient { inner })
            .ok_or(SubscriptionAdmissionError::ClientClosed)
    }

    fn lease(
        &self,
        request: Request,
        delivery: Delivery,
        reset: Reset,
    ) -> Result<Arc<Lease>, SubscriptionAdmissionError> {
        let tx = super::next_subscription_tx();
        let cancel = request.cancel(&tx);
        let bytes = encoded_size(&request.message(&tx)?)?
            .checked_add(encoded_size(&cancel)?)
            .ok_or(SubscriptionAdmissionError::RequestTooLarge)?;
        if bytes > self.scheduler.limits.max_request_bytes {
            return Err(SubscriptionAdmissionError::RequestTooLarge);
        }
        let mut usage = self
            .scheduler
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next_entries = usage
            .entries
            .checked_add(1)
            .ok_or(SubscriptionAdmissionError::CapacityExceeded)?;
        let next_bytes = usage
            .bytes
            .checked_add(bytes)
            .ok_or(SubscriptionAdmissionError::CapacityExceeded)?;
        if next_entries > self.scheduler.limits.max_subscriptions
            || next_bytes > self.scheduler.limits.max_retained_request_bytes
        {
            return Err(SubscriptionAdmissionError::CapacityExceeded);
        }
        usage.entries = next_entries;
        usage.bytes = next_bytes;
        drop(usage);
        let lease = Arc::new(Lease {
            scheduler: Arc::downgrade(&self.scheduler),
            request,
            state: Mutex::new(LeaseState {
                owner: Owner::Preparing,
                connected: false,
                dirty: false,
                in_flight: false,
                attempt: None,
                delivery: Some(delivery),
                reset: Some(reset),
                reservation: Some(Reservation {
                    scheduler: Arc::downgrade(&self.scheduler),
                    bytes,
                }),
            }),
            status_guard: Mutex::new(None),
            dispatch: self.scheduler.dispatch.clone(),
        });
        let weak = Arc::downgrade(&lease);
        let guard = self.client()?.connection_status().subscribe(move |signal| {
            if let hyphae::Signal::Value(status) = signal
                && let Some(lease) = weak.upgrade()
            {
                lease.connection(matches!(&**status, ConnectionStatus::Connected(_)));
            }
        });
        *lease
            .status_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(guard);
        Ok(lease)
    }
}

fn encoded_size(message: &MykoMessage) -> Result<usize, SubscriptionAdmissionError> {
    let json = serde_json::to_vec(message).map_err(|_| SubscriptionAdmissionError::Encoding)?;
    let mut cbor = Vec::new();
    ciborium::ser::into_writer(message, &mut cbor)
        .map_err(|_| SubscriptionAdmissionError::Encoding)?;
    Ok(json.len().max(cbor.len()))
}

impl Request {
    fn cancel(&self, tx: &str) -> MykoMessage {
        let cancel = crate::wire::CancelSubscription { tx: tx.to_owned() };
        match self {
            Self::Query(_) => MykoMessage::QueryCancel(cancel),
            Self::View(_) => MykoMessage::ViewCancel(cancel),
        }
    }

    fn message(&self, tx: &str) -> Result<MykoMessage, SubscriptionAdmissionError> {
        match self {
            Self::Query(wrapped) => {
                let mut wrapped = wrapped.clone();
                *wrapped
                    .query
                    .get_mut("tx")
                    .ok_or(SubscriptionAdmissionError::Encoding)? = Value::String(tx.to_owned());
                Ok(MykoMessage::Query(wrapped))
            }
            Self::View(wrapped) => {
                let mut wrapped = wrapped.clone();
                *wrapped
                    .view
                    .get_mut("tx")
                    .ok_or(SubscriptionAdmissionError::Encoding)? = Value::String(tx.to_owned());
                Ok(MykoMessage::View(wrapped))
            }
        }
    }
}

impl Lease {
    fn start(self: &Arc<Self>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.owner != Owner::Preparing {
            return;
        }
        state.owner = Owner::Active;
        let connected = state.connected;
        drop(state);
        if connected {
            self.drive();
        }
    }

    fn enqueue(self: &Arc<Self>) {
        let Some(scheduler) = self.scheduler.upgrade() else {
            return;
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.owner == Owner::Retired || state.dirty {
            return;
        }
        scheduler
            .dirty
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(self.clone());
        state.dirty = true;
    }

    fn remove_handler(&self, tx: &str) {
        if let Some(client) = self
            .scheduler
            .upgrade()
            .and_then(|scheduler| scheduler.client.upgrade())
        {
            client.query_handlers.remove(tx);
        }
    }

    fn connection(self: &Arc<Self>, connected: bool) {
        hyphae::batch(|| {
            let dispatch = self.dispatch.lock();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.owner == Owner::Retired || (!connected && !state.connected) {
                return;
            }
            state.connected = connected;
            let old = state.attempt.take();
            if let Some(old) = &old {
                old.epoch.retire();
            }
            let reset = state.reset.clone();
            let active = state.owner == Owner::Active;
            let closing = state.owner == Owner::Closing;
            drop(state);
            if let Some(old) = old {
                self.remove_handler(&old.tx);
            }
            let completion = reset.and_then(|reset| reset());
            drop(dispatch);
            if let Some(completion) = completion {
                completion();
            }
            if closing {
                self.retire();
            } else if connected && active {
                self.drive();
            }
        });
    }

    fn close(self: &Arc<Self>) {
        if self
            .scheduler
            .upgrade()
            .and_then(|scheduler| scheduler.client.upgrade())
            .is_none()
        {
            self.retire();
            return;
        }
        hyphae::batch(|| {
            let dispatch = self.dispatch.lock();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.owner == Owner::Retired {
                return;
            }
            state.owner = Owner::Closing;
            if let Some(attempt) = &state.attempt {
                attempt.epoch.retire();
            }
            let delivery = state.delivery.take();
            let reset = state.reset.take();
            let retire = !state.in_flight
                && state
                    .attempt
                    .as_ref()
                    .is_none_or(|attempt| !attempt.admitted);
            let cancel = state
                .attempt
                .as_ref()
                .is_some_and(|attempt| attempt.initial);
            drop(state);
            let completion = reset.and_then(|reset| reset());
            drop(dispatch);
            drop(delivery);
            if let Some(completion) = completion {
                completion();
            }
            if retire {
                self.retire();
            } else if cancel {
                self.drive();
            }
        });
    }

    fn retire(&self) {
        hyphae::batch(|| {
            let dispatch = self.dispatch.lock();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.owner = Owner::Retired;
            let attempt = state.attempt.take();
            if let Some(attempt) = &attempt {
                attempt.epoch.retire();
            }
            let reservation = if state.in_flight {
                None
            } else {
                state.reservation.take()
            };
            let delivery = state.delivery.take();
            let reset = state.reset.take();
            drop(state);
            let completion = reset.and_then(|reset| reset());
            if let Some(attempt) = attempt {
                self.remove_handler(&attempt.tx);
            }
            if let Some(scheduler) = self.scheduler.upgrade() {
                scheduler
                    .dirty
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .retain(|lease| !std::ptr::eq(lease.as_ref(), self));
            }
            drop(reservation);
            drop(dispatch);
            drop(delivery);
            if let Some(completion) = completion {
                completion();
            }
        });
    }

    fn response(self: &Arc<Self>, tx: &str, value: Value) {
        hyphae::batch(|| {
            let Some(prepared) = self.prepare_response(tx, value) else {
                return;
            };
            if let Some(completion) = prepared.completion {
                completion();
            }
            if prepared.cancel {
                self.drive();
            }
        });
    }

    fn prepare_response(&self, tx: &str, value: Value) -> Option<PreparedResponse> {
        let Ok(response) = serde_json::from_value::<ClientQueryResponse>(value) else {
            return None;
        };
        let dispatch = self.dispatch.lock();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.owner == Owner::Retired {
            return None;
        }
        let attempt = state.attempt.as_mut()?;
        if attempt.tx.as_ref() != tx || response.tx.as_ref() != tx {
            return None;
        }
        if response.sequence == 0 {
            attempt.initial = true;
        }
        let epoch = attempt.epoch.clone();
        let delivery = state.delivery.clone();
        let cancel = state.owner == Owner::Closing && response.sequence == 0;
        drop(state);
        let completion = delivery.and_then(|delivery| delivery(epoch, response));
        drop(dispatch);
        Some(PreparedResponse { completion, cancel })
    }

    fn drive(self: &Arc<Self>) {
        let Some(client) = self
            .scheduler
            .upgrade()
            .and_then(|scheduler| scheduler.client.upgrade())
            .map(|inner| MykoClient { inner })
        else {
            return;
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.owner == Owner::Retired || !state.connected {
            return;
        }
        if state.in_flight {
            drop(state);
            self.enqueue();
            return;
        }
        if state.attempt.is_none() && state.owner == Owner::Active {
            let tx = super::next_subscription_tx();
            let captured = tx.clone();
            let lease = self.clone();
            client.inner.query_handlers.insert(
                tx.clone(),
                Arc::new(move |value| lease.response(&captured, value)),
            );
            state.attempt = Some(Attempt {
                tx,
                admitted: false,
                initial: false,
                epoch: Arc::new(DeliveryEpoch(AtomicBool::new(true))),
            });
        }
        let Some(attempt) = state.attempt.as_ref() else {
            return;
        };
        let cancel = state.owner == Owner::Closing;
        if cancel && (!attempt.initial || !attempt.admitted) {
            return;
        }
        if !cancel && attempt.admitted {
            return;
        }
        let tx = attempt.tx.clone();
        let message = if cancel {
            Ok(self.request.cancel(&tx))
        } else {
            self.request.message(&tx).map_err(|error| error.to_string())
        };
        state.in_flight = true;
        drop(state);
        let result = message
            .and_then(|message| client.encode_message(&message))
            .and_then(|frame| client.inner.socket.send(frame));
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight = false;
        if state
            .attempt
            .as_ref()
            .is_none_or(|attempt| attempt.tx != tx)
        {
            let retired = matches!(state.owner, Owner::Closing | Owner::Retired);
            drop(state);
            if retired {
                self.retire();
            }
            return;
        }
        if result.is_ok()
            && !cancel
            && let Some(attempt) = state.attempt.as_mut()
        {
            attempt.admitted = true;
        }
        let retire = (cancel && result.is_ok())
            || (state.owner == Owner::Closing && result.is_err() && !cancel);
        let retry = result.is_err() && !retire;
        let cancel_next = !cancel
            && state.owner == Owner::Closing
            && state
                .attempt
                .as_ref()
                .is_some_and(|attempt| attempt.initial);
        drop(state);
        if retire {
            self.retire();
        } else if retry || cancel_next {
            self.enqueue();
        }
    }
}
