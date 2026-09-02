//! Reactive lifecycle primitives shared by Myko's Swift and `UniFFI` adapters.
//!
//! A concrete `UniFFI` crate still declares its application-specific exported
//! records and objects. This crate owns the transport-independent mechanics:
//! retaining any local or remote subscription owner, reading authoritative
//! Hyphae state, coalescing revisions, blocking a foreign-language worker
//! thread until the next revision, and cancelling that wait.

#![forbid(unsafe_code)]

use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use hyphae::{
    Cell, CellImmutable, CellValue, Gettable as _, Signal, SubscriptionGuard, Watchable as _,
};
use myko_federation::{
    LiveCollection, LiveCollectionRevision, LiveSubscription, LiveSubscriptionState,
};

trait SubscriptionOwner: Send {}

impl<T> SubscriptionOwner for T where T: Send {}

/// A blocking foreign-language subscription was explicitly cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Myko subscription cancelled")]
pub struct SubscriptionCancelled;

struct BlockingRevisionWaiter<R>
where
    R: CellValue,
{
    owner: Mutex<Option<Box<dyn SubscriptionOwner>>>,
    revision: Cell<R, CellImmutable>,
    last_observed: Mutex<R>,
    wake_tx: flume::Sender<()>,
    wake_rx: flume::Receiver<()>,
    changes: Mutex<Option<SubscriptionGuard>>,
    cancelled: AtomicBool,
}

impl<R> BlockingRevisionWaiter<R>
where
    R: CellValue,
{
    fn new<O>(owner: O, revision: &Cell<R, CellImmutable>) -> Self
    where
        O: Send + 'static,
    {
        let (wake_tx, wake_rx) = flume::bounded(1);
        let callback_tx = wake_tx.clone();
        let initial = AtomicBool::new(true);
        let changes = revision.subscribe(move |signal| {
            if let Signal::Value(_) = signal
                && !initial.swap(false, Ordering::AcqRel)
            {
                let _ignored = callback_tx.try_send(());
            }
        });
        Self {
            owner: Mutex::new(Some(Box::new(owner))),
            revision: revision.clone(),
            last_observed: Mutex::new(revision.get()),
            wake_tx,
            wake_rx,
            changes: Mutex::new(Some(changes)),
            cancelled: AtomicBool::new(false),
        }
    }

    fn observe_current(&self, revision: R) {
        *self.last_observed() = revision;
    }

    fn wait_for_change(&self) -> Result<(), SubscriptionCancelled> {
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                return Err(SubscriptionCancelled);
            }
            let revision = self.revision.get();
            let changed = {
                let mut last_observed = self.last_observed();
                if *last_observed == revision {
                    false
                } else {
                    *last_observed = revision;
                    true
                }
            };
            if changed {
                return Ok(());
            }
            if self.wake_rx.recv().is_err() {
                return Err(SubscriptionCancelled);
            }
        }
    }

    fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Ok(mut changes) = self.changes.lock() {
            drop(changes.take());
        }
        if let Ok(mut owner) = self.owner.lock() {
            drop(owner.take());
        }
        let _ignored = self.wake_tx.try_send(());
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn last_observed(&self) -> std::sync::MutexGuard<'_, R> {
        match self.last_observed.lock() {
            Ok(observed) => observed,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl<R> Drop for BlockingRevisionWaiter<R>
where
    R: CellValue,
{
    fn drop(&mut self) {
        self.cancel();
    }
}

/// A coherent Myko value adapted to synchronous `current`/`next` FFI calls.
///
/// The retained owner may be an in-process, local-socket, Iroh, or other
/// subscription driver. Transport choice does not affect the foreign-language
/// lifecycle. Call [`Self::cancel`] to wake a thread blocked in [`Self::next`].
pub struct BlockingSubscription<T, C = myko_federation::LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveSubscription<T, C>,
    waiter: BlockingRevisionWaiter<LiveSubscriptionState<T, C>>,
}

impl<T, C> BlockingSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Retains an arbitrary subscription owner and adapts its live value.
    #[must_use]
    pub fn new<O>(owner: O, live: &LiveSubscription<T, C>) -> Self
    where
        O: Send + 'static,
    {
        Self {
            waiter: BlockingRevisionWaiter::new(owner, live.state()),
            live: live.clone(),
        }
    }

    /// Reads the newest coherent value, cursor, and liveness without waiting.
    #[must_use]
    pub fn current(&self) -> LiveSubscriptionState<T, C> {
        let current = self.live.current();
        self.waiter.observe_current(current.clone());
        current
    }

    /// Blocks until the live value changes, then reads its newest state.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionCancelled`] after this subscription is cancelled.
    pub fn next(&self) -> Result<LiveSubscriptionState<T, C>, SubscriptionCancelled> {
        self.waiter.wait_for_change()?;
        Ok(self.current())
    }

    /// Cancels the subscription owner and wakes a blocked [`Self::next`] call.
    pub fn cancel(&self) {
        self.waiter.cancel();
    }

    /// Reports whether [`Self::cancel`] has been called.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.waiter.is_cancelled()
    }
}

/// A keyed Myko view adapted to synchronous `current`/`next` FFI calls.
///
/// Rows remain identity-preserving inside Hyphae. A typed vector is allocated
/// only when the foreign-language boundary asks for a snapshot.
pub struct BlockingCollectionSubscription<T, C = myko_federation::LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    live: LiveCollection<T, C>,
    waiter: BlockingRevisionWaiter<LiveCollectionRevision<T, C>>,
}

impl<T, C> BlockingCollectionSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Retains an arbitrary subscription owner and adapts its live collection.
    #[must_use]
    pub fn new<O>(owner: O, live: &LiveCollection<T, C>) -> Self
    where
        O: Send + 'static,
    {
        Self {
            waiter: BlockingRevisionWaiter::new(owner, live.revision()),
            live: live.clone(),
        }
    }

    /// Reads the newest coherent collection snapshot without waiting.
    #[must_use]
    pub fn current(&self) -> LiveSubscriptionState<Vec<T>, C> {
        let revision = self.live.revision().get();
        let state = self.live.current_state();
        let current = LiveSubscriptionState {
            value: Some(
                self.live
                    .rows()
                    .snapshot()
                    .into_iter()
                    .map(|(_, value)| value.as_ref().clone())
                    .collect(),
            ),
            through: state.through,
            liveness: state.liveness,
        };
        self.waiter.observe_current(revision);
        current
    }

    /// Blocks until the collection changes, then reads its newest snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionCancelled`] after this subscription is cancelled.
    pub fn next(&self) -> Result<LiveSubscriptionState<Vec<T>, C>, SubscriptionCancelled> {
        self.waiter.wait_for_change()?;
        Ok(self.current())
    }

    /// Cancels the subscription owner and wakes a blocked [`Self::next`] call.
    pub fn cancel(&self) {
        self.waiter.cancel();
    }

    /// Reports whether [`Self::cancel`] has been called.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.waiter.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    };

    use myko_federation::{
        LiveCollectionState, LiveSubscriptionState, SubscriptionLiveness, live_collection,
        live_subscription,
    };

    use super::*;

    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn value_stream_coalesces_revisions_and_retains_latest_state() {
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(1_u32),
            through: None::<myko_federation::LogPosition>,
            liveness: SubscriptionLiveness::Current,
        });
        let subscription = BlockingSubscription::new((), &live);

        writer.publish(2, None);
        writer.publish(3, None);

        assert_eq!(subscription.next(), Ok(live.current()));
        assert_eq!(subscription.current().value, Some(3));
    }

    #[test]
    fn next_waits_for_a_revision_newer_than_current() {
        let (writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(1_u32),
            through: None::<myko_federation::LogPosition>,
            liveness: SubscriptionLiveness::Current,
        });
        let subscription = Arc::new(BlockingSubscription::new((), &live));
        writer.publish(2, None);
        assert_eq!(subscription.current().value, Some(2));

        let waiting = Arc::clone(&subscription);
        let (result_tx, result_rx) = flume::bounded(1);
        let consumer = thread::spawn(move || {
            let _ignored = result_tx.send(waiting.next());
        });
        assert!(matches!(
            result_rx.recv_timeout(Duration::from_millis(50)),
            Err(flume::RecvTimeoutError::Timeout)
        ));

        writer.publish(3, None);
        assert_eq!(
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .map(|result| result.map(|state| state.value)),
            Ok(Ok(Some(3)))
        );
        assert!(consumer.join().is_ok());
    }

    #[test]
    fn collection_stream_projects_rows_only_at_the_ffi_boundary() {
        let (writer, live) = live_collection(
            vec![(Arc::<str>::from("one"), Arc::new(1_u32))],
            LiveCollectionState {
                through: None::<myko_federation::LogPosition>,
                liveness: SubscriptionLiveness::Current,
            },
        );
        let subscription = BlockingCollectionSubscription::new((), &live);
        assert_eq!(subscription.current().value, Some(vec![1]));

        writer.replace_all(vec![(Arc::<str>::from("two"), Arc::new(2_u32))], None);

        assert_eq!(
            subscription.next().map(|state| state.value),
            Ok(Some(vec![2]))
        );
    }

    #[test]
    fn cancellation_drops_owner_and_wakes_a_blocked_consumer() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (_writer, live) = live_subscription(LiveSubscriptionState {
            value: Some(1_u32),
            through: None::<myko_federation::LogPosition>,
            liveness: SubscriptionLiveness::Current,
        });
        let subscription = Arc::new(BlockingSubscription::new(
            DropMarker(Arc::clone(&dropped)),
            &live,
        ));
        let waiting = Arc::clone(&subscription);
        let consumer = thread::spawn(move || waiting.next());

        subscription.cancel();

        let joined = consumer.join();
        assert!(matches!(joined, Ok(Err(SubscriptionCancelled))));
        assert!(dropped.load(Ordering::Acquire));
        assert!(subscription.is_cancelled());
    }
}
