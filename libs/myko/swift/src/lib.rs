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

use hyphae::{Signal, SubscriptionGuard, Watchable as _};
use myko_federation::{LiveCollection, LiveSubscription, LiveSubscriptionState};

trait SubscriptionOwner: Send {}

impl<T> SubscriptionOwner for T where T: Send {}

/// A blocking foreign-language subscription was explicitly cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Myko subscription cancelled")]
pub struct SubscriptionCancelled;

struct BlockingRevisionWaiter {
    owner: Mutex<Option<Box<dyn SubscriptionOwner>>>,
    wake_tx: flume::Sender<()>,
    wake_rx: flume::Receiver<()>,
    changes: Mutex<Option<SubscriptionGuard>>,
    cancelled: AtomicBool,
}

impl BlockingRevisionWaiter {
    fn new<O, T>(owner: O, revision: &hyphae::Cell<T, hyphae::CellImmutable>) -> Self
    where
        O: Send + 'static,
        T: hyphae::CellValue,
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
            wake_tx,
            wake_rx,
            changes: Mutex::new(Some(changes)),
            cancelled: AtomicBool::new(false),
        }
    }

    fn wait(&self) -> Result<(), SubscriptionCancelled> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(SubscriptionCancelled);
        }
        if self.wake_rx.recv().is_err() || self.cancelled.load(Ordering::Acquire) {
            return Err(SubscriptionCancelled);
        }
        Ok(())
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
}

impl Drop for BlockingRevisionWaiter {
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
    waiter: BlockingRevisionWaiter,
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
        self.live.current()
    }

    /// Blocks until the live value changes, then reads its newest state.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionCancelled`] after this subscription is cancelled.
    pub fn next(&self) -> Result<LiveSubscriptionState<T, C>, SubscriptionCancelled> {
        self.waiter.wait()?;
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
    waiter: BlockingRevisionWaiter,
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
        let state = self.live.current_state();
        LiveSubscriptionState {
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
        }
    }

    /// Blocks until the collection changes, then reads its newest snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionCancelled`] after this subscription is cancelled.
    pub fn next(&self) -> Result<LiveSubscriptionState<Vec<T>, C>, SubscriptionCancelled> {
        self.waiter.wait()?;
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
