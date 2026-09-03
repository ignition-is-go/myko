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

#[cfg(feature = "embedded-node")]
mod embedded_node;
#[cfg(feature = "embedded-node")]
pub use embedded_node::{EmbeddedNodeError, EmbeddedNodeHost, EmbeddedNodeInfo};

#[cfg(feature = "native-ffi")]
mod native_ffi;
#[cfg(feature = "native-ffi")]
pub use native_ffi::{
    MykoAccessOperation, MykoAuthority, MykoAuthorityConstraints, MykoAuthorityGrant,
    MykoAuthorityGrantInput, MykoAuthorityGrantRecord, MykoAuthorityGrantsSubscription,
    MykoAuthorityGrantsUpdate, MykoFederation, MykoFederationError, MykoFederationPermission,
    MykoNearbyNode, MykoNearbyNodesSubscription, MykoNearbyNodesUpdate, MykoNodeInfo,
    MykoPairedNode, MykoPairedNodesSubscription, MykoPairedNodesUpdate,
    MykoPairingInitiationSubscription, MykoPairingInitiationUpdate, MykoPairingReceipt,
    MykoPairingReceiptsSubscription, MykoPairingReceiptsUpdate, MykoPendingPairingReceipt,
    MykoPrincipal, MykoPrincipalKind, MykoRevocationKind, MykoScopeSelection,
    NativeApplicationAccess, NativeAuthorityAccess, NativeAuthorityContext,
};

#[cfg(feature = "native-ffi")]
uniffi::setup_scaffolding!();

trait SubscriptionOwner: Send {}

impl<T> SubscriptionOwner for T where T: Send {}

/// A blocking foreign-language subscription was explicitly cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Myko subscription cancelled")]
pub struct SubscriptionCancelled;

/// Exports one concrete application subscription through the uniform
/// synchronous surface consumed by `MykoSubscriptionBinding`.
///
/// `UniFFI` cannot export Rust generics, so applications still name a concrete
/// object and update record. This macro keeps the `current`/`next`/`cancel`
/// lifecycle, cancellation error conversion, and method contract in Myko.
/// The mapper is the application's only responsibility; it receives the typed
/// Myko state and the concrete owner object and returns the exported update.
#[macro_export]
macro_rules! export_blocking_subscription {
    (
        $subscription:ident => $update:ty,
        field = $field:ident,
        error = $error:ty,
        transport_error = $transport_error:path,
        map = $map:expr $(,)?
    ) => {
        #[uniffi::export]
        impl $subscription {
            /// Returns the latest coherent typed Myko revision without waiting.
            ///
            /// # Errors
            ///
            /// Returns an application bridge error when the revision cannot be
            /// projected into its exported update record.
            pub fn current(&self) -> Result<$update, $error> {
                let state = self.$field.current();
                ($map)(state, self)
            }

            /// Waits for and returns a revision newer than the last one read.
            ///
            /// # Errors
            ///
            /// Returns an application bridge error when the stream closes or
            /// its revision cannot be projected into the exported update.
            pub fn next(&self) -> Result<$update, $error> {
                let state = self
                    .$field
                    .next()
                    .map_err(|error| $transport_error(&error))?;
                ($map)(state, self)
            }

            /// Cancels the subscription and wakes a blocked [`Self::next`] call.
            pub fn cancel(&self) {
                self.$field.cancel();
            }
        }
    };
}

/// Exports one concrete keyed collection subscription as lossless typed row
/// revisions through the uniform Swift lifecycle surface.
///
/// Unlike [`export_blocking_subscription!`], the mapper receives the native
/// [`myko_federation::LiveCollectionRevision`]. Its initial `current()` call
/// contains one `MapDiff::Initial`; subsequent `next()` calls retain every
/// insert, update, remove, batch, and lifecycle-only revision in order.
#[macro_export]
macro_rules! export_blocking_collection_subscription {
    (
        $subscription:ident => $update:ty,
        field = $field:ident,
        error = $error:ty,
        transport_error = $transport_error:path,
        map = $map:expr $(,)?
    ) => {
        #[uniffi::export]
        impl $subscription {
            /// Returns a complete typed reset plus the latest collection lifecycle.
            ///
            /// # Errors
            ///
            /// Returns an application bridge error when the revision cannot be
            /// projected into its exported update record.
            pub fn current(&self) -> Result<$update, $error> {
                let revision = self.$field.current_revision();
                ($map)(revision, self)
            }

            /// Waits for and returns the next lossless typed collection revision.
            ///
            /// # Errors
            ///
            /// Returns an application bridge error when the stream closes or
            /// its revision cannot be projected into the exported update.
            pub fn next(&self) -> Result<$update, $error> {
                let revision = self
                    .$field
                    .next_revision()
                    .map_err(|error| $transport_error(&error))?;
                ($map)(revision, self)
            }

            /// Cancels the subscription and wakes a blocked [`Self::next`] call.
            pub fn cancel(&self) {
                self.$field.cancel();
            }
        }
    };
}

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

struct BlockingCollectionRevisionQueue<T, C>
where
    T: CellValue,
    C: CellValue,
{
    owner: Mutex<Option<Box<dyn SubscriptionOwner>>>,
    notifications_tx: flume::Sender<Option<LiveCollectionRevision<T, C>>>,
    notifications_rx: flume::Receiver<Option<LiveCollectionRevision<T, C>>>,
    changes: Mutex<Option<SubscriptionGuard>>,
    cancelled: AtomicBool,
}

impl<T, C> BlockingCollectionRevisionQueue<T, C>
where
    T: CellValue,
    C: CellValue,
{
    fn new<O>(owner: O, revision: &Cell<LiveCollectionRevision<T, C>, CellImmutable>) -> Self
    where
        O: Send + 'static,
    {
        let (notifications_tx, notifications_rx) = flume::unbounded();
        let callback_tx = notifications_tx.clone();
        let initial = AtomicBool::new(true);
        let changes = revision.subscribe(move |signal| {
            if let Signal::Value(revision) = signal
                && !initial.swap(false, Ordering::AcqRel)
            {
                let _ignored = callback_tx.send(Some(revision.as_ref().clone()));
            }
        });
        Self {
            owner: Mutex::new(Some(Box::new(owner))),
            notifications_tx,
            notifications_rx,
            changes: Mutex::new(Some(changes)),
            cancelled: AtomicBool::new(false),
        }
    }

    fn next_revision(&self) -> Result<LiveCollectionRevision<T, C>, SubscriptionCancelled> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(SubscriptionCancelled);
        }
        match self.notifications_rx.recv() {
            Ok(Some(revision)) => Ok(revision),
            Ok(None) | Err(_) => Err(SubscriptionCancelled),
        }
    }

    fn discard_pending_revisions(&self) {
        while matches!(self.notifications_rx.try_recv(), Ok(Some(_))) {}
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
        let _ignored = self.notifications_tx.send(None);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl<T, C> Drop for BlockingCollectionRevisionQueue<T, C>
where
    T: CellValue,
    C: CellValue,
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
    revisions: BlockingCollectionRevisionQueue<T, C>,
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
            revisions: BlockingCollectionRevisionQueue::new(owner, live.revision()),
            live: live.clone(),
        }
    }

    /// Reads the newest coherent collection snapshot without waiting.
    #[must_use]
    pub fn current(&self) -> LiveSubscriptionState<Vec<T>, C> {
        self.revisions.discard_pending_revisions();
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
        let _revision = self.revisions.next_revision()?;
        Ok(self.current())
    }

    /// Returns a complete typed reset plus the current cursor and liveness.
    ///
    /// Use this with [`Self::next_revision`] when the foreign-language adapter
    /// can apply keyed incremental changes instead of rebuilding a snapshot.
    #[must_use]
    pub fn current_revision(&self) -> LiveCollectionRevision<T, C> {
        self.revisions.discard_pending_revisions();
        LiveCollectionRevision {
            diff: Some(hyphae::MapDiff::Initial {
                entries: self.live.rows().snapshot(),
            }),
            state: self.live.current_state(),
        }
    }

    /// Blocks until the next lossless typed row or lifecycle revision.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionCancelled`] after this subscription is cancelled.
    pub fn next_revision(&self) -> Result<LiveCollectionRevision<T, C>, SubscriptionCancelled> {
        self.revisions.next_revision()
    }

    /// Cancels the subscription owner and wakes a blocked [`Self::next`] call.
    pub fn cancel(&self) {
        self.revisions.cancel();
    }

    /// Reports whether [`Self::cancel`] has been called.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.revisions.is_cancelled()
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
    fn collection_revision_stream_preserves_every_typed_row_change() {
        let one = Arc::new(1_u32);
        let (writer, live) = live_collection(
            vec![(Arc::<str>::from("one"), Arc::clone(&one))],
            LiveCollectionState {
                through: None::<myko_federation::LogPosition>,
                liveness: SubscriptionLiveness::Current,
            },
        );
        let subscription = BlockingCollectionSubscription::new((), &live);
        assert!(matches!(
            subscription.current_revision().diff,
            Some(hyphae::MapDiff::Initial { entries }) if entries == [(Arc::from("one"), one.clone())]
        ));

        let two = Arc::new(2_u32);
        writer.apply(
            hyphae::MapDiff::Insert {
                key: Arc::from("two"),
                value: Arc::clone(&two),
            },
            None,
        );
        let three = Arc::new(3_u32);
        writer.apply(
            hyphae::MapDiff::Update {
                key: Arc::from("one"),
                old_value: one,
                new_value: Arc::clone(&three),
            },
            None,
        );

        assert!(matches!(
            subscription.next_revision().map(|revision| revision.diff),
            Ok(Some(hyphae::MapDiff::Insert { key, value }))
                if key.as_ref() == "two" && value == two
        ));
        assert!(matches!(
            subscription.next_revision().map(|revision| revision.diff),
            Ok(Some(hyphae::MapDiff::Update { key, new_value, .. }))
                if key.as_ref() == "one" && new_value == three
        ));
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
