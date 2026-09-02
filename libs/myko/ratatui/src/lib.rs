//! Non-visual Ratatui integration for Myko and Hyphae.
//!
//! This crate deliberately contains no widgets. It retains Hyphae
//! subscriptions and turns their updates into a bounded wake-up stream for a
//! terminal application's event loop. Rendering reads the authoritative cell
//! directly after each wake-up, so data is never copied into a second UI store.

#![forbid(unsafe_code)]

use std::sync::Arc;

use hyphae::{Cell, CellImmutable, CellMap, SubscriptionGuard, Watchable as _};
use myko_federation::{
    LiveCollection, LiveCollectionState, LiveSubscription, LiveSubscriptionState,
};

/// Coalescing redraw notifications retained for a terminal view's lifetime.
///
/// Any number of Hyphae cells can be observed. At most one pending wake-up is
/// queued, because one render pass reads the newest state from every cell.
pub struct RerenderSubscriptions {
    wake_tx: flume::Sender<()>,
    wake_rx: flume::Receiver<()>,
    guards: Vec<SubscriptionGuard>,
}

impl Default for RerenderSubscriptions {
    fn default() -> Self {
        Self::new()
    }
}

impl RerenderSubscriptions {
    /// Creates an empty set of retained observations.
    #[must_use]
    pub fn new() -> Self {
        let (wake_tx, wake_rx) = flume::bounded(1);
        Self {
            wake_tx,
            wake_rx,
            guards: Vec::new(),
        }
    }

    /// Retains a cell subscription and schedules redraws for its revisions.
    pub fn observe<T>(&mut self, cell: &Cell<T, CellImmutable>)
    where
        T: hyphae::CellValue,
    {
        let wake_tx = self.wake_tx.clone();
        self.guards.push(cell.subscribe(move |_| {
            let _ignored = wake_tx.try_send(());
        }));
    }

    /// Waits asynchronously until at least one observed cell changes.
    ///
    /// # Errors
    ///
    /// Returns an error only after this binding's send side has closed.
    pub async fn changed(&self) -> Result<(), flume::RecvError> {
        self.wake_rx.recv_async().await
    }

    /// Consumes one already-pending redraw request without blocking.
    #[must_use]
    pub fn take_pending(&self) -> bool {
        self.wake_rx.try_recv().is_ok()
    }

    /// Returns the number of retained Hyphae observations.
    #[must_use]
    pub const fn observation_count(&self) -> usize {
        self.guards.len()
    }
}

/// A Myko live value paired with its Ratatui redraw lifecycle.
pub struct LiveBinding<T, C = myko_federation::LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    subscription: LiveSubscription<T, C>,
    rerenders: RerenderSubscriptions,
}

impl<T, C> LiveBinding<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Binds a live Myko subscription to terminal redraw notifications.
    #[must_use]
    pub fn new(subscription: LiveSubscription<T, C>) -> Self {
        let mut rerenders = RerenderSubscriptions::new();
        rerenders.observe(subscription.state());
        Self {
            subscription,
            rerenders,
        }
    }

    /// Reads the newest coherent value/cursor/liveness state for rendering.
    #[must_use]
    pub fn current(&self) -> LiveSubscriptionState<T, C> {
        self.subscription.current()
    }

    /// Waits until the terminal should render again.
    ///
    /// # Errors
    ///
    /// Returns an error only if the retained observation is closed.
    pub async fn changed(&self) -> Result<(), flume::RecvError> {
        self.rerenders.changed().await
    }

    /// Returns the underlying Myko subscription for further Hyphae composition.
    #[must_use]
    pub const fn subscription(&self) -> &LiveSubscription<T, C> {
        &self.subscription
    }
}

/// A keyed Myko collection paired with its Ratatui redraw lifecycle.
///
/// Rendering reads the authoritative `CellMap` directly. The redraw signal is
/// driven by atomic collection revisions, so a render sees the cursor and
/// liveness that cover the changed rows without copying the collection.
pub struct CollectionBinding<T, C = myko_federation::LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    collection: LiveCollection<T, C>,
    rerenders: RerenderSubscriptions,
}

impl<T, C> CollectionBinding<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Binds a keyed collection to coalescing terminal redraw notifications.
    #[must_use]
    pub fn new(collection: LiveCollection<T, C>) -> Self {
        let mut rerenders = RerenderSubscriptions::new();
        rerenders.observe(collection.revision());
        Self {
            collection,
            rerenders,
        }
    }

    /// Returns the authoritative keyed rows used directly by rendering code.
    #[must_use]
    pub const fn rows(&self) -> &CellMap<Arc<str>, Arc<T>, CellImmutable> {
        self.collection.rows()
    }

    /// Reads the newest coherent cursor and liveness revision.
    #[must_use]
    pub fn state(&self) -> LiveCollectionState<C> {
        self.collection.current_state()
    }

    /// Waits until the terminal should render again.
    ///
    /// # Errors
    ///
    /// Returns an error only if the retained observation is closed.
    pub async fn changed(&self) -> Result<(), flume::RecvError> {
        self.rerenders.changed().await
    }

    /// Returns the underlying collection for further Hyphae composition.
    #[must_use]
    pub const fn collection(&self) -> &LiveCollection<T, C> {
        &self.collection
    }
}

#[cfg(test)]
mod tests {
    use myko_federation::{
        LiveCollectionState, LiveSubscriptionState, SubscriptionLiveness, live_collection,
        live_subscription,
    };

    use super::*;

    #[test]
    fn updates_coalesce_without_copying_subscription_state() {
        let (writer, subscription) = live_subscription(LiveSubscriptionState {
            value: Some(vec![1_u32]),
            through: None::<myko_federation::LogPosition>,
            liveness: SubscriptionLiveness::Current,
        });
        let binding = LiveBinding::new(subscription);
        assert!(binding.rerenders.take_pending());

        writer.publish(vec![2], None);
        writer.publish(vec![3], None);

        assert!(binding.rerenders.take_pending());
        assert!(!binding.rerenders.take_pending());
        assert_eq!(binding.current().value, Some(vec![3]));

        writer.resynchronizing("daemon restarting");
        assert!(binding.rerenders.take_pending());
        let stale = binding.current();
        assert_eq!(stale.value, Some(vec![3]));
        assert!(matches!(
            stale.liveness,
            SubscriptionLiveness::Resynchronizing { ref reason }
                if reason == "daemon restarting"
        ));

        writer.publish(vec![4], None);
        assert!(binding.rerenders.take_pending());
        assert_eq!(binding.current().liveness, SubscriptionLiveness::Current);
    }

    #[test]
    fn collection_binding_reads_rows_without_a_second_store() {
        let (writer, collection) = live_collection(
            vec![(Arc::<str>::from("row-1"), Arc::new("one".to_owned()))],
            LiveCollectionState {
                through: Some(myko_federation::LogPosition::new(1)),
                liveness: SubscriptionLiveness::Current,
            },
        );
        let binding = CollectionBinding::new(collection);
        assert!(binding.rerenders.take_pending());

        writer.apply(
            hyphae::MapDiff::Update {
                key: Arc::<str>::from("row-1"),
                old_value: Arc::new("one".to_owned()),
                new_value: Arc::new("two".to_owned()),
            },
            Some(myko_federation::LogPosition::new(2)),
        );

        assert!(binding.rerenders.take_pending());
        assert_eq!(
            binding.rows().get_value(&Arc::<str>::from("row-1")),
            Some(Arc::new("two".to_owned()))
        );
        assert_eq!(
            binding.state().through,
            Some(myko_federation::LogPosition::new(2))
        );
    }
}
