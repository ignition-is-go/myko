//! Hyphae-backed lifecycle state shared by every Myko transport adapter.

use hyphae::{
    Cell, CellImmutable, CellMutable, Gettable as _, MapExt as _, Materialize as _, Mutable as _,
};

use crate::LogPosition;

/// Whether a live subscription currently represents authoritative state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionLiveness {
    /// The adapter is establishing its initial snapshot and follow cursor.
    Connecting,
    /// The value includes every accepted update through its cursor.
    Current,
    /// The adapter is reconnecting; a retained value is stale until resynced.
    Resynchronizing { reason: String },
    /// The stream ended or violated its contract and requires a new watch.
    Invalid { reason: String },
}

/// One coherent value, cursor, and liveness revision for a live subscription.
///
/// Keeping these fields in one Hyphae cell prevents a renderer from observing
/// a new value with an old cursor or liveness state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveSubscriptionState<T, C = LogPosition> {
    pub value: Option<T>,
    pub through: Option<C>,
    pub liveness: SubscriptionLiveness,
}

/// Read-only reactive state returned to application and UI code.
#[derive(Clone)]
pub struct LiveSubscription<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    state: Cell<LiveSubscriptionState<T, C>, CellImmutable>,
}

impl<T, C> LiveSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Returns the Hyphae cell used to compose reports, views, and UI state.
    #[must_use]
    pub const fn state(&self) -> &Cell<LiveSubscriptionState<T, C>, CellImmutable> {
        &self.state
    }

    /// Takes a coherent snapshot without subscribing.
    #[must_use]
    pub fn current(&self) -> LiveSubscriptionState<T, C> {
        self.state.get()
    }

    /// Wraps an application-derived immutable Hyphae lifecycle cell.
    ///
    /// Transport adapters normally use [`live_subscription`]. Report and view
    /// handlers use this constructor after composing their dependency cells so
    /// the resulting value retains the same subscription surface.
    #[must_use]
    pub const fn from_state_cell(state: Cell<LiveSubscriptionState<T, C>, CellImmutable>) -> Self {
        Self { state }
    }

    /// Derives another live value while preserving cursor and liveness.
    ///
    /// The returned cell remains a Hyphae pipeline materialized at this API
    /// boundary; no task, polling loop, or duplicate mutable store is created.
    #[must_use]
    pub fn map_value<U, F>(&self, transform: F) -> LiveSubscription<U, C>
    where
        U: hyphae::CellValue,
        F: Fn(&T) -> U + Send + Sync + 'static,
    {
        let state = self
            .state
            .clone()
            .map(move |state| LiveSubscriptionState {
                value: state.value.as_ref().map(&transform),
                through: state.through.clone(),
                liveness: state.liveness.clone(),
            })
            .materialize()
            .with_name("myko.live_subscription.map_value");
        LiveSubscription::from_state_cell(state)
    }
}

/// Adapter-side writer for a [`LiveSubscription`].
///
/// Storage and transport crates retain this half. Applications only receive
/// the immutable Hyphae cell, so they cannot forge cursor or liveness changes.
#[derive(Clone)]
pub struct LiveSubscriptionWriter<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    state: Cell<LiveSubscriptionState<T, C>, CellMutable>,
}

impl<T, C> LiveSubscriptionWriter<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Replaces the complete coherent lifecycle revision.
    ///
    /// This is used by compatibility transports that receive an already
    /// validated Myko lifecycle state. Native adapters normally prefer the
    /// narrower [`Self::publish`], [`Self::resynchronizing`], and
    /// [`Self::invalidate`] operations.
    pub fn replace(&self, state: LiveSubscriptionState<T, C>) {
        self.state.set(state);
    }

    /// Publishes an authoritative snapshot or atomic update.
    pub fn publish(&self, value: T, through: Option<C>) {
        self.state.set(LiveSubscriptionState {
            value: Some(value),
            through,
            liveness: SubscriptionLiveness::Current,
        });
    }

    /// Retains the last value while an adapter reconnects and resynchronizes.
    pub fn resynchronizing(&self, reason: impl Into<String>) {
        let previous = self.state.get();
        self.state.set(LiveSubscriptionState {
            value: previous.value,
            through: previous.through,
            liveness: SubscriptionLiveness::Resynchronizing {
                reason: reason.into(),
            },
        });
    }

    /// Marks the subscription unusable while retaining its last stale value.
    pub fn invalidate(&self, reason: impl Into<String>) {
        let previous = self.state.get();
        self.state.set(LiveSubscriptionState {
            value: previous.value,
            through: previous.through,
            liveness: SubscriptionLiveness::Invalid {
                reason: reason.into(),
            },
        });
    }
}

/// Creates the application and adapter halves of one live reactive value.
#[must_use]
pub fn live_subscription<T, C>(
    initial: LiveSubscriptionState<T, C>,
) -> (LiveSubscriptionWriter<T, C>, LiveSubscription<T, C>)
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    let state = Cell::new(initial).with_name("myko.live_subscription");
    let readable = state.clone().lock();
    (
        LiveSubscriptionWriter { state },
        LiveSubscription { state: readable },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use hyphae::{Signal, Watchable as _};

    use super::*;

    #[test]
    fn writer_publishes_coherent_reactive_revisions() {
        let (writer, subscription) = live_subscription(LiveSubscriptionState::<Vec<String>> {
            value: None,
            through: None,
            liveness: SubscriptionLiveness::Connecting,
        });
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_callback = Arc::clone(&observed);
        let _guard = subscription.state().subscribe(move |signal| {
            if let Signal::Value(state) = signal
                && let Ok(mut observed) = observed_for_callback.lock()
            {
                observed.push(state.clone());
            }
        });

        writer.publish(vec!["ready".to_owned()], Some(LogPosition::new(7)));
        writer.resynchronizing("peer changed");
        writer.publish(vec!["new".to_owned()], Some(LogPosition::new(9)));

        let current = subscription.current();
        assert_eq!(current.value, Some(vec!["new".to_owned()]));
        assert_eq!(current.through, Some(LogPosition::new(9)));
        assert_eq!(current.liveness, SubscriptionLiveness::Current);
        assert!(observed.lock().is_ok_and(|observed| observed.len() >= 4));
    }
}
