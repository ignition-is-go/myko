//! Hyphae-backed lifecycle state shared by every Myko transport adapter.

use std::{collections::BTreeMap, sync::Arc};

use hyphae::{
    Cell, CellImmutable, CellMap, CellMutable, Gettable as _, JoinExt as _, MapDiff, MapExt as _,
    Materialize as _, Mutable as _, ScanExt as _,
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

/// Coherent cursor and liveness for a keyed reactive collection.
///
/// Collection rows live in a [`CellMap`] so additions, updates, and removals
/// retain their identity through Hyphae composition. This cell carries only
/// the lifecycle revision that applies to the map.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveCollectionState<C = LogPosition> {
    pub through: Option<C>,
    pub liveness: SubscriptionLiveness,
}

/// One atomic publication from a keyed reactive collection.
///
/// A revision carries the typed row diff together with the exact cursor and
/// liveness that cover it. Lifecycle-only transitions use `diff: None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveCollectionRevision<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    pub diff: Option<MapDiff<Arc<str>, Arc<T>>>,
    pub state: LiveCollectionState<C>,
}

/// Read-only typed reactive collection returned to applications and clients.
#[derive(Clone)]
pub struct LiveCollection<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    rows: CellMap<Arc<str>, Arc<T>, CellImmutable>,
    state: Cell<LiveCollectionState<C>, CellImmutable>,
    revision: Cell<LiveCollectionRevision<T, C>, CellImmutable>,
}

impl<T, C> LiveCollection<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Returns the keyed Hyphae collection used for composition and rendering.
    #[must_use]
    pub const fn rows(&self) -> &CellMap<Arc<str>, Arc<T>, CellImmutable> {
        &self.rows
    }

    /// Returns the coherent cursor/liveness cell for the collection.
    #[must_use]
    pub const fn state(&self) -> &Cell<LiveCollectionState<C>, CellImmutable> {
        &self.state
    }

    /// Returns atomic typed row/lifecycle publications for transports and UI
    /// rerender bindings.
    #[must_use]
    pub const fn revision(&self) -> &Cell<LiveCollectionRevision<T, C>, CellImmutable> {
        &self.revision
    }

    /// Takes the current lifecycle revision without subscribing.
    #[must_use]
    pub fn current_state(&self) -> LiveCollectionState<C> {
        self.state.get()
    }
}

/// Adapter-side writer for a [`LiveCollection`].
#[derive(Clone)]
pub struct LiveCollectionWriter<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    rows: CellMap<Arc<str>, Arc<T>, CellMutable>,
    state: Cell<LiveCollectionState<C>, CellMutable>,
    revision: Cell<LiveCollectionRevision<T, C>, CellMutable>,
}

/// Failure while reconciling a typed collection snapshot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LiveCollectionError {
    #[error("live collection contains duplicate key {0:?}")]
    DuplicateKey(String),
}

impl<T, C> LiveCollectionWriter<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Replaces the authoritative collection and publishes its cursor in one
    /// Hyphae scheduler batch.
    pub fn replace_all(&self, rows: Vec<(Arc<str>, Arc<T>)>, through: Option<C>) {
        let state = LiveCollectionState {
            through,
            liveness: SubscriptionLiveness::Current,
        };
        let revision = LiveCollectionRevision {
            diff: Some(MapDiff::Initial {
                entries: rows.clone(),
            }),
            state: state.clone(),
        };
        hyphae::batch(|| {
            self.state.set(state);
            self.rows.replace_all(rows);
        });
        self.revision.set(revision);
    }

    /// Applies one typed collection diff and publishes its cursor in one
    /// Hyphae scheduler batch.
    pub fn apply(&self, diff: MapDiff<Arc<str>, Arc<T>>, through: Option<C>) {
        let state = LiveCollectionState {
            through,
            liveness: SubscriptionLiveness::Current,
        };
        let revision = LiveCollectionRevision {
            diff: Some(diff.clone()),
            state: state.clone(),
        };
        hyphae::batch(|| {
            self.state.set(state);
            self.rows.apply_diff_owned(diff);
        });
        self.revision.set(revision);
    }

    /// Reconciles a typed snapshot into item-level additions, updates, and
    /// removals without discarding stable row identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the incoming snapshot contains a duplicate key.
    pub fn reconcile(
        &self,
        rows: Vec<(Arc<str>, Arc<T>)>,
        through: Option<C>,
    ) -> Result<(), LiveCollectionError> {
        let mut next = BTreeMap::new();
        for (key, value) in rows {
            if next.insert(key.clone(), value).is_some() {
                return Err(LiveCollectionError::DuplicateKey(key.to_string()));
            }
        }
        let current = self.rows.snapshot().into_iter().collect::<BTreeMap<_, _>>();
        let mut changes = Vec::new();
        for (key, old_value) in &current {
            match next.get(key) {
                None => changes.push(MapDiff::Remove {
                    key: key.clone(),
                    old_value: Arc::clone(old_value),
                }),
                Some(new_value) if new_value != old_value => changes.push(MapDiff::Update {
                    key: key.clone(),
                    old_value: Arc::clone(old_value),
                    new_value: Arc::clone(new_value),
                }),
                Some(_) => {}
            }
        }
        for (key, value) in next {
            if !current.contains_key(&key) {
                changes.push(MapDiff::Insert { key, value });
            }
        }
        if changes.is_empty() {
            let state = LiveCollectionState {
                through,
                liveness: SubscriptionLiveness::Current,
            };
            self.state.set(state.clone());
            self.revision
                .set(LiveCollectionRevision { diff: None, state });
        } else {
            self.apply(MapDiff::Batch { changes }, through);
        }
        Ok(())
    }

    /// Retains rows while marking the collection stale during recovery.
    pub fn resynchronizing(&self, reason: impl Into<String>) {
        let previous = self.state.get();
        let state = LiveCollectionState {
            through: previous.through,
            liveness: SubscriptionLiveness::Resynchronizing {
                reason: reason.into(),
            },
        };
        self.state.set(state.clone());
        self.revision
            .set(LiveCollectionRevision { diff: None, state });
    }

    /// Retains rows while marking the collection unusable.
    pub fn invalidate(&self, reason: impl Into<String>) {
        let previous = self.state.get();
        let state = LiveCollectionState {
            through: previous.through,
            liveness: SubscriptionLiveness::Invalid {
                reason: reason.into(),
            },
        };
        self.state.set(state.clone());
        self.revision
            .set(LiveCollectionRevision { diff: None, state });
    }
}

/// Creates application and adapter halves of one keyed live collection.
#[must_use]
pub fn live_collection<T, C>(
    rows: Vec<(Arc<str>, Arc<T>)>,
    state: LiveCollectionState<C>,
) -> (LiveCollectionWriter<T, C>, LiveCollection<T, C>)
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    let mutable_rows = CellMap::new().with_name("myko.live_collection.rows");
    mutable_rows.replace_all(rows.clone());
    let mutable_state = Cell::new(state.clone()).with_name("myko.live_collection.state");
    let mutable_revision = Cell::new(LiveCollectionRevision {
        diff: Some(MapDiff::Initial { entries: rows }),
        state,
    })
    .with_name("myko.live_collection.revision");
    let readable = LiveCollection {
        rows: mutable_rows.clone().lock(),
        state: mutable_state.clone().lock(),
        revision: mutable_revision.clone().lock(),
    };
    (
        LiveCollectionWriter {
            rows: mutable_rows,
            state: mutable_state,
            revision: mutable_revision,
        },
        readable,
    )
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

    /// Joins dependencies that advance through the same authoritative cursor stream.
    ///
    /// A single atomic batch can wake independent dependency drivers in either
    /// order. This join retains the last coherent value until both dependencies
    /// cover the same cursor, preventing a derived report or view from exposing
    /// half of that batch. Dependencies from different cursor spaces should use
    /// a composite frontier instead of this helper.
    #[must_use]
    pub fn join_coherent<U>(&self, other: &LiveSubscription<U, C>) -> LiveSubscription<(T, U), C>
    where
        U: hyphae::CellValue,
    {
        let initial_dependencies = (self.current(), other.current());
        let initial = coherent_join_state(
            &LiveSubscriptionState {
                value: None,
                through: None,
                liveness: SubscriptionLiveness::Connecting,
            },
            &initial_dependencies,
        );
        let state = self
            .state
            .clone()
            .join(other.state.clone())
            .scan(initial, coherent_join_state)
            .materialize()
            .with_name("myko.live_subscription.join_coherent");
        LiveSubscription::from_state_cell(state)
    }
}

fn coherent_join_state<T, U, C>(
    previous: &LiveSubscriptionState<(T, U), C>,
    dependencies: &(LiveSubscriptionState<T, C>, LiveSubscriptionState<U, C>),
) -> LiveSubscriptionState<(T, U), C>
where
    T: hyphae::CellValue,
    U: hyphae::CellValue,
    C: hyphae::CellValue,
{
    let (left, right) = dependencies;
    let invalid = match (&left.liveness, &right.liveness) {
        (SubscriptionLiveness::Invalid { reason }, _)
        | (_, SubscriptionLiveness::Invalid { reason }) => Some(reason.clone()),
        _ => None,
    };
    if let Some(reason) = invalid {
        return LiveSubscriptionState {
            value: previous.value.clone(),
            through: previous.through.clone(),
            liveness: SubscriptionLiveness::Invalid { reason },
        };
    }
    if left.liveness != SubscriptionLiveness::Current
        || right.liveness != SubscriptionLiveness::Current
    {
        return LiveSubscriptionState {
            value: previous.value.clone(),
            through: previous.through.clone(),
            liveness: SubscriptionLiveness::Resynchronizing {
                reason: "waiting for coherent dependencies".to_owned(),
            },
        };
    }
    if left.through != right.through {
        return LiveSubscriptionState {
            value: previous.value.clone(),
            through: previous.through.clone(),
            liveness: SubscriptionLiveness::Resynchronizing {
                reason: "aligning atomic dependency cursors".to_owned(),
            },
        };
    }
    let (Some(left_value), Some(right_value)) = (&left.value, &right.value) else {
        return LiveSubscriptionState {
            value: previous.value.clone(),
            through: previous.through.clone(),
            liveness: SubscriptionLiveness::Connecting,
        };
    };
    LiveSubscriptionState {
        value: Some((left_value.clone(), right_value.clone())),
        through: left.through.clone(),
        liveness: SubscriptionLiveness::Current,
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

    /// Advances the evaluated cursor without replacing the current value.
    ///
    /// Dependencies in a coherent join must report that they evaluated every
    /// source revision, including revisions which do not change their value.
    /// Keeping this distinct from [`Self::publish`] lets drivers express that
    /// progress without rebuilding application state.
    pub fn advance_through(&self, through: Option<C>) {
        let mut state = self.state.get();
        state.through = through;
        self.state.set(state);
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
    use std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use hyphae::{Signal, Watchable as _};

    use super::*;

    fn wait_for_revisions<T>(observed: &Mutex<Vec<T>>, count: usize) {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        while Instant::now() < deadline {
            if observed
                .lock()
                .is_ok_and(|observed| observed.len() >= count)
            {
                return;
            }
            std::thread::yield_now();
        }
    }

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
        wait_for_revisions(&observed, 4);

        let current = subscription.current();
        assert_eq!(current.value, Some(vec!["new".to_owned()]));
        assert_eq!(current.through, Some(LogPosition::new(9)));
        assert_eq!(current.liveness, SubscriptionLiveness::Current);
        assert!(observed.lock().is_ok_and(|observed| observed.len() >= 4));
    }

    #[test]
    fn mapped_subscription_exposes_the_initial_authoritative_value() {
        let (_writer, subscription) = live_subscription(LiveSubscriptionState {
            value: Some(vec!["persisted".to_owned()]),
            through: Some(LogPosition::new(7)),
            liveness: SubscriptionLiveness::Current,
        });

        let mapped = subscription.map_value(Vec::len);

        assert_eq!(
            mapped.current(),
            LiveSubscriptionState {
                value: Some(1),
                through: Some(LogPosition::new(7)),
                liveness: SubscriptionLiveness::Current,
            }
        );
    }

    #[test]
    fn coherent_join_withholds_half_of_one_cursor_revision() {
        let (left_writer, left) = live_subscription(LiveSubscriptionState {
            value: Some("left-1".to_owned()),
            through: Some(LogPosition::new(1)),
            liveness: SubscriptionLiveness::Current,
        });
        let (right_writer, right) = live_subscription(LiveSubscriptionState {
            value: Some("right-1".to_owned()),
            through: Some(LogPosition::new(1)),
            liveness: SubscriptionLiveness::Current,
        });
        let joined = left.join_coherent(&right);
        assert_eq!(
            joined.current(),
            LiveSubscriptionState {
                value: Some(("left-1".to_owned(), "right-1".to_owned())),
                through: Some(LogPosition::new(1)),
                liveness: SubscriptionLiveness::Current,
            }
        );

        left_writer.publish("left-2".to_owned(), Some(LogPosition::new(2)));
        let partial = joined.current();
        assert_eq!(
            partial.value,
            Some(("left-1".to_owned(), "right-1".to_owned()))
        );
        assert_eq!(partial.through, Some(LogPosition::new(1)));
        assert!(matches!(
            partial.liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ));

        right_writer.publish("right-2".to_owned(), Some(LogPosition::new(2)));
        assert_eq!(
            joined.current(),
            LiveSubscriptionState {
                value: Some(("left-2".to_owned(), "right-2".to_owned())),
                through: Some(LogPosition::new(2)),
                liveness: SubscriptionLiveness::Current,
            }
        );
    }

    #[test]
    fn coherent_join_accepts_a_cursor_only_dependency_advance() {
        let (left_writer, left) = live_subscription(LiveSubscriptionState {
            value: Some("left-1".to_owned()),
            through: Some(LogPosition::new(1)),
            liveness: SubscriptionLiveness::Current,
        });
        let (right_writer, right) = live_subscription(LiveSubscriptionState {
            value: Some("right-1".to_owned()),
            through: Some(LogPosition::new(1)),
            liveness: SubscriptionLiveness::Current,
        });
        let joined = left.join_coherent(&right);

        left_writer.publish("left-2".to_owned(), Some(LogPosition::new(2)));
        assert!(matches!(
            joined.current().liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ));
        right_writer.advance_through(Some(LogPosition::new(2)));

        assert_eq!(
            joined.current(),
            LiveSubscriptionState {
                value: Some(("left-2".to_owned(), "right-1".to_owned())),
                through: Some(LogPosition::new(2)),
                liveness: SubscriptionLiveness::Current,
            }
        );
    }

    #[test]
    fn collection_publishes_typed_diffs_with_the_matching_lifecycle() {
        let (writer, collection) = live_collection(
            vec![(Arc::<str>::from("message-1"), Arc::new("one".to_owned()))],
            LiveCollectionState {
                through: Some(LogPosition::new(1)),
                liveness: SubscriptionLiveness::Current,
            },
        );
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_callback = Arc::clone(&observed);
        let _guard = collection.revision().subscribe(move |signal| {
            if let Signal::Value(revision) = signal
                && let Ok(mut observed) = observed_for_callback.lock()
            {
                observed.push(revision.clone());
            }
        });

        writer.apply(
            MapDiff::Update {
                key: Arc::<str>::from("message-1"),
                old_value: Arc::new("one".to_owned()),
                new_value: Arc::new("streaming".to_owned()),
            },
            Some(LogPosition::new(2)),
        );
        wait_for_revisions(&observed, 2);

        let observed = observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            observed.last().is_some_and(|revision| matches!(
                revision.as_ref(),
                LiveCollectionRevision {
                    diff: Some(MapDiff::Update { key, new_value, .. }),
                    state,
                }
                    if key.as_ref() == "message-1"
                        && new_value.as_str() == "streaming"
                        && state.through == Some(LogPosition::new(2))
                        && state.liveness == SubscriptionLiveness::Current
            )),
            "observed revisions: {observed:?}"
        );
        drop(observed);
    }

    #[test]
    fn collection_reconcile_emits_only_changed_rows() {
        let (writer, collection) = live_collection(
            vec![
                (Arc::<str>::from("a"), Arc::new("same".to_owned())),
                (Arc::<str>::from("b"), Arc::new("before".to_owned())),
            ],
            LiveCollectionState {
                through: Some(LogPosition::new(1)),
                liveness: SubscriptionLiveness::Current,
            },
        );
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_callback = Arc::clone(&observed);
        let _guard = collection.revision().subscribe(move |signal| {
            if let Signal::Value(revision) = signal
                && let Ok(mut observed) = observed_for_callback.lock()
            {
                observed.push(revision.clone());
            }
        });

        assert!(
            writer
                .reconcile(
                    vec![
                        (Arc::<str>::from("a"), Arc::new("same".to_owned())),
                        (Arc::<str>::from("b"), Arc::new("after".to_owned())),
                    ],
                    Some(LogPosition::new(2)),
                )
                .is_ok()
        );
        wait_for_revisions(&observed, 2);

        let observed = observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            observed.last().is_some_and(|revision| matches!(
                revision.as_ref(),
                LiveCollectionRevision {
                    diff: Some(MapDiff::Batch { changes }),
                    state,
                } if matches!(
                    changes.as_slice(),
                    [MapDiff::Update { key, new_value, .. }]
                        if key.as_ref() == "b" && new_value.as_str() == "after"
                ) && state.through == Some(LogPosition::new(2))
            )),
            "observed revisions: {observed:?}"
        );
        drop(observed);
    }
}
