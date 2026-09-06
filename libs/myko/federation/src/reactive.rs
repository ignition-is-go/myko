//! Hyphae-backed lifecycle state shared by every Myko transport adapter.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use hyphae::{
    Cell, CellImmutable, CellMap, CellMutable, DepNode as _, Gettable as _, JoinExt as _, MapDiff,
    MapExt as _, MapQuery, Materialize as _, Mutable as _, ScanExt as _, Signal, Watchable as _,
};
use parking_lot::Mutex;

use crate::{LivePublication, LivePublicationStream, LogPosition, publication::PublicationSource};

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

/// Exact progress of two independently advancing reactive dependencies.
///
/// Unlike a shared cursor, neither side is required to equal or wait for the
/// other. A derived report carries this frontier so it never invents a single
/// ordering across unrelated runtime feeds, journals, or remote sources.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompositeFrontier<L, R> {
    pub left: Option<L>,
    pub right: Option<R>,
}

/// One atomic publication from a keyed reactive collection.
///
/// A revision carries the typed row diff together with the exact cursor and
/// liveness that cover it. Lifecycle-only transitions use `diff: None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveCollectionRevision<T, C = LogPosition, K = Arc<str>>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq,
{
    pub diff: Option<MapDiff<K, Arc<T>>>,
    pub state: LiveCollectionState<C>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CollectionRevisionFold<T, C, K>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq,
{
    last_diff: MapDiff<K, Arc<T>>,
    revision: LiveCollectionRevision<T, C, K>,
}

fn fold_collection_revision<T, C, K>(
    previous: &CollectionRevisionFold<T, C, K>,
    input: &(MapDiff<K, Arc<T>>, LiveCollectionState<C>),
) -> CollectionRevisionFold<T, C, K>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq,
{
    let diff_changed = previous.last_diff != input.0;
    let state_changed = previous.revision.state != input.1;
    let revision = if diff_changed {
        LiveCollectionRevision {
            diff: Some(input.0.clone()),
            state: input.1.clone(),
        }
    } else if state_changed {
        LiveCollectionRevision {
            diff: None,
            state: input.1.clone(),
        }
    } else {
        previous.revision.clone()
    };
    CollectionRevisionFold {
        last_diff: input.0.clone(),
        revision,
    }
}

/// Read-only typed reactive collection returned to applications and clients.
#[derive(Clone)]
pub struct LiveCollection<T, C = LogPosition, K = Arc<str>>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq,
{
    rows: CellMap<K, Arc<T>, CellImmutable>,
    // Derived from `revision`; this is a convenience view, not a second
    // mutable lifecycle publication.
    state: Cell<LiveCollectionState<C>, CellImmutable>,
    revision: Cell<LiveCollectionRevision<T, C, K>, CellImmutable>,
}

/// A lazy keyed collection projection together with its authoritative lifecycle.
///
/// Row operators remain an unmaterialized Hyphae [`MapQuery`] until the Myko
/// handler factory opens the collection. This keeps application composition
/// declarative and gives each registered query or view exactly one observable
/// row-map boundary.
pub struct MapCollectionPlan<T, C, K, Q>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq,
    Q: MapQuery<Key = K, Value = Arc<T>>,
{
    rows: Q,
    state: Cell<LiveCollectionState<C>, CellImmutable>,
}

/// Lazy union of two keyed collection plans with independent cursor spaces.
///
/// Both inputs must use the same row and key types. Keys are the durable row
/// identity across the union, so a key present on both sides invalidates the
/// result instead of silently choosing one value.
pub struct UnionCollectionPlan<L, R> {
    left: L,
    right: R,
}

impl<L, R> UnionCollectionPlan<L, R> {
    /// Creates a lazy two-source collection union.
    #[must_use]
    pub const fn new(left: L, right: R) -> Self {
        Self { left, right }
    }
}

impl<T, C, K, Q> MapCollectionPlan<T, C, K, Q>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
    Q: MapQuery<Key = K, Value = Arc<T>>,
{
    /// Combines a lazy row plan with the lifecycle that covers its sources.
    #[must_use]
    pub const fn new(rows: Q, state: Cell<LiveCollectionState<C>, CellImmutable>) -> Self {
        Self { rows, state }
    }

    /// Returns the lifecycle cell that covers this plan's source rows.
    #[must_use]
    pub const fn state(&self) -> &Cell<LiveCollectionState<C>, CellImmutable> {
        &self.state
    }

    /// Composes another lazy keyed projection without materializing this plan.
    #[must_use]
    pub fn project_rows<U, K2, Q2, F>(self, build: F) -> MapCollectionPlan<U, C, K2, Q2>
    where
        U: hyphae::CellValue,
        K2: hyphae::CellValue + std::hash::Hash + Eq + Ord,
        Q2: MapQuery<Key = K2, Value = Arc<U>>,
        F: FnOnce(Q) -> Q2,
    {
        MapCollectionPlan::new(build(self.rows), self.state)
    }

    /// Materializes this plan into a subscribable live collection.
    ///
    /// Handler factories own this boundary. Application handlers should
    /// return the plan instead of calling this method themselves.
    #[must_use]
    pub fn materialize(self) -> LiveCollection<T, C, K> {
        let rows = self.rows.materialize();
        let state = self.state;
        let projected_diffs = rows.diffs().materialize();
        let initial_diff = projected_diffs.get();
        let initial_state = state.get();
        let initial = CollectionRevisionFold {
            last_diff: initial_diff.clone(),
            revision: LiveCollectionRevision {
                diff: Some(initial_diff),
                state: initial_state,
            },
        };
        let revision = projected_diffs
            .join(state)
            .scan(initial, fold_collection_revision)
            .map(|fold| fold.revision.clone())
            .materialize()
            .with_name("myko.live_collection.plan.revision");
        let state = revision
            .clone()
            .map(|revision| revision.state.clone())
            .materialize()
            .with_name("myko.live_collection.plan.state");

        LiveCollection {
            rows,
            state,
            revision,
        }
    }
}

/// Lazy keyed rows that Myko can materialize as one live collection.
///
/// Query and view handlers return this trait instead of a concrete
/// [`LiveCollection`]. That makes the handler factory the sole row-map
/// materialization boundary while allowing application code to return any
/// statically typed Hyphae plan.
pub trait CollectionPlan: Sized {
    type Item: hyphae::CellValue;
    type Cursor: hyphae::CellValue;
    type Key: hyphae::CellValue + std::hash::Hash + Eq + Ord;

    /// Materializes the plan into its shared, subscribable collection.
    #[must_use]
    fn materialize(self) -> LiveCollection<Self::Item, Self::Cursor, Self::Key>;

    /// Folds the complete keyed result into one reactive report value.
    ///
    /// This is the explicit collection-to-scalar boundary. Row filtering and
    /// joins stay lazy before this call; the collection is materialized once
    /// here so Hyphae can maintain the scalar as keyed diffs arrive.
    #[must_use]
    fn project_value<U, F>(self, project: F) -> LiveSubscription<U, Self::Cursor>
    where
        U: hyphae::CellValue,
        F: Fn(&Vec<Self::Item>) -> U + Send + Sync + 'static,
    {
        self.materialize().as_subscription().map_value(project)
    }

    /// Unions two keyed plans while retaining each source's independent
    /// cursor in a [`CompositeFrontier`].
    #[must_use]
    fn union<R>(self, right: R) -> UnionCollectionPlan<Self, R>
    where
        R: CollectionPlan<Item = Self::Item, Key = Self::Key>,
    {
        UnionCollectionPlan::new(self, right)
    }
}

impl<T, C, K, Q> CollectionPlan for MapCollectionPlan<T, C, K, Q>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
    Q: MapQuery<Key = K, Value = Arc<T>>,
{
    type Item = T;
    type Cursor = C;
    type Key = K;

    fn materialize(self) -> LiveCollection<T, C, K> {
        Self::materialize(self)
    }
}

impl<T, C, K> CollectionPlan for LiveCollection<T, C, K>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
{
    type Item = T;
    type Cursor = C;
    type Key = K;

    fn materialize(self) -> Self {
        self
    }
}

impl<L, R> CollectionPlan for UnionCollectionPlan<L, R>
where
    L: CollectionPlan,
    R: CollectionPlan<Item = L::Item, Key = L::Key>,
{
    type Item = L::Item;
    type Cursor = CompositeFrontier<L::Cursor, R::Cursor>;
    type Key = L::Key;

    fn materialize(self) -> LiveCollection<Self::Item, Self::Cursor, Self::Key> {
        let left = self.left.materialize();
        let right = self.right.materialize();
        union_live_collections(&left, &right)
    }
}

impl<T, C, K> LiveCollection<T, C, K>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
{
    /// Reuses this materialized collection as a source plan without copying
    /// rows or introducing another source subscription.
    #[must_use]
    pub fn plan(&self) -> MapCollectionPlan<T, C, K, CellMap<K, Arc<T>, CellImmutable>> {
        MapCollectionPlan::new(self.rows.clone(), self.state.clone())
    }

    /// Returns the keyed Hyphae collection used for composition and rendering.
    #[must_use]
    pub const fn rows(&self) -> &CellMap<K, Arc<T>, CellImmutable> {
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
    pub const fn revision(&self) -> &Cell<LiveCollectionRevision<T, C, K>, CellImmutable> {
        &self.revision
    }

    /// Takes the current lifecycle revision without subscribing.
    #[must_use]
    pub fn current_state(&self) -> LiveCollectionState<C> {
        self.state.get()
    }

    /// Projects keyed rows into a coherent live value for derived reports.
    ///
    /// The keyed collection remains authoritative. This projection is intended
    /// for in-process reactive composition when a report needs the complete
    /// value of multiple dependencies; clients should retain the collection's
    /// fine-grained revision surface instead.
    #[must_use]
    pub fn as_subscription(&self) -> LiveSubscription<Vec<T>, C> {
        let state = self
            .rows
            .entries()
            .materialize()
            .join(self.state.clone())
            .map(|(entries, state)| {
                let mut entries = entries.clone();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                LiveSubscriptionState {
                    value: Some(
                        entries
                            .into_iter()
                            .map(|(_, value)| value.as_ref().clone())
                            .collect(),
                    ),
                    through: state.through.clone(),
                    liveness: state.liveness.clone(),
                }
            })
            .materialize()
            .with_name("myko.live_collection.as_subscription");
        LiveSubscription::from_state_cell(state)
    }

    /// Builds an incremental Hyphae row plan while retaining this collection's
    /// authoritative cursor and liveness.
    ///
    /// The projection receives the shared source `CellMap`; additions,
    /// updates, removals, filtering, and re-keying remain fine-grained map
    /// operations rather than whole-snapshot recomputations.
    #[must_use]
    pub fn project_rows<U, K2, Q, F>(&self, build: F) -> MapCollectionPlan<U, C, K2, Q>
    where
        U: hyphae::CellValue,
        K2: hyphae::CellValue + std::hash::Hash + Eq + Ord,
        Q: MapQuery<Key = K2, Value = Arc<U>>,
        F: FnOnce(CellMap<K, Arc<T>, CellImmutable>) -> Q,
    {
        MapCollectionPlan::new(build(self.rows.clone()), self.state.clone())
    }
}

#[derive(Copy, Clone)]
enum UnionSide {
    Left,
    Right,
}

struct UnionRows<T, K>
where
    T: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
{
    left: BTreeMap<K, Arc<T>>,
    right: BTreeMap<K, Arc<T>>,
    colliding: bool,
    left_seed_replayed: bool,
    right_seed_replayed: bool,
}

fn union_live_collections<T, L, R, K>(
    left: &LiveCollection<T, L, K>,
    right: &LiveCollection<T, R, K>,
) -> LiveCollection<T, CompositeFrontier<L, R>, K>
where
    T: hyphae::CellValue,
    L: hyphae::CellValue,
    R: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
{
    let left_rows = left
        .rows()
        .snapshot()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let right_rows = right
        .rows()
        .snapshot()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let collision = union_collision(&left_rows, &right_rows).cloned();
    let initial_state = union_collection_state(left.current_state(), right.current_state());
    let initial_state = if let Some(key) = collision.as_ref() {
        invalid_union_state(&initial_state, key)
    } else {
        initial_state
    };
    let initial_rows = if collision.is_some() {
        Vec::new()
    } else {
        merged_union_rows(&left_rows, &right_rows)
    };
    let (writer, output) = live_collection(initial_rows, initial_state);
    let rows = Arc::new(Mutex::new(UnionRows {
        left: left_rows,
        right: right_rows,
        colliding: collision.is_some(),
        left_seed_replayed: false,
        right_seed_replayed: false,
    }));

    let left_for_right = left.clone();
    let rows_for_right = Arc::clone(&rows);
    let writer_for_right = writer.clone();
    let right_guard = right.revision().subscribe(move |signal| {
        let Signal::Value(revision) = signal else {
            return;
        };
        let state = union_collection_state(left_for_right.current_state(), revision.state.clone());
        publish_union_revision(
            &writer_for_right,
            &rows_for_right,
            UnionSide::Right,
            revision.diff.as_ref(),
            state,
        );
    });

    let right_for_left = right.clone();
    let rows_for_right = rows;
    let writer_for_left = writer;
    let left_guard = left.revision().subscribe(move |signal| {
        let Signal::Value(revision) = signal else {
            return;
        };
        let state = union_collection_state(revision.state.clone(), right_for_left.current_state());
        publish_union_revision(
            &writer_for_left,
            &rows_for_right,
            UnionSide::Left,
            revision.diff.as_ref(),
            state,
        );
    });
    output.revision.own(left_guard);
    output.revision.own(right_guard);
    output
}

fn publish_union_revision<T, C, K>(
    writer: &LiveCollectionWriter<T, C, K>,
    rows: &Mutex<UnionRows<T, K>>,
    side: UnionSide,
    diff: Option<&MapDiff<K, Arc<T>>>,
    state: LiveCollectionState<C>,
) where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
{
    let mut rows = rows.lock();
    let seed_replayed = match side {
        UnionSide::Left => &mut rows.left_seed_replayed,
        UnionSide::Right => &mut rows.right_seed_replayed,
    };
    if !*seed_replayed {
        *seed_replayed = true;
        return;
    }
    if let Some(diff) = diff {
        let side_rows = match side {
            UnionSide::Left => &mut rows.left,
            UnionSide::Right => &mut rows.right,
        };
        apply_diff_to_snapshot(side_rows, diff);
    }

    let mut publish_state = state;
    let mut publish_diff = diff.cloned();
    let mut reconcile = None;
    if let Some(key) = union_collision(&rows.left, &rows.right).cloned() {
        rows.colliding = true;
        publish_state = invalid_union_state(&publish_state, &key);
        publish_diff = None;
    } else if rows.colliding || matches!(diff, Some(MapDiff::Initial { .. })) {
        rows.colliding = false;
        reconcile = Some(merged_union_rows(&rows.left, &rows.right));
        publish_diff = None;
    }
    drop(rows);

    if let Some(rows) = reconcile {
        writer.reconcile_revision(rows, publish_state);
    } else {
        writer.publish_revision(publish_diff, publish_state);
    }
}

fn apply_diff_to_snapshot<T, K>(rows: &mut BTreeMap<K, Arc<T>>, diff: &MapDiff<K, Arc<T>>)
where
    T: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
{
    match diff {
        MapDiff::Initial { entries } => {
            rows.clear();
            rows.extend(entries.iter().cloned());
        }
        MapDiff::Insert { key, value } => {
            rows.insert(key.clone(), Arc::clone(value));
        }
        MapDiff::Remove { key, .. } => {
            rows.remove(key);
        }
        MapDiff::Update { key, new_value, .. } => {
            rows.insert(key.clone(), Arc::clone(new_value));
        }
        MapDiff::Batch { changes } => {
            for change in changes {
                apply_diff_to_snapshot(rows, change);
            }
        }
    }
}

fn union_collision<'a, T, K>(
    left: &'a BTreeMap<K, Arc<T>>,
    right: &BTreeMap<K, Arc<T>>,
) -> Option<&'a K>
where
    K: Ord,
{
    left.keys().find(|key| right.contains_key(*key))
}

fn merged_union_rows<T, K>(
    left: &BTreeMap<K, Arc<T>>,
    right: &BTreeMap<K, Arc<T>>,
) -> Vec<(K, Arc<T>)>
where
    K: Clone + Ord,
{
    left.iter()
        .chain(right)
        .map(|(key, value)| (key.clone(), Arc::clone(value)))
        .collect()
}

fn invalid_union_state<C, K>(state: &LiveCollectionState<C>, key: &K) -> LiveCollectionState<C>
where
    C: Clone,
    K: std::fmt::Debug,
{
    LiveCollectionState {
        through: state.through.clone(),
        liveness: SubscriptionLiveness::Invalid {
            reason: format!("collection union contains duplicate key {key:?}"),
        },
    }
}

fn union_collection_state<L, R>(
    left: LiveCollectionState<L>,
    right: LiveCollectionState<R>,
) -> LiveCollectionState<CompositeFrontier<L, R>>
where
    L: Clone,
    R: Clone,
{
    let liveness = match (&left.liveness, &right.liveness) {
        (SubscriptionLiveness::Invalid { reason }, _) => SubscriptionLiveness::Invalid {
            reason: format!("left collection is invalid: {reason}"),
        },
        (_, SubscriptionLiveness::Invalid { reason }) => SubscriptionLiveness::Invalid {
            reason: format!("right collection is invalid: {reason}"),
        },
        (SubscriptionLiveness::Resynchronizing { reason }, _) => {
            SubscriptionLiveness::Resynchronizing {
                reason: format!("left collection is resynchronizing: {reason}"),
            }
        }
        (_, SubscriptionLiveness::Resynchronizing { reason }) => {
            SubscriptionLiveness::Resynchronizing {
                reason: format!("right collection is resynchronizing: {reason}"),
            }
        }
        (SubscriptionLiveness::Connecting, _) | (_, SubscriptionLiveness::Connecting) => {
            SubscriptionLiveness::Connecting
        }
        (SubscriptionLiveness::Current, SubscriptionLiveness::Current) => {
            SubscriptionLiveness::Current
        }
    };
    LiveCollectionState {
        through: Some(CompositeFrontier {
            left: left.through,
            right: right.through,
        }),
        liveness,
    }
}

/// Adapter-side writer for a [`LiveCollection`].
#[derive(Clone)]
pub struct LiveCollectionWriter<T, C = LogPosition, K = Arc<str>>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq,
{
    rows: CellMap<K, Arc<T>, CellMutable>,
    revision: Cell<LiveCollectionRevision<T, C, K>, CellMutable>,
}

/// Framework-owned keyed source for process-local state.
///
/// Producers may publish complete snapshots when an external API offers no
/// finer change stream. Myko reconciles those snapshots into stable keyed
/// diffs before any query, view, transport, or UI observes them.
#[derive(Clone)]
pub struct RuntimeCollection<T>
where
    T: hyphae::CellValue,
{
    revision: Arc<AtomicU64>,
    key: RuntimeCollectionKey<T>,
    writer: LiveCollectionWriter<T, u64>,
    live: LiveCollection<T, u64>,
}

type RuntimeCollectionKey<T> = Arc<dyn Fn(&T) -> Arc<str> + Send + Sync>;

impl<T> RuntimeCollection<T>
where
    T: hyphae::CellValue,
{
    /// Creates a current process-local collection from its initial rows.
    #[must_use]
    pub fn new(initial: Vec<T>, key: impl Fn(&T) -> Arc<str> + Send + Sync + 'static) -> Self {
        let key = Arc::new(key);
        let rows = initial
            .into_iter()
            .map(|item| (key(&item), Arc::new(item)))
            .collect();
        let (writer, live) = live_collection(
            rows,
            LiveCollectionState {
                through: Some(0),
                liveness: SubscriptionLiveness::Current,
            },
        );
        Self {
            revision: Arc::new(AtomicU64::new(0)),
            key,
            writer,
            live,
        }
    }

    /// Returns the keyed live collection exposed to reactive handlers.
    #[must_use]
    pub const fn live(&self) -> &LiveCollection<T, u64> {
        &self.live
    }

    /// Reconciles a new source snapshot into keyed insert/update/remove diffs.
    pub fn publish(&self, value: Vec<T>) {
        let revision = self.revision.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        let rows = value
            .into_iter()
            .map(|item| ((self.key)(&item), Arc::new(item)))
            .collect();
        if let Err(error) = self.writer.reconcile(rows, Some(revision)) {
            self.writer.invalidate(error.to_string());
        }
    }

    /// Invalidates the source and every handler depending on it.
    pub fn invalidate(&self, reason: impl Into<String>) {
        self.writer.invalidate(reason.into());
    }
}

/// Failure while reconciling a typed collection snapshot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LiveCollectionError {
    #[error("live collection contains duplicate key {0:?}")]
    DuplicateKey(String),
}

impl<T, C, K> LiveCollectionWriter<T, C, K>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
{
    /// Replaces the authoritative collection and publishes its cursor in one
    /// Hyphae scheduler batch.
    pub fn replace_all(&self, rows: Vec<(K, Arc<T>)>, through: Option<C>) {
        let state = LiveCollectionState {
            through,
            liveness: SubscriptionLiveness::Current,
        };
        self.publish_revision(Some(MapDiff::Initial { entries: rows }), state);
    }

    /// Applies one typed collection diff and publishes its cursor in one
    /// Hyphae scheduler batch.
    pub fn apply(&self, diff: MapDiff<K, Arc<T>>, through: Option<C>) {
        let state = LiveCollectionState {
            through,
            liveness: SubscriptionLiveness::Current,
        };
        self.publish_revision(Some(diff), state);
    }

    /// Advances lifecycle progress when an authoritative update does not
    /// change any row in this collection.
    pub fn advance_through(&self, through: Option<C>) {
        let state = LiveCollectionState {
            through,
            liveness: SubscriptionLiveness::Current,
        };
        self.publish_revision(None, state);
    }

    /// Reconciles a typed snapshot into item-level additions, updates, and
    /// removals without discarding stable row identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the incoming snapshot contains a duplicate key.
    pub fn reconcile(
        &self,
        rows: Vec<(K, Arc<T>)>,
        through: Option<C>,
    ) -> Result<(), LiveCollectionError> {
        let mut next = BTreeMap::new();
        for (key, value) in rows {
            if next.insert(key.clone(), value).is_some() {
                return Err(LiveCollectionError::DuplicateKey(format!("{key:?}")));
            }
        }
        self.reconcile_map(
            next,
            LiveCollectionState {
                through,
                liveness: SubscriptionLiveness::Current,
            },
        );
        Ok(())
    }

    fn reconcile_revision(&self, rows: Vec<(K, Arc<T>)>, state: LiveCollectionState<C>) {
        self.reconcile_map(rows.into_iter().collect(), state);
    }

    fn reconcile_map(&self, next: BTreeMap<K, Arc<T>>, state: LiveCollectionState<C>) {
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
            self.publish_revision(None, state);
        } else {
            self.publish_revision(Some(MapDiff::Batch { changes }), state);
        }
    }

    fn publish_revision(&self, diff: Option<MapDiff<K, Arc<T>>>, state: LiveCollectionState<C>) {
        hyphae::batch(|| {
            if let Some(diff) = diff.as_ref() {
                self.rows.apply_diff_owned(diff.clone());
            }
            self.revision.set(LiveCollectionRevision { diff, state });
        });
    }

    /// Retains rows while marking the collection stale during recovery.
    pub fn resynchronizing(&self, reason: impl Into<String>) {
        let previous = self.revision.get().state;
        let state = LiveCollectionState {
            through: previous.through,
            liveness: SubscriptionLiveness::Resynchronizing {
                reason: reason.into(),
            },
        };
        self.publish_revision(None, state);
    }

    /// Retains rows while marking the collection unusable.
    pub fn invalidate(&self, reason: impl Into<String>) {
        let previous = self.revision.get().state;
        let state = LiveCollectionState {
            through: previous.through,
            liveness: SubscriptionLiveness::Invalid {
                reason: reason.into(),
            },
        };
        self.publish_revision(None, state);
    }
}

/// Creates application and adapter halves of one keyed live collection.
#[must_use]
pub fn live_collection<T, C, K>(
    rows: Vec<(K, Arc<T>)>,
    state: LiveCollectionState<C>,
) -> (LiveCollectionWriter<T, C, K>, LiveCollection<T, C, K>)
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
{
    let mutable_rows = CellMap::new().with_name("myko.live_collection.rows");
    mutable_rows.replace_all(rows.clone());
    let mutable_revision = Cell::new(LiveCollectionRevision {
        diff: Some(MapDiff::Initial { entries: rows }),
        state,
    })
    .with_name("myko.live_collection.revision");
    let revision = mutable_revision.clone().lock();
    let state = revision
        .clone()
        .map(|revision| revision.state.clone())
        .materialize()
        .with_name("myko.live_collection.state");
    let readable = LiveCollection {
        rows: mutable_rows.clone().lock(),
        state,
        revision,
    };
    (
        LiveCollectionWriter {
            rows: mutable_rows,
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
    publication: Cell<LivePublication<LiveSubscriptionState<T, C>>, CellImmutable>,
}

/// A retained driver for one typed reactive value projection.
///
/// Transport adapters implement this trait so UI integrations can retain the
/// subscription lifecycle without naming or inspecting the transport-specific
/// owner type.
pub trait LiveSubscriptionHandle<T, C = LogPosition>: Send + Sync + 'static
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Returns the transport-independent Hyphae projection.
    fn live_subscription(&self) -> &LiveSubscription<T, C>;
}

/// A retained driver for one typed reactive collection projection.
///
/// The collection remains a keyed Hyphae map; this trait only erases which
/// local or remote adapter keeps it current.
pub trait LiveCollectionHandle<T, C = LogPosition, K = Arc<str>>: Send + Sync + 'static
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
    K: hyphae::CellValue + std::hash::Hash + Eq + Ord,
{
    /// Returns the transport-independent keyed Hyphae projection.
    fn live_collection(&self) -> &LiveCollection<T, C, K>;
}

impl<T, C> LiveSubscription<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Returns whether two handles observe the exact same materialized Hyphae
    /// state cell.
    #[must_use]
    pub fn shares_state_with(&self, other: &Self) -> bool {
        self.state.id() == other.state.id()
    }

    /// Returns the Hyphae cell used to compose reports, views, and UI state.
    #[must_use]
    pub const fn state(&self) -> &Cell<LiveSubscriptionState<T, C>, CellImmutable> {
        &self.state
    }

    /// Returns ordered immutable value, cursor, and liveness publications.
    ///
    /// Sequences belong to this output, not the durable journal. They may skip
    /// during reactive coalescing; a consumer must reject older or duplicate
    /// publications rather than reconstructing state from unsequenced diffs.
    #[must_use]
    pub const fn publication(
        &self,
    ) -> &Cell<LivePublication<LiveSubscriptionState<T, C>>, CellImmutable> {
        &self.publication
    }

    /// Opens an ordered current-then-live stream without a snapshot/listener gap.
    ///
    /// The subscriber is installed before its seed is accepted. Older or
    /// duplicate publications cannot overwrite a newer observed version.
    /// A slow consumer receives the latest complete snapshot and may skip
    /// intermediate publications; this is not a lossless event stream.
    #[must_use]
    pub fn watch_publications(&self) -> LivePublicationStream<LiveSubscriptionState<T, C>> {
        LivePublicationStream::from_cell(&self.publication)
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
    /// the resulting value retains the same subscription surface. This assigns
    /// local observation sequences; it does not validate durable completeness
    /// or recover ordering information absent from the supplied state cell.
    /// Native retained handlers must preserve their source publications instead.
    ///
    /// Exhausting the local sequence publishes an invalid terminal revision.
    #[must_use]
    pub fn from_state_cell(state: Cell<LiveSubscriptionState<T, C>, CellImmutable>) -> Self {
        let sequence = AtomicU64::new(0);
        let publication = state
            .map(move |state| {
                let next = sequence.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                });
                let mut state = state.clone();
                let sequence = match next {
                    Ok(sequence) => sequence,
                    Err(sequence) => {
                        state.liveness = SubscriptionLiveness::Invalid {
                            reason: "live subscription publication sequence exhausted".to_owned(),
                        };
                        sequence
                    }
                };
                LivePublication { sequence, state }
            })
            .materialize();
        Self::from_publication_cell(publication)
    }

    fn from_publication_cell(
        publication: Cell<LivePublication<LiveSubscriptionState<T, C>>, CellImmutable>,
    ) -> Self {
        let state = publication
            .clone()
            .map(|publication| publication.state.clone())
            .materialize()
            .with_name("myko.live_subscription.state");
        Self { state, publication }
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
        let initial_publication = self.publication.get();
        let initial_source = initial_publication.state;
        let initial_mapped = LiveSubscriptionState {
            value: initial_source.value.as_ref().map(&transform),
            through: initial_source.through.clone(),
            liveness: initial_source.liveness.clone(),
        };
        let publication = self
            .publication
            .clone()
            .scan(
                (
                    initial_source.value,
                    LivePublication {
                        sequence: initial_publication.sequence,
                        state: initial_mapped,
                    },
                ),
                move |(previous_source, previous_mapped), publication| {
                    if publication.sequence <= previous_mapped.sequence {
                        return (previous_source.clone(), previous_mapped.clone());
                    }
                    let source = &publication.state;
                    let value = if source.value == *previous_source {
                        previous_mapped.state.value.clone()
                    } else {
                        source.value.as_ref().map(&transform)
                    };
                    (
                        source.value.clone(),
                        LivePublication {
                            sequence: publication.sequence,
                            state: LiveSubscriptionState {
                                value,
                                through: source.through.clone(),
                                liveness: source.liveness.clone(),
                            },
                        },
                    )
                },
            )
            .map(|(_, state)| state.clone())
            .materialize()
            .with_name("myko.live_subscription.map_value");
        LiveSubscription::from_publication_cell(publication)
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

    /// Joins dependencies that advance through independent cursor spaces.
    ///
    /// Either current dependency may publish immediately. The result records
    /// both exact cursors in a [`CompositeFrontier`] instead of pretending they
    /// share an ordering. During reconnection the last complete value/frontier
    /// is retained and marked stale until both dependencies are current again.
    #[must_use]
    pub fn join_frontiers<U, D>(
        &self,
        other: &LiveSubscription<U, D>,
    ) -> LiveSubscription<(T, U), CompositeFrontier<C, D>>
    where
        U: hyphae::CellValue,
        D: hyphae::CellValue,
    {
        let initial_dependencies = (self.current(), other.current());
        let initial = frontier_join_state(
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
            .scan(initial, frontier_join_state)
            .materialize()
            .with_name("myko.live_subscription.join_frontiers");
        LiveSubscription::from_state_cell(state)
    }
}

fn frontier_join_state<T, U, C, D>(
    previous: &LiveSubscriptionState<(T, U), CompositeFrontier<C, D>>,
    dependencies: &(LiveSubscriptionState<T, C>, LiveSubscriptionState<U, D>),
) -> LiveSubscriptionState<(T, U), CompositeFrontier<C, D>>
where
    T: hyphae::CellValue,
    U: hyphae::CellValue,
    C: hyphae::CellValue,
    D: hyphae::CellValue,
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
        let liveness = if previous.value.is_some() {
            SubscriptionLiveness::Resynchronizing {
                reason: "waiting for independent dependencies".to_owned(),
            }
        } else {
            SubscriptionLiveness::Connecting
        };
        return LiveSubscriptionState {
            value: previous.value.clone(),
            through: previous.through.clone(),
            liveness,
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
        through: Some(CompositeFrontier {
            left: left.through.clone(),
            right: right.through.clone(),
        }),
        liveness: SubscriptionLiveness::Current,
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
/// Sequence exhaustion terminates the publication cell with an error.
#[derive(Clone)]
pub struct LiveSubscriptionWriter<T, C = LogPosition>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    source: PublicationSource<LiveSubscriptionState<T, C>>,
}

impl<T, C> LiveSubscriptionWriter<T, C>
where
    T: hyphae::CellValue,
    C: hyphae::CellValue,
{
    /// Replaces the complete coherent lifecycle revision.
    ///
    /// This is used by transports that receive an already
    /// validated Myko lifecycle state. Native adapters normally prefer the
    /// narrower [`Self::publish`], [`Self::resynchronizing`], and
    /// [`Self::invalidate`] operations.
    pub fn replace(&self, state: LiveSubscriptionState<T, C>) {
        self.update(|_| state);
    }

    /// Captures and replaces a snapshot in this writer's acceptance order.
    ///
    /// Adapters use this when an external notification is only a wakeup and
    /// its source must be read before assigning the next publication sequence.
    /// The reader runs under the acceptance lock and must not reenter this
    /// writer. Subscriber callbacks run after that lock is released.
    pub fn replace_with(&self, read: impl FnOnce() -> LiveSubscriptionState<T, C>) {
        self.update(|_| read());
    }

    /// Publishes an authoritative snapshot or atomic update.
    pub fn publish(&self, value: T, through: Option<C>) {
        self.replace(LiveSubscriptionState {
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
        self.update(|previous| LiveSubscriptionState {
            value: previous.value.clone(),
            through,
            liveness: previous.liveness.clone(),
        });
    }

    /// Retains the last value while an adapter reconnects and resynchronizes.
    pub fn resynchronizing(&self, reason: impl Into<String>) {
        self.update(|previous| LiveSubscriptionState {
            value: previous.value.clone(),
            through: previous.through.clone(),
            liveness: SubscriptionLiveness::Resynchronizing {
                reason: reason.into(),
            },
        });
    }

    /// Marks the subscription unusable while retaining its last stale value.
    pub fn invalidate(&self, reason: impl Into<String>) {
        self.update(|previous| LiveSubscriptionState {
            value: previous.value.clone(),
            through: previous.through.clone(),
            liveness: SubscriptionLiveness::Invalid {
                reason: reason.into(),
            },
        });
    }

    fn update(
        &self,
        update: impl FnOnce(&LiveSubscriptionState<T, C>) -> LiveSubscriptionState<T, C>,
    ) {
        if let Err(error) = self.source.update(update) {
            self.source.fail(error);
        }
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
    let source = PublicationSource::new(initial);
    let readable = LiveSubscription::from_publication_cell(source.publication());
    (LiveSubscriptionWriter { source }, readable)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use hyphae::{MapValuesExt as _, Signal, Watchable as _};

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RuntimeRow {
        id: &'static str,
        value: u32,
    }

    #[test]
    fn runtime_snapshot_publication_emits_only_changed_rows() {
        let collection = RuntimeCollection::new(
            vec![
                RuntimeRow { id: "a", value: 1 },
                RuntimeRow { id: "b", value: 1 },
            ],
            |row| Arc::from(row.id),
        );

        collection.publish(vec![
            RuntimeRow { id: "a", value: 1 },
            RuntimeRow { id: "b", value: 2 },
        ]);

        wait_until(|| collection.live().revision().get().state.through == Some(1));
        let revision = collection.live().revision().get();
        assert!(matches!(
            revision.diff,
            Some(MapDiff::Batch { changes })
                if matches!(
                    changes.as_slice(),
                    [MapDiff::Update { key, new_value, .. }]
                        if key.as_ref() == "b" && new_value.value == 2
                )
        ));
        assert_eq!(revision.state.through, Some(1));
        assert_eq!(revision.state.liveness, SubscriptionLiveness::Current);
    }

    #[test]
    fn collection_union_preserves_keyed_diffs_and_independent_frontiers() {
        let left = RuntimeCollection::new(vec![RuntimeRow { id: "a", value: 1 }], |row| {
            Arc::from(row.id)
        });
        let right = RuntimeCollection::new(vec![RuntimeRow { id: "b", value: 2 }], |row| {
            Arc::from(row.id)
        });
        let union = left
            .live()
            .clone()
            .union(right.live().clone())
            .materialize();

        assert_eq!(
            union
                .rows()
                .snapshot()
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
            BTreeMap::from([
                (Arc::from("a"), Arc::new(RuntimeRow { id: "a", value: 1 })),
                (Arc::from("b"), Arc::new(RuntimeRow { id: "b", value: 2 })),
            ])
        );
        assert_eq!(
            union.current_state().through,
            Some(CompositeFrontier {
                left: Some(0),
                right: Some(0),
            })
        );

        left.publish(vec![
            RuntimeRow { id: "a", value: 1 },
            RuntimeRow { id: "c", value: 3 },
        ]);
        wait_until(|| {
            union.revision().get().state.through
                == Some(CompositeFrontier {
                    left: Some(1),
                    right: Some(0),
                })
        });
        let revision = union.revision().get();
        assert!(
            matches!(
                revision.diff,
                Some(MapDiff::Batch { ref changes })
                    if matches!(
                        changes.as_slice(),
                        [MapDiff::Insert { key, value }]
                            if key.as_ref() == "c" && value.value == 3
                    )
            ),
            "unexpected union revision: {revision:?}"
        );

        right.publish(vec![RuntimeRow { id: "b", value: 4 }]);
        wait_until(|| {
            union.revision().get().state.through
                == Some(CompositeFrontier {
                    left: Some(1),
                    right: Some(1),
                })
        });
        assert!(matches!(
            union.revision().get().diff,
            Some(MapDiff::Batch { changes })
                if matches!(
                    changes.as_slice(),
                    [MapDiff::Update { key, new_value, .. }]
                        if key.as_ref() == "b" && new_value.value == 4
                )
        ));
    }

    #[test]
    fn collection_union_invalidates_on_duplicate_identity_and_recovers() {
        let left = RuntimeCollection::new(
            vec![RuntimeRow {
                id: "same",
                value: 1,
            }],
            |row| Arc::from(row.id),
        );
        let right = RuntimeCollection::new(
            vec![RuntimeRow {
                id: "same",
                value: 2,
            }],
            |row| Arc::from(row.id),
        );
        let union = left
            .live()
            .clone()
            .union(right.live().clone())
            .materialize();

        assert!(union.rows().snapshot().is_empty());
        assert!(matches!(
            union.current_state().liveness,
            SubscriptionLiveness::Invalid { reason }
                if reason.contains("duplicate key")
        ));

        right.publish(vec![RuntimeRow {
            id: "other",
            value: 2,
        }]);
        wait_until(|| union.revision().get().state.liveness == SubscriptionLiveness::Current);
        assert_eq!(union.rows().snapshot().len(), 2);
        assert!(union.rows().get_value(&Arc::from("same")).is_some());
        assert!(union.rows().get_value(&Arc::from("other")).is_some());
    }

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

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        while !condition() && Instant::now() < deadline {
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
        wait_until(|| {
            observed.lock().is_ok_and(|observed| {
                observed
                    .iter()
                    .any(|state| state.through == Some(LogPosition::new(9)))
            })
        });

        let current = subscription.current();
        assert_eq!(current.value, Some(vec!["new".to_owned()]));
        assert_eq!(current.through, Some(LogPosition::new(9)));
        assert_eq!(current.liveness, SubscriptionLiveness::Current);
        assert!(observed.lock().is_ok_and(|observed| {
            observed
                .iter()
                .any(|state| state.through == Some(LogPosition::new(9)))
        }));
    }

    #[test]
    fn batched_reconnection_retains_the_latest_accepted_snapshot() {
        let (writer, subscription) = live_subscription(LiveSubscriptionState {
            value: Some("old".to_owned()),
            through: Some(LogPosition::new(1)),
            liveness: SubscriptionLiveness::Current,
        });

        hyphae::batch(|| {
            writer.publish("new".to_owned(), Some(LogPosition::new(2)));
            writer.resynchronizing("connection ended after update");
        });
        wait_until(|| {
            matches!(
                subscription.current().liveness,
                SubscriptionLiveness::Resynchronizing { .. }
            )
        });

        let state = subscription.current();
        assert_eq!(state.value.as_deref(), Some("new"));
        assert_eq!(state.through, Some(LogPosition::new(2)));
    }

    #[test]
    fn batched_cursor_advance_retains_the_latest_accepted_value() {
        let (writer, subscription) = live_subscription(LiveSubscriptionState {
            value: Some("old".to_owned()),
            through: Some(LogPosition::new(1)),
            liveness: SubscriptionLiveness::Current,
        });

        hyphae::batch(|| {
            writer.publish("new".to_owned(), Some(LogPosition::new(2)));
            writer.advance_through(Some(LogPosition::new(3)));
        });
        wait_until(|| subscription.current().through == Some(LogPosition::new(3)));

        assert_eq!(subscription.current().value.as_deref(), Some("new"));
    }

    #[test]
    fn mapped_publications_advance_metadata_without_recomputing_the_value() {
        let (writer, source) = live_subscription(LiveSubscriptionState::<String, u64> {
            value: Some("value".to_owned()),
            through: Some(1),
            liveness: SubscriptionLiveness::Current,
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_map = Arc::clone(&calls);
        let mapped = source.map_value(move |value| {
            calls_for_map.fetch_add(1, Ordering::Relaxed);
            value.len()
        });
        let mut stream = mapped.watch_publications();
        let mut next = || {
            let mut publication = None;
            wait_until(|| {
                publication = stream.try_recv().ok();
                publication.is_some()
            });
            publication
        };
        let initial = next();
        assert_eq!(initial.as_ref().map(|p| p.sequence), Some(0));
        assert_eq!(initial.and_then(|p| p.state.value), Some(5));

        writer.advance_through(Some(2));
        let progressed = next();
        assert_eq!(progressed.as_ref().map(|p| p.sequence), Some(1));
        assert_eq!(progressed.as_ref().and_then(|p| p.state.value), Some(5));
        assert_eq!(progressed.and_then(|p| p.state.through), Some(2));

        writer.invalidate("source disconnected");
        let invalid = next();
        assert_eq!(invalid.as_ref().map(|p| p.sequence), Some(2));
        assert_eq!(invalid.as_ref().and_then(|p| p.state.value), Some(5));
        assert!(matches!(
            invalid.map(|p| p.state.liveness),
            Some(SubscriptionLiveness::Invalid { .. })
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
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
    fn mapped_subscription_does_not_recompute_for_cursor_or_liveness_only_changes() {
        let (writer, subscription) = live_subscription(LiveSubscriptionState {
            value: Some("persisted".to_owned()),
            through: Some(LogPosition::new(7)),
            liveness: SubscriptionLiveness::Current,
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_map = Arc::clone(&calls);
        let mapped = subscription.map_value(move |value| {
            calls_for_map.fetch_add(1, Ordering::AcqRel);
            value.len()
        });
        assert_eq!(calls.load(Ordering::Acquire), 1);

        writer.resynchronizing("peer reconnecting");
        writer.publish("persisted".to_owned(), Some(LogPosition::new(8)));
        wait_until(|| mapped.current().through == Some(LogPosition::new(8)));
        assert_eq!(mapped.current().through, Some(LogPosition::new(8)));
        assert_eq!(calls.load(Ordering::Acquire), 1);

        writer.invalidate("source released");
        wait_until(|| {
            matches!(
                mapped.current().liveness,
                SubscriptionLiveness::Invalid { .. }
            )
        });
        assert_eq!(calls.load(Ordering::Acquire), 1);

        writer.publish("changed".to_owned(), Some(LogPosition::new(9)));
        wait_until(|| mapped.current().value == Some(7));
        assert_eq!(mapped.current().value, Some(7));
        assert_eq!(calls.load(Ordering::Acquire), 2);
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
        wait_until(|| {
            matches!(
                joined.current().liveness,
                SubscriptionLiveness::Resynchronizing { .. }
            )
        });
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
        wait_until(|| joined.current().through == Some(LogPosition::new(2)));
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
        wait_until(|| {
            matches!(
                joined.current().liveness,
                SubscriptionLiveness::Resynchronizing { .. }
            )
        });
        assert!(matches!(
            joined.current().liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ));
        right_writer.advance_through(Some(LogPosition::new(2)));
        wait_until(|| joined.current().through == Some(LogPosition::new(2)));

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
    fn frontier_join_tracks_independent_cursor_spaces_without_waiting() {
        let (left_writer, left) = live_subscription(LiveSubscriptionState {
            value: Some("runtime-1".to_owned()),
            through: Some(1_u64),
            liveness: SubscriptionLiveness::Current,
        });
        let (right_writer, right) = live_subscription(LiveSubscriptionState {
            value: Some("history-7".to_owned()),
            through: Some(LogPosition::new(7)),
            liveness: SubscriptionLiveness::Current,
        });
        let joined = left.join_frontiers(&right);
        assert_eq!(
            joined.current(),
            LiveSubscriptionState {
                value: Some(("runtime-1".to_owned(), "history-7".to_owned())),
                through: Some(CompositeFrontier {
                    left: Some(1),
                    right: Some(LogPosition::new(7)),
                }),
                liveness: SubscriptionLiveness::Current,
            }
        );

        left_writer.publish("runtime-2".to_owned(), Some(2));
        wait_until(|| {
            joined
                .current()
                .through
                .as_ref()
                .and_then(|value| value.left)
                == Some(2)
        });
        assert_eq!(
            joined.current(),
            LiveSubscriptionState {
                value: Some(("runtime-2".to_owned(), "history-7".to_owned())),
                through: Some(CompositeFrontier {
                    left: Some(2),
                    right: Some(LogPosition::new(7)),
                }),
                liveness: SubscriptionLiveness::Current,
            }
        );

        right_writer.resynchronizing("remote reconnect");
        wait_until(|| {
            matches!(
                joined.current().liveness,
                SubscriptionLiveness::Resynchronizing { .. }
            )
        });
        let stale = joined.current();
        assert_eq!(
            stale.value,
            Some(("runtime-2".to_owned(), "history-7".to_owned()))
        );
        assert!(matches!(
            stale.liveness,
            SubscriptionLiveness::Resynchronizing { .. }
        ));
        right_writer.publish("history-8".to_owned(), Some(LogPosition::new(8)));
        wait_until(|| {
            joined
                .current()
                .through
                .as_ref()
                .and_then(|value| value.right)
                == Some(LogPosition::new(8))
        });
        assert_eq!(
            joined.current().through,
            Some(CompositeFrontier {
                left: Some(2),
                right: Some(LogPosition::new(8)),
            })
        );
    }

    #[test]
    fn collection_projects_rows_into_a_live_derived_value() {
        let (writer, collection) = live_collection(
            vec![(Arc::<str>::from("one"), Arc::new(1_u32))],
            LiveCollectionState {
                through: Some(1_u64),
                liveness: SubscriptionLiveness::Current,
            },
        );
        let values = collection.as_subscription();
        assert_eq!(values.current().value, Some(vec![1]));

        writer.apply(
            MapDiff::Insert {
                key: Arc::from("two"),
                value: Arc::new(2),
            },
            Some(2),
        );
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        while values.current().through != Some(2) && Instant::now() < deadline {
            std::thread::yield_now();
        }

        assert_eq!(
            values.current(),
            LiveSubscriptionState {
                value: Some(vec![1, 2]),
                through: Some(2),
                liveness: SubscriptionLiveness::Current,
            }
        );
    }

    #[test]
    fn collection_projects_rows_as_fine_grained_hyphae_diffs() {
        let key = Arc::<str>::from("message-1");
        let (writer, collection) = live_collection(
            vec![(Arc::clone(&key), Arc::new(1_u32))],
            LiveCollectionState {
                through: Some(1_u64),
                liveness: SubscriptionLiveness::Current,
            },
        );
        let projected = collection
            .project_rows(|rows| rows.map_values(|_, value| Arc::new(format!("value:{value}"))))
            .materialize();

        writer.apply(
            MapDiff::Update {
                key: Arc::clone(&key),
                old_value: Arc::new(1),
                new_value: Arc::new(2),
            },
            Some(2),
        );

        let deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        while (projected
            .rows()
            .get_value(&key)
            .is_none_or(|value| value.as_str() != "value:2")
            || projected.revision().get().state.through != Some(2))
            && Instant::now() < deadline
        {
            std::thread::yield_now();
        }

        assert_eq!(
            projected.rows().get_value(&key).as_deref(),
            Some(&"value:2".to_owned())
        );
        let revision = projected.revision().get();
        assert_eq!(revision.state.through, Some(2));
        assert!(matches!(
            revision.diff,
            Some(MapDiff::Update { ref key, .. }) if key.as_ref() == "message-1"
        ));
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
