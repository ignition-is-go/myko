//! Query registration via inventory.

use std::{
    any::Any,
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use hyphae::{MapDiff, SelectExt};
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use super::{
    super::item::Eventable,
    cell::FilteredCellMap,
    context::{QueryCellContext, QueryContext},
    request::QueryRequest,
    traits::{AnyQuery, QueryBuildCellCtx, QueryHandler, QueryParams, QueryTestCtx},
};
use crate::{
    common::with_id::WithId, core::item::downcast_any_item_arc, request::RequestContext,
    server::CellServerCtx, store::StoreRegistry,
};

// ─────────────────────────────────────────────────────────────────────────────
// Type aliases for function pointers
// ─────────────────────────────────────────────────────────────────────────────

/// Type alias for query parse function.
pub type QueryParseFn = fn(Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;

/// Type-erased cell factory for queries.
/// Takes a typed query, registry, and host_id, returns a FilteredCellMap.
pub type QueryCellFactory = fn(
    Arc<dyn AnyQuery>,
    Arc<StoreRegistry>,
    Arc<RequestContext>,
    Option<Arc<CellServerCtx>>,
) -> Result<FilteredCellMap, String>;

type AnyItemArc = Arc<dyn crate::core::item::AnyItem>;
type AnyItemMap = hyphae::CellMap<Arc<str>, AnyItemArc>;
type WeakAnyItemMap = hyphae::WeakCellMap<Arc<str>, AnyItemArc>;
type BucketEntries = Vec<(Arc<str>, AnyItemArc)>;
type BucketDiff = MapDiff<Arc<str>, AnyItemArc>;
type BucketDiffs = Vec<BucketDiff>;

/// Ordered foreign-key values for a `BelongsToSourceIndex` bucket. A single
/// `#[belongs_to]` field yields a 1-element key (the pre-compound-routing
/// shape); an entity with 2+ `#[belongs_to]` fields queried with more than
/// one set at once yields one element per field that was `Some`, in the
/// entity's declared field order. Two queries that populate different SETS
/// of belongs_to fields land in different buckets even for the same entity
/// type — the bucket only ever holds items matching every field in the key.
type CompoundKey = Vec<Arc<str>>;

/// Extracts the compound foreign-key values (see [`CompoundKey`]) an item
/// contributes for one specific field combination. Position `i` in the
/// returned `Vec` corresponds to field `i` of that combination. Returns
/// `None` only on a downcast failure (item is the wrong entity type) —
/// `#[belongs_to]` fields feeding this are all non-optional, so a
/// correctly-typed item always has a value for every field in play.
///
/// Deliberately a distinct type from
/// [`crate::core::relationship::FkExtractor`]: that alias backs
/// `RelationshipManager`'s own single-field child-index tracking and
/// `export_tree`'s traversal, both unrelated to query routing — widening it
/// in place would ripple into those unrelated subsystems for no reason.
type CompoundFkExtractor = fn(&dyn std::any::Any) -> Option<Vec<Arc<str>>>;

// ─────────────────────────────────────────────────────────────────────────────
// QueryRegistration - inventory-based registration
// ─────────────────────────────────────────────────────────────────────────────

inventory::collect!(QueryRegistration);

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryRuntimeMetrics {
    pub cell_factories_created: u64,
    pub per_item_guards_created: u64,
    pub per_item_guards_removed: u64,
}

#[derive(Debug, Clone, Default)]
pub struct QueryRuntimePerIdMetrics {
    pub query_id: Arc<str>,
    pub cell_factories_created: u64,
    pub per_item_guards_created: u64,
    pub per_item_guards_removed: u64,
}

static QUERY_CELL_FACTORIES_CREATED: AtomicU64 = AtomicU64::new(0);
static QUERY_PER_ITEM_GUARDS_CREATED: AtomicU64 = AtomicU64::new(0);
static QUERY_PER_ITEM_GUARDS_REMOVED: AtomicU64 = AtomicU64::new(0);
static QUERY_FACTORIES_BY_ID: OnceLock<DashMap<Arc<str>, u64>> = OnceLock::new();
static QUERY_GUARDS_CREATED_BY_ID: OnceLock<DashMap<Arc<str>, u64>> = OnceLock::new();
static QUERY_GUARDS_REMOVED_BY_ID: OnceLock<DashMap<Arc<str>, u64>> = OnceLock::new();
static BELONGS_TO_SOURCE_INDEXES: OnceLock<DashMap<String, Arc<BelongsToSourceIndex>>> =
    OnceLock::new();

fn query_factories_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_FACTORIES_BY_ID.get_or_init(DashMap::new)
}

fn query_guards_created_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_GUARDS_CREATED_BY_ID.get_or_init(DashMap::new)
}

fn query_guards_removed_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_GUARDS_REMOVED_BY_ID.get_or_init(DashMap::new)
}

fn belongs_to_source_indexes() -> &'static DashMap<String, Arc<BelongsToSourceIndex>> {
    BELONGS_TO_SOURCE_INDEXES.get_or_init(DashMap::new)
}

fn increment_counter(map: &DashMap<Arc<str>, u64>, key: Arc<str>) {
    if let Some(mut value) = map.get_mut(&key) {
        *value = value.saturating_add(1);
    } else {
        map.insert(key, 1);
    }
}

pub fn query_runtime_metrics() -> QueryRuntimeMetrics {
    QueryRuntimeMetrics {
        cell_factories_created: QUERY_CELL_FACTORIES_CREATED.load(Ordering::Relaxed),
        per_item_guards_created: QUERY_PER_ITEM_GUARDS_CREATED.load(Ordering::Relaxed),
        per_item_guards_removed: QUERY_PER_ITEM_GUARDS_REMOVED.load(Ordering::Relaxed),
    }
}

pub fn query_runtime_metrics_by_id(limit: usize) -> Vec<QueryRuntimePerIdMetrics> {
    let mut rows: Vec<QueryRuntimePerIdMetrics> = query_factories_by_id()
        .iter()
        .map(|entry| {
            let query_id = entry.key().clone();
            let cell_factories_created = *entry.value();
            let per_item_guards_created = query_guards_created_by_id()
                .get(&query_id)
                .map(|v| *v.value())
                .unwrap_or(0);
            let per_item_guards_removed = query_guards_removed_by_id()
                .get(&query_id)
                .map(|v| *v.value())
                .unwrap_or(0);
            QueryRuntimePerIdMetrics {
                query_id,
                cell_factories_created,
                per_item_guards_created,
                per_item_guards_removed,
            }
        })
        .collect();

    rows.sort_by(|a, b| {
        let a_live = a
            .per_item_guards_created
            .saturating_sub(a.per_item_guards_removed);
        let b_live = b
            .per_item_guards_created
            .saturating_sub(b.per_item_guards_removed);
        b_live
            .cmp(&a_live)
            .then_with(|| b.cell_factories_created.cmp(&a.cell_factories_created))
    });
    if rows.len() > limit {
        rows.truncate(limit);
    }
    rows
}

/// Per-compound-key bucket index backing `#[belongs_to]` reactive queries.
///
/// One `BelongsToSourceIndex` instance is scoped to a single SET of
/// belongs_to fields (see `build_belongs_to_source_map`'s registry key,
/// which includes the field-name list) — routing a query that sets fields
/// `{node_id}` and one that sets `{node_id, session_id}` always land in
/// different indexes, never sharing buckets, even though both touch
/// `node_id`. Within one index, `buckets` holds only *weak* handles: a
/// bucket's `AnyItemMap` stays alive exactly as long as some subscriber (via
/// [`build_belongs_to_source_map`]) holds a strong reference to it. Once the
/// last subscriber drops it, the weak entry naturally fails to upgrade and
/// gets lazily reaped — the alternative (a strong `Arc<AnyItemMap>` retained
/// forever) is a real memory leak: one permanent bucket per distinct key
/// ever seen, which never shrinks even after every relation is unassigned.
struct BelongsToSourceIndex {
    store: Arc<crate::store::EntityStore>,
    buckets: DashMap<CompoundKey, WeakAnyItemMap>,
    _driver: Arc<AnyItemMap>,
}

impl BelongsToSourceIndex {
    fn new(store: Arc<crate::store::EntityStore>, extract_fk: CompoundFkExtractor) -> Arc<Self> {
        let driver = Arc::new(AnyItemMap::new());
        let index = Arc::new(Self {
            store: store.clone(),
            buckets: DashMap::new(),
            _driver: driver.clone(),
        });

        let index_for_diffs = index.clone();
        let guard = store.subscribe_diffs(move |diff| {
            index_for_diffs.apply_diff(diff, extract_fk);
        });
        driver.own_guard(guard);
        index
    }

    /// Look up a bucket for *internal diff routing only* — never creates
    /// one. If nobody's subscribed to `key` there's no bucket state worth
    /// maintaining; a dead weak entry found here is reaped immediately, so
    /// `self.buckets` never accumulates more than what's currently live. See
    /// [`sweep_dead_buckets`](Self::sweep_dead_buckets) for the backstop
    /// covering keys that go dead but are never looked up again.
    fn route_to_live_bucket(&self, key: &CompoundKey) -> Option<AnyItemMap> {
        let entry = self.buckets.get(key)?;
        if let Some(map) = entry.upgrade() {
            return Some(map);
        }
        drop(entry);
        self.buckets.remove(key);
        None
    }

    /// Subscriber entry point (via [`build_belongs_to_source_map`]): returns
    /// the live bucket if one exists, or creates a fresh one backfilled from
    /// the current store state.
    ///
    /// The backfill matters: `apply_diff` (via `route_to_live_bucket`) never
    /// creates or updates a bucket nobody's watching, so a newly-live bucket
    /// may have missed every diff since this relation's one-time index-wide
    /// bootstrap in [`new`](Self::new). Without backfilling here, a client
    /// subscribing (or re-subscribing after a prior subscriber dropped off)
    /// would silently see an empty result instead of the parent's actual
    /// current children.
    ///
    /// Uses `self.buckets.entry(key)` rather than a separate get-then-insert
    /// (what `route_to_live_bucket` does for pure lookups) because this path
    /// *creates*: two concurrent callers for the same key that both observe
    /// no live bucket would otherwise each build and backfill their own
    /// `AnyItemMap`, then race an unconditional `insert` — whichever lands
    /// second wins `self.buckets`, silently orphaning the other's bucket
    /// (still a valid handle, already returned to its caller and subscribed
    /// to, but now unreachable from `apply_diff`'s routing, so it gets its
    /// one-time backfill and then nothing else, ever). `entry()` holds the
    /// shard lock across the whole check-or-create, so only one caller per
    /// key can ever end up as the live entry.
    fn bucket_for(&self, key: CompoundKey, extract_fk: CompoundFkExtractor) -> AnyItemMap {
        match self.buckets.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                if let Some(map) = occupied.get().upgrade() {
                    return map;
                }
                let map = Self::build_backfilled_bucket(&self.store, occupied.key(), extract_fk);
                occupied.insert(map.downgrade());
                map
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                let map = Self::build_backfilled_bucket(&self.store, vacant.key(), extract_fk);
                vacant.insert(map.downgrade());
                map
            }
        }
    }

    fn build_backfilled_bucket(
        store: &crate::store::EntityStore,
        key: &CompoundKey,
        extract_fk: CompoundFkExtractor,
    ) -> AnyItemMap {
        let map = AnyItemMap::new();
        let backfill: BucketEntries = store
            .snapshot()
            .into_iter()
            .filter(|(_, item)| extract_fk(item.as_any()).as_ref() == Some(key))
            .collect();
        if !backfill.is_empty() {
            map.apply_batch(vec![MapDiff::Initial { entries: backfill }]);
        }
        map
    }

    /// Drop bucket entries nobody's subscribed to anymore. `route_to_live_bucket`
    /// already reaps dead entries lazily on next access, but a key that goes
    /// dead and is never looked up again would otherwise sit in
    /// `self.buckets` forever (just a `Weak` + a small `Vec<Arc<str>>` key,
    /// far smaller than the leak this replaces, but still unbounded over
    /// time). Called from `CellServerCtx::sweep_dead_cache_entries` via
    /// [`sweep_all_belongs_to_source_indexes`].
    fn sweep_dead_buckets(&self) {
        self.buckets.retain(|_, weak| weak.upgrade().is_some());
    }

    fn apply_diff(&self, diff: &BucketDiff, extract_fk: CompoundFkExtractor) {
        match diff {
            MapDiff::Initial { entries } => {
                let mut grouped: HashMap<CompoundKey, BucketEntries> = HashMap::new();
                for (id, item) in entries {
                    if let Some(fk) = extract_fk(item.as_any()) {
                        grouped
                            .entry(fk)
                            .or_default()
                            .push((id.clone(), item.clone()));
                    }
                }

                // Every currently-*live* bucket needs to receive the new
                // state — if its key is absent from `grouped`, that bucket
                // is now empty and must emit `Initial { empty }` so
                // downstream subscribers observe the drop. (Dead/unsubscribed
                // buckets don't need this — nobody's watching them, and
                // `route_to_live_bucket` reaps them lazily regardless.)
                // This explicit empty-notify (rather than just letting a
                // dead bucket's Drop silently orphan subscribers) is why
                // this path exists at all: an earlier version just cleared
                // `self.buckets` and relied on ownership drop to clean up,
                // which silently dropped removal events on the floor — a
                // full-clear source diff (e.g. the last matching row being
                // deleted via `remove_many`) never reached bucket
                // subscribers.
                let live_keys: Vec<CompoundKey> = self
                    .buckets
                    .iter()
                    .filter(|entry| entry.value().upgrade().is_some())
                    .map(|entry| entry.key().clone())
                    .collect();
                for key in &live_keys {
                    if !grouped.contains_key(key)
                        && let Some(bucket) = self.route_to_live_bucket(key)
                    {
                        bucket.apply_batch(vec![MapDiff::Initial {
                            entries: Vec::new(),
                        }]);
                    }
                }

                // Route new state only to buckets someone's actually
                // watching — an FK with no live subscriber gets built
                // lazily (with backfill) next time `bucket_for` is called
                // for it via `build_belongs_to_source_map`.
                for (fk, bucket_entries) in grouped {
                    if let Some(bucket) = self.route_to_live_bucket(&fk) {
                        bucket.apply_batch(vec![MapDiff::Initial {
                            entries: bucket_entries,
                        }]);
                    }
                }
            }
            MapDiff::Insert { key, value } => {
                if let Some(fk) = extract_fk(value.as_any())
                    && let Some(bucket) = self.route_to_live_bucket(&fk)
                {
                    bucket.apply_batch(vec![MapDiff::Insert {
                        key: key.clone(),
                        value: value.clone(),
                    }]);
                }
            }
            MapDiff::Remove { key, old_value } => {
                if let Some(fk) = extract_fk(old_value.as_any())
                    && let Some(bucket) = self.route_to_live_bucket(&fk)
                {
                    bucket.apply_batch(vec![MapDiff::Remove {
                        key: key.clone(),
                        old_value: old_value.clone(),
                    }]);
                }
            }
            MapDiff::Update {
                key,
                old_value,
                new_value,
            } => {
                let old_fk = extract_fk(old_value.as_any());
                let new_fk = extract_fk(new_value.as_any());
                match (old_fk, new_fk) {
                    (Some(old_fk), Some(new_fk)) if old_fk == new_fk => {
                        if let Some(bucket) = self.route_to_live_bucket(&new_fk) {
                            bucket.apply_batch(vec![MapDiff::Update {
                                key: key.clone(),
                                old_value: old_value.clone(),
                                new_value: new_value.clone(),
                            }]);
                        }
                    }
                    (Some(old_fk), Some(new_fk)) => {
                        if let Some(bucket) = self.route_to_live_bucket(&old_fk) {
                            bucket.apply_batch(vec![MapDiff::Remove {
                                key: key.clone(),
                                old_value: old_value.clone(),
                            }]);
                        }
                        if let Some(bucket) = self.route_to_live_bucket(&new_fk) {
                            bucket.apply_batch(vec![MapDiff::Insert {
                                key: key.clone(),
                                value: new_value.clone(),
                            }]);
                        }
                    }
                    (Some(old_fk), None) => {
                        if let Some(bucket) = self.route_to_live_bucket(&old_fk) {
                            bucket.apply_batch(vec![MapDiff::Remove {
                                key: key.clone(),
                                old_value: old_value.clone(),
                            }]);
                        }
                    }
                    (None, Some(new_fk)) => {
                        if let Some(bucket) = self.route_to_live_bucket(&new_fk) {
                            bucket.apply_batch(vec![MapDiff::Insert {
                                key: key.clone(),
                                value: new_value.clone(),
                            }]);
                        }
                    }
                    (None, None) => {}
                }
            }
            MapDiff::Batch { changes } => {
                let mut by_fk: HashMap<CompoundKey, BucketDiffs> = HashMap::new();

                for change in changes {
                    match change {
                        MapDiff::Insert { key, value } => {
                            if let Some(fk) = extract_fk(value.as_any()) {
                                by_fk.entry(fk).or_default().push(MapDiff::Insert {
                                    key: key.clone(),
                                    value: value.clone(),
                                });
                            }
                        }
                        MapDiff::Remove { key, old_value } => {
                            if let Some(fk) = extract_fk(old_value.as_any()) {
                                by_fk.entry(fk).or_default().push(MapDiff::Remove {
                                    key: key.clone(),
                                    old_value: old_value.clone(),
                                });
                            }
                        }
                        MapDiff::Update {
                            key,
                            old_value,
                            new_value,
                        } => {
                            let old_fk = extract_fk(old_value.as_any());
                            let new_fk = extract_fk(new_value.as_any());
                            match (old_fk, new_fk) {
                                (Some(old_fk), Some(new_fk)) if old_fk == new_fk => {
                                    by_fk.entry(new_fk).or_default().push(MapDiff::Update {
                                        key: key.clone(),
                                        old_value: old_value.clone(),
                                        new_value: new_value.clone(),
                                    });
                                }
                                (Some(old_fk), Some(new_fk)) => {
                                    by_fk.entry(old_fk).or_default().push(MapDiff::Remove {
                                        key: key.clone(),
                                        old_value: old_value.clone(),
                                    });
                                    by_fk.entry(new_fk).or_default().push(MapDiff::Insert {
                                        key: key.clone(),
                                        value: new_value.clone(),
                                    });
                                }
                                (Some(old_fk), None) => {
                                    by_fk.entry(old_fk).or_default().push(MapDiff::Remove {
                                        key: key.clone(),
                                        old_value: old_value.clone(),
                                    });
                                }
                                (None, Some(new_fk)) => {
                                    by_fk.entry(new_fk).or_default().push(MapDiff::Insert {
                                        key: key.clone(),
                                        value: new_value.clone(),
                                    });
                                }
                                (None, None) => {}
                            }
                        }
                        MapDiff::Initial { .. } | MapDiff::Batch { .. } => {
                            self.apply_diff(change, extract_fk);
                        }
                    }
                }

                for (fk, bucket_changes) in by_fk {
                    if let Some(bucket) = self.route_to_live_bucket(&fk) {
                        bucket.apply_batch(bucket_changes);
                    }
                }
            }
        }
    }
}

/// Build a reactive, `#[belongs_to]`-routed source map for a query that has
/// one or more belongs_to fields set. `field_names` and `foreign_ids` are
/// positionally paired (same order `extract_fk` reads them in) and must be
/// the SAME LENGTH — exactly the fields the caller's query populated, not
/// necessarily every belongs_to field the entity declares. Two calls for the
/// same `local_type` with different `field_names` sets (e.g. `["node_id"]`
/// vs `["node_id", "session_id"]`) always route through separate indexes —
/// see [`BelongsToSourceIndex`] — so a query that pins more fields never
/// shares a bucket with one that pins fewer, even when the fields overlap.
pub fn build_belongs_to_source_map(
    registry: Arc<StoreRegistry>,
    host_id: Uuid,
    local_type: &'static str,
    field_names: &'static [&'static str],
    extract_fk: CompoundFkExtractor,
    foreign_ids: Vec<Arc<str>>,
) -> FilteredCellMap {
    debug_assert_eq!(
        field_names.len(),
        foreign_ids.len(),
        "build_belongs_to_source_map: field_names and foreign_ids must be positionally paired"
    );
    let index =
        belongs_to_source_index_for(&registry, host_id, local_type, field_names, extract_fk);
    index.bucket_for(foreign_ids, extract_fk).lock()
}

fn belongs_to_source_index_for(
    registry: &Arc<StoreRegistry>,
    host_id: Uuid,
    local_type: &'static str,
    field_names: &'static [&'static str],
    extract_fk: CompoundFkExtractor,
) -> Arc<BelongsToSourceIndex> {
    let key = format!("{host_id}:{local_type}:{}", field_names.join("+"));
    belongs_to_source_indexes()
        .entry(key)
        .or_insert_with(|| {
            let store = registry.get_or_create(local_type);
            BelongsToSourceIndex::new(store, extract_fk)
        })
        .clone()
}

/// Above this many union keys, `build_belongs_to_union_source_map` logs a
/// warning instead of silently subscribing to a huge number of buckets — a
/// caller combining several large `In` fields on the same query hits a
/// cartesian-product blow-up (spec §4: "either cap the product size or
/// document the blow-up and log when it exceeds a threshold"). No hard cap:
/// per the spec's hard requirement, an indexed field must never fall back
/// to a table scan, so this only observes, never degrades.
pub const UNION_KEYS_WARN_THRESHOLD: usize = 1000;

/// Rewrite a source bucket's diff into a form safe to apply additively to a
/// union of several such sources: `Initial { entries }` becomes a `Batch`
/// of `Insert`s (a source's Initial is "here is MY full state," which would
/// otherwise replace the union's contents rather than contribute to it —
/// see [`build_belongs_to_union_source_map`]). Insert/Remove/Update are
/// already scoped to one key and forward unchanged; a nested `Batch` is
/// rewritten recursively (its own `Initial`s get the same treatment) since
/// a source can legitimately emit one. Returns `None` for an empty
/// resulting batch (nothing to apply) rather than an empty `Batch` diff.
fn additive_union_diff(diff: &BucketDiff) -> Option<BucketDiff> {
    match diff {
        MapDiff::Initial { entries } => {
            if entries.is_empty() {
                return None;
            }
            Some(MapDiff::Batch {
                changes: entries
                    .iter()
                    .map(|(key, value)| MapDiff::Insert {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            })
        }
        MapDiff::Batch { changes } => {
            let rewritten: Vec<BucketDiff> =
                changes.iter().filter_map(additive_union_diff).collect();
            if rewritten.is_empty() {
                None
            } else {
                Some(MapDiff::Batch { changes: rewritten })
            }
        }
        other => Some(other.clone()),
    }
}

/// Union of K belongs_to buckets — routes an `In` (or multi-field compound
/// `In`) query through [`BelongsToSourceIndex`] as a union of exact-key
/// lookups instead of a table scan (spec §4 hard requirement: an `In` on an
/// indexed field must be index-servable, by construction, since id filters
/// only ever express `Eq`/`In`). Each item can only ever satisfy exactly
/// one of the given compound keys — a field has exactly one value — so this
/// is a partition merge, not a proper union: no dedup/collision handling is
/// needed between the K sources.
///
/// Each source bucket is kept alive by `result.own(guard)` — the same
/// pattern `typed_map_from_any_item_with_typed_id` uses, and deliberately
/// NOT the bare-clone-with-nothing-retained shape that froze `CellMap::
/// size()` (see the `count_fresh_cell_test.rs` regression this mirrors):
/// the guard's own strong reference back to its source is what keeps each
/// bucket's store subscription alive for as long as `result` is.
pub fn build_belongs_to_union_source_map(
    registry: Arc<StoreRegistry>,
    host_id: Uuid,
    local_type: &'static str,
    field_names: &'static [&'static str],
    extract_fk: CompoundFkExtractor,
    keys: Vec<CompoundKey>,
) -> FilteredCellMap {
    if keys.len() > UNION_KEYS_WARN_THRESHOLD {
        tracing::warn!(
            target: "myko::core::query::registration",
            "belongs_to union route for {local_type}[{}] is subscribing to {} buckets \
             (> {UNION_KEYS_WARN_THRESHOLD}) — likely a cartesian product of several \
             large `In` fields on the same query",
            field_names.join("+"),
            keys.len(),
        );
    }

    let index =
        belongs_to_source_index_for(&registry, host_id, local_type, field_names, extract_fk);
    let result: AnyItemMap = AnyItemMap::new();
    for key in keys {
        let bucket = index.bucket_for(key, extract_fk).lock();
        let result_weak = result.downgrade();
        let guard = bucket.subscribe_diffs(move |diff| {
            let Some(result) = result_weak.upgrade() else {
                return;
            };
            // Each source's `Initial` means "here is MY full current state"
            // — applying it to `result` as-is would REPLACE result's
            // contents, wiping out every other source's entries (an
            // Initial is a resync, not an incremental add). `result` is a
            // union of K independently-diffing sources, so a source's
            // Initial must be additive here: rewrite it into plain Inserts
            // before forwarding. Insert/Remove/Update/Batch are already
            // incremental relative to a single key, so they forward as-is.
            let additive = additive_union_diff(diff);
            if let Some(additive) = additive {
                result.apply_batch(vec![additive]);
            }
        });
        result.own(guard);
    }
    result.lock()
}

/// Cartesian product of N value sets — expands multi-field compound `In`
/// filters into the full set of (v1, v2, ..., vN) compound keys to
/// union-route through [`build_belongs_to_union_source_map`]. Empty if any
/// input set is empty: an empty value set for one field means no key any
/// item could satisfy exists, matching `In([])` matching nothing (spec §1).
pub fn cartesian_product(sets: Vec<Vec<Arc<str>>>) -> Vec<Vec<Arc<str>>> {
    sets.into_iter().fold(vec![Vec::new()], |acc, set| {
        acc.into_iter()
            .flat_map(|prefix| {
                set.iter().map(move |v| {
                    let mut next = prefix.clone();
                    next.push(v.clone());
                    next
                })
            })
            .collect()
    })
}

/// Sweep dead (no-longer-subscribed) buckets across every belongs-to
/// relation's source index. `route_to_live_bucket` reaps dead entries lazily
/// on next access, but a foreign id that goes dead and is never looked up
/// again would otherwise linger; called from
/// `CellServerCtx::sweep_dead_cache_entries`.
pub fn sweep_all_belongs_to_source_indexes() {
    for entry in belongs_to_source_indexes().iter() {
        entry.value().sweep_dead_buckets();
    }
}

/// Build a `FilteredCellMap` containing only the entries at the given ids,
/// using direct per-key store lookups instead of an O(N) `test_entity` scan.
///
/// Used by `Get<Entity>sByIds::build_view` so the initial query result is
/// constructed in O(M) where M = ids.len(). Per-key cells from the store
/// keep the result reactive to inserts / updates / deletes for those
/// specific ids; `test_entity` semantics still hold because the returned
/// map only ever contains keys from `ids`.
pub fn build_ids_source_map(
    store: &Arc<crate::store::EntityStore>,
    ids: &[Arc<str>],
) -> FilteredCellMap {
    use hyphae::{Signal, Watchable};

    let result: hyphae::CellMap<Arc<str>, AnyItemArc> = hyphae::CellMap::new();
    for id in ids {
        let key_cell = store.get(id);
        // Weak, not a strong clone: `key_cell` belongs to the store, which
        // lives for the whole process — a strong capture here would make the
        // store's per-key subscriber list hold `result` (and everything
        // built on top of it downstream) alive forever, regardless of
        // whether any external caller still references it. This is exactly
        // the reference cycle `query_cache`'s weak-ref design assumes never
        // happens (see `MapCacheEntry` in server/context.rs).
        let result_weak = result.downgrade();
        let key_for_cb = id.clone();
        let guard = key_cell.subscribe(move |signal| {
            let Some(result_for_cb) = result_weak.upgrade() else {
                return;
            };
            if let Signal::Value(arc_opt) = signal {
                match arc_opt.as_ref() {
                    Some(item) => {
                        result_for_cb.insert(key_for_cb.clone(), item.clone());
                    }
                    None => {
                        result_for_cb.remove(&key_for_cb);
                    }
                }
            }
        });
        result.own(guard);
    }
    result.lock()
}

pub fn filter_query_over_source<Q>(
    source: FilteredCellMap,
    query: Arc<Q>,
    query_context: Arc<QueryContext>,
) -> FilteredCellMap
where
    Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
    Q::Item:
        DeserializeOwned + Eventable + WithId + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    hyphae::MapQuery::materialize(source.select(move |item_any: &AnyItemArc| {
        let item = downcast_any_item_arc::<Q::Item>(item_any, "filter_query_over_source");
        Q::test_entity(QueryTestCtx {
            item,
            query: query.clone(),
            query_context: query_context.clone(),
        })
    }))
}

/// Registration entry for a query type.
/// Collected via inventory for automatic discovery.
pub struct QueryRegistration {
    /// Query identifier (e.g., "GetAllTargets")
    pub query_id: &'static str,
    /// Entity type this query returns (e.g., "Target")
    pub query_item_type: &'static str,
    /// Crate where this query is defined (for type_gen filtering)
    pub crate_name: &'static str,
    /// Parse function for deserializing query from JSON
    pub parse: QueryParseFn,
    /// Factory for creating reactive cell from query
    pub cell_factory: QueryCellFactory,
    /// Query struct's own fields, captured at macro-expansion time. Backs
    /// the MCP `search()` tool's operation index — see `crate::reflection`.
    pub args: &'static [crate::reflection::OperationArgField],
    /// Query struct's doc comment, if any.
    pub description: Option<&'static str>,
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryFactory - Static methods for query types
// ─────────────────────────────────────────────────────────────────────────────

/// Factory trait for creating query registration data.
///
/// This trait has a blanket implementation for all types implementing `QueryParams`,
/// so user-defined queries automatically get `parse` and `cell_factory` methods.
pub trait QueryFactory: QueryParams {
    /// Parse JSON into this query type.
    fn parse(value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;

    /// Create a reactive cell for this query.
    fn cell_factory(
        query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Option<Arc<CellServerCtx>>,
    ) -> Result<FilteredCellMap, String>;
}

impl<Q: QueryParams> QueryFactory for Q
where
    Q::Item:
        Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    fn parse(value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error> {
        let query = serde_json::from_value::<QueryRequest<Q>>(value)?;
        Ok(Arc::new(query))
    }

    fn cell_factory(
        any_query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Option<Arc<CellServerCtx>>,
    ) -> Result<FilteredCellMap, String> {
        QUERY_CELL_FACTORIES_CREATED.fetch_add(1, Ordering::Relaxed);
        let query_id = Q::query_id_static();
        // Bounded cardinality (one span per query *registration*, not per
        // `test_entity` item test — that runs reactively per store mutation
        // and would be far too hot to span), matching `myko.command`.
        let _span = tracing::trace_span!("myko.query", query = query_id.as_ref()).entered();
        crate::server::dispatch_metrics::record_query(query_id.as_ref(), request_ctx.origin());
        increment_counter(query_factories_by_id(), query_id);
        let any_ref: &dyn Any = any_query.as_ref();
        let request: QueryRequest<Q> = any_ref
            .downcast_ref::<QueryRequest<Q>>()
            .cloned()
            .ok_or_else(|| "Failed to downcast query payload".to_string())?;
        let query: Arc<Q> = Arc::new(request.query);

        let query_ctx = Arc::new(QueryContext {
            req: request_ctx.clone(),
        });
        let query_cell_ctx =
            QueryCellContext::new(request_ctx, query_ctx.clone(), registry.clone(), server_ctx);

        if let Some(built) = Q::build_view(QueryBuildCellCtx {
            query: query.clone(),
            query_context: query_cell_ctx.clone(),
        }) {
            return Ok(hyphae::MapQuery::materialize(built));
        }

        let store: crate::store::EntityStore =
            (*registry.get_or_create(&Q::query_item_type_static())).clone();
        Ok(hyphae::MapQuery::materialize(store.select(
            move |item_any: &AnyItemArc| {
                let item = downcast_any_item_arc::<Q::Item>(item_any, "QueryFactory::cell_factory");
                Q::test_entity(QueryTestCtx {
                    item,
                    query: query.clone(),
                    query_context: query_ctx.clone(),
                })
            },
        )))
    }
}

#[cfg(test)]
mod belongs_to_source_index_tests {
    use std::any::Any;

    use serde::Serialize;

    use super::*;
    use crate::common::with_id::WithId;

    #[derive(Debug, Clone, PartialEq, Serialize)]
    struct TestChild {
        id: Arc<str>,
        parent_id: Arc<str>,
    }

    impl WithId for TestChild {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl crate::core::item::AnyItem for TestChild {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "TestChild"
        }

        fn equals(&self, other: &dyn crate::core::item::AnyItem) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .map(|t| t == self)
                .unwrap_or(false)
        }
    }

    fn extract_parent_fk(item: &dyn Any) -> Option<Vec<Arc<str>>> {
        item.downcast_ref::<TestChild>()
            .map(|c| vec![c.parent_id.clone()])
    }

    fn child(id: &str, parent: &str) -> (Arc<str>, AnyItemArc) {
        (
            Arc::from(id),
            Arc::new(TestChild {
                id: Arc::from(id),
                parent_id: Arc::from(parent),
            }) as AnyItemArc,
        )
    }

    fn new_store() -> Arc<crate::store::EntityStore> {
        Arc::new(hyphae::CellMap::new())
    }

    #[test]
    fn dropped_subscriptions_do_not_leak_across_many_distinct_parents() {
        // Reproduces the leak: every distinct foreign id ever subscribed to
        // used to leave a permanent bucket behind. With weak-ref buckets,
        // dropping every subscriber and sweeping must bring the count to 0
        // regardless of how many distinct parents were ever seen.
        let store = new_store();
        let index = BelongsToSourceIndex::new(store, extract_parent_fk);

        for i in 0..50 {
            let parent: Arc<str> = Arc::from(format!("parent-{i}"));
            let bucket = index.bucket_for(vec![parent], extract_parent_fk);
            drop(bucket); // simulates every subscriber unsubscribing
        }

        index.sweep_dead_buckets();
        assert_eq!(
            index.buckets.len(),
            0,
            "sweep must reap all buckets once every subscriber has dropped"
        );
    }

    #[test]
    fn live_subscription_survives_going_empty_then_repopulating() {
        // The naive fix (remove a bucket the moment it goes empty) breaks
        // this: a still-live subscriber would get orphaned from a bucket
        // that later gets silently replaced. Weak-ref buckets avoid this —
        // as long as the subscriber holds their strong handle, `bucket_for`
        // keeps returning the *same* object.
        let store = new_store();
        let (id, item) = child("c1", "parent-x");
        store.insert(id.clone(), item);

        let index = BelongsToSourceIndex::new(store.clone(), extract_parent_fk);
        let bucket = index.bucket_for(vec![Arc::from("parent-x")], extract_parent_fk);
        assert_eq!(bucket.snapshot().len(), 1);

        // Remove the only child — bucket goes empty, but `bucket` is still
        // held here, simulating a live subscriber.
        store.remove(&id);
        assert_eq!(bucket.snapshot().len(), 0);

        // A new child arrives under the same parent — the still-held handle
        // must see it, not a disconnected/orphaned bucket.
        let (id2, item2) = child("c2", "parent-x");
        store.insert(id2, item2);
        assert_eq!(
            bucket.snapshot().len(),
            1,
            "a live subscriber must see re-population after its bucket went empty"
        );
    }

    #[test]
    fn resubscribing_after_reap_backfills_current_children() {
        // The bug a naive weak-ref swap alone would introduce: once a
        // bucket is reaped, apply_diff never creates buckets nobody's
        // watching, so a fresh subscription must explicitly backfill from
        // the current store state rather than starting empty.
        let store = new_store();
        let (id, item) = child("c1", "parent-y");
        store.insert(id, item);

        let index = BelongsToSourceIndex::new(store.clone(), extract_parent_fk);

        {
            let bucket = index.bucket_for(vec![Arc::from("parent-y")], extract_parent_fk);
            assert_eq!(bucket.snapshot().len(), 1);
        }
        index.sweep_dead_buckets();
        assert!(index.buckets.is_empty());

        let bucket = index.bucket_for(vec![Arc::from("parent-y")], extract_parent_fk);
        assert_eq!(
            bucket.snapshot().len(),
            1,
            "resubscribing after the bucket was reaped must backfill current children, not start empty"
        );
    }

    #[test]
    fn concurrent_bucket_for_calls_for_the_same_key_never_orphan_a_bucket() {
        // The layer-2 mechanism bright-eagle's investigation converged on:
        // bucket_for used to check-then-act (route_to_live_bucket, then a
        // separate unconditional insert) with no lock held across the gap.
        // N callers racing for the same key could each observe "no live
        // bucket," each build and backfill their own AnyItemMap, then race
        // an unconditional `insert` — whichever landed last won
        // `self.buckets`, silently orphaning every other caller's bucket:
        // still a valid handle, already returned and subscribed to, but
        // unreachable from apply_diff's routing from then on. That's
        // exactly "first/arbitrary winner, everyone else starves forever,
        // outcome varies by scheduling" — matches rship-qtu's calm-time,
        // registration-order-dependent symptom. entry()-based bucket_for
        // holds the shard lock across the whole check-or-create, so this
        // must converge on exactly one live bucket no matter how many
        // callers race.
        let store = new_store();
        let index = Arc::new(BelongsToSourceIndex::new(store.clone(), extract_parent_fk));
        let key: CompoundKey = vec![Arc::from("parent-race")];

        const N: usize = 16;
        let barrier = Arc::new(std::sync::Barrier::new(N));
        let handles: Vec<_> = (0..N)
            .map(|_| {
                let index = index.clone();
                let key = key.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    index.bucket_for(key, extract_parent_fk)
                })
            })
            .collect();
        let buckets: Vec<AnyItemMap> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        assert_eq!(
            index.buckets.len(),
            1,
            "N concurrent creators for the same key must converge on exactly one bucket entry"
        );

        // A new matching item must be visible through EVERY returned
        // handle — an orphaned loser of the old insert race would have
        // received its one-time (empty) backfill and nothing since.
        let (id, item) = child("c-race", "parent-race");
        store.insert(id, item);
        for (i, bucket) in buckets.iter().enumerate() {
            assert_eq!(
                bucket.snapshot().len(),
                1,
                "handle {i} must observe the post-race insert — an orphaned bucket stays empty forever"
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // Compound (multi-field) routing — the layer-1 fix for rship-qtu:
    // an entity with 2+ belongs_to fields, queried with more than one
    // set at once, must NOT collapse different combinations onto one
    // shared bucket.
    // ─────────────────────────────────────────────────────────────────

    #[derive(Debug, Clone, PartialEq, Serialize)]
    struct TestCursor {
        id: Arc<str>,
        node_id: Arc<str>,
        session_id: Arc<str>,
    }

    impl WithId for TestCursor {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl crate::core::item::AnyItem for TestCursor {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "TestCursor"
        }

        fn equals(&self, other: &dyn crate::core::item::AnyItem) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .map(|t| t == self)
                .unwrap_or(false)
        }
    }

    fn cursor(id: &str, node: &str, session: &str) -> (Arc<str>, AnyItemArc) {
        (
            Arc::from(id),
            Arc::new(TestCursor {
                id: Arc::from(id),
                node_id: Arc::from(node),
                session_id: Arc::from(session),
            }) as AnyItemArc,
        )
    }

    // Mirrors the macro-generated compound extractor for a query that pins
    // BOTH belongs_to fields: position 0 = node_id, position 1 = session_id.
    fn extract_node_and_session_fk(item: &dyn Any) -> Option<Vec<Arc<str>>> {
        item.downcast_ref::<TestCursor>()
            .map(|c| vec![c.node_id.clone(), c.session_id.clone()])
    }

    #[test]
    fn compound_key_separates_watchers_sharing_one_field_but_not_the_other() {
        // Two cursors in the SAME session but for DIFFERENT nodes. Before
        // compound routing, both watchers would collapse onto one
        // session-keyed bucket (single-field routing on whichever field
        // came first in the struct). With compound (node_id, session_id)
        // keys, each watcher gets its own bucket, scoped to exactly its
        // node+session pair.
        let store = new_store();
        let (id_a, item_a) = cursor("cursor-a", "node-A", "session-PROD");
        let (id_b, item_b) = cursor("cursor-b", "node-B", "session-PROD");
        store.insert(id_a.clone(), item_a);
        store.insert(id_b.clone(), item_b);

        let index = BelongsToSourceIndex::new(store.clone(), extract_node_and_session_fk);

        let key_a: CompoundKey = vec![Arc::from("node-A"), Arc::from("session-PROD")];
        let key_b: CompoundKey = vec![Arc::from("node-B"), Arc::from("session-PROD")];

        let bucket_a = index.bucket_for(key_a.clone(), extract_node_and_session_fk);
        let bucket_b = index.bucket_for(key_b.clone(), extract_node_and_session_fk);

        assert_eq!(
            bucket_a.snapshot().len(),
            1,
            "node-A's bucket sees only its own cursor"
        );
        assert_eq!(
            bucket_b.snapshot().len(),
            1,
            "node-B's bucket sees only its own cursor"
        );
        assert_eq!(
            index.buckets.len(),
            2,
            "distinct (node, session) pairs get distinct buckets"
        );

        // Live diffs must reach BOTH watchers independently — this is the
        // exact symptom rship-qtu reported: one watcher (bucket_a) must not
        // starve the other (bucket_b) of updates once both are subscribed.
        let (id_a2, item_a2) = cursor("cursor-a-tick2", "node-A", "session-PROD");
        store.insert(id_a2, item_a2);
        assert_eq!(
            bucket_a.snapshot().len(),
            2,
            "node-A's bucket must see its own new entry"
        );
        assert_eq!(
            bucket_b.snapshot().len(),
            1,
            "node-B's bucket must be unaffected by node-A's insert"
        );

        let (id_b2, item_b2) = cursor("cursor-b-tick2", "node-B", "session-PROD");
        store.insert(id_b2, item_b2);
        assert_eq!(
            bucket_b.snapshot().len(),
            2,
            "node-B's bucket must independently see its own new entry — this is the \
             regression rship-qtu hit: the second-registered watcher never received \
             a diff under single-field session-only routing"
        );
    }

    #[test]
    fn compound_and_single_field_routing_never_share_a_bucket() {
        // A query pinning only node_id (single-field key) and a query
        // pinning (node_id, session_id) (compound key) for the same node
        // must land in different BelongsToSourceIndex instances entirely —
        // build_belongs_to_source_map keys the outer index registry by the
        // field-name SET, not just the entity type, so this is exercised at
        // that layer instead of here (single BelongsToSourceIndex is always
        // scoped to one fixed field combination by construction — a
        // `bucket_for` call with a 1-element key and one with a 2-element
        // key against the SAME index would be a caller bug, not a
        // supported mixed-arity usage).
        let store = new_store();
        let index = BelongsToSourceIndex::new(store, extract_node_and_session_fk);
        let key: CompoundKey = vec![Arc::from("node-A"), Arc::from("session-PROD")];
        let bucket = index.bucket_for(key.clone(), extract_node_and_session_fk);
        assert_eq!(bucket.snapshot().len(), 0);
        assert!(index.buckets.contains_key(&key));
    }

    // ─────────────────────────────────────────────────────────────────
    // K-bucket union routing — what build_view routes an `In` (or
    // multi-field compound `In`) filter on a #[belongs_to] field through,
    // instead of a table scan (spec §4 hard requirement).
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn cartesian_product_expands_multi_field_value_sets() {
        let sets = vec![
            vec![Arc::from("node-A"), Arc::from("node-B")],
            vec![Arc::from("session-PROD")],
        ];
        let mut product = cartesian_product(sets);
        product.sort();
        assert_eq!(
            product,
            vec![
                vec![Arc::<str>::from("node-A"), Arc::from("session-PROD")],
                vec![Arc::<str>::from("node-B"), Arc::from("session-PROD")],
            ]
        );
    }

    #[test]
    fn cartesian_product_empty_set_yields_no_keys() {
        // In([]) on any one field means no key any item could satisfy
        // exists — matches "In([]) matches nothing" (spec §1).
        let sets = vec![vec![Arc::from("node-A")], vec![]];
        assert!(cartesian_product(sets).is_empty());
    }

    #[test]
    fn union_source_map_unions_k_buckets_and_stays_reactive() {
        // The mechanism build_view routes an `In` filter through: N
        // compound keys, each backed by its own BelongsToSourceIndex
        // bucket, unioned into one reactive result. Proves both the
        // partition-merge (every item appears exactly once, from whichever
        // bucket it actually belongs to) and that the union keeps tracking
        // writes made after construction — the keepalive pattern
        // deliberately mirrors typed_map_from_any_item_with_typed_id's
        // own(guard), not the bare-clone shape that froze hyphae's
        // CellMap::size() (see count_fresh_cell_test.rs).
        let registry = Arc::new(crate::store::StoreRegistry::new());
        let store = registry.get_or_create("TestCursor");
        let (id_a, item_a) = cursor("cursor-a", "node-A", "session-PROD");
        let (id_b, item_b) = cursor("cursor-b", "node-B", "session-PROD");
        let (id_c, item_c) = cursor("cursor-c", "node-C", "session-PROD"); // not in the union
        store.insert(id_a, item_a);
        store.insert(id_b, item_b);
        store.insert(id_c, item_c);

        let host_id = Uuid::new_v4();
        let keys = vec![
            vec![Arc::from("node-A"), Arc::from("session-PROD")],
            vec![Arc::from("node-B"), Arc::from("session-PROD")],
        ];
        let union = build_belongs_to_union_source_map(
            registry.clone(),
            host_id,
            "TestCursor",
            &["node_id", "session_id"],
            extract_node_and_session_fk,
            keys,
        );

        assert_eq!(
            union.snapshot().len(),
            2,
            "union must contain exactly node-A's and node-B's cursors, not node-C's"
        );

        let (id_a2, item_a2) = cursor("cursor-a2", "node-A", "session-PROD");
        store.insert(id_a2, item_a2);
        assert_eq!(
            union.snapshot().len(),
            3,
            "union must keep tracking writes to any of its unioned buckets"
        );

        let (id_c2, item_c2) = cursor("cursor-c2", "node-C", "session-PROD");
        store.insert(id_c2, item_c2);
        assert_eq!(
            union.snapshot().len(),
            3,
            "writes to a non-unioned bucket must never appear in the union"
        );
    }

    #[test]
    fn union_source_map_empty_keys_yields_empty_reactive_map() {
        let registry = Arc::new(crate::store::StoreRegistry::new());
        let store = registry.get_or_create("TestCursor");
        let (id, item) = cursor("cursor-a", "node-A", "session-PROD");
        store.insert(id, item);

        let union = build_belongs_to_union_source_map(
            registry,
            Uuid::new_v4(),
            "TestCursor",
            &["node_id", "session_id"],
            extract_node_and_session_fk,
            Vec::new(),
        );
        assert_eq!(union.snapshot().len(), 0);
    }
}
