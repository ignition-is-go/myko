//! Query registration via inventory.

use std::{
    any::Any,
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use hyphae::{MapDiff, SelectExt, Signal, SubscriptionGuard, Watchable};
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use super::{
    super::item::Eventable,
    cell::FilteredCellMap,
    context::{QueryBuildContext, QueryContext},
    filter::{
        BelongsToRoute, CompoundFkExtractor, CompoundKey, ID_ROUTE_FIELD_NAMES, LiveFilterQuery,
        QueryRoute,
    },
    request::QueryRequest,
    traits::{
        AnyQuery, QueryBuildArgs, QueryHandler, QueryParams, QueryTestContext, QueryWindowBuildArgs,
    },
};
use crate::{
    common::with_id::WithId, core::item::downcast_any_item_arc, request::RequestContext,
    server::MykoServerContext, store::StoreRegistry,
};

// ─────────────────────────────────────────────────────────────────────────────
// Type aliases for function pointers
// ─────────────────────────────────────────────────────────────────────────────

/// Type alias for query parse function.
pub type QueryParseFn = fn(Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;

#[cfg(not(target_arch = "wasm32"))]
pub type QueryAuthorityFactory =
    fn(Value, myko_federation::NodeId) -> Result<crate::server::HandlerAuthority, String>;

/// Type-erased cell factory for queries.
/// Takes a typed query, registry, and `host_id`, returns a `FilteredCellMap`.
pub type QueryCellFactory = fn(
    Arc<dyn AnyQuery>,
    Arc<StoreRegistry>,
    Arc<RequestContext>,
    Option<Arc<MykoServerContext>>,
    Option<crate::server::federated_source::FederatedRequest>,
) -> Result<FilteredCellMap, String>;

/// Type-erased factory for a query that can push a requested window into its
/// backing source instead of materializing the complete result map.
pub type QueryWindowCellFactory = fn(
    Arc<dyn AnyQuery>,
    Arc<StoreRegistry>,
    Arc<RequestContext>,
    Arc<MykoServerContext>,
    crate::wire::QueryWindow,
) -> Result<Option<super::WindowedQuerySource>, String>;

type AnyItemArc = Arc<dyn crate::core::item::AnyItem>;
type AnyItemMap = hyphae::CellMap<Arc<str>, AnyItemArc>;
type WeakAnyItemMap = hyphae::WeakCellMap<Arc<str>, AnyItemArc>;
type BucketEntries = Vec<(Arc<str>, AnyItemArc)>;
type BucketDiff = MapDiff<Arc<str>, AnyItemArc>;
type BucketDiffs = Vec<BucketDiff>;
type BucketAction = (AnyItemMap, BucketDiff);

#[derive(Default)]
struct BelongsToMutationState {
    pending: VecDeque<Vec<BucketAction>>,
    dispatching: bool,
}

/// Lazily-built per-key source constructor for whichever indexed mode a
/// `reconcile_live_query` tick is in (id route or `belongs_to` route).
type BucketSourceFn = Box<dyn Fn(&CompoundKey) -> FilteredCellMap>;

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
static BELONGS_TO_SOURCE_INDEXES: OnceLock<DashMap<String, Weak<BelongsToSourceIndex>>> =
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

fn belongs_to_source_indexes() -> &'static DashMap<String, Weak<BelongsToSourceIndex>> {
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

#[must_use]
pub fn query_runtime_metrics_by_id(limit: usize) -> Vec<QueryRuntimePerIdMetrics> {
    let mut rows: Vec<QueryRuntimePerIdMetrics> = query_factories_by_id()
        .iter()
        .map(|entry| {
            let query_id = entry.key().clone();
            let cell_factories_created = *entry.value();
            let per_item_guards_created = query_guards_created_by_id()
                .get(&query_id)
                .map_or(0, |v| *v.value());
            let per_item_guards_removed = query_guards_removed_by_id()
                .get(&query_id)
                .map_or(0, |v| *v.value());
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
/// `belongs_to` fields (see `build_belongs_to_source_map`'s registry key,
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
    mutation_gate: Mutex<BelongsToMutationState>,
    driver: Arc<AnyItemMap>,
}

impl BelongsToSourceIndex {
    fn new(store: Arc<crate::store::EntityStore>, extract_fk: CompoundFkExtractor) -> Arc<Self> {
        let driver = Arc::new(AnyItemMap::new());
        let index = Arc::new(Self {
            store,
            buckets: DashMap::new(),
            mutation_gate: Mutex::new(BelongsToMutationState::default()),
            driver: driver.clone(),
        });

        let index_for_diffs = Arc::downgrade(&index);
        let guard = index.store.subscribe_diffs(move |diff| {
            if let Some(index) = index_for_diffs.upgrade() {
                index.apply_diff(diff, extract_fk);
            }
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
    fn bucket_for(
        self: &Arc<Self>,
        key: CompoundKey,
        extract_fk: CompoundFkExtractor,
    ) -> AnyItemMap {
        // Store callbacks take the same gate. This makes the snapshot and
        // bucket publication one logical operation relative to every diff:
        // a concurrent write is either present in the backfill or routed to
        // the newly published bucket after this critical section.
        let _mutation = self
            .mutation_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.buckets.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                if let Some(map) = occupied.get().upgrade() {
                    return map;
                }
                let map = Self::build_backfilled_bucket(&self.store, occupied.key(), extract_fk);
                self.retain_for_bucket(&map);
                occupied.insert(map.downgrade());
                map
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                let map = Self::build_backfilled_bucket(&self.store, vacant.key(), extract_fk);
                self.retain_for_bucket(&map);
                vacant.insert(map.downgrade());
                map
            }
        }
    }

    /// Keep the routing subscription alive exactly as long as a bucket is
    /// live. The global registry is intentionally weak, and the index holds
    /// only weak bucket handles, so this bucket -> index edge cannot form a
    /// cycle but still prevents active subscribers from losing their driver.
    fn retain_for_bucket(self: &Arc<Self>, map: &AnyItemMap) {
        let index = self.clone();
        let guard = self.driver.subscribe_diffs(move |_| {
            let _ = &index;
        });
        map.own_guard(guard);
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
            map.apply_diff_owned(MapDiff::Initial { entries: backfill });
        }
        map
    }

    /// Drop bucket entries nobody's subscribed to anymore. `route_to_live_bucket`
    /// already reaps dead entries lazily on next access, but a key that goes
    /// dead and is never looked up again would otherwise sit in
    /// `self.buckets` forever (just a `Weak` + a small `Vec<Arc<str>>` key,
    /// far smaller than the leak this replaces, but still unbounded over
    /// time). Called from `MykoServerContext::sweep_dead_cache_entries` via
    /// [`sweep_all_belongs_to_source_indexes`].
    fn sweep_dead_buckets(&self) {
        self.buckets.retain(|_, weak| weak.upgrade().is_some());
    }

    fn apply_diff(&self, diff: &BucketDiff, extract_fk: CompoundFkExtractor) {
        {
            let mut mutation = self
                .mutation_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let actions = self.prepare_diff_locked(diff, extract_fk);
            mutation.pending.push_back(actions);
            if mutation.dispatching {
                return;
            }
            mutation.dispatching = true;
        }

        // CellMap fanout is synchronous and may construct another query,
        // which re-enters bucket_for on this index. Never notify while the
        // non-reentrant publication gate is held. A single drainer preserves
        // store-diff order even when callbacks arrive concurrently; reentrant
        // diffs enqueue above and are picked up by this loop.
        loop {
            let actions = {
                let mut mutation = self
                    .mutation_gate
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(actions) = mutation.pending.pop_front() {
                    drop(mutation);
                    actions
                } else {
                    mutation.dispatching = false;
                    drop(mutation);
                    return;
                }
            };
            for (bucket, diff) in actions {
                bucket.apply_diff_owned(diff);
            }
        }
    }

    fn prepare_diff_locked(
        &self,
        diff: &BucketDiff,
        extract_fk: CompoundFkExtractor,
    ) -> Vec<BucketAction> {
        match diff {
            MapDiff::Initial { entries } => self.prepare_initial(entries, extract_fk),
            MapDiff::Insert { key, value } => extract_fk(value.as_any())
                .and_then(|fk| {
                    self.route_action(
                        &fk,
                        MapDiff::Insert {
                            key: key.clone(),
                            value: value.clone(),
                        },
                    )
                })
                .into_iter()
                .collect(),
            MapDiff::Remove { key, old_value } => extract_fk(old_value.as_any())
                .and_then(|fk| {
                    self.route_action(
                        &fk,
                        MapDiff::Remove {
                            key: key.clone(),
                            old_value: old_value.clone(),
                        },
                    )
                })
                .into_iter()
                .collect(),
            MapDiff::Update {
                key,
                old_value,
                new_value,
            } => self.prepare_update(key, old_value, new_value, extract_fk),
            MapDiff::Batch { changes } => self.prepare_batch(changes, extract_fk),
        }
    }

    fn route_action(&self, foreign_key: &CompoundKey, diff: BucketDiff) -> Option<BucketAction> {
        self.route_to_live_bucket(foreign_key)
            .map(|bucket| (bucket, diff))
    }

    fn prepare_initial(
        &self,
        entries: &BucketEntries,
        extract_fk: CompoundFkExtractor,
    ) -> Vec<BucketAction> {
        let mut grouped: HashMap<CompoundKey, BucketEntries> = HashMap::new();
        for (id, item) in entries {
            if let Some(fk) = extract_fk(item.as_any()) {
                grouped
                    .entry(fk)
                    .or_default()
                    .push((id.clone(), item.clone()));
            }
        }

        self.buckets
            .iter()
            .filter(|entry| entry.value().upgrade().is_some())
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>()
            .into_iter()
            .filter_map(|key| {
                self.route_action(
                    &key,
                    MapDiff::Initial {
                        entries: grouped.remove(&key).unwrap_or_default(),
                    },
                )
            })
            .collect()
    }

    fn prepare_update(
        &self,
        key: &Arc<str>,
        old_value: &AnyItemArc,
        new_value: &AnyItemArc,
        extract_fk: CompoundFkExtractor,
    ) -> Vec<BucketAction> {
        let old_fk = extract_fk(old_value.as_any());
        let new_fk = extract_fk(new_value.as_any());
        let mut actions = Vec::new();
        match (old_fk, new_fk) {
            (Some(old_fk), Some(new_fk)) if old_fk == new_fk => {
                actions.extend(self.route_action(
                    &new_fk,
                    MapDiff::Update {
                        key: key.clone(),
                        old_value: old_value.clone(),
                        new_value: new_value.clone(),
                    },
                ));
            }
            (old_fk, new_fk) => {
                if let Some(old_fk) = old_fk {
                    actions.extend(self.route_action(
                        &old_fk,
                        MapDiff::Remove {
                            key: key.clone(),
                            old_value: old_value.clone(),
                        },
                    ));
                }
                if let Some(new_fk) = new_fk {
                    actions.extend(self.route_action(
                        &new_fk,
                        MapDiff::Insert {
                            key: key.clone(),
                            value: new_value.clone(),
                        },
                    ));
                }
            }
        }
        actions
    }

    fn group_change(
        by_fk: &mut HashMap<CompoundKey, BucketDiffs>,
        change: &BucketDiff,
        extract_fk: CompoundFkExtractor,
    ) {
        let mut push = |foreign_key: Option<CompoundKey>, diff: BucketDiff| {
            if let Some(foreign_key) = foreign_key {
                by_fk.entry(foreign_key).or_default().push(diff);
            }
        };
        match change {
            MapDiff::Insert { key, value } => push(
                extract_fk(value.as_any()),
                MapDiff::Insert {
                    key: key.clone(),
                    value: value.clone(),
                },
            ),
            MapDiff::Remove { key, old_value } => push(
                extract_fk(old_value.as_any()),
                MapDiff::Remove {
                    key: key.clone(),
                    old_value: old_value.clone(),
                },
            ),
            MapDiff::Update {
                key,
                old_value,
                new_value,
            } => {
                let old_fk = extract_fk(old_value.as_any());
                let new_fk = extract_fk(new_value.as_any());
                if old_fk == new_fk {
                    push(
                        new_fk,
                        MapDiff::Update {
                            key: key.clone(),
                            old_value: old_value.clone(),
                            new_value: new_value.clone(),
                        },
                    );
                } else {
                    push(
                        old_fk,
                        MapDiff::Remove {
                            key: key.clone(),
                            old_value: old_value.clone(),
                        },
                    );
                    push(
                        new_fk,
                        MapDiff::Insert {
                            key: key.clone(),
                            value: new_value.clone(),
                        },
                    );
                }
            }
            MapDiff::Initial { .. } | MapDiff::Batch { .. } => {}
        }
    }

    fn prepare_batch(
        &self,
        changes: &BucketDiffs,
        extract_fk: CompoundFkExtractor,
    ) -> Vec<BucketAction> {
        let mut actions = Vec::new();
        let mut by_fk: HashMap<CompoundKey, BucketDiffs> = HashMap::new();
        for change in changes {
            if matches!(change, MapDiff::Initial { .. } | MapDiff::Batch { .. }) {
                actions.extend(self.prepare_diff_locked(change, extract_fk));
            } else {
                Self::group_change(&mut by_fk, change, extract_fk);
            }
        }
        actions.extend(by_fk.into_iter().filter_map(|(foreign_key, changes)| {
            self.route_action(&foreign_key, MapDiff::Batch { changes })
        }));
        actions
    }
}

/// Build a reactive, `#[belongs_to]`-routed source map for a query that has
///
/// one or more `belongs_to` fields set. `field_names` and `foreign_ids` are
/// positionally paired (same order `extract_fk` reads them in) and must be
/// the SAME LENGTH — exactly the fields the caller's query populated, not
/// necessarily every `belongs_to` field the entity declares. Two calls for the
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
    foreign_ids: CompoundKey,
) -> FilteredCellMap {
    debug_assert_eq!(
        field_names.len(),
        foreign_ids.len(),
        "build_belongs_to_source_map: field_names and foreign_ids must be positionally paired"
    );
    let index =
        belongs_to_source_index_for(&registry, host_id, local_type, field_names, extract_fk);
    drop(registry);
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
    match belongs_to_source_indexes().entry(key) {
        dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
            if let Some(index) = occupied.get().upgrade() {
                return index;
            }
            let store = registry.get_or_create(local_type);
            let index = BelongsToSourceIndex::new(store, extract_fk);
            occupied.insert(Arc::downgrade(&index));
            index
        }
        dashmap::mapref::entry::Entry::Vacant(vacant) => {
            let store = registry.get_or_create(local_type);
            let index = BelongsToSourceIndex::new(store, extract_fk);
            vacant.insert(Arc::downgrade(&index));
            index
        }
    }
}

/// Above this many union keys, `build_belongs_to_union_source_map` logs a
///
/// warning instead of silently subscribing to a huge number of buckets — a
/// caller combining several large `In` fields on the same query hits a
/// cartesian-product blow-up (spec §4: "either cap the product size or
/// document the blow-up and log when it exceeds a threshold"). No hard cap:
/// per the spec's hard requirement, an indexed field must never fall back
/// to a table scan, so this only observes, never degrades.
pub const UNION_KEYS_WARN_THRESHOLD: usize = 1000;

/// Rewrite one bucket's diffs into incremental changes safe to apply to the
/// union of several buckets. An `Initial` is a replacement of this bucket's
/// contribution, so the previous contribution must be retracted before the
/// new snapshot is inserted. Treating it as inserts alone loses an
/// `Initial { entries: [] }` clear when the last source row is deleted.
fn additive_union_diff(
    diff: &BucketDiff,
    contribution: &mut HashMap<Arc<str>, AnyItemArc>,
) -> Option<BucketDiff> {
    match diff {
        MapDiff::Initial { entries } => {
            let mut changes = Vec::with_capacity(contribution.len().saturating_add(entries.len()));
            for (key, old_value) in contribution.drain() {
                changes.push(MapDiff::Remove { key, old_value });
            }
            for (key, value) in entries {
                contribution.insert(key.clone(), value.clone());
                changes.push(MapDiff::Insert {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
            (!changes.is_empty()).then_some(MapDiff::Batch { changes })
        }
        MapDiff::Batch { changes } => {
            let rewritten: Vec<BucketDiff> = changes
                .iter()
                .filter_map(|change| additive_union_diff(change, contribution))
                .collect();
            if rewritten.is_empty() {
                None
            } else {
                Some(MapDiff::Batch { changes: rewritten })
            }
        }
        MapDiff::Insert { key, value } => {
            contribution.insert(key.clone(), value.clone());
            Some(diff.clone())
        }
        MapDiff::Remove { key, .. } => {
            contribution.remove(key);
            Some(diff.clone())
        }
        MapDiff::Update { key, new_value, .. } => {
            contribution.insert(key.clone(), new_value.clone());
            Some(diff.clone())
        }
    }
}

/// Union of K `belongs_to` buckets — routes an `In` (or multi-field compound
///
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
        let contribution = Mutex::new(HashMap::new());
        let guard = bucket.subscribe_diffs(move |diff| {
            let Some(result) = result_weak.upgrade() else {
                return;
            };
            // Each source's `Initial` replaces only that bucket's current
            // contribution. Applying it directly would wipe the other
            // buckets, while treating it as inserts would fail to retract
            // rows absent from the replacement. Track this bucket's prior
            // contribution and rewrite the replacement into removals plus
            // inserts before forwarding it to the shared union.
            let additive = additive_union_diff(
                diff,
                &mut contribution
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            if let Some(additive) = additive {
                result.apply_diff_owned(additive);
            }
        });
        result.own(guard);
    }
    drop(registry);
    result.lock()
}

/// Cartesian product of N value sets — expands multi-field compound `In`
///
/// filters into the full set of (v1, v2, ..., vN) compound keys to
/// union-route through [`build_belongs_to_union_source_map`]. Empty if any
/// input set is empty: an empty value set for one field means no key any
/// item could satisfy exists, matching `In([])` matching nothing (spec §1).
#[must_use]
pub fn cartesian_product(sets: Vec<Vec<Arc<str>>>) -> Vec<CompoundKey> {
    sets.into_iter().fold(vec![CompoundKey::new()], |acc, set| {
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

// ─────────────────────────────────────────────────────────────────────────
// query_live — phase 2 of the advanced-query-design spec (§5): a reactive
// filter *parameter* instead of a plain value, so a filter derived from
// other cells no longer needs a switch_map wrapper. Server-side only.
// ─────────────────────────────────────────────────────────────────────────

fn live_filter_matches<F: LiveFilterQuery>(filter: &F, item: &AnyItemArc) -> bool {
    downcast_any_item_arc::<F::Item>(item, "query_live").is_some_and(|typed| filter.matches(&typed))
}

/// Re-test `candidates` — ONE bucket's `snapshot()`, nothing more — against
/// `filter`, inserting newly-matching items and retracting no-longer-
/// matching ones. Deliberately does NOT touch any id outside `candidates`:
/// `result` is a union of potentially several buckets (the partition-merge
/// invariant means an id can only ever come from one bucket for a given
/// field combination), so removing "anything not in this bucket" would
/// wrongly retract items OTHER buckets contributed. Safe for both a
/// freshly-added bucket's initial population (nothing to retract, every
/// candidate either inserts or no-ops) and an unchanged bucket rescanned
/// because a non-indexed field changed (retracts exactly this bucket's own
/// now-stale contributions).
fn apply_bucket_candidates<F: LiveFilterQuery>(
    result: &AnyItemMap,
    filter: &F,
    candidates: Vec<(Arc<str>, AnyItemArc)>,
) {
    for (id, item) in candidates {
        if live_filter_matches(filter, &item) {
            result.insert(id, item);
        } else {
            result.remove(&id);
        }
    }
}

/// Diff `candidates` — the WHOLE authoritative scope of `result` (the
/// entire store, in scan mode) — against `result`'s current membership
/// under `filter`: insert newly-matching items, remove no-longer-matching
/// ones, AND remove anything in `result` that isn't in `candidates` at all
/// (covers deletes — an item removed from the store entirely is simply
/// absent from a fresh `snapshot()`, not present-but-non-matching). Unlike
/// [`apply_bucket_candidates`], this only makes sense when `candidates` is
/// the FULL scope `result` is meant to reflect, never a single bucket's
/// contents alongside other buckets' contributions.
fn reconcile_full_scope_membership<F: LiveFilterQuery>(
    result: &AnyItemMap,
    filter: &F,
    candidates: Vec<(Arc<str>, AnyItemArc)>,
) {
    // for_each: read-only visit — no snapshot Vec cloned just to build the
    // id set. All mutation of `result` happens after this returns.
    let mut current_ids: HashSet<Arc<str>> = HashSet::new();
    result.for_each(|id, _| {
        current_ids.insert(id.clone());
    });
    let mut seen: HashSet<Arc<str>> = HashSet::with_capacity(candidates.len());
    for (id, item) in candidates {
        seen.insert(id.clone());
        let should_have = live_filter_matches(filter, &item);
        let currently_has = current_ids.contains(&id);
        match (should_have, currently_has) {
            (true, false) => {
                result.insert(id, item);
            }
            (false, true) => {
                result.remove(&id);
            }
            _ => {}
        }
    }
    for id in current_ids.difference(&seen) {
        result.remove(id);
    }
}

/// Whether an [`apply_live_diff`] callback is watching one compound-key
/// bucket (only ever affects the ids that bucket itself owns) or the whole
/// store in scan mode (its `Initial` represents `result`'s entire scope, so
/// deletes must retract ids absent from it too — see
/// [`reconcile_full_scope_membership`] vs [`apply_bucket_candidates`]).
#[derive(Clone, Copy)]
enum LiveDiffScope {
    Bucket,
    FullStore,
}

/// Apply one incoming diff from a live-tracked source (a bucket, or the
/// whole store in scan mode) to `result`, re-testing every affected item
/// against `filter` — always read fresh at the moment the diff arrives
/// (never a value captured at subscribe time), since a bucket/store
/// subscription outlives many filter ticks.
fn apply_live_diff<F: LiveFilterQuery>(
    result: &AnyItemMap,
    diff: &BucketDiff,
    filter: &F,
    scope: LiveDiffScope,
) {
    match diff {
        MapDiff::Initial { entries } => match scope {
            LiveDiffScope::Bucket => apply_bucket_candidates(result, filter, entries.clone()),
            LiveDiffScope::FullStore => {
                reconcile_full_scope_membership(result, filter, entries.clone());
            }
        },
        MapDiff::Insert { key, value } => {
            if live_filter_matches(filter, value) {
                result.insert(key.clone(), value.clone());
            }
        }
        MapDiff::Remove { key, .. } => {
            result.remove(key);
        }
        MapDiff::Update { key, new_value, .. } => {
            if live_filter_matches(filter, new_value) {
                result.insert(key.clone(), new_value.clone());
            } else {
                result.remove(key);
            }
        }
        MapDiff::Batch { changes } => {
            for change in changes {
                apply_live_diff(result, change, filter, scope);
            }
        }
    }
}

/// Per-`query_live`-call state: which compound-key buckets are currently
/// subscribed (with a handle to each bucket for `snapshot()`-based
/// retraction/rescan), the scan-mode store subscription if active, and the
/// filter value from the previous tick (to detect route-shape changes).
struct LiveQueryState<F> {
    bucket_guards: HashMap<CompoundKey, (FilteredCellMap, SubscriptionGuard)>,
    // Shared with each bucket's subscribe_diffs callback so it always
    // reads the LATEST filter (never one captured at subscribe time) —
    // a bucket subscription outlives many filter ticks.
    bucket_filter_refs: HashMap<CompoundKey, Arc<Mutex<LiveFilterGeneration<F>>>>,
    scan_guard: Option<SubscriptionGuard>,
    scan_filter_ref: Option<Arc<Mutex<LiveFilterGeneration<F>>>>,
    prev_route_field_names: Option<&'static [&'static str]>,
}

struct LiveFilterGeneration<F> {
    generation: u64,
    filter: F,
}

#[derive(Default)]
struct LiveQuerySynchronization {
    generation: AtomicU64,
    reconciliation_gate: Mutex<()>,
}

impl<F> Default for LiveQueryState<F> {
    fn default() -> Self {
        Self {
            bucket_guards: HashMap::new(),
            bucket_filter_refs: HashMap::new(),
            scan_guard: None,
            scan_filter_ref: None,
            prev_route_field_names: None,
        }
    }
}

/// Reactive filter parameters: `filter_cell` replaces the value-based
///
/// `GetXsByQuery` query with a live `Cell`, so a filter whose value is
/// itself derived reactively no longer needs a `switch_map` wrapper. The
/// returned map is a single persistent reactive graph node —
/// filter changes are handled by incrementally adding/removing bucket
/// subscriptions (for `In`/`Eq` changes on indexed `#[belongs_to]` fields)
/// or, for non-indexed (`Range`/`Contains`) changes, rescanning the
/// currently-scoped item set — never by tearing down and rebuilding
/// `result` itself. Downstream cells built on the returned map keep their
/// subscription across every filter change.
///
/// Server-side only (a `Cell` can't cross the wire); no cache — a cell is
/// object identity, so callers get an independent graph node per call site
/// (see spec §5's "no value-identity cache sharing").
pub fn query_live<F>(
    registry: Arc<StoreRegistry>,
    host_id: Uuid,
    filter_cell: impl Watchable<F>,
) -> FilteredCellMap
where
    F: LiveFilterQuery,
{
    let result: AnyItemMap = AnyItemMap::new();
    let state: Arc<Mutex<LiveQueryState<F>>> = Arc::new(Mutex::new(LiveQueryState::default()));
    let synchronization = Arc::new(LiveQuerySynchronization::default());

    let result_weak = result.downgrade();
    let guard = filter_cell.subscribe(move |signal| {
        let Signal::Value(new_filter) = signal else {
            return;
        };
        let Some(result) = result_weak.upgrade() else {
            return;
        };
        let _reconciliation = synchronization
            .reconciliation_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current_generation = synchronization
            .generation
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        reconcile_live_query(
            &registry,
            host_id,
            &result,
            &mut state,
            new_filter.as_ref(),
            current_generation,
            &synchronization,
        );
    });
    drop(filter_cell);
    result.own(guard);
    result.lock()
}

fn reconcile_live_query<F: LiveFilterQuery>(
    registry: &Arc<StoreRegistry>,
    host_id: Uuid,
    result: &AnyItemMap,
    state: &mut LiveQueryState<F>,
    new_filter: &F,
    current_generation: u64,
    synchronization: &Arc<LiveQuerySynchronization>,
) {
    if let Some(route) = new_filter.query_route() {
        reconcile_indexed_live_query(
            registry,
            host_id,
            result,
            state,
            new_filter,
            (route, current_generation),
            synchronization,
        );
    } else {
        reconcile_scan_live_query(
            registry,
            result,
            state,
            new_filter,
            current_generation,
            synchronization,
        );
    }
}

fn current_live_filter<F: LiveFilterQuery>(
    filter_state: &Mutex<LiveFilterGeneration<F>>,
    synchronization: &LiveQuerySynchronization,
) -> Option<F> {
    let filter_state = filter_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    (filter_state.generation == synchronization.generation.load(Ordering::Relaxed))
        .then(|| filter_state.filter.clone())
}

fn build_live_diff_callback<F: LiveFilterQuery>(
    result: &AnyItemMap,
    filter_state: Arc<Mutex<LiveFilterGeneration<F>>>,
    synchronization: &Arc<LiveQuerySynchronization>,
    scope: LiveDiffScope,
) -> impl Fn(&BucketDiff) + Send + Sync + 'static {
    let synchronization = synchronization.clone();
    let result = result.downgrade();
    let first = AtomicBool::new(true);
    move |diff| {
        let Some(result) = result.upgrade() else {
            return;
        };
        if first.swap(false, Ordering::Relaxed) {
            if let Some(filter) = current_live_filter(&filter_state, &synchronization) {
                apply_live_diff(&result, diff, &filter, scope);
            }
            return;
        }
        let _reconciliation = synchronization
            .reconciliation_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(filter) = current_live_filter(&filter_state, &synchronization) else {
            return;
        };
        apply_live_diff(&result, diff, &filter, scope);
    }
}

fn clear_live_result(result: &AnyItemMap) {
    for (id, _) in result.snapshot() {
        result.remove(&id);
    }
}

fn split_query_route(
    route: QueryRoute,
) -> (
    &'static [&'static str],
    HashSet<CompoundKey>,
    Option<CompoundFkExtractor>,
) {
    match route {
        QueryRoute::Ids(ids) => (
            ID_ROUTE_FIELD_NAMES,
            ids.into_iter()
                .map(|id| CompoundKey::from_iter([id]))
                .collect(),
            None,
        ),
        QueryRoute::BelongsTo(BelongsToRoute {
            field_names,
            keys,
            extract_fk,
        }) => (field_names, keys.into_iter().collect(), Some(extract_fk)),
    }
}

fn build_live_bucket_source<F: LiveFilterQuery>(
    registry: &Arc<StoreRegistry>,
    host_id: Uuid,
    route_field_names: &'static [&'static str],
    extract_fk: Option<CompoundFkExtractor>,
) -> BucketSourceFn {
    extract_fk.map_or_else(
        || {
            let store = registry.get_or_create(F::entity_type());
            let make: BucketSourceFn = Box::new(move |key: &CompoundKey| {
                key.first().map_or_else(
                    || AnyItemMap::new().lock(),
                    |id| build_ids_source_map(&store, std::slice::from_ref(id)),
                )
            });
            make
        },
        |extract_fk| {
            let index = belongs_to_source_index_for(
                registry,
                host_id,
                F::entity_type(),
                route_field_names,
                extract_fk,
            );
            let make: BucketSourceFn =
                Box::new(move |key: &CompoundKey| index.bucket_for(key.clone(), extract_fk).lock());
            make
        },
    )
}

fn reconcile_existing_buckets<F: LiveFilterQuery>(
    result: &AnyItemMap,
    state: &mut LiveQueryState<F>,
    new_filter: &F,
    new_keys: &HashSet<CompoundKey>,
    current_generation: u64,
) {
    for (key, (bucket, _guard)) in state
        .bucket_guards
        .extract_if(|key, _| !new_keys.contains(key))
    {
        for (id, _) in bucket.snapshot() {
            result.remove(&id);
        }
        state.bucket_filter_refs.remove(&key);
    }
    for key in new_keys {
        if let Some(filter_state) = state.bucket_filter_refs.get(key) {
            *filter_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = LiveFilterGeneration {
                generation: current_generation,
                filter: new_filter.clone(),
            };
        }
        if let Some((bucket, _)) = state.bucket_guards.get(key) {
            apply_bucket_candidates(result, new_filter, bucket.snapshot());
        }
    }
}

fn add_live_buckets<F: LiveFilterQuery>(
    result: &AnyItemMap,
    state: &mut LiveQueryState<F>,
    new_filter: &F,
    new_keys: &HashSet<CompoundKey>,
    current_generation: u64,
    make_source: &BucketSourceFn,
    synchronization: &Arc<LiveQuerySynchronization>,
) {
    for key in new_keys {
        if state.bucket_guards.contains_key(key) {
            continue;
        }
        let bucket = make_source(key);
        apply_bucket_candidates(result, new_filter, bucket.snapshot());
        let filter_state = Arc::new(Mutex::new(LiveFilterGeneration {
            generation: current_generation,
            filter: new_filter.clone(),
        }));
        let guard = bucket.subscribe_diffs(build_live_diff_callback(
            result,
            filter_state.clone(),
            synchronization,
            LiveDiffScope::Bucket,
        ));
        state.bucket_guards.insert(key.clone(), (bucket, guard));
        state.bucket_filter_refs.insert(key.clone(), filter_state);
    }
}

fn reconcile_indexed_live_query<F: LiveFilterQuery>(
    registry: &Arc<StoreRegistry>,
    host_id: Uuid,
    result: &AnyItemMap,
    state: &mut LiveQueryState<F>,
    new_filter: &F,
    route_generation: (QueryRoute, u64),
    synchronization: &Arc<LiveQuerySynchronization>,
) {
    state.scan_guard = None;
    let (route, current_generation) = route_generation;
    let (route_field_names, new_keys, extract_fk) = split_query_route(route);
    if state.prev_route_field_names != Some(route_field_names) {
        state.bucket_guards.clear();
        state.bucket_filter_refs.clear();
        clear_live_result(result);
    }
    reconcile_existing_buckets(result, state, new_filter, &new_keys, current_generation);
    if new_keys
        .iter()
        .any(|key| !state.bucket_guards.contains_key(key))
    {
        let make_source =
            build_live_bucket_source::<F>(registry, host_id, route_field_names, extract_fk);
        add_live_buckets(
            result,
            state,
            new_filter,
            &new_keys,
            current_generation,
            &make_source,
            synchronization,
        );
    }
    state.prev_route_field_names = Some(route_field_names);
}

fn reconcile_scan_live_query<F: LiveFilterQuery>(
    registry: &Arc<StoreRegistry>,
    result: &AnyItemMap,
    state: &mut LiveQueryState<F>,
    new_filter: &F,
    current_generation: u64,
    synchronization: &Arc<LiveQuerySynchronization>,
) {
    if state.prev_route_field_names.is_some() {
        state.bucket_guards.clear();
        state.bucket_filter_refs.clear();
        clear_live_result(result);
        state.prev_route_field_names = None;
    }
    let store = registry.get_or_create(F::entity_type());
    reconcile_full_scope_membership(result, new_filter, store.snapshot());
    if state.scan_guard.is_none() {
        let filter_state = Arc::new(Mutex::new(LiveFilterGeneration {
            generation: current_generation,
            filter: new_filter.clone(),
        }));
        state.scan_filter_ref = Some(filter_state.clone());
        state.scan_guard = Some(store.subscribe_diffs(build_live_diff_callback(
            result,
            filter_state,
            synchronization,
            LiveDiffScope::FullStore,
        )));
    } else if let Some(filter_state) = &state.scan_filter_ref {
        *filter_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = LiveFilterGeneration {
            generation: current_generation,
            filter: new_filter.clone(),
        };
    }
}

/// Sweep dead (no-longer-subscribed) buckets across every belongs-to
///
/// relation's source index. `route_to_live_bucket` reaps dead entries lazily
/// on next access, but a foreign id that goes dead and is never looked up
/// again would otherwise linger; called from
/// `MykoServerContext::sweep_dead_cache_entries`.
pub fn sweep_all_belongs_to_source_indexes() {
    belongs_to_source_indexes().retain(|_, weak| {
        let Some(index) = weak.upgrade() else {
            return false;
        };
        index.sweep_dead_buckets();
        true
    });
}

/// Build a `FilteredCellMap` containing only the entries at the given ids,
/// using direct per-key store lookups instead of an O(N) `test_entity` scan.
///
/// Used by `Get<Entity>sByIds::build_view` so the initial query result is
/// constructed in O(M) where M = `ids.len()`. Per-key cells from the store
/// keep the result reactive to inserts / updates / deletes for those
/// specific ids; `test_entity` semantics still hold because the returned
/// map only ever contains keys from `ids`.
#[must_use]
pub fn build_ids_source_map(
    store: &Arc<crate::store::EntityStore>,
    ids: &[Arc<str>],
) -> FilteredCellMap {
    use hyphae::{Materialize, Signal, Watchable};

    let result: hyphae::CellMap<Arc<str>, AnyItemArc> = hyphae::CellMap::new();
    for id in ids {
        let key_cell = store.get(id).materialize();
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
) -> impl hyphae::MapQuery<Key = Arc<str>, Value = AnyItemArc>
where
    Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
    Q::Item:
        DeserializeOwned + Eventable + WithId + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    source.select(move |item_any: &AnyItemArc| {
        downcast_any_item_arc::<Q::Item>(item_any, "filter_query_over_source").is_some_and(|item| {
            Q::test_entity(QueryTestContext {
                item,
                query: query.clone(),
                query_context: query_context.clone(),
            })
        })
    })
}

/// Apply a typed predicate to an already-keyed source map.
///
/// This is useful when the source itself establishes key membership (for
/// example, a direct multi-ID lookup) and the remaining predicate only needs
/// to validate a secondary scope. Keeping key membership out of that hot
/// predicate avoids turning M direct lookups into M scans of the requested ID
/// list.
pub fn filter_typed_source<T, F>(
    source: FilteredCellMap,
    predicate: F,
) -> impl hyphae::MapQuery<Key = Arc<str>, Value = AnyItemArc>
where
    T: Eventable + WithId + Clone + std::fmt::Debug + Send + Sync + 'static,
    F: Fn(&Arc<T>) -> bool + Send + Sync + 'static,
{
    source.select(move |item_any: &AnyItemArc| {
        downcast_any_item_arc::<T>(item_any, "filter_typed_source")
            .is_some_and(|item| predicate(&item))
    })
}

/// Registration entry for a query type.
/// Collected via inventory for automatic discovery.
pub struct QueryRegistration {
    /// Query identifier (e.g., "`GetAllTargets`")
    pub query_id: &'static str,
    /// Entity type this query returns (e.g., "Target")
    pub query_item_type: &'static str,
    /// Typed service owner used by application activation.
    pub service_id: Option<crate::ServiceTypeId>,
    /// Crate where this query is defined (for `type_gen` filtering)
    pub crate_name: &'static str,
    /// Parse function for deserializing query from JSON
    pub parse: QueryParseFn,
    /// Factory for creating reactive cell from query
    pub cell_factory: QueryCellFactory,
    /// Factory for an optional source-level bounded query window.
    pub window_cell_factory: QueryWindowCellFactory,
    #[cfg(not(target_arch = "wasm32"))]
    pub authority: QueryAuthorityFactory,
    /// Query struct's own fields, captured at macro-expansion time. Backs
    /// the MCP `search()` tool's operation index — see `crate::reflection`.
    pub args: &'static [crate::reflection::OperationArgField],
    /// Query struct's doc comment, if any.
    pub description: Option<&'static str>,
    /// Whether language binding generators should publish this query.
    pub include_in_typegen: bool,
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
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn parse(value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error>;

    #[cfg(not(target_arch = "wasm32"))]
    /// Resolve typed source, scope, claims, and capabilities before opening.
    ///
    /// # Errors
    ///
    /// Returns an error when the serialized query parameters are invalid.
    fn authority(
        value: Value,
        local_node: myko_federation::NodeId,
    ) -> Result<crate::server::HandlerAuthority, String>;

    /// Create a reactive cell for this query.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn cell_factory(
        query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Option<Arc<MykoServerContext>>,
        #[cfg(not(target_arch = "wasm32"))] federated: Option<
            crate::server::federated_source::FederatedRequest,
        >,
    ) -> Result<FilteredCellMap, String>;

    /// Create a source-level bounded query window when the handler supports
    /// pushdown.
    ///
    /// # Errors
    ///
    /// Returns an error when the query payload is invalid or the source
    /// cannot build the requested window.
    fn window_cell_factory(
        query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
        window: crate::wire::QueryWindow,
    ) -> Result<Option<super::WindowedQuerySource>, String>;
}

impl<Q: QueryParams> QueryFactory for Q
where
    Q::Item:
        Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    fn authority(
        value: Value,
        local_node: myko_federation::NodeId,
    ) -> Result<crate::server::HandlerAuthority, String> {
        let query: Q = serde_json::from_value(value).map_err(|error| error.to_string())?;
        Ok(crate::server::HandlerAuthority {
            source_node: query.source_node(local_node),
            scope_id: query.scope_id(local_node),
            resource_claims: query.authority_claims(local_node),
            application_capabilities: query.required_capabilities(),
        })
    }

    fn parse(value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error> {
        let query = serde_json::from_value::<QueryRequest<Q>>(value)?;
        Ok(Arc::new(query))
    }

    fn cell_factory(
        any_query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Option<Arc<MykoServerContext>>,
        #[cfg(not(target_arch = "wasm32"))] federated: Option<
            crate::server::federated_source::FederatedRequest,
        >,
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
        let request: QueryRequest<Q> =
            crate::common::downcast::downcast_request(any_ref, "query payload")?;
        let query: Arc<Q> = Arc::new(request.query);

        let query_ctx = Arc::new(QueryContext { req: request_ctx });
        let query_cell_ctx = QueryBuildContext::new_routed(
            query_ctx.clone(),
            registry.clone(),
            server_ctx,
            #[cfg(not(target_arch = "wasm32"))]
            federated.clone(),
        );

        if let Some(built) = Q::build_view(QueryBuildArgs {
            query: query.clone(),
            query_context: query_cell_ctx,
            #[cfg(not(target_arch = "wasm32"))]
            federated,
        }) {
            return Ok(hyphae::MapQuery::materialize(built));
        }

        let store: crate::store::EntityStore =
            (*registry.get_or_create(&Q::query_item_type_static())).clone();
        Ok(hyphae::MapQuery::materialize(store.select(
            move |item_any: &AnyItemArc| {
                downcast_any_item_arc::<Q::Item>(item_any, "QueryFactory::cell_factory")
                    .is_some_and(|item| {
                        Q::test_entity(QueryTestContext {
                            item,
                            query: query.clone(),
                            query_context: query_ctx.clone(),
                        })
                    })
            },
        )))
    }

    fn window_cell_factory(
        any_query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Arc<MykoServerContext>,
        window: crate::wire::QueryWindow,
    ) -> Result<Option<super::WindowedQuerySource>, String> {
        let any_ref: &dyn Any = any_query.as_ref();
        let request: QueryRequest<Q> =
            crate::common::downcast::downcast_request(any_ref, "windowed query payload")?;
        let query_context = Arc::new(QueryContext { req: request_ctx });
        Q::build_window(QueryWindowBuildArgs {
            query: Arc::new(request.query),
            query_context: QueryBuildContext::new(query_context, registry, Some(server_ctx)),
            window,
        })
    }
}

#[cfg(test)]
mod belongs_to_source_index_tests {
    use std::{any::Any, time::Instant};

    use hyphae::{Gettable, Materialize};
    use serde::Serialize;

    use super::*;
    use crate::{common::with_id::WithId, test_util::scheduler_test_serial};

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
                .is_some_and(|t| t == self)
        }
    }

    fn extract_parent_fk(item: &dyn Any) -> Option<CompoundKey> {
        item.downcast_ref::<TestChild>()
            .map(|c| smallvec::smallvec![c.parent_id.clone()])
    }

    fn child(id: &str, parent: &str) -> (Arc<str>, AnyItemArc) {
        let item: AnyItemArc = Arc::new(TestChild {
            id: Arc::from(id),
            parent_id: Arc::from(parent),
        });
        (Arc::from(id), item)
    }

    fn new_store() -> Arc<crate::store::EntityStore> {
        Arc::new(hyphae::CellMap::new())
    }

    fn wait_for_bucket_len(bucket: &AnyItemMap, expected: usize) {
        let started = Instant::now();
        while bucket.snapshot().len() != expected
            && started.elapsed() < std::time::Duration::from_secs(1)
        {
            std::thread::yield_now();
        }
    }

    #[test]
    fn dropped_subscriptions_do_not_leak_across_many_distinct_parents() {
        let _serial = scheduler_test_serial();
        // Reproduces the leak: every distinct foreign id ever subscribed to
        // used to leave a permanent bucket behind. With weak-ref buckets,
        // dropping every subscriber and sweeping must bring the count to 0
        // regardless of how many distinct parents were ever seen.
        let store = new_store();
        let index = BelongsToSourceIndex::new(store, extract_parent_fk);

        for i in 0..50 {
            let parent: Arc<str> = Arc::from(format!("parent-{i}"));
            let bucket = index.bucket_for(smallvec::smallvec![parent], extract_parent_fk);
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
        let _serial = scheduler_test_serial();
        // The naive fix (remove a bucket the moment it goes empty) breaks
        // this: a still-live subscriber would get orphaned from a bucket
        // that later gets silently replaced. Weak-ref buckets avoid this —
        // as long as the subscriber holds their strong handle, `bucket_for`
        // keeps returning the *same* object.
        let store = new_store();
        let (id, item) = child("c1", "parent-x");
        store.insert(id.clone(), item);

        let index = BelongsToSourceIndex::new(store.clone(), extract_parent_fk);
        let bucket = index.bucket_for(
            smallvec::smallvec![Arc::from("parent-x")],
            extract_parent_fk,
        );
        assert_eq!(bucket.snapshot().len(), 1);

        // Remove the only child — bucket goes empty, but `bucket` is still
        // held here, simulating a live subscriber.
        store.remove_many(vec![id]);
        wait_for_bucket_len(&bucket, 0);
        assert_eq!(bucket.snapshot().len(), 0);

        // A new child arrives under the same parent — the still-held handle
        // must see it, not a disconnected/orphaned bucket.
        let (id2, item2) = child("c2", "parent-x");
        store.insert(id2, item2);
        wait_for_bucket_len(&bucket, 1);
        assert_eq!(
            bucket.snapshot().len(),
            1,
            "a live subscriber must see re-population after its bucket went empty"
        );
    }

    #[test]
    fn resubscribing_after_reap_backfills_current_children() {
        let _serial = scheduler_test_serial();
        // The bug a naive weak-ref swap alone would introduce: once a
        // bucket is reaped, apply_diff never creates buckets nobody's
        // watching, so a fresh subscription must explicitly backfill from
        // the current store state rather than starting empty.
        let store = new_store();
        let (id, item) = child("c1", "parent-y");
        store.insert(id, item);

        let index = BelongsToSourceIndex::new(store.clone(), extract_parent_fk);

        {
            let bucket = index.bucket_for(
                smallvec::smallvec![Arc::from("parent-y")],
                extract_parent_fk,
            );
            assert_eq!(bucket.snapshot().len(), 1);
        }
        index.sweep_dead_buckets();
        assert!(index.buckets.is_empty());

        let bucket = index.bucket_for(
            smallvec::smallvec![Arc::from("parent-y")],
            extract_parent_fk,
        );
        assert_eq!(
            bucket.snapshot().len(),
            1,
            "resubscribing after the bucket was reaped must backfill current children, not start empty"
        );
    }

    #[test]
    fn concurrent_bucket_for_calls_for_the_same_key_never_orphan_a_bucket() {
        const N: usize = 16;
        let _serial = scheduler_test_serial();

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
        let key: CompoundKey = smallvec::smallvec![Arc::from("parent-race")];

        let barrier = Arc::new(std::sync::Barrier::new(N));
        let mut handles = Vec::with_capacity(N);
        for _ in 0..N {
            let index = index.clone();
            let key = key.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                index.bucket_for(key, extract_parent_fk)
            }));
        }
        let buckets: Vec<AnyItemMap> = handles
            .into_iter()
            .filter_map(|handle| handle.join().ok())
            .collect();
        assert_eq!(buckets.len(), N, "all bucket threads must complete");

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

    #[test]
    fn concurrent_first_subscription_and_insert_cannot_lose_the_insert() {
        let _serial = scheduler_test_serial();
        // Exercise the publication/backfill boundary repeatedly. The index
        // mutation gate makes the insert callback fall wholly before or
        // after bucket construction, so either ordering must converge on
        // the inserted child without requiring a later source diff.
        for i in 0..128 {
            let store = new_store();
            let index = BelongsToSourceIndex::new(store.clone(), extract_parent_fk);
            let barrier = Arc::new(std::sync::Barrier::new(2));

            let bucket_thread = {
                let index = index.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    index.bucket_for(
                        smallvec::smallvec![Arc::from("parent-race")],
                        extract_parent_fk,
                    )
                })
            };
            let insert_thread = {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let (id, item) = child(&format!("child-{i}"), "parent-race");
                    store.insert(id, item);
                })
            };

            let bucket = bucket_thread.join();
            assert!(bucket.is_ok(), "bucket thread must complete");
            let Ok(bucket) = bucket else {
                return;
            };
            assert!(insert_thread.join().is_ok());
            assert_eq!(
                bucket.snapshot().len(),
                1,
                "iteration {i} lost the insert racing first bucket construction"
            );
        }
    }

    #[test]
    fn bucket_diff_fanout_can_reenter_bucket_creation() {
        let _serial = scheduler_test_serial();
        let store = new_store();
        let index = BelongsToSourceIndex::new(store.clone(), extract_parent_fk);
        let bucket = index.bucket_for(
            smallvec::smallvec![Arc::from("parent-source")],
            extract_parent_fk,
        );
        let (sent, received) = std::sync::mpsc::channel();
        let index_for_callback = index.clone();
        let guard = bucket.subscribe_diffs(move |_| {
            // Real query graphs can synchronously construct another routed
            // query while handling this diff. That must not attempt to
            // reacquire an index gate still held by apply_diff.
            let nested = index_for_callback.bucket_for(
                smallvec::smallvec![Arc::from("parent-nested")],
                extract_parent_fk,
            );
            drop(nested);
            let _send_result = sent.send(());
        });

        // Discard subscribe_diffs' synchronous Initial notification.
        assert!(received.recv().is_ok());
        let insert = std::thread::spawn(move || {
            let (id, item) = child("child-reentrant", "parent-source");
            store.insert(id, item);
        });
        assert!(
            received
                .recv_timeout(std::time::Duration::from_secs(2))
                .is_ok(),
            "bucket fanout deadlocked while re-entering bucket_for"
        );
        assert!(insert.join().is_ok());
        drop(guard);
    }

    #[test]
    fn global_registry_does_not_retain_an_unused_index() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(crate::store::StoreRegistry::new());
        let host_id = Uuid::new_v4();
        let registry_key = format!("{host_id}:TestChild:parent_id");
        let index = belongs_to_source_index_for(
            &registry,
            host_id,
            "TestChild",
            &["parent_id"],
            extract_parent_fk,
        );
        let weak = Arc::downgrade(&index);
        let bucket = index.bucket_for(
            smallvec::smallvec![Arc::from("parent-live")],
            extract_parent_fk,
        );

        drop(index);
        assert!(
            weak.upgrade().is_some(),
            "a live bucket must retain the index that routes its updates"
        );
        drop(bucket);
        assert!(
            weak.upgrade().is_none(),
            "the global registry or driver subscription retained an unused index"
        );

        sweep_all_belongs_to_source_indexes();
        assert!(
            !belongs_to_source_indexes().contains_key(&registry_key),
            "sweeping must remove the dead weak registry entry"
        );
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
        anchor_id: Arc<str>,
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
                .is_some_and(|t| t == self)
        }
    }

    fn cursor(id: &str, node: &str, session: &str) -> (Arc<str>, AnyItemArc) {
        cursor_with_anchor(id, node, session, "anchor-default")
    }

    fn cursor_with_anchor(
        id: &str,
        node: &str,
        session: &str,
        anchor: &str,
    ) -> (Arc<str>, AnyItemArc) {
        let item: AnyItemArc = Arc::new(TestCursor {
            id: Arc::from(id),
            node_id: Arc::from(node),
            session_id: Arc::from(session),
            anchor_id: Arc::from(anchor),
        });
        (Arc::from(id), item)
    }

    // Mirrors the macro-generated compound extractor for a query that pins
    // BOTH belongs_to fields: position 0 = node_id, position 1 = session_id.
    fn extract_node_and_session_fk(item: &dyn Any) -> Option<CompoundKey> {
        item.downcast_ref::<TestCursor>()
            .map(|c| smallvec::smallvec![c.node_id.clone(), c.session_id.clone()])
    }

    #[test]
    fn compound_key_separates_watchers_sharing_one_field_but_not_the_other() {
        let _serial = scheduler_test_serial();
        // Two cursors in the SAME session but for DIFFERENT nodes. Before
        // compound routing, both watchers would collapse onto one
        // session-keyed bucket (single-field routing on whichever field
        // came first in the struct). With compound (node_id, session_id)
        // keys, each watcher gets its own bucket, scoped to exactly its
        // node+session pair.
        let store = new_store();
        let (id_a, item_a) = cursor("cursor-a", "node-A", "session-PROD");
        let (id_b, item_b) = cursor("cursor-b", "node-B", "session-PROD");
        store.insert(id_a, item_a);
        store.insert(id_b, item_b);

        let index = BelongsToSourceIndex::new(store.clone(), extract_node_and_session_fk);

        let key_a: CompoundKey =
            smallvec::smallvec![Arc::from("node-A"), Arc::from("session-PROD")];
        let key_b: CompoundKey =
            smallvec::smallvec![Arc::from("node-B"), Arc::from("session-PROD")];

        let bucket_a = index.bucket_for(key_a, extract_node_and_session_fk);
        let bucket_b = index.bucket_for(key_b, extract_node_and_session_fk);

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
        let (alpha_id, alpha_item) = cursor("cursor-a-tick2", "node-A", "session-PROD");
        store.insert(alpha_id, alpha_item);
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

        let (bravo_id, bravo_item) = cursor("cursor-b-tick2", "node-B", "session-PROD");
        store.insert(bravo_id, bravo_item);
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
        let _serial = scheduler_test_serial();
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
        let key: CompoundKey = smallvec::smallvec![Arc::from("node-A"), Arc::from("session-PROD")];
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
        let _serial = scheduler_test_serial();
        let sets = vec![
            vec![Arc::from("node-A"), Arc::from("node-B")],
            vec![Arc::from("session-PROD")],
        ];
        let mut product = cartesian_product(sets);
        product.sort();
        assert_eq!(
            product,
            vec![
                CompoundKey::from_iter([Arc::<str>::from("node-A"), Arc::from("session-PROD")]),
                CompoundKey::from_iter([Arc::<str>::from("node-B"), Arc::from("session-PROD")]),
            ]
        );
    }

    #[test]
    fn cartesian_product_empty_set_yields_no_keys() {
        let _serial = scheduler_test_serial();
        // In([]) on any one field means no key any item could satisfy
        // exists — matches "In([]) matches nothing" (spec §1).
        let sets = vec![vec![Arc::from("node-A")], vec![]];
        assert!(cartesian_product(sets).is_empty());
    }

    #[test]
    fn union_source_map_unions_k_buckets_and_stays_reactive() {
        let _serial = scheduler_test_serial();
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
            smallvec::smallvec![Arc::from("node-A"), Arc::from("session-PROD")],
            smallvec::smallvec![Arc::from("node-B"), Arc::from("session-PROD")],
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

        let (alpha_id, alpha_item) = cursor("cursor-a2", "node-A", "session-PROD");
        store.insert(alpha_id, alpha_item);
        assert_eq!(
            union.snapshot().len(),
            3,
            "union must keep tracking writes to any of its unioned buckets"
        );

        let (charlie_id, charlie_item) = cursor("cursor-c2", "node-C", "session-PROD");
        store.insert(charlie_id, charlie_item);
        assert_eq!(
            union.snapshot().len(),
            3,
            "writes to a non-unioned bucket must never appear in the union"
        );
    }

    #[test]
    fn compound_union_with_residual_filter_propagates_batch_delete() {
        let _serial = scheduler_test_serial();
        let registry = Arc::new(crate::store::StoreRegistry::new());
        let store = registry.get_or_create("TestCursor");
        let (matched_id, matched) =
            cursor_with_anchor("cursor-match", "node-A", "session-PROD", "anchor-match");
        let (residual_miss_id, residual_miss) = cursor_with_anchor(
            "cursor-residual-miss",
            "node-A",
            "session-PROD",
            "anchor-other",
        );
        store.insert(matched_id.clone(), matched);
        store.insert(residual_miss_id, residual_miss);

        let source = build_belongs_to_union_source_map(
            registry,
            Uuid::new_v4(),
            "TestCursor",
            &["node_id", "session_id"],
            extract_node_and_session_fk,
            vec![smallvec::smallvec![
                Arc::from("node-A"),
                Arc::from("session-PROD")
            ]],
        );
        let filtered = hyphae::MapQuery::materialize(source.select(|item| {
            item.as_any()
                .downcast_ref::<TestCursor>()
                .is_some_and(|cursor| cursor.anchor_id.as_ref() == "anchor-match")
        }));
        let items = filtered.items().materialize();
        assert_eq!(items.get().len(), 1);

        // One row remains in the store, so remove_many emits Batch { Remove }
        // rather than Initial { empty }. Both deletion shapes must propagate.
        store.remove_many(vec![matched_id]);

        assert_eq!(items.get().len(), 0);
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
