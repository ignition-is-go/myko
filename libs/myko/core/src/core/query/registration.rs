//! Query registration via inventory.

use std::{
    any::Any,
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex, OnceLock,
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
        BelongsToRoute, CompoundFkExtractor, CompoundKey, ID_ROUTE_FIELD_NAMES, KeyExtractor,
        LiveFilterQuery, QueryRoute,
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

/// Type-erased cell factory for queries.
/// Takes a typed query, registry, and `host_id`, returns a `FilteredCellMap`.
pub type QueryCellFactory = fn(
    Arc<dyn AnyQuery>,
    Arc<StoreRegistry>,
    Arc<RequestContext>,
    Option<Arc<MykoServerContext>>,
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
type BucketDiff = MapDiff<Arc<str>, AnyItemArc>;
type BucketDiffs = Vec<BucketDiff>;
/// A bucket and the domain it lives in.
type HomedBucket = (hyphae::scheduler::Domain, AnyItemMap);
type BucketAction = (HomedBucket, BucketDiff);

#[derive(Default)]
struct BelongsToMutationState {
    members: HashMap<CompoundKey, HashMap<Arc<str>, AnyItemArc>>,
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

fn query_factories_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_FACTORIES_BY_ID.get_or_init(DashMap::new)
}

fn query_guards_created_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_GUARDS_CREATED_BY_ID.get_or_init(DashMap::new)
}

fn query_guards_removed_by_id() -> &'static DashMap<Arc<str>, u64> {
    QUERY_GUARDS_REMOVED_BY_ID.get_or_init(DashMap::new)
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
    /// Per key, one bucket for each domain that reads it.
    buckets: DashMap<CompoundKey, Vec<(hyphae::scheduler::Domain, WeakAnyItemMap)>>,
    mutation_gate: Mutex<BelongsToMutationState>,
    driver: Arc<AnyItemMap>,
    /// The domain of the store, where every bucket is born: the index writes
    /// its buckets from the store's propagation.
    domain: hyphae::scheduler::Domain,
    idle: IdleProbe,
}

impl BelongsToSourceIndex {
    fn new(store: Arc<crate::store::EntityStore>, extract_fk: KeyExtractor) -> Arc<Self> {
        let driver = Arc::new(AnyItemMap::new());
        let index = Arc::new(Self {
            store,
            buckets: DashMap::new(),
            mutation_gate: Mutex::new(BelongsToMutationState::default()),
            driver: driver.clone(),
            domain: hyphae::scheduler::Domain::current(),
            idle: idle_probe(),
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
    fn route_to_live_buckets(&self, key: &CompoundKey) -> Vec<HomedBucket> {
        let Some(mut entry) = self.buckets.get_mut(key) else {
            return Vec::new();
        };
        let mut live = Vec::new();
        entry.retain(|(domain, weak)| {
            weak.upgrade().is_some_and(|map| {
                live.push((domain.clone(), map));
                true
            })
        });
        let empty = entry.is_empty();
        drop(entry);
        if empty {
            self.buckets.remove_if(key, |_, homes| homes.is_empty());
        }
        live
    }

    /// The bucket of `key` in the calling thread's ambient domain.
    ///
    /// A reader in the index's own domain shares one bucket per key. A child
    /// domain reading a root index gets a bucket of its own, born in the
    /// child and backfilled there, which the index feeds through the
    /// child's hand-off queue in root order. Nothing the index writes from
    /// root's propagation is therefore a child cell, and a child never
    /// writes a root cell to build its bucket.
    fn bucket_for(self: &Arc<Self>, key: CompoundKey) -> AnyItemMap {
        let ambient = hyphae::scheduler::Domain::current();
        let home = if self.domain.is_root() {
            ambient
        } else {
            self.domain.clone()
        };
        let mutation = self
            .mutation_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut homes = self.buckets.entry(key).or_default();
        homes.retain(|(_, weak)| weak.upgrade().is_some());
        if let Some(map) = homes
            .iter()
            .find(|(domain, _)| *domain == home)
            .and_then(|(_, weak)| weak.upgrade())
        {
            return map;
        }
        let map = home.enter(|| {
            let map = AnyItemMap::new();
            self.retain_for_bucket(&map);
            map
        });
        Self::backfill(&map, mutation.members.get(homes.key()));
        homes.push((home, map.downgrade()));
        drop(homes);
        drop(mutation);
        map
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

    fn backfill(map: &AnyItemMap, members: Option<&HashMap<Arc<str>, AnyItemArc>>) {
        if let Some(members) = members {
            map.apply_diff_owned(MapDiff::Initial {
                entries: members
                    .iter()
                    .map(|(id, item)| (id.clone(), item.clone()))
                    .collect(),
            });
        }
    }

    /// Drop bucket entries nobody's subscribed to anymore. `route_to_live_bucket`
    /// already reaps dead entries lazily on next access, but a key that goes
    /// dead and is never looked up again would otherwise sit in
    /// `self.buckets` forever (just a `Weak` + a small `Vec<Arc<str>>` key,
    /// far smaller than the leak this replaces, but still unbounded over
    /// time). Called from `MykoServerContext::sweep_dead_cache_entries` via
    /// [`sweep_all_belongs_to_source_indexes`].
    fn sweep_dead_buckets(&self) {
        self.buckets.retain(|_, homes| {
            homes.retain(|(_, weak)| weak.upgrade().is_some());
            !homes.is_empty()
        });
    }

    fn apply_diff(&self, diff: &BucketDiff, extract_fk: KeyExtractor) {
        {
            let mut mutation = self
                .mutation_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let actions = self.prepare_diff_locked(diff, extract_fk, &mut mutation.members);
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
            for ((home, bucket), diff) in actions {
                if home == self.domain {
                    bucket.apply_diff_owned(diff);
                } else {
                    home.hand_off(move || bucket.apply_diff_owned(diff));
                }
            }
        }
    }

    fn prepare_diff_locked(
        &self,
        diff: &BucketDiff,
        extract_fk: KeyExtractor,
        members: &mut HashMap<CompoundKey, HashMap<Arc<str>, AnyItemArc>>,
    ) -> Vec<BucketAction> {
        match diff {
            MapDiff::Initial { entries } => {
                members.clear();
                for (id, item) in entries {
                    for fk in extract_fk.keys(item.as_any()) {
                        members
                            .entry(fk)
                            .or_default()
                            .insert(id.clone(), item.clone());
                    }
                }
                let keys = self
                    .buckets
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect::<Vec<_>>();
                keys.into_iter()
                    .flat_map(|key| {
                        let entries = members
                            .get(&key)
                            .map(|rows| {
                                rows.iter()
                                    .map(|(id, item)| (id.clone(), item.clone()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        self.route_action(&key, MapDiff::Initial { entries })
                    })
                    .collect()
            }
            MapDiff::Batch { changes } => {
                let mut actions = Vec::new();
                let mut grouped = HashMap::new();
                for change in changes {
                    if matches!(change, MapDiff::Initial { .. } | MapDiff::Batch { .. }) {
                        actions.extend(self.apply_grouped(std::mem::take(&mut grouped), members));
                        actions.extend(self.prepare_diff_locked(change, extract_fk, members));
                    } else {
                        Self::group_change(&mut grouped, change, extract_fk);
                    }
                }
                actions.extend(self.apply_grouped(grouped, members));
                actions
            }
            _ => {
                let mut grouped = HashMap::new();
                Self::group_change(&mut grouped, diff, extract_fk);
                self.apply_grouped(grouped, members)
            }
        }
    }

    fn apply_grouped(
        &self,
        grouped: HashMap<CompoundKey, BucketDiffs>,
        members: &mut HashMap<CompoundKey, HashMap<Arc<str>, AnyItemArc>>,
    ) -> Vec<BucketAction> {
        grouped
            .into_iter()
            .flat_map(|(fk, changes)| {
                let rows = members.entry(fk.clone()).or_default();
                for change in &changes {
                    match change {
                        MapDiff::Insert { key, value } => {
                            rows.insert(key.clone(), value.clone());
                        }
                        MapDiff::Update { key, new_value, .. } => {
                            rows.insert(key.clone(), new_value.clone());
                        }
                        MapDiff::Remove { key, .. } => {
                            rows.remove(key);
                        }
                        MapDiff::Initial { .. } | MapDiff::Batch { .. } => {}
                    }
                }
                if rows.is_empty() {
                    members.remove(&fk);
                }
                self.route_action(&fk, MapDiff::Batch { changes })
            })
            .collect()
    }

    fn route_action(&self, foreign_key: &CompoundKey, diff: BucketDiff) -> Vec<BucketAction> {
        let mut buckets = self.route_to_live_buckets(foreign_key);
        let last = buckets.pop();
        let mut actions: Vec<BucketAction> = buckets
            .into_iter()
            .map(|bucket| (bucket, diff.clone()))
            .collect();
        actions.extend(last.map(|bucket| (bucket, diff)));
        actions
    }

    fn group_change(
        by_fk: &mut HashMap<CompoundKey, BucketDiffs>,
        change: &BucketDiff,
        extract_fk: KeyExtractor,
    ) {
        let mut push = |foreign_key: CompoundKey, diff: BucketDiff| {
            by_fk.entry(foreign_key).or_default().push(diff);
        };
        match change {
            MapDiff::Insert { key, value } => {
                for fk in extract_fk.keys(value.as_any()) {
                    push(
                        fk,
                        MapDiff::Insert {
                            key: key.clone(),
                            value: value.clone(),
                        },
                    );
                }
            }
            MapDiff::Remove { key, old_value } => {
                for fk in extract_fk.keys(old_value.as_any()) {
                    push(
                        fk,
                        MapDiff::Remove {
                            key: key.clone(),
                            old_value: old_value.clone(),
                        },
                    );
                }
            }
            MapDiff::Update {
                key,
                old_value,
                new_value,
            } => {
                // A key in both is an update in place; a key only in the old
                // row loses it and a key only in the new row gains it.
                let old_keys = extract_fk.keys(old_value.as_any());
                let new_keys = extract_fk.keys(new_value.as_any());
                for fk in &old_keys {
                    if !new_keys.contains(fk) {
                        push(
                            fk.clone(),
                            MapDiff::Remove {
                                key: key.clone(),
                                old_value: old_value.clone(),
                            },
                        );
                    }
                }
                for fk in new_keys {
                    let diff = if old_keys.contains(&fk) {
                        MapDiff::Update {
                            key: key.clone(),
                            old_value: old_value.clone(),
                            new_value: new_value.clone(),
                        }
                    } else {
                        MapDiff::Insert {
                            key: key.clone(),
                            value: new_value.clone(),
                        }
                    };
                    push(fk, diff);
                }
            }
            MapDiff::Initial { .. } | MapDiff::Batch { .. } => {}
        }
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
    let index = belongs_to_source_index_for(
        &registry,
        local_type,
        field_names,
        KeyExtractor::One(extract_fk),
    );
    drop(registry);
    index.bucket_for(foreign_ids).lock()
}

fn belongs_to_source_index_for(
    registry: &StoreRegistry,
    local_type: &str,
    field_names: &'static [&'static str],
    extract_fk: KeyExtractor,
) -> Arc<BelongsToSourceIndex> {
    // Building the index reads every row of the store. The registry that
    // owns the store keeps it, so a routed query with no other live query on
    // the store does not pay for that again, and a partition never reads an
    // index built over another partition's store.
    registry.index(
        local_type,
        format!("belongs_to:{local_type}:{}", field_names.join("+")),
        |owner| BelongsToSourceIndex::new(owner.get_or_create(local_type), extract_fk),
    )
}

thread_local! {
    static IDLE_PROBE_RAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether the store's domain has no batch open and no drain running, so the
/// index has applied every diff the store produced.
///
/// A cell set runs its subscriber on the calling thread only then; otherwise
/// the scheduler queues it. hyphae exposes no direct read of that state, so
/// the index keeps a probe cell born in the store's domain. A thread inside
/// any propagation is never idle, and could not write the probe of another
/// domain.
fn domain_is_idle(probe: &IdleProbe) -> bool {
    use hyphae::Mutable;
    static TICKET: AtomicU64 = AtomicU64::new(0);
    if hyphae::scheduler::Domain::propagating().is_some() {
        return false;
    }
    IDLE_PROBE_RAN.with(|ran| ran.set(false));
    probe
        .0
        .set(TICKET.fetch_add(1, Ordering::Relaxed).wrapping_add(1));
    IDLE_PROBE_RAN.with(std::cell::Cell::get)
}

type IdleProbe = (
    hyphae::Cell<u64, hyphae::CellMutable>,
    hyphae::SubscriptionGuard,
);

fn idle_probe() -> IdleProbe {
    use hyphae::Watchable;
    let cell = hyphae::Cell::new(0);
    let guard = cell.subscribe(|_| IDLE_PROBE_RAN.with(|ran| ran.set(true)));
    (cell, guard)
}

/// The rows a `belongs_to` route selects, read from the index without
/// building a reactive map.
///
/// The index follows the store through its diffs, which the scheduler may
/// still hold. `None` is returned while the scheduler has work queued, so a
/// caller that reads the store instead never misses a row just written.
#[must_use]
pub fn belongs_to_route_snapshot(
    registry: &Arc<StoreRegistry>,
    local_type: &str,
    route: &crate::query::BelongsToRoute,
) -> Option<Vec<AnyItemArc>> {
    let index =
        belongs_to_source_index_for(registry, local_type, route.field_names, route.extractor());
    ROUTE_SNAPSHOTS.fetch_add(1, Ordering::Relaxed);
    if !domain_is_idle(&index.idle) {
        ROUTE_SNAPSHOT_FALLBACKS.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    let mutation = index
        .mutation_gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // A row filed under several of the keys is returned once.
    let keys: HashSet<&CompoundKey> = route.keys.iter().collect();
    let mut seen: HashSet<&Arc<str>> = HashSet::new();
    Some(
        keys.into_iter()
            .filter_map(|key| mutation.members.get(key))
            .flat_map(|members| members.iter())
            .filter(|(id, _)| seen.insert(*id))
            .map(|(_, item)| item.clone())
            .collect(),
    )
}

static ROUTE_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);
static ROUTE_SNAPSHOT_FALLBACKS: AtomicU64 = AtomicU64::new(0);

/// Index-routed snapshot reads since the process started, and how many of
/// them found the scheduler busy and read the whole store instead.
#[must_use]
pub fn route_snapshot_counts() -> (u64, u64) {
    (
        ROUTE_SNAPSHOTS.load(Ordering::Relaxed),
        ROUTE_SNAPSHOT_FALLBACKS.load(Ordering::Relaxed),
    )
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

/// How many of a union's buckets hold each row. A row leaves the union only
/// when the last bucket holding it drops it: a row that moves between two
/// buckets of one union, or that is filed under two of them, must not be
/// removed by whichever bucket happens to report first.
type UnionHolders = HashMap<Arc<str>, usize>;

/// Rewrite one bucket's diffs into incremental changes safe to apply to the
/// union of several buckets. An `Initial` is a replacement of this bucket's
/// contribution, so the previous contribution must be retracted before the
/// new snapshot is inserted. Treating it as inserts alone loses an
/// `Initial { entries: [] }` clear when the last source row is deleted.
fn additive_union_diff(
    diff: &BucketDiff,
    contribution: &mut HashMap<Arc<str>, AnyItemArc>,
    holders: &mut UnionHolders,
) -> Option<BucketDiff> {
    let mut changes = Vec::new();
    union_changes(diff, contribution, holders, &mut changes);
    match changes.len() {
        0 => None,
        1 => changes.pop(),
        _ => Some(MapDiff::Batch { changes }),
    }
}

fn union_changes(
    diff: &BucketDiff,
    contribution: &mut HashMap<Arc<str>, AnyItemArc>,
    holders: &mut UnionHolders,
    out: &mut Vec<BucketDiff>,
) {
    match diff {
        MapDiff::Initial { entries } => {
            let previous: Vec<(Arc<str>, AnyItemArc)> = contribution.drain().collect();
            for (key, old_value) in previous {
                release(holders, key, old_value, out);
            }
            for (key, value) in entries {
                hold(contribution, holders, key, value, out);
            }
        }
        MapDiff::Batch { changes } => {
            for change in changes {
                union_changes(change, contribution, holders, out);
            }
        }
        MapDiff::Insert { key, value } => hold(contribution, holders, key, value, out),
        MapDiff::Update {
            key,
            old_value,
            new_value,
        } => {
            // An update must stay an update: the predicate downstream drops
            // a row that stops matching only when it sees one.
            if contribution
                .insert(key.clone(), new_value.clone())
                .is_some()
            {
                out.push(MapDiff::Update {
                    key: key.clone(),
                    old_value: old_value.clone(),
                    new_value: new_value.clone(),
                });
            } else {
                contribution.remove(key);
                hold(contribution, holders, key, new_value, out);
            }
        }
        MapDiff::Remove { key, old_value } => {
            if contribution.remove(key).is_some() {
                release(holders, key.clone(), old_value.clone(), out);
            }
        }
    }
}

fn hold(
    contribution: &mut HashMap<Arc<str>, AnyItemArc>,
    holders: &mut UnionHolders,
    key: &Arc<str>,
    value: &AnyItemArc,
    out: &mut Vec<BucketDiff>,
) {
    if let Some(old_value) = contribution.insert(key.clone(), value.clone()) {
        out.push(MapDiff::Update {
            key: key.clone(),
            old_value,
            new_value: value.clone(),
        });
        return;
    }
    let count = holders.entry(key.clone()).or_default();
    *count = count.saturating_add(1);
    out.push(MapDiff::Insert {
        key: key.clone(),
        value: value.clone(),
    });
}

/// States each change against what the union shows: an insert or update of
/// a row it shows becomes an update from its shown value, and of a row it
/// does not show, an insert. A predicate downstream drops a row that stops
/// matching only on an update, and adds a row only on an insert.
fn as_changes_to(result: &AnyItemMap, diff: BucketDiff) -> BucketDiff {
    let mut shown: HashMap<Arc<str>, Option<AnyItemArc>> = HashMap::new();
    restate(result, diff, &mut shown)
}

fn restate(
    result: &AnyItemMap,
    diff: BucketDiff,
    shown: &mut HashMap<Arc<str>, Option<AnyItemArc>>,
) -> BucketDiff {
    let mut current = |key: &Arc<str>| {
        shown
            .entry(key.clone())
            .or_insert_with(|| result.get_value(key))
            .clone()
    };
    let upsert = |key: Arc<str>, value: AnyItemArc, current: Option<AnyItemArc>| match current {
        Some(old_value) => MapDiff::Update {
            key,
            old_value,
            new_value: value,
        },
        None => MapDiff::Insert { key, value },
    };
    let change = match diff {
        MapDiff::Insert { key, value } => {
            let was = current(&key);
            upsert(key, value, was)
        }
        MapDiff::Update { key, new_value, .. } => {
            let was = current(&key);
            upsert(key, new_value, was)
        }
        MapDiff::Batch { changes } => {
            return MapDiff::Batch {
                changes: changes
                    .into_iter()
                    .map(|change| restate(result, change, shown))
                    .collect(),
            };
        }
        other => other,
    };
    match &change {
        MapDiff::Insert { key, value }
        | MapDiff::Update {
            key,
            new_value: value,
            ..
        } => {
            shown.insert(key.clone(), Some(value.clone()));
        }
        MapDiff::Remove { key, .. } => {
            shown.insert(key.clone(), None);
        }
        MapDiff::Initial { .. } | MapDiff::Batch { .. } => {}
    }
    change
}

fn release(
    holders: &mut UnionHolders,
    key: Arc<str>,
    old_value: AnyItemArc,
    out: &mut Vec<BucketDiff>,
) {
    let remaining = holders.get_mut(&key).map(|count| {
        *count = count.saturating_sub(1);
        *count
    });
    if remaining == Some(0) {
        holders.remove(&key);
        out.push(MapDiff::Remove { key, old_value });
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
    local_type: &'static str,
    field_names: &'static [&'static str],
    extract_fk: CompoundFkExtractor,
    keys: Vec<CompoundKey>,
) -> FilteredCellMap {
    union_source_map(
        registry,
        local_type,
        field_names,
        KeyExtractor::One(extract_fk),
        keys,
    )
}

/// The rows of every key a route names, kept live: a union of the index's
/// buckets for those keys. A row filed under several of them appears once.
pub fn build_index_union_source_map(
    registry: Arc<StoreRegistry>,
    local_type: &'static str,
    route: BelongsToRoute,
) -> FilteredCellMap {
    let extractor = route.extractor();
    union_source_map(
        registry,
        local_type,
        route.field_names,
        extractor,
        route.keys,
    )
}

fn union_source_map(
    registry: Arc<StoreRegistry>,
    local_type: &'static str,
    field_names: &'static [&'static str],
    extract_fk: KeyExtractor,
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

    let index = belongs_to_source_index_for(&registry, local_type, field_names, extract_fk);
    let result: AnyItemMap = AnyItemMap::new();
    let holders = Arc::new(Mutex::new(UnionHolders::new()));
    for key in keys {
        let bucket = index.bucket_for(key).lock();
        let result_weak = result.downgrade();
        let contribution = Mutex::new(HashMap::new());
        let holders = holders.clone();
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
                &mut holders
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
            if let Some(additive) = additive {
                result.apply_diff_owned(as_changes_to(&result, additive));
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
#[derive(Clone)]
enum LiveDiffScope {
    /// One key's bucket of the store named here. A row this bucket drops
    /// may still belong to the result through another key: it moved
    /// between two keys of the query, or it is filed under both.
    Bucket(Arc<crate::store::EntityStore>),
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
    scope: &LiveDiffScope,
) {
    match diff {
        MapDiff::Initial { entries } => match scope {
            LiveDiffScope::Bucket(_) => apply_bucket_candidates(result, filter, entries.clone()),
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
            // The filter pins the route's keys, so a row that still matches
            // it in the store is still in another of the query's buckets.
            let current = match scope {
                LiveDiffScope::Bucket(store) => store
                    .get_value(key)
                    .filter(|item| live_filter_matches(filter, item)),
                LiveDiffScope::FullStore => None,
            };
            if let Some(item) = current {
                result.insert(key.clone(), item);
            } else {
                result.remove(key);
            }
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
    source_override: Option<FilteredCellMap>,
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
    // subscribe_diffs can synchronously replay queued writes before returning.
    reconciliation_gate: parking_lot::ReentrantMutex<()>,
}

impl<F> Default for LiveQueryState<F> {
    fn default() -> Self {
        Self {
            bucket_guards: HashMap::new(),
            bucket_filter_refs: HashMap::new(),
            scan_guard: None,
            source_override: None,
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
    query_live_in_context(
        registry,
        Arc::new(crate::request::RequestContext::internal(
            Uuid::new_v4().to_string().into(),
            host_id,
            "query",
        )),
        filter_cell,
    )
}

/// Build a live query while preserving the originating request and transaction.
pub fn query_live_in_context<F>(
    registry: Arc<StoreRegistry>,
    request: Arc<crate::request::RequestContext>,
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
        let _reconciliation = synchronization.reconciliation_gate.lock();
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current_generation = synchronization
            .generation
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        reconcile_live_query(
            &registry,
            &request,
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
    request: &Arc<crate::request::RequestContext>,
    result: &AnyItemMap,
    state: &mut LiveQueryState<F>,
    new_filter: &F,
    current_generation: u64,
    synchronization: &Arc<LiveQuerySynchronization>,
) {
    if reconcile_registered_live_source(
        registry,
        request,
        result,
        state,
        new_filter,
        current_generation,
        synchronization,
    ) {
        return;
    }
    if let Some(route) = new_filter.query_route() {
        reconcile_indexed_live_query(
            registry,
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

fn reconcile_registered_live_source<F: LiveFilterQuery>(
    registry: &Arc<StoreRegistry>,
    request: &Arc<crate::request::RequestContext>,
    result: &AnyItemMap,
    state: &mut LiveQueryState<F>,
    filter: &F,
    generation: u64,
    synchronization: &Arc<LiveQuerySynchronization>,
) -> bool {
    if !super::has_entity_source(F::entity_type()) {
        return false;
    }
    let context = super::QueryBuildContext::new(
        Arc::new(super::QueryContext {
            req: request.clone(),
        }),
        registry.clone(),
        None,
    );
    let Some(source) = super::registered_entity_source(&context, F::entity_type(), filter) else {
        return false;
    };
    state.scan_guard = None;
    state.source_override = None;
    state.bucket_guards.clear();
    state.bucket_filter_refs.clear();
    state.prev_route_field_names = None;
    reconcile_full_scope_membership(result, filter, source.snapshot());
    let filter_state = Arc::new(Mutex::new(LiveFilterGeneration {
        generation,
        filter: filter.clone(),
    }));
    state.scan_filter_ref = Some(filter_state.clone());
    state.scan_guard = Some(source.subscribe_diffs(build_live_diff_callback(
        result,
        filter_state,
        synchronization,
        LiveDiffScope::FullStore,
    )));
    state.source_override = Some(source);
    true
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
                apply_live_diff(&result, diff, &filter, &scope);
            }
            return;
        }
        let _reconciliation = synchronization.reconciliation_gate.lock();
        let Some(filter) = current_live_filter(&filter_state, &synchronization) else {
            return;
        };
        apply_live_diff(&result, diff, &filter, &scope);
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
    Option<KeyExtractor>,
) {
    match route {
        QueryRoute::Ids(ids) => (
            ID_ROUTE_FIELD_NAMES,
            ids.into_iter()
                .map(|id| CompoundKey::from_iter([id]))
                .collect(),
            None,
        ),
        QueryRoute::BelongsTo(route) => {
            let extractor = route.extractor();
            (
                route.field_names,
                route.keys.into_iter().collect(),
                Some(extractor),
            )
        }
    }
}

fn build_live_bucket_source<F: LiveFilterQuery>(
    registry: &Arc<StoreRegistry>,
    route_field_names: &'static [&'static str],
    extract_fk: Option<KeyExtractor>,
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
                F::entity_type(),
                route_field_names,
                extract_fk,
            );
            let make: BucketSourceFn =
                Box::new(move |key: &CompoundKey| index.bucket_for(key.clone()).lock());
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
    (make_source, store): (&BucketSourceFn, &Arc<crate::store::EntityStore>),
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
            LiveDiffScope::Bucket(store.clone()),
        ));
        state.bucket_guards.insert(key.clone(), (bucket, guard));
        state.bucket_filter_refs.insert(key.clone(), filter_state);
    }
}

fn reconcile_indexed_live_query<F: LiveFilterQuery>(
    registry: &Arc<StoreRegistry>,
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
        let make_source = build_live_bucket_source::<F>(registry, route_field_names, extract_fk);
        add_live_buckets(
            result,
            state,
            new_filter,
            &new_keys,
            current_generation,
            (&make_source, &registry.get_or_create(F::entity_type())),
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

/// Sweep dead buckets of every belongs-to source index `registry` holds.
///
/// `route_to_live_bucket` reaps dead entries lazily
/// on next access, but a foreign id that goes dead and is never looked up
/// again would otherwise linger; called from
/// `MykoServerContext::sweep_dead_cache_entries`.
pub fn sweep_belongs_to_source_indexes(registry: &StoreRegistry) {
    for index in registry.indexes_of::<BelongsToSourceIndex>() {
        index.sweep_dead_buckets();
    }
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
    /// Crate where this query is defined (for `type_gen` filtering)
    pub crate_name: &'static str,
    /// Parse function for deserializing query from JSON
    pub parse: QueryParseFn,
    /// Factory for creating reactive cell from query
    pub cell_factory: QueryCellFactory,
    /// Factory for an optional source-level bounded query window.
    pub window_cell_factory: QueryWindowCellFactory,
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
    fn parse(value: Value) -> Result<Arc<dyn AnyQuery>, anyhow::Error> {
        let query = crate::reflection::deserialize_operation::<QueryRequest<Q>>(value)?;
        Ok(Arc::new(query))
    }

    fn cell_factory(
        any_query: Arc<dyn AnyQuery>,
        registry: Arc<StoreRegistry>,
        request_ctx: Arc<RequestContext>,
        server_ctx: Option<Arc<MykoServerContext>>,
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
        let query_cell_ctx =
            QueryBuildContext::new(query_ctx.clone(), registry.clone(), server_ctx);

        if let Some(built) = Q::build_view(QueryBuildArgs {
            query: query.clone(),
            query_context: query_cell_ctx,
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
    use std::any::Any;

    use hyphae::{Gettable, Materialize};
    use serde::Serialize;

    use super::*;
    use crate::common::with_id::WithId;

    static MEMBERSHIP_EXTRACTIONS: AtomicU64 = AtomicU64::new(0);

    fn counted_parent_fk(item: &dyn Any) -> Option<CompoundKey> {
        MEMBERSHIP_EXTRACTIONS.fetch_add(1, Ordering::Relaxed);
        extract_parent_fk(item)
    }

    #[test]
    fn bucket_backfill_work_is_local_after_one_membership_bootstrap() {
        for size in [10, 1000] {
            MEMBERSHIP_EXTRACTIONS.store(0, Ordering::Relaxed);
            let store = new_store();
            store.apply_diff_owned(MapDiff::Initial {
                entries: (0..size)
                    .map(|i| child(&format!("child-{i}"), &format!("parent-{i}")))
                    .collect(),
            });
            let index = BelongsToSourceIndex::new(store, KeyExtractor::One(counted_parent_fk));
            assert_eq!(MEMBERSHIP_EXTRACTIONS.load(Ordering::Relaxed), size);
            for i in 0..size {
                let key = smallvec::smallvec![Arc::from(format!("parent-{i}"))];
                let bucket = index.bucket_for(key.clone());
                assert_eq!(bucket.snapshot().len(), 1);
                drop(bucket);
                index.sweep_dead_buckets();
                assert_eq!(index.bucket_for(key).snapshot().len(), 1);
            }
            assert_eq!(MEMBERSHIP_EXTRACTIONS.load(Ordering::Relaxed), size);
        }
    }

    #[test]
    fn unwatched_membership_tracks_moves_removals_and_initial_reset() {
        let store = new_store();
        let index = BelongsToSourceIndex::new(store.clone(), KeyExtractor::One(extract_parent_fk));
        let (id, old) = child("child", "old");
        store.insert(id.clone(), old.clone());
        let (_, new) = child("child", "new");
        store.insert(id.clone(), new);
        let old_bucket = index.bucket_for(smallvec::smallvec![Arc::from("old")]);
        assert!(old_bucket.snapshot().is_empty());
        let new_key = smallvec::smallvec![Arc::from("new")];
        let bucket = index.bucket_for(new_key.clone());
        assert_eq!(bucket.snapshot().len(), 1);
        drop(bucket);
        index.sweep_dead_buckets();
        store.remove(&id);
        assert!(index.bucket_for(new_key).snapshot().is_empty());
        store.apply_diff_owned(MapDiff::Initial {
            entries: vec![child("reset", "old")],
        });
        assert_eq!(old_bucket.snapshot().len(), 1);
        store.apply_diff_owned(MapDiff::Initial {
            entries: Vec::new(),
        });
        assert!(old_bucket.snapshot().is_empty());
        assert!(
            index
                .mutation_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .members
                .is_empty()
        );
    }

    #[test]
    fn live_bucket_replays_queued_write_during_reconciliation() {
        let (done, completed) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            use crate::entities::client::{Client, ClientQuery};
            let make_client = |id: &str| -> AnyItemArc {
                Arc::new(Client {
                    id: id.into(),
                    server_id: "server".into(),
                    address: None,
                    windback: None,
                })
            };
            let bucket = AnyItemMap::new();
            bucket.insert("first".into(), make_client("first"));
            let result = AnyItemMap::new();
            let write_bucket = bucket.clone();
            let second = make_client("second");
            let written = AtomicBool::new(false);
            let _write_guard = result.subscribe_diffs(move |diff| {
                if matches!(diff, MapDiff::Insert { .. }) && !written.swap(true, Ordering::SeqCst) {
                    write_bucket.insert("second".into(), second.clone());
                }
            });
            let synchronization = Arc::new(LiveQuerySynchronization::default());
            let filter = Arc::new(Mutex::new(LiveFilterGeneration {
                generation: 0,
                filter: ClientQuery::default(),
            }));
            let _reconciliation = synchronization.reconciliation_gate.lock();
            let _guard = bucket.subscribe_diffs(build_live_diff_callback(
                &result,
                filter,
                &synchronization,
                LiveDiffScope::Bucket(Arc::new(crate::store::EntityStore::new())),
            ));
            assert_eq!(result.snapshot().len(), 2);
            assert!(done.send(()).is_ok());
        });
        assert!(
            completed
                .recv_timeout(std::time::Duration::from_secs(3))
                .is_ok(),
            "queued subscription replay must not lock its own reconciliation gate"
        );
    }

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

    #[test]
    fn dropped_subscriptions_do_not_leak_across_many_distinct_parents() {
        // Reproduces the leak: every distinct foreign id ever subscribed to
        // used to leave a permanent bucket behind. With weak-ref buckets,
        // dropping every subscriber and sweeping must bring the count to 0
        // regardless of how many distinct parents were ever seen.
        let store = new_store();
        let index = BelongsToSourceIndex::new(store, KeyExtractor::One(extract_parent_fk));

        for i in 0..50 {
            let parent: Arc<str> = Arc::from(format!("parent-{i}"));
            let bucket = index.bucket_for(smallvec::smallvec![parent]);
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

        let index = BelongsToSourceIndex::new(store.clone(), KeyExtractor::One(extract_parent_fk));
        let bucket = index.bucket_for(smallvec::smallvec![Arc::from("parent-x")]);
        assert_eq!(bucket.snapshot().len(), 1);

        // Remove the only child — bucket goes empty, but `bucket` is still
        // held here, simulating a live subscriber.
        store.remove_many(vec![id]);
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

        let index = BelongsToSourceIndex::new(store.clone(), KeyExtractor::One(extract_parent_fk));

        {
            let bucket = index.bucket_for(smallvec::smallvec![Arc::from("parent-y")]);
            assert_eq!(bucket.snapshot().len(), 1);
        }
        index.sweep_dead_buckets();
        assert!(index.buckets.is_empty());

        let bucket = index.bucket_for(smallvec::smallvec![Arc::from("parent-y")]);
        assert_eq!(
            bucket.snapshot().len(),
            1,
            "resubscribing after the bucket was reaped must backfill current children, not start empty"
        );
    }

    #[test]
    fn concurrent_bucket_for_calls_for_the_same_key_never_orphan_a_bucket() {
        const N: usize = 16;

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
        let index = Arc::new(BelongsToSourceIndex::new(
            store.clone(),
            KeyExtractor::One(extract_parent_fk),
        ));
        let key: CompoundKey = smallvec::smallvec![Arc::from("parent-race")];

        let barrier = Arc::new(std::sync::Barrier::new(N));
        let mut handles = Vec::with_capacity(N);
        for _ in 0..N {
            let index = index.clone();
            let key = key.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                index.bucket_for(key)
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
        // Exercise the publication/backfill boundary repeatedly. The index
        // mutation gate makes the insert callback fall wholly before or
        // after bucket construction, so either ordering must converge on
        // the inserted child without requiring a later source diff.
        for i in 0..128 {
            let store = new_store();
            let index =
                BelongsToSourceIndex::new(store.clone(), KeyExtractor::One(extract_parent_fk));
            let barrier = Arc::new(std::sync::Barrier::new(2));

            let bucket_thread = {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    index.bucket_for(smallvec::smallvec![Arc::from("parent-race")])
                })
            };
            let insert_thread = {
                let store = store.clone();
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
        let store = new_store();
        let index = BelongsToSourceIndex::new(store.clone(), KeyExtractor::One(extract_parent_fk));
        let bucket = index.bucket_for(smallvec::smallvec![Arc::from("parent-source")]);
        let (sent, received) = std::sync::mpsc::channel();
        let index_for_callback = index.clone();
        let guard = bucket.subscribe_diffs(move |_| {
            // Real query graphs can synchronously construct another routed
            // query while handling this diff. That must not attempt to
            // reacquire an index gate still held by apply_diff.
            let nested =
                index_for_callback.bucket_for(smallvec::smallvec![Arc::from("parent-nested")]);
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
    fn route_snapshot_reads_the_index_only_while_the_scheduler_is_idle() {
        let _serial = crate::test_util::scheduler_test_serial();
        let registry = Arc::new(crate::store::StoreRegistry::new());
        let store = registry.get_or_create("TestChild");
        let (id, item) = child("child-1", "parent-1");
        store.insert(id, item);
        let snapshot = || {
            belongs_to_route_snapshot(
                &registry,
                "TestChild",
                &crate::query::BelongsToRoute {
                    field_names: &["parent_id"],
                    keys: vec![smallvec::smallvec![Arc::from("parent-1")]],
                    extract_fk: extract_parent_fk,
                    extract_each: None,
                },
            )
            .map(|rows| rows.len())
        };

        assert_eq!(snapshot(), Some(1));
        let (id, item) = child("child-2", "parent-1");
        store.insert(id, item);
        assert_eq!(snapshot(), Some(2));

        hyphae::batch(|| {
            let (id, item) = child("child-3", "parent-1");
            store.insert(id, item);
            assert_eq!(
                snapshot(),
                None,
                "the index is behind the store until the batch drains"
            );
        });
        assert_eq!(snapshot(), Some(3));
    }

    #[test]
    fn the_store_registry_keeps_an_index_no_query_is_using() {
        let registry = Arc::new(crate::store::StoreRegistry::new());
        let lookup = || {
            belongs_to_source_index_for(
                &registry,
                "TestChild",
                &["parent_id"],
                KeyExtractor::One(extract_parent_fk),
            )
        };
        let index = lookup();
        let weak = Arc::downgrade(&index);
        let bucket = index.bucket_for(smallvec::smallvec![Arc::from("parent-live")]);
        drop(bucket);
        drop(index);

        let again = lookup();
        assert!(
            weak.upgrade()
                .is_some_and(|first| Arc::ptr_eq(&first, &again)),
            "the index was rebuilt once its last bucket was dropped"
        );
        drop(again);

        drop(registry);
        assert!(
            weak.upgrade().is_none(),
            "the index outlived the registry that owns its store"
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

        let index = BelongsToSourceIndex::new(
            store.clone(),
            KeyExtractor::One(extract_node_and_session_fk),
        );

        let key_a: CompoundKey =
            smallvec::smallvec![Arc::from("node-A"), Arc::from("session-PROD")];
        let key_b: CompoundKey =
            smallvec::smallvec![Arc::from("node-B"), Arc::from("session-PROD")];

        let bucket_a = index.bucket_for(key_a);
        let bucket_b = index.bucket_for(key_b);

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
        let index =
            BelongsToSourceIndex::new(store, KeyExtractor::One(extract_node_and_session_fk));
        let key: CompoundKey = smallvec::smallvec![Arc::from("node-A"), Arc::from("session-PROD")];
        let bucket = index.bucket_for(key.clone());
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
                CompoundKey::from_iter([Arc::<str>::from("node-A"), Arc::from("session-PROD")]),
                CompoundKey::from_iter([Arc::<str>::from("node-B"), Arc::from("session-PROD")]),
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

        let keys = vec![
            smallvec::smallvec![Arc::from("node-A"), Arc::from("session-PROD")],
            smallvec::smallvec![Arc::from("node-B"), Arc::from("session-PROD")],
        ];
        let union = build_belongs_to_union_source_map(
            registry.clone(),
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
    fn a_row_moving_between_two_keys_of_one_union_stays_in_it() {
        // Whether the old key's remove or the new key's insert reaches the
        // union first depends on hash order, so try many key pairs and rows.
        for pair in 0..40 {
            let registry = Arc::new(crate::store::StoreRegistry::new());
            let store = registry.get_or_create("TestChild");
            let (from, to): (Arc<str>, Arc<str>) = (
                format!("parent-{pair}-a").into(),
                format!("parent-{pair}-b").into(),
            );
            for row in 0..4 {
                let (id, item) = child(&format!("child-{row}"), &from);
                store.insert(id, item);
            }
            let union = build_belongs_to_union_source_map(
                registry.clone(),
                "TestChild",
                &["parent_id"],
                extract_parent_fk,
                vec![
                    smallvec::smallvec![from.clone()],
                    smallvec::smallvec![to.clone()],
                ],
            );
            for (round, parent) in [&to, &from, &to].into_iter().enumerate() {
                for row in 0..4 {
                    let (id, item) = child(&format!("child-{row}"), parent);
                    store.insert(id, item);
                }
                assert_eq!(
                    union.snapshot().len(),
                    4,
                    "pair {pair} round {round}: a row that moved between two keys left the union"
                );
            }
        }
    }

    #[test]
    fn union_source_map_empty_keys_yields_empty_reactive_map() {
        let registry = Arc::new(crate::store::StoreRegistry::new());
        let store = registry.get_or_create("TestCursor");
        let (id, item) = cursor("cursor-a", "node-A", "session-PROD");
        store.insert(id, item);

        let union = build_belongs_to_union_source_map(
            registry,
            "TestCursor",
            &["node_id", "session_id"],
            extract_node_and_session_fk,
            Vec::new(),
        );
        assert_eq!(union.snapshot().len(), 0);
    }
}
