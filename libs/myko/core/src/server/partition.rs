//! Partitioned runtimes: several independent live runtimes in one server.
//!
//! # Model
//!
//! An application declares, in a [`PartitionSchema`], which entity types are
//! partitioned and how an item of each maps to a [`PartitionKey`]. Every other
//! type is shared.
//!
//! Shared data lives in the root scheduling domain. Each open partition owns a
//! child domain, and the store of a partitioned type exists once per
//! partition, inside that partition's domain. A partition reads shared data;
//! nothing reads a partition from root or from another partition. Hyphae
//! rejects such a dependency when it is built.
//!
//! [`MykoServerContext::partition`] returns the context of one partition. It
//! resolves a partitioned type to that partition's store and a shared type to
//! the root store, builds every query, view and report inside the partition's
//! domain, and caches them in caches of its own. Two partitions therefore
//! never share a cache entry or a compute gate.
//!
//! # Writes
//!
//! A write through any context is routed by its item:
//!
//! - a shared item is applied in root, as it is without partitions;
//! - an item of a type declared with [`PartitionSchema::partition_by`] goes to
//!   the partition its key names, and is rejected when that partition is not
//!   open;
//! - an item of a type declared with [`PartitionSchema::route`] goes to the
//!   partition the application's resolver names. The resolver reads shared
//!   data. A row whose resolver names no open partition is held outside every
//!   domain, and enters a partition when one opens or the resolver's answer
//!   changes.
//!
//! A partition write runs on the writing thread, on the partition's own tick.
//! It takes no root lock and waits on no other partition.
//!
//! A call that writes shared and partition items applies the shared items
//! first. Its partition items then follow the shared write across the domain
//! boundary, so the partition applies them after it has applied that shared
//! write. A later write to the same partition waits until they are applied,
//! which keeps the partition's writes in the order they were made.
//!
//! Data flows from shared into partitions only. A write made while a
//! partition's wave runs is refused when it would land in shared data or in
//! another partition. A write made while root's wave runs reaches a
//! partition behind that wave.
//!
//! # Reads
//!
//! A reactive read of a partitioned type built on the root context sees no
//! rows. [`MykoServerContext::checked`] refuses such a build, and
//! [`MykoServerContext::subscription_context`] opens a client subscription
//! in the partition the application's hook names.
//!
//! # Re-routing
//!
//! Each routed type keeps the placement of every row it has seen. A write to
//! one of the types the resolver reads re-resolves each placed row and moves
//! the rows whose partition changed. A move and a write of the same row
//! exclude each other, so a row is in exactly one place at any instant and a
//! move carries the latest write.

use std::{
    cell::Cell as ThreadCell,
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use hyphae::{Cell, CellMutable, Mutable, SubscriptionGuard, Watchable, scheduler::Domain};
use parking_lot::{Mutex, MutexGuard, RwLock};

use super::{MykoServerContext, OperationProvenance, context::Origin, persister::PersistError};
use crate::{
    core::item::{AnyItem, Eventable},
    graph::EdgeApplyMode,
    request::RequestContext,
    store::StoreRegistry,
    wire::MEventType,
};

type Item = Arc<dyn AnyItem>;
type KeyOf = Arc<dyn Fn(&dyn AnyItem) -> Option<PartitionKey> + Send + Sync>;
type SubscriptionPartition =
    Arc<dyn Fn(&RequestContext, &serde_json::Value) -> Option<PartitionKey> + Send + Sync>;
type Resolve = Arc<dyn Fn(&MykoServerContext, &dyn AnyItem) -> Option<PartitionKey> + Send + Sync>;

/// Names one partition.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionKey(Arc<str>);

impl PartitionKey {
    #[must_use]
    pub fn new(key: impl Into<Arc<str>>) -> Self {
        Self(key.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a read could not be built where it was asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionError {
    /// A reactive read of a partitioned entity type was built on the root
    /// context, where the type has no rows.
    RootRead { entity_type: Arc<str> },
    /// The partition named for a read is not open.
    NotOpen(PartitionKey),
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RootRead { entity_type } => write!(
                f,
                "{entity_type} is partitioned: read it from the context of its partition, not \
                 from the root context"
            ),
            Self::NotOpen(key) => write!(f, "partition {key} is not open"),
        }
    }
}

impl std::error::Error for PartitionError {}

/// Registered by `#[myko_partition_key]` on a field of an item: the entity
/// type is partitioned by that field.
pub struct PartitionKeyRegistration {
    pub entity_type: &'static str,
    pub key_of: fn(&dyn AnyItem) -> Option<PartitionKey>,
}

inventory::collect!(PartitionKeyRegistration);

/// A field type that can name a partition.
pub trait PartitionKeyField {
    fn partition_key(&self) -> Option<PartitionKey>;
}

impl PartitionKeyField for Arc<str> {
    fn partition_key(&self) -> Option<PartitionKey> {
        Some(PartitionKey(Self::clone(self)))
    }
}

impl PartitionKeyField for String {
    fn partition_key(&self) -> Option<PartitionKey> {
        Some(PartitionKey::new(self.as_str()))
    }
}

impl<T: PartitionKeyField> PartitionKeyField for Option<T> {
    fn partition_key(&self) -> Option<PartitionKey> {
        self.as_ref()?.partition_key()
    }
}

enum Route {
    /// The item names its partition.
    Keyed(KeyOf),
    /// The application resolves the partition from shared data.
    Resolved {
        resolve: Resolve,
        /// Shared entity types the resolver reads.
        inputs: Vec<&'static str>,
    },
}

/// Which entity types are partitioned, and how an item finds its partition.
pub struct PartitionSchema {
    routes: HashMap<&'static str, Route>,
    lifecycle: Option<&'static str>,
    subscriptions: Option<SubscriptionPartition>,
}

fn typed<T: Eventable, R>(
    read: impl Fn(&T) -> Option<R> + Send + Sync + 'static,
) -> impl Fn(&dyn AnyItem) -> Option<R> + Send + Sync + 'static {
    move |item| item.as_any().downcast_ref::<T>().and_then(&read)
}

impl Default for PartitionSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionSchema {
    /// A schema that partitions every entity type with a
    /// `#[myko_partition_key]` field by that field.
    #[must_use]
    pub fn new() -> Self {
        let routes = inventory::iter::<PartitionKeyRegistration>()
            .map(|registration| {
                let key_of = registration.key_of;
                let route = Route::Keyed(Arc::new(move |item: &dyn AnyItem| key_of(item)));
                (registration.entity_type, route)
            })
            .collect();
        Self {
            routes,
            lifecycle: None,
            subscriptions: None,
        }
    }

    /// Partition `T` by a key each item carries.
    #[must_use]
    pub fn partition_by<T: Eventable>(
        mut self,
        key: impl Fn(&T) -> PartitionKey + Send + Sync + 'static,
    ) -> Self {
        let key_of = typed::<T, _>(move |item| Some(key(item)));
        self.routes
            .insert(T::ENTITY_NAME_STATIC, Route::Keyed(Arc::new(key_of)));
        self
    }

    /// Partition `T` by a resolver, for a type keyed by something other than
    /// its partition.
    ///
    /// `resolve` receives the root context and reads shared data from it with
    /// one-shot reads. `inputs` names the shared entity types it reads: a
    /// write to one of them re-routes the rows of `T`.
    #[must_use]
    pub fn route<T: Eventable>(
        mut self,
        inputs: &[&'static str],
        resolve: impl Fn(&MykoServerContext, &T) -> Option<PartitionKey> + Send + Sync + 'static,
    ) -> Self {
        let resolve: Resolve = Arc::new(move |root, item| {
            item.as_any()
                .downcast_ref::<T>()
                .and_then(|item| resolve(root, item))
        });
        self.routes.insert(
            T::ENTITY_NAME_STATIC,
            Route::Resolved {
                resolve,
                inputs: inputs.to_vec(),
            },
        );
        self
    }

    /// Tie partitions to the shared entity type `T`: setting an item opens the
    /// partition named by its id, and deleting it closes that partition.
    #[must_use]
    pub const fn lifecycle<T: Eventable>(mut self) -> Self {
        self.lifecycle = Some(T::ENTITY_NAME_STATIC);
        self
    }

    /// Name the partition a client's subscription opens in, from the
    /// request and the subscription's payload. A subscription for which it
    /// names none opens in root, where a read of a partitioned type is
    /// refused.
    #[must_use]
    pub fn subscribe_in(
        mut self,
        partition: impl Fn(&RequestContext, &serde_json::Value) -> Option<PartitionKey>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.subscriptions = Some(Arc::new(partition));
        self
    }

    fn is_partitioned(&self, entity_type: &str) -> bool {
        self.routes.contains_key(entity_type)
    }
}

/// Where a routed row currently is.
#[derive(Clone)]
enum Placement {
    /// No open partition takes the row. It is kept here, outside every domain.
    Held(Item),
    In(PartitionKey),
}

const STRIPES: u64 = 64;

/// Placement of every row of one routed type.
struct Placements {
    resolve: Resolve,
    /// A write of a row and a move of that row hold the row's stripe.
    stripes: Vec<Mutex<()>>,
    hasher: ahash::RandomState,
    rows: DashMap<Arc<str>, Placement, ahash::RandomState>,
}

impl Placements {
    fn new(resolve: Resolve) -> Self {
        Self {
            resolve,
            stripes: (0..STRIPES).map(|_| Mutex::new(())).collect(),
            hasher: ahash::RandomState::new(),
            rows: DashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    /// Lock the stripes of `ids`, in one global order.
    fn lock<'a>(&self, ids: impl IntoIterator<Item = &'a Arc<str>>) -> Vec<MutexGuard<'_, ()>> {
        let stripes: BTreeSet<usize> = ids
            .into_iter()
            .map(|id| usize::try_from(self.hasher.hash_one(id) % STRIPES).unwrap_or(0))
            .collect();
        stripes
            .into_iter()
            .filter_map(|stripe| self.stripes.get(stripe))
            .map(Mutex::lock)
            .collect()
    }
}

type Job = Box<dyn FnOnce() + Send>;

/// One open partition.
pub(crate) struct Partition {
    key: PartitionKey,
    domain: Domain,
    ctx: MykoServerContext,
    /// `true` while the partition takes writes. A write holds it shared; the
    /// close takes it exclusively, so it sees every admitted write.
    admitting: RwLock<bool>,
    /// Writes that follow a shared write, until root hands them over.
    waiting: Arc<Mutex<VecDeque<Job>>>,
    /// A root cell. Writing it makes root hand the waiting writes to this
    /// partition after every delivery root produced before the write.
    courier: Cell<u64, CellMutable>,
    /// How many writes have been sent after a shared write, and how many of
    /// them this partition is known to have applied.
    sent: AtomicU64,
    applied: AtomicU64,
    _relay: SubscriptionGuard,
}

impl Partition {
    fn open(partitions: &Arc<Partitions>, key: PartitionKey) -> Arc<Self> {
        let domain = Domain::child();
        let schema = Arc::clone(&partitions.schema);
        let registry = Arc::new(StoreRegistry::for_partition(
            Arc::clone(&partitions.root.registry),
            domain.clone(),
            Arc::new(move |entity_type| schema.is_partitioned(entity_type)),
        ));
        let ctx = partitions
            .root
            .for_partition(registry, key.clone(), domain.clone());
        let waiting: Arc<Mutex<VecDeque<Job>>> = Arc::default();
        let (courier, relay) = Domain::root().enter(|| {
            let courier = Cell::new(0_u64);
            let waiting = Arc::clone(&waiting);
            let domain = domain.clone();
            let relay = courier.subscribe(move |_| {
                let writes = std::mem::take(&mut *waiting.lock());
                for write in writes {
                    domain.hand_off(write);
                }
            });
            (courier, relay)
        });
        Arc::new(Self {
            key,
            domain,
            ctx,
            admitting: RwLock::new(true),
            waiting,
            courier,
            sent: AtomicU64::new(0),
            applied: AtomicU64::new(0),
            _relay: relay,
        })
    }

    pub(crate) const fn ctx(&self) -> &MykoServerContext {
        &self.ctx
    }

    fn closed(&self, entity_type: &str) -> PersistError {
        PersistError {
            entity_type: Arc::from(entity_type),
            message: format!("partition {} is closed", self.key),
        }
    }

    /// Apply a write on the calling thread, on this partition's tick.
    fn write(
        &self,
        items: Vec<Item>,
        change: MEventType,
        origin: Origin,
        mode: EdgeApplyMode,
    ) -> Result<(), PersistError> {
        // Held until the write is applied: a close waits for it.
        let admitting = self.admitting.read_recursive();
        if !*admitting {
            drop(admitting);
            return Err(self.closed(items.first().map_or("mutation", |item| item.entity_type())));
        }
        // A write that followed a shared write may still be on its way here.
        // This one was made later, so it waits for that one to be applied. A
        // write made from this partition's own wave is already in order.
        let sent = self.sent.load(Ordering::Acquire);
        if self.applied.load(Ordering::Acquire) < sent && Domain::current() != self.domain {
            self.domain.flush();
            self.applied.fetch_max(sent, Ordering::AcqRel);
        }
        let result = self
            .domain
            .enter(|| self.ctx.submit_local(items, change, origin, mode));
        drop(admitting);
        result
    }

    /// Apply a write after the shared write the calling thread just made, on
    /// this partition's worker. The result is not returned: a failure is
    /// logged.
    fn write_following(
        self: &Arc<Self>,
        items: Vec<Item>,
        change: MEventType,
        origin: Origin,
        mode: EdgeApplyMode,
        provenance: Option<OperationProvenance>,
    ) {
        let partition = Arc::clone(self);
        let run: Job = Box::new(move || {
            let admitting = partition.admitting.read_recursive();
            if *admitting {
                let _operation = partition.ctx.restore_operation(provenance);
                let result = partition
                    .domain
                    .enter(|| partition.ctx.submit_local(items, change, origin, mode));
                if let Err(error) = result {
                    tracing::error!(partition = %partition.key, %error, "partition write failed");
                }
            } else {
                tracing::error!(
                    partition = %partition.key,
                    "a write that followed a shared write found its partition closed"
                );
            }
            drop(admitting);
        });
        self.waiting.lock().push_back(run);
        let sent = self.sent.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        hyphae::batch(|| self.courier.set(sent));
    }

    fn store(&self, entity_type: &str) -> Arc<crate::store::EntityStore> {
        self.ctx.registry.get_or_create(entity_type)
    }

    /// Remove a row because it now belongs elsewhere. Not an entity deletion:
    /// nothing is persisted and no relationship runs.
    fn take(&self, entity_type: &str, id: &Arc<str>) -> Option<Item> {
        let store = self.store(entity_type);
        self.domain.batch(|| store.remove(id))
    }

    /// Insert a row that was moved here.
    fn place(&self, entity_type: &str, row: Item) {
        let store = self.store(entity_type);
        self.domain.batch(|| drop(store.insert(row.id(), row)));
    }
}

thread_local! {
    /// Depth of the shared writes the thread is inside.
    static SHARED_WRITES: ThreadCell<usize> = const { ThreadCell::new(0) };
}

struct SharedWrite;

impl SharedWrite {
    fn begin() -> Self {
        SHARED_WRITES.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }

    fn active() -> bool {
        SHARED_WRITES.with(|depth| depth.get() > 0)
    }
}

impl Drop for SharedWrite {
    fn drop(&mut self) {
        SHARED_WRITES.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// A write from a partition's wave to data outside that partition: shared
/// data when `to` is `None`, another partition's otherwise.
pub(crate) fn backwards(entity_type: &str, to: Option<&PartitionKey>) -> PersistError {
    let target = to.map_or_else(
        || "shared data".to_owned(),
        |key| format!("partition {key}"),
    );
    PersistError {
        entity_type: Arc::from(entity_type),
        message: format!(
            "a partition's wave cannot write {target}: data flows from shared data into \
             partitions, never back or across"
        ),
    }
}

/// The partitions of one server.
pub(crate) struct Partitions {
    schema: Arc<PartitionSchema>,
    /// The root context. It names these partitions weakly.
    root: MykoServerContext,
    open: DashMap<PartitionKey, Arc<Partition>, ahash::RandomState>,
    placements: HashMap<&'static str, Placements>,
    /// Serializes opening, closing and re-routing.
    topology: Mutex<()>,
}

impl Partitions {
    pub(crate) fn new(schema: PartitionSchema, root: &MykoServerContext) -> Arc<Self> {
        let placements = schema
            .routes
            .iter()
            .filter_map(|(entity_type, route)| match route {
                Route::Resolved { resolve, .. } => {
                    Some((*entity_type, Placements::new(Arc::clone(resolve))))
                }
                Route::Keyed(_) => None,
            })
            .collect();
        let schema = Arc::new(schema);
        let declared = Arc::clone(&schema);
        root.registry
            .declare_partitioned(Arc::new(move |entity_type| {
                declared.is_partitioned(entity_type)
            }));
        Arc::new_cyclic(|partitions: &Weak<Self>| Self {
            schema,
            root: root.as_partition_member(partitions.clone()),
            open: DashMap::with_hasher(ahash::RandomState::new()),
            placements,
            topology: Mutex::new(()),
        })
    }

    pub(crate) fn is_partitioned(&self, entity_type: &str) -> bool {
        self.schema.is_partitioned(entity_type)
    }

    pub(crate) fn subscription_partition(
        &self,
        request: &RequestContext,
        payload: &serde_json::Value,
    ) -> Option<PartitionKey> {
        self.schema
            .subscriptions
            .as_ref()
            .and_then(|partition| partition(request, payload))
    }

    pub(crate) fn get(&self, key: &PartitionKey) -> Option<Arc<Partition>> {
        self.open.get(key).map(|partition| Arc::clone(&partition))
    }

    pub(crate) fn keys(&self) -> Vec<PartitionKey> {
        self.open.iter().map(|entry| entry.key().clone()).collect()
    }

    /// The partition a routed row is in, if it is in one.
    pub(crate) fn placement(&self, entity_type: &str, id: &Arc<str>) -> Option<PartitionKey> {
        match self.placements.get(entity_type)?.rows.get(id)?.value() {
            Placement::In(key) => Some(key.clone()),
            Placement::Held(_) => None,
        }
    }

    /// The rows of a partitioned type in every open partition, read now.
    pub(crate) fn snapshot(&self, entity_type: &str) -> Vec<(PartitionKey, Vec<Item>)> {
        let partitions: Vec<Arc<Partition>> = self
            .open
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        partitions
            .into_iter()
            .map(|partition| {
                let rows = partition
                    .store(entity_type)
                    .snapshot()
                    .into_iter()
                    .map(|(_, row)| row)
                    .collect();
                (partition.key.clone(), rows)
            })
            .collect()
    }

    pub(crate) fn open(self: &Arc<Self>, key: PartitionKey) -> Arc<Partition> {
        let _topology = self.topology.lock();
        if let Some(partition) = self.get(&key) {
            return partition;
        }
        let partition = Partition::open(self, key.clone());
        self.open.insert(key, Arc::clone(&partition));
        for entity_type in self.placements.keys() {
            self.reroute(entity_type, None);
        }
        partition
    }

    pub(crate) fn close(&self, key: &PartitionKey) {
        let _topology = self.topology.lock();
        let Some((_, partition)) = self.open.remove(key) else {
            return;
        };
        *partition.admitting.write() = false;
        for entity_type in self.placements.keys() {
            self.reroute(entity_type, Some(&partition));
        }
        partition.domain.close();
        // Releasing a partition's graphs is not the closing thread's work.
        crate::core::off_wave::off_wave(move || drop(partition));
    }

    fn no_partition(entity_type: &str, key: Option<&PartitionKey>) -> PersistError {
        PersistError {
            entity_type: Arc::from(entity_type),
            message: key.map_or_else(
                || "the item names no partition".to_owned(),
                |key| format!("partition {key} is not open"),
            ),
        }
    }

    /// Route one write.
    pub(crate) fn submit(
        &self,
        from: &MykoServerContext,
        items: Vec<Item>,
        change: MEventType,
        origin: Origin,
        mode: EdgeApplyMode,
    ) -> Result<(), PersistError> {
        let mut shared = Vec::new();
        let mut keyed: BTreeMap<PartitionKey, (Arc<Partition>, Vec<Item>)> = BTreeMap::new();
        let mut resolved: BTreeMap<&'static str, Vec<Item>> = BTreeMap::new();
        for item in items {
            match self.schema.routes.get(item.entity_type()) {
                None => shared.push(item),
                Some(Route::Keyed(key_of)) => {
                    let key = key_of(item.as_ref())
                        .ok_or_else(|| Self::no_partition(item.entity_type(), None))?;
                    if let Some((_, items)) = keyed.get_mut(&key) {
                        items.push(item);
                    } else {
                        let partition = self
                            .get(&key)
                            .ok_or_else(|| Self::no_partition(item.entity_type(), Some(&key)))?;
                        keyed.insert(key, (partition, vec![item]));
                    }
                }
                Some(Route::Resolved { .. }) => {
                    resolved.entry(item.entity_type()).or_default().push(item);
                }
            }
        }

        // Data flows from shared into partitions, never back. A partition's
        // wave writes only its own partition.
        let propagating = Domain::propagating();
        if let Some(wave) = propagating.as_ref().filter(|domain| !domain.is_root()) {
            if let Some(item) = shared.first() {
                return Err(backwards(item.entity_type(), None));
            }
            if let Some((_, (partition, items))) = keyed
                .iter()
                .find(|(_, (partition, _))| partition.domain != *wave)
            {
                let entity_type = items.first().map_or("mutation", |item| item.entity_type());
                return Err(backwards(entity_type, Some(&partition.key)));
            }
            for (entity_type, items) in &resolved {
                self.refuse_resolved_outside(entity_type, items, wave)?;
            }
        }
        let root_wave = propagating.as_ref().is_some_and(Domain::is_root);
        if root_wave && !resolved.is_empty() {
            // NOTE(ts): a routed write from root's wave would have to cross
            // into the partition behind the wave, and the placement a move
            // takes must cover that write. Neither exists yet.
            let entity_type = resolved.keys().next().copied().unwrap_or("mutation");
            return Err(PersistError {
                entity_type: Arc::from(entity_type),
                message: "a routed partitioned type cannot be written from a shared wave yet"
                    .into(),
            });
        }

        // Shared data first: a partition applies the shared part of a write
        // before the partition part. A write made from root's wave reaches a
        // partition the same way, after the wave's own deliveries.
        let follows_shared = !shared.is_empty() || SharedWrite::active() || root_wave;
        if !shared.is_empty() {
            let _shared = SharedWrite::begin();
            self.root.submit_local(shared, change, origin, mode)?;
        }
        for (_, (partition, items)) in keyed {
            if follows_shared && Domain::current() != partition.domain {
                partition.write_following(items, change, origin, mode, from.current_provenance());
            } else {
                partition.write(items, change, origin, mode)?;
            }
        }
        for (entity_type, items) in resolved {
            self.submit_resolved(entity_type, items, change, origin, mode)?;
        }
        Ok(())
    }

    /// Refuse routed rows that a partition's wave writes into another
    /// partition, or that no open partition takes.
    fn refuse_resolved_outside(
        &self,
        entity_type: &'static str,
        items: &[Item],
        wave: &Domain,
    ) -> Result<(), PersistError> {
        let Some(placements) = self.placements.get(entity_type) else {
            return Ok(());
        };
        for item in items {
            let key = (placements.resolve)(&self.root, item.as_ref());
            let partition = key.as_ref().and_then(|key| self.get(key));
            if partition.is_none_or(|partition| partition.domain != *wave) {
                return Err(backwards(entity_type, key.as_ref()));
            }
        }
        Ok(())
    }

    fn submit_resolved(
        &self,
        entity_type: &'static str,
        items: Vec<Item>,
        change: MEventType,
        origin: Origin,
        mode: EdgeApplyMode,
    ) -> Result<(), PersistError> {
        let Some(placements) = self.placements.get(entity_type) else {
            return Ok(());
        };
        let ids: Vec<Arc<str>> = items.iter().map(|item| item.id()).collect();
        let _rows = placements.lock(&ids);
        let mut by_partition: BTreeMap<PartitionKey, (Arc<Partition>, Vec<Item>)> = BTreeMap::new();
        for (item, id) in items.into_iter().zip(ids) {
            let previous = placements.rows.get(&id).map(|row| row.value().clone());
            let target = match change {
                MEventType::SET => {
                    (placements.resolve)(&self.root, item.as_ref()).and_then(|key| self.get(&key))
                }
                MEventType::DEL => match &previous {
                    Some(Placement::In(key)) => self.get(key),
                    Some(Placement::Held(_)) | None => None,
                },
            };
            if let Some(Placement::In(old)) = &previous
                && target.as_ref().is_none_or(|target| target.key != *old)
                && let Some(old) = self.get(old)
            {
                drop(old.take(entity_type, &id));
            }
            match (change, &target) {
                (MEventType::SET, Some(target)) => {
                    placements
                        .rows
                        .insert(id, Placement::In(target.key.clone()));
                }
                (MEventType::SET, None) => {
                    placements.rows.insert(id, Placement::Held(item));
                    continue;
                }
                (MEventType::DEL, _) => {
                    placements.rows.remove(&id);
                }
            }
            if let Some(target) = target {
                by_partition
                    .entry(target.key.clone())
                    .or_insert_with(|| (target, Vec::new()))
                    .1
                    .push(item);
            }
        }
        for (_, (partition, items)) in by_partition {
            partition.write(items, change, origin, mode)?;
        }
        Ok(())
    }

    /// Re-resolve every placed row of `entity_type` and move the rows whose
    /// partition changed. `closing` is a partition that was just removed from
    /// the open set and still holds its rows.
    fn reroute(&self, entity_type: &'static str, closing: Option<&Arc<Partition>>) {
        let Some(placements) = self.placements.get(entity_type) else {
            return;
        };
        let holder = |key: &PartitionKey| {
            closing
                .filter(|closing| closing.key == *key)
                .cloned()
                .or_else(|| self.get(key))
        };
        let ids: Vec<Arc<str>> = placements
            .rows
            .iter()
            .map(|row| row.key().clone())
            .collect();
        for id in ids {
            let _row = placements.lock([&id]);
            let Some(current) = placements.rows.get(&id).map(|row| row.value().clone()) else {
                continue;
            };
            let (row, from) = match &current {
                Placement::Held(row) => (Some(Arc::clone(row)), None),
                Placement::In(key) => {
                    let from = holder(key);
                    let row = from
                        .as_ref()
                        .and_then(|from| from.store(entity_type).get_value(&id));
                    (row, from)
                }
            };
            let Some(row) = row else {
                placements.rows.remove(&id);
                continue;
            };
            let target =
                (placements.resolve)(&self.root, row.as_ref()).and_then(|key| self.get(&key));
            if let (Some(from), Some(target)) = (&from, &target)
                && from.key == target.key
            {
                continue;
            }
            let row = match from {
                Some(from) => from.take(entity_type, &id).unwrap_or(row),
                None => row,
            };
            match target {
                Some(target) => {
                    placements
                        .rows
                        .insert(id, Placement::In(target.key.clone()));
                    target.place(entity_type, row);
                }
                None => {
                    placements.rows.insert(id, Placement::Held(row));
                }
            }
        }
    }

    /// Called by the root context once a shared write is applied.
    pub(crate) fn shared_write_applied(self: &Arc<Self>, items: &[Item], change: MEventType) {
        let Some(entity_type) = items.first().map(|item| item.entity_type()) else {
            return;
        };
        if self.schema.lifecycle == Some(entity_type) {
            for item in items {
                let key = PartitionKey(item.id());
                match change {
                    MEventType::SET => drop(self.open(key)),
                    MEventType::DEL => self.close(&key),
                }
            }
        }
        for (routed, route) in &self.schema.routes {
            if let Route::Resolved { inputs, .. } = route
                && inputs.contains(&entity_type)
            {
                let _topology = self.topology.lock();
                self.reroute(routed, None);
            }
        }
    }
}
