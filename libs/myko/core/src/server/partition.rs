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
//! Such a write is admitted or refused when it is made: a partition that is
//! closed by then, or a validator, refuses it with an error. Once admitted
//! it is applied, even when the partition closes before it arrives, because
//! the close waits behind every write the partition admitted.
//!
//! Every admitted write is recorded in the event log when it is admitted,
//! on the writer's path, whatever its placement: applied now, in transit,
//! or held. Placement only decides which store holds the row, and moving a
//! row records nothing.
//!
//! A partition applies its routed writes and moves one at a time in the
//! order they were admitted. The rows' locks are released before a write is
//! applied, so a wave the write runs may write the same rows again.
//!
//! A routed row whose write is in transit keeps that write in its
//! placement. A move re-sends it to the row's new partition, where it is
//! applied; the copy that reaches the old partition is skipped there.
//!
//! Data flows from shared into partitions only. A write made while a
//! partition's wave runs is refused when it would land in shared data or in
//! another partition. A write made while root's wave runs, keyed or routed,
//! reaches a partition behind that wave, and so does a move root's wave
//! makes.
//!
//! # Reads
//!
//! A reactive read of a partitioned type built on the root context sees no
//! rows. [`MykoServerContext::checked`] refuses such a build, and
//! [`MykoServerContext::subscription_context`] opens a client subscription
//! in the partition the application's hook names.
//!
//! # Relationships
//!
//! A relationship runs where the rows it writes live. A shared write
//! derives its relationships in root for shared rows, and once in each open
//! partition, behind root's wave, for that partition's rows. A relation
//! between two partitioned types runs inside the partition of the write
//! that triggers it. A relation whose derived writes would flow from a
//! partition into shared data or across partitions is refused when the
//! schema is built.
//!
//! What an admitted write derives is part of it: it is applied in its own
//! partition without validation, even once that partition stopped
//! admitting writes, and recorded when it is applied.
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
    thread::ThreadId,
};

use dashmap::DashMap;
use hyphae::{Cell, CellMutable, Mutable, SubscriptionGuard, Watchable, scheduler::Domain};
use parking_lot::{Condvar, Mutex, MutexGuard, RwLock};

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

    /// Refuse every declared relationship whose derived writes would carry
    /// data from a partition back into shared data or across partitions.
    ///
    /// - `belongs_to`: a shared parent may have partitioned children; the
    ///   cascade runs in each partition. A partitioned parent's children
    ///   must be partitioned: deleting it would delete shared rows.
    /// - `owns_many`: parent and children are both shared or both
    ///   partitioned. Deleting a child rewrites its parent, and deleting a
    ///   parent deletes its children, so the relation flows both ways.
    /// - `ensure_for`: an ensured type depending on a partitioned type is
    ///   partitioned. It may depend on shared types too.
    ///
    /// A relation between two partitioned types runs inside one partition.
    fn check_relations(&self) -> Result<(), String> {
        let side = |entity_type: &str| {
            if self.is_partitioned(entity_type) {
                "partitioned"
            } else {
                "shared"
            }
        };
        let refused: Vec<String> = crate::relationship::iter_relations()
            .filter_map(|registration| match &registration.relation {
                crate::relationship::Relation::BelongsTo {
                    local_type,
                    foreign_type,
                    ..
                } => (self.is_partitioned(foreign_type) && !self.is_partitioned(local_type)).then(
                    || {
                        format!(
                            "{local_type} belongs to {foreign_type}: deleting a partitioned \
                             parent would delete shared children"
                        )
                    },
                ),
                crate::relationship::Relation::OwnsMany {
                    local_type,
                    foreign_type,
                    ..
                } => (self.is_partitioned(local_type) != self.is_partitioned(foreign_type)).then(
                    || {
                        format!(
                            "{local_type} ({}) owns many {foreign_type} ({}): owning parent and \
                             children must be on one side, as each writes the other",
                            side(local_type),
                            side(foreign_type)
                        )
                    },
                ),
                crate::relationship::Relation::EnsureFor {
                    local_type,
                    dependencies,
                    ..
                } => dependencies
                    .iter()
                    .find(|dependency| {
                        self.is_partitioned(dependency.foreign_type)
                            && !self.is_partitioned(local_type)
                    })
                    .map(|dependency| {
                        format!(
                            "{local_type} is ensured for partitioned {}: a shared type cannot \
                             be created from partition data",
                            dependency.foreign_type
                        )
                    }),
            })
            .collect();
        if refused.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "partition schema refused: data flows from shared data into partitions, never \
                 back or across:\n{}",
                refused.join("\n")
            ))
        }
    }
}

/// Where a routed row currently is.
#[derive(Clone)]
enum Placement {
    /// No open partition takes the row. It is kept here, outside every domain.
    Held(Item),
    /// In this partition, or queued there behind earlier writes. The row is
    /// its latest admitted value.
    In(PartitionKey, Item),
    /// Writes of the row are on their way to this partition behind a shared
    /// write, oldest first, and are not in its store yet. A move re-sends
    /// them to the row's new partition.
    Sent(PartitionKey, Vec<(Item, Write)>),
}

impl Placement {
    /// The row's latest admitted value.
    fn row(&self) -> Option<&Item> {
        match self {
            Self::Held(row) | Self::In(_, row) => Some(row),
            Self::Sent(_, pending) => Self::sent_row(pending),
        }
    }

    const fn key(&self) -> Option<&PartitionKey> {
        match self {
            Self::In(key, _) | Self::Sent(key, _) => Some(key),
            Self::Held(_) => None,
        }
    }

    /// The row a write in transit leaves, or `None` when it deletes it.
    fn sent_row(pending: &[(Item, Write)]) -> Option<&Item> {
        let (item, write) = pending.last()?;
        (write.change == MEventType::SET).then_some(item)
    }
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

/// Applies one partition's routed writes and moves one at a time, in the
/// order they were admitted. A job is queued while its rows' stripes are
/// held, and run after they are released, so a wave a job runs can write the
/// same rows again.
#[derive(Default)]
struct Sequencer {
    state: Mutex<Sequence>,
    turn: Condvar,
}

#[derive(Default)]
struct Sequence {
    queue: VecDeque<Job>,
    queued: u64,
    applied: u64,
    /// The thread running the queue, while one is.
    runner: Option<ThreadId>,
}

impl Sequencer {
    /// Queue `job` behind every job queued before it. Returns its ticket.
    fn queue(&self, job: Job) -> u64 {
        let mut sequence = self.state.lock();
        sequence.queue.push_back(job);
        sequence.queued = sequence.queued.wrapping_add(1);
        sequence.queued
    }

    /// Return once the job of `ticket` has run, running the queue on this
    /// thread when no other thread is. A job queued by the job this thread is
    /// running runs right after it, on this thread.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "the lock is released while each job runs and held between them"
    )]
    fn run_until(&self, ticket: u64) {
        let me = std::thread::current().id();
        let mut sequence = self.state.lock();
        loop {
            if sequence.applied >= ticket || sequence.runner == Some(me) {
                return;
            }
            if sequence.runner.is_some() {
                self.turn.wait(&mut sequence);
                continue;
            }
            sequence.runner = Some(me);
            while let Some(job) = sequence.queue.pop_front() {
                let ran = MutexGuard::unlocked(&mut sequence, || {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))
                });
                sequence.applied = sequence.applied.wrapping_add(1);
                self.turn.notify_all();
                if let Err(panic) = ran {
                    sequence.runner = None;
                    drop(sequence);
                    std::panic::resume_unwind(panic);
                }
            }
            sequence.runner = None;
            self.turn.notify_all();
        }
    }
}

/// How a write is applied, carried with it across a domain boundary.
#[derive(Clone)]
struct Write {
    change: MEventType,
    origin: Origin,
    mode: EdgeApplyMode,
    provenance: Option<OperationProvenance>,
    /// A row moved by root's wave: inserted where it lands, not written.
    place: bool,
}

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
    sequencer: Sequencer,
    _relay: SubscriptionGuard,
}

impl Partition {
    fn open(partitions: &Arc<Partitions>, key: PartitionKey) -> Arc<Self> {
        let domain = Domain::child();
        let schema = Arc::clone(&partitions.schema);
        let is_partitioned: crate::store::IsPartitioned =
            Arc::new(move |entity_type| schema.is_partitioned(entity_type));
        let registry = Arc::new(StoreRegistry::for_partition(
            Arc::clone(&partitions.root.registry),
            domain.clone(),
            Arc::clone(&is_partitioned),
        ));
        let search = Arc::new(crate::search::SearchIndex::for_partition(
            Arc::clone(partitions.root.search_index()),
            is_partitioned,
        ));
        let ctx = partitions
            .root
            .for_partition(registry, search, key.clone(), domain.clone());
        let waiting: Arc<Mutex<VecDeque<Job>>> = Arc::default();
        let (courier, relay) = Domain::root().enter(|| {
            let courier = Cell::new(0_u64);
            let waiting = Arc::clone(&waiting);
            let domain = domain.clone();
            let relay = courier.subscribe(move |_| {
                // Handed over under the lock, so a close that takes it sees
                // each write either handed over or still waiting.
                let mut writes = waiting.lock();
                for write in writes.drain(..) {
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
            sequencer: Sequencer::default(),
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
        // Held until the write is applied: a close waits for it. A write made
        // on this partition's own domain derives from one it admitted, and
        // the close runs behind it.
        let admitting = self.admitting.read_recursive();
        if !*admitting && Domain::current() != self.domain {
            drop(admitting);
            return Err(self.closed(items.first().map_or("mutation", |item| item.entity_type())));
        }
        // A write that followed a shared write may still be on its way here.
        // This one was made later, so it waits for that one to be applied. A
        // write made from this partition's own wave is already in order.
        self.catch_up();
        let result = self
            .domain
            .enter(|| self.ctx.submit_local(items, change, origin, mode));
        drop(admitting);
        result
    }

    /// Wait until every write sent here behind a shared write is applied,
    /// unless this thread runs this partition's wave and is already behind
    /// them.
    fn catch_up(&self) {
        let sent = self.sent.load(Ordering::Acquire);
        if self.applied.load(Ordering::Acquire) < sent && Domain::current() != self.domain {
            self.domain.flush();
            self.applied.fetch_max(sent, Ordering::AcqRel);
        }
    }

    /// Queue a job in admission order. Call it with the rows' stripes held,
    /// and [`settle`](Self::settle) the ticket once they are released.
    fn sequence(&self, job: impl FnOnce() + Send + 'static) -> u64 {
        self.sequencer.queue(Box::new(job))
    }

    /// Run queued jobs through `ticket`: now, or, from root's wave, behind it.
    fn settle(self: &Arc<Self>, ticket: u64) {
        match Domain::propagating() {
            Some(wave) if wave != self.domain => {
                let partition = Arc::clone(self);
                self.domain
                    .hand_off(move || partition.sequencer.run_until(ticket));
            }
            Some(_) => self.sequencer.run_until(ticket),
            None => {
                self.catch_up();
                self.sequencer.run_until(ticket);
            }
        }
    }

    /// Apply a write after the shared write the calling thread just made, on
    /// this partition's worker. A closed partition refuses it here. Once
    /// admitted it is applied: a close applies every admitted write first.
    ///
    /// Everything that can refuse it runs here, on the writer's path, and
    /// its events are recorded here.
    fn write_following(
        self: &Arc<Self>,
        items: Vec<Item>,
        change: MEventType,
        origin: Origin,
        mode: EdgeApplyMode,
        provenance: Option<OperationProvenance>,
    ) -> Result<(), PersistError> {
        let admitting = self.admitting.read_recursive();
        if !*admitting {
            drop(admitting);
            return Err(self.closed(items.first().map_or("mutation", |item| item.entity_type())));
        }
        let origin = self.ctx.admit(&items, change, origin, mode, |item| {
            self.store(item.entity_type()).get_value(&item.id())
        })?;
        let partition = Arc::clone(self);
        self.follow(Box::new(move || {
            let _operation = partition.ctx.restore_operation(provenance);
            partition.apply(items, change, origin, mode);
        }));
        drop(admitting);
        Ok(())
    }

    /// Queue `job` to run on this partition's worker after every delivery
    /// root has produced so far.
    ///
    /// The caller has admitted the job: it holds `admitting` open, or the
    /// topology lock a close takes. A close therefore finds the job queued
    /// and runs its own job behind it.
    fn follow(&self, job: Job) {
        self.waiting.lock().push_back(job);
        let sent = self.sent.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        hyphae::batch(|| self.courier.set(sent));
    }

    /// Apply an admitted write here, whether or not the partition still
    /// admits writes.
    fn apply(&self, items: Vec<Item>, change: MEventType, origin: Origin, mode: EdgeApplyMode) {
        let result = self.domain.enter(|| {
            let _admitted = AdmittedApply::begin();
            self.ctx.submit_local(items, change, origin, mode)
        });
        if let Err(error) = result {
            Self::failed(&self.key, &error);
        }
    }

    /// Run, here, the relationships a shared write applied in root derives
    /// in this partition. Root admitted the write, so what it derives here
    /// is applied as part of it.
    fn derive(&self, items: &[Item], change: MEventType, opened: bool) {
        let result = self.domain.enter(|| {
            let _admitted = AdmittedApply::begin();
            if opened {
                self.ctx.ensure_relations()?;
            }
            self.ctx.derive_relations(items, change)
        });
        if let Err(error) = result {
            Self::failed(&self.key, &error);
        }
    }

    /// Nothing refuses what an admitted write derives: it is applied in its
    /// own partition, unvalidated, and its relations are checked when the
    /// schema is built. What remains is the persister failing to take an
    /// event and the derived-mutation budget, which stops a runaway cascade.
    fn failed(key: &PartitionKey, error: &PersistError) {
        tracing::error!(partition = %key, %error, "a write derived from an admitted write failed");
    }

    /// Close the domain once every admitted write has been applied. The
    /// partition admits no more by now, so the close is queued behind the
    /// last of them, and runs on the partition's worker. A close made from
    /// root's wave cannot wait for them, and one made anywhere else need not.
    fn close_behind_writes(self: Arc<Self>) {
        let partition = Arc::clone(&self);
        self.follow(Box::new(move || {
            partition.domain.close();
            // Releasing a partition's graphs is not its worker's work.
            crate::core::off_wave::off_wave(move || drop(partition));
        }));
    }

    fn store(&self, entity_type: &str) -> Arc<crate::store::EntityStore> {
        self.ctx.registry.get_or_create(entity_type)
    }

    /// Remove a row because it now belongs elsewhere. Not an entity deletion:
    /// nothing is persisted and no relationship runs.
    fn take(&self, entity_type: &str, id: &Arc<str>) -> Option<Item> {
        let store = self.store(entity_type);
        let row = self.domain.batch(|| store.remove(id));
        if let Some(row) = &row {
            self.ctx
                .track_relations(std::slice::from_ref(row), MEventType::DEL);
            self.ctx.search_index().remove_entity(entity_type, id);
        }
        row
    }

    /// Remove a row that moved away, behind the writes queued before.
    fn take_in_order(self: &Arc<Self>, entity_type: &'static str, id: &Arc<str>) -> u64 {
        let partition = Arc::clone(self);
        let id = Arc::clone(id);
        self.sequence(move || drop(partition.take(entity_type, &id)))
    }

    /// Insert a row that was moved here, behind the writes queued before.
    fn place_in_order(self: &Arc<Self>, entity_type: &'static str, row: Item) -> u64 {
        let partition = Arc::clone(self);
        self.sequence(move || partition.place(entity_type, row))
    }

    /// Insert a row that was moved here.
    fn place(&self, entity_type: &str, row: Item) {
        let store = self.store(entity_type);
        let rows = [row];
        self.domain.batch(|| {
            for row in &rows {
                drop(store.insert(row.id(), Arc::clone(row)));
            }
        });
        self.ctx.track_relations(&rows, MEventType::SET);
        for row in &rows {
            self.ctx.search_index().index_item(row);
        }
    }
}

thread_local! {
    /// Depth of the shared writes the thread is inside.
    static SHARED_WRITES: ThreadCell<usize> = const { ThreadCell::new(0) };
}

struct SharedWrite;

thread_local! {
    /// Depth of the admitted writes the thread is applying.
    static ADMITTED_APPLIES: ThreadCell<usize> = const { ThreadCell::new(0) };
}

/// Marks the calling thread as applying a write a partition admitted.
struct AdmittedApply;

impl AdmittedApply {
    fn begin() -> Self {
        ADMITTED_APPLIES.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Drop for AdmittedApply {
    fn drop(&mut self) {
        ADMITTED_APPLIES.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Whether the calling thread is applying a write a partition admitted, so
/// that a write a relationship derives from it is part of it.
pub(crate) fn applying_admitted() -> bool {
    ADMITTED_APPLIES.with(|depth| depth.get() > 0)
}

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
    /// These partitions, for a write that lands after the call that sent it.
    this: Weak<Self>,
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
        if let Err(refused) = schema.check_relations() {
            #[allow(
                clippy::panic,
                reason = "a schema whose data flows backwards is a program error"
            )]
            std::panic::panic_any(refused);
        }
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
            this: partitions.clone(),
        })
    }

    pub(crate) const fn root(&self) -> &MykoServerContext {
        &self.root
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

    /// The partition `item` belongs in by its route, now.
    pub(crate) fn home_of(&self, item: &dyn AnyItem) -> Option<PartitionKey> {
        match self.schema.routes.get(item.entity_type())? {
            Route::Keyed(key_of) => key_of(item),
            Route::Resolved { resolve, .. } => resolve(&self.root, item),
        }
    }

    /// The partition a routed row is in, if it is in one.
    pub(crate) fn placement(&self, entity_type: &str, id: &Arc<str>) -> Option<PartitionKey> {
        self.placements
            .get(entity_type)?
            .rows
            .get(id)?
            .value()
            .key()
            .cloned()
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
        partition.close_behind_writes();
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
        let mut closing = Vec::new();
        for item in items {
            match self.schema.routes.get(item.entity_type()) {
                None => shared.push(item),
                Some(Route::Keyed(key_of)) => {
                    let key = key_of(item.as_ref())
                        .ok_or_else(|| Self::no_partition(item.entity_type(), None))?;
                    if let Some((_, items)) = keyed.get_mut(&key) {
                        items.push(item);
                    } else if let Some(partition) = self.get(&key) {
                        keyed.insert(key, (partition, vec![item]));
                    } else if from.partition_key() == Some(&key)
                        && Domain::current() == from.domain()
                    {
                        // Derived on a closing partition's own domain from a
                        // write it admitted: its close runs behind this.
                        closing.push(item);
                    } else {
                        return Err(Self::no_partition(item.entity_type(), Some(&key)));
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

        // Shared data first: a partition applies the shared part of a write
        // before the partition part. A write made from root's wave reaches a
        // partition the same way, after the wave's own deliveries.
        let follows_shared = !shared.is_empty() || SharedWrite::active() || root_wave;
        if !shared.is_empty() {
            let _shared = SharedWrite::begin();
            self.root.submit_local(shared, change, origin, mode)?;
        }
        if !closing.is_empty() {
            from.submit_local(closing, change, origin, mode)?;
        }
        for (_, (partition, items)) in keyed {
            if follows_shared && Domain::current() != partition.domain {
                partition.write_following(
                    items,
                    change,
                    origin,
                    mode,
                    from.current_provenance(),
                )?;
            } else {
                partition.write(items, change, origin, mode)?;
            }
        }
        for (entity_type, items) in resolved {
            let write = Write {
                change,
                origin,
                mode,
                provenance: from.current_provenance(),
                place: false,
            };
            self.submit_resolved(entity_type, items, &write, follows_shared)?;
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
        write: &Write,
        follows_shared: bool,
    ) -> Result<(), PersistError> {
        let Some(placements) = self.placements.get(entity_type) else {
            return Ok(());
        };
        let ids: Vec<Arc<str>> = items.iter().map(|item| item.id()).collect();
        let mut queued = Vec::new();
        let result = {
            let _rows = placements.lock(&ids);
            items.into_iter().zip(ids).try_for_each(|(item, id)| {
                self.admit_routed(
                    entity_type,
                    placements,
                    (item, id),
                    write,
                    follows_shared,
                    &mut queued,
                )
            })
        };
        // Applied with the stripes released: a wave a write runs may write
        // the same rows again.
        for (partition, ticket) in queued {
            partition.settle(ticket);
        }
        result
    }

    /// Admit one routed write and decide where its row goes. Called with the
    /// row's stripe held. The write's event is recorded here, whatever its
    /// placement; placement only decides which store holds the row.
    fn admit_routed(
        &self,
        entity_type: &'static str,
        placements: &Placements,
        (item, id): (Item, Arc<str>),
        write: &Write,
        follows_shared: bool,
        queued: &mut Vec<(Arc<Partition>, u64)>,
    ) -> Result<(), PersistError> {
        let previous = placements.rows.get(&id).map(|row| row.value().clone());
        let target = match write.change {
            MEventType::SET => {
                (placements.resolve)(&self.root, item.as_ref()).and_then(|key| self.get(&key))
            }
            MEventType::DEL => previous
                .as_ref()
                .and_then(Placement::key)
                .and_then(|key| self.get(key)),
        };
        let from = previous
            .as_ref()
            .and_then(Placement::key)
            .and_then(|key| self.get(key));
        let before = previous.as_ref().and_then(Placement::row).cloned();
        // Held until the write is queued: a close that takes it exclusively
        // finds the row placed and moves it.
        let gate = target.clone();
        let admitting = gate
            .as_ref()
            .map(|target| target.admitting.read_recursive());
        if let (Some(target), Some(admitting)) = (&target, &admitting)
            && !**admitting
        {
            return Err(target.closed(entity_type));
        }
        let ctx = target.as_ref().map_or(&self.root, |target| &target.ctx);
        let write = Write {
            origin: ctx.admit(
                std::slice::from_ref(&item),
                write.change,
                write.origin,
                write.mode,
                |_| before.clone(),
            )?,
            ..write.clone()
        };
        let mut pending = match previous {
            Some(Placement::Sent(_, pending)) => pending,
            _ => Vec::new(),
        };
        let moved = from
            .as_ref()
            .is_some_and(|from| target.as_ref().is_none_or(|target| from.key != target.key));
        if moved && let Some(from) = &from {
            queued.push((Arc::clone(from), from.take_in_order(entity_type, &id)));
        }
        let Some(target) = target else {
            match write.change {
                MEventType::SET => drop(placements.rows.insert(id, Placement::Held(item))),
                MEventType::DEL => drop(placements.rows.remove(&id)),
            }
            return Ok(());
        };
        let follows = !pending.is_empty() || follows_shared && Domain::current() != target.domain;
        if follows {
            // Writes of this row still in transit go first, where this one
            // goes.
            let resent = if moved { pending.clone() } else { Vec::new() };
            pending.push((Arc::clone(&item), write.clone()));
            placements
                .rows
                .insert(id, Placement::Sent(target.key.clone(), pending));
            for (resend, resend_write) in resent {
                self.send(entity_type, &target, vec![resend], resend_write);
            }
            self.send(entity_type, &target, vec![item], write);
            return Ok(());
        }
        match write.change {
            MEventType::SET => drop(
                placements
                    .rows
                    .insert(id, Placement::In(target.key.clone(), Arc::clone(&item))),
            ),
            MEventType::DEL => drop(placements.rows.remove(&id)),
        }
        let partition = Arc::clone(&target);
        let ticket = target.sequence(move || {
            let _operation = partition.ctx.restore_operation(write.provenance);
            partition.apply(vec![item], write.change, write.origin, write.mode);
        });
        queued.push((target, ticket));
        Ok(())
    }

    /// Send routed rows to `to` behind root's deliveries.
    fn send(&self, entity_type: &'static str, to: &Arc<Partition>, items: Vec<Item>, write: Write) {
        let Some(partitions) = self.this.upgrade() else {
            return;
        };
        let via = Arc::clone(to);
        to.follow(Box::new(move || {
            partitions.land(entity_type, &via, items, write);
        }));
    }

    /// Apply routed writes that arrived at `via`, on its worker. A write is
    /// applied only while its row's placement still sends it here: a move
    /// re-sent it to the row's new partition, and a row since held outside
    /// every partition keeps its latest write.
    fn land(
        &self,
        entity_type: &'static str,
        via: &Arc<Partition>,
        items: Vec<Item>,
        write: Write,
    ) {
        let Some(placements) = self.placements.get(entity_type) else {
            return;
        };
        let ids: Vec<Arc<str>> = items.iter().map(|item| item.id()).collect();
        let rows = placements.lock(&ids);
        let mut landing = Vec::with_capacity(items.len());
        for (item, id) in items.into_iter().zip(ids) {
            let Some(Placement::Sent(key, mut pending)) =
                placements.rows.get(&id).map(|row| row.value().clone())
            else {
                continue;
            };
            let Some(index) = pending
                .iter()
                .position(|(sent, _)| Arc::ptr_eq(sent, &item))
            else {
                continue;
            };
            if key != via.key {
                continue;
            }
            pending.remove(index);
            if pending.is_empty() {
                match write.change {
                    MEventType::SET => drop(
                        placements
                            .rows
                            .insert(id, Placement::In(key, Arc::clone(&item))),
                    ),
                    MEventType::DEL => drop(placements.rows.remove(&id)),
                }
            } else {
                placements.rows.insert(id, Placement::Sent(key, pending));
            }
            landing.push(item);
        }
        if landing.is_empty() {
            return;
        }
        let partition = Arc::clone(via);
        let ticket = via.sequence(move || {
            if write.place {
                for row in landing {
                    partition.place(entity_type, row);
                }
            } else {
                let _operation = partition.ctx.restore_operation(write.provenance);
                partition.apply(landing, write.change, write.origin, write.mode);
            }
        });
        drop(rows);
        via.settle(ticket);
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
        let mut queued = Vec::new();
        for id in ids {
            let _row = placements.lock([&id]);
            let Some(current) = placements.rows.get(&id).map(|row| row.value().clone()) else {
                continue;
            };
            let Some(row) = current.row().cloned() else {
                // Its delete is in transit; there is nothing to move.
                continue;
            };
            let from = current.key().and_then(holder);
            let target =
                (placements.resolve)(&self.root, row.as_ref()).and_then(|key| self.get(&key));
            if let (Some(from), Some(target)) = (&from, &target)
                && from.key == target.key
            {
                continue;
            }
            if let Some(from) = &from {
                queued.push((Arc::clone(from), from.take_in_order(entity_type, &id)));
            }
            // Root's wave cannot write a partition's store; the row follows
            // it there like a write.
            let behind = Domain::propagating().is_some();
            match (target, current) {
                // The writes in transit are re-sent, in order, to land there.
                (Some(target), Placement::Sent(_, pending)) => {
                    placements
                        .rows
                        .insert(id, Placement::Sent(target.key.clone(), pending.clone()));
                    for (item, write) in pending {
                        self.send(entity_type, &target, vec![item], write);
                    }
                }
                (Some(target), _) if behind => {
                    let write = Write {
                        change: MEventType::SET,
                        origin: Origin::Local,
                        mode: EdgeApplyMode::Authoritative,
                        provenance: None,
                        place: true,
                    };
                    placements.rows.insert(
                        id,
                        Placement::Sent(
                            target.key.clone(),
                            vec![(Arc::clone(&row), write.clone())],
                        ),
                    );
                    self.send(entity_type, &target, vec![row], write);
                }
                (Some(target), _) => {
                    placements
                        .rows
                        .insert(id, Placement::In(target.key.clone(), Arc::clone(&row)));
                    let ticket = target.place_in_order(entity_type, row);
                    queued.push((target, ticket));
                }
                (None, _) => {
                    placements.rows.insert(id, Placement::Held(row));
                }
            }
        }
        for (partition, ticket) in queued {
            partition.settle(ticket);
        }
    }

    /// Apply a persisted event log, oldest first, as startup does: every
    /// shared event first, then every partitioned event in log order, each in
    /// the partition that holds its row once shared data is applied.
    ///
    /// A keyed event is dropped when its partition is not open at the end of
    /// the log or closes after it, as the partition's store was when it
    /// closed. A routed row survives a close, as it does live.
    pub(crate) fn replay(&self, events: Vec<(Item, MEventType)>) -> Result<(), PersistError> {
        let last_close: HashMap<PartitionKey, usize> = events
            .iter()
            .enumerate()
            .filter(|(_, (item, change))| {
                *change == MEventType::DEL && self.schema.lifecycle == Some(item.entity_type())
            })
            .map(|(index, (item, _))| (PartitionKey(item.id()), index))
            .collect();
        let (shared, partitioned): (Vec<_>, Vec<_>) = events
            .into_iter()
            .enumerate()
            .partition(|(_, (item, _))| !self.is_partitioned(item.entity_type()));
        let replay = |item: Item, change| {
            self.submit(
                &self.root,
                vec![item],
                change,
                Origin::Replay,
                EdgeApplyMode::Authoritative,
            )
        };
        for (_, (item, change)) in shared {
            replay(item, change)?;
        }
        for (index, (item, change)) in partitioned {
            if let Some(Route::Keyed(key_of)) = self.schema.routes.get(item.entity_type()) {
                let live = key_of(item.as_ref()).is_some_and(|key| {
                    self.open.contains_key(&key)
                        && last_close.get(&key).is_none_or(|closed| *closed < index)
                });
                if !live {
                    continue;
                }
            }
            replay(item, change)?;
        }
        Ok(())
    }

    /// Queue, in every open partition, the relationships `items` derive
    /// there.
    fn derive_in_partitions(
        &self,
        items: &[Item],
        change: MEventType,
        opened: &BTreeSet<PartitionKey>,
    ) {
        let partitions: Vec<Arc<Partition>> = self
            .open
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        for partition in partitions {
            // Held while the job is queued: a close finds it queued.
            let admitting = partition.admitting.read_recursive();
            if !*admitting {
                continue;
            }
            let opened = opened.contains(&partition.key);
            let items = items.to_vec();
            let target = Arc::clone(&partition);
            partition.follow(Box::new(move || target.derive(&items, change, opened)));
            drop(admitting);
        }
    }

    /// Called by the root context once a shared write is applied.
    ///
    /// `derive` is whether the write runs relationships. A shared write
    /// derives in every open partition once, there, behind root's wave: a
    /// partition opened by this write establishes its `ensure_for` rows
    /// first.
    pub(crate) fn shared_write_applied(
        self: &Arc<Self>,
        items: &[Item],
        change: MEventType,
        derive: bool,
    ) {
        let Some(entity_type) = items.first().map(|item| item.entity_type()) else {
            return;
        };
        let mut opened = BTreeSet::new();
        if self.schema.lifecycle == Some(entity_type) {
            for item in items {
                let key = PartitionKey(item.id());
                match change {
                    MEventType::SET => {
                        if self.get(&key).is_none() {
                            opened.insert(key.clone());
                        }
                        drop(self.open(key));
                    }
                    MEventType::DEL => self.close(&key),
                }
            }
        }
        let partitioned = |entity_type: &str| self.is_partitioned(entity_type);
        if !self.root.ensures_into(&partitioned) {
            opened.clear();
        }
        if derive
            && (!opened.is_empty() || self.root.derives_into(entity_type, change, &partitioned))
        {
            self.derive_in_partitions(items, change, &opened);
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
