//! Server context for the cell-based server.
//!
//! Provides modules (like PeerRegistry) with the ability to:
//! - Run reactive queries (like GetPeerServers)
//! - Publish entities (Reduce → Relationships → Persist)
//! - Access server identity (host_id)

use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use dashmap::DashMap;
use hyphae::{
    Cell, CellImmutable, CellMap, CellMutable, Gettable, IdFor, MaterializeDefinite, Mutable,
    WeakCellMap,
};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::{
    HandlerRegistry, RelationshipManager,
    persister::{PersistError, PersistHealth, PersisterRouter},
};
use crate::{
    cache::CacheKey,
    client::{ConnectionStatus, MykoClient},
    common::{
        to_value::ToValue,
        with_id::{WithId, WithTypedId},
    },
    core::item::{
        AnyItem, Eventable, IngestBufferPolicy, downcast_any_item_arc, typed_map_arc_from_any_item,
        typed_map_from_any_item_with_typed_id,
    },
    query::{
        FilteredCellMap, QueryContext, QueryFactory, QueryHandler, QueryParams, QueryRequest,
        QueryTestCtx,
    },
    report::{ReportContext, ReportHandler, ReportId},
    request::RequestContext,
    search::SearchIndex,
    store::{LwwStamp, StoreRegistry},
    view::{FilteredViewCellMap, TypedViewCellMap, ViewFactory},
    wire::{EventOptions, MEvent, MEventType},
};

type AnyItemArc = Arc<dyn AnyItem>;
/// An item plus its LWW stamp inputs (`created_at`, `source_id`) — batch input.
type StampedItem = (AnyItemArc, Arc<str>, Option<Arc<str>>);
/// An item paired with its resolved LWW stamp — batch winners.
type StampedWinner = (AnyItemArc, LwwStamp);

/// Where a mutation came from. This is the single policy point for the apply
/// pipeline's loop-safety: it determines whether a mutation should run
/// relationship cascades. (Both origins produce.)
///
/// It replaces the scattered per-call loop-guard flag checks; `from_options`
/// bridges the legacy `EventOptions::prevent_relationship_updates` flag to an
/// `Origin` for the deprecated `*_with_options` methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    /// A command handler / server module emitting a new mutation here (also a
    /// client event ingested over the WebSocket). Cascades and produces.
    Local,
    /// A relationship cascade product — a consequence of another mutation here.
    Cascade,
    /// An event replicated from a peer server: already durable and already
    /// cascaded at its origin. Applied to the store + search index only — it must
    /// not cascade (the origin already replicated its cascade products) and must
    /// not produce (which would echo it back around the peer mesh).
    ///
    /// Reserved: nothing constructs this right now (peer-origin tracking has been
    /// moved off the wire). The wiring is kept for when that mechanism returns.
    #[allow(dead_code)]
    Remote,
}

impl Origin {
    /// Bridge the legacy `EventOptions::prevent_relationship_updates` flag to an
    /// `Origin`: cascade products set it (→ `Cascade`); everything else `Local`.
    pub(crate) fn from_options(options: &EventOptions) -> Origin {
        if options.prevent_relationship_updates {
            Origin::Cascade
        } else {
            Origin::Local
        }
    }

    /// Whether this origin's mutations should run relationship cascades.
    ///
    /// - `Local` mutations always cascade.
    /// - `Cascade` products are gated on the change type: a **DEL** product
    ///   keeps cascading, so a deleted parent's children, grandchildren, … are
    ///   all removed at runtime (not just one level, and not deferred to the
    ///   boot-time orphan sweep). The owns_many array-fixup **SET** product must
    ///   not descend structurally.
    /// - `Remote` never cascades (the origin already replicated its products).
    ///
    /// Transitive DEL cascade terminates without a depth counter or visited set:
    /// reduce runs before cascade, so each node is removed from the store before
    /// its own cascade runs. The store is therefore a monotonically shrinking
    /// visited-set — a cyclic schema (A→B→A) finds nothing the second time, and
    /// a cascade-deleted child cannot resurrect its already-removed parent.
    fn should_cascade(self, change: MEventType) -> bool {
        match self {
            Origin::Local => true,
            Origin::Cascade => change == MEventType::DEL,
            Origin::Remote => false,
        }
    }

    /// Whether this origin's mutations should be produced to persisters/sink.
    ///
    /// `Remote` events are already durable and already cascaded at their origin,
    /// so re-producing them would echo them back around the peer mesh. Everything
    /// else produces; per-type durability is the persister router's job
    /// (`BlackholePersister`), not a per-event flag.
    fn should_produce(self) -> bool {
        self != Origin::Remote
    }
}

/// Weak-ref report cache entry. The cell stays alive as long as someone is
/// subscribed to it. When all subscribers drop, the weak ref fails to upgrade
/// and the next request recomputes.
trait ReportCacheEntryDyn: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn is_alive(&self) -> bool;
}

struct ReportCacheEntry<T> {
    weak: hyphae::cell::WeakCell<T, CellImmutable>,
}

impl<T> ReportCacheEntry<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn new(cell: &Cell<T, CellImmutable>) -> Self {
        Self {
            weak: cell.downgrade(),
        }
    }

    fn get(&self) -> Option<Cell<T, CellImmutable>> {
        self.weak.upgrade()
    }
}

impl<T> ReportCacheEntryDyn for ReportCacheEntry<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_alive(&self) -> bool {
        self.weak.upgrade().is_some()
    }
}

struct MapCacheEntry {
    weak: WeakCellMap<Arc<str>, AnyItemArc>,
    /// Lazily-created typed projections keyed by `TypeId` of the output
    /// `CellMap<K, V>`. Each value is a type-erased weak cell map that can be
    /// downcast back to the concrete `WeakCellMap<K, V>`.
    typed: Mutex<HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
}

#[derive(Default)]
struct BufferedIngestState {
    events: Vec<MEvent>,
    flush_scheduled: bool,
}

struct BufferedIngestType {
    state: Mutex<BufferedIngestState>,
}

impl BufferedIngestType {
    fn new() -> Self {
        Self {
            state: Mutex::new(BufferedIngestState::default()),
        }
    }
}

impl MapCacheEntry {
    fn new(map: &FilteredCellMap) -> Self {
        Self {
            weak: map.downgrade(),
            typed: Mutex::new(HashMap::new()),
        }
    }

    fn get(&self) -> Option<FilteredCellMap> {
        self.weak.upgrade().map(|map| map.lock())
    }

    /// Get or create a typed projection of this untyped map.
    ///
    /// `F` is called at most once per projection type to create the typed map
    /// from the untyped source. Subsequent calls return the cached projection.
    fn get_or_create_typed<K, V, F>(&self, create: F) -> Option<CellMap<K, V, CellImmutable>>
    where
        K: std::hash::Hash + Eq + hyphae::traits::CellValue + 'static,
        V: hyphae::traits::CellValue + 'static,
        F: FnOnce(FilteredCellMap) -> CellMap<K, V, CellImmutable>,
    {
        let type_key = std::any::TypeId::of::<WeakCellMap<K, V>>();
        let mut typed = self.typed.lock().unwrap();

        // Try to upgrade an existing weak ref
        if let Some(entry) = typed.get(&type_key) {
            if let Some(weak) = entry.downcast_ref::<WeakCellMap<K, V>>()
                && let Some(strong) = weak.upgrade()
            {
                return Some(strong.lock());
            }
            // Dead — remove stale entry
            typed.remove(&type_key);
        }

        // Create from the untyped source
        let source = self.weak.upgrade()?.lock();
        let built = create(source);
        typed.insert(type_key, Box::new(built.downgrade()));
        Some(built)
    }
}

/// Context providing capabilities to server modules.
///
/// This is the cell-based equivalent of `MykoServerCtx`, providing:
/// - Entity store access (read-only, via queries)
/// - Event publishing (Reduce → Relationships → Persist)
/// - Server identity
#[derive(Clone)]
pub struct CellServerCtx {
    /// Unique identifier for this server instance
    pub host_id: Uuid,
    /// Store registry for entity access
    pub registry: Arc<StoreRegistry>,
    /// Handler registry for item parsers
    pub handler_registry: Arc<HandlerRegistry>,
    /// Relationship manager - handles cascades
    relationship_manager: Arc<RelationshipManager>,
    /// Persister routing (default + per-entity overrides)
    persisters: Arc<PersisterRouter>,
    /// Full-text search index
    search_index: Arc<SearchIndex>,
    /// Live peer clients by peer server id (populated by peer registry).
    peer_clients: Arc<DashMap<Arc<str>, Arc<MykoClient>>>,
    /// Monotonic tick bumped on peer client register/unregister.
    peer_clients_tick: Cell<u64, CellMutable>,
    /// Optional event sink used to fan out applied events to saga runtimes.
    event_sink: Option<flume::Sender<MEvent>>,
    // AHash on the cache + dispatch maps below: every subscriber and every
    // applied event goes through one or more of these. Bench: ~1.6× faster
    // DashMap lookups vs default SipHash.
    /// Top-level cache for reactive query maps.
    query_cache: Arc<DashMap<String, MapCacheEntry, ahash::RandomState>>,
    /// Top-level cache for reactive view maps.
    view_cache: Arc<DashMap<String, MapCacheEntry, ahash::RandomState>>,
    /// Top-level cache for reactive report cells with short-lived strong retention.
    report_cache: Arc<DashMap<String, Arc<dyn ReportCacheEntryDyn>, ahash::RandomState>>,
    /// Per-key coordination for concurrent report/query/view computation.
    /// Prevents duplicate computation when multiple threads request the same key.
    compute_gates: Arc<DashMap<String, Arc<std::sync::Mutex<()>>, ahash::RandomState>>,
    /// Optional ingest buffers keyed by entity type for opt-in burst smoothing.
    ingest_buffers: Arc<DashMap<Arc<str>, Arc<BufferedIngestType>, ahash::RandomState>>,
    /// Optional history replay provider for point-in-time snapshots.
    history_replay: Option<Arc<dyn crate::server::HistoryReplayProvider>>,
}

impl CellServerCtx {
    /// Create a new server context.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host_id: Uuid,
        registry: Arc<StoreRegistry>,
        handler_registry: Arc<HandlerRegistry>,
        relationship_manager: Arc<RelationshipManager>,
        persisters: Arc<PersisterRouter>,
        search_index: Arc<SearchIndex>,
        peer_clients: Arc<DashMap<Arc<str>, Arc<MykoClient>>>,
        event_sink: Option<flume::Sender<MEvent>>,
        history_replay: Option<Arc<dyn crate::server::HistoryReplayProvider>>,
    ) -> Self {
        Self {
            host_id,
            registry,
            handler_registry,
            relationship_manager,
            persisters,
            search_index,
            peer_clients,
            peer_clients_tick: Cell::new(0).with_name("peer_clients_tick"),
            event_sink,
            query_cache: Arc::new(DashMap::with_hasher(ahash::RandomState::new())),
            view_cache: Arc::new(DashMap::with_hasher(ahash::RandomState::new())),
            report_cache: Arc::new(DashMap::with_hasher(ahash::RandomState::new())),
            compute_gates: Arc::new(DashMap::with_hasher(ahash::RandomState::new())),
            ingest_buffers: Arc::new(DashMap::with_hasher(ahash::RandomState::new())),
            history_replay,
        }
    }

    fn cache_key<T: CacheKey>(
        &self,
        kind: &str,
        id: &str,
        params: &T,
        request: &RequestContext,
    ) -> String {
        let payload_hash = params.cache_key_hash();
        format!("{}:{kind}:{id}:{payload_hash:016x}", request.host_id)
    }

    /// Get the search index.
    pub fn search_index(&self) -> &Arc<SearchIndex> {
        &self.search_index
    }

    /// Get the history replay provider, if configured.
    pub fn history_replay(&self) -> Option<&Arc<dyn crate::server::HistoryReplayProvider>> {
        self.history_replay.as_ref()
    }

    /// Register or replace a live peer client for a server id.
    pub fn register_peer_client<S: AsRef<str>>(&self, peer_id: S, client: Arc<MykoClient>) {
        self.peer_clients
            .insert(Arc::<str>::from(peer_id.as_ref()), client);
        let next = self.peer_clients_tick.get().saturating_add(1);
        self.peer_clients_tick.set(next);
    }

    /// Remove a live peer client for a server id.
    pub fn unregister_peer_client(&self, peer_id: &str) {
        if self.peer_clients.remove(peer_id).is_some() {
            let next = self.peer_clients_tick.get().saturating_add(1);
            self.peer_clients_tick.set(next);
        }
    }

    /// Get a live peer client by server id, if present.
    pub fn peer_client(&self, peer_id: &str) -> Option<Arc<MykoClient>> {
        self.peer_clients
            .get(peer_id)
            .map(|entry| entry.value().clone())
    }

    /// Get a peer's current connection status if the client is present.
    pub fn peer_connection_status(&self, peer_id: &str) -> Option<ConnectionStatus> {
        self.peer_client(peer_id)
            .map(|client| client.get_connection_status_sync())
    }

    /// Reactive tick that updates whenever peer client membership changes.
    pub fn peer_clients_tick(&self) -> Cell<u64, CellImmutable> {
        self.peer_clients_tick.clone().lock()
    }

    /// Number of currently tracked peer clients.
    pub fn peer_client_count(&self) -> usize {
        self.peer_clients.len()
    }

    /// Get the live persist health counters from the default persister.
    pub fn persist_health(&self) -> Arc<PersistHealth> {
        self.persisters.default_health()
    }

    /// Number of entries in the query cache (includes dead weak refs).
    pub fn query_cache_len(&self) -> usize {
        self.query_cache.len()
    }

    /// Number of entries in the view cache (includes dead weak refs).
    pub fn view_cache_len(&self) -> usize {
        self.view_cache.len()
    }

    /// Number of entries in the report cache (includes dead weak refs).
    pub fn report_cache_len(&self) -> usize {
        self.report_cache.len()
    }

    /// Count live (upgradeable) entries in the report cache.
    pub fn report_cache_live_count(&self) -> usize {
        self.report_cache
            .iter()
            .filter(|entry| entry.value().is_alive())
            .count()
    }

    /// Count live (upgradeable) entries in the query cache.
    pub fn query_cache_live_count(&self) -> usize {
        self.query_cache
            .iter()
            .filter(|entry| entry.value().weak.upgrade().is_some())
            .count()
    }

    /// Count live (upgradeable) entries in the view cache.
    pub fn view_cache_live_count(&self) -> usize {
        self.view_cache
            .iter()
            .filter(|entry| entry.value().weak.upgrade().is_some())
            .count()
    }

    /// Remove dead weak-ref entries from all caches.
    pub fn sweep_dead_cache_entries(&self) {
        self.query_cache
            .retain(|_, entry| entry.weak.upgrade().is_some());
        self.view_cache
            .retain(|_, entry| entry.weak.upgrade().is_some());
        self.report_cache.retain(|_, entry| entry.is_alive());
    }

    /// Parse JSON to a typed entity using the registered item parser.
    ///
    /// Takes the `Value` by ownership so we don't pay a deep-clone of the
    /// nested-enum tree on every applied event. Bench `from_value_with_clone`
    /// shows the clone is ~136 ns/event on a typical entity payload — small
    /// per event, but multiplied by the apply_event_batch hot path it's the
    /// cheapest non-breaking win on the ingest path.
    ///
    /// Returns None if the entity type is not registered or parsing fails.
    pub fn parse_item(
        &self,
        entity_type: &str,
        json: serde_json::Value,
    ) -> Option<Arc<dyn AnyItem>> {
        let parse = self.handler_registry.get_item_parser(entity_type)?;
        parse(json).ok()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Typed entity publishing (for server modules)
    // ─────────────────────────────────────────────────────────────────────────

    /// Publish an entity (SET) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn set<T>(&self, entity: &T) -> Result<(), PersistError>
    where
        T: Eventable + 'static,
    {
        self.set_with_origin(entity, Origin::Local)
    }

    /// Publish an entity (SET) with options.
    ///
    /// **Deprecated.** `EventOptions` are internal loop-guard plumbing (cascade
    /// and peer-replication markers) and must not be set by callers — use
    /// [`set`](Self::set) instead.
    #[deprecated(note = "EventOptions is internal plumbing; use `set` instead")]
    pub fn set_with_options<T>(
        &self,
        entity: &T,
        options: Option<EventOptions>,
    ) -> Result<(), PersistError>
    where
        T: Eventable + 'static,
    {
        self.set_with_origin(entity, Origin::from_options(&options.unwrap_or_default()))
    }

    /// Internal SET: typed reduce (direct `Arc` store insert) followed by the
    /// shared `apply_effects` tail, gated by `origin`.
    pub(crate) fn set_with_origin<T>(
        &self,
        entity: &T,
        origin: Origin,
    ) -> Result<(), PersistError>
    where
        T: Eventable + 'static,
    {
        let item: Arc<dyn AnyItem> = Arc::new(entity.clone());
        let (created_at, source) = self.local_stamp();
        self.apply_one(item, MEventType::SET, origin, created_at, source)
    }

    /// Delete an entity (DEL) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn del<T>(&self, entity: &T) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        self.del_with_origin(entity, Origin::Local)
    }

    /// Delete an entity (DEL) with options.
    ///
    /// **Deprecated.** `EventOptions` are internal plumbing; use [`del`](Self::del).
    #[deprecated(note = "EventOptions is internal plumbing; use `del` instead")]
    pub fn del_with_options<T>(
        &self,
        entity: &T,
        options: Option<EventOptions>,
    ) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        self.del_with_origin(entity, Origin::from_options(&options.unwrap_or_default()))
    }

    pub(crate) fn del_with_origin<T>(
        &self,
        entity: &T,
        origin: Origin,
    ) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        let item: Arc<dyn AnyItem> = Arc::new(entity.clone());
        let (created_at, source) = self.local_stamp();
        self.apply_one(item, MEventType::DEL, origin, created_at, source)
    }

    /// Publish a batch of entities (SET) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn batch_set<T>(&self, entities: &[T]) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        self.batch_set_with_origin(entities, Origin::Local)
    }

    /// Publish a batch of entities (SET) with shared options.
    ///
    /// **Deprecated.** `EventOptions` are internal plumbing; use [`batch_set`](Self::batch_set).
    #[deprecated(note = "EventOptions is internal plumbing; use `batch_set` instead")]
    pub fn batch_set_with_options<T>(
        &self,
        entities: &[T],
        options: Option<EventOptions>,
    ) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        self.batch_set_with_origin(entities, Origin::from_options(&options.unwrap_or_default()))
    }

    /// Publish a batch of entities (SET) with one grouped store insert.
    pub(crate) fn batch_set_with_origin<T>(
        &self,
        entities: &[T],
        origin: Origin,
    ) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        if entities.is_empty() {
            return Ok(());
        }
        let (created_at, source) = self.local_stamp();
        let entries = entities
            .iter()
            .map(|e| {
                (
                    Arc::new(e.clone()) as Arc<dyn AnyItem>,
                    created_at.clone(),
                    source.clone(),
                )
            })
            .collect();
        self.emit_grouped(entries, MEventType::SET, origin)
    }

    /// Delete a batch of entities (DEL) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn batch_del<T>(&self, entities: &[T]) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        self.batch_del_with_origin(entities, Origin::Local)
    }

    /// Delete a batch of entities (DEL) with shared options.
    ///
    /// **Deprecated.** `EventOptions` are internal plumbing; use [`batch_del`](Self::batch_del).
    #[deprecated(note = "EventOptions is internal plumbing; use `batch_del` instead")]
    pub fn batch_del_with_options<T>(
        &self,
        entities: &[T],
        options: Option<EventOptions>,
    ) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        self.batch_del_with_origin(entities, Origin::from_options(&options.unwrap_or_default()))
    }

    /// Delete a batch of entities (DEL) with one grouped store remove.
    pub(crate) fn batch_del_with_origin<T>(
        &self,
        entities: &[T],
        origin: Origin,
    ) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        if entities.is_empty() {
            return Ok(());
        }
        let (created_at, source) = self.local_stamp();
        let entries = entities
            .iter()
            .map(|e| {
                (
                    Arc::new(e.clone()) as Arc<dyn AnyItem>,
                    created_at.clone(),
                    source.clone(),
                )
            })
            .collect();
        self.emit_grouped(entries, MEventType::DEL, origin)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic item publishing (for parsed JSON)
    // ─────────────────────────────────────────────────────────────────────────

    /// Publish a dynamic item (SET) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn set_dyn(&self, item: Arc<dyn AnyItem>) -> Result<(), PersistError> {
        self.set_dyn_with_origin(item, Origin::Local)
    }

    /// Publish a dynamic item (SET) with options.
    ///
    /// **Deprecated.** `EventOptions` are internal plumbing; use [`set_dyn`](Self::set_dyn).
    #[deprecated(note = "EventOptions is internal plumbing; use `set_dyn` instead")]
    pub fn set_dyn_with_options(
        &self,
        item: Arc<dyn AnyItem>,
        options: Option<EventOptions>,
    ) -> Result<(), PersistError> {
        self.set_dyn_with_origin(item, Origin::from_options(&options.unwrap_or_default()))
    }

    pub(crate) fn set_dyn_with_origin(
        &self,
        item: Arc<dyn AnyItem>,
        origin: Origin,
    ) -> Result<(), PersistError> {
        let (created_at, source) = self.local_stamp();
        self.apply_one(item, MEventType::SET, origin, created_at, source)
    }

    /// Publish a batch of dynamic items (SET).
    pub fn batch_set_dyn(&self, items: &[Arc<dyn AnyItem>]) -> Result<(), PersistError> {
        self.batch_set_dyn_with_origin(items, Origin::Local)
    }

    /// Publish a batch of dynamic items (SET) with shared options.
    ///
    /// **Deprecated.** `EventOptions` are internal plumbing; use [`batch_set_dyn`](Self::batch_set_dyn).
    #[deprecated(note = "EventOptions is internal plumbing; use `batch_set_dyn` instead")]
    pub fn batch_set_dyn_with_options(
        &self,
        items: &[Arc<dyn AnyItem>],
        options: Option<EventOptions>,
    ) -> Result<(), PersistError> {
        self.batch_set_dyn_with_origin(items, Origin::from_options(&options.unwrap_or_default()))
    }

    pub(crate) fn batch_set_dyn_with_origin(
        &self,
        items: &[Arc<dyn AnyItem>],
        origin: Origin,
    ) -> Result<(), PersistError> {
        let (created_at, source) = self.local_stamp();
        let entries = items
            .iter()
            .map(|item| (item.clone(), created_at.clone(), source.clone()))
            .collect();
        self.emit_grouped(entries, MEventType::SET, origin)
    }

    /// Delete a dynamic item (DEL) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn del_dyn(&self, item: Arc<dyn AnyItem>) -> Result<(), PersistError> {
        self.del_dyn_with_origin(item, Origin::Local)
    }

    /// Delete a dynamic item (DEL) with options.
    ///
    /// **Deprecated.** `EventOptions` are internal plumbing; use [`del_dyn`](Self::del_dyn).
    #[deprecated(note = "EventOptions is internal plumbing; use `del_dyn` instead")]
    pub fn del_dyn_with_options(
        &self,
        item: Arc<dyn AnyItem>,
        options: Option<EventOptions>,
    ) -> Result<(), PersistError> {
        self.del_dyn_with_origin(item, Origin::from_options(&options.unwrap_or_default()))
    }

    pub(crate) fn del_dyn_with_origin(
        &self,
        item: Arc<dyn AnyItem>,
        origin: Origin,
    ) -> Result<(), PersistError> {
        let (created_at, source) = self.local_stamp();
        self.apply_one(item, MEventType::DEL, origin, created_at, source)
    }

    /// Publish a batch of dynamic items (DEL).
    pub fn batch_del_dyn(&self, items: &[Arc<dyn AnyItem>]) -> Result<(), PersistError> {
        self.batch_del_dyn_with_origin(items, Origin::Local)
    }

    /// Publish a batch of dynamic items (DEL) with shared options.
    ///
    /// **Deprecated.** `EventOptions` are internal plumbing; use [`batch_del_dyn`](Self::batch_del_dyn).
    #[deprecated(note = "EventOptions is internal plumbing; use `batch_del_dyn` instead")]
    pub fn batch_del_dyn_with_options(
        &self,
        items: &[Arc<dyn AnyItem>],
        options: Option<EventOptions>,
    ) -> Result<(), PersistError> {
        self.batch_del_dyn_with_origin(items, Origin::from_options(&options.unwrap_or_default()))
    }

    pub(crate) fn batch_del_dyn_with_origin(
        &self,
        items: &[Arc<dyn AnyItem>],
        origin: Origin,
    ) -> Result<(), PersistError> {
        let (created_at, source) = self.local_stamp();
        let entries = items
            .iter()
            .map(|item| (item.clone(), created_at.clone(), source.clone()))
            .collect();
        self.emit_grouped(entries, MEventType::DEL, origin)
    }

    /// Delete an entity by type/id and publish DEL even if the item is not present locally.
    ///
    /// This is useful for explicit tombstoning of entities (e.g. disconnected peers)
    /// where we must ensure a DEL event is produced to durable backend.
    ///
    /// Note: relationship cascades require the full item and are therefore skipped here.
    pub fn del_by_id(&self, entity_type: &str, id: &str) -> Result<(), PersistError> {
        self.del_by_id_with_origin(entity_type, id, Origin::Local)
    }

    /// Delete an entity by type/id with options.
    ///
    /// **Deprecated.** `EventOptions` are internal plumbing; use [`del_by_id`](Self::del_by_id).
    #[deprecated(note = "EventOptions is internal plumbing; use `del_by_id` instead")]
    pub fn del_by_id_with_options(
        &self,
        entity_type: &str,
        id: &str,
        options: Option<EventOptions>,
    ) -> Result<(), PersistError> {
        self.del_by_id_with_origin(
            entity_type,
            id,
            Origin::from_options(&options.unwrap_or_default()),
        )
    }

    pub(crate) fn del_by_id_with_origin(
        &self,
        entity_type: &str,
        id: &str,
        origin: Origin,
    ) -> Result<(), PersistError> {
        let id_arc: Arc<str> = id.into();
        let (created_at, source) = self.local_stamp();

        let existing = self
            .registry
            .get(entity_type)
            .and_then(|store| store.get(&id_arc).get());

        crate::server::entity_set_stats::record_del(entity_type);

        // Reduce: LWW-gated remove (leaves a tombstone). No cascade here — this
        // tombstoning path has no full item to descend from.
        let stamp = LwwStamp::new(&created_at, source.as_deref(), true);
        if !self.registry.lww_del(entity_type, id_arc, stamp) {
            return Ok(());
        }

        // Search: remove from index.
        self.search_index.remove_entity(entity_type, id);

        // Persist: produce unless this origin must not.
        if origin.should_produce() {
            if let Some(item) = existing {
                self.produce_del_dyn(&item, &created_at, source.as_deref())?;
            } else {
                log::warn!(
                    "del_by_id could not persist DEL without full entity: {}:{}",
                    entity_type,
                    id
                );
            }
        }

        log::trace!("Published DEL {}:{}", entity_type, id);
        Ok(())
    }

    /// Apply a single wire event (parse -> reduce -> relationships -> persist).
    ///
    /// Returns `true` when the event was parsed and applied, `false` otherwise.
    pub fn apply_event(&self, event: MEvent) -> Result<bool, PersistError> {
        Ok(self.apply_event_batch(vec![event])? == 1)
    }

    /// Apply a batch of wire events with a single parse pass and grouped store updates.
    ///
    /// This reduces overhead versus calling `set_dyn`/`del_dyn` for each event individually.
    /// Returns the number of successfully parsed/applied events.
    pub fn apply_event_batch(&self, events: Vec<MEvent>) -> Result<usize, PersistError> {
        if events.is_empty() {
            return Ok(0);
        }

        let mut accepted = 0usize;
        let mut immediate_events = Vec::new();
        let mut buffered_by_type: HashMap<Arc<str>, (u64, Vec<MEvent>)> = HashMap::new();

        for event in events {
            match self
                .handler_registry
                .get_item_buffer_policy(&event.item_type)
            {
                IngestBufferPolicy::None => immediate_events.push(event),
                IngestBufferPolicy::TimeWindow { window_ms } => {
                    let entity_type: Arc<str> = event.item_type.clone().into();
                    buffered_by_type
                        .entry(entity_type)
                        .or_insert_with(|| (window_ms, Vec::new()))
                        .1
                        .push(event);
                }
            }
        }

        if !immediate_events.is_empty() {
            accepted += self.apply_event_batch_immediate(immediate_events)?;
        }

        for (entity_type, (window_ms, buffered_events)) in buffered_by_type {
            accepted += buffered_events.len();
            self.enqueue_buffered_events(entity_type, window_ms, buffered_events);
        }

        Ok(accepted)
    }

    fn apply_event_batch_immediate(&self, events: Vec<MEvent>) -> Result<usize, PersistError> {
        if events.is_empty() {
            return Ok(0);
        }
        let input_len = events.len();

        let mut set_entries: Vec<StampedItem> = Vec::new();
        let mut del_entries: Vec<StampedItem> = Vec::new();

        for event in events {
            let change = event.change_type;
            let created_at: Arc<str> = event.created_at.into();
            let source_id: Option<Arc<str>> = event.source_id.map(Arc::from);
            let item_type = event.item_type;
            let Some(item) = self.parse_item(&item_type, event.item) else {
                log::warn!("Unknown entity type or parse error for ingest: {item_type}");
                continue;
            };
            match change {
                MEventType::SET => set_entries.push((item, created_at, source_id)),
                MEventType::DEL => del_entries.push((item, created_at, source_id)),
            }
        }

        let applied = set_entries.len() + del_entries.len();
        if applied == 0 {
            return Ok(0);
        }

        log::trace!(
            target: "myko::server::context",
            "apply_event_batch parsed: input_events={} sets={} dels={}",
            input_len,
            set_entries.len(),
            del_entries.len()
        );

        // Ingested wire events are Local; the shared batch path groups by type,
        // LWW-gates each group, then runs the cascade/produce tail on the writes
        // that won — carrying each event's original `created_at`/`source_id`.
        self.emit_grouped(set_entries, MEventType::SET, Origin::Local)?;
        self.emit_grouped(del_entries, MEventType::DEL, Origin::Local)?;

        Ok(applied)
    }

    fn ingest_buffer_for(&self, entity_type: Arc<str>) -> Arc<BufferedIngestType> {
        self.ingest_buffers
            .entry(entity_type)
            .or_insert_with(|| Arc::new(BufferedIngestType::new()))
            .clone()
    }

    fn enqueue_buffered_events(&self, entity_type: Arc<str>, window_ms: u64, events: Vec<MEvent>) {
        let buffer = self.ingest_buffer_for(entity_type.clone());
        let should_schedule = {
            let Ok(mut state) = buffer.state.lock() else {
                log::error!(
                    "Could not acquire ingest buffer lock for entity_type={}",
                    entity_type
                );
                if let Err(e) = self.apply_event_batch_immediate(events) {
                    log::error!("Failed to apply buffered events for {}: {}", entity_type, e);
                }
                return;
            };

            state.events.extend(events);
            if state.flush_scheduled {
                false
            } else {
                state.flush_scheduled = true;
                true
            }
        };

        if !should_schedule {
            return;
        }

        let ctx = self.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(window_ms));
            ctx.flush_buffered_events_for_type(&entity_type);
        });
    }

    fn flush_buffered_events_for_type(&self, entity_type: &Arc<str>) -> usize {
        let Some(buffer) = self
            .ingest_buffers
            .get(entity_type.as_ref())
            .map(|entry| entry.clone())
        else {
            return 0;
        };

        let events = {
            let Ok(mut state) = buffer.state.lock() else {
                log::error!(
                    "Could not acquire ingest buffer lock for flush entity_type={}",
                    entity_type
                );
                return 0;
            };

            state.flush_scheduled = false;
            if state.events.is_empty() {
                return 0;
            }

            std::mem::take(&mut state.events)
        };

        log::trace!(
            target: "myko::server::context",
            "flush_buffered_events entity_type={} count={}",
            entity_type,
            events.len()
        );

        match self.apply_event_batch_immediate(events) {
            Ok(count) => count,
            Err(e) => {
                log::error!("Failed to flush buffered events for {}: {}", entity_type, e);
                0
            }
        }
    }

    #[cfg(test)]
    fn flush_all_buffered_events(&self) -> usize {
        let entity_types: Vec<Arc<str>> = self
            .ingest_buffers
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        entity_types
            .into_iter()
            .map(|entity_type| self.flush_buffered_events_for_type(&entity_type))
            .sum()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // shared emission pipeline (batch is first-class; single is a thin wrapper)
    // ─────────────────────────────────────────────────────────────────────────

    /// The LWW stamp for a locally-originated write: `created_at = now`,
    /// `source_id = this host`.
    fn local_stamp(&self) -> (Arc<str>, Option<Arc<str>>) {
        let created_at: Arc<str> = chrono::Utc::now().to_rfc3339().into();
        let source: Arc<str> = self.host_id.to_string().into();
        (created_at, Some(source))
    }

    /// Single-item LWW apply — **no allocation**. Gates the write through the
    /// store-layer LWW guard with `(created_at, source_id)`; only if it wins do
    /// we run the shared `apply_effects` tail and produce the event carrying the
    /// same stamp (so the broadcast stamp matches the applied one).
    fn apply_one(
        &self,
        item: Arc<dyn AnyItem>,
        change: MEventType,
        origin: Origin,
        created_at: Arc<str>,
        source_id: Option<Arc<str>>,
    ) -> Result<(), PersistError> {
        let entity_type = item.entity_type();
        let id = item.id();
        let stamp = LwwStamp::new(&created_at, source_id.as_deref(), change == MEventType::DEL);
        let won = match change {
            MEventType::SET => {
                crate::server::entity_set_stats::record_set(entity_type);
                self.registry.lww_set(entity_type, id, item.clone(), stamp)
            }
            MEventType::DEL => {
                crate::server::entity_set_stats::record_del(entity_type);
                self.registry.lww_del(entity_type, id, stamp)
            }
        };
        if !won {
            return Ok(());
        }
        self.apply_effects(std::slice::from_ref(&item), change, origin)?;
        if origin.should_produce() {
            self.produce_dyn(&item, change, &created_at, source_id.as_deref())?;
        }
        Ok(())
    }

    /// Batch emission (first-class). Each entry carries its own LWW stamp
    /// (`created_at` + `source_id`) — ingested wire events have heterogeneous
    /// stamps. Groups by type, gates each group through the store-layer LWW guard
    /// (one store diff per type), then runs `apply_effects` + produce on exactly
    /// the writes that won (a stale/suppressed write neither cascades nor echoes).
    fn emit_grouped(
        &self,
        entries: Vec<StampedItem>,
        change: MEventType,
        origin: Origin,
    ) -> Result<(), PersistError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut by_type: std::collections::BTreeMap<&'static str, Vec<StampedWinner>> =
            std::collections::BTreeMap::new();
        for (item, created_at, source_id) in entries {
            let entity_type = item.entity_type();
            match change {
                MEventType::SET => crate::server::entity_set_stats::record_set(entity_type),
                MEventType::DEL => crate::server::entity_set_stats::record_del(entity_type),
            }
            let stamp = LwwStamp::new(&created_at, source_id.as_deref(), change == MEventType::DEL);
            by_type.entry(entity_type).or_default().push((item, stamp));
        }

        for (entity_type, group) in by_type {
            let winners = match change {
                MEventType::SET => self.registry.lww_set_many(entity_type, group),
                MEventType::DEL => self.registry.lww_del_many(entity_type, group),
            };
            if winners.is_empty() {
                continue;
            }
            let winner_items: Vec<Arc<dyn AnyItem>> =
                winners.iter().map(|(item, _)| item.clone()).collect();
            self.apply_effects(&winner_items, change, origin)?;
            if origin.should_produce() {
                for (item, stamp) in &winners {
                    self.produce_dyn(item, change, &stamp.created_at, stamp.source_id.as_deref())?;
                }
            }
        }
        Ok(())
    }

    /// Shared post-reduce tail: search index + relationship cascade (gated by
    /// `origin`). The store reduce already ran inside the LWW guard; produce is
    /// done by the caller because it needs the per-item stamp. Items are all the
    /// same entity type.
    fn apply_effects(
        &self,
        items: &[Arc<dyn AnyItem>],
        change: MEventType,
        origin: Origin,
    ) -> Result<(), PersistError> {
        // Search: index searchable fields.
        match change {
            MEventType::SET => {
                for item in items {
                    self.search_index.index_item(item);
                }
            }
            MEventType::DEL => {
                for item in items {
                    self.search_index
                        .remove_entity(item.entity_type(), &item.id());
                }
            }
        }

        // Relationships: run cascades unless this origin must not descend.
        if origin.should_cascade(change) {
            match change {
                MEventType::SET => {
                    for item in items {
                        self.relationship_manager.forward_set(item.clone(), self)?;
                    }
                }
                MEventType::DEL => self.relationship_manager.forward_del_batch(items, self)?,
            }
        }

        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // durable backend production (private)
    // ─────────────────────────────────────────────────────────────────────────

    /// Produce a SET/DEL event carrying the applied LWW stamp.
    fn produce_dyn(
        &self,
        item: &Arc<dyn AnyItem>,
        change: MEventType,
        created_at: &str,
        source_id: Option<&str>,
    ) -> Result<(), PersistError> {
        match change {
            MEventType::SET => self.produce_set_dyn(item, created_at, source_id),
            MEventType::DEL => self.produce_del_dyn(item, created_at, source_id),
        }
    }

    fn produce_del_dyn(
        &self,
        item: &Arc<dyn AnyItem>,
        created_at: &str,
        source_id: Option<&str>,
    ) -> Result<(), PersistError> {
        if let Some(persister) = self.persisters.resolve(item.entity_type()) {
            persister.persist(MEvent::del_with_stamp(item, created_at, source_id))?;
        }
        if let Some(sink) = &self.event_sink {
            let _ = sink.send(MEvent::del_with_stamp(item, created_at, source_id));
        }
        Ok(())
    }

    fn produce_set_dyn(
        &self,
        item: &Arc<dyn AnyItem>,
        created_at: &str,
        source_id: Option<&str>,
    ) -> Result<(), PersistError> {
        if let Some(persister) = self.persisters.resolve(item.entity_type()) {
            persister.persist(MEvent::set_with_stamp(
                item.entity_type(),
                item.to_value(),
                created_at,
                source_id,
            ))?;
        }
        if let Some(sink) = &self.event_sink {
            let _ = sink.send(MEvent::set_with_stamp(
                item.entity_type(),
                item.to_value(),
                created_at,
                source_id,
            ));
        }
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Query methods
    // ─────────────────────────────────────────────────────────────────────────

    /// Run a reactive query and return a typed map keyed by the item's typed id.
    ///
    /// The typed projection is cached — multiple callers with the same query
    /// share a single underlying map instead of each creating their own copy.
    pub fn query_map<Q>(
        &self,
        query: Q,
        request: Arc<RequestContext>,
    ) -> CellMap<<Q::Item as WithTypedId>::Id, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item: Eventable
            + WithId
            + WithTypedId
            + DeserializeOwned
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + 'static,
    {
        let key = self.cache_key("query", Q::query_id_static().as_ref(), &query, &request);
        // Hold the untyped map alive so the weak ref in the cache entry stays valid.
        let untyped = self.query_map_untyped(query, request);
        if let Some(entry) = self.query_cache.get(&key)
            && let Some(typed) = entry.value().get_or_create_typed(|source| {
                typed_map_from_any_item_with_typed_id(source, "CellServerCtx::query_map")
            })
        {
            return typed;
        }
        // Concurrent cache sweep may have evicted the entry — re-insert and retry
        self.query_cache
            .insert(key.clone(), MapCacheEntry::new(&untyped));
        let entry = self.query_cache.get(&key).expect("just re-inserted");
        entry
            .value()
            .get_or_create_typed(|source| {
                typed_map_from_any_item_with_typed_id(source, "CellServerCtx::query_map")
            })
            .expect("typed projection from freshly inserted entry")
    }

    /// Run a reactive query and return a typed map keyed by canonical string ids.
    ///
    /// Prefer `query_map()` unless you specifically need string ids.
    pub fn query_map_by_str<Q>(
        &self,
        query: Q,
        request: Arc<RequestContext>,
    ) -> CellMap<Arc<str>, Arc<Q::Item>, CellImmutable>
    where
        Q: QueryParams + 'static,
        Q::Item:
            Eventable + WithId + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let key = self.cache_key("query", Q::query_id_static().as_ref(), &query, &request);
        let untyped = self.query_map_untyped(query, request);
        if let Some(entry) = self.query_cache.get(&key)
            && let Some(typed) = entry.value().get_or_create_typed(|source| {
                typed_map_arc_from_any_item(source, "CellServerCtx::query_map_by_str")
            })
        {
            return typed;
        }
        // Concurrent cache sweep may have evicted the entry — re-insert and retry
        self.query_cache
            .insert(key.clone(), MapCacheEntry::new(&untyped));
        let entry = self.query_cache.get(&key).expect("just re-inserted");
        entry
            .value()
            .get_or_create_typed(|source| {
                typed_map_arc_from_any_item(source, "CellServerCtx::query_map_by_str")
            })
            .expect("typed projection from freshly inserted entry")
    }

    /// Run a reactive query.
    ///
    /// Returns a type-erased map that updates whenever the query results change.
    /// The query's `test_entity` is applied with proper server context.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use myko::entities::server::GetPeerServers;
    /// use myko::request::RequestContext;
    /// use myko::server::CellServerCtx;
    ///
    /// fn demo(ctx: &CellServerCtx, req: Arc<RequestContext>) {
    ///     let _peer_servers = ctx.query_map_untyped(GetPeerServers {}, req);
    ///     // _peer_servers is CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>
    /// }
    /// ```
    pub fn query_map_untyped<Q>(&self, query: Q, request: Arc<RequestContext>) -> FilteredCellMap
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let key = self.cache_key("query", Q::query_id_static().as_ref(), &query, &request);

        // Fast path
        if let Some(cell) = self.try_get_cached_query(&key) {
            return cell;
        }

        let gate = self
            .compute_gates
            .entry(key.clone())
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
            .clone();
        let _lock = gate.lock().unwrap();

        // Re-check after gate
        if let Some(cell) = self.try_get_cached_query(&key) {
            return cell;
        }

        let query_req = QueryRequest::with_tx(query, request.tx.clone());
        let any_query: Arc<dyn crate::query::AnyQuery> = Arc::new(query_req);

        let built = Q::cell_factory(
            any_query,
            self.registry.clone(),
            request,
            Some(Arc::new(self.clone())),
        )
        .expect("query cell factory should not fail for typed query");
        self.query_cache.insert(key, MapCacheEntry::new(&built));
        built
    }

    fn try_get_cached_query(&self, key: &str) -> Option<FilteredCellMap> {
        let existing = self.query_cache.get(key)?;
        if let Some(shared) = existing.value().get() {
            return Some(shared);
        }
        drop(existing);
        self.query_cache.remove(key);
        None
    }

    /// Build a reactive view cell map (type-erased for framework internals).
    pub fn view_map_untyped<V>(&self, view: V, request: Arc<RequestContext>) -> FilteredViewCellMap
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let key = self.cache_key("view", V::view_id_static().as_ref(), &view, &request);

        // Fast path
        if let Some(cell) = self.try_get_cached_view(&key) {
            return cell;
        }

        let gate = self
            .compute_gates
            .entry(key.clone())
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
            .clone();
        let _lock = gate.lock().unwrap();

        // Re-check after gate
        if let Some(cell) = self.try_get_cached_view(&key) {
            return cell;
        }

        let view_req = crate::view::ViewRequest::with_tx(view, request.tx.clone());
        let any_view: Arc<dyn crate::view::AnyView> = Arc::new(view_req);

        let built = V::cell_factory(
            any_view,
            self.registry.clone(),
            request,
            Arc::new(self.clone()),
        )
        .expect("view cell factory should not fail for typed view");
        self.view_cache.insert(key, MapCacheEntry::new(&built));
        built
    }

    fn try_get_cached_view(&self, key: &str) -> Option<FilteredViewCellMap> {
        let existing = self.view_cache.get(key)?;
        if let Some(shared) = existing.value().get() {
            return Some(shared);
        }
        drop(existing);
        self.view_cache.remove(key);
        None
    }

    /// Back-compat alias for type-erased view map.
    pub fn view_map<V>(&self, view: V, request: Arc<RequestContext>) -> FilteredViewCellMap
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        self.view_map_untyped(view, request)
    }

    /// Build a typed reactive view cell map.
    pub fn view<V>(&self, view: V, request: Arc<RequestContext>) -> TypedViewCellMap<V::Item>
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let key = self.cache_key("view", V::view_id_static().as_ref(), &view, &request);
        let _untyped = self.view_map_untyped(view, request);
        if let Some(entry) = self.view_cache.get(&key)
            && let Some(typed) = entry.value().get_or_create_typed(|source| {
                typed_map_arc_from_any_item(source, "CellServerCtx::view")
            })
        {
            return typed;
        }
        unreachable!("view_map_untyped just populated the cache")
    }

    /// Get a one-shot typed entity snapshot by id.
    pub fn entity_snapshot<T>(&self, id: &<T as WithTypedId>::Id) -> Option<Arc<T>>
    where
        T: Eventable + WithTypedId + Send + Sync + 'static,
        <T as WithTypedId>::Id: hyphae::IdFor<T, MapKey = Arc<str>>,
    {
        let store = self.registry.get_or_create(T::entity_name_static());
        let map_key = id.map_key();
        let item = store.get_value(&map_key)?;
        Some(downcast_any_item_arc::<T>(
            &item,
            "CellServerCtx::entity_snapshot",
        ))
    }

    /// Get one-shot typed entity snapshots for an item type.
    pub fn entity_snapshots<T>(&self) -> Vec<Arc<T>>
    where
        T: Eventable + WithTypedId + Send + Sync + 'static,
        <T as WithTypedId>::Id: hyphae::IdFor<T, MapKey = Arc<str>>,
    {
        let store = self.registry.get_or_create(T::entity_name_static());
        store
            .snapshot()
            .into_iter()
            .map(|(_, item)| downcast_any_item_arc::<T>(&item, "CellServerCtx::entity_snapshots"))
            .collect()
    }

    /// Get one-shot typed entity snapshots for the provided ids.
    pub fn entity_snapshots_by_id<T>(
        &self,
        ids: impl IntoIterator<Item = <T as WithTypedId>::Id>,
    ) -> Vec<Arc<T>>
    where
        T: Eventable + WithTypedId + Send + Sync + 'static,
        <T as WithTypedId>::Id: hyphae::IdFor<T, MapKey = Arc<str>>,
    {
        ids.into_iter()
            .filter_map(|id| self.entity_snapshot::<T>(&id))
            .collect()
    }

    /// Run a one-shot (non-reactive) query.
    ///
    /// Iterates the store directly and returns matching entities without creating
    /// any reactive cells or subscriptions. Use this for command handlers and other
    /// contexts where you need a point-in-time snapshot, not a live query.
    pub fn query_snapshot<Q>(&self, query: Q, request: Arc<RequestContext>) -> Vec<Arc<Q::Item>>
    where
        Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let query_item_type = Q::query_item_type_static();
        let store = self.registry.get_or_create(&query_item_type);

        let query_context = Arc::new(QueryContext {
            req: request.clone(),
        });
        let query = Arc::new(query);

        store
            .snapshot()
            .into_iter()
            .filter_map(|(_, item)| {
                let typed_item =
                    downcast_any_item_arc::<Q::Item>(&item, "CellServerCtx::query_snapshot");
                let ctx = QueryTestCtx {
                    item: typed_item.clone(),
                    query: query.clone(),
                    query_context: query_context.clone(),
                };
                if Q::test_entity(ctx) {
                    Some(typed_item)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn report<R>(
        &self,
        report: R,
        request: Arc<RequestContext>,
    ) -> Cell<Arc<R::Output>, CellImmutable>
    where
        R: ReportHandler + ReportId + CacheKey + Clone + serde::Serialize + 'static,
    {
        let key = self.cache_key("report", report.report_id().as_ref(), &report, &request);
        let report_id = report.report_id();

        // Fast path: cache hit with live cell.
        if let Some(cell) = self.try_get_cached_report::<R>(&key) {
            crate::server::report_cache_stats::record_hit(&report_id);
            log::trace!(
                target: "myko::server::context::report_cache",
                "report_cache HIT report_id={} key={}",
                report_id,
                key,
            );
            return cell;
        }

        // NOTE(ts): Per-key gate prevents duplicate computation when multiple threads
        // request the same report concurrently. First thread computes, others wait.
        let gate = self
            .compute_gates
            .entry(key.clone())
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
            .clone();
        let _lock = gate.lock().unwrap();

        // Re-check after acquiring the gate — another thread may have computed while we waited.
        if let Some(cell) = self.try_get_cached_report::<R>(&key) {
            crate::server::report_cache_stats::record_hit_after_gate(&report_id);
            log::trace!(
                target: "myko::server::context::report_cache",
                "report_cache HIT_AFTER_GATE report_id={} key={}",
                report_id,
                key,
            );
            return cell;
        }

        // Emit MISS_COMPUTE *before* compute() so the analyze pass can correlate
        // the miss with the work that follows even if compute panics or hangs.
        // Payload is only serialized when the trace target is enabled.
        if log::log_enabled!(target: "myko::server::context::report_cache", log::Level::Trace) {
            let payload = serde_json::to_string(&report)
                .unwrap_or_else(|e| format!("<serialize error: {e}>"));
            log::trace!(
                target: "myko::server::context::report_cache",
                "report_cache MISS_COMPUTE report_id={} key={} payload={}",
                report_id,
                key,
                payload,
            );
        }

        let nested_ctx = ReportContext::new(request, Arc::new(self.clone()));
        // The trait returns `impl Pipeline<...>`; materialize once here so the
        // cache and downstream consumers get a concrete `Cell`. This is the only
        // materialization per report, regardless of how deep the inner chain is.
        let built = report.compute(nested_ctx).materialize();
        self.report_cache
            .insert(key.clone(), Arc::new(ReportCacheEntry::new(&built)));

        crate::server::report_cache_stats::record_miss(&report_id);

        built
    }

    /// Try to get a cached report cell. Returns None if missing or dead.
    fn try_get_cached_report<R>(&self, key: &str) -> Option<Cell<Arc<R::Output>, CellImmutable>>
    where
        R: ReportHandler + 'static,
    {
        let existing = self.report_cache.get(key)?;
        if let Some(entry) = existing
            .value()
            .as_any()
            .downcast_ref::<ReportCacheEntry<Arc<R::Output>>>()
            && let Some(shared) = entry.get()
        {
            return Some(shared);
        }
        // Dead entry — drop the ref before removing to avoid DashMap deadlock
        drop(existing);
        self.report_cache.remove(key);
        None
    }

    pub fn new_server_transaction(&self) -> Arc<RequestContext> {
        Arc::new(RequestContext {
            tx: Arc::<str>::from(Uuid::new_v4().to_string()),
            client_id: None,
            lineage: vec![],
            host_id: self.host_id,
            created_at: chrono::Utc::now().to_string(),
            windback: None,
        })
    }
}

impl std::fmt::Debug for CellServerCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellServerCtx").finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use uuid::Uuid;

    use super::CellServerCtx;
    use crate::{
        common::with_id::WithId,
        core::item::{
            AnyItem, Eventable, IngestBufferPolicy, IngestBufferRegistration, ItemRegistration,
        },
        hyphae::Gettable,
        search::SearchIndex,
        server::{HandlerRegistry, RelationshipManager, persister::PersisterRouter},
        store::StoreRegistry,
        wire::{MEvent, MEventType},
    };

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct BufferedTestItem {
        id: Arc<str>,
        value: i32,
    }

    impl WithId for BufferedTestItem {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl AnyItem for BufferedTestItem {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "BufferedTestItem"
        }

        fn equals(&self, other: &dyn AnyItem) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .map(|typed| self == typed)
                .unwrap_or(false)
        }
    }

    impl Eventable for BufferedTestItem {
        const ENTITY_NAME_STATIC: &'static str = "BufferedTestItem";
    }

    inventory::submit! {
        ItemRegistration {
            entity_type: "BufferedTestItem",
            crate_name: env!("CARGO_PKG_NAME"),
            parse: BufferedTestItem::parse,
            parse_bytes: BufferedTestItem::parse_bytes,
            serialize_json: |any| {
                let typed = any.as_any().downcast_ref::<BufferedTestItem>().unwrap();
                ::serde_json::value::to_raw_value(typed)
            },
        }
    }

    inventory::submit! {
        IngestBufferRegistration {
            entity_type: "BufferedTestItem",
            policy: IngestBufferPolicy::TimeWindow { window_ms: 60_000 },
        }
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct ImmediateTestItem {
        id: Arc<str>,
        value: i32,
    }

    impl WithId for ImmediateTestItem {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl AnyItem for ImmediateTestItem {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "ImmediateTestItem"
        }

        fn equals(&self, other: &dyn AnyItem) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .map(|typed| self == typed)
                .unwrap_or(false)
        }
    }

    impl Eventable for ImmediateTestItem {
        const ENTITY_NAME_STATIC: &'static str = "ImmediateTestItem";
    }

    inventory::submit! {
        ItemRegistration {
            entity_type: "ImmediateTestItem",
            crate_name: env!("CARGO_PKG_NAME"),
            parse: ImmediateTestItem::parse,
            parse_bytes: ImmediateTestItem::parse_bytes,
            serialize_json: |any| {
                let typed = any.as_any().downcast_ref::<ImmediateTestItem>().unwrap();
                ::serde_json::value::to_raw_value(typed)
            },
        }
    }

    fn make_ctx() -> CellServerCtx {
        CellServerCtx::new(
            Uuid::new_v4(),
            Arc::new(StoreRegistry::new()),
            Arc::new(HandlerRegistry::new()),
            Arc::new(RelationshipManager::new()),
            Arc::new(PersisterRouter::default()),
            Arc::new(SearchIndex::new()),
            Arc::new(dashmap::DashMap::new()),
            None,
            None,
        )
    }

    #[test]
    fn apply_event_batch_keeps_default_entities_immediate() {
        let ctx = make_ctx();
        let applied = ctx
            .apply_event_batch(vec![MEvent {
                item: json!({
                    "id": "immediate-1",
                    "value": 7,
                }),
                change_type: MEventType::SET,
                item_type: "ImmediateTestItem".to_string(),
                created_at: "2026-03-12T00:00:00Z".to_string(),
                tx: "tx-immediate".to_string(),
                source_id: Some("test".to_string()),
            }])
            .expect("apply_event_batch should succeed");

        assert_eq!(applied, 1);
        let store = ctx.registry.get_or_create("ImmediateTestItem");
        assert!(store.get(&Arc::<str>::from("immediate-1")).get().is_some());
    }

    #[test]
    fn apply_event_batch_buffers_opted_in_entities() {
        let ctx = make_ctx();
        let applied = ctx
            .apply_event_batch(vec![MEvent {
                item: json!({
                    "id": "buffered-1",
                    "value": 42,
                }),
                change_type: MEventType::SET,
                item_type: "BufferedTestItem".to_string(),
                created_at: "2026-03-12T00:00:00Z".to_string(),
                tx: "tx-buffered".to_string(),
                source_id: Some("test".to_string()),
            }])
            .expect("apply_event_batch should succeed");

        assert_eq!(applied, 1);
        let store = ctx.registry.get_or_create("BufferedTestItem");
        assert!(store.get(&Arc::<str>::from("buffered-1")).get().is_none());

        let flushed = ctx.flush_all_buffered_events();
        assert_eq!(flushed, 1);
        assert!(store.get(&Arc::<str>::from("buffered-1")).get().is_some());
    }

    // ── Deterministic LWW convergence (Prereq 1) ────────────────────────────

    fn set_event(id: &str, value: i32, created_at: &str, source: &str) -> MEvent {
        MEvent {
            item: json!({ "id": id, "value": value }),
            change_type: MEventType::SET,
            item_type: "ImmediateTestItem".to_string(),
            created_at: created_at.to_string(),
            tx: format!("tx-{created_at}-{source}"),
            source_id: Some(source.to_string()),
        }
    }

    fn del_event(id: &str, created_at: &str, source: &str) -> MEvent {
        MEvent {
            item: json!({ "id": id, "value": 0 }),
            change_type: MEventType::DEL,
            item_type: "ImmediateTestItem".to_string(),
            created_at: created_at.to_string(),
            tx: format!("tx-{created_at}-{source}"),
            source_id: Some(source.to_string()),
        }
    }

    fn value_of(ctx: &CellServerCtx, id: &str) -> Option<i32> {
        ctx.registry
            .get("ImmediateTestItem")
            .and_then(|store| store.get(&Arc::<str>::from(id)).get())
            .and_then(|item| {
                item.as_any()
                    .downcast_ref::<ImmediateTestItem>()
                    .map(|t| t.value)
            })
    }

    /// Two nodes that apply the same conflicting writes in opposite orders must
    /// converge to the same value (the one with the higher `created_at`).
    #[test]
    fn lww_converges_regardless_of_apply_order() {
        let lo = set_event("x", 1, "2026-06-18T00:00:00Z", "A");
        let hi = set_event("x", 2, "2026-06-18T00:00:01Z", "B");

        let a = make_ctx();
        a.apply_event(lo.clone()).unwrap();
        a.apply_event(hi.clone()).unwrap();

        let b = make_ctx();
        b.apply_event(hi).unwrap();
        b.apply_event(lo).unwrap();

        assert_eq!(value_of(&a, "x"), Some(2));
        assert_eq!(
            value_of(&b, "x"),
            Some(2),
            "both orders converge to the newer write"
        );
    }

    /// A tombstone suppresses a stale SET (no resurrection) but a genuinely
    /// newer SET still wins over it.
    #[test]
    fn lww_tombstone_suppresses_stale_set_but_allows_newer() {
        let ctx = make_ctx();
        ctx.apply_event(set_event("y", 1, "2026-06-18T00:00:00Z", "A"))
            .unwrap();
        ctx.apply_event(del_event("y", "2026-06-18T00:00:03Z", "A"))
            .unwrap();
        assert_eq!(value_of(&ctx, "y"), None, "deleted");

        // Older than the tombstone → suppressed.
        ctx.apply_event(set_event("y", 2, "2026-06-18T00:00:02Z", "A"))
            .unwrap();
        assert_eq!(value_of(&ctx, "y"), None, "stale SET must not resurrect");

        // Newer than the tombstone → wins.
        ctx.apply_event(set_event("y", 3, "2026-06-18T00:00:04Z", "A"))
            .unwrap();
        assert_eq!(value_of(&ctx, "y"), Some(3), "newer SET resurrects");
    }

    /// Re-delivering the identical event (gossip ∪ anti-entropy) is a no-op.
    #[test]
    fn lww_idempotent_redelivery() {
        let ctx = make_ctx();
        let ev = set_event("z", 5, "2026-06-18T00:00:00Z", "A");
        ctx.apply_event(ev.clone()).unwrap();
        ctx.apply_event(ev).unwrap();
        assert_eq!(value_of(&ctx, "z"), Some(5));
    }
}
