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
    Watchable, WeakCellMap,
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
        FilteredCellMap, LiveFilterQuery, QueryContext, QueryFactory, QueryHandler, QueryParams,
        QueryRequest, QueryTestCtx,
    },
    report::{ReportContext, ReportHandler, ReportId},
    request::RequestContext,
    search::SearchIndex,
    store::StoreRegistry,
    view::{FilteredViewCellMap, TypedViewCellMap, ViewFactory},
    wire::{EventOptions, MEvent, MEventType},
};

type AnyItemArc = Arc<dyn AnyItem>;

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

    /// Remove dead weak-ref entries from all caches, including belongs-to
    /// source index buckets (process-global, not per-`CellServerCtx`, but
    /// swept from here for hosting apps that already call this
    /// periodically). Bucket entries are also reaped lazily on next access
    /// regardless — this is a backstop for foreign ids that go dead and are
    /// never looked up again.
    pub fn sweep_dead_cache_entries(&self) {
        self.query_cache
            .retain(|_, entry| entry.weak.upgrade().is_some());
        self.view_cache
            .retain(|_, entry| entry.weak.upgrade().is_some());
        self.report_cache.retain(|_, entry| entry.is_alive());
        crate::query::sweep_all_belongs_to_source_indexes();
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
    pub(crate) fn set_with_origin<T>(&self, entity: &T, origin: Origin) -> Result<(), PersistError>
    where
        T: Eventable + 'static,
    {
        let item: Arc<dyn AnyItem> = Arc::new(entity.clone());
        self.reduce_one(&item, MEventType::SET);
        self.apply_effects(std::slice::from_ref(&item), MEventType::SET, origin)
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

    pub(crate) fn del_with_origin<T>(&self, entity: &T, origin: Origin) -> Result<(), PersistError>
    where
        T: Eventable + Clone + 'static,
    {
        let item: Arc<dyn AnyItem> = Arc::new(entity.clone());
        self.reduce_one(&item, MEventType::DEL);
        self.apply_effects(std::slice::from_ref(&item), MEventType::DEL, origin)
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
        let items: Vec<Arc<dyn AnyItem>> = entities
            .iter()
            .map(|e| Arc::new(e.clone()) as Arc<dyn AnyItem>)
            .collect();
        self.emit_grouped(&items, MEventType::SET, origin)
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
        let items: Vec<Arc<dyn AnyItem>> = entities
            .iter()
            .map(|e| Arc::new(e.clone()) as Arc<dyn AnyItem>)
            .collect();
        self.emit_grouped(&items, MEventType::DEL, origin)
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
        self.reduce_one(&item, MEventType::SET);
        self.apply_effects(std::slice::from_ref(&item), MEventType::SET, origin)
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
        self.emit_grouped(items, MEventType::SET, origin)
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
        self.reduce_one(&item, MEventType::DEL);
        self.apply_effects(std::slice::from_ref(&item), MEventType::DEL, origin)
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
        self.emit_grouped(items, MEventType::DEL, origin)
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

        let existing = self
            .registry
            .get(entity_type)
            .and_then(|store| store.get(&id_arc).get());

        crate::server::entity_set_stats::record_del(entity_type);

        // Reduce: remove from store
        self.registry.get_or_create(entity_type).remove(&id_arc);

        // Search: remove from index
        self.search_index.remove_entity(entity_type, id);

        // Persist: produce unless this origin must not (e.g. a peer tombstone).
        if origin.should_produce() {
            if let Some(item) = existing {
                self.produce_del_dyn(&item)?;
            } else {
                tracing::warn!(
                    "del_by_id could not persist DEL without full entity: {}:{}",
                    entity_type,
                    id
                );
            }
        }

        tracing::trace!("Published DEL {}:{}", entity_type, id);
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

        let mut set_items: Vec<Arc<dyn AnyItem>> = Vec::new();
        let mut del_items: Vec<Arc<dyn AnyItem>> = Vec::new();

        for event in events {
            let change = event.change_type;
            let item_type = event.item_type;
            let item_value = event.item;
            let Some(item) = self.parse_item(&item_type, item_value) else {
                tracing::warn!("Unknown entity type or parse error for ingest: {item_type}");
                continue;
            };
            match change {
                MEventType::SET => set_items.push(item),
                MEventType::DEL => del_items.push(item),
            }
        }

        let applied = set_items.len() + del_items.len();
        if applied == 0 {
            return Ok(0);
        }

        tracing::trace!(
            target: "myko::server::context",
            "apply_event_batch parsed: input_events={} sets={} dels={}",
            input_len,
            set_items.len(),
            del_items.len()
        );

        // Ingested wire events are Local (cascade + produce); the shared batch
        // path groups by type, reduces, then runs the cascade/produce tail.
        // `emit_grouped` itself opens the `hyphae::batch` window (scoped to
        // just its reduce loop — see the comment there for why).
        let emit = || -> Result<(), PersistError> {
            self.emit_grouped(&set_items, MEventType::SET, Origin::Local)?;
            self.emit_grouped(&del_items, MEventType::DEL, Origin::Local)?;
            Ok(())
        };
        #[cfg(feature = "profiling")]
        {
            hyphae::profiling::pass(emit)?;
            if let Some(report) = hyphae::profiling::take_report() {
                tracing::trace!(
                    target: "myko::server::context",
                    "apply_event_batch fanout: cells_fired={} total_fires={} total_refires={} coalesceable_fraction={:.3}",
                    report.cells_fired(),
                    report.total_fires(),
                    report.total_refires(),
                    report.coalesceable_fraction()
                );
            }
        }
        #[cfg(not(feature = "profiling"))]
        emit()?;

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
                tracing::error!(
                    "Could not acquire ingest buffer lock for entity_type={}",
                    entity_type
                );
                if let Err(e) = self.apply_event_batch_immediate(events) {
                    tracing::error!("Failed to apply buffered events for {}: {}", entity_type, e);
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
                tracing::error!(
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

        tracing::trace!(
            target: "myko::server::context",
            "flush_buffered_events entity_type={} count={}",
            entity_type,
            events.len()
        );

        match self.apply_event_batch_immediate(events) {
            Ok(count) => count,
            Err(e) => {
                tracing::error!("Failed to flush buffered events for {}: {}", entity_type, e);
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

    /// Single-item store reduce — **no allocation**. Records the stat and applies
    /// the store insert/remove for one item. Paired with
    /// `apply_effects(slice::from_ref(&item), …)` by the single-item entry points
    /// so a single mutation never allocates a Vec or groups by type.
    fn reduce_one(&self, item: &Arc<dyn AnyItem>, change: MEventType) {
        let entity_type = item.entity_type();
        // Every SET/DEL entry point (typed set/del, batch_*, apply_event,
        // set_dyn/del_dyn) funnels through here — the true root of a fanout
        // cascade. One span here, discriminated by entity_type (bounded
        // cardinality — never per-instance id), lets a span-based profiler
        // (e.g. a tracing-Tracy layer) show the whole downstream
        // `hyphae.fanout` subtree nested under one legible typed zone
        // instead of an anonymous root.
        let _span = tracing::trace_span!("myko.reduce", ty = entity_type, op = ?change).entered();
        match change {
            MEventType::SET => {
                crate::server::entity_set_stats::record_set(entity_type);
                self.registry
                    .get_or_create(entity_type)
                    .insert(item.id(), item.clone());
            }
            MEventType::DEL => {
                crate::server::entity_set_stats::record_del(entity_type);
                self.registry.get_or_create(entity_type).remove(&item.id());
            }
        }
    }

    /// The batch emission path (first-class). Groups `items` by entity type,
    /// applies one grouped store reduce per type (a single store diff each) for
    /// **all** groups before any cascade runs, then runs the shared
    /// `apply_effects` tail per (same-type) group.
    ///
    /// Every batch entry point and the wire-ingest path funnel through here. The
    /// single-item entry points deliberately do **not** — they call
    /// `reduce_one` + `apply_effects` directly to avoid the grouping/Vec cost.
    fn emit_grouped(
        &self,
        items: &[Arc<dyn AnyItem>],
        change: MEventType,
        origin: Origin,
    ) -> Result<(), PersistError> {
        if items.is_empty() {
            return Ok(());
        }

        let mut by_type: std::collections::BTreeMap<&'static str, Vec<Arc<dyn AnyItem>>> =
            std::collections::BTreeMap::new();
        for item in items {
            by_type
                .entry(item.entity_type())
                .or_default()
                .push(item.clone());
        }

        // Reduce: one store diff per type, across all groups, before any cascade
        // (so the store is fully settled — load-bearing for transitive cascade).
        //
        // Wrapped in `hyphae::batch` so N distinct types' stores settle in one
        // glitch-free drain instead of firing eagerly per type — but scoped to
        // *only* this loop, not the `apply_effects` tail below. `by_type`
        // guarantees each type's `diffs_cell` is set at most once in this loop,
        // which is the invariant `batch`'s last-write-wins coalescing needs
        // (`diffs_cell` carries diff *events*, not latest-value state, and
        // isn't `no_coalesce`-stamped — two sets to the same one in one window
        // silently drops the first). `apply_effects` runs after this batch has
        // already drained, specifically because cascades recurse back into
        // `emit_grouped` (e.g. `handle_belongs_to_cascade_batch` ->
        // `publish_del_cascade_batch` -> `batch_del_dyn_with_origin` ->
        // `emit_grouped`) and could touch a type already reduced in this same
        // window — running effects outside the batch means that recursive call
        // opens its own fresh window instead of joining (and colliding with)
        // this one.
        // Wrapped in `hyphae::batch` so N distinct types' stores settle in one
        // glitch-free drain instead of firing eagerly per type — but scoped to
        // *only* this loop, not the `apply_effects` tail below. `by_type`
        // guarantees each type's `diffs_cell` is set at most once in this loop,
        // which is the invariant `batch`'s last-write-wins coalescing needs
        // (`diffs_cell` carries diff *events*, not latest-value state, and
        // isn't `no_coalesce`-stamped — two sets to the same one in one window
        // silently drops the first). `apply_effects` runs after this batch has
        // already drained, specifically because cascades recurse back into
        // `emit_grouped` (e.g. `handle_belongs_to_cascade_batch` ->
        // `publish_del_cascade_batch` -> `batch_del_dyn_with_origin` ->
        // `emit_grouped`) and could touch a type already reduced in this same
        // window — running effects outside the batch means that recursive call
        // opens its own fresh window instead of joining (and colliding with)
        // this one.
        hyphae::batch(|| {
            for (entity_type, group) in &by_type {
                let store = self.registry.get_or_create(entity_type);
                match change {
                    MEventType::SET => {
                        let mut entries: Vec<(Arc<str>, Arc<dyn AnyItem>)> =
                            Vec::with_capacity(group.len());
                        for item in group {
                            crate::server::entity_set_stats::record_set(entity_type);
                            entries.push((item.id(), item.clone()));
                        }
                        store.insert_many(entries);
                    }
                    MEventType::DEL => {
                        let mut ids: Vec<Arc<str>> = Vec::with_capacity(group.len());
                        for item in group {
                            crate::server::entity_set_stats::record_del(entity_type);
                            ids.push(item.id());
                        }
                        store.remove_many(ids);
                    }
                }
            }
        });

        // Effects: search + cascade + produce, per same-type group.
        for group in by_type.values() {
            self.apply_effects(group, change, origin)?;
        }
        Ok(())
    }

    /// Shared post-reduce tail: search index, relationship cascade (gated by
    /// `origin`), and produce (gated by `origin`).
    ///
    /// Operates on a slice of items **of the same entity type** whose store
    /// reduce has already run. Single-item callers pass `slice::from_ref(&item)`
    /// (zero alloc); `emit_grouped` passes each type-group. The type-erased
    /// produce path is equivalent to the typed one (`MEvent::from_item` ≡
    /// `MEvent::set_from_value(item.to_value())`, modulo the fresh `created_at`/`tx`).
    fn apply_effects(
        &self,
        items: &[Arc<dyn AnyItem>],
        change: MEventType,
        origin: Origin,
    ) -> Result<(), PersistError> {
        // Separate from `myko.reduce` so the relationship-cascade/persist
        // tail is distinguishable from the direct state-cell write in a
        // profiler trace. `items` is always a single entity-type group by
        // the time it reaches here (see the doc comment above).
        let _span = tracing::trace_span!(
            "myko.apply_effects",
            ty = items.first().map(|i| i.entity_type()).unwrap_or("empty"),
            op = ?change,
        )
        .entered();

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

        // Persist: produce to persisters + sink unless this origin must not.
        if origin.should_produce() {
            match change {
                MEventType::SET => {
                    for item in items {
                        self.produce_set_dyn(item)?;
                    }
                }
                MEventType::DEL => {
                    for item in items {
                        self.produce_del_dyn(item)?;
                    }
                }
            }
        }

        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // durable backend production (private)
    // ─────────────────────────────────────────────────────────────────────────

    fn produce_del_dyn(&self, item: &Arc<dyn AnyItem>) -> Result<(), PersistError> {
        if let Some(persister) = self.persisters.resolve(item.entity_type()) {
            let event = MEvent::del_from_any(item, &self.host_id.to_string());
            persister.persist(event)?;
        }
        if let Some(sink) = &self.event_sink {
            let event = MEvent::del_from_any(item, &self.host_id.to_string());
            let _ = sink.send(event);
        }
        Ok(())
    }

    fn produce_set_dyn(&self, item: &Arc<dyn AnyItem>) -> Result<(), PersistError> {
        if let Some(persister) = self.persisters.resolve(item.entity_type()) {
            let event = MEvent::set_from_value(
                item.entity_type(),
                item.to_value(),
                &self.host_id.to_string(),
            );
            persister.persist(event)?;
        }
        if let Some(sink) = &self.event_sink {
            let event = MEvent::set_from_value(
                item.entity_type(),
                item.to_value(),
                &self.host_id.to_string(),
            );
            let _ = sink.send(event);
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

    /// [`Self::query_map`]'s twin for advanced (`GetXsByFilter`) queries —
    /// see docs/superpowers/specs/2026-07-13-advanced-query-design.md §3. A
    /// thin wrapper: `GetXsByFilter` flows through the same generic
    /// `QueryParams` path as any other query type (already verified end to
    /// end against `query_map` directly), so this exists purely as a named,
    /// discoverable seam rather than adding new dispatch logic.
    pub fn query_map_filtered<Q>(
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
        self.query_map(query, request)
    }

    /// Reactive filter parameters (phase 2 of the advanced-query-design
    /// spec, §5): `filter_cell` replaces a value-based `GetXsByFilter` with
    /// a live `Cell`, so a filter derived from other cells no longer needs
    /// a `switch_map` wrapper — see `query::query_live` for the mechanics
    /// (incremental bucket-diffing on `In`/`Eq` field changes, scoped
    /// rescan on `Range`/`Contains` changes, never a graph teardown).
    ///
    /// Deliberately uncached, unlike `query_map`'s typed-projection cache:
    /// a `Cell` is object identity, not a value, so there's no meaningful
    /// key to share a projection under — each call site gets its own
    /// independent graph node (spec §5: "no value-identity cache sharing").
    pub fn query_live<F>(
        &self,
        filter_cell: impl Watchable<F>,
    ) -> CellMap<<F::Item as WithTypedId>::Id, Arc<F::Item>, CellImmutable>
    where
        F: LiveFilterQuery,
        F::Item: WithTypedId,
    {
        let untyped = crate::query::query_live(self.registry.clone(), self.host_id, filter_cell);
        typed_map_from_any_item_with_typed_id(untyped, "CellServerCtx::query_live")
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
        self.query_cache
            .insert(key.clone(), MapCacheEntry::new(&built));
        // The gate's only job was deduping concurrent first-computation; once
        // the cache entry above is visible, any racing caller's re-check
        // (line ~1268 above) will hit it directly, gate or no gate. Removing
        // it here — rather than never, which is a compute_gates memory leak
        // that grows with every distinct query/param combination ever
        // computed — is safe regardless of ordering relative to `_lock`'s
        // drop, since a fresh gate + a cache hit on re-check behaves
        // identically to blocking on the old gate.
        self.compute_gates.remove(&key);
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
        self.view_cache
            .insert(key.clone(), MapCacheEntry::new(&built));
        // See the matching comment in `query_map_untyped` — the gate is only
        // needed to dedupe concurrent first-computation, not after the cache
        // entry above is visible.
        self.compute_gates.remove(&key);
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
            tracing::trace!(
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
            tracing::trace!(
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
        if tracing::enabled!(target: "myko::server::context::report_cache", tracing::Level::TRACE) {
            let payload = serde_json::to_string(&report)
                .unwrap_or_else(|e| format!("<serialize error: {e}>"));
            tracing::trace!(
                target: "myko::server::context::report_cache",
                "report_cache MISS_COMPUTE report_id={} key={} payload={}",
                report_id,
                key,
                payload,
            );
        }

        // Bounded cardinality (one name per report *type*, not per invocation),
        // matching the `myko.reduce`/`myko.command` spans — this is the one-time
        // cache-miss materialization, not a per-subscriber-update hot path.
        let _span = tracing::trace_span!("myko.report", report = report_id.as_ref()).entered();
        crate::server::dispatch_metrics::record_report(report_id.as_ref(), request.origin());
        let nested_ctx = ReportContext::new(request, Arc::new(self.clone()));
        // The trait returns `impl Pipeline<...>`; materialize once here so the
        // cache and downstream consumers get a concrete `Cell`. This is the only
        // materialization per report, regardless of how deep the inner chain is.
        let built = report.compute(nested_ctx).materialize();
        // Named by report id (bounded cardinality — one name per report
        // *type*, not per invocation) so hyphae's `hyphae.fanout` span
        // (under the `profiling` feature) surfaces `cell.name` instead of
        // being anonymous. `Cell<T, CellImmutable>::with_name` is available
        // post-materialize (unlike `CellMap`, which only exposes it pre-lock
        // — query/view result maps can't be named at this seam the same way).
        #[cfg(feature = "profiling")]
        let built = built.with_name(report_id.as_ref());
        self.report_cache
            .insert(key.clone(), Arc::new(ReportCacheEntry::new(&built)));
        // See the matching comment in `query_map_untyped` — the gate is only
        // needed to dedupe concurrent first-computation, not after the cache
        // entry above is visible.
        self.compute_gates.remove(&key);

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
        test_util::scheduler_test_serial,
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
        let _serial = scheduler_test_serial();
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
        let _serial = scheduler_test_serial();
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

    #[test]
    fn apply_event_batch_delivers_both_diffs_for_mixed_set_and_del_same_type() {
        // Regression test for the hazard documented on `emit` in
        // `apply_event_batch_immediate`: CellMap's `diffs_cell` coalesces
        // last-write-wins like any other cell under `hyphae::batch`, so a
        // single shared batch window across the SET and DEL groups would
        // silently drop whichever of the two diffs isn't last. Each
        // `emit_grouped` call now opens its own batch window instead, so a
        // wire batch mixing a SET and a DEL of the *same* entity type must
        // still deliver both diffs to subscribers.
        let _serial = scheduler_test_serial();
        let ctx = make_ctx();

        ctx.apply_event_batch(vec![MEvent {
            item: json!({ "id": "old-1", "value": 1 }),
            change_type: MEventType::SET,
            item_type: "ImmediateTestItem".to_string(),
            created_at: "2026-03-12T00:00:00Z".to_string(),
            tx: "tx-seed".to_string(),
            source_id: Some("test".to_string()),
        }])
        .expect("seed apply_event_batch should succeed");

        let store = ctx.registry.get_or_create("ImmediateTestItem");
        let diffs_seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let diffs_seen_for_closure = diffs_seen.clone();
        let _guard = store.subscribe_diffs(move |diff| {
            diffs_seen_for_closure
                .lock()
                .unwrap()
                .push(format!("{diff:?}"));
        });
        // subscribe_diffs replays the current snapshot synchronously on
        // subscribe -- drop that so only diffs from the batch below count.
        diffs_seen.lock().unwrap().clear();

        let applied = ctx
            .apply_event_batch(vec![
                MEvent {
                    item: json!({ "id": "new-1", "value": 2 }),
                    change_type: MEventType::SET,
                    item_type: "ImmediateTestItem".to_string(),
                    created_at: "2026-03-12T00:00:01Z".to_string(),
                    tx: "tx-mixed".to_string(),
                    source_id: Some("test".to_string()),
                },
                MEvent {
                    item: json!({ "id": "old-1", "value": 1 }),
                    change_type: MEventType::DEL,
                    item_type: "ImmediateTestItem".to_string(),
                    created_at: "2026-03-12T00:00:01Z".to_string(),
                    tx: "tx-mixed".to_string(),
                    source_id: Some("test".to_string()),
                },
            ])
            .expect("mixed apply_event_batch should succeed");

        assert_eq!(applied, 2);
        assert!(store.get(&Arc::<str>::from("new-1")).get().is_some());
        assert!(store.get(&Arc::<str>::from("old-1")).get().is_none());

        let seen = diffs_seen.lock().unwrap();
        assert_eq!(
            seen.len(),
            2,
            "both the SET and DEL diffs must reach subscribers, not just the last one: {:?}",
            *seen
        );
    }

    #[test]
    fn compute_gates_does_not_leak_after_cache_populates() {
        // compute_gates only exists to dedupe concurrent first-computation
        // (see the comment on the removal call in query_map_untyped); once
        // the corresponding cache entry lands, the gate must not linger —
        // otherwise every distinct (kind, id, param-hash) ever computed
        // leaves a permanent entry, unbounded over the process lifetime.
        use crate::{entities::server::GetPeerServers, request::RequestContext};

        let _serial = scheduler_test_serial();
        let ctx = make_ctx();
        let request = Arc::new(RequestContext::internal(
            Arc::<str>::from(Uuid::new_v4().to_string()),
            ctx.host_id,
            "test",
        ));

        // First call: cache miss — populates compute_gates transiently, then
        // must remove it once query_cache is populated.
        let _ = ctx.query_map_untyped(GetPeerServers {}, request.clone());
        assert!(
            ctx.compute_gates.is_empty(),
            "compute_gates must be empty once the query cache is populated, got {:?}",
            ctx.compute_gates
        );

        // Second call: cache hit on the fast path — must not touch
        // compute_gates at all.
        let _ = ctx.query_map_untyped(GetPeerServers {}, request);
        assert!(ctx.compute_gates.is_empty());
    }

    #[test]
    fn compute_gates_does_not_leak_after_report_cache_populates() {
        // Same invariant as compute_gates_does_not_leak_after_cache_populates,
        // exercised through the report() call site's independent gate-removal
        // (a separate line, since it inserts into report_cache instead of
        // query_cache — verified both were fixed, not just the query one).
        use crate::{
            entities::client::{ClientId, ClientStatus},
            request::RequestContext,
        };

        let _serial = scheduler_test_serial();
        let ctx = make_ctx();
        let request = Arc::new(RequestContext::internal(
            Arc::<str>::from(Uuid::new_v4().to_string()),
            ctx.host_id,
            "test",
        ));

        let report = ClientStatus {
            client_id: ClientId::from(Arc::<str>::from("test-client")),
        };
        let _ = ctx.report(report.clone(), request.clone());
        assert!(
            ctx.compute_gates.is_empty(),
            "compute_gates must be empty once the report cache is populated, got {:?}",
            ctx.compute_gates
        );

        let _ = ctx.report(report, request);
        assert!(ctx.compute_gates.is_empty());
    }
}
