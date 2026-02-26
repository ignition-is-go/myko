//! Server context for the cell-based server.
//!
//! Provides modules (like PeerRegistry) with the ability to:
//! - Run reactive queries (like GetPeerServers)
//! - Publish entities (Reduce → Relationships → Persist)
//! - Access server identity (host_id)

use std::{any::Any, collections::HashMap, sync::Arc};

use dashmap::DashMap;
use hypha::{Cell, CellImmutable, CellMutable, Gettable, MapExt, Mutable, WeakCellMap};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::{HandlerRegistry, RelationshipManager, persister::PersisterRouter};
use crate::{
    client::{ConnectionStatus, MykoClient},
    core::item::{AnyItem, Eventable, typed_map_from_any_item},
    query::{
        FilteredCellMap, QueryContext, QueryFactory, QueryHandler, QueryParams, QueryRequest,
        QueryTestCtx,
    },
    report::{ReportContext, ReportHandler, ReportId},
    request::RequestContext,
    search::SearchIndex,
    store::StoreRegistry,
    view::{FilteredViewCellMap, TypedViewCellMap, ViewFactory},
    wire::{EventOptions, MEvent, MEventType},
};

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
    /// Top-level cache for reactive query maps.
    query_cache: Arc<DashMap<String, WeakCellMap<Arc<str>, Arc<dyn AnyItem>>>>,
    /// Top-level cache for reactive view maps.
    view_cache: Arc<DashMap<String, WeakCellMap<Arc<str>, Arc<dyn AnyItem>>>>,
    /// Top-level cache for reactive report cells (typed weak cells behind Any).
    report_cache: Arc<DashMap<String, Arc<dyn Any + Send + Sync>>>,
}

impl CellServerCtx {
    /// Create a new server context.
    pub fn new(
        host_id: Uuid,
        registry: Arc<StoreRegistry>,
        handler_registry: Arc<HandlerRegistry>,
        relationship_manager: Arc<RelationshipManager>,
        persisters: Arc<PersisterRouter>,
        search_index: Arc<SearchIndex>,
        peer_clients: Arc<DashMap<Arc<str>, Arc<MykoClient>>>,
        event_sink: Option<flume::Sender<MEvent>>,
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
            query_cache: Arc::new(DashMap::new()),
            view_cache: Arc::new(DashMap::new()),
            report_cache: Arc::new(DashMap::new()),
        }
    }

    fn cache_key<T: serde::Serialize>(
        &self,
        kind: &str,
        id: &str,
        params: &T,
        request: &RequestContext,
    ) -> String {
        let payload = serde_json::to_string(params).unwrap_or_else(|_| "<serde_error>".to_string());
        format!("{}:{kind}:{id}:{payload}", request.host_id)
    }

    /// Get the search index.
    pub fn search_index(&self) -> &Arc<SearchIndex> {
        &self.search_index
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

    /// Parse JSON to a typed entity using the registered item parser.
    ///
    /// Returns None if the entity type is not registered or parsing fails.
    pub fn parse_item(
        &self,
        entity_type: &str,
        json: &serde_json::Value,
    ) -> Option<Arc<dyn AnyItem>> {
        let parse = self.handler_registry.get_item_parser(entity_type)?;
        parse(json.clone()).ok()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Typed entity publishing (for server modules)
    // ─────────────────────────────────────────────────────────────────────────

    /// Publish an entity (SET) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn set<T>(&self, entity: &T)
    where
        T: Eventable + 'static,
    {
        self.set_with_options(entity, None);
    }

    /// Publish an entity (SET) with options.
    ///
    /// Options control:
    /// - `prevent_relationship_updates`: skip cascade processing
    /// - `prevent_persist`: skip Kafka
    pub fn set_with_options<T>(&self, entity: &T, options: Option<EventOptions>)
    where
        T: Eventable + 'static,
    {
        let options = options.unwrap_or_default();
        let id = entity.id();
        let entity_type = entity.entity_type();
        let item: Arc<dyn AnyItem> = Arc::new(entity.clone());

        // Reduce: update store
        self.registry
            .get_or_create(entity_type)
            .insert(id.clone(), item.clone());

        // Search: index searchable fields
        self.search_index.index_item(&item);

        // Relationships: process cascades (unless prevented)
        if !options.prevent_relationship_updates {
            self.relationship_manager.forward_set(item, self);
        }

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            self.produce_set(entity);
        }

        log::trace!("Published SET {}", id);
    }

    /// Delete an entity (DEL) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn del<T>(&self, entity: &T)
    where
        T: Eventable + Clone + 'static,
    {
        self.del_with_options(entity, None);
    }

    /// Delete an entity (DEL) with options.
    pub fn del_with_options<T>(&self, entity: &T, options: Option<EventOptions>)
    where
        T: Eventable + Clone + 'static,
    {
        let options = options.unwrap_or_default();
        let entity_type = entity.entity_type();
        let id = entity.id();
        let item: Arc<dyn AnyItem> = Arc::new(entity.clone());

        // Reduce: remove from store
        self.registry.get_or_create(entity_type).remove(&id);

        // Search: remove from index
        self.search_index.remove_entity(&id);

        // Relationships: process cascades (unless prevented)
        if !options.prevent_relationship_updates {
            self.relationship_manager.forward_del(item.clone(), self);
        }

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            self.produce_del(entity);
        }

        log::trace!("Published DEL {}:{}", entity_type, id);
    }

    /// Publish a batch of entities (SET) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn batch_set<T>(&self, entities: &[T])
    where
        T: Eventable + Clone + 'static,
    {
        self.batch_set_with_options(entities, None);
    }

    /// Publish a batch of entities (SET) with shared options.
    ///
    /// This avoids manual `MEvent` construction and performs a grouped store insert.
    pub fn batch_set_with_options<T>(&self, entities: &[T], options: Option<EventOptions>)
    where
        T: Eventable + Clone + 'static,
    {
        if entities.is_empty() {
            return;
        }

        let options = options.unwrap_or_default();
        let entity_type = T::entity_name_static();
        let store = self.registry.get_or_create(entity_type);

        let mut entries: Vec<(Arc<str>, Arc<dyn AnyItem>)> = Vec::with_capacity(entities.len());
        let mut items: Vec<Arc<dyn AnyItem>> = Vec::with_capacity(entities.len());

        for entity in entities {
            let item: Arc<dyn AnyItem> = Arc::new(entity.clone());
            self.search_index.index_item(&item);
            entries.push((entity.id(), item.clone()));
            items.push(item);
        }

        // Reduce: one diff emission for the whole batch.
        store.insert_many(entries);

        // Relationships: process cascades (unless prevented)
        if !options.prevent_relationship_updates {
            for item in &items {
                self.relationship_manager.forward_set(item.clone(), self);
            }
        }

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            for item in &items {
                self.produce_set_dyn(item);
            }
        }

        log::trace!("Published batch SET {} count={}", entity_type, items.len());
    }

    /// Delete a batch of entities (DEL) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn batch_del<T>(&self, entities: &[T])
    where
        T: Eventable + Clone + 'static,
    {
        self.batch_del_with_options(entities, None);
    }

    /// Delete a batch of entities (DEL) with shared options.
    ///
    /// This avoids manual `MEvent` construction and performs a grouped store remove.
    pub fn batch_del_with_options<T>(&self, entities: &[T], options: Option<EventOptions>)
    where
        T: Eventable + Clone + 'static,
    {
        if entities.is_empty() {
            return;
        }

        let options = options.unwrap_or_default();
        let entity_type = T::entity_name_static();
        let store = self.registry.get_or_create(entity_type);

        let mut ids: Vec<Arc<str>> = Vec::with_capacity(entities.len());
        let mut items: Vec<Arc<dyn AnyItem>> = Vec::with_capacity(entities.len());

        for entity in entities {
            let item: Arc<dyn AnyItem> = Arc::new(entity.clone());
            let id = item.id();
            self.search_index.remove_entity(&id);
            ids.push(id);
            items.push(item);
        }

        // Reduce: one diff emission for the whole batch.
        store.remove_many(ids);

        // Relationships: process cascades (unless prevented)
        if !options.prevent_relationship_updates {
            for item in &items {
                self.relationship_manager.forward_del(item.clone(), self);
            }
        }

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            for item in &items {
                self.produce_del_dyn(item);
            }
        }

        log::trace!("Published batch DEL {} count={}", entity_type, items.len());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dynamic item publishing (for parsed JSON)
    // ─────────────────────────────────────────────────────────────────────────

    /// Publish a dynamic item (SET) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn set_dyn(&self, item: Arc<dyn AnyItem>) {
        self.set_dyn_with_options(item, None);
    }

    /// Publish a dynamic item (SET) with options.
    pub fn set_dyn_with_options(&self, item: Arc<dyn AnyItem>, options: Option<EventOptions>) {
        let options = options.unwrap_or_default();
        let entity_type = item.entity_type();
        let id = item.id();

        // Reduce: update store
        self.registry
            .get_or_create(entity_type)
            .insert(id.clone(), item.clone());

        // Search: index searchable fields
        self.search_index.index_item(&item);

        // Relationships: process cascades (unless prevented)
        if !options.prevent_relationship_updates {
            self.relationship_manager.forward_set(item.clone(), self);
        }

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            self.produce_set_dyn(&item);
        }

        log::trace!("Published SET {}:{}", entity_type, id);
    }

    /// Delete a dynamic item (DEL) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn del_dyn(&self, item: Arc<dyn AnyItem>) {
        self.del_dyn_with_options(item, None);
    }

    /// Delete a dynamic item (DEL) with options.
    pub fn del_dyn_with_options(&self, item: Arc<dyn AnyItem>, options: Option<EventOptions>) {
        let options = options.unwrap_or_default();
        let entity_type = item.entity_type();
        let id = item.id();

        // Reduce: remove from store
        self.registry.get_or_create(entity_type).remove(&id);

        // Search: remove from index
        self.search_index.remove_entity(&id);

        // Relationships: process cascades (unless prevented)
        if !options.prevent_relationship_updates {
            self.relationship_manager.forward_del(item.clone(), self);
        }

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            self.produce_del_dyn(&item);
        }

        log::trace!("Published DEL {}:{}", entity_type, id);
    }

    /// Delete an entity by type/id and publish DEL even if the item is not present locally.
    ///
    /// This is useful for explicit tombstoning of entities (e.g. disconnected peers)
    /// where we must ensure a DEL event is produced to Kafka.
    ///
    /// Note: relationship cascades require the full item and are therefore skipped here.
    pub fn del_by_id_with_options(
        &self,
        entity_type: &str,
        id: &str,
        options: Option<EventOptions>,
    ) {
        let options = options.unwrap_or_default();
        let id_arc: Arc<str> = id.into();

        let existing = self
            .registry
            .get(entity_type)
            .and_then(|store| store.get(&id_arc).get());

        // Reduce: remove from store
        self.registry.get_or_create(entity_type).remove(&id_arc);

        // Search: remove from index
        self.search_index.remove_entity(id);

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            if let Some(item) = existing {
                self.produce_del_dyn(&item);
            } else {
                log::warn!(
                    "del_by_id could not persist DEL without full entity: {}:{}",
                    entity_type,
                    id
                );
            }
        }

        log::trace!("Published DEL {}:{}", entity_type, id);
    }

    /// Delete an entity by type/id with default options.
    pub fn del_by_id(&self, entity_type: &str, id: &str) {
        self.del_by_id_with_options(entity_type, id, None);
    }

    /// Apply a single wire event (parse -> reduce -> relationships -> persist).
    ///
    /// Returns `true` when the event was parsed and applied, `false` otherwise.
    pub fn apply_event(&self, event: MEvent) -> bool {
        self.apply_event_batch(vec![event]) == 1
    }

    /// Apply a batch of wire events with a single parse pass and grouped store updates.
    ///
    /// This reduces overhead versus calling `set_dyn`/`del_dyn` for each event individually.
    /// Returns the number of successfully parsed/applied events.
    pub fn apply_event_batch(&self, events: Vec<MEvent>) -> usize {
        if events.is_empty() {
            return 0;
        }
        let input_len = events.len();

        #[derive(Clone)]
        struct SetOp {
            item: Arc<dyn AnyItem>,
            options: EventOptions,
        }

        #[derive(Clone)]
        struct DelOp {
            item: Arc<dyn AnyItem>,
            options: EventOptions,
        }

        let mut sets: Vec<SetOp> = Vec::new();
        let mut dels: Vec<DelOp> = Vec::new();

        for event in events {
            let options = event.options.clone().unwrap_or_default();
            match event.change_type {
                MEventType::SET => {
                    if let Some(item) = self.parse_item(&event.item_type, &event.item) {
                        sets.push(SetOp { item, options });
                    } else {
                        log::warn!(
                            "Unknown entity type or parse error for SET: {}",
                            event.item_type
                        );
                    }
                }
                MEventType::DEL => {
                    if let Some(item) = self.parse_item(&event.item_type, &event.item) {
                        dels.push(DelOp { item, options });
                    } else {
                        log::warn!(
                            "Unknown entity type or parse error for DEL: {}",
                            event.item_type
                        );
                    }
                }
            }
        }

        if sets.is_empty() && dels.is_empty() {
            return 0;
        }

        log::trace!(
            target: "myko_rs::server::context",
            "apply_event_batch parsed: input_events={} sets={} dels={}",
            input_len,
            sets.len(),
            dels.len()
        );

        let mut inserts_by_type: HashMap<Arc<str>, Vec<(Arc<str>, Arc<dyn AnyItem>)>> =
            HashMap::new();
        let mut removes_by_type: HashMap<Arc<str>, Vec<Arc<str>>> = HashMap::new();

        for op in &sets {
            let entity_type: Arc<str> = op.item.entity_type().into();
            inserts_by_type
                .entry(entity_type)
                .or_default()
                .push((op.item.id(), op.item.clone()));
            self.search_index.index_item(&op.item);
        }
        for op in &dels {
            let entity_type: Arc<str> = op.item.entity_type().into();
            let id = op.item.id();
            removes_by_type
                .entry(entity_type)
                .or_default()
                .push(id.clone());
            self.search_index.remove_entity(&id);
        }

        // Reduce: one diff emission per entity type per operation kind.
        for (entity_type, entries) in inserts_by_type {
            log::trace!(
                target: "myko_rs::server::context",
                "apply_event_batch reduce inserts: entity_type={} count={}",
                entity_type,
                entries.len()
            );
            let store = self.registry.get_or_create(entity_type.as_ref());
            store.insert_many(entries);
        }
        for (entity_type, keys) in removes_by_type {
            log::trace!(
                target: "myko_rs::server::context",
                "apply_event_batch reduce removes: entity_type={} count={}",
                entity_type,
                keys.len()
            );
            let store = self.registry.get_or_create(entity_type.as_ref());
            store.remove_many(keys);
        }

        // Relationships
        for op in &sets {
            if !op.options.prevent_relationship_updates {
                self.relationship_manager.forward_set(op.item.clone(), self);
            }
        }
        for op in &dels {
            if !op.options.prevent_relationship_updates {
                self.relationship_manager.forward_del(op.item.clone(), self);
            }
        }

        // Persist
        for op in &sets {
            if !op.options.prevent_persist {
                self.produce_set_dyn(&op.item);
            }
        }
        for op in &dels {
            if !op.options.prevent_persist {
                self.produce_del_dyn(&op.item);
            }
        }

        sets.len() + dels.len()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Kafka production (private)
    // ─────────────────────────────────────────────────────────────────────────

    fn produce_set<T: Eventable>(&self, entity: &T) {
        if let Some(persister) = self.persisters.resolve(T::entity_name_static()) {
            let event = MEvent::from_item(entity, MEventType::SET, &self.host_id.to_string());
            persister.persist(event);
        }
        if let Some(sink) = &self.event_sink {
            let event = MEvent::from_item(entity, MEventType::SET, &self.host_id.to_string());
            let _ = sink.send(event);
        }
    }

    fn produce_del<T: Eventable>(&self, entity: &T) {
        if let Some(persister) = self.persisters.resolve(T::entity_name_static()) {
            let event = MEvent::del(entity, &self.host_id.to_string());
            persister.persist(event);
        }
        if let Some(sink) = &self.event_sink {
            let event = MEvent::del(entity, &self.host_id.to_string());
            let _ = sink.send(event);
        }
    }

    fn produce_del_dyn(&self, item: &Arc<dyn AnyItem>) {
        if let Some(persister) = self.persisters.resolve(item.entity_type()) {
            let event = MEvent::del_from_any(item, &self.host_id.to_string());
            persister.persist(event);
        }
        if let Some(sink) = &self.event_sink {
            let event = MEvent::del_from_any(item, &self.host_id.to_string());
            let _ = sink.send(event);
        }
    }

    fn produce_set_dyn(&self, item: &Arc<dyn AnyItem>) {
        if let Some(persister) = self.persisters.resolve(item.entity_type()) {
            let event = MEvent::set_from_value(
                item.entity_type(),
                item.to_value(),
                &self.host_id.to_string(),
            );
            persister.persist(event);
        }
        if let Some(sink) = &self.event_sink {
            let event = MEvent::set_from_value(
                item.entity_type(),
                item.to_value(),
                &self.host_id.to_string(),
            );
            let _ = sink.send(event);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Query methods
    // ─────────────────────────────────────────────────────────────────────────

    /// Run a reactive query.
    ///
    /// Returns a cell that updates whenever the query results change.
    /// The query's `test_entity` is applied with proper server context.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use myko_rs::entities::server::GetPeerServers;
    /// use myko_rs::request::RequestContext;
    /// use myko_rs::server::CellServerCtx;
    ///
    /// fn demo(ctx: &CellServerCtx, req: Arc<RequestContext>) {
    ///     let _peer_servers = ctx.query(GetPeerServers {}, req);
    ///     // _peer_servers is Cell<Vec<Server>, CellImmutable>
    /// }
    /// ```
    pub fn query<Q>(
        &self,
        query: Q,
        request: Arc<RequestContext>,
    ) -> Cell<Vec<Q::Item>, CellImmutable>
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let query_id = Q::query_id_static();
        let query_name = format!("query:{}", query_id);
        self.query_map(query, request)
            .entries()
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|(_, item)| item.as_any().downcast_ref::<Q::Item>().cloned())
                    .collect()
            })
            .with_name(query_name.as_str())
    }

    pub fn query_map<Q>(&self, query: Q, request: Arc<RequestContext>) -> FilteredCellMap
    where
        Q: QueryFactory + QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let key = self.cache_key("query", Q::query_id_static().as_ref(), &query, &request);
        if let Some(existing) = self.query_cache.get(&key)
            && let Some(shared) = existing.value().upgrade()
        {
            return shared.lock();
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
        self.query_cache.insert(key, built.downgrade());
        built
    }

    /// Build a reactive view cell map (type-erased for framework internals).
    pub fn view_map_untyped<V>(&self, view: V, request: Arc<RequestContext>) -> FilteredViewCellMap
    where
        V: ViewFactory + Clone + Send + Sync + 'static,
        V::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let key = self.cache_key("view", V::view_id_static().as_ref(), &view, &request);
        if let Some(existing) = self.view_cache.get(&key)
            && let Some(shared) = existing.value().upgrade()
        {
            return shared.lock();
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
        self.view_cache.insert(key, built.downgrade());
        built
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
        typed_map_from_any_item(self.view_map_untyped(view, request), "CellServerCtx::view")
    }

    /// Run a one-shot (non-reactive) query.
    ///
    /// Iterates the store directly and returns matching entities without creating
    /// any reactive cells or subscriptions. Use this for command handlers and other
    /// contexts where you need a point-in-time snapshot, not a live query.
    pub fn query_snapshot<Q>(&self, query: Q, request: Arc<RequestContext>) -> Vec<Q::Item>
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
                let typed_item = item.as_any().downcast_ref::<Q::Item>()?;
                let ctx = QueryTestCtx {
                    item: Arc::new(typed_item.clone()),
                    query: query.clone(),
                    query_context: query_context.clone(),
                };
                if Q::test_entity(ctx) {
                    Some(typed_item.clone())
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
    ) -> Cell<R::Output, CellImmutable>
    where
        R: ReportHandler + ReportId + Clone + serde::Serialize + 'static,
    {
        let report_name = format!("report:{}", report.report_id());
        let key = self.cache_key("report", report.report_id().as_ref(), &report, &request);

        if let Some(existing) = self.report_cache.get(&key)
            && let Some(weak) = existing
                .value()
                .as_ref()
                .downcast_ref::<hypha::cell::WeakCell<R::Output, CellImmutable>>()
            && let Some(shared) = weak.upgrade()
        {
            return shared.with_name(report_name.as_str());
        }

        let nested_ctx = ReportContext::new(request, Arc::new(self.clone()));
        let built = report.compute(nested_ctx).map(|v| v.clone());
        self.report_cache.insert(
            key,
            Arc::new(built.downgrade()) as Arc<dyn Any + Send + Sync>,
        );
        built.with_name(report_name.as_str())
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
