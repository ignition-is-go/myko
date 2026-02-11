//! Server context for the cell-based server.
//!
//! Provides modules (like PeerRegistry) with the ability to:
//! - Run reactive queries (like GetPeerServers)
//! - Publish entities (Reduce → Relationships → Persist)
//! - Access server identity (host_id)

use std::sync::Arc;

use hypha::{Cell, CellImmutable, MapExt, SelectExt};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::{HandlerRegistry, RelationshipManager, persister::Persister};
use crate::{
    core::item::{AnyItem, Eventable},
    query::{QueryContext, QueryHandler, QueryParams, QueryTestCtx},
    report::{ReportContext, ReportHandler, ReportId},
    request::RequestContext,
    search::SearchIndex,
    store::StoreRegistry,
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
    /// Optional persister for event durability
    persister: Option<Arc<dyn Persister>>,
    /// Full-text search index
    search_index: Arc<SearchIndex>,
}

impl CellServerCtx {
    /// Create a new server context.
    pub fn new(
        host_id: Uuid,
        registry: Arc<StoreRegistry>,
        handler_registry: Arc<HandlerRegistry>,
        relationship_manager: Arc<RelationshipManager>,
        persister: Option<Arc<dyn Persister>>,
        search_index: Arc<SearchIndex>,
    ) -> Self {
        Self {
            host_id,
            registry,
            handler_registry,
            relationship_manager,
            persister,
            search_index,
        }
    }

    /// Get the search index.
    pub fn search_index(&self) -> &Arc<SearchIndex> {
        &self.search_index
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
            self.relationship_manager.forward_del(item, self);
        }

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            self.produce_del(entity_type, &id);
        }

        log::trace!("Published DEL {}:{}", entity_type, id);
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
            self.relationship_manager.forward_del(item, self);
        }

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            self.produce_del(entity_type, &id);
        }

        log::trace!("Published DEL {}:{}", entity_type, id);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Kafka production (private)
    // ─────────────────────────────────────────────────────────────────────────

    fn produce_set<T: Eventable>(&self, entity: &T) {
        if let Some(ref persister) = self.persister {
            let event = MEvent::from_item(entity, MEventType::SET, &self.host_id.to_string());
            persister.persist(event);
        }
    }

    fn produce_del(&self, entity_type: &str, id: &str) {
        if let Some(ref persister) = self.persister {
            let event = MEvent::del(entity_type, id, &self.host_id.to_string());
            persister.persist(event);
        }
    }

    fn produce_set_dyn(&self, item: &Arc<dyn AnyItem>) {
        if let Some(ref persister) = self.persister {
            let event = MEvent::set_from_value(
                item.entity_type(),
                item.to_value(),
                &self.host_id.to_string(),
            );
            persister.persist(event);
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
    /// ```ignore
    /// let peer_servers = ctx.query(GetPeerServers {});
    /// // peer_servers is Cell<Vec<Server>, CellImmutable>
    /// ```
    pub fn query<Q>(
        &self,
        query: Q,
        request: Arc<RequestContext>,
    ) -> Cell<Vec<Q::Item>, CellImmutable>
    where
        Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let query_item_type = Q::query_item_type_static();
        let store = self.registry.get_or_create(&query_item_type);

        // Create a MykoServerCtx for query compatibility
        let query_context = Arc::new(QueryContext {
            req: request.clone(),
        });
        let query = Arc::new(query);

        // Filter using the query's test_entity
        let query_id = Q::query_id_static();
        let query_name = format!("query:{}", query_id);
        store
            .select(move |item| {
                if let Some(typed_item) = item.as_any().downcast_ref::<Q::Item>() {
                    let ctx = QueryTestCtx {
                        item: Arc::new(typed_item.clone()),
                        query: query.clone(),
                        query_context: query_context.clone(),
                    };
                    Q::test_entity(ctx)
                } else {
                    false
                }
            })
            .entries()
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|(_, item)| item.as_any().downcast_ref::<Q::Item>().cloned())
                    .collect()
            })
            .with_name(query_name.as_str())
    }

    /// Run a one-shot (non-reactive) query.
    ///
    /// Iterates the store directly and returns matching entities without creating
    /// any reactive cells or subscriptions. Use this for command handlers and other
    /// contexts where you need a point-in-time snapshot, not a live query.
    pub fn query_snapshot<Q>(
        &self,
        query: Q,
        request: Arc<RequestContext>,
    ) -> Vec<Q::Item>
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
        R: ReportHandler + ReportId + Clone + 'static,
    {
        let report_name = format!("report:{}", report.report_id());

        // Create a nested context - sub-report args are accessed via &self in compute
        let nested_ctx = ReportContext::new(request, Arc::new(self.clone()));

        // Wrap the compute result in a named relay so the inspector
        // shows the report as a parent of its compute graph
        report
            .compute(nested_ctx)
            .map(|v| v.clone())
            .with_name(report_name.as_str())
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
