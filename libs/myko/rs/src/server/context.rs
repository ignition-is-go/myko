//! Server context for the cell-based server.
//!
//! Provides modules (like PeerRegistry) with the ability to:
//! - Run reactive queries (like GetPeerServers)
//! - Publish entities (Reduce → Relationships → Persist)
//! - Access server identity (host_id)

use std::sync::Arc;

use hypha::{Cell, CellImmutable, MapExt};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::query::{MykoServerCtx, QueryHandler, QueryHandlerCtx, QueryParams};
use crate::registry::item::{AnyItem, Eventable};
use crate::store::StoreRegistry;
use crate::wire::{EventOptions, MEvent, MEventType};

use super::{HandlerRegistry, KafkaProducerHandle, RelationshipManager};

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
    /// Optional Kafka producer for persistence
    kafka_producer: Option<KafkaProducerHandle>,
}

impl CellServerCtx {
    /// Create a new server context.
    pub fn new(
        host_id: Uuid,
        registry: Arc<StoreRegistry>,
        handler_registry: Arc<HandlerRegistry>,
        relationship_manager: Arc<RelationshipManager>,
        kafka_producer: Option<KafkaProducerHandle>,
    ) -> Self {
        Self {
            host_id,
            registry,
            handler_registry,
            relationship_manager,
            kafka_producer,
        }
    }

    /// Parse JSON to a typed entity using the registered item parser.
    ///
    /// Returns None if the entity type is not registered or parsing fails.
    pub fn parse_item(&self, entity_type: &str, json: &serde_json::Value) -> Option<Arc<dyn AnyItem>> {
        let parser = self.handler_registry.get_item_parser(entity_type)?;
        parser.parse(json.clone()).ok()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Typed entity publishing (for server modules)
    // ─────────────────────────────────────────────────────────────────────────

    /// Publish an entity (SET) with default options.
    ///
    /// Default behavior: Reduce + Relationships + Persist
    pub fn set<T>(&self, entity: T)
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
    pub fn set_with_options<T>(&self, entity: T, options: Option<EventOptions>)
    where
        T: Eventable + 'static,
    {
        let options = options.unwrap_or_default();
        let id = entity.id();
        let entity_type = entity.entity_type();
        let item: Arc<dyn AnyItem> = Arc::new(entity.clone());

        // Reduce: update store
        self.registry.get_or_create(entity_type).set(id.clone(), item.clone());

        // Relationships: process cascades (unless prevented)
        if !options.prevent_relationship_updates {
            self.relationship_manager.forward_set(item, self);
        }

        // Persist: produce to Kafka (unless prevented)
        if !options.prevent_persist {
            self.produce_set(&entity);
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
    pub fn set_dyn_with_options(
        &self,
        item: Arc<dyn AnyItem>,
        options: Option<EventOptions>,
    ) {
        let options = options.unwrap_or_default();
        let entity_type = item.entity_type();
        let id = item.id();

        // Reduce: update store
        self.registry.get_or_create(entity_type).set(id.clone(), item.clone());

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
    pub fn del_dyn_with_options(
        &self,
        item: Arc<dyn AnyItem>,
        options: Option<EventOptions>,
    ) {
        let options = options.unwrap_or_default();
        let entity_type = item.entity_type();
        let id = item.id();

        // Reduce: remove from store
        self.registry.get_or_create(entity_type).remove(&id);

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
        if let Some(ref producer) = self.kafka_producer {
            let event = MEvent::from_item(entity, MEventType::SET, &self.host_id.to_string());
            producer.produce(event);
        }
    }

    fn produce_del(&self, entity_type: &str, id: &str) {
        if let Some(ref producer) = self.kafka_producer {
            let event = MEvent::del(entity_type, id, &self.host_id.to_string());
            producer.produce(event);
        }
    }


    fn produce_set_dyn(&self, item: &Arc<dyn AnyItem>) {
        if let Some(ref producer) = self.kafka_producer {
            let event = MEvent::set_from_value(
                item.entity_type(),
                item.to_value(),
                &self.host_id.to_string(),
            );
            producer.produce(event);
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
    pub fn query<Q>(&self, query: Q) -> Cell<Vec<Q::Item>, CellImmutable>
    where
        Q: QueryHandler + QueryParams + Clone + Send + Sync + 'static,
        Q::Item: DeserializeOwned + Clone + Send + Sync + 'static,
    {
        let query_item_type = Q::query_item_type_static();
        let store = self.registry.get_or_create(&query_item_type);

        // Create a MykoServerCtx for query compatibility
        let server_ctx = Arc::new(MykoServerCtx::new(self.host_id, self.registry.clone()));
        let query = Arc::new(query);

        // Filter using the query's test_entity
        store
            .select(move |item| {
                if let Some(typed_item) = item.as_any().downcast_ref::<Q::Item>() {
                    let ctx = QueryHandlerCtx {
                        item: Arc::new(typed_item.clone()),
                        query: query.clone(),
                        server_ctx: server_ctx.clone(),
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
    }

    /// Get the server context for use with queries.
    ///
    /// This is useful when you need to pass the server context to
    /// query handlers directly.
    pub fn server_ctx(&self) -> Arc<MykoServerCtx> {
        Arc::new(MykoServerCtx::new(self.host_id, self.registry.clone()))
    }
}

impl std::fmt::Debug for CellServerCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellServerCtx")
            .field("host_id", &self.host_id)
            .finish()
    }
}
