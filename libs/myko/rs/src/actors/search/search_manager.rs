//! SearchManager actor for full-text search indexing.
//!
//! The SearchManager:
//! - Discovers searchable entities via inventory
//! - Builds tantivy RAM indices for each entity type
//! - Subscribes to EventBus to keep indices updated
//! - Provides search queries via RPC

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, trace, warn};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, FuzzyTermQuery, Occur, Query};
use tantivy::schema::{Field, Schema, STORED, TEXT};
use tantivy::schema::OwnedValue;
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, Term};

use crate::actors::event::EventBus;
use crate::event::{MEvent, MEventType};
use crate::runtime::{Actor, ActorRef, RpcReplyPort};
use crate::search::{extract_searchable_text, iter_searchable, SearchableRegistration};
use crate::server::MykoServerCtx;

/// Commit debounce interval - commits are batched for performance
const COMMIT_DEBOUNCE_MS: u64 = 50;

/// tantivy index for a single entity type
struct EntityIndex {
    #[allow(dead_code)] // Kept alive for writer/reader
    index: Index,
    writer: IndexWriter,
    reader: IndexReader,
    id_field: Field,
    text_field: Field,
    searchable_fields: &'static [&'static str],
    dirty: bool,
}

impl EntityIndex {
    fn new(registration: &SearchableRegistration) -> Result<Self, tantivy::TantivyError> {
        // Build schema with id (stored) and text (searchable) fields
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", TEXT | STORED);
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();

        // Create RAM index
        let index = Index::create_in_ram(schema);

        // Create writer with 50MB heap (reasonable for in-memory use)
        let writer = index.writer(50_000_000)?;

        // Create reader
        let reader = index.reader()?;

        Ok(Self {
            index,
            writer,
            reader,
            id_field,
            text_field,
            searchable_fields: registration.fields,
            dirty: false,
        })
    }

    fn index_item(&mut self, id: &str, item: &serde_json::Value) {
        // Delete existing document (handles updates)
        let id_term = Term::from_field_text(self.id_field, id);
        self.writer.delete_term(id_term);

        // Extract searchable text and add new document
        let searchable_text = extract_searchable_text(item, self.searchable_fields);
        if searchable_text.is_empty() {
            self.dirty = true;
            return;
        }

        let mut doc = TantivyDocument::new();
        doc.add_text(self.id_field, id);
        doc.add_text(self.text_field, searchable_text.to_lowercase());

        if let Err(e) = self.writer.add_document(doc) {
            error!("Failed to index document {}: {}", id, e);
        }
        self.dirty = true;
    }

    fn delete_item(&mut self, id: &str) {
        let id_term = Term::from_field_text(self.id_field, id);
        self.writer.delete_term(id_term);
        self.dirty = true;
    }

    fn commit(&mut self) {
        if !self.dirty {
            return;
        }
        if let Err(e) = self.writer.commit() {
            error!("Failed to commit index: {}", e);
        }
        // Reload reader to see committed changes
        if let Err(e) = self.reader.reload() {
            error!("Failed to reload index reader: {}", e);
        }
        self.dirty = false;
    }

    fn search(&self, query_str: &str, limit: usize) -> Vec<Arc<str>> {
        let searcher = self.reader.searcher();
        let query_lower = query_str.to_lowercase();

        // Build fuzzy query for each term
        // Distance 1 for short terms, 2 for longer terms
        let term_queries: Vec<(Occur, Box<dyn Query>)> = query_lower
            .split_whitespace()
            .filter(|term| !term.is_empty())
            .map(|term_str| {
                let term = Term::from_field_text(self.text_field, term_str);
                // Use distance 1 for short terms, 2 for longer
                let distance = if term_str.len() <= 4 { 1 } else { 2 };
                let fuzzy_query = FuzzyTermQuery::new_prefix(term, distance, true);
                (Occur::Should, Box::new(fuzzy_query) as Box<dyn Query>)
            })
            .collect();

        if term_queries.is_empty() {
            return vec![];
        }

        let query = BooleanQuery::new(term_queries);

        // Execute search
        let top_docs = match searcher.search(&query, &TopDocs::with_limit(limit)) {
            Ok(docs) => docs,
            Err(e) => {
                error!("Search failed: {}", e);
                return vec![];
            }
        };

        // Extract IDs from results
        top_docs
            .into_iter()
            .filter_map(|(_score, doc_address)| {
                let doc: TantivyDocument = searcher.doc(doc_address).ok()?;
                match doc.get_first(self.id_field)? {
                    OwnedValue::Str(s) => Some(Arc::from(s.as_str())),
                    _ => None,
                }
            })
            .collect()
    }
}

/// Actor that manages full-text search indices.
pub struct SearchManager {
    ctx: Arc<MykoServerCtx>,
    event_manager: ActorRef<crate::actors::event::event_manager::EventManagerMsg>,
    indices: HashMap<String, EntityIndex>,
    commit_scheduled: bool,
    myself: ActorRef<SearchManagerMsg>,
}

/// Messages for SearchManager actor.
pub enum SearchManagerMsg {
    /// Initialize indices after EventBus is available
    Initialize,
    /// Populate all indices with existing items (called after handlers are registered)
    PopulateAll,
    /// Populate index with existing items for an entity type
    PopulateIndex(String, Vec<serde_json::Value>),
    /// Process an event from EventBus
    ProcessEvent(Arc<MEvent>),
    /// Search for entities matching query (entity_type, query, limit, reply)
    Search(String, String, usize, RpcReplyPort<Vec<Arc<str>>>),
    /// Internal: commit pending changes
    CommitPending,
}

impl std::fmt::Debug for SearchManagerMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchManagerMsg::Initialize => write!(f, "Initialize"),
            SearchManagerMsg::PopulateAll => write!(f, "PopulateAll"),
            SearchManagerMsg::PopulateIndex(entity_type, items) => {
                write!(f, "PopulateIndex({}, {} items)", entity_type, items.len())
            }
            SearchManagerMsg::ProcessEvent(_) => write!(f, "ProcessEvent"),
            SearchManagerMsg::Search(entity_type, query, limit, _) => {
                write!(f, "Search({}, {}, {})", entity_type, query, limit)
            }
            SearchManagerMsg::CommitPending => write!(f, "CommitPending"),
        }
    }
}

/// Arguments for spawning SearchManager.
pub struct SearchManagerArgs {
    pub ctx: Arc<MykoServerCtx>,
    pub event_manager: ActorRef<crate::actors::event::event_manager::EventManagerMsg>,
}

impl SearchManager {
    fn create(args: SearchManagerArgs, myself: ActorRef<SearchManagerMsg>) -> Self {
        // Build indices from registered searchable entities
        let mut indices = HashMap::new();

        for registration in iter_searchable() {
            match EntityIndex::new(registration) {
                Ok(index) => {
                    debug!(
                        "SearchManager: created index for '{}' with fields: {:?}",
                        registration.entity_type,
                        registration.fields
                    );
                    indices.insert(registration.entity_type.to_string(), index);
                }
                Err(e) => {
                    error!(
                        "Failed to create search index for {}: {}",
                        registration.entity_type, e
                    );
                }
            }
        }

        debug!("SearchManager: initialized {} indices", indices.len());

        Self {
            ctx: args.ctx,
            event_manager: args.event_manager,
            indices,
            commit_scheduled: false,
            myself,
        }
    }
}

impl Actor for SearchManager {
    type Msg = SearchManagerMsg;
    type Args = SearchManagerArgs;

    fn new(args: Self::Args, myself: ActorRef<Self::Msg>) -> Self {
        Self::create(args, myself)
    }

    fn handle(&mut self, msg: Self::Msg) {
        match msg {
            SearchManagerMsg::Initialize => {
                let myself = self.myself.clone();

                // Subscribe to EventBus
                let event_bus = match self.ctx.event_bus.get() {
                    Some(bus) => bus.clone(),
                    None => {
                        warn!("SearchManager: EventBus not yet initialized, retrying...");
                        // Retry after a short delay
                        let myself_clone = myself.clone();
                        self.ctx.tokio_handle.spawn(async move {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            let _ = myself_clone.send_message(SearchManagerMsg::Initialize);
                        });
                        return;
                    }
                };

                let myself_clone = myself.clone();
                let entity_types: Vec<String> = self.indices.keys().cloned().collect();

                // Spawn event processor task
                self.ctx.tokio_handle.spawn(async move {
                    process_events(myself_clone, event_bus, entity_types).await;
                });

                debug!("SearchManager: subscribed to EventBus");
            }

            SearchManagerMsg::PopulateAll => {
                debug!("SearchManager: populating all indices with existing items");

                let myself = self.myself.clone();

                // Populate indices with existing items
                for entity_type in self.indices.keys().cloned().collect::<Vec<_>>() {
                    let myself_clone = myself.clone();
                    let event_manager = self.event_manager.clone();
                    let entity_type_clone: Arc<str> = entity_type.clone().into();

                    std::thread::spawn(move || {
                        use crate::actors::event::event_manager::EventManagerMsg;

                        trace!("SearchManager: fetching existing {} items", entity_type_clone);

                        match event_manager.call(|r| EventManagerMsg::GetAllItems(entity_type_clone.clone(), r)) {
                            Ok(items) => {
                                let values: Vec<serde_json::Value> = items
                                    .iter()
                                    .map(|item| item.to_value())
                                    .collect();

                                if values.is_empty() {
                                    trace!(
                                        "SearchManager: no existing '{}' items to index",
                                        entity_type_clone
                                    );
                                } else {
                                    debug!(
                                        "SearchManager: populating '{}' index with {} existing items",
                                        entity_type_clone, values.len()
                                    );
                                    let _ = myself_clone.send_message(
                                        SearchManagerMsg::PopulateIndex(entity_type_clone.to_string(), values)
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("SearchManager: failed to get existing {} items: {}", entity_type_clone, e);
                            }
                        }
                    });
                }
            }

            SearchManagerMsg::PopulateIndex(entity_type, items) => {
                let Some(index) = self.indices.get_mut(&entity_type) else {
                    warn!("SearchManager: PopulateIndex for '{}' but no index exists", entity_type);
                    return;
                };

                let mut indexed_count = 0;
                for item in &items {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                        index.index_item(id, item);
                        indexed_count += 1;
                    }
                }

                // Commit immediately after population
                index.commit();
                debug!("SearchManager: indexed {} items and committed '{}' index", indexed_count, entity_type);
            }

            SearchManagerMsg::ProcessEvent(event) => {
                // Get the index for this entity type
                let Some(index) = self.indices.get_mut(&event.item_type) else {
                    trace!("SearchManager: no index for entity type '{}'", event.item_type);
                    return;
                };

                // Get item ID
                let Some(id) = event.item.get("id").and_then(|v| v.as_str()) else {
                    return;
                };

                // Process based on event type
                match event.change_type {
                    MEventType::SET => {
                        trace!("SearchManager: indexing {} id={}", event.item_type, id);
                        index.index_item(id, &event.item);
                    }
                    MEventType::DEL => {
                        trace!("SearchManager: removing {} id={}", event.item_type, id);
                        index.delete_item(id);
                    }
                }

                // Schedule commit if not already scheduled
                if !self.commit_scheduled {
                    self.commit_scheduled = true;
                    let myself = self.myself.clone();
                    self.ctx.tokio_handle.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(COMMIT_DEBOUNCE_MS)).await;
                        let _ = myself.send_message(SearchManagerMsg::CommitPending);
                    });
                }
            }

            SearchManagerMsg::Search(entity_type, query, limit, reply) => {
                let results = match self.indices.get(&entity_type) {
                    Some(index) => {
                        let results = index.search(&query, limit);
                        trace!(
                            "SearchManager: search '{}' for '{}' returned {} results",
                            entity_type, query, results.len()
                        );
                        results
                    }
                    None => {
                        warn!("SearchManager: no index for entity type '{}' (available: {:?})",
                            entity_type, self.indices.keys().collect::<Vec<_>>());
                        vec![]
                    }
                };

                let _ = reply.send(results);
            }

            SearchManagerMsg::CommitPending => {
                self.commit_scheduled = false;

                for (entity_type, index) in &mut self.indices {
                    if index.dirty {
                        trace!("Committing search index for {}", entity_type);
                        index.commit();
                    }
                }
            }
        }
    }

    fn on_shutdown(&mut self) {
        trace!("SearchManager stopping");

        // Cancel event processing task - we detached threads so we can't abort them directly
        // The threads will stop when EventBus closes or when the actor ref becomes invalid

        // Final commit
        for index in self.indices.values_mut() {
            index.commit();
        }
    }
}

/// Process events from EventBus and forward to SearchManager actor.
async fn process_events(
    myself: ActorRef<SearchManagerMsg>,
    event_bus: EventBus,
    entity_types: Vec<String>,
) {
    let mut subscriber = event_bus.subscribe();

    loop {
        let event = match subscriber.recv().await {
            Some(e) => e,
            None => {
                debug!("EventBus closed, stopping search event processor");
                break;
            }
        };

        // Only process events for indexed entity types
        if !entity_types.contains(&event.item_type) {
            continue;
        }

        // Forward to actor for processing
        if myself
            .send_message(SearchManagerMsg::ProcessEvent(event))
            .is_err()
        {
            debug!("SearchManager actor stopped, ending event processor");
            break;
        }
    }
}
