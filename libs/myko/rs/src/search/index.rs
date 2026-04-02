//! Tantivy-based full-text search index for Myko entities.
//!
//! Provides in-memory full-text search using tantivy's RAM directory.
//! Entities with `#[searchable]` fields are indexed automatically.

use std::{collections::HashMap, sync::Arc};

use tantivy::{
    Index, IndexReader, IndexWriter, TantivyDocument, Term,
    collector::TopDocs,
    query::{BooleanQuery, FuzzyTermQuery, Occur, Query, TermQuery},
    schema::{Field, IndexRecordOption, OwnedValue, STORED, STRING, Schema, TEXT},
};

use super::{extract_searchable_text, iter_searchable};
use crate::{common::to_value::ToValue, core::item::AnyItem};

/// Thread-safe full-text search index backed by tantivy.
///
/// Uses a RAM directory for in-memory indexing. The writer is behind a mutex
/// for thread-safe writes; the reader is already thread-safe.
pub struct SearchIndex {
    _index: Index,
    reader: IndexReader,
    writer: std::sync::Mutex<IndexWriter>,
    /// Schema fields
    entity_type_field: Field,
    entity_id_field: Field,
    content_field: Field,
    /// Searchable field names per entity type (from SearchableRegistration)
    searchable_fields: HashMap<&'static str, &'static [&'static str]>,
}

impl SearchIndex {
    /// Create a new search index, collecting searchable field metadata from inventory.
    pub fn new() -> Self {
        let mut schema_builder = Schema::builder();
        let entity_type_field = schema_builder.add_text_field("entity_type", STRING | STORED);
        let entity_id_field = schema_builder.add_text_field("entity_id", STRING | STORED);
        let content_field = schema_builder.add_text_field("content", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);

        // 50MB heap for the writer
        let writer = index
            .writer(50_000_000)
            .expect("failed to create tantivy index writer");

        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .expect("failed to create tantivy index reader");

        // Collect searchable field registrations
        let mut searchable_fields = HashMap::new();
        for reg in iter_searchable() {
            searchable_fields.insert(reg.entity_type, reg.fields);
        }

        let registered_count = searchable_fields.len();
        log::info!(
            "SearchIndex initialized with {} searchable entity types",
            registered_count
        );

        Self {
            _index: index,
            reader,
            writer: std::sync::Mutex::new(writer),
            entity_type_field,
            entity_id_field,
            content_field,
            searchable_fields,
        }
    }

    /// Index a dynamic entity item.
    ///
    /// Extracts searchable text from the item's JSON value and adds/updates it in the index.
    /// If the entity type has no searchable fields, this is a no-op.
    pub fn index_item(&self, item: &Arc<dyn AnyItem>) {
        let entity_type = item.entity_type();
        let Some(fields) = self.searchable_fields.get(entity_type) else {
            return;
        };

        let id = item.id();
        let value = item.to_value();
        let text = extract_searchable_text(&value, fields);

        if text.is_empty() {
            return;
        }

        self.index_entity(entity_type, &id, &text);
    }

    /// Index an entity with pre-extracted searchable text.
    fn index_entity(&self, entity_type: &str, entity_id: &str, content: &str) {
        let Ok(writer) = self.writer.lock() else {
            log::error!("SearchIndex: writer mutex poisoned");
            return;
        };

        // Delete existing document for this entity (upsert semantics)
        let delete_term = tantivy::Term::from_field_text(self.entity_id_field, entity_id);
        writer.delete_term(delete_term);

        // Add the new document
        let mut doc = TantivyDocument::new();
        doc.add_text(self.entity_type_field, entity_type);
        doc.add_text(self.entity_id_field, entity_id);
        doc.add_text(self.content_field, content);
        writer.add_document(doc).ok();
    }

    /// Remove an entity from the search index.
    pub fn remove_entity(&self, entity_id: &str) {
        let Ok(writer) = self.writer.lock() else {
            log::error!("SearchIndex: writer mutex poisoned");
            return;
        };

        let delete_term = tantivy::Term::from_field_text(self.entity_id_field, entity_id);
        writer.delete_term(delete_term);
    }

    /// Commit pending index changes.
    ///
    /// Call this after a batch of indexing operations (e.g., after Kafka catch-up).
    pub fn commit(&self) {
        let Ok(mut writer) = self.writer.lock() else {
            log::error!("SearchIndex: writer mutex poisoned");
            return;
        };

        if let Err(e) = writer.commit() {
            log::error!("SearchIndex: commit failed: {}", e);
            return;
        }

        // Reload the reader to pick up committed changes
        if let Err(e) = self.reader.reload() {
            log::error!("SearchIndex: reader reload failed: {}", e);
        }
    }

    /// Search for entities matching a query string.
    ///
    /// Returns matching entity IDs (up to `limit` results).
    /// Automatically commits any pending writes before searching.
    ///
    /// Each word in the query becomes a fuzzy prefix match: it matches any
    /// indexed term that starts with the word (prefix) OR is within edit
    /// distance 1 (typo tolerance). All words must match (AND semantics).
    pub fn search(&self, entity_type: &str, query: &str, limit: usize) -> Vec<Arc<str>> {
        if query.is_empty() || limit == 0 {
            return vec![];
        }

        // Commit pending changes so the searcher sees them
        self.commit();

        let searcher = self.reader.searcher();

        // Filter by entity_type (STRING field — exact match)
        let type_query: Box<dyn Query> = Box::new(TermQuery::new(
            Term::from_field_text(self.entity_type_field, entity_type),
            IndexRecordOption::Basic,
        ));

        // Each word becomes: prefix-fuzzy (distance=1, prefix=true)
        // This matches terms that start with the word OR are within 1 edit
        let mut must_clauses: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Must, type_query)];

        for word in query.split_whitespace() {
            let lower = word.to_lowercase();
            let term = Term::from_field_text(self.content_field, &lower);
            // Prefix match: "ba" → "base1", "base2"
            let prefix: Box<dyn Query> =
                Box::new(FuzzyTermQuery::new_prefix(term.clone(), 0, true));
            // Only add fuzzy for words >= 4 chars to avoid noisy short-term matches
            if lower.len() >= 4 {
                let fuzzy: Box<dyn Query> = Box::new(FuzzyTermQuery::new(term, 1, true));
                let either =
                    BooleanQuery::from(vec![(Occur::Should, prefix), (Occur::Should, fuzzy)]);
                must_clauses.push((Occur::Must, Box::new(either)));
            } else {
                must_clauses.push((Occur::Must, prefix));
            }
        }

        let combined = BooleanQuery::from(must_clauses);

        let top_docs = match searcher.search(&combined, &TopDocs::with_limit(limit)) {
            Ok(docs) => docs,
            Err(e) => {
                log::error!("SearchIndex: search error: {}", e);
                return vec![];
            }
        };

        top_docs
            .into_iter()
            .filter_map(|(_score, doc_address)| {
                let doc: TantivyDocument = searcher.doc(doc_address).ok()?;
                match doc.get_first(self.entity_id_field)? {
                    OwnedValue::Str(s) => Some(Arc::<str>::from(s.as_str())),
                    _ => None,
                }
            })
            .collect()
    }

    /// Check if an entity type has searchable fields registered.
    pub fn is_searchable(&self, entity_type: &str) -> bool {
        self.searchable_fields.contains_key(entity_type)
    }

    /// Build the initial index from all entities currently in the store registry.
    ///
    /// Call this after Kafka catch-up to index all pre-existing entities.
    pub fn build_from_registry(&self, registry: &crate::store::StoreRegistry) {
        use hyphae::Gettable;

        let mut count = 0;

        for (entity_type, fields) in &self.searchable_fields {
            let Some(store) = registry.get(entity_type) else {
                continue;
            };

            let entries = store.entries().get();
            for (id, item) in entries.iter() {
                let value = item.to_value();
                let text = extract_searchable_text(&value, fields);
                if !text.is_empty() {
                    self.index_entity(entity_type, id, &text);
                    count += 1;
                }
            }
        }

        self.commit();
        log::info!("SearchIndex: built initial index with {} entities", count);
    }
}

impl Default for SearchIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_index_creation() {
        let index = SearchIndex::new();
        // Should not panic and should have searchable fields from inventory
        assert!(index.search("NonExistent", "test", 10).is_empty());
    }

    #[test]
    fn test_index_and_search() {
        let index = SearchIndex::new();

        // Manually index a document (bypassing searchable field check)
        index.index_entity("TestEntity", "id-1", "hello world search test");
        index.index_entity("TestEntity", "id-2", "another document here");
        index.index_entity("OtherEntity", "id-3", "hello from other type");
        index.commit();

        // Search for "hello" in TestEntity
        let results = index.search("TestEntity", "hello", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref(), "id-1");

        // Search for "hello" in OtherEntity
        let results = index.search("OtherEntity", "hello", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref(), "id-3");
    }

    #[test]
    fn test_index_upsert() {
        let index = SearchIndex::new();

        index.index_entity("TestEntity", "id-1", "original text");
        index.commit();

        // Update same entity
        index.index_entity("TestEntity", "id-1", "updated text");
        index.commit();

        // Should find updated text
        let results = index.search("TestEntity", "updated", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref(), "id-1");

        // Should not find original text
        let results = index.search("TestEntity", "original", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_remove_entity() {
        let index = SearchIndex::new();

        index.index_entity("TestEntity", "id-1", "hello world");
        index.commit();

        index.remove_entity("id-1");
        index.commit();

        let results = index.search("TestEntity", "hello", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_empty_query() {
        let index = SearchIndex::new();
        assert!(index.search("TestEntity", "", 10).is_empty());
    }

    #[test]
    fn test_zero_limit() {
        let index = SearchIndex::new();
        index.index_entity("TestEntity", "id-1", "hello world");
        index.commit();
        assert!(index.search("TestEntity", "hello", 0).is_empty());
    }

    #[test]
    fn test_prefix_search() {
        let index = SearchIndex::new();
        index.index_entity("Target", "id-1", "base1");
        index.index_entity("Target", "id-2", "base2");
        index.index_entity("Target", "id-3", "camera");
        index.commit();

        // "ba" should match "base1" and "base2" via prefix
        let results = index.search("Target", "ba", 10);
        assert_eq!(
            results.len(),
            2,
            "prefix 'ba' should match base1 and base2, got: {:?}",
            results
        );

        // "base" should match "base1" and "base2"
        let results = index.search("Target", "base", 10);
        assert_eq!(
            results.len(),
            2,
            "prefix 'base' should match base1 and base2, got: {:?}",
            results
        );

        // "cam" should match "camera"
        let results = index.search("Target", "cam", 10);
        assert_eq!(
            results.len(),
            1,
            "prefix 'cam' should match camera, got: {:?}",
            results
        );

        // Fuzzy only for 4+ char words: "camra" should match "camera"
        let results = index.search("Target", "camra", 10);
        assert_eq!(
            results.len(),
            1,
            "fuzzy 'camra' should match camera, got: {:?}",
            results
        );

        // Short fuzzy should NOT match: "ba" should not fuzzy-match "A"
        let results = index.search("Target", "ba", 10);
        assert_eq!(
            results.len(),
            2,
            "'ba' should only prefix-match base1/base2, got: {:?}",
            results
        );

        // Multi-word: both words must match
        index.index_entity("Target", "id-4", "light panel front");
        index.commit();
        let results = index.search("Target", "light front", 10);
        assert_eq!(
            results.len(),
            1,
            "multi-word should match id-4, got: {:?}",
            results
        );
    }
}
