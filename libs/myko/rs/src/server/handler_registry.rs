//! Handler registry for the cell-based server.
//!
//! Collects all Item, Query, and Report registrations from inventory
//! and provides lookup by type/id.

use std::collections::HashMap;
use std::sync::Arc;

use crate::item::{ItemParseFn, ItemRegistration};
use crate::query::{QueryCellFactory, QueryParseFn, QueryRegistration};
use crate::report::{ReportCellFactory, ReportParseFn, ReportRegistration};

/// Stored query registration data.
pub struct StoredQueryData {
    pub query_id: Arc<str>,
    pub query_item_type: Arc<str>,
    pub parse: QueryParseFn,
    pub cell_factory: QueryCellFactory,
}

/// Stored report registration data.
pub struct StoredReportData {
    pub report_id: Arc<str>,
    pub parse: ReportParseFn,
    pub cell_factory: ReportCellFactory,
}

/// Registry of all handlers for the cell-based server.
///
/// Collects registrations from inventory at construction time
/// and provides O(1) lookup by type/id.
pub struct HandlerRegistry {
    /// Item parse functions by entity type name
    item_parsers: HashMap<Arc<str>, ItemParseFn>,
    /// Query data by query id
    query_data: HashMap<Arc<str>, StoredQueryData>,
    /// Report data by report id
    report_data: HashMap<Arc<str>, StoredReportData>,
}

impl HandlerRegistry {
    /// Create a new handler registry by collecting all registrations from inventory.
    pub fn new() -> Self {
        let mut item_parsers = HashMap::new();
        let mut query_data = HashMap::new();
        let mut report_data = HashMap::new();

        // Collect item registrations
        for registration in inventory::iter::<ItemRegistration> {
            log::trace!("Registered item parser: {}", registration.entity_type);
            item_parsers.insert(registration.entity_type.into(), registration.parse);
        }

        // Collect query registrations
        for registration in inventory::iter::<QueryRegistration> {
            log::trace!("Registered query: {}", registration.query_id);
            let data = StoredQueryData {
                query_id: registration.query_id.into(),
                query_item_type: registration.query_item_type.into(),
                parse: registration.parse,
                cell_factory: registration.cell_factory,
            };
            query_data.insert(data.query_id.clone(), data);
        }

        // Collect report registrations
        for registration in inventory::iter::<ReportRegistration> {
            log::trace!("Registered report: {}", registration.report_id);
            let data = StoredReportData {
                report_id: registration.report_id.into(),
                parse: registration.parse,
                cell_factory: registration.cell_factory,
            };
            report_data.insert(data.report_id.clone(), data);
        }

        fn format_list<'a>(keys: impl Iterator<Item = &'a Arc<str>>) -> String {
            let mut items: Vec<&str> = keys.map(|k| k.as_ref()).collect();
            items.sort_unstable();
            if items.is_empty() {
                "(none)".to_string()
            } else {
                items.join(", ")
            }
        }

        log::info!(
            "HandlerRegistry initialized:\n  Items ({}):\n    {}\n  Queries ({}):\n    {}\n  Reports ({}):\n    {}",
            item_parsers.len(),
            format_list(item_parsers.keys()),
            query_data.len(),
            format_list(query_data.keys()),
            report_data.len(),
            format_list(report_data.keys()),
        );

        Self {
            item_parsers,
            query_data,
            report_data,
        }
    }

    /// Get an item parse function by entity type name.
    pub fn get_item_parser(&self, entity_type: &str) -> Option<ItemParseFn> {
        self.item_parsers.get(entity_type).copied()
    }

    /// Get query registration data by query id.
    pub fn get_query(&self, query_id: &str) -> Option<&StoredQueryData> {
        self.query_data.get(query_id)
    }

    /// Get report registration data by report id.
    pub fn get_report(&self, report_id: &str) -> Option<&StoredReportData> {
        self.report_data.get(report_id)
    }

    /// Check if an entity type has a registered parser.
    pub fn has_item_parser(&self, entity_type: &str) -> bool {
        self.item_parsers.contains_key(entity_type)
    }

    /// Get all registered entity type names.
    pub fn entity_types(&self) -> impl Iterator<Item = &Arc<str>> {
        self.item_parsers.keys()
    }

    /// Get all registered query ids.
    pub fn query_ids(&self) -> impl Iterator<Item = &Arc<str>> {
        self.query_data.keys()
    }

    /// Get all registered report ids.
    pub fn report_ids(&self) -> impl Iterator<Item = &Arc<str>> {
        self.report_data.keys()
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        // Just verify it doesn't panic - actual registrations depend on linked crates
        let registry = HandlerRegistry::new();
        // Registry should be created without error
        let _ = registry.entity_types().count();
    }
}
