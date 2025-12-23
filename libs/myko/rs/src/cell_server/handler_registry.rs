//! Handler registry for the cell-based server.
//!
//! Collects all Item, Query, and Report registrations from inventory
//! and provides lookup by type/id.

use std::collections::HashMap;
use std::sync::Arc;

use crate::item::ItemRegistration;
use crate::parsers::item::MykoItemParser;
use crate::query::{QueryRegistration, RegisterQueryData};
use crate::report::{RegisterReportData, ReportRegistration};

/// Registry of all handlers for the cell-based server.
///
/// Collects registrations from inventory at construction time
/// and provides O(1) lookup by type/id.
pub struct HandlerRegistry {
    /// Item parsers by entity type name
    item_parsers: HashMap<Arc<str>, Arc<dyn MykoItemParser>>,
    /// Query factories by query id
    query_factories: HashMap<Arc<str>, RegisterQueryData>,
    /// Report factories by report id
    report_factories: HashMap<Arc<str>, RegisterReportData>,
}

impl HandlerRegistry {
    /// Create a new handler registry by collecting all registrations from inventory.
    pub fn new() -> Self {
        let mut item_parsers = HashMap::new();
        let mut query_factories = HashMap::new();
        let mut report_factories = HashMap::new();

        // Collect item registrations
        for registration in inventory::iter::<ItemRegistration> {
            let data = (registration.factory)();
            log::trace!("Registered item parser: {}", data.entity_type);
            item_parsers.insert(data.entity_type.clone(), data.parser);
        }

        // Collect query registrations
        for registration in inventory::iter::<QueryRegistration> {
            let data = (registration.factory)();
            log::trace!("Registered query: {}", data.query_id);
            query_factories.insert(data.query_id.clone(), data);
        }

        // Collect report registrations
        for registration in inventory::iter::<ReportRegistration> {
            let data = (registration.factory)();
            log::trace!("Registered report: {}", data.report_id);
            report_factories.insert(data.report_id.clone(), data);
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
            query_factories.len(),
            format_list(query_factories.keys()),
            report_factories.len(),
            format_list(report_factories.keys()),
        );


        Self {
            item_parsers,
            query_factories,
            report_factories,
        }
    }

    /// Get an item parser by entity type name.
    pub fn get_item_parser(&self, entity_type: &str) -> Option<&Arc<dyn MykoItemParser>> {
        self.item_parsers.get(entity_type)
    }

    /// Get query registration data by query id.
    pub fn get_query(&self, query_id: &str) -> Option<&RegisterQueryData> {
        self.query_factories.get(query_id)
    }

    /// Get report registration data by report id.
    pub fn get_report(&self, report_id: &str) -> Option<&RegisterReportData> {
        self.report_factories.get(report_id)
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
        self.query_factories.keys()
    }

    /// Get all registered report ids.
    pub fn report_ids(&self) -> impl Iterator<Item = &Arc<str>> {
        self.report_factories.keys()
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
