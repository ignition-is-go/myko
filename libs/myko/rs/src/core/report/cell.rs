//! Cell-based reactive reports
//!
//! Provides a reactive report system backed by hypha cells instead of
//! the actor-based stream system.
//!
//! # Example
//! ```rust,no_run
//! use myko_rs::prelude::*;
//! use myko_rs::report::cell::{CellReportContext, CellReportHandler};
//!
//! struct CountTargets;
//!
//! impl CellReportHandler for CountTargets {
//!     type Output = usize;
//!
//!     fn compute(&self, ctx: &CellReportContext) -> Cell<Self::Output, CellImmutable> {
//!         ctx.query_count("Target", |_| true)
//!     }
//! }
//! ```

use std::{collections::HashSet, sync::Arc};

use hypha::{Cell, CellImmutable, MapExt, SelectExt};

use super::super::item::AnyItem;
use crate::store::StoreRegistry;

/// Context for cell-based report handlers.
///
/// Provides access to the store registry for querying entities reactively.
#[derive(Clone)]
pub struct CellReportContext {
    pub registry: Arc<StoreRegistry>,
}

impl CellReportContext {
    /// Create a new report context.
    pub fn new(registry: Arc<StoreRegistry>) -> Self {
        Self { registry }
    }

    /// Query all entities of a given type.
    ///
    /// Returns a cell that updates whenever any entity of this type changes.
    pub fn query_all(&self, entity_type: &str) -> Cell<Vec<Arc<dyn AnyItem>>, CellImmutable> {
        self.registry
            .get_or_create(entity_type)
            .select(|_| true)
            .entries()
            .map(|entries| entries.iter().map(|(_, item)| item.clone()).collect())
    }

    /// Query entities by IDs.
    ///
    /// Returns a cell containing entities whose IDs are in the provided set.
    pub fn query_by_ids(
        &self,
        entity_type: &str,
        ids: Vec<Arc<str>>,
    ) -> Cell<Vec<Arc<dyn AnyItem>>, CellImmutable> {
        let id_set: HashSet<Arc<str>> = ids.into_iter().collect();
        self.registry
            .get_or_create(entity_type)
            .select(move |item| id_set.contains(&item.id()))
            .entries()
            .map(|entries| entries.iter().map(|(_, item)| item.clone()).collect())
    }

    /// Query entities by predicate.
    ///
    /// Returns a cell containing entities that satisfy the predicate.
    pub fn query_where<F>(
        &self,
        entity_type: &str,
        predicate: F,
    ) -> Cell<Vec<Arc<dyn AnyItem>>, CellImmutable>
    where
        F: Fn(&Arc<dyn AnyItem>) -> bool + Send + Sync + 'static,
    {
        self.registry
            .get_or_create(entity_type)
            .select(predicate)
            .entries()
            .map(|entries| entries.iter().map(|(_, item)| item.clone()).collect())
    }

    /// Query a single entity by ID.
    ///
    /// Returns a cell containing the entity or None.
    pub fn query_by_id(
        &self,
        entity_type: &str,
        id: Arc<str>,
    ) -> Cell<Option<Arc<dyn AnyItem>>, CellImmutable> {
        self.registry
            .get_or_create(entity_type)
            .select(move |item| *item.id() == *id)
            .entries()
            .map(|entries| entries.iter().next().map(|(_, item)| item.clone()))
    }

    /// Count entities matching a predicate.
    pub fn query_count<F>(&self, entity_type: &str, predicate: F) -> Cell<usize, CellImmutable>
    where
        F: Fn(&Arc<dyn AnyItem>) -> bool + Send + Sync + 'static,
    {
        self.registry
            .get_or_create(entity_type)
            .select(predicate)
            .len()
    }

    /// Execute a sub-report and get its reactive output.
    ///
    /// The returned cell updates whenever the sub-report's output changes.
    pub fn report<R: CellReportHandler>(&self, report: R) -> Cell<R::Output, CellImmutable> {
        report.compute(self)
    }
}

/// Trait for cell-based report handlers.
///
/// Reports that implement this trait produce a reactive cell that
/// updates whenever any dependency changes.
///
/// # Example
/// ```rust,no_run
/// use myko_rs::prelude::*;
/// use myko_rs::report::cell::{CellReportContext, CellReportHandler};
///
/// struct GetOnlineTargetCount;
///
/// impl CellReportHandler for GetOnlineTargetCount {
///     type Output = usize;
///
///     fn compute(&self, ctx: &CellReportContext) -> Cell<Self::Output, CellImmutable> {
///         ctx.query_count("Target", |_| true)
///     }
/// }
/// ```
pub trait CellReportHandler: Sized + Send + Sync + 'static {
    type Output: Clone + Send + Sync + 'static;

    /// Compute the report output as a reactive cell.
    ///
    /// The returned cell should update whenever any dependency changes.
    fn compute(&self, ctx: &CellReportContext) -> Cell<Self::Output, CellImmutable>;
}

#[cfg(test)]
mod tests {
    use hypha::{Gettable, MapExt};
    use serde_json::Value;

    use super::*;
    use crate::common::{to_value::ToValue, with_id::WithId};

    // Test entity
    #[derive(Debug, Clone, PartialEq)]
    struct TestTarget {
        id: Arc<str>,
        name: String,
        online: bool,
    }

    impl WithId for TestTarget {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl ToValue for TestTarget {
        fn to_value(&self) -> Value {
            serde_json::json!({
                "id": self.id.as_ref(),
                "name": self.name,
                "online": self.online
            })
        }
    }

    impl AnyItem for TestTarget {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn entity_type(&self) -> &'static str {
            "TestTarget"
        }

        fn equals(&self, other: &dyn AnyItem) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .map(|typed| self == typed)
                .unwrap_or(false)
        }
    }

    fn make_target(id: &str, name: &str, online: bool) -> Arc<dyn AnyItem> {
        Arc::new(TestTarget {
            id: id.into(),
            name: name.to_string(),
            online,
        }) as Arc<dyn AnyItem>
    }

    // Test report: count online targets
    struct OnlineTargetCount;

    impl CellReportHandler for OnlineTargetCount {
        type Output = usize;

        fn compute(&self, ctx: &CellReportContext) -> Cell<Self::Output, CellImmutable> {
            ctx.query_where("Target", |item| {
                if let Some(target) = item.as_any().downcast_ref::<TestTarget>() {
                    target.online
                } else {
                    false
                }
            })
            .map(|targets| targets.len())
        }
    }

    // Test report: list of target names
    struct TargetNames;

    impl CellReportHandler for TargetNames {
        type Output = Vec<String>;

        fn compute(&self, ctx: &CellReportContext) -> Cell<Self::Output, CellImmutable> {
            ctx.query_all("Target").map(|targets| {
                targets
                    .iter()
                    .filter_map(|item| item.as_any().downcast_ref::<TestTarget>())
                    .map(|t| t.name.clone())
                    .collect()
            })
        }
    }

    #[test]
    fn test_simple_report() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Target");

        store.insert("a".into(), make_target("a", "Alice", true));
        store.insert("b".into(), make_target("b", "Bob", false));
        store.insert("c".into(), make_target("c", "Charlie", true));

        let ctx = CellReportContext::new(registry.clone());
        let online_count = ctx.report(OnlineTargetCount);

        assert_eq!(online_count.get(), 2);

        // Update Bob to online
        store.insert("b".into(), make_target("b", "Bob", true));
        assert_eq!(online_count.get(), 3);

        // Remove a target
        store.remove(&"a".into());
        assert_eq!(online_count.get(), 2);
    }

    #[test]
    fn test_composed_report() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Target");

        store.insert("a".into(), make_target("a", "Alice", true));
        store.insert("b".into(), make_target("b", "Bob", false));

        let ctx = CellReportContext::new(registry.clone());
        let names = ctx.report(TargetNames);

        let mut result = names.get();
        result.sort();
        assert_eq!(result, vec!["Alice", "Bob"]);

        // Add a target
        store.insert("c".into(), make_target("c", "Charlie", true));
        let mut result = names.get();
        result.sort();
        assert_eq!(result, vec!["Alice", "Bob", "Charlie"]);
    }

    // Test report: uses a sub-report
    struct OnlineTargetReport {
        threshold: usize,
    }

    impl CellReportHandler for OnlineTargetReport {
        type Output = bool;

        fn compute(&self, ctx: &CellReportContext) -> Cell<Self::Output, CellImmutable> {
            let threshold = self.threshold;
            ctx.report(OnlineTargetCount)
                .map(move |count| *count >= threshold)
        }
    }

    #[test]
    fn test_sub_report() {
        let registry = Arc::new(StoreRegistry::new());
        let store = registry.get_or_create("Target");

        store.insert("a".into(), make_target("a", "Alice", true));
        store.insert("b".into(), make_target("b", "Bob", true));

        let ctx = CellReportContext::new(registry.clone());

        // Threshold 2 - should be true
        let report = ctx.report(OnlineTargetReport { threshold: 2 });
        assert!(report.get());

        // Threshold 3 - should be false
        let report = ctx.report(OnlineTargetReport { threshold: 3 });
        assert!(!report.get());

        // Add another online target
        store.insert("c".into(), make_target("c", "Charlie", true));
        // Now threshold 3 should be true
        assert!(report.get());
    }
}
