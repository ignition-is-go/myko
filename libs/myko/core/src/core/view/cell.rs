use std::sync::Arc;

use hyphae::{CellImmutable, CellMap, MapDiff, MapQuery, MapValuesExt, SubscriptionGuard};

use crate::core::item::AnyItem;

pub type TypedViewCellMap<T> = CellMap<Arc<str>, Arc<T>, CellImmutable>;
pub type FilteredViewCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;

/// A diff of a type-erased view.
pub type ErasedViewDiff = MapDiff<Arc<str>, Arc<dyn AnyItem>>;

/// Materialize a view plan straight into its type-erased map.
///
/// The erasure is a `map_values` stage fused into the plan, so each changed
/// row costs one scheduled write to the output map. Materializing typed and
/// then forwarding into a second erased map cost two.
pub fn materialize_erased_view<T, Q>(plan: Q) -> FilteredViewCellMap
where
    T: AnyItem + PartialEq + Send + Sync + 'static,
    Q: MapQuery<Key = Arc<str>, Value = Arc<T>>,
{
    plan.map_values(|_, value| -> Arc<dyn AnyItem> { value.clone() })
        .materialize()
}

/// A view's output read as type-erased rows, without a second map.
///
/// Wraps the view's typed map and erases each value as a reader takes it, so
/// erasure costs no scheduled write. Subscribers see exactly the typed map's
/// diffs, in its order.
#[derive(Clone)]
pub struct ErasedViewMap {
    source: Arc<dyn ErasedSource>,
}

impl ErasedViewMap {
    /// Read a typed view map as erased rows.
    #[must_use]
    pub fn from_typed<T>(map: TypedViewCellMap<T>) -> Self
    where
        T: AnyItem + PartialEq + Send + Sync + 'static,
    {
        Self {
            source: Arc::new(map),
        }
    }

    /// Subscribe to erased diffs, starting with `MapDiff::Initial`.
    ///
    /// The returned guard keeps the underlying map alive; dropping it cancels
    /// the subscription.
    pub fn subscribe_diffs<F>(&self, callback: F) -> SubscriptionGuard
    where
        F: Fn(&ErasedViewDiff) + Send + Sync + 'static,
    {
        self.source.subscribe_erased(Box::new(callback))
    }

    /// Point-in-time erased rows.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(Arc<str>, Arc<dyn AnyItem>)> {
        self.source.snapshot_erased()
    }
}

impl From<FilteredViewCellMap> for ErasedViewMap {
    fn from(map: FilteredViewCellMap) -> Self {
        Self {
            source: Arc::new(map),
        }
    }
}

type ErasedCallback = Box<dyn Fn(&ErasedViewDiff) + Send + Sync>;

trait ErasedSource: Send + Sync {
    fn subscribe_erased(&self, callback: ErasedCallback) -> SubscriptionGuard;
    fn snapshot_erased(&self) -> Vec<(Arc<str>, Arc<dyn AnyItem>)>;
}

impl<T> ErasedSource for TypedViewCellMap<T>
where
    T: AnyItem + PartialEq + Send + Sync + 'static,
{
    fn subscribe_erased(&self, callback: ErasedCallback) -> SubscriptionGuard {
        self.subscribe_diffs(move |diff| callback(&erase_diff(diff)))
    }

    fn snapshot_erased(&self) -> Vec<(Arc<str>, Arc<dyn AnyItem>)> {
        self.snapshot()
            .into_iter()
            .map(|(key, value)| (key, erase(&value)))
            .collect()
    }
}

impl ErasedSource for FilteredViewCellMap {
    fn subscribe_erased(&self, callback: ErasedCallback) -> SubscriptionGuard {
        self.subscribe_diffs(callback)
    }

    fn snapshot_erased(&self) -> Vec<(Arc<str>, Arc<dyn AnyItem>)> {
        self.snapshot()
    }
}

fn erase<T: AnyItem + 'static>(value: &Arc<T>) -> Arc<dyn AnyItem> {
    value.clone()
}

fn erase_diff<T: AnyItem + 'static>(diff: &MapDiff<Arc<str>, Arc<T>>) -> ErasedViewDiff {
    match diff {
        MapDiff::Initial { entries } => MapDiff::Initial {
            entries: entries.iter().map(|(k, v)| (k.clone(), erase(v))).collect(),
        },
        MapDiff::Insert { key, value } => MapDiff::Insert {
            key: key.clone(),
            value: erase(value),
        },
        MapDiff::Remove { key, old_value } => MapDiff::Remove {
            key: key.clone(),
            old_value: erase(old_value),
        },
        MapDiff::Update {
            key,
            old_value,
            new_value,
        } => MapDiff::Update {
            key: key.clone(),
            old_value: erase(old_value),
            new_value: erase(new_value),
        },
        MapDiff::Batch { changes } => MapDiff::Batch {
            changes: changes.iter().map(erase_diff).collect(),
        },
    }
}
