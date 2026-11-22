use std::sync::Arc;

use hyphae::{CellImmutable, CellMap, MapQuery, MapValuesExt};

use crate::core::item::AnyItem;

pub type TypedViewCellMap<T> = CellMap<Arc<str>, Arc<T>, CellImmutable>;
pub type FilteredViewCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;

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
