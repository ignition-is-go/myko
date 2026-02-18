use std::sync::Arc;

use hypha::{CellImmutable, CellMap, CellMutable, MapDiff};

use crate::core::item::AnyItem;

pub type TypedViewCellMap<T> = CellMap<Arc<str>, T, CellImmutable>;
pub type FilteredViewCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;

pub fn erase_typed_view_map<T>(typed: TypedViewCellMap<T>) -> FilteredViewCellMap
where
    T: AnyItem + Clone + Send + Sync + 'static,
{
    fn apply_any_diff<T>(
        output: &CellMap<Arc<str>, Arc<dyn AnyItem>, CellMutable>,
        diff: &MapDiff<Arc<str>, T>,
    ) where
        T: AnyItem + Clone + Send + Sync + 'static,
    {
        match diff {
            MapDiff::Initial { entries } => {
                let existing_keys: Vec<Arc<str>> =
                    output.snapshot().into_iter().map(|(k, _)| k).collect();
                output.remove_many(existing_keys);
                output.insert_many(
                    entries
                        .iter()
                        .map(|(k, v)| (k.clone(), Arc::new(v.clone()) as Arc<dyn AnyItem>))
                        .collect(),
                );
            }
            MapDiff::Insert { key, value } => {
                output.insert(key.clone(), Arc::new(value.clone()) as Arc<dyn AnyItem>);
            }
            MapDiff::Remove { key, .. } => {
                output.remove(key);
            }
            MapDiff::Update { key, new_value, .. } => {
                output.insert(key.clone(), Arc::new(new_value.clone()) as Arc<dyn AnyItem>);
            }
            MapDiff::Batch { changes } => {
                for change in changes {
                    apply_any_diff(output, change);
                }
            }
        }
    }

    let output = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
    let output_clone = output.clone();
    let guard = typed.subscribe_diffs(move |diff| apply_any_diff(&output_clone, diff));
    output.own_guard(guard);
    output.lock()
}
