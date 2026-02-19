use std::sync::Arc;

use hypha::{CellImmutable, CellMap, MapDiff};

use crate::core::item::AnyItem;

pub type TypedViewCellMap<T> = CellMap<Arc<str>, T, CellImmutable>;
pub type FilteredViewCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;

pub fn erase_typed_view_map<T>(typed: TypedViewCellMap<T>) -> FilteredViewCellMap
where
    T: AnyItem + Clone + Send + Sync + 'static,
{
    fn map_any_diff<T>(diff: &MapDiff<Arc<str>, T>) -> MapDiff<Arc<str>, Arc<dyn AnyItem>>
    where
        T: AnyItem + Clone + Send + Sync + 'static,
    {
        match diff {
            MapDiff::Initial { entries } => MapDiff::Initial {
                entries: entries
                    .iter()
                    .map(|(k, v)| (k.clone(), Arc::new(v.clone()) as Arc<dyn AnyItem>))
                    .collect(),
            },
            MapDiff::Insert { key, value } => MapDiff::Insert {
                key: key.clone(),
                value: Arc::new(value.clone()) as Arc<dyn AnyItem>,
            },
            MapDiff::Remove { key, old_value } => MapDiff::Remove {
                key: key.clone(),
                old_value: Arc::new(old_value.clone()) as Arc<dyn AnyItem>,
            },
            MapDiff::Update {
                key,
                old_value,
                new_value,
            } => MapDiff::Update {
                key: key.clone(),
                old_value: Arc::new(old_value.clone()) as Arc<dyn AnyItem>,
                new_value: Arc::new(new_value.clone()) as Arc<dyn AnyItem>,
            },
            MapDiff::Batch { changes } => MapDiff::Batch {
                changes: changes.iter().map(map_any_diff::<T>).collect(),
            },
        }
    }

    let output = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
    let output_clone = output.clone();
    let guard = typed.subscribe_diffs(move |diff| {
        let mapped = map_any_diff(diff);
        match mapped {
            MapDiff::Batch { changes } => output_clone.apply_batch(changes),
            other => output_clone.apply_batch(vec![other]),
        }
    });
    output.own_guard(guard);
    output.lock()
}
