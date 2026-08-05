use std::sync::Arc;

use hyphae::{CellImmutable, CellMap, MapDiff};

use crate::core::item::AnyItem;

pub type TypedViewCellMap<T> = CellMap<Arc<str>, Arc<T>, CellImmutable>;
pub type FilteredViewCellMap = CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>;

pub fn erase_typed_view_map<T>(typed: TypedViewCellMap<T>) -> FilteredViewCellMap
where
    T: AnyItem + PartialEq + Send + Sync + 'static,
{
    fn erase_item<T>(value: &Arc<T>) -> Arc<dyn AnyItem>
    where
        T: AnyItem + PartialEq + Send + Sync + 'static,
    {
        value.clone()
    }

    fn map_any_diff<T>(diff: &MapDiff<Arc<str>, Arc<T>>) -> MapDiff<Arc<str>, Arc<dyn AnyItem>>
    where
        T: AnyItem + PartialEq + Send + Sync + 'static,
    {
        match diff {
            MapDiff::Initial { entries } => MapDiff::Initial {
                entries: entries
                    .iter()
                    .map(|(k, v)| (k.clone(), erase_item(v)))
                    .collect(),
            },
            MapDiff::Insert { key, value } => MapDiff::Insert {
                key: key.clone(),
                value: erase_item(value),
            },
            MapDiff::Remove { key, old_value } => MapDiff::Remove {
                key: key.clone(),
                old_value: erase_item(old_value),
            },
            MapDiff::Update {
                key,
                old_value,
                new_value,
            } => MapDiff::Update {
                key: key.clone(),
                old_value: erase_item(old_value),
                new_value: erase_item(new_value),
            },
            MapDiff::Batch { changes } => MapDiff::Batch {
                changes: changes.iter().map(map_any_diff::<T>).collect(),
            },
        }
    }

    let output = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
    let weak = output.downgrade();
    let guard = typed.subscribe_diffs(move |diff| {
        let Some(output) = weak.upgrade() else { return };
        let mapped = map_any_diff(diff);
        match mapped {
            MapDiff::Batch { changes } => output.apply_batch(changes),
            other => output.apply_batch(vec![other]),
        }
    });
    output.own_guard(guard);
    drop(typed);
    output.lock()
}
