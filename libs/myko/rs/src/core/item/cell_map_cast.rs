use std::{hash::Hash, sync::Arc};

use hypha::{CellImmutable, CellMap, CellMutable, MapDiff, traits::CellValue};

use super::AnyItem;
use crate::common::with_id::WithTypedId;

pub fn downcast_any_item_map_diff<T: Clone + 'static>(
    diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>,
    context: &'static str,
) -> MapDiff<Arc<str>, T> {
    match diff {
        MapDiff::Initial { entries } => MapDiff::Initial {
            entries: entries
                .iter()
                .map(|(k, v)| {
                    let typed = v
                        .as_any()
                        .downcast_ref::<T>()
                        .unwrap_or_else(|| panic!("{context} type mismatch in Initial"));
                    (k.clone(), typed.clone())
                })
                .collect(),
        },
        MapDiff::Insert { key, value } => {
            let typed = value
                .as_any()
                .downcast_ref::<T>()
                .unwrap_or_else(|| panic!("{context} type mismatch in Insert"));
            MapDiff::Insert {
                key: key.clone(),
                value: typed.clone(),
            }
        }
        MapDiff::Remove { key, old_value } => {
            let typed = old_value
                .as_any()
                .downcast_ref::<T>()
                .unwrap_or_else(|| panic!("{context} type mismatch in Remove"));
            MapDiff::Remove {
                key: key.clone(),
                old_value: typed.clone(),
            }
        }
        MapDiff::Update {
            key,
            old_value,
            new_value,
        } => {
            let old_typed = old_value
                .as_any()
                .downcast_ref::<T>()
                .unwrap_or_else(|| panic!("{context} type mismatch in Update old_value"));
            let new_typed = new_value
                .as_any()
                .downcast_ref::<T>()
                .unwrap_or_else(|| panic!("{context} type mismatch in Update new_value"));
            MapDiff::Update {
                key: key.clone(),
                old_value: old_typed.clone(),
                new_value: new_typed.clone(),
            }
        }
        MapDiff::Batch { changes } => MapDiff::Batch {
            changes: changes
                .iter()
                .map(|change| downcast_any_item_map_diff::<T>(change, context))
                .collect(),
        },
    }
}

pub fn apply_map_diff<K, V>(output: &CellMap<K, V, CellMutable>, diff: &MapDiff<K, V>)
where
    K: Hash + Eq + CellValue,
    V: CellValue,
{
    output.apply_batch(vec![diff.clone()]);
}

pub fn typed_map_from_any_item<T: CellValue + 'static>(
    source: CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
    context: &'static str,
) -> CellMap<Arc<str>, T, CellImmutable> {
    let typed = CellMap::<Arc<str>, T>::new();
    let typed_clone = typed.clone();
    let guard = source.subscribe_diffs(move |diff| {
        let typed_diff = downcast_any_item_map_diff::<T>(diff, context);
        apply_map_diff(&typed_clone, &typed_diff);
    });
    typed.own_guard(guard);
    typed.lock()
}

pub fn typed_map_from_any_item_with_typed_id<T>(
    source: CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
    context: &'static str,
) -> CellMap<<T as WithTypedId>::Id, T, CellImmutable>
where
    T: CellValue + WithTypedId + 'static,
{
    let typed = CellMap::<<T as WithTypedId>::Id, T>::new();
    let typed_clone = typed.clone();
    let guard = source.subscribe_diffs(move |diff| {
        let typed_diff = downcast_any_item_map_diff::<T>(diff, context);
        let mut changes: Vec<MapDiff<<T as WithTypedId>::Id, T>> = Vec::new();
        remap_diff_to_typed_id(&typed_diff, &mut changes);
        typed_clone.apply_batch(changes);
    });
    typed.own_guard(guard);
    typed.lock()
}

fn remap_diff_to_typed_id<T>(
    diff: &MapDiff<Arc<str>, T>,
    out: &mut Vec<MapDiff<<T as WithTypedId>::Id, T>>,
) where
    T: CellValue + WithTypedId + 'static,
{
    match diff {
        MapDiff::Initial { entries } => {
            out.push(MapDiff::Initial {
                entries: entries
                    .iter()
                    .map(|(_, value)| (value.typed_id(), value.clone()))
                    .collect(),
            });
        }
        MapDiff::Insert { value, .. } => {
            out.push(MapDiff::Insert {
                key: value.typed_id(),
                value: value.clone(),
            });
        }
        MapDiff::Remove { old_value, .. } => {
            out.push(MapDiff::Remove {
                key: old_value.typed_id(),
                old_value: old_value.clone(),
            });
        }
        MapDiff::Update {
            old_value,
            new_value,
            ..
        } => {
            let old_key = old_value.typed_id();
            let new_key = new_value.typed_id();
            if old_key == new_key {
                out.push(MapDiff::Update {
                    key: new_key,
                    old_value: old_value.clone(),
                    new_value: new_value.clone(),
                });
            } else {
                out.push(MapDiff::Remove {
                    key: old_key,
                    old_value: old_value.clone(),
                });
                out.push(MapDiff::Insert {
                    key: new_key,
                    value: new_value.clone(),
                });
            }
        }
        MapDiff::Batch { changes } => {
            for change in changes {
                remap_diff_to_typed_id(change, out);
            }
        }
    }
}
