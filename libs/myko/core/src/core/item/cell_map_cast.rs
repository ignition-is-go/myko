use std::{hash::Hash, sync::Arc};

use hyphae::{CellImmutable, CellMap, CellMutable, MapDiff, traits::CellValue};

use super::AnyItem;
use crate::common::with_id::WithTypedId;

#[must_use]
pub fn downcast_any_item_map_diff<T: Clone + 'static>(
    diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>,
    context: &'static str,
) -> Option<MapDiff<Arc<str>, T>> {
    match diff {
        MapDiff::Initial { entries } => Some(MapDiff::Initial {
            entries: entries
                .iter()
                .filter_map(|(k, v)| {
                    v.as_any().downcast_ref::<T>().map_or_else(
                        || {
                            tracing::error!("{context} type mismatch in Initial");
                            None
                        },
                        |typed| Some((k.clone(), typed.clone())),
                    )
                })
                .collect(),
        }),
        MapDiff::Insert { key, value } => {
            let Some(typed) = value.as_any().downcast_ref::<T>() else {
                tracing::error!("{context} type mismatch in Insert");
                return None;
            };
            Some(MapDiff::Insert {
                key: key.clone(),
                value: typed.clone(),
            })
        }
        MapDiff::Remove { key, old_value } => {
            let Some(typed) = old_value.as_any().downcast_ref::<T>() else {
                tracing::error!("{context} type mismatch in Remove");
                return None;
            };
            Some(MapDiff::Remove {
                key: key.clone(),
                old_value: typed.clone(),
            })
        }
        MapDiff::Update {
            key,
            old_value,
            new_value,
        } => {
            let Some(old_typed) = old_value.as_any().downcast_ref::<T>() else {
                tracing::error!("{context} type mismatch in Update old_value");
                return None;
            };
            let Some(new_typed) = new_value.as_any().downcast_ref::<T>() else {
                tracing::error!("{context} type mismatch in Update new_value");
                return None;
            };
            Some(MapDiff::Update {
                key: key.clone(),
                old_value: old_typed.clone(),
                new_value: new_typed.clone(),
            })
        }
        MapDiff::Batch { changes } => Some(MapDiff::Batch {
            changes: changes
                .iter()
                .filter_map(|change| downcast_any_item_map_diff::<T>(change, context))
                .collect(),
        }),
    }
}

pub fn apply_map_diff<K, V>(output: &CellMap<K, V, CellMutable>, diff: &MapDiff<K, V>)
where
    K: Hash + Eq + CellValue,
    V: CellValue,
{
    output.apply_diff_owned(diff.clone());
}

#[must_use]
pub fn typed_map_from_any_item<T: CellValue + 'static>(
    source: CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
    context: &'static str,
) -> CellMap<Arc<str>, T, CellImmutable> {
    let typed = CellMap::<Arc<str>, T>::new();
    let weak = typed.downgrade();
    let guard = source.subscribe_diffs(move |diff| {
        let Some(typed) = weak.upgrade() else { return };
        if let Some(typed_diff) = downcast_any_item_map_diff::<T>(diff, context) {
            apply_map_diff(&typed, &typed_diff);
        }
    });
    typed.own_guard(guard);
    drop(source);
    typed.lock()
}

#[must_use]
pub fn typed_map_arc_from_any_item<T: std::fmt::Debug + PartialEq + Send + Sync + 'static>(
    source: CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
    context: &'static str,
) -> CellMap<Arc<str>, Arc<T>, CellImmutable> {
    let typed = CellMap::<Arc<str>, Arc<T>>::new();
    let weak = typed.downgrade();
    let guard = source.subscribe_diffs(move |diff| {
        let Some(typed) = weak.upgrade() else { return };
        if let Some(typed_diff) = downcast_any_item_map_diff_arc::<T>(diff, context) {
            apply_map_diff(&typed, &typed_diff);
        }
    });
    typed.own_guard(guard);
    drop(source);
    typed.lock()
}

#[must_use]
pub fn typed_map_from_any_item_with_typed_id<T>(
    source: CellMap<Arc<str>, Arc<dyn AnyItem>, CellImmutable>,
    context: &'static str,
) -> CellMap<<T as WithTypedId>::Id, Arc<T>, CellImmutable>
where
    T: CellValue + WithTypedId + Send + Sync + 'static,
{
    let typed = CellMap::<<T as WithTypedId>::Id, Arc<T>>::new();
    let weak = typed.downgrade();
    let guard = source.subscribe_diffs(move |diff| {
        let Some(typed) = weak.upgrade() else { return };
        let Some(typed_diff) = downcast_any_item_map_diff_arc::<T>(diff, context) else {
            return;
        };
        let mut changes: Vec<MapDiff<<T as WithTypedId>::Id, Arc<T>>> = Vec::new();
        remap_diff_to_typed_id(&typed_diff, &mut changes);
        if changes.len() == 1 {
            if let Some(change) = changes.pop() {
                typed.apply_diff_owned(change);
            }
        } else {
            typed.apply_batch(changes);
        }
    });
    typed.own_guard(guard);
    drop(source);
    typed.lock()
}

pub fn downcast_any_item_arc<T: Send + Sync + 'static>(
    value: &Arc<dyn AnyItem>,
    context: &'static str,
) -> Option<Arc<T>> {
    let any_arc: Arc<dyn std::any::Any + Send + Sync> = value.clone();
    any_arc.downcast::<T>().map_or_else(
        |_| {
            tracing::error!("{context} type mismatch while downcasting Arc<dyn AnyItem>");
            None
        },
        Some,
    )
}

pub fn downcast_any_item_map_diff_arc<T: Send + Sync + 'static>(
    diff: &MapDiff<Arc<str>, Arc<dyn AnyItem>>,
    context: &'static str,
) -> Option<MapDiff<Arc<str>, Arc<T>>> {
    match diff {
        MapDiff::Initial { entries } => Some(MapDiff::Initial {
            entries: entries
                .iter()
                .filter_map(|(k, v)| {
                    downcast_any_item_arc::<T>(v, context).map(|typed| (k.clone(), typed))
                })
                .collect(),
        }),
        MapDiff::Insert { key, value } => Some(MapDiff::Insert {
            key: key.clone(),
            value: downcast_any_item_arc::<T>(value, context)?,
        }),
        MapDiff::Remove { key, old_value } => Some(MapDiff::Remove {
            key: key.clone(),
            old_value: downcast_any_item_arc::<T>(old_value, context)?,
        }),
        MapDiff::Update {
            key,
            old_value,
            new_value,
        } => Some(MapDiff::Update {
            key: key.clone(),
            old_value: downcast_any_item_arc::<T>(old_value, context)?,
            new_value: downcast_any_item_arc::<T>(new_value, context)?,
        }),
        MapDiff::Batch { changes } => Some(MapDiff::Batch {
            changes: changes
                .iter()
                .filter_map(|change| downcast_any_item_map_diff_arc::<T>(change, context))
                .collect(),
        }),
    }
}

fn remap_diff_to_typed_id<T>(
    diff: &MapDiff<Arc<str>, Arc<T>>,
    out: &mut Vec<MapDiff<<T as WithTypedId>::Id, Arc<T>>>,
) where
    T: CellValue + WithTypedId + 'static,
{
    match diff {
        MapDiff::Initial { entries } => {
            out.push(MapDiff::Initial {
                entries: entries
                    .iter()
                    .map(|(_, value)| (value.as_ref().typed_id(), value.clone()))
                    .collect(),
            });
        }
        MapDiff::Insert { value, .. } => {
            out.push(MapDiff::Insert {
                key: value.as_ref().typed_id(),
                value: value.clone(),
            });
        }
        MapDiff::Remove { old_value, .. } => {
            out.push(MapDiff::Remove {
                key: old_value.as_ref().typed_id(),
                old_value: old_value.clone(),
            });
        }
        MapDiff::Update {
            old_value,
            new_value,
            ..
        } => {
            let old_key = old_value.as_ref().typed_id();
            let new_key = new_value.as_ref().typed_id();
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
