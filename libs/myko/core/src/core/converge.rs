//! Converge engine — the shared apply primitive behind restore-to-point and
//! undo-transaction.
//!
//! Given a `current` and a `target` set of flattened entities (for a scope), it
//! computes the minimal SET/DEL plan that makes the live tree equal the target, and
//! emits it as raw events. Resurrection (an entity present in `target` but currently
//! deleted) is just an ordinary SET; removals (present now, absent in `target`) become
//! DELs. Entities are re-emitted with their original ids, so references heal — this is
//! restore-in-place, never an id-remapping clone.
//!
//! See `docs/superpowers/specs/2026-06-17-restore-points-design.md`.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::report::export_tree::ExportedEntity;
use crate::wire::WrappedItem;

/// A minimal plan to converge live state to a target.
#[derive(Debug, Default, Clone)]
pub struct ConvergePlan {
    /// Entities to SET (added, resurrected, or reverted), parent-first order.
    pub sets: Vec<WrappedItem>,
    /// Entities to DEL (present now, absent in target), child-first order.
    pub dels: Vec<WrappedItem>,
}

impl ConvergePlan {
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty() && self.dels.is_empty()
    }

    pub fn len(&self) -> usize {
        self.sets.len() + self.dels.len()
    }
}

fn entity_key(e: &ExportedEntity) -> (Arc<str>, Arc<str>) {
    let id = e
        .data
        .get("id")
        .and_then(|v| v.as_str())
        .map(Arc::<str>::from)
        .unwrap_or_else(|| Arc::from(""));
    (e.entity_type.clone(), id)
}

/// Compute the converge plan from `current` to `target`.
///
/// `target` and `current` are expected in BFS (parent-first) order, as produced by
/// `walk_tree`. SETs preserve `target` order (parents first); DELs are emitted in
/// reverse `current` order (children first). Only entities that are new or whose value
/// actually differs are SET, so re-running a converge is a no-op.
pub fn converge(current: &[ExportedEntity], target: &[ExportedEntity]) -> ConvergePlan {
    let current_map: HashMap<(Arc<str>, Arc<str>), &Value> =
        current.iter().map(|e| (entity_key(e), &e.data)).collect();
    let target_keys: std::collections::HashSet<(Arc<str>, Arc<str>)> =
        target.iter().map(entity_key).collect();

    let mut sets = Vec::new();
    for e in target {
        let key = entity_key(e);
        let changed = match current_map.get(&key) {
            None => true,
            Some(cur) => **cur != e.data,
        };
        if changed {
            sets.push(WrappedItem {
                item: e.data.clone(),
                item_type: e.entity_type.clone(),
            });
        }
    }

    // Removals: present in current, absent in target — child-first (reverse BFS).
    let mut dels = Vec::new();
    for e in current.iter().rev() {
        let key = entity_key(e);
        if !target_keys.contains(&key) {
            dels.push(WrappedItem {
                item: e.data.clone(),
                item_type: e.entity_type.clone(),
            });
        }
    }

    ConvergePlan { sets, dels }
}

/// Emit a converge plan as a single batch of SET/DEL events on the command's
/// transaction. Returns the number of events applied.
///
/// DELs include only the entities computed here; the relationship manager cascades
/// deletions to descendants, so shallow removals are sufficient (matching `ImportItems`).
#[cfg(not(target_arch = "wasm32"))]
pub fn emit_converge(
    ctx: &crate::command::CommandContext,
    plan: ConvergePlan,
) -> Result<usize, crate::command::CommandError> {
    use crate::wire::{MEvent, MEventType};

    if plan.is_empty() {
        return Ok(0);
    }

    let tx = ctx.tx().to_string();
    let source_id = Some(ctx.host_id().to_string());
    let created_at = ctx.created_at().to_string();

    let mut events = Vec::with_capacity(plan.len());

    for item in &plan.sets {
        events.push(MEvent {
            item: item.item.clone(),
            change_type: MEventType::SET,
            item_type: item.item_type.to_string(),
            created_at: created_at.clone(),
            tx: tx.clone(),
            source_id: source_id.clone(),
            options: None,
        });
    }
    for item in &plan.dels {
        events.push(MEvent {
            item: item.item.clone(),
            change_type: MEventType::DEL,
            item_type: item.item_type.to_string(),
            created_at: created_at.clone(),
            tx: tx.clone(),
            source_id: source_id.clone(),
            options: None,
        });
    }

    ctx.emit_event_batch(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::export_tree::{diff_exported, ChangeStatus, ExportedEntity};
    use serde_json::json;

    fn ent(entity_type: &str, id: &str, extra: Value) -> ExportedEntity {
        let mut obj = serde_json::Map::new();
        obj.insert("id".into(), Value::String(id.to_string()));
        if let Value::Object(m) = extra {
            for (k, v) in m {
                obj.insert(k, v);
            }
        }
        ExportedEntity {
            entity_type: Arc::from(entity_type),
            data: Value::Object(obj),
        }
    }

    fn ids(items: &[WrappedItem]) -> Vec<String> {
        items
            .iter()
            .map(|w| w.item.get("id").unwrap().as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn added_entity_is_set() {
        let plan = converge(&[], &[ent("Scene", "s1", json!({}))]);
        assert_eq!(ids(&plan.sets), vec!["s1"]);
        assert!(plan.dels.is_empty());
    }

    #[test]
    fn resurrected_entity_is_set() {
        // Present in target (as of restore point) but absent from current => SET (revive).
        let current = vec![ent("Scene", "s1", json!({}))];
        let target = vec![
            ent("Scene", "s1", json!({})),
            ent("Cue", "c1", json!({})), // deleted since the restore point
        ];
        let plan = converge(&current, &target);
        assert_eq!(ids(&plan.sets), vec!["c1"]);
        assert!(plan.dels.is_empty());
    }

    #[test]
    fn unchanged_entity_is_noop() {
        let a = ent("Scene", "s1", json!({"name": "x"}));
        let plan = converge(&[a.clone()], &[a]);
        assert!(plan.is_empty());
    }

    #[test]
    fn modified_entity_is_set() {
        let current = vec![ent("Scene", "s1", json!({"name": "old"}))];
        let target = vec![ent("Scene", "s1", json!({"name": "new"}))];
        let plan = converge(&current, &target);
        assert_eq!(ids(&plan.sets), vec!["s1"]);
        assert!(plan.dels.is_empty());
    }

    #[test]
    fn removed_entities_del_child_first() {
        // current in BFS (parent-first) order; removals must be child-first.
        let current = vec![ent("Scene", "parent", json!({})), ent("Cue", "child", json!({}))];
        let plan = converge(&current, &[]);
        assert!(plan.sets.is_empty());
        assert_eq!(ids(&plan.dels), vec!["child", "parent"]);
    }

    #[test]
    fn diff_reports_add_remove_modify_and_skips_hash() {
        let before = vec![
            ent("Scene", "s1", json!({"name": "old", "hash": "h1"})),
            ent("Cue", "gone", json!({})),
        ];
        let after = vec![
            ent("Scene", "s1", json!({"name": "new", "hash": "h2"})),
            ent("Scene", "s2", json!({})),
        ];
        let changes = diff_exported(&before, &after);

        let modified = changes.iter().find(|c| c.id.as_ref() == "s1").unwrap();
        assert_eq!(modified.status, ChangeStatus::Modified);
        // Only `name` changed; `hash` is skipped as derived noise.
        assert_eq!(modified.fields.len(), 1);
        assert_eq!(modified.fields[0].field, "name");

        assert_eq!(
            changes.iter().find(|c| c.id.as_ref() == "s2").unwrap().status,
            ChangeStatus::Added
        );
        assert_eq!(
            changes.iter().find(|c| c.id.as_ref() == "gone").unwrap().status,
            ChangeStatus::Removed
        );
    }
}
