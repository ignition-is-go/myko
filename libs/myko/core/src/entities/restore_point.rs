//! Restore points — lightweight, named bookmarks into the event log.
//!
//! A restore point is a *pointer into history*, not a copy of state: it records a
//! server-stamped timestamp anchoring a moment in the event log. Past state is
//! reconstructed on demand via `ExportEntityTree { as_of }` / history replay — see
//! `docs/superpowers/specs/2026-06-17-restore-points-design.md`.
//!
//! It is an on-log `#[myko_item]` (like `Client`/`Server`) so it inherits myko's
//! reactive query/sync surface for free. `root_type`/`root_id` are opaque `Arc<str>`
//! (the framework can't name downstream domain types); they identify what the bookmark
//! is about and double as the listing/cleanup key. There is intentionally **no scope
//! field** and **no cascade cleanup** — a restore point must outlive the entities it
//! references so they can be resurrected later.

use std::sync::Arc;

use crate::prelude::*;

#[myko_item]
pub struct RestorePoint {
    /// Display name for this restore point.
    #[searchable]
    pub name: String,

    /// Optional description.
    #[serde(default)]
    pub description: Option<String>,

    /// Entity type this point is anchored to (e.g. "Project", "Scene"). Opaque to myko.
    pub root_type: Arc<str>,

    /// Entity id this point is anchored to. Opaque to myko; may not exist live.
    pub root_id: Arc<str>,

    /// RFC3339 timestamp, stamped server-side at creation. The `as_of` anchor.
    pub at_timestamp: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Commands
// ─────────────────────────────────────────────────────────────────────────────

/// Create a restore point anchored at the current server time.
///
/// The timestamp is stamped server-side (never client-supplied), which is why this is a
/// command rather than a direct client event.
#[myko_command(RestorePointId)]
pub struct CreateRestorePoint {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub root_type: Arc<str>,
    pub root_id: Arc<str>,
}

impl CommandHandler for CreateRestorePoint {
    fn execute(self, ctx: CommandContext) -> Result<RestorePointId, CommandError> {
        let id = RestorePointId(Uuid::new_v4().to_string().into());

        let restore_point = RestorePoint {
            id: id.clone(),
            name: self.name,
            description: self.description,
            root_type: self.root_type,
            root_id: self.root_id,
            at_timestamp: Utc::now().to_rfc3339(),
        };

        ctx.emit_set(&restore_point)?;

        Ok(id)
    }
}

/// Restore the subtree to the state captured by a restore point.
///
/// Converges live state to `as_of(restore_point.at_timestamp)`: reverts changed
/// entities, **resurrects** entities deleted since the point, and removes entities
/// created since. Re-emits with original ids (restore-in-place). Returns the number of
/// events applied. The restore is itself a forward transaction (so it does not rewrite
/// history and is itself undoable).
#[myko_command(usize)]
pub struct RestoreToPoint {
    pub id: RestorePointId,
}

#[cfg(not(target_arch = "wasm32"))]
impl CommandHandler for RestoreToPoint {
    fn execute(self, ctx: CommandContext) -> Result<usize, CommandError> {
        use std::collections::{HashMap, HashSet};

        use serde_json::Value;

        use crate::converge::{emit_converge, ConvergePlan};
        use crate::relationship::{iter_relations, Relation};
        use crate::report::export_tree::{build_adjacency_map, walk_tree};
        use crate::wire::WrappedItem;

        let rp = ctx
            .exec_report(GetRestorePointById { id: self.id.clone() })?
            .ok_or_else(|| CommandError {
                tx: ctx.tx().to_string(),
                command_id: "RestoreToPoint".to_string(),
                message: format!("Restore point {} not found", self.id.as_ref()),
            })?;

        let registry = ctx.registry();
        let adjacency = build_adjacency_map();
        let id_of = |v: &Value| -> Arc<str> {
            v.get("id")
                .and_then(|x| x.as_str())
                .map(Arc::<str>::from)
                .unwrap_or_else(|| Arc::from(""))
        };

        // Current subtree id-set (in-memory).
        let current = walk_tree(&rp.root_type, &rp.root_id, &registry, &adjacency);
        let mut in_subtree: HashSet<(Arc<str>, Arc<str>)> = current
            .iter()
            .map(|e| (e.entity_type.clone(), id_of(&e.data)))
            .collect();

        // Everything changed since the restore point + its value then. Fallible — a history
        // read failure aborts the restore rather than applying a partial plan.
        let changes = ctx.restore_changes_since(&rp.at_timestamp)?;

        // Scope deleted-since entities into the subtree via their value's FK fields (a
        // deleted child's parent may itself be deleted — resolve to a fixpoint).
        let mut fk_fields: HashMap<&'static str, Vec<(&'static str, &'static str)>> = HashMap::new();
        for reg in iter_relations() {
            if let Relation::BelongsTo {
                local_type,
                foreign_type,
                fk_field_json,
                ..
            } = &reg.relation
            {
                fk_fields
                    .entry(local_type)
                    .or_default()
                    .push((fk_field_json, foreign_type));
            }
        }
        let mut pending: Vec<(Arc<str>, Arc<str>, &Value)> = changes
            .iter()
            .filter_map(|(t, i, v)| v.as_ref().map(|val| (t.clone(), i.clone(), val)))
            .filter(|(t, i, _)| !in_subtree.contains(&(t.clone(), i.clone())))
            .collect();
        loop {
            let mut progressed = false;
            let mut still = Vec::with_capacity(pending.len());
            for (t, i, val) in pending {
                let member = fk_fields
                    .get(t.as_ref())
                    .map(|fields| {
                        fields.iter().any(|(field, foreign_type)| {
                            val.get(field).and_then(|x| x.as_str()).is_some_and(|pid| {
                                in_subtree
                                    .contains(&(Arc::<str>::from(*foreign_type), Arc::<str>::from(pid)))
                            })
                        })
                    })
                    .unwrap_or(false);
                if member {
                    in_subtree.insert((t.clone(), i.clone()));
                    progressed = true;
                } else {
                    still.push((t, i, val));
                }
            }
            pending = still;
            if !progressed || pending.is_empty() {
                break;
            }
        }

        // Revert/resurrect entities present at the restore point; delete those created since.
        let mut sets = Vec::new();
        let mut dels = Vec::new();
        for (t, i, value_opt) in &changes {
            if !in_subtree.contains(&(t.clone(), i.clone())) {
                continue;
            }
            match value_opt {
                Some(v) => sets.push(WrappedItem {
                    item: v.clone(),
                    item_type: t.clone(),
                }),
                None => {
                    if let Some(store) = registry.get(t)
                        && let Some(item) = store.get_value(i)
                    {
                        dels.push(WrappedItem {
                            item: item.to_value(),
                            item_type: t.clone(),
                        });
                    }
                }
            }
        }

        emit_converge(&ctx, ConvergePlan { sets, dels })
    }
}
