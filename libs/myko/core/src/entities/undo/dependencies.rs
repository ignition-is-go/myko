//! Prevent reversal cascades from deleting entities introduced by later work.
use super::failure;
use crate::{
    command::{CommandContext, CommandError},
    item::AnyItem,
    prelude::*,
    relationship::{Relation, iter_relations},
};
use std::collections::HashMap;
use std::{collections::HashSet, sync::Arc};

pub(super) fn validate(
    ctx: &CommandContext,
    upserts: &[Arc<dyn AnyItem>],
    deletes: &[Arc<dyn AnyItem>],
) -> Result<(), CommandError> {
    let planned: HashSet<_> = upserts
        .iter()
        .chain(deletes)
        .map(|item| (item.entity_type(), item.id()))
        .collect();
    for parent in deletes {
        for registration in iter_relations() {
            for (kind, id) in related_children(ctx, parent.as_ref(), &registration.relation) {
                require_planned(ctx, &planned, kind, &id)?;
            }
            // Removing an owned child also rewrites its owning parent's ID array.
            if let Relation::OwnsMany {
                local_type,
                foreign_type,
                extract_ids,
                ..
            } = &registration.relation
                && *foreign_type == parent.entity_type()
                && let Some(store) = ctx.__server_ctx().registry.get(local_type)
            {
                for (id, owner) in store.snapshot() {
                    if extract_ids(owner.as_any()).is_some_and(|ids| ids.contains(&parent.id())) {
                        require_planned(ctx, &planned, local_type, &id)?;
                    }
                }
            }
        }
        if let Some(graph) = ctx
            .__server_ctx()
            .graph_index()
            .filter(|graph| graph.coordinates(parent.as_ref(), MEventType::DEL))
        {
            let edges = graph
                .endpoint_delete_plan(&crate::graph::EntityRef::new(
                    parent.entity_type(),
                    parent.id(),
                ))
                .map_err(|error| failure(ctx, error))?
                .cascade_edges;
            for edge in edges {
                require_planned(ctx, &planned, edge.entity_type(), &edge.id())?;
            }
        }
    }
    validate_final_state(ctx, upserts, deletes)
}

fn require_planned(
    ctx: &CommandContext,
    planned: &HashSet<(&str, Arc<str>)>,
    kind: &str,
    id: &Arc<str>,
) -> Result<(), CommandError> {
    if planned.contains(&(kind, id.clone())) {
        Ok(())
    } else {
        Err(failure(
            ctx,
            format!("undo would change a newer dependent {kind}:{id}"),
        ))
    }
}

fn related_children(
    ctx: &CommandContext,
    parent: &dyn AnyItem,
    relation: &Relation,
) -> Vec<(&'static str, Arc<str>)> {
    match relation {
        Relation::BelongsTo {
            local_type,
            foreign_type,
            extract_fk,
            ..
        } if *foreign_type == parent.entity_type() => {
            by_foreign_key(ctx, local_type, *extract_fk, &parent.id())
        }
        Relation::EnsureFor {
            local_type,
            dependencies,
            ..
        } => dependencies
            .iter()
            .filter(|dependency| dependency.foreign_type == parent.entity_type())
            .flat_map(|dependency| {
                by_foreign_key(ctx, local_type, dependency.extract_fk, &parent.id())
            })
            .collect(),
        Relation::OwnsMany {
            local_type,
            foreign_type,
            extract_ids,
            ..
        } if *local_type == parent.entity_type() => extract_ids(parent.as_any())
            .unwrap_or_default()
            .into_iter()
            .filter(|id| {
                ctx.__server_ctx()
                    .registry
                    .get(foreign_type)
                    .is_some_and(|store| store.get_value(id).is_some())
            })
            .map(|id| (*foreign_type, id))
            .collect(),
        _ => Vec::new(),
    }
}

fn by_foreign_key(
    ctx: &CommandContext,
    kind: &'static str,
    extract: crate::relationship::FkExtractor,
    parent: &Arc<str>,
) -> Vec<(&'static str, Arc<str>)> {
    ctx.__server_ctx()
        .registry
        .get(kind)
        .map_or_else(Vec::new, |store| {
            store
                .snapshot()
                .into_iter()
                .filter(|(_, item)| extract(item.as_any()).as_ref() == Some(parent))
                .map(|(id, _)| (kind, id))
                .collect()
        })
}

fn validate_final_state(
    ctx: &CommandContext,
    upserts: &[Arc<dyn AnyItem>],
    deletes: &[Arc<dyn AnyItem>],
) -> Result<(), CommandError> {
    let desired: HashMap<_, _> = upserts
        .iter()
        .map(|item| ((item.entity_type(), item.id()), item))
        .collect();
    let removed: HashSet<_> = deletes
        .iter()
        .map(|item| (item.entity_type(), item.id()))
        .collect();
    let exists = |kind: &str, id: &Arc<str>| {
        desired.contains_key(&(kind, id.clone()))
            || (!removed.contains(&(kind, id.clone()))
                && ctx
                    .__server_ctx()
                    .registry
                    .get(kind)
                    .is_some_and(|store| store.get_value(id).is_some()))
    };
    for item in upserts {
        for registration in iter_relations() {
            let dependencies: Vec<_> = match &registration.relation {
                Relation::BelongsTo {
                    local_type,
                    foreign_type,
                    extract_fk,
                    ..
                } if *local_type == item.entity_type() => extract_fk(item.as_any())
                    .map(|id| (*foreign_type, id))
                    .into_iter()
                    .collect(),
                Relation::EnsureFor {
                    local_type,
                    dependencies,
                    ..
                } if *local_type == item.entity_type() => dependencies
                    .iter()
                    .filter_map(|dependency| {
                        (dependency.extract_fk)(item.as_any())
                            .map(|id| (dependency.foreign_type, id))
                    })
                    .collect(),
                Relation::OwnsMany {
                    local_type,
                    foreign_type,
                    extract_ids,
                    ..
                } if *local_type == item.entity_type() => extract_ids(item.as_any())
                    .unwrap_or_default()
                    .into_iter()
                    .map(|id| (*foreign_type, id))
                    .collect(),
                _ => Vec::new(),
            };
            for (kind, id) in dependencies {
                if !exists(kind, &id) {
                    return Err(failure(
                        ctx,
                        format!("undo has a missing dependency {kind}:{id}"),
                    ));
                }
            }
        }
    }
    Ok(())
}
