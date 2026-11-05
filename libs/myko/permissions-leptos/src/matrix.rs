use std::collections::BTreeMap;

use myko_permissions::{PermissionEntityDescription, PermissionScopeExplanation};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PermissionMatrixRow<T> {
    pub entity_type: String,
    pub parents: Vec<String>,
    pub read: Option<T>,
    pub write: Option<T>,
    pub delete: Option<T>,
}

const fn row<T>(entity_type: String, parents: Vec<String>) -> PermissionMatrixRow<T> {
    PermissionMatrixRow {
        entity_type,
        parents,
        read: None,
        write: None,
        delete: None,
    }
}

fn insert<T>(row: &mut PermissionMatrixRow<T>, action: &str, value: T) {
    match action {
        "read" => row.read = Some(value),
        "write" => row.write = Some(value),
        "delete" => row.delete = Some(value),
        _ => {}
    }
}

/// Missing action cells remain absent instead of becoming an implicit grant or denial.
#[must_use]
pub fn permission_matrix(
    entities: &[PermissionEntityDescription],
) -> Vec<PermissionMatrixRow<String>> {
    entities
        .iter()
        .map(|entity| {
            let mut result = row(entity.entity_type.clone(), entity.parents.clone());
            for scope in &entity.scopes {
                if let Some((name, action)) = scope.rsplit_once(':')
                    && name == entity.entity_type
                {
                    insert(&mut result, action, scope.clone());
                }
            }
            result
        })
        .collect()
}

/// Project only reported decisions. A missing decision is not a denial.
#[must_use]
pub fn inspection_matrix(
    scopes: &[PermissionScopeExplanation],
) -> Vec<PermissionMatrixRow<PermissionScopeExplanation>> {
    let mut rows = BTreeMap::new();
    for scope in scopes {
        if let Some((entity, action @ ("read" | "write" | "delete"))) = scope.scope.rsplit_once(':')
        {
            let current = rows
                .entry(entity.to_string())
                .or_insert_with(|| row(entity.to_string(), vec![]));
            insert(current, action, scope.clone());
        }
    }
    rows.into_values().collect()
}

/// A display edge never changes the declared scope inheritance relationships.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PermissionTreeRow<T> {
    pub row: PermissionMatrixRow<T>,
    pub depth: usize,
    pub ancestors: Vec<String>,
    pub display_parent: Option<String>,
    pub additional_parents: Vec<String>,
    pub has_children: bool,
}

/// Expand a graph into a deterministic forest with one display row per entity.
/// Cyclic components receive an alphabetical anchor after ordinary roots.
#[must_use]
pub fn permission_tree<T: Clone>(rows: &[PermissionMatrixRow<T>]) -> Vec<PermissionTreeRow<T>> {
    use std::collections::BTreeSet;

    let mut nodes = BTreeMap::new();
    for row in rows {
        nodes.entry(row.entity_type.as_str()).or_insert(row);
    }
    let mut children = BTreeMap::<&str, BTreeSet<&str>>::new();
    let mut roots = BTreeSet::new();
    for (name, row) in &nodes {
        let parents: BTreeSet<_> = row
            .parents
            .iter()
            .map(String::as_str)
            .filter(|parent| parent != name && nodes.contains_key(parent))
            .collect();
        if parents.is_empty() {
            roots.insert(*name);
        }
        for parent in parents {
            children.entry(parent).or_default().insert(name);
        }
    }
    let mut pending: Vec<_> = roots
        .into_iter()
        .rev()
        .map(|name| (name, Vec::<String>::new()))
        .collect();
    let mut visited = BTreeSet::new();
    let mut result = Vec::new();
    while visited.len() < nodes.len() {
        if pending.is_empty()
            && let Some(name) = nodes.keys().find(|name| !visited.contains(**name))
        {
            pending.push((*name, vec![]));
        }
        let Some((name, ancestors)) = pending.pop() else {
            break;
        };
        if !visited.insert(name) {
            continue;
        }
        let Some(row) = nodes.get(name) else {
            continue;
        };
        let display_parent = ancestors.last().cloned();
        let mut additional_parents: Vec<_> = row
            .parents
            .iter()
            .filter(|parent| Some(*parent) != display_parent.as_ref())
            .cloned()
            .collect();
        additional_parents.sort();
        additional_parents.dedup();
        result.push(PermissionTreeRow {
            row: (*row).clone(),
            depth: ancestors.len(),
            ancestors: ancestors.clone(),
            display_parent,
            additional_parents,
            has_children: false,
        });
        let mut path = ancestors;
        path.push(name.to_string());
        if let Some(descendants) = children.get(name) {
            pending.extend(
                descendants
                    .iter()
                    .rev()
                    .filter(|child| !visited.contains(**child))
                    .map(|child| (*child, path.clone())),
            );
        }
    }
    let displayed_parents: BTreeSet<_> = result
        .iter()
        .filter_map(|item| item.display_parent.clone())
        .collect();
    for item in &mut result {
        item.has_children = displayed_parents.contains(&item.row.entity_type);
    }
    result
}

/// Hide display descendants without changing matrix cells, controller drafts, or grants.
#[must_use]
pub fn visible_permission_tree<'a, T>(
    rows: &'a [PermissionTreeRow<T>],
    collapsed: &std::collections::BTreeSet<String>,
) -> Vec<&'a PermissionTreeRow<T>> {
    rows.iter()
        .filter(|row| !row.ancestors.iter().any(|name| collapsed.contains(name)))
        .collect()
}
