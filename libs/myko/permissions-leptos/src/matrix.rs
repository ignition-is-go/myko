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
