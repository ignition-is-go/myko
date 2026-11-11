//! Headless administration models and actions. Render these signals with any component library.
//! Memos preserve equal rows across policy revisions; mutations use the current revision and
//! ignore stale completions after disconnects. These helpers never grant permissions locally.
use crate::{PermissionConnection, PermissionMutationState};
use leptos::prelude::*;
use myko_permissions::{
    AUTHORITY_ID, ApplyPermissionChanges, PERMISSION_STRUCTURE_VERSION, PermissionAuthorityId,
    PermissionChange, PermissionChangeResult, PermissionDocument, PermissionGrantee,
    PermissionInspectionStatus, PermissionPrincipal, PermissionPrincipalKind, PermissionRole,
    PermissionRoleBinding, PermissionRoleBindingId, PermissionRoleId,
};
/// Role identifiers and display names from the live document.
#[must_use]
pub fn roles_options(permissions: PermissionConnection) -> Signal<Vec<(String, String)>> {
    Memo::new(move |_| {
        permissions
            .structure
            .get()
            .and_then(|value| value.document)
            .map_or_else(Vec::new, |document| {
                document
                    .roles
                    .into_iter()
                    .map(|role| (role.id.to_string(), role.name))
                    .collect()
            })
    })
    .into()
}

/// Group identifiers and display names from the live document.
#[must_use]
pub fn groups_options(permissions: PermissionConnection) -> Signal<Vec<(String, String)>> {
    Memo::new(move |_| {
        permissions
            .structure
            .get()
            .and_then(|value| value.document)
            .map_or_else(Vec::new, |document| {
                document
                    .groups
                    .into_iter()
                    .map(|group| (group.id.to_string(), group.name))
                    .collect()
            })
    })
    .into()
}

/// Direct and group-inherited roles, retaining disabled sources for explanation.
#[must_use]
pub fn user_roles(
    document: &PermissionDocument,
    principal: &PermissionPrincipal,
) -> Vec<PrincipalRoleSummary> {
    let mut assigned = std::collections::BTreeMap::<String, PrincipalRoleSummary>::new();
    for binding in &document.bindings {
        let source = match &binding.grantee {
            PermissionGrantee::Principal(id) if id == &principal.id => {
                Some(("Assigned directly".to_owned(), true))
            }
            PermissionGrantee::Group(id)
                if document.memberships.iter().any(|membership| {
                    membership.principal_id == principal.id && membership.group_id == *id
                }) =>
            {
                document
                    .groups
                    .iter()
                    .find(|group| group.id == *id)
                    .map(|group| {
                        (
                            format!(
                                "Via group {}{}",
                                group.name,
                                if group.enabled { "" } else { " (disabled)" }
                            ),
                            group.enabled,
                        )
                    })
            }
            _ => None,
        };
        let Some((source, active)) = source else {
            continue;
        };
        let Some(role) = document
            .roles
            .iter()
            .find(|role| role.id == binding.role_id)
        else {
            continue;
        };
        let summary = assigned
            .entry(role.id.to_string())
            .or_insert_with(|| PrincipalRoleSummary {
                role_id: role.id.clone(),
                name: role.name.clone(),
                active: false,
                sources: String::new(),
            });
        summary.active |= role.enabled && active;
        if !summary.sources.is_empty() {
            summary.sources.push_str("; ");
        }
        summary.sources.push_str(&source);
        if !role.enabled {
            summary.sources.push_str(" (role disabled)");
        }
    }
    let mut roles = assigned.into_values().collect::<Vec<_>>();
    roles.sort_by_key(|role| role.name.to_lowercase());
    roles
}

/// Construct a direct role assignment. The server validates and authorizes the change.
#[must_use]
pub fn assign_role(role_id: PermissionRoleId, grantee: PermissionGrantee) -> PermissionChange {
    PermissionChange::UpsertRoleBinding(PermissionRoleBinding {
        id: uuid::Uuid::new_v4().to_string().into(),
        authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
        role_id,
        grantee,
    })
}

#[derive(Clone, PartialEq, Eq)]
pub struct PrincipalRoleSummary {
    pub role_id: PermissionRoleId,
    pub name: String,
    pub active: bool,
    pub sources: String,
}

#[derive(Clone, PartialEq)]
pub struct PermissionUserSummary {
    pub principal: PermissionPrincipal,
    pub roles: Vec<PrincipalRoleSummary>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct RoleAssignmentSummary {
    pub role: PrincipalRoleSummary,
    pub direct: Option<PermissionRoleBindingId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleMemberSummary {
    pub principal_id: String,
    pub name: String,
    pub sources: String,
    pub direct: Option<PermissionRoleBindingId>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct PermissionReferenceEntry {
    pub key: String,
    pub name: String,
    pub kind: String,
    pub scopes: Vec<String>,
    pub parents: Vec<String>,
    pub entity: bool,
    pub mapped: bool,
}

/// Search human principals by name or assigned role.
#[must_use]
pub fn use_permission_users(
    permissions: PermissionConnection,
    search: Signal<String>,
) -> Memo<Vec<PermissionUserSummary>> {
    Memo::new(move |_| {
        let query = search.get().trim().to_lowercase();
        let Some(document) = permissions
            .structure
            .get()
            .and_then(|structure| structure.document)
        else {
            return Vec::new();
        };
        let mut users = document
            .principals
            .iter()
            .filter(|principal| principal.kind == PermissionPrincipalKind::User)
            .map(|principal| PermissionUserSummary {
                principal: principal.clone(),
                roles: user_roles(&document, principal),
            })
            .filter(|user| {
                query.is_empty()
                    || user.principal.name.to_lowercase().contains(&query)
                    || user
                        .roles
                        .iter()
                        .any(|role| role.name.to_lowercase().contains(&query))
            })
            .collect::<Vec<_>>();
        users.sort_by_key(|user| {
            (
                user.principal.name.to_lowercase(),
                user.principal.id.to_string(),
            )
        });
        users
    })
}

/// Alphabetically sorted roles, independent of assignment-only revisions.
#[must_use]
pub fn use_permission_roles(permissions: PermissionConnection) -> Memo<Vec<PermissionRole>> {
    Memo::new(move |_| {
        let mut roles = permissions
            .structure
            .get()
            .and_then(|s| s.document)
            .map(|document| document.roles)
            .unwrap_or_default();
        roles.sort_by(|a, b| a.name.cmp(&b.name));
        roles
    })
}

/// Direct and inherited roles with removable direct binding identifiers.
#[must_use]
pub fn use_grantee_roles(
    permissions: PermissionConnection,
    grantee: Signal<PermissionGrantee>,
) -> Memo<Vec<RoleAssignmentSummary>> {
    Memo::new(move |_| {
        let Some(document) = permissions
            .structure
            .get()
            .and_then(|structure| structure.document)
        else {
            return Vec::new();
        };
        let target = grantee.get();
        let roles = match &target {
            PermissionGrantee::Principal(id) => document
                .principals
                .iter()
                .find(|principal| &principal.id == id)
                .map_or_else(Vec::new, |principal| user_roles(&document, principal)),
            PermissionGrantee::Group(_) => document
                .roles
                .iter()
                .filter(|role| {
                    document
                        .bindings
                        .iter()
                        .any(|binding| binding.role_id == role.id && binding.grantee == target)
                })
                .map(|role| PrincipalRoleSummary {
                    role_id: role.id.clone(),
                    name: role.name.clone(),
                    active: role.enabled,
                    sources: "Assigned to this group".into(),
                })
                .collect(),
        };
        roles
            .into_iter()
            .map(|role| {
                let direct = document
                    .bindings
                    .iter()
                    .find(|binding| binding.role_id == role.role_id && binding.grantee == target)
                    .map(|binding| binding.id.clone());
                RoleAssignmentSummary { role, direct }
            })
            .collect::<Vec<_>>()
    })
}

/// Enabled roles without an existing direct assignment to this grantee.
#[must_use]
pub fn use_assignable_roles(
    permissions: PermissionConnection,
    grantee: Signal<PermissionGrantee>,
) -> Memo<Vec<(String, String)>> {
    Memo::new(move |_| {
        let Some(document) = permissions
            .structure
            .get()
            .and_then(|structure| structure.document)
        else {
            return Vec::new();
        };
        let target = grantee.get();
        document
            .roles
            .into_iter()
            .filter(|role| {
                role.enabled
                    && !document
                        .bindings
                        .iter()
                        .any(|binding| binding.role_id == role.id && binding.grantee == target)
            })
            .map(|role| (role.id.to_string(), role.name))
            .collect::<Vec<_>>()
    })
}

/// Human users holding a role, including group-inherited assignments.
#[must_use]
pub fn use_role_users(
    permissions: PermissionConnection,
    role_id: Signal<PermissionRoleId>,
) -> Memo<Vec<RoleMemberSummary>> {
    Memo::new(move |_| {
        let Some(document) = permissions
            .structure
            .get()
            .and_then(|structure| structure.document)
        else {
            return Vec::new();
        };
        document
            .principals
            .iter()
            .filter(|principal| principal.kind == PermissionPrincipalKind::User)
            .filter_map(|principal| {
                let role = user_roles(&document, principal)
                    .into_iter()
                    .find(|role| role.role_id == role_id.get())?;
                let direct = document
                    .bindings
                    .iter()
                    .find(|binding| {
                        binding.role_id == role_id.get()
                            && binding.grantee == PermissionGrantee::Principal(principal.id.clone())
                    })
                    .map(|binding| binding.id.clone());
                Some(RoleMemberSummary {
                    principal_id: principal.id.to_string(),
                    name: format!(
                        "{}{}",
                        principal.name,
                        if principal.enabled { "" } else { " (disabled)" }
                    ),
                    sources: role.sources,
                    direct,
                })
            })
            .collect::<Vec<_>>()
    })
}

/// Enabled human users without a direct assignment to this role.
#[must_use]
pub fn use_assignable_users(
    permissions: PermissionConnection,
    role_id: Signal<PermissionRoleId>,
) -> Memo<Vec<(String, String)>> {
    Memo::new(move |_| {
        let Some(document) = permissions
            .structure
            .get()
            .and_then(|structure| structure.document)
        else {
            return Vec::new();
        };
        document
            .principals
            .iter()
            .filter(|principal| {
                principal.kind == PermissionPrincipalKind::User
                    && principal.enabled
                    && !document.bindings.iter().any(|binding| {
                        binding.role_id == role_id.get()
                            && binding.grantee == PermissionGrantee::Principal(principal.id.clone())
                    })
            })
            .map(|principal| (principal.id.to_string(), principal.name.clone()))
            .collect::<Vec<_>>()
    })
}

/// Entity and operation scope metadata, independent of policy revisions.
#[must_use]
pub fn use_permission_reference(
    permissions: PermissionConnection,
) -> Memo<Vec<PermissionReferenceEntry>> {
    Memo::new(move |_| {
        let Some(structure) = permissions.structure.get().filter(|structure| {
            structure.version == PERMISSION_STRUCTURE_VERSION
                && structure.status == PermissionInspectionStatus::Ready
        }) else {
            return Vec::new();
        };
        let mut entries = structure
            .entities
            .into_iter()
            .map(|entity| PermissionReferenceEntry {
                key: format!("entity:{}", entity.entity_type),
                name: entity.entity_type,
                kind: "Entity".into(),
                scopes: entity.scopes,
                parents: entity.parents,
                entity: true,
                mapped: true,
            })
            .chain(
                structure
                    .operations
                    .into_iter()
                    .map(|operation| PermissionReferenceEntry {
                        key: format!("{}:{}", operation.kind, operation.operation),
                        name: operation.operation,
                        kind: operation.kind,
                        scopes: operation.required_scopes,
                        parents: vec![],
                        entity: false,
                        mapped: operation.mapped,
                    }),
            )
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.name.to_lowercase(), entry.key.clone()));
        entries
    })
}

/// Revision-safe batch mutation action and its advertised edit capability.
#[must_use]
pub fn use_permission_changes(
    permissions: PermissionConnection,
    reason: impl Into<String>,
) -> (Signal<bool>, Callback<Vec<PermissionChange>>) {
    let reason = StoredValue::new(reason.into());
    let active_request = RwSignal::new(None::<String>);
    Effect::new(move || {
        if permissions.mutation.get() != PermissionMutationState::Pending {
            active_request.set(None);
        }
    });
    let sink = myko_leptos::use_command_sink();
    let can_edit = Memo::new(move |_| {
        permissions.structure.get().is_some_and(|value| {
            value.commands.iter().any(|command| {
                command.operation == stringify!(ApplyPermissionChanges) && command.allowed
            })
        }) && permissions.mutation.get() != PermissionMutationState::Pending
    });
    let apply = Callback::new(move |changes: Vec<PermissionChange>| {
        if !can_edit.get_untracked() {
            return;
        }
        let Some(revision) = permissions
            .structure
            .get_untracked()
            .and_then(|value| value.revision)
        else {
            return;
        };
        let request = uuid::Uuid::new_v4().to_string();
        active_request.set(Some(request.clone()));
        permissions.mutation.set(PermissionMutationState::Pending);
        sink.dispatch::<_, PermissionChangeResult>(
            ApplyPermissionChanges {
                expected_revision: revision,
                changes,
                reason: reason.get_value(),
            },
            move |result| {
                if active_request.try_get_untracked().flatten().as_ref() != Some(&request)
                    || permissions.mutation.try_get_untracked()
                        != Some(PermissionMutationState::Pending)
                {
                    return;
                }
                let _ = permissions.mutation.try_set(match result {
                    Ok(result) => PermissionMutationState::Saved {
                        revision: result.revision,
                        change_id: result.change_id,
                    },
                    Err(error) => PermissionMutationState::Error(error),
                });
            },
        );
    });
    (can_edit.into(), apply)
}

/// Rule choices indexed once per draft update, rather than scanning the role in every cell.
#[must_use]
pub fn use_role_rule_values(
    editor: crate::RoleEditorController,
) -> Memo<std::collections::BTreeMap<String, &'static str>> {
    Memo::new(move |_| {
        editor
            .role()
            .map_or_else(std::collections::BTreeMap::new, |role| {
                role.rules
                    .into_iter()
                    .map(|rule| {
                        (
                            rule.scope,
                            if rule.effect == myko_permissions::PermissionEffect::Allow {
                                "allow"
                            } else {
                                "deny"
                            },
                        )
                    })
                    .collect()
            })
    })
}

/// Filtered entity hierarchy that does not change when only assignments or grants change.
#[must_use]
pub fn use_role_permission_rows(
    permissions: PermissionConnection,
    search: Signal<String>,
) -> Memo<Vec<crate::PermissionTreeRow<String>>> {
    let entities = Memo::new(move |_| {
        permissions
            .structure
            .get()
            .map_or_else(Vec::new, |s| s.entities)
    });
    Memo::new(move |_| {
        let query = search.get().trim().to_lowercase();
        let rows = crate::permission_matrix(&entities.get())
            .into_iter()
            .filter(|row| query.is_empty() || row.entity_type.to_lowercase().contains(&query))
            .collect::<Vec<_>>();
        crate::permission_tree(&rows)
    })
}

/// Format decision provenance using current role display names.
#[must_use]
pub fn permission_explanation_text(
    explanation: &myko_permissions::PermissionScopeExplanation,
    roles: &[(String, String)],
) -> String {
    use myko_permissions::PermissionRuleSource;
    let label = if explanation.allowed { "Allow" } else { "Deny" };
    let mut lines = vec![format!("{}: {label}", explanation.scope)];
    for step in &explanation.steps {
        let sources = step
            .sources
            .iter()
            .map(|source| match source {
                PermissionRuleSource::PrincipalOverride(_) => "Personal override".to_owned(),
                PermissionRuleSource::GroupOverride(_) => "Group override".to_owned(),
                PermissionRuleSource::Role(id) => {
                    roles.iter().find(|(role_id, _)| role_id == id).map_or_else(
                        || format!("Role: {id}"),
                        |(_, name)| format!("Role: {name}"),
                    )
                }
            })
            .collect::<Vec<_>>();
        lines.push(format!(
            "{}: {}{} — {}",
            step.scope,
            step.decision,
            if step.distance > 0 {
                " (inherited)"
            } else {
                ""
            },
            if sources.is_empty() {
                "No explicit rule".to_owned()
            } else {
                sources.join(", ")
            }
        ));
    }
    lines.join("\n")
}

/// Create an enabled role with no grants. Submission still requires server authorization.
#[must_use]
pub fn create_role(name: String) -> PermissionChange {
    PermissionChange::UpsertRole(PermissionRole {
        id: uuid::Uuid::new_v4().to_string().into(),
        authority_id: AUTHORITY_ID.into(),
        name,
        enabled: true,
        rules: vec![],
    })
}
/// Create an enabled group with no members or role bindings.
#[must_use]
pub fn create_group(name: String) -> PermissionChange {
    PermissionChange::UpsertGroup(myko_permissions::PermissionGroup {
        id: uuid::Uuid::new_v4().to_string().into(),
        authority_id: AUTHORITY_ID.into(),
        name,
        enabled: true,
    })
}

/// Alphabetically sorted groups, independent of assignment-only revisions.
#[must_use]
pub fn use_permission_groups(
    permissions: PermissionConnection,
) -> Memo<Vec<myko_permissions::PermissionGroup>> {
    Memo::new(move |_| {
        let mut groups = permissions
            .structure
            .get()
            .and_then(|s| s.document)
            .map(|document| document.groups)
            .unwrap_or_default();
        groups.sort_by(|a, b| a.name.cmp(&b.name));
        groups
    })
}

/// Search a permission list without changing its source rows or editor drafts.
/// Apps select the searchable text; normalization is shared across lists and pickers.
#[must_use]
pub fn use_permission_list_search<T, F>(
    rows: impl Into<Signal<Vec<T>>>,
    search: Signal<String>,
    text: F,
) -> Memo<Vec<T>>
where
    T: Clone + PartialEq + Send + Sync + 'static,
    F: Fn(&T) -> String + Send + Sync + 'static,
{
    let rows = rows.into();
    Memo::new(move |_| {
        let query = search.get().trim().to_lowercase();
        rows.get()
            .into_iter()
            .filter(|row| query.is_empty() || text(row).to_lowercase().contains(&query))
            .collect()
    })
}
