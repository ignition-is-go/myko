use leptos::prelude::*;
use myko_permissions::{
    PERMISSION_STRUCTURE_VERSION, PermissionEffect, PermissionGroupId, PermissionInspectionStatus,
    PermissionMembership, PermissionMembershipId, PermissionPrincipalInspection, PermissionRole,
    PermissionRuleSource, PermissionStructure,
};

use crate::{MembershipEdit, RoleEdit, RoleRuleChoice, set_role_rule};

fn status(structure: &PermissionStructure) -> Option<String> {
    if structure.version != PERMISSION_STRUCTURE_VERSION {
        return Some(format!(
            "Unsupported permission contract version {}",
            structure.version
        ));
    }
    match structure.status {
        PermissionInspectionStatus::Ready => None,
        PermissionInspectionStatus::Denied => Some("Permission inspection denied".into()),
        PermissionInspectionStatus::Unsupported => {
            Some("This server does not support permission inspection".into())
        }
        PermissionInspectionStatus::Unavailable => Some(format!(
            "Permission information unavailable. {}",
            structure.message
        )),
    }
}
fn command_allowed(structure: &PermissionStructure, operation: &str) -> bool {
    status(structure).is_none()
        && structure
            .commands
            .iter()
            .any(|command| command.operation == operation && command.allowed)
}

#[component]
pub fn PermissionExplorer(structure: Signal<Option<PermissionStructure>>) -> impl IntoView {
    move || {
        match structure.get() {
        None => view! { <p role="status">"Loading permissions…"</p> }.into_any(),
        Some(value) => match status(&value) {
            Some(message) => view! { <p role="alert">{message}</p> }.into_any(),
            None => view! {
                <section class="myko-permission-explorer">
                    <h2>"Entity permissions"</h2>
                    <p>"Read, write, and delete apply to entity types. Individual rows are not isolated by these grants."</p>
                    <table><caption>"Generated scopes and inheritance"</caption>
                        <thead><tr><th>"Entity"</th><th>"Inherits from"</th><th>"Scopes"</th></tr></thead>
                        <tbody>{value.entities.iter().map(|entity| view! {
                            <tr><th scope="row">{entity.entity_type.clone()}</th>
                                <td>{if entity.parents.is_empty() { "None".into() } else { entity.parents.join(", ") }}</td>
                                <td>{entity.scopes.join(", ")}</td></tr>
                        }).collect_view()}</tbody>
                    </table>
                    <details><summary>"Associated operations"</summary>
                        <ul>{value.operations.into_iter().map(|operation| view! {
                            <li><code>{operation.operation}</code>" ("{operation.kind}") "
                                {if operation.mapped { operation.required_scopes.join(" + ") } else { "Unmapped; denied by scoped authorization".into() }}</li>
                        }).collect_view()}</ul>
                    </details>
                    <details><summary>"Stored permission structure"</summary>
                        {value.document.map(|document| view! {
                            <p>{format!("{} principals · {} groups · {} roles · {} memberships · {} bindings · {} overrides", document.principals.len(), document.groups.len(), document.roles.len(), document.memberships.len(), document.bindings.len(), document.overrides.len())}</p>
                            <ul>{document.principals.into_iter().map(|entry| view! { <li>{format!("Principal {}: {} ({:?}, {}) · {} / {}", entry.id, entry.name, entry.kind, if entry.enabled { "enabled" } else { "disabled" }, entry.issuer, entry.subject)}</li> }).collect_view()}</ul>
                            <ul>{document.groups.into_iter().map(|entry| view! { <li>{format!("Group {}: {} ({})", entry.id, entry.name, if entry.enabled { "enabled" } else { "disabled" })}</li> }).collect_view()}</ul>
                            <ul>{document.roles.into_iter().map(|entry| view! { <li>{format!("Role {}: {} ({})", entry.id, entry.name, if entry.enabled { "enabled" } else { "disabled" })}<ul>{entry.rules.into_iter().map(|rule| view! { <li>{format!("{} {:?}", rule.scope, rule.effect)}</li> }).collect_view()}</ul></li> }).collect_view()}</ul>
                            <ul>{document.memberships.into_iter().map(|entry| view! { <li>{format!("Principal {} → group {}", entry.principal_id, entry.group_id)}</li> }).collect_view()}</ul>
                            <ul>{document.bindings.into_iter().map(|binding| view! { <li>{format!("Role {} → {:?}", binding.role_id, binding.grantee)}</li> }).collect_view()}</ul>
                            <ul>{document.overrides.into_iter().map(|entry| view! { <li>{format!("{:?}: {} {:?}", entry.grantee, entry.rule.scope, entry.rule.effect)}</li> }).collect_view()}</ul>
                        })}
                    </details>
                </section>
            }.into_any(),
        },
    }
    }
}

#[component]
pub fn RoleEditor(
    structure: Signal<Option<PermissionStructure>>,
    role: PermissionRole,
    #[prop(optional)] on_save: Option<Callback<RoleEdit>>,
) -> impl IntoView {
    move || {
        match structure.get() {
        Some(value) if status(&value).is_none() => {
            value.document.as_ref().and_then(|document| document.roles.iter().find(|entry| entry.id == role.id)).cloned().map_or_else(
                || view! { <p role="alert">"This role no longer exists. Reload the current role selection."</p> }.into_any(),
                |current| view! { <RoleEditorForm structure role=current on_save/> }.into_any(),
            )
        }
        _ => view! { <p role="status">"Load current permissions before editing a role."</p> }
            .into_any(),
    }
    }
}

#[component]
pub fn GroupMembershipEditor(
    structure: Signal<Option<PermissionStructure>>,
    group_id: PermissionGroupId,
    #[prop(optional)] on_save: Option<Callback<MembershipEdit>>,
) -> impl IntoView {
    move || {
        match structure.get() {
        Some(value) if status(&value).is_none() => {
            if value.document.as_ref().is_some_and(|document| document.groups.iter().any(|group| group.id == group_id)) {
                view! { <GroupMembershipEditorForm structure group_id=group_id.clone() on_save/> }.into_any()
            } else {
                view! { <p role="alert">"This group no longer exists. Reload the current group selection."</p> }.into_any()
            }
        },
        _ => view! { <p role="status">"Load current permissions before editing group membership."</p> }.into_any(),
    }
    }
}

#[component]
fn RoleEditorForm(
    structure: Signal<Option<PermissionStructure>>,
    role: PermissionRole,
    #[prop(optional_no_strip)] on_save: Option<Callback<RoleEdit>>,
) -> impl IntoView {
    let draft = RwSignal::new(role);
    let reason = RwSignal::new(String::new());
    let initial_revision = structure.get_untracked().and_then(|value| value.revision);
    let writable = move || {
        on_save.is_some()
            && structure.get().is_some_and(|value| {
                command_allowed(&value, "UpsertPermissionRole")
                    && value.revision == initial_revision
            })
    };
    let mut scopes = structure.get_untracked().map_or_else(Vec::new, |value| {
        value
            .entities
            .into_iter()
            .flat_map(|entity| entity.scopes)
            .collect::<Vec<_>>()
    });
    let known_scopes = scopes.clone();
    scopes.extend(
        draft
            .get_untracked()
            .rules
            .into_iter()
            .map(|rule| rule.scope),
    );
    scopes.sort();
    scopes.dedup();
    view! {
        <section class="myko-role-editor">
            <h2>{format!("Role: {}", draft.get_untracked().name)}</h2>
            <p>"Inherit removes this role's rule. Other roles, overrides, and parent scopes still affect effective access."</p>
            <table><caption>"Role scope rules"</caption><thead><tr><th>"Scope"</th><th>"Rule"</th></tr></thead><tbody>
                {scopes.into_iter().map(|scope| {
                    let label = if known_scopes.contains(&scope) { scope.clone() } else { format!("{scope} (obsolete; remove with Inherit)") };
                    let scope_for_value = scope.clone();
                    let scope_for_change = scope.clone();
                    view! { <tr><th scope="row">{label}</th><td>
                        <select aria-label=format!("Rule for {scope}") disabled=move || !writable()
                            prop:value=move || draft.with(|role| role.rules.iter().find(|rule| rule.scope == scope_for_value).map_or("inherit", |rule| match rule.effect { PermissionEffect::Allow => "allow", PermissionEffect::Deny => "deny" }))
                            on:change=move |event| draft.update(|role| set_role_rule(role, &scope_for_change, RoleRuleChoice::from_value(&event_target_value(&event))))>
                            <option value="inherit">"Inherit"</option><option value="allow">"Allow"</option><option value="deny">"Deny"</option>
                        </select>
                    </td></tr> }
                }).collect_view()}
            </tbody></table>
            <label>"Reason for role change"<input aria-label="Reason for role change" prop:value=move || reason.get() on:input=move |event| reason.set(event_target_value(&event)) disabled=move || !writable()/></label>
            <button type="button" disabled=move || !writable() || reason.get().trim().is_empty()
                on:click=move |_| {
                    if writable() && !reason.get_untracked().trim().is_empty()
                        && let (Some(callback), Some(expected_revision)) = (on_save, initial_revision) {
                        callback.run(RoleEdit { expected_revision, role: draft.get_untracked(), reason: reason.get_untracked() });
                    }
                }>"Save role"</button>
            <p role="status">{move || if writable() { "Changes are checked by the server before saving. A new server revision reloads this draft." } else { "Read-only. Editing requires permission and a current revision." }}</p>
        </section>
    }
}

#[component]
fn GroupMembershipEditorForm(
    structure: Signal<Option<PermissionStructure>>,
    group_id: PermissionGroupId,
    #[prop(optional_no_strip)] on_save: Option<Callback<MembershipEdit>>,
) -> impl IntoView {
    let snapshot = structure.get_untracked();
    let revision = snapshot.as_ref().and_then(|value| value.revision);
    let document = snapshot
        .and_then(|value| value.document)
        .unwrap_or_default();
    let group_name = document
        .groups
        .iter()
        .find(|group| group.id == group_id)
        .map_or_else(|| group_id.to_string(), |group| group.name.clone());
    let reason = RwSignal::new(String::new());
    view! {
        <section class="myko-membership-editor"><h2>{format!("Group: {group_name}")}</h2>
            <label>"Reason for membership change"<input aria-label="Reason for membership change" prop:value=move || reason.get() on:input=move |event| reason.set(event_target_value(&event))/></label>
            <ul>{document.principals.into_iter().map(|principal| {
                let membership = document.memberships.iter().find(|entry| entry.principal_id == principal.id && entry.group_id == group_id).cloned();
                let is_member = membership.is_some();
                let membership = membership.unwrap_or_else(|| PermissionMembership {
                    id: PermissionMembershipId::from(uuid::Uuid::new_v4().to_string()),
                    authority_id: principal.authority_id.clone(), principal_id: principal.id.clone(), group_id: group_id.clone(),
                });
                let operation = if is_member { "RemovePermissionMembership" } else { "UpsertPermissionMembership" };
                view! { <li>{principal.name}<button type="button" aria-label=format!("{} {}", if is_member { "Remove" } else { "Add" }, principal.subject)
                    disabled=move || on_save.is_none() || reason.get().trim().is_empty() || !structure.get().is_some_and(|value| value.revision == revision && command_allowed(&value, operation))
                    on:click=move |_| {
                        if let (Some(callback), Some(expected_revision)) = (on_save, revision)
                            && !reason.get_untracked().trim().is_empty()
                            && structure.get_untracked().is_some_and(|value| value.revision == revision && command_allowed(&value, operation)) {
                            callback.run(MembershipEdit { expected_revision, membership: membership.clone(), member: !is_member, reason: reason.get_untracked() });
                        }
                    }>{if is_member { "Remove member" } else { "Add member" }}</button></li> }
            }).collect_view()}</ul>
        </section>
    }
}

#[component]
pub fn PrincipalAccessInspector(
    inspection: Signal<Option<PermissionPrincipalInspection>>,
) -> impl IntoView {
    move || {
        match inspection.get() {
        None => view! { <p role="status">"Select a principal to inspect local grants."</p> }.into_any(),
        Some(value) if value.version != PERMISSION_STRUCTURE_VERSION => view! { <p role="alert">"Unsupported permission inspection version"</p> }.into_any(),
        Some(value) if value.status != PermissionInspectionStatus::Ready => view! { <p role="alert">{value.message}</p> }.into_any(),
        Some(value) => view! {
            <section class="myko-access-inspector"><h2>"Principal local access"</h2>
                <p>"Local grant evaluation only. Token scope ceilings, delegation, external identity availability, and row policies are not evaluated here."</p>
                <table><thead><tr><th>"Scope"</th><th>"Effective local access"</th><th>"Explanation"</th></tr></thead><tbody>
                    {value.scopes.into_iter().map(|scope| view! { <tr><th scope="row">{scope.scope}</th><td>{if scope.allowed { "Allow" } else { "Deny" }}</td><td><ul>
                        {scope.steps.into_iter().map(|step| view! { <li>{format!("{}: {} (distance {})", step.scope, step.decision, step.distance)}
                            <ul>{step.sources.into_iter().map(|source| view! { <li>{match source { PermissionRuleSource::PrincipalOverride(id) => format!("Principal override {id}"), PermissionRuleSource::GroupOverride(id) => format!("Group override {id}"), PermissionRuleSource::Role(id) => format!("Role {id}") }}</li> }).collect_view()}</ul>
                        </li> }).collect_view()}
                    </ul></td></tr> }).collect_view()}
                </tbody></table>
            </section>
        }.into_any(),
    }
    }
}

#[component]
pub fn PermissionMutationStatus(state: Signal<crate::PermissionMutationState>) -> impl IntoView {
    move || {
        match state.get() {
        crate::PermissionMutationState::Idle => view! { <p role="status">"No pending permission edit."</p> }.into_any(),
        crate::PermissionMutationState::Pending => view! { <p role="status">"Saving permissions… A policy change may disconnect this session."</p> }.into_any(),
        crate::PermissionMutationState::Saved { revision, change_id } => view! { <p role="status">{format!("Saved revision {revision}, change {change_id}. Reconnect to load current permissions.")}</p> }.into_any(),
        crate::PermissionMutationState::Error(error) => view! { <p role="alert">{format!("Permission edit failed: {error}. Reload current permissions before retrying.")}</p> }.into_any(),
        crate::PermissionMutationState::OutcomeUnknown => view! { <p role="alert">"Connection closed before the edit result arrived. The edit may have committed. Reconnect and inspect the current revision before retrying."</p> }.into_any(),
    }
    }
}
