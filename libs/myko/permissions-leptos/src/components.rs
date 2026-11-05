use leptos::prelude::*;
use myko_permissions::{
    PERMISSION_STRUCTURE_VERSION, PermissionGroupId, PermissionInspectionStatus,
    PermissionPrincipalInspection, PermissionRole, PermissionRuleSource, PermissionStructure,
};

use crate::controllers::structure_status;
use crate::{
    MembershipEdit, PermissionEditorStatus, RoleEdit, RoleEditorController, RoleRuleChoice,
    inspection_matrix, permission_matrix, permission_tree, use_group_membership_editor,
    use_role_editor,
};

#[component]
pub fn PermissionExplorer(structure: Signal<Option<PermissionStructure>>) -> impl IntoView {
    move || {
        match structure.get() {
        None => view! { <p role="status">"Loading permissions…"</p> }.into_any(),
        Some(value) => match structure_status(&value) {
            PermissionEditorStatus::Ready => view! {
                <section class="myko-permission-explorer">
                    <h2>"Entity permissions"</h2>
                    <p>"Read, write, and delete apply to entity types. Individual rows are not isolated by these grants."</p>
                    <table><caption>"Generated scopes and inheritance"</caption>
                        <thead><tr><th>"Entity"</th><th>"Read"</th><th>"Write"</th><th>"Delete"</th></tr></thead>
                        <tbody>{permission_tree(&permission_matrix(&value.entities)).into_iter().map(|item| view! {
                            <tr data-depth=item.depth data-entity=item.row.entity_type.clone() data-display-parent=item.display_parent.clone().unwrap_or_default() data-has-children=item.has_children style=format!("--myko-permission-depth: {}", item.depth)>
                                <th scope="row"><TreeEntityLabel entity=item.row.entity_type.clone() depth=item.depth parents=item.row.parents/></th>
                                <td>{item.row.read.unwrap_or_else(|| "Not advertised".into())}</td>
                                <td>{item.row.write.unwrap_or_else(|| "Not advertised".into())}</td>
                                <td>{item.row.delete.unwrap_or_else(|| "Not advertised".into())}</td></tr>
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
            state => view! { <p role="alert">{state.message()}</p> }.into_any(),
        },
    }
    }
}

#[component]
fn TreeEntityLabel(entity: String, depth: usize, parents: Vec<String>) -> impl IntoView {
    view! {
        <span class="myko-permission-tree-label"><span aria-hidden="true">{if depth == 0 { "" } else { "↳ " }}</span>{entity}</span>
        {(!parents.is_empty()).then(|| view! { <small>{format!("Inherits from: {}", parents.join(", "))}</small> })}
    }
}

#[component]
pub fn RoleEditor(
    structure: Signal<Option<PermissionStructure>>,
    role: PermissionRole,
    #[prop(optional)] on_save: Option<Callback<RoleEdit>>,
) -> impl IntoView {
    let editor = use_role_editor(structure, Signal::stored(role.id), on_save);
    move || {
        let status = editor.status();
        if !status.is_visible() {
            return view! { <p role="status">{status.message()}</p> }.into_any();
        }
        view! {
            <section class="myko-role-editor">
                <h2>{move || editor.role().map_or_else(String::new, |role| format!("Role: {}", role.name))}</h2>
                <p>"Inherit removes this role's rule. Other roles, overrides, and parent scopes still affect effective access."</p>
                <table><caption>"Role scope rules"</caption><thead><tr><th>"Entity"</th><th>"Read"</th><th>"Write"</th><th>"Delete"</th></tr></thead><tbody>
                    {permission_tree(&editor.rows()).into_iter().map(|item| view! {
                        <tr data-depth=item.depth data-entity=item.row.entity_type.clone() data-display-parent=item.display_parent.clone().unwrap_or_default() data-has-children=item.has_children style=format!("--myko-permission-depth: {}", item.depth)>
                            <th scope="row"><TreeEntityLabel entity=item.row.entity_type.clone() depth=item.depth parents=item.row.parents/></th>
                            <td><RoleRuleCell editor scope=item.row.read/></td>
                            <td><RoleRuleCell editor scope=item.row.write/></td>
                            <td><RoleRuleCell editor scope=item.row.delete/></td>
                        </tr>
                    }).collect_view()}
                </tbody></table>
                <details><summary>"Obsolete rules"</summary><p>"These scopes are no longer advertised. Remove them before saving this role."</p>
                    {move || editor.obsolete_rules().into_iter().map(|rule| {
                        let scope = rule.scope.clone();
                        view! { <p><code>{rule.scope}</code>" "{format!("{:?}", rule.effect)}<button type="button" disabled=move || !editor.status().is_editable() on:click=move |_| { let _ = editor.remove_obsolete(&scope); }>"Remove obsolete rule"</button></p> }
                    }).collect_view()}
                </details>
                <label>"Reason for role change"<input aria-label="Reason for role change" prop:value=move || editor.reason.get() on:input=move |event| editor.reason.set(event_target_value(&event)) disabled=move || !editor.status().is_editable()/></label>
                <button type="button" disabled=move || !editor.can_submit() on:click=move |_| { let _ = editor.submit(); }>"Save role"</button>
                <button type="button" on:click=move |_| editor.reload()>"Reload role draft"</button>
                <p role="status">{move || editor.status().message()}</p>
            </section>
        }.into_any()
    }
}

#[component]
fn RoleRuleCell(editor: RoleEditorController, scope: Option<String>) -> impl IntoView {
    scope.map_or_else(|| view! { <span>"Not advertised"</span> }.into_any(), |scope| {
        let value_scope = scope.clone();
        view! {
            <select aria-label=format!("Rule for {scope}") disabled=move || !editor.status().is_editable()
                prop:value=move || match editor.rule(&value_scope) { Some(RoleRuleChoice::Allow) => "allow", Some(RoleRuleChoice::Deny) => "deny", _ => "inherit" }
                on:change=move |event| { let _ = editor.set_rule(&scope, RoleRuleChoice::from_value(&event_target_value(&event))); }>
                <option value="inherit">"Inherit"</option><option value="allow">"Allow"</option><option value="deny">"Deny"</option>
            </select>
        }.into_any()
    })
}

#[component]
pub fn GroupMembershipEditor(
    structure: Signal<Option<PermissionStructure>>,
    group_id: PermissionGroupId,
    #[prop(optional)] on_save: Option<Callback<MembershipEdit>>,
) -> impl IntoView {
    let editor = use_group_membership_editor(structure, Signal::stored(group_id), on_save);
    move || {
        let status = editor.status();
        if !status.is_visible() {
            return view! { <p role="status">{status.message()}</p> }.into_any();
        }
        view! {
            <section class="myko-membership-editor"><h2>{editor.group_name().map_or_else(String::new, |name| format!("Group: {name}"))}</h2>
                <label>"Reason for membership change"<input aria-label="Reason for membership change" prop:value=move || editor.reason.get() on:input=move |event| editor.reason.set(event_target_value(&event))/></label>
                <ul>{editor.rows().into_iter().map(|row| {
                    let is_member = row.membership.is_some();
                    let principal_id = row.principal.id.clone();
                    view! { <li>{row.principal.name}<button type="button" aria-label=format!("{} {}", if is_member { "Remove" } else { "Add" }, row.principal.subject)
                        disabled=move || !row.editable || !editor.status().is_editable() || editor.reason.get().trim().is_empty()
                        on:click=move |_| { let _ = editor.set_member(&principal_id, !is_member); }>{if is_member { "Remove member" } else { "Add member" }}</button></li> }
                }).collect_view()}</ul>
                <button type="button" on:click=move |_| editor.reload()>"Reload membership draft"</button>
                <p role="status">{move || editor.status().message()}</p>
            </section>
        }.into_any()
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
                <table><thead><tr><th>"Entity"</th><th>"Read"</th><th>"Write"</th><th>"Delete"</th></tr></thead><tbody>
                    {inspection_matrix(&value.scopes).into_iter().map(|row| view! {
                        <tr><th scope="row">{row.entity_type}</th><td><InspectionCell explanation=row.read/></td><td><InspectionCell explanation=row.write/></td><td><InspectionCell explanation=row.delete/></td></tr>
                    }).collect_view()}
                </tbody></table>
            </section>
        }.into_any(),
    }
    }
}

#[component]
fn InspectionCell(
    explanation: Option<myko_permissions::PermissionScopeExplanation>,
) -> impl IntoView {
    explanation.map_or_else(|| view! { <span>"Not reported"</span> }.into_any(), |scope| view! {
        <span>{if scope.allowed { "Allow" } else { "Deny" }}</span><details><summary>{format!("Why {}?", scope.scope)}</summary><ul>
            {scope.steps.into_iter().map(|step| view! { <li>{format!("{}: {} (distance {})", step.scope, step.decision, step.distance)}
                <ul>{step.sources.into_iter().map(|source| view! { <li>{match source { PermissionRuleSource::PrincipalOverride(id) => format!("Principal override {id}"), PermissionRuleSource::GroupOverride(id) => format!("Group override {id}"), PermissionRuleSource::Role(id) => format!("Role {id}") }}</li> }).collect_view()}</ul>
            </li> }).collect_view()}
        </ul></details>
    }.into_any())
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
