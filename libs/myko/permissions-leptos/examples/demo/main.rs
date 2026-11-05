use leptos::prelude::*;
use myko_permissions_leptos::{
    GroupMembershipEditor, MembershipEdit, PermissionExplorer, PrincipalAccessInspector, RoleEdit,
    RoleEditor, myko_permissions::*,
};

fn fixture() -> PermissionStructure {
    let principal = PermissionPrincipal {
        id: PermissionPrincipalId::from("alice"),
        authority_id: PermissionAuthorityId::from("permissions"),
        issuer: "demo".into(),
        subject: "alice".into(),
        kind: PermissionPrincipalKind::User,
        enabled: true,
        name: "Alice".into(),
    };
    let group = PermissionGroup {
        id: PermissionGroupId::from("operators"),
        authority_id: PermissionAuthorityId::from("permissions"),
        name: "Operators".into(),
        enabled: true,
    };
    let role = PermissionRole {
        id: PermissionRoleId::from("camera-operator"),
        authority_id: PermissionAuthorityId::from("permissions"),
        name: "Camera operator".into(),
        enabled: true,
        rules: vec![PermissionRule {
            scope: "StudioProject:write".into(),
            effect: PermissionEffect::Allow,
        }],
    };
    PermissionStructure {
        version: PERMISSION_STRUCTURE_VERSION,
        status: PermissionInspectionStatus::Ready,
        message: "Disposable demo".into(),
        revision: Some(1),
        entities: vec![
            PermissionEntityDescription {
                entity_type: "StudioProject".into(),
                parents: vec![],
                scopes: vec![
                    "StudioProject:read".into(),
                    "StudioProject:write".into(),
                    "StudioProject:delete".into(),
                ],
            },
            PermissionEntityDescription {
                entity_type: "RemoteCamera".into(),
                parents: vec!["StudioProject".into()],
                scopes: vec![
                    "RemoteCamera:read".into(),
                    "RemoteCamera:write".into(),
                    "RemoteCamera:delete".into(),
                ],
            },
        ],
        operations: vec![PermissionOperationDescription {
            kind: "Command".into(),
            operation: "AdjustRemoteCamera".into(),
            required_scopes: vec!["RemoteCamera:write".into()],
            mapped: true,
        }],
        document: Some(PermissionDocument {
            principals: vec![principal],
            groups: vec![group],
            roles: vec![role],
            ..PermissionDocument::default()
        }),
        commands: [
            "UpsertPermissionRole",
            "UpsertPermissionMembership",
            "RemovePermissionMembership",
        ]
        .map(|operation| PermissionCommandCapability {
            operation: operation.into(),
            allowed: true,
        })
        .to_vec(),
    }
}

#[component]
fn Demo() -> impl IntoView {
    let structure = RwSignal::new(Some(fixture()));
    let message = RwSignal::new("No changes saved.".to_string());
    let role_save = Callback::new(move |edit: RoleEdit| {
        structure.update(|value| {
            if let Some(value) = value
                && value.revision == Some(edit.expected_revision)
            {
                if let Some(document) = &mut value.document {
                    document.roles.retain(|role| role.id != edit.role.id);
                    document.roles.push(edit.role);
                }
                value.revision = value.revision.map(|revision| revision.saturating_add(1));
                message.set(format!(
                    "Role saved in disposable memory. Revision {}. {}",
                    value.revision.unwrap_or(0),
                    edit.reason
                ));
            }
        });
    });
    let membership_save = Callback::new(move |edit: MembershipEdit| {
        structure.update(|value| {
            if let Some(value) = value
                && value.revision == Some(edit.expected_revision)
            {
                if let Some(document) = &mut value.document {
                    document
                        .memberships
                        .retain(|entry| entry.id != edit.membership.id);
                    if edit.member {
                        document.memberships.push(edit.membership);
                    }
                }
                value.revision = value.revision.map(|revision| revision.saturating_add(1));
                message.set(format!(
                    "Membership saved in disposable memory. Revision {}. {}",
                    value.revision.unwrap_or(0),
                    edit.reason
                ));
            }
        });
    });
    let inspection = Signal::derive(move || {
        structure.get().map(|value| PermissionPrincipalInspection {
            version: value.version,
            status: PermissionInspectionStatus::Ready,
            message: "Synthetic local grant illustration".into(),
            revision: value.revision,
            principal_id: PermissionPrincipalId::from("alice"),
            scopes: vec![PermissionScopeExplanation {
                scope: "RemoteCamera:write".into(),
                allowed: true,
                steps: vec![
                    PermissionDecisionStep {
                        scope: "RemoteCamera:write".into(),
                        decision: "Unset".into(),
                        distance: 0,
                        sources: vec![],
                    },
                    PermissionDecisionStep {
                        scope: "StudioProject:write".into(),
                        decision: "Allow".into(),
                        distance: 1,
                        sources: vec![PermissionRuleSource::Role("camera-operator".into())],
                    },
                ],
            }],
        })
    });
    view! {
        <h1>"Myko permissions"</h1>
        <p class="notice">"Disposable component demo. Edits stay in browser memory. The access explanation below is a fixed synthetic illustration, not live server authorization."</p>
        <button type="button" on:click=move |_| structure.set(None)>"Clear loaded snapshot"</button>
        <button type="button" on:click=move |_| { structure.set(Some(fixture())); message.set("Demo snapshot reloaded.".into()); }>"Reload demo snapshot"</button>
        <PermissionExplorer structure=Signal::from(structure)/>
        <p id="save-status" role="status">{move || message.get()}</p>
        {move || structure.get().and_then(|value| value.document).map(|document| view! {
            {document.roles.into_iter().map(|role| view! { <RoleEditor structure=Signal::from(structure) role on_save=role_save/> }).collect_view()}
            <GroupMembershipEditor structure=Signal::from(structure) group_id=PermissionGroupId::from("operators") on_save=membership_save/>
        })}
        <PrincipalAccessInspector inspection/>
    }
}

fn main() {
    leptos::mount::mount_to_body(Demo);
}
