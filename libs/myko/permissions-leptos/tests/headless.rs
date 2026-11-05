#![cfg(feature = "ssr")]

use std::sync::{Arc, Mutex};

use leptos::prelude::*;
use myko_permissions_leptos::{
    MembershipEdit, PermissionEditError, PermissionEditorStatus, RoleEdit, RoleRuleChoice,
    inspection_matrix,
    myko_permissions::{
        PERMISSION_STRUCTURE_VERSION, PermissionAuthorityId, PermissionCommandCapability,
        PermissionDocument, PermissionEffect, PermissionEntityDescription, PermissionGroup,
        PermissionGroupId, PermissionInspectionStatus, PermissionMembership,
        PermissionMembershipId, PermissionPrincipal, PermissionPrincipalId,
        PermissionPrincipalKind, PermissionRole, PermissionRoleId, PermissionRule,
        PermissionScopeExplanation, PermissionStructure,
    },
    permission_matrix, use_group_membership_editor, use_role_editor,
};

fn snapshot() -> PermissionStructure {
    let authority_id = PermissionAuthorityId::from("permissions");
    PermissionStructure {
        version: PERMISSION_STRUCTURE_VERSION,
        status: PermissionInspectionStatus::Ready,
        message: String::new(),
        revision: Some(5),
        entities: vec![PermissionEntityDescription {
            entity_type: "ForeignCamera".into(),
            parents: vec!["Project".into(), "ForeignCamera".into()],
            scopes: vec![
                "ForeignCamera:read".into(),
                "ForeignCamera:write".into(),
                "ForeignCamera:execute".into(),
            ],
        }],
        operations: vec![],
        document: Some(PermissionDocument {
            roles: vec![
                PermissionRole {
                    id: PermissionRoleId::from("role-a"),
                    authority_id: authority_id.clone(),
                    name: "Operators".into(),
                    enabled: true,
                    rules: vec![PermissionRule {
                        scope: "Legacy:execute".into(),
                        effect: PermissionEffect::Allow,
                    }],
                },
                PermissionRole {
                    id: PermissionRoleId::from("role-b"),
                    authority_id: authority_id.clone(),
                    name: "Readers".into(),
                    enabled: true,
                    rules: vec![],
                },
            ],
            groups: vec![
                PermissionGroup {
                    id: PermissionGroupId::from("group-a"),
                    authority_id: authority_id.clone(),
                    name: "Operators".into(),
                    enabled: true,
                },
                PermissionGroup {
                    id: PermissionGroupId::from("group-b"),
                    authority_id: authority_id.clone(),
                    name: "Readers".into(),
                    enabled: true,
                },
            ],
            principals: ["alice", "bob"]
                .map(|name| PermissionPrincipal {
                    id: PermissionPrincipalId::from(name),
                    authority_id: authority_id.clone(),
                    issuer: "fixture".into(),
                    subject: name.into(),
                    kind: PermissionPrincipalKind::User,
                    enabled: true,
                    name: name.into(),
                })
                .to_vec(),
            memberships: vec![PermissionMembership {
                id: PermissionMembershipId::from("existing"),
                authority_id,
                principal_id: PermissionPrincipalId::from("alice"),
                group_id: PermissionGroupId::from("group-a"),
            }],
            ..PermissionDocument::default()
        }),
        commands: ["UpsertPermissionRole", "UpsertPermissionMembership"]
            .map(|operation| PermissionCommandCapability {
                operation: operation.into(),
                allowed: true,
            })
            .into_iter()
            .chain([PermissionCommandCapability {
                operation: "RemovePermissionMembership".into(),
                allowed: false,
            }])
            .collect(),
    }
}

#[test]
fn matrix_keeps_missing_actions_distinct_from_inherit_and_denial() {
    let value = snapshot();
    let rows = permission_matrix(&value.entities);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.first().map(|row| row.parents.len()), Some(2));
    assert_eq!(
        rows.first().and_then(|row| row.read.as_deref()),
        Some("ForeignCamera:read")
    );
    assert!(rows.first().is_some_and(|row| row.delete.is_none()));
    let rows = inspection_matrix(&[
        PermissionScopeExplanation {
            scope: "ForeignCamera:read".into(),
            allowed: false,
            steps: vec![],
        },
        PermissionScopeExplanation {
            scope: "ForeignCamera:write".into(),
            allowed: true,
            steps: vec![],
        },
    ]);
    assert_eq!(rows.len(), 1);
    assert!(
        rows.first()
            .and_then(|row| row.read.as_ref())
            .is_some_and(|cell| !cell.allowed)
    );
    assert!(
        rows.first()
            .and_then(|row| row.write.as_ref())
            .is_some_and(|cell| cell.allowed)
    );
    assert!(rows.first().is_some_and(|row| row.delete.is_none()));
}

#[test]
fn headless_role_loads_async_edits_and_rechecks_revision_before_submission() {
    Owner::new().with(|| {
        let source = RwSignal::new(None);
        let edits = Arc::new(Mutex::new(Vec::<RoleEdit>::new()));
        let captured = edits.clone();
        let editor = use_role_editor(
            source.into(),
            Signal::stored(PermissionRoleId::from("role-a")),
            Some(Callback::new(move |edit| {
                captured
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(edit);
            })),
        );
        assert_eq!(editor.status(), PermissionEditorStatus::Loading);
        assert!(editor.role().is_none());
        source.set(Some(snapshot()));
        assert_eq!(editor.status(), PermissionEditorStatus::Ready);
        assert_eq!(editor.rule("ForeignCamera:delete"), None);
        assert!(
            editor
                .set_rule("ForeignCamera:delete", RoleRuleChoice::Deny)
                .is_err()
        );
        assert_eq!(
            editor.set_rule("ForeignCamera:write", RoleRuleChoice::Deny),
            Ok(())
        );
        assert_eq!(editor.obsolete_rules().len(), 1);
        assert_eq!(editor.remove_obsolete("Legacy:execute"), Ok(()));
        editor.reason.set("Restrict camera control".into());
        assert_eq!(editor.submit(), Ok(()));
        assert_eq!(
            edits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .first()
                .map(|edit| edit.expected_revision),
            Some(5)
        );
        source.update(|value| {
            if let Some(value) = value {
                value.revision = Some(6);
            }
        });
        assert_eq!(editor.status(), PermissionEditorStatus::Stale);
        assert!(!editor.can_submit());
        assert!(editor.submit().is_err());
        editor.reload();
        assert_eq!(editor.status(), PermissionEditorStatus::Ready);
        assert!(editor.reason.get_untracked().is_empty());
        editor.reason.set("Current revision edit".into());
        assert_eq!(editor.submit(), Ok(()));
        assert_eq!(
            edits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .last()
                .map(|edit| edit.expected_revision),
            Some(6)
        );
    });
}

#[test]
fn hidden_policy_and_changed_selection_cannot_expose_or_submit_previous_role() {
    Owner::new().with(|| {
        let source = RwSignal::new(Some(snapshot()));
        let selected = RwSignal::new(PermissionRoleId::from("role-a"));
        let editor = use_role_editor(source.into(), selected.into(), Some(Callback::new(|_| {})));
        assert_eq!(
            editor.set_rule("ForeignCamera:write", RoleRuleChoice::Deny),
            Ok(())
        );
        editor.reason.set("Old role draft".into());
        selected.set(PermissionRoleId::from("role-b"));
        assert_eq!(
            editor.role().map(|role| role.id),
            Some(PermissionRoleId::from("role-b"))
        );
        assert!(editor.reason.get_untracked().is_empty());
        assert_eq!(editor.submit(), Err(PermissionEditError::ReasonRequired));
        source.update(|value| {
            if let Some(value) = value {
                value.status = PermissionInspectionStatus::Denied;
            }
        });
        assert_eq!(editor.status(), PermissionEditorStatus::Denied);
        assert!(editor.role().is_none());
        assert!(editor.rows().is_empty());
        assert!(editor.submit().is_err());
        source.set(Some(snapshot()));
        assert_eq!(
            editor.rule("ForeignCamera:write"),
            Some(RoleRuleChoice::Inherit)
        );
        source.update(|value| {
            if let Some(value) = value {
                value.version = 999;
            }
        });
        assert!(matches!(
            editor.status(),
            PermissionEditorStatus::Unsupported(_)
        ));
        assert!(editor.role().is_none());
        source.set(None);
        assert_eq!(editor.status(), PermissionEditorStatus::Loading);
    });
}

#[test]
fn headless_membership_checks_each_operation_and_current_target() {
    Owner::new().with(|| {
        let source = RwSignal::new(Some(snapshot()));
        let selected = RwSignal::new(PermissionGroupId::from("group-a"));
        let edits = Arc::new(Mutex::new(Vec::<MembershipEdit>::new()));
        let captured = edits.clone();
        let editor = use_group_membership_editor(
            source.into(),
            selected.into(),
            Some(Callback::new(move |edit| {
                captured
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(edit);
            })),
        );
        assert_eq!(editor.status(), PermissionEditorStatus::Ready);
        editor.reason.set("Membership update".into());
        assert!(
            editor
                .set_member(&PermissionPrincipalId::from("alice"), false)
                .is_err()
        );
        assert_eq!(
            editor.set_member(&PermissionPrincipalId::from("bob"), true),
            Ok(())
        );
        let saved = edits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(saved.len(), 1);
        assert_eq!(
            saved
                .first()
                .map(|edit| edit.membership.principal_id.clone()),
            Some(PermissionPrincipalId::from("bob"))
        );
        drop(saved);
        selected.set(PermissionGroupId::from("group-b"));
        assert_eq!(editor.group_name().as_deref(), Some("Readers"));
        assert!(editor.reason.get_untracked().is_empty());
        source.update(|value| {
            if let Some(value) = value {
                value.revision = Some(6);
            }
        });
        assert_eq!(editor.status(), PermissionEditorStatus::Stale);
        assert!(
            editor
                .set_member(&PermissionPrincipalId::from("bob"), true)
                .is_err()
        );
        source.set(None);
        assert!(editor.rows().is_empty());
    });
}

#[test]
fn an_app_can_render_and_submit_with_its_own_markup_only() {
    Owner::new().with(|| {
        let saved = Arc::new(Mutex::new(None::<RoleEdit>));
        let captured = saved.clone();
        let editor = use_role_editor(Signal::stored(Some(snapshot())), Signal::stored(PermissionRoleId::from("role-a")), Some(Callback::new(move |edit| *captured.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(edit))));
        assert_eq!(editor.set_rule("ForeignCamera:read", RoleRuleChoice::Allow), Ok(()));
        editor.reason.set("App-owned form".into());
        let html = view! { <article class="host-card"><h1>"Access settings"</h1><button class="host-primary" disabled=!editor.can_submit()>"Apply access"</button><output>{format!("{:?}", editor.rule("ForeignCamera:read"))}</output></article> }.to_html();
        assert!(html.contains("host-card"));
        assert!(html.contains("host-primary"));
        assert!(html.contains("Some(Allow)"));
        assert!(!html.contains("myko-role-editor"));
        assert!(!html.contains("<section"));
        assert_eq!(editor.submit(), Ok(()));
        assert_eq!(saved.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().map(|edit| edit.reason.as_str()), Some("App-owned form"));
    });
}

#[test]
fn reload_invalidates_derived_status_without_another_server_update() {
    Owner::new().with(|| {
        let source = RwSignal::new(Some(snapshot()));
        let role = use_role_editor(
            source.into(),
            Signal::stored(PermissionRoleId::from("role-a")),
            Some(Callback::new(|_| {})),
        );
        let group = use_group_membership_editor(
            source.into(),
            Signal::stored(PermissionGroupId::from("group-a")),
            Some(Callback::new(|_| {})),
        );
        let role_status = Memo::new(move |_| role.status());
        let group_status = Memo::new(move |_| group.status());
        assert_eq!(role_status.get(), PermissionEditorStatus::Ready);
        assert_eq!(group_status.get(), PermissionEditorStatus::Ready);
        source.update(|value| {
            if let Some(value) = value {
                value.revision = Some(6);
            }
        });
        assert_eq!(role_status.get(), PermissionEditorStatus::Stale);
        assert_eq!(group_status.get(), PermissionEditorStatus::Stale);
        role.reload();
        group.reload();
        assert_eq!(role_status.get(), PermissionEditorStatus::Ready);
        assert_eq!(group_status.get(), PermissionEditorStatus::Ready);
    });
}
