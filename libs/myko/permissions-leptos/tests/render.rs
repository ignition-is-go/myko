#![cfg(feature = "ssr")]
#![allow(clippy::panic_in_result_fn)]

use leptos::{prelude::*, reactive::owner::Owner};
use myko_permissions_leptos::{
    GroupMembershipEditor, PermissionExplorer, PrincipalAccessInspector, RoleEditor,
    myko_permissions::*,
};

fn fixture() -> PermissionStructure {
    PermissionStructure {
        version: PERMISSION_STRUCTURE_VERSION,
        status: PermissionInspectionStatus::Ready,
        message: String::new(),
        revision: Some(7),
        entities: vec![PermissionEntityDescription {
            entity_type: "ForeignCamera".into(),
            parents: vec!["ForeignProject".into(), "ForeignCamera".into()],
            scopes: vec![
                "ForeignCamera:read".into(),
                "ForeignCamera:write".into(),
                "ForeignCamera:delete".into(),
            ],
        }],
        operations: vec![PermissionOperationDescription {
            kind: "Command".into(),
            operation: "ForeignMoveCamera".into(),
            required_scopes: vec!["ForeignCamera:write".into()],
            mapped: true,
        }],
        document: Some(PermissionDocument::default()),
        commands: vec![],
    }
}

#[test]
fn explorer_renders_unknown_entities_multiple_parents_and_cycles_without_application_types() {
    Owner::new().with(|| {
        let value = Signal::stored(Some(fixture()));
        let html = view! { <PermissionExplorer structure=value/> }.to_html();
        assert!(html.contains("ForeignCamera"));
        assert!(html.contains("ForeignProject, ForeignCamera"));
        assert!(html.contains("ForeignCamera:write"));
        assert!(html.contains("ForeignMoveCamera"));
    });
}

#[test]
fn unsupported_and_denied_never_render_policy_rows() {
    Owner::new().with(|| {
        let mut value = fixture();
        value.version = 9000;
        let html =
            view! { <PermissionExplorer structure=Signal::stored(Some(value.clone()))/> }.to_html();
        assert!(html.contains("Unsupported permission contract version 9000"));
        assert!(!html.contains("ForeignCamera"));
        value.version = PERMISSION_STRUCTURE_VERSION;
        value.status = PermissionInspectionStatus::Denied;
        let html = view! { <PermissionExplorer structure=Signal::stored(Some(value))/> }.to_html();
        assert!(html.contains("Permission inspection denied"));
        assert!(!html.contains("ForeignCamera"));
    });
}

#[test]
fn role_editor_exposes_three_choices_and_is_readonly_without_capability() {
    Owner::new().with(|| {
        let role = PermissionRole {
            id: PermissionRoleId::from("operator"),
            authority_id: PermissionAuthorityId::from("permissions"),
            name: "Operators".into(),
            enabled: true,
            rules: vec![],
        };
        let mut value = fixture();
        if let Some(document) = &mut value.document {
            document.roles.push(role.clone());
        }
        let html = view! { <RoleEditor structure=Signal::stored(Some(value)) role/> }.to_html();
        assert!(html.contains("Inherit"));
        assert!(html.contains("Allow"));
        assert!(html.contains("Deny"));
        assert!(html.contains("disabled"));
        assert!(html.contains("Other roles, overrides"));
    });
}

#[test]
fn membership_loading_and_local_inspection_limits_are_explicit() {
    Owner::new().with(|| {
        let html = view! { <GroupMembershipEditor structure=Signal::stored(None) group_id=PermissionGroupId::from("operators")/> }.to_html();
        assert!(html.contains("Loading permissions"));
        let inspection = PermissionPrincipalInspection { version: PERMISSION_STRUCTURE_VERSION, status: PermissionInspectionStatus::Ready, message: String::new(), revision: Some(1), principal_id: PermissionPrincipalId::from("alice"), scopes: vec![PermissionScopeExplanation { scope: "ForeignCamera:write".into(), allowed: true, steps: vec![PermissionDecisionStep { scope: "ForeignProject:write".into(), decision: "Allow".into(), distance: 1, sources: vec![PermissionRuleSource::Role("operator".into())] }] }] };
        let html = view! { <PrincipalAccessInspector inspection=Signal::stored(Some(inspection))/> }.to_html();
        assert!(html.contains("Local grant evaluation only"));
        assert!(html.contains("ForeignProject:write"));
        assert!(html.contains("Role operator"));
    });
}

#[test]
fn defaults_render_one_entity_row_with_three_distinct_action_cells() {
    Owner::new().with(|| {
        let mut value = fixture();
        if let Some(entity) = value.entities.first_mut() {
            entity.scopes.retain(|scope| !scope.ends_with(":delete"));
        }
        let role = PermissionRole {
            id: PermissionRoleId::from("operator"),
            authority_id: PermissionAuthorityId::from("permissions"),
            name: "Operators".into(),
            enabled: true,
            rules: vec![],
        };
        if let Some(document) = &mut value.document {
            document.roles.push(role.clone());
        }
        let html =
            view! { <PermissionExplorer structure=Signal::stored(Some(value.clone()))/> }.to_html();
        assert_eq!(html.matches("<th>").count(), 4);
        assert_eq!(html.matches("<td>").count(), 3);
        assert!(html.contains("Not advertised"));
        let html = view! { <RoleEditor structure=Signal::stored(Some(value)) role/> }.to_html();
        assert_eq!(html.matches("<td>").count(), 3);
        assert_eq!(html.matches("<select").count(), 2);
        let inspection = PermissionPrincipalInspection {
            version: PERMISSION_STRUCTURE_VERSION,
            status: PermissionInspectionStatus::Ready,
            message: String::new(),
            revision: Some(7),
            principal_id: PermissionPrincipalId::from("alice"),
            scopes: vec![
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
            ],
        };
        let html =
            view! { <PrincipalAccessInspector inspection=Signal::stored(Some(inspection))/> }
                .to_html();
        assert_eq!(html.matches("<td>").count(), 3);
        assert_eq!(html.matches("<tr>").count(), 2);
        assert!(html.contains("Not reported"));
        assert!(html.contains("Why ForeignCamera:read?"));
        assert!(html.contains("Why ForeignCamera:write?"));
    });
}
