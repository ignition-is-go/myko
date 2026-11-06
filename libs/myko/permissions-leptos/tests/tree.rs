#![cfg(feature = "ssr")]

use std::collections::BTreeSet;

use leptos::prelude::*;
use myko_permissions_leptos::{
    PermissionExplorer, PermissionMatrixRow, RoleEditor, RoleRuleChoice,
    myko_permissions::{
        PERMISSION_STRUCTURE_VERSION, PermissionAuthorityId, PermissionCommandCapability,
        PermissionDocument, PermissionEntityDescription, PermissionInspectionStatus,
        PermissionRole, PermissionRoleId, PermissionStructure,
    },
    permission_tree, use_role_editor, visible_permission_tree,
};

fn row(name: &str, parents: &[&str]) -> PermissionMatrixRow<String> {
    PermissionMatrixRow {
        entity_type: name.into(),
        parents: parents.iter().map(|parent| (*parent).into()).collect(),
        read: Some(format!("{name}:read")),
        write: Some(format!("{name}:write")),
        delete: Some(format!("{name}:delete")),
    }
}
fn hierarchy() -> Vec<PermissionMatrixRow<String>> {
    vec![
        row("RemoteCamera", &["StudioProject"]),
        row("Client", &["Server"]),
        row("PermissionRole", &["PermissionAuthority"]),
        row("StudioProject", &[]),
        row("PermissionAuthority", &[]),
        row("PermissionPrincipal", &["PermissionAuthority"]),
        row("Server", &[]),
    ]
}
fn structure() -> PermissionStructure {
    PermissionStructure {
        version: PERMISSION_STRUCTURE_VERSION,
        status: PermissionInspectionStatus::Ready,
        message: String::new(),
        revision: Some(1),
        entities: hierarchy()
            .into_iter()
            .map(|row| PermissionEntityDescription {
                entity_type: row.entity_type,
                parents: row.parents,
                scopes: [row.read, row.write, row.delete]
                    .into_iter()
                    .flatten()
                    .collect(),
            })
            .collect(),
        document: Some(PermissionDocument {
            roles: vec![PermissionRole {
                id: PermissionRoleId::from("operator"),
                authority_id: PermissionAuthorityId::from("permissions"),
                name: "Operators".into(),
                enabled: true,
                rules: vec![],
            }],
            ..PermissionDocument::default()
        }),
        operations: vec![],
        commands: vec![PermissionCommandCapability {
            operation: "UpsertPermissionRole".into(),
            allowed: true,
        }],
    }
}

#[test]
fn declared_hierarchies_are_grouped_with_parent_before_sorted_children() {
    let rows = permission_tree(&hierarchy());
    let names: Vec<_> = rows
        .iter()
        .map(|item| (item.row.entity_type.as_str(), item.depth))
        .collect();
    assert_eq!(
        names,
        [
            ("PermissionAuthority", 0),
            ("PermissionPrincipal", 1),
            ("PermissionRole", 1),
            ("Server", 0),
            ("Client", 1),
            ("StudioProject", 0),
            ("RemoteCamera", 1)
        ]
    );
    assert!(
        rows.iter()
            .filter(|item| item.has_children)
            .map(|item| item.row.entity_type.as_str())
            .eq(["PermissionAuthority", "Server", "StudioProject"])
    );
    assert_eq!(
        rows.last().and_then(|item| item.display_parent.as_deref()),
        Some("StudioProject")
    );
    assert_eq!(
        rows.last().map(|item| item.ancestors.as_slice()),
        Some(["StudioProject".to_string()].as_slice())
    );
}

#[test]
fn multiparent_entities_have_one_row_and_keep_cross_parent_relationships() {
    let mut input = vec![
        row("ZetaRoot", &[]),
        row("Shared", &["ZetaRoot", "AlphaRoot"]),
        row("BetaChild", &["AlphaRoot"]),
        row("AlphaRoot", &[]),
    ];
    let rows = permission_tree(&input);
    assert_eq!(
        rows.iter()
            .map(|item| item.row.entity_type.as_str())
            .collect::<Vec<_>>(),
        ["AlphaRoot", "BetaChild", "Shared", "ZetaRoot"]
    );
    let shared = rows.iter().find(|item| item.row.entity_type == "Shared");
    assert_eq!(
        shared.and_then(|item| item.display_parent.as_deref()),
        Some("AlphaRoot")
    );
    assert_eq!(
        shared.map(|item| item.additional_parents.clone()),
        Some(vec!["ZetaRoot".to_string()])
    );
    assert_eq!(
        shared.map(|item| item.row.parents.clone()),
        Some(vec!["ZetaRoot".to_string(), "AlphaRoot".to_string()])
    );
    assert!(
        rows.iter()
            .find(|item| item.row.entity_type == "ZetaRoot")
            .is_some_and(|item| !item.has_children)
    );
    input.reverse();
    assert_eq!(permission_tree(&input), rows);
}

#[test]
fn cycles_self_cycles_and_missing_parents_keep_every_entity_once() {
    let input = vec![
        row("CycleB", &["CycleA"]),
        row("Self", &["Self"]),
        row("Orphan", &["Missing"]),
        row("CycleA", &["CycleB"]),
    ];
    let rows = permission_tree(&input);
    let names: BTreeSet<_> = rows
        .iter()
        .map(|item| item.row.entity_type.as_str())
        .collect();
    assert_eq!(rows.len(), input.len());
    assert_eq!(
        names,
        BTreeSet::from(["CycleA", "CycleB", "Orphan", "Self"])
    );
    for item in &rows {
        assert!(!item.ancestors.contains(&item.row.entity_type));
        assert_eq!(item.depth, item.ancestors.len());
        assert_eq!(
            item.row.parents,
            input
                .iter()
                .find(|source| source.entity_type == item.row.entity_type)
                .map_or_else(Vec::new, |source| source.parents.clone())
        );
    }
    assert!(
        rows.iter()
            .find(|item| item.row.entity_type == "Orphan")
            .is_some_and(|item| item.depth == 0 && item.additional_parents == ["Missing"])
    );
}

#[test]
fn default_tree_rows_keep_one_aligned_read_write_delete_cell_per_entity() {
    Owner::new().with(|| {
        let value = structure();
        let role = PermissionRole { id: PermissionRoleId::from("operator"), authority_id: PermissionAuthorityId::from("permissions"), name: "Operators".into(), enabled: true, rules: vec![] };
        let explorer =
            view! { <PermissionExplorer structure=Signal::stored(Some(value.clone()))/> }.to_html();
        assert_eq!(explorer.matches("<td>").count(), 21);
        assert!(matches!((explorer.find("data-entity=\"StudioProject\""), explorer.find("data-entity=\"RemoteCamera\"")), (Some(parent), Some(child)) if parent < child));
        assert!(explorer.contains("data-display-parent=\"StudioProject\""));
        {
            let editor =
                view! { <RoleEditor structure=Signal::stored(Some(value)) role/> }.to_html();
            assert_eq!(editor.matches("<td>").count(), 21);
            assert_eq!(editor.matches("<select").count(), 21);
            assert_eq!(
                editor
                    .matches("aria-label=\"Rule for RemoteCamera:write\"")
                    .count(),
                1
            );
            assert!(matches!((editor.find("data-entity=\"Server\""), editor.find("data-entity=\"Client\"")), (Some(parent), Some(child)) if parent < child));
            assert!(editor.contains("data-depth=\"1\""));
        }
    });
}

#[test]
fn headless_collapse_only_filters_display_rows_and_preserves_the_role_draft() {
    Owner::new().with(|| {
        let editor = use_role_editor(
            Signal::stored(Some(structure())),
            Signal::stored(PermissionRoleId::from("operator")),
            Some(Callback::new(|_| {})),
        );
        assert_eq!(
            editor.set_rule("RemoteCamera:write", RoleRuleChoice::Deny),
            Ok(())
        );
        let tree = permission_tree(&editor.rows());
        let collapsed = BTreeSet::from(["StudioProject".to_string()]);
        let visible = visible_permission_tree(&tree, &collapsed);
        assert_eq!(visible.len(), 6);
        assert!(
            visible
                .iter()
                .all(|item| item.row.entity_type != "RemoteCamera")
        );
        assert_eq!(
            editor.rule("RemoteCamera:write"),
            Some(RoleRuleChoice::Deny)
        );
        assert_eq!(visible_permission_tree(&tree, &BTreeSet::new()).len(), 7);
    });
}
