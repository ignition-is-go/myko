//! Permission components driven by a server's versioned discovery contract.
mod components;
mod connection;
mod controllers;
mod editing;
mod matrix;

pub use components::{
    GroupMembershipEditor, PermissionExplorer, PermissionMutationStatus, PrincipalAccessInspector,
    RoleEditor,
};
pub use connection::{PermissionConnection, PermissionMutationState, use_permissions};
pub use editing::{MembershipEdit, RoleEdit, RoleRuleChoice, set_role_rule};
pub use myko_permissions;

pub use controllers::{
    GroupMembershipController, MembershipEditorRow, PermissionEditError, PermissionEditorStatus,
    RoleEditorController, use_group_membership_editor, use_role_editor,
};
pub use matrix::{
    PermissionMatrixRow, PermissionTreeRow, inspection_matrix, permission_matrix, permission_tree,
    visible_permission_tree,
};

mod administration;
pub use administration::{
    PermissionReferenceEntry, PermissionUserSummary, PrincipalRoleSummary, RoleAssignmentSummary,
    RoleMemberSummary, assign_role, groups_options, roles_options, use_assignable_roles,
    use_assignable_users, use_grantee_roles, use_permission_changes, use_permission_reference,
    use_permission_roles, use_permission_users, use_role_users, user_roles,
};

pub use administration::{use_role_permission_rows, use_role_rule_values};

pub use administration::{create_group, create_role, permission_explanation_text};

pub use administration::use_permission_groups;

pub use administration::use_permission_list_search;
