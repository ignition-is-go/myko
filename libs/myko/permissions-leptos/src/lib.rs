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
pub use matrix::{PermissionMatrixRow, inspection_matrix, permission_matrix};
