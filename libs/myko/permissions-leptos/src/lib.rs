//! Permission components driven by a server's versioned discovery contract.
mod components;
mod connection;
mod editing;

pub use components::{
    GroupMembershipEditor, PermissionExplorer, PermissionMutationStatus, PrincipalAccessInspector,
    RoleEditor,
};
pub use connection::{PermissionConnection, PermissionMutationState, use_permissions};
pub use editing::{MembershipEdit, RoleEdit, RoleRuleChoice, set_role_rule};
pub use myko_permissions;
