//! Persisted grants, reactive permission entities, and protected administration.
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
mod admin;
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub use admin::{AdminEmail, PermissionAuthenticator};
mod commands;
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
mod engine;
pub mod error;
pub mod inspection;
pub mod model;
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
mod operator;
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
mod runtime;
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub mod storage;
pub use inspection::*;

pub use commands::{
    ApplyPermissionChanges, ApplyPermissionChangesArgs, RemovePermissionGroup,
    RemovePermissionGroupArgs, RemovePermissionMembership, RemovePermissionMembershipArgs,
    RemovePermissionOverride, RemovePermissionOverrideArgs, RemovePermissionPrincipal,
    RemovePermissionPrincipalArgs, RemovePermissionRole, RemovePermissionRoleArgs,
    RemovePermissionRoleBinding, RemovePermissionRoleBindingArgs, UpsertPermissionGroup,
    UpsertPermissionGroupArgs, UpsertPermissionMembership, UpsertPermissionMembershipArgs,
    UpsertPermissionOverride, UpsertPermissionOverrideArgs, UpsertPermissionPrincipal,
    UpsertPermissionPrincipalArgs, UpsertPermissionRole, UpsertPermissionRoleArgs,
    UpsertPermissionRoleBinding, UpsertPermissionRoleBindingArgs,
};
pub use error::{PermissionError, PermissionResult};
pub use model::*;
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub use operator::{OperatorPrincipal, bootstrap, recover, replace_as_operator};
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub use runtime::{CredentialScopeMode, Permissions};
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub use storage::{FilePermissionStorage, MemoryPermissionStorage, PermissionStorage};
