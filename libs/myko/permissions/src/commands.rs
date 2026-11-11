use crate::model::{
    PermissionChange, PermissionChangeResult, PermissionGroup, PermissionGroupId,
    PermissionMembership, PermissionMembershipId, PermissionOverride, PermissionOverrideId,
    PermissionPrincipal, PermissionPrincipalId, PermissionRole, PermissionRoleBinding,
    PermissionRoleBindingId, PermissionRoleId,
};
use myko::command::CommandHandler;
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
use myko::command::{CommandContext, CommandError};

#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
use crate::runtime::{PermissionEdit, Permissions};
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
use myko::core::capability::ServerScoped;

#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub struct PermissionAdminOperation {
    pub operation: &'static str,
}
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
inventory::collect!(PermissionAdminOperation);

#[myko::myko_command(PermissionChangeResult)]
#[myko_scope(crate::model::PermissionAuthority, write)]
#[myko_scope(crate::model::PermissionAuthority, delete)]
pub struct ApplyPermissionChanges {
    pub expected_revision: u64,
    pub changes: Vec<PermissionChange>,
    pub reason: String,
}
impl CommandHandler for ApplyPermissionChanges {
    #[cfg(all(feature = "server", not(target_arch = "wasm32")))]
    fn execute(self, ctx: CommandContext) -> Result<PermissionChangeResult, CommandError> {
        let arguments =
            myko::serde_json::to_value(&self).map_err(|error| command_error(&ctx, error))?;
        let runtime = Permissions::for_context(ctx.__server_ctx())
            .map_err(|error| command_error(&ctx, error))?;
        runtime
            .apply(
                &ctx.req,
                &ctx.command_id,
                &arguments,
                PermissionEdit {
                    expected_revision: self.expected_revision,
                    changes: self.changes,
                    reason: self.reason,
                },
            )
            .map_err(|error| command_error(&ctx, error))
    }
}
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
inventory::submit! { PermissionAdminOperation { operation: stringify!(ApplyPermissionChanges) } }

macro_rules! permission_command {
    ($name:ident, $entity:ident, $action:ident, $payload:ident: $ty:ty, $variant:ident) => {
        #[myko::myko_command(PermissionChangeResult)]
        #[myko_scope($entity, $action)]
        pub struct $name {
            pub expected_revision: u64,
            pub $payload: $ty,
            pub reason: String,
        }
        impl CommandHandler for $name {
            #[cfg(all(feature = "server", not(target_arch = "wasm32")))]
            fn execute(self, ctx: CommandContext) -> Result<PermissionChangeResult, CommandError> {
                let arguments = myko::serde_json::to_value(&self)
                    .map_err(|error| command_error(&ctx, error))?;
                let runtime = Permissions::for_context(ctx.__server_ctx())
                    .map_err(|error| command_error(&ctx, error))?;
                runtime
                    .apply(
                        &ctx.req,
                        &ctx.command_id,
                        &arguments,
                        PermissionEdit {
                            expected_revision: self.expected_revision,
                            changes: vec![PermissionChange::$variant(self.$payload)],
                            reason: self.reason,
                        },
                    )
                    .map_err(|error| command_error(&ctx, error))
            }
        }
        #[cfg(all(feature = "server", not(target_arch = "wasm32")))]
        inventory::submit! { PermissionAdminOperation { operation: stringify!($name) } }
    };
}
permission_command!(UpsertPermissionPrincipal, PermissionPrincipal, write, principal: PermissionPrincipal, UpsertPrincipal);
permission_command!(RemovePermissionPrincipal, PermissionPrincipal, delete, id: PermissionPrincipalId, DeletePrincipal);
permission_command!(UpsertPermissionGroup, PermissionGroup, write, group: PermissionGroup, UpsertGroup);
permission_command!(RemovePermissionGroup, PermissionGroup, delete, id: PermissionGroupId, DeleteGroup);
permission_command!(UpsertPermissionMembership, PermissionMembership, write, membership: PermissionMembership, UpsertMembership);
permission_command!(RemovePermissionMembership, PermissionMembership, delete, id: PermissionMembershipId, DeleteMembership);
permission_command!(UpsertPermissionRole, PermissionRole, write, role: PermissionRole, UpsertRole);
permission_command!(RemovePermissionRole, PermissionRole, delete, id: PermissionRoleId, DeleteRole);
permission_command!(UpsertPermissionRoleBinding, PermissionRoleBinding, write, binding: PermissionRoleBinding, UpsertRoleBinding);
permission_command!(RemovePermissionRoleBinding, PermissionRoleBinding, delete, id: PermissionRoleBindingId, DeleteRoleBinding);
permission_command!(UpsertPermissionOverride, PermissionOverride, write, permission_override: PermissionOverride, UpsertOverride);
permission_command!(RemovePermissionOverride, PermissionOverride, delete, id: PermissionOverrideId, DeleteOverride);

#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
fn command_error(ctx: &CommandContext, error: impl std::fmt::Display) -> CommandError {
    CommandError::new(
        ctx.req.tx.to_string(),
        ctx.command_id.to_string(),
        error.to_string(),
    )
}
