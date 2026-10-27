use crate::command::ClientCommandRegistration;

/// Mutation surface exposed by a server's client transports.
///
/// This is not authentication or per-user authorization. Commands permitted here
/// still need to validate their arguments and application permissions.
#[derive(Clone, Copy, Debug, Default)]
pub enum ClientMutationPolicy {
    /// Preserve the trusted-peer protocol: all commands and raw events are accepted.
    #[default]
    Trusted,
    /// Accept only `#[myko_command(..., client)]` commands and reject raw events.
    /// With no client-marked commands, client transports are read-only.
    DeclaredCommands,
}

impl ClientMutationPolicy {
    #[must_use]
    pub fn allows_command(&self, command_id: &str) -> bool {
        match self {
            Self::Trusted => true,
            Self::DeclaredCommands => inventory::iter::<ClientCommandRegistration>
                .into_iter()
                .any(|registration| registration.command_id == command_id),
        }
    }

    #[must_use]
    pub const fn allows_events(&self) -> bool {
        matches!(self, Self::Trusted)
    }
}
