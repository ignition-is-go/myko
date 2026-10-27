use std::{collections::HashSet, sync::Arc};

/// Mutation surface exposed by a server's client transports.
///
/// This is not authentication or per-user authorization. Commands permitted here
/// still need to validate their arguments and application permissions.
#[derive(Clone, Debug, Default)]
pub enum ClientMutationPolicy {
    /// Preserve the trusted-peer protocol: all commands and raw events are accepted.
    #[default]
    Trusted,
    /// Only explicitly named commands may mutate state; raw events are rejected.
    CommandsOnly(Arc<HashSet<String>>),
}

impl ClientMutationPolicy {
    /// Expose only these command IDs, rejecting client-origin events and batches.
    /// An empty list makes client transports read-only.
    #[must_use]
    pub fn commands_only(commands: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::CommandsOnly(Arc::new(commands.into_iter().map(Into::into).collect()))
    }

    #[must_use]
    pub fn allows_command(&self, command_id: &str) -> bool {
        match self {
            Self::Trusted => true,
            Self::CommandsOnly(commands) => commands.contains(command_id),
        }
    }

    #[must_use]
    pub const fn allows_events(&self) -> bool {
        matches!(self, Self::Trusted)
    }
}
