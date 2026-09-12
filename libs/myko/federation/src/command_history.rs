use thiserror::Error;

use crate::{
    BatchId, CommandId, CommandSnapshot, CommandState, EventId, NodeEvent, NodeId,
    RetainedHistoryStatement, SelectedHistoryManifest, StorageIncarnationId,
};

/// Frozen history containing one exact command commit.
///
/// Framework evidence consumers derive their expected statement here before
/// authenticating a holder's assertion. Construction does not verify authority,
/// the obligation, persistence, holder eligibility, or continuing custody.
/// This is not a replication milestone or a serializable certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandHistoryTarget {
    command_id: CommandId,
    batch_id: BatchId,
    committed_at: EventId,
    obligation: EventId,
    manifest: SelectedHistoryManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CommandHistoryTargetError {
    #[error("command {0} has not committed")]
    NotCommitted(CommandId),
    #[error("command history does not contain commit {0:?}")]
    MissingCommit(EventId),
    #[error("command {0} does not match its immutable commit")]
    CommitMismatch(CommandId),
}

impl CommandHistoryTarget {
    /// Bind an independently obtained command snapshot to its retained history.
    ///
    /// The manifest must come from the intended history selection, not from the
    /// assertion being checked. Later lifecycle labels do not prove replication;
    /// the original commit and its exact result must still exist in the manifest.
    ///
    /// # Errors
    ///
    /// Rejects uncommitted commands, absent commit events, and inconsistent
    /// command, batch, origin, or result identities.
    pub fn from_committed(
        snapshot: &CommandSnapshot,
        manifest: SelectedHistoryManifest,
        obligation: EventId,
    ) -> Result<Self, CommandHistoryTargetError> {
        let (CommandState::CommittedLocally {
            batch_id,
            position: committed_at,
        }
        | CommandState::Replicating {
            batch_id,
            position: committed_at,
        }
        | CommandState::ReplicationDelayed {
            batch_id,
            position: committed_at,
            ..
        }
        | CommandState::Replicated {
            batch_id,
            position: committed_at,
            ..
        }
        | CommandState::Reconciled {
            batch_id,
            position: committed_at,
            ..
        }) = snapshot.state
        else {
            return Err(CommandHistoryTargetError::NotCommitted(snapshot.request.id));
        };
        let envelope = manifest
            .events()
            .iter()
            .find(|event| event.origin == committed_at)
            .ok_or(CommandHistoryTargetError::MissingCommit(committed_at))?;
        let NodeEvent::CommandCommitted { command, batch } = &envelope.event else {
            return Err(CommandHistoryTargetError::CommitMismatch(
                snapshot.request.id,
            ));
        };
        if command.request != snapshot.request
            || command.result.is_none()
            || command.result != snapshot.result
            || command.updated_at != committed_at
            || command.state
                != (CommandState::CommittedLocally {
                    batch_id,
                    position: committed_at,
                })
            || batch.id != batch_id
            || batch.command_id != snapshot.request.id
            || batch.service_id != snapshot.request.service_id
            || batch.scope_id != snapshot.request.scope_id
        {
            return Err(CommandHistoryTargetError::CommitMismatch(
                snapshot.request.id,
            ));
        }
        Ok(Self {
            command_id: snapshot.request.id,
            batch_id,
            committed_at,
            obligation,
            manifest,
        })
    }

    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    #[must_use]
    pub const fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    #[must_use]
    pub const fn committed_at(&self) -> EventId {
        self.committed_at
    }

    #[must_use]
    pub const fn obligation(&self) -> EventId {
        self.obligation
    }

    #[must_use]
    pub const fn manifest(&self) -> &SelectedHistoryManifest {
        &self.manifest
    }

    /// Derive the exact assertion expected from an independently trusted holder.
    ///
    /// Neither matching this value nor signing it establishes durable retention.
    /// The holder must verify and persist history before issuing its assertion;
    /// the consumer must authenticate it against trusted identity and policy.
    ///
    /// # Errors
    ///
    /// Returns an error if the retained-history commitment cannot be encoded.
    pub fn expected_statement(
        &self,
        holder: NodeId,
        incarnation: StorageIncarnationId,
    ) -> Result<RetainedHistoryStatement, serde_json::Error> {
        RetainedHistoryStatement::new(holder, incarnation, self.obligation, &self.manifest)
    }
}
