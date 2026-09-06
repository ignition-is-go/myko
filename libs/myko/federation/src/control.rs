use serde::{Deserialize, Serialize};

use crate::{
    EventId, ScopeId, ScopeSelection, SignedRetainedHistoryStatement,
    control_quorum::{SignedControlProposal, SignedControlVote},
};

/// Framework history that is never executable application work.
///
/// Retaining a statement does not validate its signature or establish custody,
/// membership, scope existence, or permission to serve the referenced history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "record",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum FrameworkControlEvent {
    RetainedHistoryStatement(SignedRetainedHistoryStatement),
    ControlVote(SignedControlVote),
    ControlProposal(SignedControlProposal),
}

impl FrameworkControlEvent {
    /// Full selection whose retained history the record describes.
    #[must_use]
    pub fn selection(&self) -> ScopeSelection {
        match self {
            Self::RetainedHistoryStatement(signed) => signed.statement().selection().clone(),
            Self::ControlVote(signed) => ScopeSelection::Exact(signed.message.slot.realm.clone()),
            Self::ControlProposal(signed) => {
                ScopeSelection::Exact(signed.message.slot.realm.clone())
            }
        }
    }

    #[must_use]
    pub const fn scope_id(&self) -> &ScopeId {
        match self {
            Self::RetainedHistoryStatement(signed) => signed.statement().selection().root(),
            Self::ControlVote(signed) => &signed.message.slot.realm,
            Self::ControlProposal(signed) => &signed.message.slot.realm,
        }
    }

    /// Statements wait for their obligation. Votes have no application dependency.
    /// These dependencies alone do not establish coverage or authorize control.
    #[must_use]
    pub fn causal_dependencies(&self) -> Vec<EventId> {
        match self {
            Self::RetainedHistoryStatement(signed) => vec![signed.statement().obligation()],
            Self::ControlVote(_) | Self::ControlProposal(_) => Vec::new(),
        }
    }
}
