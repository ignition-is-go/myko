use serde::{Deserialize, Serialize};

use crate::{
    EventId, NodeId, RetainedHistoryCommitment, ScopeSelection, SelectedHistoryManifest,
    StorageIncarnationId,
};

const STATEMENT_DOMAIN: &[u8] = b"myko.retained-history-statement.v1\0";

/// Content and context of a retained-history assertion, not a custody receipt.
///
/// Constructing or signing this value does not verify storage, the referenced
/// obligation, holder eligibility, or current membership. Durable issuance must
/// verify those conditions and persist the record before releasing a receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedHistoryStatement {
    holder: NodeId,
    storage_incarnation: StorageIncarnationId,
    obligation: EventId,
    selection: ScopeSelection,
    commitment: RetainedHistoryCommitment,
}

impl RetainedHistoryStatement {
    /// Bind one manifest to a holder, store incarnation, and control obligation.
    ///
    /// The obligation identifies the custody agreement or handoff record whose
    /// requirements the manifest must satisfy. This constructor does not read or
    /// authorize that record. An incarnation does not detect database rollback.
    ///
    /// # Errors
    ///
    /// Returns an error if immutable history cannot be encoded for commitment.
    pub fn new(
        holder: NodeId,
        storage_incarnation: StorageIncarnationId,
        obligation: EventId,
        manifest: &SelectedHistoryManifest,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            holder,
            storage_incarnation,
            obligation,
            selection: manifest.selection().clone(),
            commitment: manifest.commitment()?,
        })
    }

    #[must_use]
    pub const fn holder(&self) -> NodeId {
        self.holder
    }

    #[must_use]
    pub const fn storage_incarnation(&self) -> StorageIncarnationId {
        self.storage_incarnation
    }

    #[must_use]
    pub const fn obligation(&self) -> EventId {
        self.obligation
    }

    #[must_use]
    pub const fn selection(&self) -> &ScopeSelection {
        &self.selection
    }

    #[must_use]
    pub const fn commitment(&self) -> &RetainedHistoryCommitment {
        &self.commitment
    }

    /// Version-one signing bytes: domain prefix then compact JSON with every
    /// object recursively sorted by Rust string ordering. Arrays retain order.
    /// This is this format's encoding contract, not a claim of RFC 8785 support.
    ///
    /// # Errors
    ///
    /// Returns an error if the statement cannot be represented as JSON.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut value = serde_json::to_value(self)?;
        value.sort_all_objects();
        let mut bytes = STATEMENT_DOMAIN.to_vec();
        bytes.extend(serde_json::to_vec(&value)?);
        Ok(bytes)
    }
}
