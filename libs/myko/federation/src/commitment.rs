use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{EventId, NodeEvent, ScopeSelection, SelectedHistoryManifest};

const COMMITMENT_DOMAIN: &str = "myko.retained-history-commitment";
const COMMITMENT_VERSION: u32 = 1;

/// Content commitment for one exact retained-history manifest.
///
/// This value is neither signed nor evidence of durability, currentness, or
/// custody. Version 1 hashes UTF-8 JSON for an object containing `domain`,
/// `version`, `selection`, and origin-sorted immutable event records. Every
/// JSON object is recursively key-sorted with `Value::sort_all_objects` before
/// encoding. Arrays retain their original order. This is a Myko-specific byte
/// contract, not RFC 8785 canonical JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedHistoryCommitment {
    digest: [u8; 32],
    event_count: u64,
}

impl RetainedHistoryCommitment {
    /// SHA-256 digest of the versioned canonical payload.
    #[must_use]
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// Number of immutable events bound by this commitment.
    #[must_use]
    pub const fn event_count(&self) -> u64 {
        self.event_count
    }
}

#[derive(Serialize)]
struct CommitmentPayload<'a> {
    domain: &'static str,
    version: u32,
    selection: &'a ScopeSelection,
    events: Vec<CommittedEvent<'a>>,
}

#[derive(Serialize)]
struct CommittedEvent<'a> {
    origin: EventId,
    recorded_at: &'a chrono::DateTime<chrono::Utc>,
    event: &'a NodeEvent,
}

impl SelectedHistoryManifest {
    /// Commits to this manifest's selection and immutable event content.
    ///
    /// Observer-local event positions and the manifest's local recording cut
    /// are deliberately excluded. Equal event sets therefore commit equally
    /// after import at different positions.
    ///
    /// # Errors
    ///
    /// Returns an error if the canonical JSON payload cannot be encoded.
    pub fn commitment(&self) -> Result<RetainedHistoryCommitment, serde_json::Error> {
        let mut events = self.events().iter().collect::<Vec<_>>();
        events.sort_by_key(|event| {
            (
                event.origin.node_id.as_uuid().as_u128(),
                event.origin.sequence.get(),
            )
        });
        let events = events
            .into_iter()
            .map(|event| CommittedEvent {
                origin: event.origin,
                recorded_at: &event.recorded_at,
                event: &event.event,
            })
            .collect::<Vec<_>>();
        let event_count = u64::try_from(events.len()).map_err(|_| {
            serde_json::Error::io(std::io::Error::other(
                "retained history event count exceeds u64",
            ))
        })?;
        let mut canonical = serde_json::to_value(CommitmentPayload {
            domain: COMMITMENT_DOMAIN,
            version: COMMITMENT_VERSION,
            selection: self.selection(),
            events,
        })?;
        canonical.sort_all_objects();
        let encoded = serde_json::to_vec(&canonical)?;
        Ok(RetainedHistoryCommitment {
            digest: Sha256::digest(encoded).into(),
            event_count,
        })
    }
}
