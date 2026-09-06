use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::{ChallengeId, LeaseId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct EvaluationSeed([u8; 32]);

impl EvaluationSeed {
    pub(super) const fn from_bytes(seed: [u8; 32]) -> Self {
        Self(seed)
    }

    pub(super) const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    pub(super) fn random() -> Self {
        Self::from_uuid(Uuid::new_v4())
    }

    fn from_uuid(uuid: Uuid) -> Self {
        let mut bytes = b"myko/authority-evaluation-seed/v1\0".to_vec();
        bytes.extend_from_slice(uuid.as_bytes());
        Self(Sha256::digest(bytes).into())
    }

    pub(super) fn challenge_id(self, obligation_id: &myko_federation::ObligationId) -> ChallengeId {
        ChallengeId::new(derived_id(
            b"myko/authority-challenge-id/v1\0",
            &self.0,
            obligation_id.as_str(),
        ))
    }

    pub(super) fn lease_id(self) -> LeaseId {
        LeaseId::new(derived_id(b"myko/authority-lease-id/v1\0", &self.0, ""))
    }
}

fn derived_id(domain: &[u8], seed: &[u8; 32], suffix: &str) -> String {
    let mut bytes = domain.to_vec();
    bytes.extend_from_slice(seed);
    bytes.extend_from_slice(suffix.as_bytes());
    format!("deterministic:{:x}", Sha256::digest(bytes))
}
