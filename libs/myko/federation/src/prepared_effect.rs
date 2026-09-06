use super::*;

/// Durable, immutable command effect body prepared before effect authorization.
///
/// The public constructor derives the digest from the full body. Backends
/// validate decoded history before exposing prepared state because serde can
/// still decode old or corrupt bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCommandEffect {
    command_updated_at: EventId,
    batch: Box<ChangeBatch>,
    result: Vec<u8>,
    resource_claims: Vec<ResourceClaim>,
    application_capabilities: Vec<CapabilityId>,
    topology_proof: ScopeTopology,
    effect_digest: String,
}

impl PreparedCommandEffect {
    /// Freezes one exact handler result and mutation batch for authorization.
    ///
    /// # Errors
    ///
    /// Returns an error if the prepared body cannot be canonically encoded.
    pub fn new(
        command_updated_at: EventId,
        batch: ChangeBatch,
        result: Vec<u8>,
        resource_claims: Vec<ResourceClaim>,
        application_capabilities: Vec<CapabilityId>,
        topology_proof: ScopeTopology,
    ) -> Result<Self, NodeError> {
        let effect_digest = Self::digest_for(
            command_updated_at,
            &batch,
            &result,
            &resource_claims,
            &application_capabilities,
            &topology_proof,
        )?;
        Ok(Self {
            command_updated_at,
            batch: Box::new(batch),
            result,
            resource_claims,
            application_capabilities,
            topology_proof,
            effect_digest,
        })
    }

    /// Recomputes the digest and rejects corrupt retained history.
    ///
    /// # Errors
    ///
    /// Returns an error if the body cannot be encoded or the retained digest no
    /// longer matches it.
    pub fn validate_digest(&self) -> Result<(), NodeError> {
        let expected = Self::digest_for(
            self.command_updated_at,
            &self.batch,
            &self.result,
            &self.resource_claims,
            &self.application_capabilities,
            &self.topology_proof,
        )?;
        if self.effect_digest != expected {
            return Err(NodeError::CorruptHistory(format!(
                "prepared command effect digest mismatch: expected {expected}, found {}",
                self.effect_digest
            )));
        }
        Ok(())
    }

    #[must_use]
    pub const fn command_updated_at(&self) -> EventId {
        self.command_updated_at
    }

    #[must_use]
    pub const fn authorization_phase(&self) -> AuthorizationPhase {
        AuthorizationPhase::Effect
    }

    #[must_use]
    pub const fn batch(&self) -> &ChangeBatch {
        &self.batch
    }

    #[must_use]
    pub fn result(&self) -> &[u8] {
        &self.result
    }

    #[must_use]
    pub fn resource_claims(&self) -> &[ResourceClaim] {
        &self.resource_claims
    }

    #[must_use]
    pub fn application_capabilities(&self) -> &[CapabilityId] {
        &self.application_capabilities
    }

    #[must_use]
    pub const fn topology_proof(&self) -> &ScopeTopology {
        &self.topology_proof
    }

    #[must_use]
    pub fn effect_digest(&self) -> &str {
        &self.effect_digest
    }

    pub(super) fn into_batch_result(self) -> (ChangeBatch, Vec<u8>) {
        (*self.batch, self.result)
    }

    fn digest_for(
        command_updated_at: EventId,
        batch: &ChangeBatch,
        result: &[u8],
        resource_claims: &[ResourceClaim],
        application_capabilities: &[CapabilityId],
        topology_proof: &ScopeTopology,
    ) -> Result<String, NodeError> {
        serde_json::to_vec(&(
            "myko-prepared-command-effect-v1",
            command_updated_at,
            AuthorizationPhase::Effect,
            batch,
            result,
            resource_claims,
            application_capabilities,
            topology_proof,
        ))
        .map(|bytes| digest_bytes(&bytes))
        .map_err(|error| NodeError::ResultEncoding(error.to_string()))
    }
}
