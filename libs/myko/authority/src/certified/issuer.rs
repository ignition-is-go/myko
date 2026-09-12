use std::sync::Arc;

use ed25519_dalek::SigningKey;
use myko_federation::{
    Node,
    control_quorum::{
        ControlBallot, ControlHead, ControlValue, SignedControlProposal, SignedControlVote,
    },
};

use super::coordinator::history_cache::AuthorityHistoryCache;
use super::{AuthorityAnchor, AuthorityHistory};

/// Issues durable control evidence from a freshly validated historical context.
/// This does not establish network freshness, remote request authority or custody.
pub struct AuthorityController {
    node: Node,
    anchor: AuthorityAnchor,
    history_cache: AuthorityHistoryCache,
}

impl AuthorityController {
    #[must_use]
    pub fn new(node: Node, anchor: AuthorityAnchor) -> Self {
        let history_cache = AuthorityHistoryCache::new(node.clone(), anchor.clone());
        Self {
            node,
            anchor,
            history_cache,
        }
    }

    pub(super) async fn cached_history(&self) -> Result<Arc<AuthorityHistory>, String> {
        self.history_cache.history_for_exact_snapshot().await
    }

    pub(super) async fn prepare_cached(
        &self,
        head: ControlHead,
        ballot: ControlBallot,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        let history = self.cached_history().await?;
        self.prepare_from_history(&history, head, ballot, key)
    }

    pub(super) async fn propose_cached(
        &self,
        head: ControlHead,
        ballot: ControlBallot,
        promises: &[SignedControlVote],
        value: &ControlValue,
        key: &SigningKey,
    ) -> Result<SignedControlProposal, String> {
        let history = self.cached_history().await?;
        self.propose_from_history(&history, head, ballot, promises, value, key)
    }

    pub(super) async fn accept_cached(
        &self,
        head: ControlHead,
        proposal: &SignedControlProposal,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        let history = self.cached_history().await?;
        self.accept_from_history(&history, head, proposal, key)
    }

    /// Persist a promise using the electorate certified after `head`.
    ///
    /// # Errors
    /// Rejects incomplete authority history, invalid ballots, local history races,
    /// unavailable persistence and durable voter conflicts.
    pub fn prepare(
        &self,
        head: ControlHead,
        ballot: ControlBallot,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        let history = AuthorityHistory::replay(&self.node, self.anchor.clone())?;
        self.prepare_from_history(&history, head, ballot, key)
    }

    fn prepare_from_history(
        &self,
        history: &AuthorityHistory,
        head: ControlHead,
        ballot: ControlBallot,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        let verifier = history.context_at(head)?.verifier()?;
        let request = verifier
            .prepare_request(ballot)
            .map_err(|error| error.to_string())?
            .bind_history(history.history());
        self.node
            .vote_control(&request, key)
            .map_err(|error| error.to_string())
    }

    /// Persist a recovered proposal under the electorate certified after `head`.
    ///
    /// # Errors
    /// Rejects incomplete history, invalid prepare evidence, conflicting proposals,
    /// malformed authority payloads, local history races and persistence failures.
    pub fn propose(
        &self,
        head: ControlHead,
        ballot: ControlBallot,
        promises: &[SignedControlVote],
        value: &ControlValue,
        key: &SigningKey,
    ) -> Result<SignedControlProposal, String> {
        let history = AuthorityHistory::replay(&self.node, self.anchor.clone())?;
        self.propose_from_history(&history, head, ballot, promises, value, key)
    }

    fn propose_from_history(
        &self,
        history: &AuthorityHistory,
        head: ControlHead,
        ballot: ControlBallot,
        promises: &[SignedControlVote],
        value: &ControlValue,
        key: &SigningKey,
    ) -> Result<SignedControlProposal, String> {
        history.validate_transition_at(head, value)?;
        let verifier = history.context_at(head)?.verifier()?;
        let prepared = verifier
            .verify_prepare(ballot, promises)
            .map_err(|error| error.to_string())?;
        let request = prepared
            .proposal_request(value)
            .map_err(|error| error.to_string())?
            .bind_history(history.history());
        self.node
            .propose_control(&request, key)
            .map_err(|error| error.to_string())
    }

    /// Persist an acceptance only under the certified predecessor configuration.
    ///
    /// # Errors
    /// Rejects incomplete history, stale epochs, invalid proposal evidence, local
    /// history races, malformed authority payloads and persistence failures.
    pub fn accept(
        &self,
        head: ControlHead,
        proposal: &SignedControlProposal,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        let history = AuthorityHistory::replay(&self.node, self.anchor.clone())?;
        self.accept_from_history(&history, head, proposal, key)
    }

    fn accept_from_history(
        &self,
        history: &AuthorityHistory,
        head: ControlHead,
        proposal: &SignedControlProposal,
        key: &SigningKey,
    ) -> Result<SignedControlVote, String> {
        history.validate_transition_at(head, &proposal.message.value)?;
        let verifier = history.context_at(head)?.verifier()?;
        let request = verifier
            .accept_request(proposal)
            .map_err(|error| error.to_string())?
            .bind_history(history.history());
        self.node
            .vote_control(&request, key)
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AuthorityRealmKey;
    use myko_federation::control_quorum::{ControlEpochId, ControllerId};

    #[tokio::test]
    async fn cached_controller_history_still_rejects_a_local_log_race()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let node = myko_redb::RedbJournal::open_node(directory.path().join("controller.redb"))?;
        let key = SigningKey::from_bytes(&[61; 32]);
        let proposer = ControllerId(key.verifying_key().to_bytes());
        let head = ControlHead([62; 32]);
        let anchor = AuthorityAnchor::new(
            AuthorityRealmKey::new("controller-cache-race"),
            ControlEpochId([63; 32]),
            head,
            vec![proposer],
        )?;
        let controller = AuthorityController::new(node.clone(), anchor);
        let stale = controller.cached_history().await?;
        controller
            .prepare_cached(
                head,
                ControlBallot {
                    counter: 1,
                    proposer,
                },
                &key,
            )
            .await?;
        let retained = node.events_after(None)?;
        if controller
            .prepare_from_history(
                &stale,
                head,
                ControlBallot {
                    counter: 2,
                    proposer,
                },
                &key,
            )
            .is_ok()
        {
            return Err("stale cached history issued a durable vote".into());
        }
        if node.events_after(None)? != retained {
            return Err("rejected stale history changed the durable log".into());
        }
        controller
            .prepare_cached(
                head,
                ControlBallot {
                    counter: 2,
                    proposer,
                },
                &key,
            )
            .await?;
        if node.events_after(None)? == retained {
            return Err("refreshed history did not issue the next vote".into());
        }
        Ok(())
    }
}
