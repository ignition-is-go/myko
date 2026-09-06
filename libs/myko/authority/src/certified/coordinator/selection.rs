use myko_federation::control_quorum::{ControlBallot, ControlHead};

use super::{AuthorityDecisionCoordinator, AuthorityHistory, runtime::next_counter};
use crate::certified::AuthoritySelection;

impl AuthorityDecisionCoordinator {
    /// Certify an exact selection of retained authority records through the controllers.
    ///
    /// Retrying the same selection recovers its original chosen head, even after
    /// later choices. This does not rewrite the records or establish live permission.
    ///
    /// # Errors
    /// Rejects another realm, reused operation identities with different records,
    /// missing or invalid authority history, unavailable quorum, and exhausted retries.
    pub async fn certify_selection(
        &self,
        selection: &AuthoritySelection,
    ) -> Result<ControlHead, String> {
        if selection.realm_id() != self.anchor.realm_id() {
            return Err("authority selection belongs to another realm".to_owned());
        }
        let value = selection.control_value()?;
        let _turn = self.proposal_turn.lock().await;
        for _ in 0..self.max_rounds {
            self.synchronize().await?;
            let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
            let head = history.retained_head()?;
            if let Some(chosen) = history.selection_head_at(head, selection)? {
                return Ok(chosen);
            }
            history.validate_transition_at(head, &value)?;
            let ballot = ControlBallot {
                counter: next_counter(&history, head)?,
                proposer: self.proposer.controller,
            };
            self.choose_value(&history, head, ballot, value.clone())
                .await?;
            let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
            if let Some(chosen) = history.selection_head_at(history.retained_head()?, selection)? {
                return Ok(chosen);
            }
        }
        Err("authority selection did not converge before the retry limit".to_owned())
    }
}
