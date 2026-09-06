use myko_federation::{AuthorizationPhase, ChallengeId, CommandId, CommandSnapshot, CommandState};

use super::{AuthorityDecisionCoordinator, AuthorityHistory};
use crate::certified::AuthorityDecisionRoot;

impl AuthorityDecisionCoordinator {
    pub(super) fn park_prepared_challenge(
        &self,
        command_id: CommandId,
        digest: &str,
        target: ChallengeId,
    ) -> Result<CommandSnapshot, String> {
        let command = self
            .observer
            .command(command_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "prepared command is missing".to_owned())?;
        let previous = match command.state {
            CommandState::AuthorizationPending { challenge_id, .. } if challenge_id != target => {
                challenge_id
            }
            _ => {
                return self
                    .observer
                    .await_prepared_authorization(command_id, digest, target)
                    .map_err(|error| error.to_string());
            }
        };
        let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
        let head = history.retained_head()?;
        let root = AuthorityDecisionRoot::new(
            self.anchor.realm_id().clone(),
            command_id,
            AuthorizationPhase::Effect,
        )?;
        let mut previous = previous;
        loop {
            let (next, approval) = history
                .next_pending_challenge_at(head, &root, &previous)?
                .ok_or_else(|| {
                    "pending command has no certified challenge advancement".to_owned()
                })?;
            let snapshot = self
                .observer
                .advance_authorization(command_id, &previous, next.id.clone(), approval)
                .map_err(|error| error.to_string())?;
            if next.id == target {
                return Ok(snapshot);
            }
            previous = next.id;
        }
    }
}
