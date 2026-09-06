use std::sync::Arc;

use myko_federation::{
    AuthorizationDecision, CommandId, CommandSnapshot, CommandState, FrameworkControlEvent,
    NodeEvent,
    control_quorum::{ControlBallot, ControlHead},
};

use super::{AuthorityDecisionCoordinator, AuthorityHistory, AuthorityRequestSource};

mod policy;
pub use policy::PreparedEffectPolicy;

/// Async prepared-effect execution. Admission and reads retain their explicitly
/// supplied policy; this is not a replacement for certified read authorization.
pub struct PreparedAuthorityRuntime {
    coordinator: AuthorityDecisionCoordinator,
    wake: flume::Receiver<()>,
}

impl PreparedAuthorityRuntime {
    /// Pair the worker with the policy installed on the application node.
    /// The channel carries only wakeups. The journal owns all pending work.
    #[must_use]
    pub fn new(
        coordinator: AuthorityDecisionCoordinator,
        non_effect_policy: Arc<dyn myko_federation::AccessPolicy>,
    ) -> (Self, Arc<PreparedEffectPolicy>) {
        let (notify, wake) = flume::bounded(1);
        (
            Self { coordinator, wake },
            Arc::new(PreparedEffectPolicy::new(non_effect_policy, notify)),
        )
    }

    /// Resume retained work on startup and whenever application dispatch retries.
    /// Failures leave effects prepared and are reported without blocking the next
    /// command. Dropping the policy closes the worker; cancelling it closes the
    /// policy's wake channel, so subsequent access remains unavailable.
    pub async fn run(self, mut report: impl FnMut(Result<CommandSnapshot, String>)) {
        loop {
            self.resolve_pending(&mut report).await;
            if self.wake.recv_async().await.is_err() {
                return;
            }
        }
    }

    async fn resolve_pending(&self, report: &mut impl FnMut(Result<CommandSnapshot, String>)) {
        let commands = match self
            .coordinator
            .observer
            .pending_local_application_commands()
        {
            Ok(commands) => commands,
            Err(error) => {
                report(Err(error.to_string()));
                return;
            }
        };
        for command in commands {
            if matches!(command.state, CommandState::AuthorizationPrepared { .. }) {
                let id = command.request.id;
                report(
                    self.coordinator
                        .release_prepared(id)
                        .await
                        .map_err(|error| format!("command {id}: {error}")),
                );
            }
        }
    }
}

impl AuthorityDecisionCoordinator {
    /// Certify or recover consumption, freshly revalidate, then immediately
    /// release the exact saved effect. No permit crosses this async boundary.
    ///
    /// # Errors
    /// Unavailable quorum, invalid history, changed effects, and persistence
    /// failures leave the command uncommitted. Foreign commands cannot execute.
    pub async fn release_prepared(&self, command_id: CommandId) -> Result<CommandSnapshot, String> {
        if self
            .observer
            .command_origin(command_id)
            .map_err(|error| error.to_string())?
            != Some(self.observer.node_id())
        {
            return Err("prepared authority runtime cannot execute a foreign command".to_owned());
        }
        let request = AuthorityRequestSource::new(self.observer.clone())
            .prepared_command_request(command_id)?;
        let digest = request
            .request()
            .effect_digest
            .clone()
            .ok_or_else(|| "prepared effect digest is missing".to_owned())?;
        self.synchronize().await?;
        let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
        let head = history.retained_head()?;
        let root = request.root(self.anchor.realm_id(), command_id)?;
        let original = if let Some(original) = history.decision_at(head, &root)? {
            let mut expected = request.request().clone();
            expected.topology = Some(request.topology().clone());
            if !original.matches_prepared_request(expected) {
                return Err(
                    "retained authority decision differs from the prepared effect".to_owned(),
                );
            }
            original.decision().clone()
        } else {
            self.decide(
                head,
                next_counter(&history, head)?,
                CommandId::new(),
                command_id,
                request.clone(),
            )
            .await?
            .decision()
            .clone()
        };
        let decision = if original.is_permit() {
            let history = AuthorityHistory::replay(&self.observer, self.anchor.clone())?;
            let head = history.retained_head()?;
            self.revalidate(head, next_counter(&history, head)?, command_id, request)
                .await?
                .into_decision()
                .map_err(|error| error.to_string())?
        } else {
            original
        };
        // No await, queue, or policy retry may separate this check from release.
        match decision {
            AuthorizationDecision::Permit(_) => self
                .observer
                .commit_prepared_authorization(command_id, &digest),
            AuthorizationDecision::Challenge { challenge, .. } => self
                .observer
                .await_prepared_authorization(command_id, &digest, challenge.id),
            AuthorizationDecision::Deny(denial) => self.observer.reject(
                command_id,
                AuthorizationDecision::Deny(denial).public_message(),
            ),
        }
        .map_err(|error| error.to_string())
    }
}

fn next_counter(history: &AuthorityHistory, head: ControlHead) -> Result<u64, String> {
    let context = history.context_at(head)?;
    let verifier = context.verifier()?;
    let maximum = history
        .history()
        .iter()
        .filter_map(|event| {
            let NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(vote)) =
                &event.event
            else {
                return None;
            };
            let member = ControlBallot {
                counter: vote.message.ballot.counter,
                proposer: vote.message.controller,
            };
            (vote.message.slot == *context.slot()
                && vote.verify_signature().is_ok()
                && verifier.prepare_request(member).is_ok())
            .then_some(vote.message.ballot.counter)
        })
        .max()
        .unwrap_or(0);
    maximum
        .checked_add(1)
        .ok_or_else(|| "authority ballot counter exhausted".to_owned())
}
