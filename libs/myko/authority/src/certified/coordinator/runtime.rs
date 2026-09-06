use std::{sync::Arc, time::Duration};

use myko_federation::{
    AuthorizationDecision, CommandId, CommandSnapshot, CommandState, EventSubscription,
    FrameworkControlEvent, MykoService as _, NodeEvent, ServiceId,
    control_quorum::{ControlBallot, ControlHead},
};

use super::{AuthorityDecisionCoordinator, AuthorityHistory, AuthorityRequestSource};
use crate::{AuthorityService, authority_realm_scope};

mod challenge;
mod lifecycle;
mod policy;
pub use lifecycle::PreparedAuthorityGuard;
pub use policy::CertifiedRuntimePolicy;

/// Local authority publication and prepared-effect execution with certified scoped reads.
/// Other admission and read operations retain their explicitly supplied policy.
pub struct PreparedAuthorityRuntime {
    coordinator: Arc<AuthorityDecisionCoordinator>,
    wake: flume::Receiver<()>,
}

impl PreparedAuthorityRuntime {
    /// Pair the worker with the policy installed on the application node.
    /// The channel carries only wakeups. The journal owns all pending work.
    #[must_use]
    pub fn new(
        coordinator: AuthorityDecisionCoordinator,
        non_effect_policy: Arc<dyn myko_federation::AccessPolicy>,
    ) -> (Self, Arc<CertifiedRuntimePolicy>) {
        let (notify, wake) = flume::bounded(1);
        let coordinator = Arc::new(coordinator);
        (
            Self {
                coordinator: coordinator.clone(),
                wake,
            },
            Arc::new(CertifiedRuntimePolicy::new(
                non_effect_policy,
                notify,
                coordinator,
            )),
        )
    }

    /// Publish locally accepted authority on startup and after administration commits.
    /// Resume prepared work on wakeup and every five seconds while publication or
    /// effects remain pending. Parked approvals are not retried by application dispatch.
    /// Publication failures postpone effect release. Individual effect failures
    /// leave that effect prepared without blocking the next command.
    /// History transports must become ready independently of this worker.
    /// Dropping the policy closes the worker; cancelling it closes the
    /// policy's wake channel, so subsequent effect release remains unavailable.
    pub async fn run(self, mut report: impl FnMut(Result<CommandSnapshot, String>)) {
        let mut authority_changes = match self.coordinator.observer.subscribe_from_now() {
            Ok(events) => events,
            Err(error) => {
                report(Err(format!("authority publication watch failed: {error}")));
                return;
            }
        };
        loop {
            let pending = match self.coordinator.certify_local_authority().await {
                Ok(_) => self.resolve_pending(&mut report).await,
                Err(error) => {
                    report(Err(format!("authority publication failed: {error}")));
                    true
                }
            };
            tokio::select! {
                wake = self.wake.recv_async() => if wake.is_err() { return; },
                change = self.next_authority_commit(&mut authority_changes) => {
                    if let Err(error) = change {
                        report(Err(format!("authority publication watch failed: {error}")));
                        return;
                    }
                },
                () = tokio::time::sleep(Duration::from_secs(5)), if pending => {},
            }
        }
    }

    async fn next_authority_commit(&self, events: &mut EventSubscription) -> Result<(), String> {
        let scope = authority_realm_scope(self.coordinator.anchor.realm_id());
        let service = ServiceId::new(AuthorityService::SERVICE_ID);
        loop {
            let event = events
                .recv_async()
                .await
                .map_err(|error| error.to_string())?;
            if event.origin.node_id == self.coordinator.observer.node_id()
                && let NodeEvent::CommandCommitted { command, .. } = event.event
                && command.request.service_id == service
                && command.request.scope_id == scope
            {
                return Ok(());
            }
        }
    }

    async fn resolve_pending(
        &self,
        report: &mut impl FnMut(Result<CommandSnapshot, String>),
    ) -> bool {
        let commands = match self
            .coordinator
            .observer
            .pending_local_authorization_commands()
        {
            Ok(commands) => commands,
            Err(error) => {
                report(Err(error.to_string()));
                return true;
            }
        };
        let mut pending = false;
        for command in commands {
            let id = command.request.id;
            let result = self
                .coordinator
                .release_prepared(id)
                .await
                .map_err(|error| format!("command {id}: {error}"));
            pending |= result.as_ref().map_or(true, |current| {
                matches!(
                    current.state,
                    CommandState::AuthorizationPrepared { .. }
                        | CommandState::AuthorizationPending { .. }
                )
            });
            if !result.as_ref().is_ok_and(|current| current == &command) {
                report(result);
            }
        }
        pending
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
        let history = self.history_for_exact_snapshot()?;
        let head = history.retained_head()?;
        let root = request.root(self.anchor.realm_id(), command_id)?;
        let original = if let Some(original) = history.decision_at(head, &root)? {
            let mut expected = request.request().clone();
            expected.topology = Some(request.topology().clone());
            if !original.matches_retained_request(expected) {
                return Err(
                    "retained authority decision differs from the prepared effect".to_owned(),
                );
            }
            if matches!(original.decision(), AuthorizationDecision::Challenge { .. })
                && self
                    .observer
                    .command(command_id)
                    .map_err(|error| error.to_string())?
                    .is_some_and(|command| {
                        matches!(command.state, CommandState::AuthorizationPending { .. })
                    })
            {
                self.continue_available_prepared(command_id)
                    .await?
                    .map_or_else(
                        || original.decision().clone(),
                        |continued| continued.decision().clone(),
                    )
            } else {
                original.decision().clone()
            }
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
            let history = self.history_for_exact_snapshot()?;
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
            AuthorizationDecision::Challenge { challenge, .. } => {
                return self.park_prepared_challenge(command_id, &digest, challenge.id);
            }
            AuthorizationDecision::Deny(denial) => self.observer.reject(
                command_id,
                AuthorizationDecision::Deny(denial).public_message(),
            ),
        }
        .map_err(|error| error.to_string())
    }
}

pub(super) fn next_counter(history: &AuthorityHistory, head: ControlHead) -> Result<u64, String> {
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
