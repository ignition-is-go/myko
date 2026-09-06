use std::sync::Arc;

use myko::{ApplicationHost, CommandDispatchGuard};
use myko_federation::{AccessPolicy, CommandSnapshot};
use tokio::task::JoinHandle;

use super::{AuthorityDecisionCoordinator, PreparedAuthorityRuntime};

/// Owns application dispatch and certified effect recovery together.
/// Drop requests cancellation; `shutdown` also waits until neither can execute.
#[must_use = "retain the guard while the application is serving commands"]
pub struct PreparedAuthorityGuard {
    dispatch: Option<CommandDispatchGuard>,
    worker: Option<JoinHandle<()>>,
}

impl PreparedAuthorityGuard {
    /// Report a terminated dispatcher or authority worker to the supervisor.
    #[must_use]
    pub fn failure(&self) -> Option<String> {
        self.dispatch
            .as_ref()
            .and_then(CommandDispatchGuard::failure)
            .or_else(|| {
                self.worker
                    .as_ref()
                    .filter(|worker| worker.is_finished())
                    .map(|_| "prepared authority worker stopped".to_owned())
            })
    }

    /// Stop command scheduling, cancel in-flight coordination, and join both.
    /// Already persisted votes and effects are not rolled back. Pending commands
    /// recover from the journal when another guard is installed.
    ///
    /// # Errors
    /// Reports a worker panic rather than treating it as normal cancellation.
    pub async fn shutdown(mut self) -> Result<(), String> {
        if let Some(dispatch) = self.dispatch.take() {
            dispatch.shutdown().await;
        }
        if let Some(worker) = self.worker.take() {
            worker.abort();
            match worker.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => return Err(format!("prepared authority worker failed: {error}")),
            }
        }
        Ok(())
    }
}

impl Drop for PreparedAuthorityGuard {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
        drop(self.dispatch.take());
    }
}

impl PreparedAuthorityRuntime {
    /// Install the effect policy and start both command dispatch and recovery.
    /// Retain the returned guard and shut it down before replacing this policy.
    /// Admission, reads, and administration still use `non_effect_policy`.
    ///
    /// # Errors
    /// Rejects a different coordinator node, a missing Tokio runtime, or a
    /// failure to install the policy or start command dispatch.
    pub fn install(
        host: ApplicationHost,
        coordinator: AuthorityDecisionCoordinator,
        non_effect_policy: Arc<dyn AccessPolicy>,
        report: impl FnMut(Result<CommandSnapshot, String>) + Send + 'static,
    ) -> Result<(ApplicationHost, PreparedAuthorityGuard), String> {
        if host.node_id() != coordinator.observer.node_id() {
            return Err("authority coordinator and application must use the same node".to_owned());
        }
        let executor = tokio::runtime::Handle::try_current()
            .map_err(|error| format!("prepared authority requires a Tokio runtime: {error}"))?;
        let (runtime, policy) = Self::new(coordinator, non_effect_policy);
        let host = host
            .with_access_policy(policy)
            .map_err(|error| error.to_string())?;
        let dispatch = host.drive_commands().map_err(|error| error.to_string())?;
        let worker = executor.spawn(runtime.run(report));
        Ok((
            host,
            PreparedAuthorityGuard {
                dispatch: Some(dispatch),
                worker: Some(worker),
            },
        ))
    }
}
