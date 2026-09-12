use std::{collections::HashMap, sync::Arc};

use myko::server::{RetainedEvidenceError, RetainedEvidenceFuture, ScopedRetainedEvidenceEndpoint};
use myko_federation::{
    AuthorityUnavailable, Node, NodeError, ScopeId, ScopedReplicationCheckpoint,
};
use tokio::sync::Mutex;

use crate::{EndpointAddr, IrohReplicationError, IrohReplicator};

type ScopeCheckpoint = Arc<Mutex<Option<ScopedReplicationCheckpoint>>>;

/// Refreshes exact scopes from one authenticated peer into this local node.
///
/// Omits server sessions and router handles to avoid an ownership cycle when a
/// controller retains this adapter.
/// Clones share transient checkpoints and serialize refreshes of the same scope.
#[derive(Debug, Clone)]
pub struct IrohScopedEvidenceEndpoint {
    node: Node,
    endpoint: iroh::Endpoint,
    remote: EndpointAddr,
    request_timeout: std::time::Duration,
    checkpoints: Arc<Mutex<HashMap<ScopeId, ScopeCheckpoint>>>,
}

impl IrohScopedEvidenceEndpoint {
    #[must_use]
    pub fn new(local: IrohReplicator, remote: EndpointAddr) -> Self {
        Self {
            endpoint: local.router.endpoint().clone(),
            node: local.node,
            remote,
            request_timeout: std::time::Duration::from_secs(10),
            checkpoints: Arc::default(),
        }
    }

    /// Bounds one scope refresh, including lock wait, connection setup, and transfer.
    /// Defaults to ten seconds. A timeout leaves already retained history intact.
    #[must_use]
    pub const fn with_request_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

impl ScopedRetainedEvidenceEndpoint for IrohScopedEvidenceEndpoint {
    fn refresh_scopes<'a>(&'a self, scopes: &'a [ScopeId]) -> RetainedEvidenceFuture<'a> {
        Box::pin(async move {
            for scope in scopes {
                let started = std::time::Instant::now();
                let report = tokio::time::timeout(self.request_timeout, async {
                    let checkpoint = self
                        .checkpoints
                        .lock()
                        .await
                        .entry(scope.clone())
                        .or_default()
                        .clone();
                    let mut checkpoint = checkpoint.lock().await;
                    let report = IrohReplicator::pull_scope_on(
                        &self.node,
                        &self.endpoint,
                        self.remote.clone(),
                        scope.clone(),
                        checkpoint.clone(),
                    )
                    .await?;
                    *checkpoint = Some(report.checkpoint());
                    drop(checkpoint);
                    Ok::<_, IrohReplicationError>(report)
                })
                .await
                .map_err(|_| {
                    RetainedEvidenceError::Unavailable(AuthorityUnavailable::HistoryUnavailable)
                })?
                .map_err(|error| evidence_error(&error))?;
                tracing::debug!(
                    scope_id = %scope,
                    source_node = %report.source_node,
                    applied = report.applied,
                    duplicates = report.duplicates,
                    elapsed_ms = started.elapsed().as_millis(),
                    "scoped evidence refreshed"
                );
            }
            Ok(())
        })
    }
}

fn evidence_error(error: &IrohReplicationError) -> RetainedEvidenceError {
    match error {
        IrohReplicationError::AuthorityUnavailable(reason)
        | IrohReplicationError::Ingest(NodeError::AuthorityUnavailable(reason)) => {
            RetainedEvidenceError::Unavailable(*reason)
        }
        IrohReplicationError::Ingest(NodeError::Backend(_)) => {
            RetainedEvidenceError::Unavailable(AuthorityUnavailable::PersistenceUnavailable)
        }
        IrohReplicationError::Endpoint(_)
        | IrohReplicationError::Stream(_)
        | IrohReplicationError::Supervisor(_)
        | IrohReplicationError::Ingest(NodeError::SubscriptionDisconnected) => {
            RetainedEvidenceError::Unavailable(AuthorityUnavailable::HistoryUnavailable)
        }
        IrohReplicationError::Encoding(_)
        | IrohReplicationError::Ingest(_)
        | IrohReplicationError::Cursor(_)
        | IrohReplicationError::Identity(_)
        | IrohReplicationError::Authorization { .. } => {
            RetainedEvidenceError::Invalid(error.to_string())
        }
    }
}
