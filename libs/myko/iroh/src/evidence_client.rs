use myko::server::{RetainedEvidenceError, RetainedEvidenceFuture, ScopedRetainedEvidenceEndpoint};
use myko_federation::{AuthorityUnavailable, Node, NodeError, ScopeId};

use crate::{EndpointAddr, IrohReplicationError, IrohReplicator};

/// Refreshes exact scopes from one authenticated peer into this local node.
/// Omits server sessions and router handles to avoid an ownership cycle when a
/// controller retains this adapter.
#[derive(Debug, Clone)]
pub struct IrohScopedEvidenceEndpoint {
    node: Node,
    endpoint: iroh::Endpoint,
    remote: EndpointAddr,
}

impl IrohScopedEvidenceEndpoint {
    #[must_use]
    pub fn new(local: IrohReplicator, remote: EndpointAddr) -> Self {
        Self {
            endpoint: local.router.endpoint().clone(),
            node: local.node,
            remote,
        }
    }
}

impl ScopedRetainedEvidenceEndpoint for IrohScopedEvidenceEndpoint {
    fn refresh_scopes<'a>(&'a self, scopes: &'a [ScopeId]) -> RetainedEvidenceFuture<'a> {
        Box::pin(async move {
            for scope in scopes {
                IrohReplicator::pull_scope_on(
                    &self.node,
                    &self.endpoint,
                    self.remote.clone(),
                    scope.clone(),
                    None,
                )
                .await
                .map_err(|error| evidence_error(&error))?;
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
