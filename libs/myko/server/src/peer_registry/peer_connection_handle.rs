use std::sync::Arc;

use hyphae::{
    Cell, CellImmutable, DedupedExt, JoinExt, MapExt, MaterializeDefinite, MaterializeEmpty,
    PairwiseExt, TapExt,
};
use myko::{
    client::{ConnectionStatus, MykoClient},
    entities::server::{GetConnectedServer, Server},
};
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq)]
enum IdentityCycle {
    Pending,
    Imposter,
    Correct,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PeerConnectionCycle {
    Pending,
    Connected,
    Failed,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) enum PeerState {
    Keep,
    Delete,
}

/// RAII handle for a peer connection.
/// Dropping the handle closes the client.
pub(super) struct PeerConnectionHandle {
    server_ent: Arc<Server>,
    client: Arc<MykoClient>,
    pub(super) signal_state: Cell<PeerState, CellImmutable>,
}

impl PeerConnectionHandle {
    pub(super) fn new(server_ent: Arc<Server>) -> Self {
        let addr = format!("{}:{}", server_ent.address, server_ent.port);

        let client = MykoClient::new_with_auto_reconnect(false);
        client.set_address(Some(addr));

        let status_log_address = server_ent.address.clone();
        let status_log_port = server_ent.port;

        // `pairwise()` is `Empty`-seeded: the first source emission has no
        // prior partner, so the materialized cell starts as `None` and flips
        // to `Some((prev, current))` once the second emission lands.
        let connection_status = client
            .connection_status()
            .pairwise()
            .tap(move |s| {
                tracing::info!(
                    "Connection status updated: {}:{} [{:?} -> {:?}]",
                    status_log_address,
                    status_log_port,
                    s.0,
                    s.1
                )
            })
            .materialize()
            .deduped()
            .map(move |status| match status.as_ref() {
                Some((_, ConnectionStatus::Connected(_))) => PeerConnectionCycle::Connected,
                Some((ConnectionStatus::Connected(_), ConnectionStatus::Disconnected)) => {
                    PeerConnectionCycle::Failed
                }
                Some((ConnectionStatus::Connecting(_), ConnectionStatus::Disconnected)) => {
                    PeerConnectionCycle::Failed
                }
                _ => PeerConnectionCycle::Pending,
            })
            .materialize();

        let identity_server_id = server_ent.id.clone();

        let identity = client
            .watch_query(GetConnectedServer {})
            .map(move |servers| {
                let Some(server) = servers.iter().next() else {
                    return IdentityCycle::Pending;
                };

                if server.id != identity_server_id {
                    return IdentityCycle::Imposter;
                }

                IdentityCycle::Correct
            })
            .materialize();

        let signal_server_id = server_ent.id.clone();
        let signal_server_address = server_ent.address.clone();
        let signal_server_port = server_ent.port;

        let signal_state = connection_status
            .join(&identity)
            .map(move |(status, identity)| {
                tracing::debug!(
                    "server: {}:{}:{}, status: {:?}, {:?}",
                    signal_server_id,
                    signal_server_address,
                    signal_server_port,
                    status,
                    identity
                );
                match (status, identity) {
                    (_, IdentityCycle::Imposter) => PeerState::Delete,
                    (PeerConnectionCycle::Failed, _) => PeerState::Delete,
                    (_, _) => PeerState::Keep,
                }
            })
            .materialize();

        Self {
            server_ent,
            client: Arc::new(client),
            signal_state,
        }
    }

    pub(super) fn client(&self) -> Arc<MykoClient> {
        self.client.clone()
    }
}

impl std::fmt::Debug for PeerConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConnectionHandle")
            .field("server_id", &self.server_ent.id)
            .finish()
    }
}

impl Drop for PeerConnectionHandle {
    fn drop(&mut self) {
        self.client.close();
        info!(
            "Peer handle dropped: {} (closed peer client)",
            self.server_ent.id
        );
    }
}
