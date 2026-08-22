//! Reaping the disconnect cascade a dead process never ran.
//!
//! A `Client` entity is written when a WebSocket connects and deleted when it
//! disconnects, and the cascade does the real work: anything declaring
//! `#[belongs_to(Client)]` — a viewer's presence, a control lock's holder — is
//! deleted with it. That contract holds for every ordinary disconnection.
//!
//! It does not hold when the process dies with clients attached (crash,
//! restart, container recreate). Those rows are durable, so they replay on
//! boot, but the connections they describe died with the previous process and
//! can never disconnect again. Nothing deletes them, so the cascade never
//! runs: a viewer who never leaves, a control lock nobody can take back.
//!
//! [`ConnectedClients`] fixes the read side by semi-joining persisted rows
//! against live writers, so a consumer asking "who is connected" is never
//! lied to. It cannot fix the write side: the ghost row still exists, so
//! everything hanging off it stays alive too. This module deletes the row,
//! which is what makes the existing cascade release those dependents.
use myko::{
    entities::client::Client,
    server::{MykoServerContext, try_client_registry},
};

/// Delete this server's persisted `Client` rows at startup.
///
/// Correct by placement: this runs after catch-up and *before* the listener
/// binds, so this process cannot yet have a single connection. Every `Client`
/// row attributed to this server is therefore a leftover from a previous
/// process — there is no live connection it could describe.
///
/// Rows owned by other servers are left alone: those connections may be alive
/// elsewhere, and reaping them here would delete presence out from under a
/// healthy peer.
///
/// Returns the number of rows reaped.
pub fn reap_stale_clients(ctx: &MykoServerContext) -> usize {
    // Defensive: the "no connections yet" invariant is what makes a blanket
    // reap safe. If a caller ever moves this after the listener binds, refuse
    // rather than delete rows for clients that are genuinely connected.
    if let Some(registry) = try_client_registry()
        && !registry.is_empty()
    {
        tracing::warn!(
            "Skipping stale-client reap: {} live connection(s) already registered, so \
             persisted rows can no longer be assumed dead. This reap must run before the \
             listener binds.",
            registry.len()
        );
        return 0;
    }

    let host_id = ctx.host_id.to_string();
    let store = ctx.registry.get_or_create("Client");
    let stale: Vec<Client> = store
        .snapshot()
        .into_iter()
        .filter_map(|(_, item)| item.as_any().downcast_ref::<Client>().cloned())
        .filter(|client| client.server_id.as_ref() == host_id.as_str())
        .collect();

    let mut reaped = 0usize;
    for client in &stale {
        match ctx.del(client) {
            Ok(()) => reaped = reaped.saturating_add(1),
            Err(error) => {
                tracing::error!(
                    "Failed to reap stale client {}: {error}",
                    client.id.as_ref()
                );
            }
        }
    }
    if reaped > 0 {
        tracing::info!(
            "Reaped {reaped} stale client entit{} left by a previous process; \
             dependent presence and locks cascade away with them",
            if reaped == 1 { "y" } else { "ies" }
        );
    }
    reaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use myko::entities::client::ClientId;

    fn client(id: &str, server_id: &str) -> Client {
        Client {
            id: ClientId(id.into()),
            server_id: server_id.into(),
            address: None,
            windback: None,
        }
    }

    /// The ownership predicate the reap applies. Liveness is not consulted:
    /// before the listener binds there are no live connections, so ownership
    /// is the whole decision.
    fn is_ours(client: &Client, host_id: &str) -> bool {
        client.server_id.as_ref() == host_id
    }

    #[test]
    fn our_own_rows_are_reaped_because_nothing_is_connected_yet() {
        assert!(is_ours(&client("ghost", "server-a"), "server-a"));
    }

    #[test]
    fn another_servers_rows_are_left_alone() {
        // Those connections may be alive on that peer; deleting them here
        // would cascade away presence for a healthy client.
        assert!(!is_ours(&client("peer-client", "server-b"), "server-a"));
    }
}
