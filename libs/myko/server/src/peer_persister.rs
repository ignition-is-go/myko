//! `PeerPersister` — in-memory event fanout to peer servers over the peer-WS mesh.
//!
//! This persister does **not** provide durability. Events are fanned out via
//! `MykoClient::send_event` to every currently-connected peer server. Each
//! peer's `ws_handler` receives the frame as `MykoMessage::Event(MEvent)` and
//! applies it to its local store via the existing `ctx.apply_event` path.
//!
//! Use this for ephemeral per-entity state that needs live cross-peer
//! replication but no durable history — e.g. sync-value runtime state, live
//! clock coordination. For entities where you need durability, continue to
//! use `PostgresProducerHandle`.
//!
//! Ordering guarantees: per-origin, monotonic. The local peer's order-of-emit
//! is preserved on the wire because each peer's `MykoClient` write path is
//! serialized. On the receive side, `handle_frame` delivers events to
//! `ctx.apply_event` in arrival order on the WS read thread, so per-origin
//! ordering survives the trip. Events from a different peer may interleave
//! with local emits — that's fine for last-writer-wins semantics on the
//! entities this is intended for.
//!
//! Echo suppression: `MEvent::source_id` is populated with the origin
//! server's host id when the event is produced (see `MykoServerCtx::produce_*`).
//! Receivers skip events they themselves originated (see
//! `normalize_incoming_event` + `apply_event_batch_immediate` filtering).

use std::sync::Arc;

use dashmap::DashMap;
use myko::{
    client::MykoClient,
    event::MEvent,
    server::{PersistError, PersistHealth, Persister},
};
use tracing::warn;

/// Persister that replicates events to all connected peer servers.
pub struct PeerPersister {
    peer_clients: Arc<DashMap<Arc<str>, Arc<MykoClient>>>,
    health: Arc<PersistHealth>,
}

impl PeerPersister {
    /// Create a new `PeerPersister` that broadcasts through the given peer-client map.
    ///
    /// The map is shared with `MykoServerCtx::peer_clients` so entries added by
    /// `PeerRegistry::register_peer_client` become visible here automatically.
    pub fn new(peer_clients: Arc<DashMap<Arc<str>, Arc<MykoClient>>>) -> Self {
        Self {
            peer_clients,
            health: Arc::new(PersistHealth::default()),
        }
    }

    /// Peer client count — mostly for diagnostics and health reporting.
    pub fn peer_count(&self) -> usize {
        self.peer_clients.len()
    }
}

impl Persister for PeerPersister {
    fn persist(&self, event: MEvent) -> Result<(), PersistError> {
        let entity_type = event.item_type.clone();
        self.health.record_enqueue();

        // No peers currently connected — nothing to broadcast. Local apply
        // already happened on the emit-side, so this is a valid no-op.
        if self.peer_clients.is_empty() {
            self.health.record_success();
            return Ok(());
        }

        let peer_count = self.peer_clients.len();
        let mut error_count = 0usize;

        // Iterate and fan out. `send_event` clones the event internally when
        // it encodes to a frame, so we clone once per peer here. A future
        // optimization could encode once and send the same `WsFrame` to every
        // peer, but it would require shape changes in `MykoClient`.
        for entry in self.peer_clients.iter() {
            let client = entry.value();
            if let Err(e) = client.send_event(event.clone()) {
                warn!(
                    "PeerPersister: broadcast failed for entity={} peer={}: {}",
                    entity_type,
                    entry.key(),
                    e
                );
                error_count += 1;
            }
        }

        if error_count == peer_count {
            let msg = format!(
                "PeerPersister: broadcast failed for all {} peer(s)",
                peer_count
            );
            self.health.record_error(msg.clone());
            return Err(PersistError {
                entity_type,
                message: msg,
            });
        }

        // Partial or full success. Treat partial as success — the remaining
        // peers will reconcile when they reconnect. Health tracking could be
        // expanded to distinguish partial vs. full later.
        self.health.record_success();
        Ok(())
    }

    fn health(&self) -> Arc<PersistHealth> {
        self.health.clone()
    }

    fn startup_healthcheck(&self) -> Result<(), String> {
        // PeerPersister has no dependencies that can fail at startup — peers
        // may legitimately be absent when the server first comes up. We rely
        // on the peer registry's reconcile loop to populate `peer_clients`
        // later.
        Ok(())
    }
}
