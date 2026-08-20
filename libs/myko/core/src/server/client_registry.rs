//! Global client registry for sending messages to connected WebSocket clients.
//!
//! Provides a thread-safe mapping from `client_id` to `WsWriter`,
//! enabling any part of the server to send messages to specific clients.

use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use serde::Serialize;

use super::WsWriter;
use crate::{
    command::{CommandId, CommandRequest},
    wire::{MykoMessage, encode_command_message},
};

/// Thread-safe registry mapping client IDs to their WebSocket writers.
pub struct ClientRegistry {
    writers: DashMap<Arc<str>, Arc<dyn WsWriter>>,
}

impl ClientRegistry {
    fn new() -> Self {
        Self {
            writers: DashMap::new(),
        }
    }

    /// Register a client's writer.
    pub fn register(&self, client_id: Arc<str>, writer: Arc<dyn WsWriter>) {
        self.writers.insert(client_id, writer);
    }

    /// Unregister a client's writer.
    pub fn unregister(&self, client_id: &str) {
        self.writers.remove(client_id);
    }

    /// Whether this client id has a live WS writer right now. This is the
    /// process-local ground truth for connection liveness — unlike the
    /// persisted `Client` entity store, which replays rows for connections
    /// that died with a previous process and never saw their disconnect
    /// cascade (the zombie-presence class: a stale entity row that keeps a
    /// viewer "present" forever).
    #[must_use]
    pub fn contains(&self, client_id: &str) -> bool {
        self.writers.contains_key(client_id)
    }

    /// Snapshot of every client id with a live WS writer. For sweepers that
    /// reconcile persisted presence entities against actual connections.
    #[must_use]
    pub fn live_ids(&self) -> Vec<Arc<str>> {
        self.writers
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Send a message to a specific client.
    ///
    /// Returns `true` if the client was found and the message was sent.
    #[must_use]
    pub fn send_to(&self, client_id: &str, msg: MykoMessage) -> bool {
        self.writers.get(client_id).is_some_and(|writer| {
            writer.send(msg);
            true
        })
    }

    pub fn send_command_request_to<C>(&self, client_id: &str, request: &CommandRequest<C>) -> bool
    where
        C: CommandId + Serialize,
    {
        let Some(writer) = self.writers.get(client_id) else {
            return false;
        };

        let command_id = request.command_id().to_string();
        let protocol = writer.protocol();

        match encode_command_message(protocol, request) {
            Ok(payload) => {
                writer.send_serialized_command(request.tx.clone(), command_id, payload);
                true
            }
            Err(err) => {
                tracing::error!(
                    "Failed to serialize command {} for client {}: {}",
                    request.command_id(),
                    client_id,
                    err
                );
                false
            }
        }
    }

    /// Number of currently connected clients.
    #[must_use]
    pub fn len(&self) -> usize {
        self.writers.len()
    }

    /// Returns true when there are no connected clients.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.writers.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Global accessor (same pattern as sync_client)
// ─────────────────────────────────────────────────────────────────────────────

static CLIENT_REGISTRY: OnceLock<Arc<ClientRegistry>> = OnceLock::new();

/// Initialize the global client registry.
///
/// Safe to call multiple times — only the first call has effect.
pub fn init_client_registry() {
    let _ = CLIENT_REGISTRY.set(Arc::new(ClientRegistry::new()));
}

/// Get the global client registry.
pub fn client_registry() -> Arc<ClientRegistry> {
    CLIENT_REGISTRY
        .get_or_init(|| Arc::new(ClientRegistry::new()))
        .clone()
}

/// Try to get the global client registry.
///
/// Returns None if `init_client_registry()` has not been called.
pub fn try_client_registry() -> Option<Arc<ClientRegistry>> {
    CLIENT_REGISTRY.get().cloned()
}

#[cfg(test)]
mod liveness_tests {
    use super::*;
    use crate::server::client_session::WsWriter;
    use crate::wire::message::MykoMessage;

    struct NullWriter;
    impl WsWriter for NullWriter {
        fn send(&self, _msg: MykoMessage) {}
        fn send_serialized_command(
            &self,
            _tx: Arc<str>,
            _command_id: String,
            _payload: crate::wire::command::EncodedCommandMessage,
        ) {
        }
    }

    #[test]
    fn contains_and_live_ids_reflect_registered_writers() {
        let registry = ClientRegistry::new();
        assert!(!registry.contains("a"));
        assert!(registry.live_ids().is_empty());
        registry.register("a".into(), Arc::new(NullWriter));
        assert!(registry.contains("a"));
        assert_eq!(registry.live_ids(), vec![Arc::<str>::from("a")]);
        registry.unregister("a");
        assert!(!registry.contains("a"));
    }
}
