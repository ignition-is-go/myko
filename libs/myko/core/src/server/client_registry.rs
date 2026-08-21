//! Global client registry for sending messages to connected WebSocket clients.
//!
//! Provides a thread-safe mapping from `client_id` to `WsWriter`,
//! enabling any part of the server to send messages to specific clients.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use hyphae::{Cell, CellImmutable, CellMap, MapExt, Materialize};
use serde::Serialize;

use super::WsWriter;
use crate::{
    command::{CommandId, CommandRequest},
    wire::{MykoMessage, encode_command_message},
};

/// Thread-safe registry mapping client IDs to their WebSocket writers.
pub struct ClientRegistry {
    writers: CellMap<Arc<str>, RegisteredWriter>,
}

#[derive(Clone)]
struct RegisteredWriter(Arc<dyn WsWriter>);

impl fmt::Debug for RegisteredWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RegisteredWriter").finish()
    }
}

impl PartialEq for RegisteredWriter {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl ClientRegistry {
    fn new() -> Self {
        Self {
            writers: CellMap::new().with_name("client_registry"),
        }
    }

    /// Register a client's writer.
    pub fn register(&self, client_id: Arc<str>, writer: Arc<dyn WsWriter>) {
        self.writers.insert(client_id, RegisteredWriter(writer));
    }

    /// Unregister a client's writer.
    pub fn unregister(&self, client_id: &str) {
        self.writers.remove(&Arc::<str>::from(client_id));
    }

    /// Reactively track whether a client has a live WebSocket writer.
    ///
    /// This derives directly from the registry's per-key `CellMap` observation;
    /// persisted `Client` entities are deliberately not part of liveness.
    #[must_use]
    pub(crate) fn watch_connected(&self, client_id: &Arc<str>) -> Cell<bool, CellImmutable> {
        self.writers
            .get(client_id)
            .map(Option::is_some)
            .materialize()
    }

    /// Send a message to a specific client.
    ///
    /// Returns `true` if the client was found and the message was sent.
    #[must_use]
    pub fn send_to(&self, client_id: &str, msg: MykoMessage) -> bool {
        self.writers
            .get_value(&Arc::<str>::from(client_id))
            .is_some_and(|writer| {
                writer.0.send(msg);
                true
            })
    }

    pub fn send_command_request_to<C>(&self, client_id: &str, request: &CommandRequest<C>) -> bool
    where
        C: CommandId + Serialize,
    {
        let Some(writer) = self.writers.get_value(&Arc::<str>::from(client_id)) else {
            return false;
        };

        let command_id = request.command_id().to_string();
        let protocol = writer.0.protocol();

        match encode_command_message(protocol, request) {
            Ok(payload) => {
                writer
                    .0
                    .send_serialized_command(request.tx.clone(), command_id, payload);
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
        self.writers.keys_snapshot().len()
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
    use hyphae::Gettable;

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
    fn cellmap_liveness_reflects_registered_writers() {
        let registry = ClientRegistry::new();
        let client_id = Arc::<str>::from("a");
        let connected = registry.watch_connected(&client_id);
        assert!(!connected.get());
        registry.register("a".into(), Arc::new(NullWriter));
        assert!(connected.get());
        registry.unregister("a");
        assert!(!connected.get());
    }
}
