//! Global client registry for sending messages to connected WebSocket clients.
//!
//! Provides a thread-safe mapping from client_id to WsWriter,
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

    /// Send a message to a specific client.
    ///
    /// Returns `true` if the client was found and the message was sent.
    pub fn send_to(&self, client_id: &str, msg: MykoMessage) -> bool {
        if let Some(writer) = self.writers.get(client_id) {
            writer.send(msg);
            true
        } else {
            false
        }
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
                log::error!(
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
    pub fn len(&self) -> usize {
        self.writers.len()
    }

    /// Returns true when there are no connected clients.
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
///
/// # Panics
/// Panics if `init_client_registry()` has not been called.
pub fn client_registry() -> Arc<ClientRegistry> {
    CLIENT_REGISTRY
        .get()
        .expect("Client registry not initialized - call init_client_registry() first")
        .clone()
}

/// Try to get the global client registry.
///
/// Returns None if `init_client_registry()` has not been called.
pub fn try_client_registry() -> Option<Arc<ClientRegistry>> {
    CLIENT_REGISTRY.get().cloned()
}
