use std::sync::Arc;

use hyphae::{Cell, CellImmutable};

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::AutoReconnectSocket;

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::WasmSocket;

// ─────────────────────────────────────────────────────────────────────────────
// Shared types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "data")]
pub enum SocketConnectionStatus {
    Idle,
    Disconnected,
    Connecting(String),
    Reconnecting(String),
    Connected(String),
}

/// Platform-agnostic WebSocket frame.
#[derive(Clone, Debug)]
pub enum WsFrame {
    Text(String),
    Binary(Vec<u8>),
}

/// Callback invoked for each incoming frame by transports that can dispatch
/// directly from their platform event loop.
pub type FrameCallback = Arc<dyn Fn(WsFrame) + Send + Sync>;

// ─────────────────────────────────────────────────────────────────────────────
// CallbackGuard — RAII guard for callback subscriptions
// ─────────────────────────────────────────────────────────────────────────────

/// RAII guard that runs a cleanup function on drop.
///
/// Used to manage callback subscriptions — when the guard is dropped,
/// the callback is unregistered from the transport.
pub struct CallbackGuard {
    drop_fn: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl CallbackGuard {
    pub fn new(drop_fn: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            drop_fn: Some(Box::new(drop_fn)),
        }
    }

    /// Create a no-op guard that does nothing on drop.
    #[must_use]
    pub fn noop() -> Self {
        Self { drop_fn: None }
    }
}

impl Drop for CallbackGuard {
    fn drop(&mut self) {
        if let Some(f) = self.drop_fn.take() {
            f();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Transport trait
// ─────────────────────────────────────────────────────────────────────────────

/// Abstraction over a WebSocket connection with auto-reconnect behaviour.
///
/// Implemented by `AutoReconnectSocket` (native) and `WasmSocket` (browser).
/// Object-safe: uses callback-based subscriptions instead of channel returns.
pub trait SocketTransport: Send + Sync + 'static {
    /// Replace credentials and reconnect. Unsupported transports reject configuration.
    ///
    /// # Errors
    /// Returns an error when the transport cannot authenticate connections.
    fn set_credential_provider(
        &self,
        _provider: Option<Arc<dyn myko_auth::CredentialProvider>>,
    ) -> Result<(), String> {
        Err("transport does not support connection credentials".into())
    }

    /// Set the remote address. `None` disconnects.
    fn set_addr(&self, addr: Option<String>);

    /// Close the connection and stop reconnection attempts.
    fn close(&self);

    /// Intended connection state requested by the caller.
    fn intended_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable>;

    /// Actual observed connection state of the transport.
    fn actual_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable>;

    /// Send a frame to the remote end.
    ///
    /// # Errors
    ///
    /// Returns an error when the transport's outgoing channel is disconnected.
    fn send(&self, frame: WsFrame) -> Result<(), String>;

    /// Read stream of incoming websocket frames.
    fn read_rx(&self) -> flume::Receiver<WsFrame>;

    /// Install a direct incoming-frame callback when supported.
    ///
    /// Native transports use [`Self::read_rx`]; the browser transport overrides
    /// this so WebSocket events do not cross a shared-memory channel first.
    fn set_frame_callback(&self, _callback: FrameCallback) -> Option<CallbackGuard> {
        None
    }
}

/// A fixed bearer token for clients whose application manages token renewal.
pub struct StaticCredentialProvider {
    bearer: String,
}

impl StaticCredentialProvider {
    #[must_use]
    pub const fn new(bearer: String) -> Self {
        Self { bearer }
    }
}

impl myko_auth::CredentialProvider for StaticCredentialProvider {
    fn credentials(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<myko_auth::Credentials, myko_auth::AuthError>> {
        Box::pin(async {
            Ok(myko_auth::Credentials {
                bearer: Some(self.bearer.clone()),
            })
        })
    }
}

pub(crate) fn connection_bearer(credentials: myko_auth::Credentials) -> Result<String, String> {
    let bearer = credentials
        .bearer
        .ok_or("credential provider returned no bearer token")?;
    if bearer.is_empty()
        || bearer.len() > 4096
        || !bearer.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err("credential provider returned an invalid bearer token".into());
    }
    Ok(bearer)
}

#[cfg(test)]
mod credential_tests {
    use super::*;

    #[test]
    fn invalid_bearer_cannot_become_a_handshake_header() {
        for token in ["", "token\r\nX-Injected: yes", "token with spaces"] {
            assert_eq!(
                connection_bearer(myko_auth::Credentials {
                    bearer: Some(token.into())
                }),
                Err("credential provider returned an invalid bearer token".into())
            );
        }
        assert_eq!(
            connection_bearer(myko_auth::Credentials { bearer: None }),
            Err("credential provider returned no bearer token".into())
        );
        assert_eq!(
            connection_bearer(myko_auth::Credentials {
                bearer: Some("signed.token".into())
            }),
            Ok("signed.token".into())
        );
    }
}
