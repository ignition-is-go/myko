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

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, ts_rs::TS)]
#[ts(export)]
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
    /// Set the remote address. `None` disconnects.
    fn set_addr(&self, addr: Option<String>);

    /// Close the connection and stop reconnection attempts.
    fn close(&self);

    /// Intended connection state requested by the caller.
    fn intended_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable>;

    /// Actual observed connection state of the transport.
    fn actual_connection_state(&self) -> Cell<SocketConnectionStatus, CellImmutable>;

    /// Send a frame to the remote end.
    fn send(&self, frame: WsFrame) -> Result<(), String>;

    /// Read stream of incoming websocket frames.
    fn read_rx(&self) -> flume::Receiver<WsFrame>;
}
