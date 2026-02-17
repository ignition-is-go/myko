use std::sync::atomic::{AtomicU64, Ordering};

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

/// Generates unique callback IDs for DashMap-based callback registries.
pub(crate) fn next_callback_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
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

    /// Get the current connection status.
    fn get_status(&self) -> SocketConnectionStatus;

    /// Send a frame to the remote end.
    fn send(&self, frame: WsFrame) -> Result<(), String>;

    /// Register a callback for incoming messages.
    /// The callback is called for each received frame.
    /// Drop the returned guard to unsubscribe.
    fn on_message(&self, cb: Box<dyn Fn(WsFrame) + Send + Sync>) -> CallbackGuard;

    /// Register a callback for connection status changes.
    /// The callback is called immediately with the current status,
    /// then again on each status change.
    /// Drop the returned guard to unsubscribe.
    fn on_status_change(
        &self,
        cb: Box<dyn Fn(SocketConnectionStatus) + Send + Sync>,
    ) -> CallbackGuard;
}
