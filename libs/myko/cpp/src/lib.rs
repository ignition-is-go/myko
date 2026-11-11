//! C++ bindings for myko
//!
//! This crate provides C++ bindings for the Rust myko library using cxx,
//! enabling C++ applications to use WebSocket connectivity to Myko servers.

use once_cell::sync::Lazy;
use std::sync::Arc;

// Global tokio runtime for async operations
static RUNTIME: Lazy<std::io::Result<tokio::runtime::Runtime>> =
    Lazy::new(tokio::runtime::Runtime::new);

/// Wrapper around MykoClient for C++ interop
pub struct MykoClientWrapper {
    inner: Arc<myko::client::MykoClient>,
}

impl MykoClientWrapper {
    fn new() -> Self {
        Self {
            inner: Arc::new(myko::client::MykoClient::new()),
        }
    }

    fn set_address(&self, address: &str) {
        let addr = if address.is_empty() {
            None
        } else {
            Some(address.to_string())
        };
        self.inner.set_address(addr);
    }

    fn disconnect(&self) {
        self.inner.set_address(None);
    }

    fn is_connected(&self) -> bool {
        let Ok(runtime) = &*RUNTIME else {
            return false;
        };
        let inner = self.inner.clone();
        runtime.block_on(async {
            let status = inner.get_connection_status().await;
            matches!(status, myko::client::ConnectionStatus::Connected(_))
        })
    }

    fn send_event_json(&self, event_json: &str) -> String {
        let Ok(runtime) = &*RUNTIME else {
            return "failed to create Tokio runtime".to_string();
        };
        let inner = self.inner.clone();
        let json = event_json.to_string();

        runtime.block_on(async {
            match serde_json::from_str::<myko::event::MEvent>(&json) {
                Ok(event) => {
                    match inner.send_event(event) {
                        Ok(()) => String::new(),
                        Err(e) => e,
                    }
                }
                Err(e) => e.to_string(),
            }
        })
    }
}

#[cxx::bridge(namespace = "myko")]
mod ffi {
    /// Connection status for C++
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ConnectionStatus {
        Connected,
        Disconnected,
    }

    extern "Rust" {
        type MykoClientWrapper;

        /// Create a new MykoClient instance
        fn new_client() -> Box<MykoClientWrapper>;

        /// Set the server address (empty string to disconnect)
        fn set_address(self: &MykoClientWrapper, address: &str);

        /// Disconnect from the server
        fn disconnect(self: &MykoClientWrapper);

        /// Check if connected
        fn is_connected(self: &MykoClientWrapper) -> bool;

        /// Send an event as JSON string
        /// Returns empty string on success, error message on failure
        fn send_event_json(self: &MykoClientWrapper, event_json: &str) -> String;
    }
}

/// Create a new MykoClient instance (called from C++)
fn new_client() -> Box<MykoClientWrapper> {
    Box::new(MykoClientWrapper::new())
}
