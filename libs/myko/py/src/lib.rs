//! Python bindings for myko
//!
//! This crate provides PyO3 bindings for the Rust myko library,
//! enabling Python applications to use reactive queries, reports, and commands.

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyDict;
use std::sync::Arc;

/// Connection status enum matching the Rust client
#[pyclass]
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
}

/// Python wrapper around MykoClient
///
/// Provides WebSocket connectivity to a Myko server with support for
/// queries, reports, and commands.
#[pyclass]
pub struct MykoClient {
    inner: Arc<myko::client::MykoClient>,
    runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl MykoClient {
    /// Create a new MykoClient instance
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;

        let client = myko::client::MykoClient::new();

        Ok(Self {
            inner: Arc::new(client),
            runtime: Arc::new(runtime),
        })
    }

    /// Set the server address (e.g., "ws://localhost:5155/myko")
    ///
    /// Args:
    ///     address: The WebSocket URL of the Myko server, or None to disconnect
    fn set_address(&self, address: Option<String>) {
        self.inner.set_address(address);
    }

    /// Get the current connection status
    ///
    /// Returns:
    ///     ConnectionStatus.Connected or ConnectionStatus.Disconnected
    fn get_connection_status(&self, py: Python<'_>) -> PyResult<ConnectionStatus> {
        let inner = self.inner.clone();
        py.allow_threads(|| {
            self.runtime.block_on(async {
                let status = inner.get_connection_status().await;
                match status {
                    myko::client::ConnectionStatus::Connected(_) => Ok(ConnectionStatus::Connected),
                    myko::client::ConnectionStatus::Idle
                    | myko::client::ConnectionStatus::Connecting(_)
                    | myko::client::ConnectionStatus::Reconnecting(_)
                    | myko::client::ConnectionStatus::Disconnected => {
                        Ok(ConnectionStatus::Disconnected)
                    }
                }
            })
        })
    }

    /// Disconnect from the server
    fn disconnect(&self) {
        self.inner.set_address(None);
    }

    /// Send an event to the server
    ///
    /// Args:
    ///     event: The event data as a dict with keys: item, itemType, changeType, tx, createdAt
    ///
    /// Raises:
    ///     RuntimeError: If the event fails to send
    fn send_event<'py>(&self, py: Python<'py>, event: Bound<'py, PyDict>) -> PyResult<()> {
        let event_json: serde_json::Value = pythonize::depythonize(&event)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to serialize event: {}", e)))?;

        let event: myko::event::MEvent = serde_json::from_value(event_json)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid event format: {}", e)))?;

        let inner = self.inner.clone();

        py.allow_threads(move || {
            self.runtime.block_on(async {
                inner.send_event(event)
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to send event: {}", e)))
            })
        })
    }

    /// String representation
    fn __repr__(&self) -> String {
        "MykoClient()".to_string()
    }
}

/// Python module definition
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MykoClient>()?;
    m.add_class::<ConnectionStatus>()?;
    Ok(())
}
