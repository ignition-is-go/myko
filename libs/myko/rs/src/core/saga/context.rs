//! Saga context for accessing server resources during event processing.
//!
//! The [`SagaContext`] provides sagas with access to:
//! - **Store registry**: Access entities directly
//! - **Event sink**: Publish events for persistence
//! - **Server identity**: Access the host ID for filtering events
//!
//! # Example
//!
//! ```rust,no_run
//! use myko_rs::event::MEvent;
//! use myko_rs::saga::SagaContext;
//!
//! fn handle_event(event: MEvent, ctx: &SagaContext) -> Option<MEvent> {
//!     // Check if this event originated from our server
//!     if event.source_id.as_deref() != Some(&ctx.host_id().to_string()) {
//!         return None;
//!     }
//!
//!     // Return an event to publish
//!     Some(event)
//! }
//! ```

use std::sync::Arc;

use uuid::Uuid;

use crate::{event::MEvent, store::StoreRegistry};

/// Error type for saga operations.
///
/// Wraps errors that occur during saga execution.
#[derive(Debug, Clone)]
pub struct SagaError {
    /// Identifier of the saga that encountered the error
    pub saga_id: String,
    /// Human-readable error message
    pub message: String,
}

impl std::fmt::Display for SagaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SagaError({}): {}", self.saga_id, self.message)
    }
}

impl std::error::Error for SagaError {}

/// Context provided to sagas for accessing server resources.
///
/// SagaContext allows sagas to:
/// - Access the store registry for queries
/// - Publish events via the event sink
/// - Access server identity (host_id)
#[derive(Clone)]
pub struct SagaContext {
    /// Server host ID
    pub host_id: Uuid,

    /// Store registry for accessing entities
    pub registry: Arc<StoreRegistry>,

    /// Event sink for publishing events (optional)
    pub event_sink: Option<flume::Sender<MEvent>>,
}

impl SagaContext {
    /// Create a new SagaContext
    pub fn new(host_id: Uuid, registry: Arc<StoreRegistry>) -> Self {
        Self {
            host_id,
            registry,
            event_sink: None,
        }
    }

    /// Create a new SagaContext with an event sink
    pub fn with_event_sink(
        host_id: Uuid,
        registry: Arc<StoreRegistry>,
        event_sink: flume::Sender<MEvent>,
    ) -> Self {
        Self {
            host_id,
            registry,
            event_sink: Some(event_sink),
        }
    }

    /// Get the host ID for this server
    pub fn host_id(&self) -> Uuid {
        self.host_id
    }

    /// Get the store registry
    pub fn registry(&self) -> &Arc<StoreRegistry> {
        &self.registry
    }

    /// Publish an event (if event sink is configured)
    pub fn publish_event(&self, event: MEvent) -> Result<(), SagaError> {
        if let Some(ref sink) = self.event_sink {
            sink.send(event).map_err(|e| SagaError {
                saga_id: "context".to_string(),
                message: format!("Failed to publish event: {}", e),
            })
        } else {
            Ok(()) // No sink configured, silently succeed
        }
    }
}
