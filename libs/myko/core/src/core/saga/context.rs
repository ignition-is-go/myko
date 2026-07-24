//! Saga context for accessing server resources during event processing.
//!
//! Sagas react to events and emit **commands** — never events. The emitted
//! `SagaCommand`s are executed by the server against a `CommandContext`, which
//! is where any state mutation happens. So the [`SagaContext`] a saga receives
//! is a minimal *read* context: server identity and the store registry, enough
//! to decide which commands to emit.
//!
//! # Example
//!
//! ```rust,no_run
//! use myko::event::MEvent;
//! use myko::saga::SagaContext;
//!
//! fn is_ours(event: &MEvent, ctx: &SagaContext) -> bool {
//!     // Only react to events this server originated.
//!     event.source_id.as_deref() == Some(&ctx.host_id().to_string())
//! }
//! ```

use std::sync::Arc;

use uuid::Uuid;

use crate::store::StoreRegistry;

/// Context provided to sagas during event processing.
///
/// A saga's job is to react to events and emit commands (executed later against
/// a `CommandContext`), so this is a read-only context — server identity plus
/// the store registry to decide which commands to emit. It deliberately carries
/// no way to emit events or mutate state directly.
#[derive(Clone)]
pub struct SagaContext {
    /// Server host ID
    pub host_id: Uuid,

    /// Store registry for accessing entities
    pub registry: Arc<StoreRegistry>,
}

impl SagaContext {
    /// Create a new SagaContext
    pub fn new(host_id: Uuid, registry: Arc<StoreRegistry>) -> Self {
        Self { host_id, registry }
    }

    /// Get the host ID for this server
    pub fn host_id(&self) -> Uuid {
        self.host_id
    }

    /// Get the store registry
    pub fn registry(&self) -> &Arc<StoreRegistry> {
        &self.registry
    }
}
