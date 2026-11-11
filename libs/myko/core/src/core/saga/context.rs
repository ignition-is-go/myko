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
    pub(crate) host_id: Uuid,

    /// Store registry for accessing entities
    pub(crate) registry: Arc<StoreRegistry>,
}

impl SagaContext {
    /// Create a new `SagaContext`
    #[must_use]
    pub const fn new(host_id: Uuid, registry: Arc<StoreRegistry>) -> Self {
        Self { host_id, registry }
    }

    /// Get the host ID for this server.
    ///
    /// Inherent (not [`RequestScoped`](crate::core::capability::RequestScoped)):
    /// a saga has no originating [`RequestContext`], so `host_id` stays here.
    #[must_use]
    pub const fn host_id(&self) -> Uuid {
        self.host_id
    }
}

impl crate::core::capability::sealed::Sealed for SagaContext {}
impl crate::core::capability::RegistryScoped for SagaContext {
    fn __registry(&self) -> &Arc<StoreRegistry> {
        &self.registry
    }
}
