//! Trait for persisting events to a durable store.
//!
//! Implementations may be sync (in-memory/no-op) or internally async (Kafka).
//! The `persist` call is fire-and-forget — the implementation handles delivery.

use crate::wire::MEvent;

/// Trait for persisting events to a durable store.
pub trait Persister: Send + Sync + 'static {
    /// Persist a single event.
    fn persist(&self, event: MEvent);
}

/// No-op persister for in-memory-only operation (dev mode).
pub struct NullPersister;

impl Persister for NullPersister {
    fn persist(&self, _event: MEvent) {}
}
