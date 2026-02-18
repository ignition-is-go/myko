//! Trait for persisting events to a durable store.
//!
//! Implementations may be sync (in-memory/no-op) or internally async (Kafka).
//! The `persist` call is fire-and-forget — the implementation handles delivery.

use std::{collections::HashMap, sync::Arc};

use crate::wire::MEvent;

/// Trait for persisting events to a durable store.
pub trait Persister: Send + Sync + 'static {
    /// Persist a single event.
    fn persist(&self, event: MEvent);

    /// Whether this persister requires Kafka topic catch-up for this entity stream.
    ///
    /// Durable persisters (Kafka-backed) should return `true`.
    /// Ephemeral/no-op persisters should return `false`.
    fn should_register_kafka_topic(&self) -> bool {
        true
    }

    /// Startup healthcheck hook.
    ///
    /// Persisters can override this to fail server startup when dependencies
    /// (broker, credentials, etc.) are not healthy.
    fn startup_healthcheck(&self) -> Result<(), String> {
        Ok(())
    }
}

/// No-op persister for in-memory-only operation (dev mode).
pub struct NullPersister;

impl Persister for NullPersister {
    fn persist(&self, _event: MEvent) {}

    fn should_register_kafka_topic(&self) -> bool {
        false
    }
}

/// No-op persister that intentionally drops all events for selected entity types.
///
/// Use this when an entity stream should never be durable and should be treated as
/// immediately caught up during Kafka initialization.
pub struct BlackholePersister;

impl Persister for BlackholePersister {
    fn persist(&self, _event: MEvent) {}

    fn should_register_kafka_topic(&self) -> bool {
        false
    }
}

/// Resolves persisters by entity type using:
/// 1) per-entity override
/// 2) default persister
#[derive(Default, Clone)]
pub struct PersisterRouter {
    default: Option<Arc<dyn Persister>>,
    overrides: HashMap<String, Arc<dyn Persister>>,
}

impl PersisterRouter {
    /// Set the default persister used when no per-entity override exists.
    pub fn set_default(&mut self, persister: Option<Arc<dyn Persister>>) {
        self.default = persister;
    }

    /// Set a persister override for an entity type name (e.g. "Pulse").
    pub fn set_override(&mut self, entity_type: impl Into<String>, persister: Arc<dyn Persister>) {
        self.overrides.insert(entity_type.into(), persister);
    }

    /// Resolve the persister for an entity type.
    pub fn resolve(&self, entity_type: &str) -> Option<Arc<dyn Persister>> {
        self.overrides
            .get(entity_type)
            .cloned()
            .or_else(|| self.default.clone())
    }

    /// Whether this entity type should be included in Kafka catch-up topic registration.
    pub fn should_register_kafka_topic(&self, entity_type: &str) -> bool {
        self.resolve(entity_type)
            .map(|p| p.should_register_kafka_topic())
            .unwrap_or(false)
    }

    /// Run startup healthchecks for all resolved persisters across known entity types.
    pub fn startup_healthcheck(&self, entity_types: &[&str]) -> Result<(), String> {
        for entity_type in entity_types {
            if let Some(persister) = self.resolve(entity_type) {
                persister.startup_healthcheck().map_err(|reason| {
                    format!(
                        "Persister startup healthcheck failed for entity type `{}`: {}",
                        entity_type, reason
                    )
                })?;
            }
        }
        Ok(())
    }
}
