//! Trait for persisting events to a durable store.
//!
//! Implementations may be sync (in-memory/no-op) or internally async (Kafka).
//! The `persist` call is fire-and-forget — the implementation handles delivery.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::wire::MEvent;

/// Error returned when event persistence fails.
#[derive(Debug, Clone)]
pub struct PersistError {
    pub entity_type: String,
    pub message: String,
}

impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "persist failed for {}: {}",
            self.entity_type, self.message
        )
    }
}

impl std::error::Error for PersistError {}

/// Duration of the sliding rate window for writes_per_second calculation.
const RATE_WINDOW_SECS: f64 = 1.0;

/// Shared health state for a persister, readable from any thread.
#[derive(Debug)]
pub struct PersistHealth {
    /// Events queued but not yet written to the durable store.
    pub queued: AtomicU64,
    /// Lifetime count of successfully persisted events.
    pub total_persisted: AtomicU64,
    /// Lifetime count of failed persist attempts.
    pub total_errors: AtomicU64,
    /// Consecutive failures since last success (resets to 0 on success).
    pub consecutive_errors: AtomicU64,
    /// Most recent error message, if any.
    pub last_error: std::sync::RwLock<Option<String>>,
    /// Persisted count at the start of the current rate window.
    rate_window_count: AtomicU64,
    /// Start of the current rate window.
    rate_window_start: std::sync::RwLock<Instant>,
}

impl Default for PersistHealth {
    fn default() -> Self {
        Self {
            queued: AtomicU64::new(0),
            total_persisted: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            consecutive_errors: AtomicU64::new(0),
            last_error: std::sync::RwLock::new(None),
            rate_window_count: AtomicU64::new(0),
            rate_window_start: std::sync::RwLock::new(Instant::now()),
        }
    }
}

impl PersistHealth {
    pub fn record_enqueue(&self) {
        self.queued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_success(&self) {
        self.queued.fetch_sub(1, Ordering::Relaxed);
        self.total_persisted.fetch_add(1, Ordering::Relaxed);
        if self.consecutive_errors.swap(0, Ordering::Relaxed) > 0 {
            *self.last_error.write().unwrap() = None;
        }
    }

    /// Record a batch of successful writes, decrementing queued by `count`.
    pub fn record_success_batch(&self, count: u64) {
        self.queued.fetch_sub(count, Ordering::Relaxed);
        self.total_persisted.fetch_add(count, Ordering::Relaxed);
        if self.consecutive_errors.swap(0, Ordering::Relaxed) > 0 {
            *self.last_error.write().unwrap() = None;
        }
    }

    pub fn record_error(&self, msg: String) {
        self.queued.fetch_sub(1, Ordering::Relaxed);
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.write().unwrap() = Some(msg);
    }

    pub fn record_dropped(&self, msg: String) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.write().unwrap() = Some(msg);
    }

    /// Record an error without decrementing queued (event will be retried).
    pub fn record_error_no_dequeue(&self, msg: String) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
        *self.last_error.write().unwrap() = Some(msg);
    }

    /// Compute writes per second over a sliding window.
    ///
    /// Each call checks whether the window has elapsed. If so, it rotates the
    /// window and returns the rate from the completed window. Otherwise it
    /// returns the instantaneous rate within the current window.
    pub fn writes_per_second(&self) -> f64 {
        let current_total = self.total_persisted.load(Ordering::Relaxed);
        let mut start = self.rate_window_start.write().unwrap();
        let elapsed = start.elapsed().as_secs_f64();

        if elapsed >= RATE_WINDOW_SECS {
            let window_count = self
                .rate_window_count
                .swap(current_total, Ordering::Relaxed);
            let delta = current_total.saturating_sub(window_count);
            *start = Instant::now();
            delta as f64 / elapsed
        } else if elapsed > 0.0 {
            let window_count = self.rate_window_count.load(Ordering::Relaxed);
            let delta = current_total.saturating_sub(window_count);
            delta as f64 / elapsed
        } else {
            0.0
        }
    }
}

/// Trait for persisting events to a durable store.
pub trait Persister: Send + Sync + 'static {
    /// Persist a single event.
    fn persist(&self, event: MEvent) -> Result<(), PersistError>;

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

    /// Health counters for monitoring persist throughput and errors.
    fn health(&self) -> Arc<PersistHealth> {
        // Default: always-healthy, zero counters
        static HEALTHY: std::sync::OnceLock<Arc<PersistHealth>> = std::sync::OnceLock::new();
        HEALTHY
            .get_or_init(|| Arc::new(PersistHealth::default()))
            .clone()
    }
}

/// No-op persister for in-memory-only operation (dev mode).
pub struct NullPersister;

impl Persister for NullPersister {
    fn persist(&self, _event: MEvent) -> Result<(), PersistError> {
        Ok(())
    }

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
    fn persist(&self, _event: MEvent) -> Result<(), PersistError> {
        Ok(())
    }

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

    /// Get the shared health state from the default persister.
    ///
    /// Returns live atomic counters — callers can poll these to read
    /// current values. Returns a static zero-health if no default persister
    /// is configured.
    pub fn default_health(&self) -> Arc<PersistHealth> {
        self.default
            .as_ref()
            .map(|p| p.health())
            .unwrap_or_else(|| {
                static HEALTHY: std::sync::OnceLock<Arc<PersistHealth>> =
                    std::sync::OnceLock::new();
                HEALTHY
                    .get_or_init(|| Arc::new(PersistHealth::default()))
                    .clone()
            })
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
