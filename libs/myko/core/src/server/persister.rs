//! Trait for persisting events to a durable store.
//!
//! Implementations may perform their I/O directly or delegate it internally,
//! but a successful `persist` call confirms that the event is durable.

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
    pub entity_type: std::sync::Arc<str>,
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

/// Duration of the sliding rate window for `writes_per_second` calculation.
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
            *self
                .last_error
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
    }

    /// Record a batch of successful writes, decrementing queued by `count`.
    pub fn record_success_batch(&self, count: u64) {
        self.queued.fetch_sub(count, Ordering::Relaxed);
        self.total_persisted.fetch_add(count, Ordering::Relaxed);
        if self.consecutive_errors.swap(0, Ordering::Relaxed) > 0 {
            *self
                .last_error
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
    }

    pub fn record_error(&self, msg: String) {
        self.queued.fetch_sub(1, Ordering::Relaxed);
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
        *self
            .last_error
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(msg);
    }

    pub fn record_dropped(&self, msg: String) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
        *self
            .last_error
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(msg);
    }

    /// Record an error without decrementing queued (event will be retried).
    pub fn record_error_no_dequeue(&self, msg: String) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
        *self
            .last_error
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(msg);
    }

    /// Compute writes per second over a sliding window.
    ///
    /// Each call checks whether the window has elapsed. If so, it rotates the
    /// window and returns the rate from the completed window. Otherwise it
    /// returns the instantaneous rate within the current window.
    pub fn writes_per_second(&self) -> f64 {
        let current_total = self.total_persisted.load(Ordering::Relaxed);
        let elapsed = self
            .rate_window_start
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .elapsed()
            .as_secs_f64();

        if elapsed >= RATE_WINDOW_SECS {
            let mut start = self
                .rate_window_start
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let elapsed = start.elapsed().as_secs_f64();
            let window_count = self
                .rate_window_count
                .swap(current_total, Ordering::Relaxed);
            let delta = current_total.saturating_sub(window_count);
            *start = Instant::now();
            drop(start);
            num_traits::ToPrimitive::to_f64(&delta).unwrap_or(0.0) / elapsed
        } else if elapsed > 0.0 {
            let window_count = self.rate_window_count.load(Ordering::Relaxed);
            let delta = current_total.saturating_sub(window_count);
            num_traits::ToPrimitive::to_f64(&delta).unwrap_or(0.0) / elapsed
        } else {
            0.0
        }
    }
}

/// Trait for persisting events to a durable store.
pub trait Persister: Send + Sync + 'static {
    /// Persist a single event and wait for durable acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn persist(&self, event: MEvent) -> Result<(), PersistError>;

    /// Startup healthcheck hook.
    ///
    /// Persisters can override this to fail server startup when dependencies
    /// (broker, credentials, etc.) are not healthy.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
}

/// No-op persister that intentionally drops all events for selected entity types.
pub struct BlackholePersister;

impl Persister for BlackholePersister {
    fn persist(&self, _event: MEvent) -> Result<(), PersistError> {
        Ok(())
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
    #[must_use]
    pub fn resolve(&self, entity_type: &str) -> Option<Arc<dyn Persister>> {
        self.overrides
            .get(entity_type)
            .cloned()
            .or_else(|| self.default.clone())
    }

    /// Get the shared health state from the default persister.
    ///
    /// Returns live atomic counters — callers can poll these to read
    /// current values. Returns a static zero-health if no default persister
    /// is configured.
    #[must_use]
    pub fn default_health(&self) -> Arc<PersistHealth> {
        self.default.as_ref().map_or_else(
            || {
                static HEALTHY: std::sync::OnceLock<Arc<PersistHealth>> =
                    std::sync::OnceLock::new();
                HEALTHY
                    .get_or_init(|| Arc::new(PersistHealth::default()))
                    .clone()
            },
            |p| p.health(),
        )
    }

    /// Run startup healthchecks for all resolved persisters across known entity types.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn startup_healthcheck(&self, entity_types: &[&str]) -> Result<(), String> {
        for entity_type in entity_types {
            if let Some(persister) = self.resolve(entity_type) {
                persister.startup_healthcheck().map_err(|reason| {
                    format!(
                        "Persister startup healthcheck failed for entity type `{entity_type}`: {reason}"
                    )
                })?;
            }
        }
        Ok(())
    }
}
