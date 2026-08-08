//! Core framework reports for entity traversal, logging, and event history.
//!
//! These reports provide framework-level functionality that operates across
//! all entity types rather than specific domain entities.

use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::Ordering;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use hyphae::MapExt;
#[cfg(not(target_arch = "wasm32"))]
use hyphae::SwitchMapExt;
use hyphae::{Cell, Definite, Materialize};
#[cfg(not(target_arch = "wasm32"))]
use hyphae::{DedupedExt, interval};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(not(target_arch = "wasm32"))]
use crate::core::capability::{PeerAccess, Replaying};
use crate::{
    TS,
    core::capability::EventPublishing,
    report::{ReportContext, ReportHandler},
    wire::{MEvent, WrappedItem},
};

// ─────────────────────────────────────────────────────────────────────────────
// Entity Stub Types
// ─────────────────────────────────────────────────────────────────────────────

/// Stub representation of an entity for tree traversal.
/// Contains minimal identifying information without full entity data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ItemStub {
    pub id: Arc<str>,
    pub item_type: String,
    pub name: Option<String>,
}

/// Data returned by `EntitySnapshotDifference` report.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct EntitySnapshotDifferenceData {
    pub changed: Vec<ItemStub>,
    pub added: Vec<ItemStub>,
    pub removed: Vec<ItemStub>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Entity Traversal Reports
// ─────────────────────────────────────────────────────────────────────────────

/// Report that fetches all items by type and IDs.
#[myko_macros::myko_report(Vec<Value>)]
pub struct GetItemsByTypeAndIds {
    /// The entity type name (e.g., "Scene", "Target")
    #[serde(rename = "type")]
    pub item_type: String,
    /// List of entity IDs to fetch
    pub ids: Vec<Arc<str>>,
}

impl ReportHandler for GetItemsByTypeAndIds {
    type Output = Vec<serde_json::Value>;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        // TODO(ts): Implement dynamic type lookup once we have entity registry
        // For now, return empty - this requires runtime type resolution
        Cell::new(Arc::new(Vec::new())).lock()
    }
}

/// Report that fetches immediate child entities of a parent.
#[myko_macros::myko_report(Vec<ItemStub>)]
pub struct ChildEntities {
    pub parent_type: String,
    pub parent_id: Arc<str>,
}

impl ReportHandler for ChildEntities {
    type Output = Vec<ItemStub>;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        // TODO(ts): Implement using relationship manager
        // This requires querying the relationship graph for direct children
        Cell::new(Arc::new(Vec::new())).lock()
    }
}

/// Report that recursively fetches all child entities of a parent.
#[myko_macros::myko_report(Vec<ItemStub>)]
pub struct FullChildEntities {
    pub parent_type: String,
    pub parent_id: Arc<str>,
}

impl ReportHandler for FullChildEntities {
    type Output = Vec<ItemStub>;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        // TODO(ts): Implement recursive traversal using relationship manager
        Cell::new(Arc::new(Vec::new())).lock()
    }
}

/// Report that fetches all-time child entities (including deleted).
#[myko_macros::myko_report(Vec<ItemStub>)]
pub struct ChildEntitiesAllTime {
    pub parent_type: String,
    pub parent_id: Arc<str>,
}

impl ReportHandler for ChildEntitiesAllTime {
    type Output = Vec<ItemStub>;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        // TODO(ts): Implement with historical event store query
        Cell::new(Arc::new(Vec::new())).lock()
    }
}

/// Report that computes the difference between entity snapshots.
#[myko_macros::myko_report(EntitySnapshotDifferenceData)]
pub struct EntitySnapshotDifference {
    pub parent_type: String,
    pub parent_id: Arc<str>,
}

impl ReportHandler for EntitySnapshotDifference {
    type Output = EntitySnapshotDifferenceData;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        // TODO(ts): Implement snapshot comparison
        Cell::new(Arc::new(EntitySnapshotDifferenceData::default())).lock()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Logging Support
// ─────────────────────────────────────────────────────────────────────────────

/// Log level enum for controlling logging verbosity.
#[derive(Clone, Debug, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
#[ts(export)]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Verbose,
}

/// Report that returns the list of available logger names.
#[myko_macros::myko_report(Vec<String>)]
pub struct Loggers {}

impl ReportHandler for Loggers {
    type Output = Vec<String>;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        // TODO(ts): Integrate with tracing subscriber to list available targets
        Cell::new(Arc::new(vec![
            "myko".to_string(),
            "myko::server".to_string(),
            "myko::query".to_string(),
            "myko::command".to_string(),
            "myko::report".to_string(),
        ]))
        .lock()
    }
}

/// Report that returns the current log level for a server.
#[myko_macros::myko_report(LogLevel)]
pub struct ServerLogLevel {
    pub server_id: Arc<str>,
}

impl ReportHandler for ServerLogLevel {
    type Output = LogLevel;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        // TODO(ts): Query actual log level from tracing config
        Cell::new(Arc::new(LogLevel::Info)).lock()
    }
}

/// Command to set the log level for a server.
#[myko_macros::myko_command(bool)]
pub struct SetLogLevel {
    pub server_id: Arc<str>,
    pub level: LogLevel,
}

impl crate::command::CommandHandler for SetLogLevel {
    fn execute(
        self,
        _ctx: crate::command::CommandContext,
    ) -> Result<bool, crate::command::CommandError> {
        // TODO(ts): Implement dynamic log level adjustment
        // This requires integration with tracing-subscriber's reload layer
        Ok(true)
    }
}

/// Report that checks whether a peer server client is currently connected.
/// Returns ping in milliseconds when available, otherwise `-1`.
#[myko_macros::myko_report(i64)]
pub struct PeerAlive {
    pub peer_id: Arc<str>,
}

#[cfg(not(target_arch = "wasm32"))]
impl ReportHandler for PeerAlive {
    type Output = i64;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let peer_id = self.peer_id.clone();
        let report_ctx = ctx.clone();
        ctx.peer_clients_tick().switch_map(move |_| {
            let Some(peer_client) = report_ctx.peer_client(peer_id.as_ref()) else {
                return Cell::new(Arc::new(-1)).lock();
            };

            peer_client
                .ping_ms()
                .clone()
                .map(|ping_ms| {
                    Arc::new(ping_ms.map_or(-1, |ms| i64::try_from(ms).unwrap_or(i64::MAX)))
                })
                .materialize()
        })
    }
}

#[cfg(target_arch = "wasm32")]
impl ReportHandler for PeerAlive {
    type Output = i64;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        Cell::new(Arc::new(-1)).lock()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event History Support
// ─────────────────────────────────────────────────────────────────────────────

/// Container for an event with associated metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct EventContainer {
    pub id: Arc<str>,
    pub event: crate::wire::MEvent,
}

/// Report that returns events for a specific transaction.
#[myko_macros::myko_report(Vec<MEvent>)]
pub struct EventsForTransaction {
    pub transaction_id: String,
}

impl ReportHandler for EventsForTransaction {
    type Output = Vec<crate::wire::MEvent>;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        // TODO(ts): Query event store by transaction ID
        Cell::new(Arc::new(Vec::new())).lock()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Import/Export Commands
// ─────────────────────────────────────────────────────────────────────────────

/// Command to import items into the system as SET events, and optionally
/// delete items as DEL events. Returns the total number of events applied.
///
/// Used by the entity tree import/export system to apply diffs.
#[myko_macros::myko_command(usize)]
pub struct ImportItems {
    /// Items to SET (add or update).
    pub items: Vec<WrappedItem>,
    /// Items to DEL (remove). Only the shallowest removed entities should be
    /// included — the relationship manager cascades deletions to children.
    #[serde(default)]
    pub delete_items: Vec<WrappedItem>,
}

impl crate::command::CommandHandler for ImportItems {
    fn execute(
        self,
        ctx: crate::command::CommandContext,
    ) -> Result<usize, crate::command::CommandError> {
        use crate::wire::{MEvent, MEventType};

        let tx: std::sync::Arc<str> = ctx.req.tx.clone();
        let source_id: Option<std::sync::Arc<str>> =
            Some(std::sync::Arc::from(ctx.req.host_id.to_string()));
        let created_at: std::sync::Arc<str> = std::sync::Arc::from(ctx.created_at());

        let mut events =
            Vec::with_capacity(self.items.len().saturating_add(self.delete_items.len()));

        for wrapped in &self.items {
            events.push(MEvent {
                item: wrapped.item.clone(),
                change_type: MEventType::SET,
                item_type: wrapped.item_type.clone(),
                created_at: created_at.clone(),
                tx: tx.clone(),
                source_id: source_id.clone(),
            });
        }

        for wrapped in &self.delete_items {
            events.push(MEvent {
                item: wrapped.item.clone(),
                change_type: MEventType::DEL,
                item_type: wrapped.item_type.clone(),
                created_at: created_at.clone(),
                tx: tx.clone(),
                source_id: source_id.clone(),
            });
        }

        ctx.emit_event_batch(events)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Persist Health
// ─────────────────────────────────────────────────────────────────────────────

/// Health snapshot for the persist subsystem.
#[myko_macros::myko_report_output]
pub struct PersistHealthStatus {
    /// Events queued but not yet written.
    pub queued: u64,
    /// Lifetime successful writes.
    pub total_persisted: u64,
    /// Lifetime failed writes.
    pub total_errors: u64,
    /// Consecutive failures since last success.
    pub consecutive_errors: u64,
    /// Most recent error message, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub last_error: Option<String>,
    /// Whether the persister is currently healthy (no consecutive errors).
    pub healthy: bool,
    /// Writes per second over a sliding window.
    pub writes_per_second: f64,
}

/// Report that returns the current health of the persist subsystem.
/// Polls every 500ms and deduplicates, so subscribers only receive updates
/// when values actually change.
#[myko_macros::myko_report(PersistHealthStatus)]
pub struct GetPersistHealth {}

#[cfg(not(target_arch = "wasm32"))]
impl ReportHandler for GetPersistHealth {
    type Output = PersistHealthStatus;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let health = ctx.persist_health();
        interval(Duration::from_millis(500))
            .map(move |_tick| {
                let queued = health.queued.load(Ordering::Relaxed);
                let total_persisted = health.total_persisted.load(Ordering::Relaxed);
                let total_errors = health.total_errors.load(Ordering::Relaxed);
                let consecutive_errors = health.consecutive_errors.load(Ordering::Relaxed);
                let last_error = health
                    .last_error
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let writes_per_second = health.writes_per_second();
                Arc::new(PersistHealthStatus {
                    queued,
                    total_persisted,
                    total_errors,
                    consecutive_errors,
                    last_error,
                    healthy: consecutive_errors == 0,
                    writes_per_second,
                })
            })
            .materialize()
            .deduped()
    }
}

#[cfg(target_arch = "wasm32")]
impl ReportHandler for GetPersistHealth {
    type Output = PersistHealthStatus;

    fn compute(&self, _ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        Cell::new(Arc::new(PersistHealthStatus {
            queued: 0,
            total_persisted: 0,
            total_errors: 0,
            consecutive_errors: 0,
            last_error: None,
            healthy: true,
            writes_per_second: 0.0,
        }))
        .lock()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ts-rs Export Registrations
// ─────────────────────────────────────────────────────────────────────────────

// Register output types for ts-rs export
crate::register_ts_export!(
    ItemStub,
    EntitySnapshotDifferenceData,
    LogLevel,
    EventContainer,
    PersistHealthStatus
);
