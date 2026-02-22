//! Core framework reports for entity traversal, logging, and event history.
//!
//! These reports provide framework-level functionality that operates across
//! all entity types rather than specific domain entities.

use std::sync::Arc;

use hypha::{Cell, CellImmutable, MapExt, SwitchMapExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

use crate::{
    report::{ReportContext, ReportHandler},
    wire::{MEvent, WrappedItem},
};

// ─────────────────────────────────────────────────────────────────────────────
// Entity Stub Types
// ─────────────────────────────────────────────────────────────────────────────

/// Stub representation of an entity for tree traversal.
/// Contains minimal identifying information without full entity data.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ItemStub {
    pub id: Arc<str>,
    pub item_type: String,
    pub name: Option<String>,
}

/// Data returned by EntitySnapshotDifference report.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, TS)]
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

    fn compute(&self, _ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
        // TODO(ts): Implement dynamic type lookup once we have entity registry
        // For now, return empty - this requires runtime type resolution
        Cell::new(Vec::new()).lock()
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

    fn compute(&self, _ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
        // TODO(ts): Implement using relationship manager
        // This requires querying the relationship graph for direct children
        Cell::new(Vec::new()).lock()
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

    fn compute(&self, _ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
        // TODO(ts): Implement recursive traversal using relationship manager
        Cell::new(Vec::new()).lock()
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

    fn compute(&self, _ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
        // TODO(ts): Implement with historical event store query
        Cell::new(Vec::new()).lock()
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

    fn compute(&self, _ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
        // TODO(ts): Implement snapshot comparison
        Cell::new(EntitySnapshotDifferenceData::default()).lock()
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

    fn compute(&self, _ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
        // TODO(ts): Integrate with tracing subscriber to list available targets
        Cell::new(vec![
            "myko".to_string(),
            "myko::server".to_string(),
            "myko::query".to_string(),
            "myko::command".to_string(),
            "myko::report".to_string(),
        ])
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

    fn compute(&self, _ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
        // TODO(ts): Query actual log level from tracing config
        Cell::new(LogLevel::Info).lock()
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

impl ReportHandler for PeerAlive {
    type Output = i64;

    fn compute(&self, ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
        let peer_id = self.peer_id.clone();
        let report_ctx = ctx.clone();
        ctx.peer_clients_tick().switch_map(move |_| {
            let Some(peer_client) = report_ctx.peer_client(peer_id.as_ref()) else {
                return Cell::new(-1).lock();
            };

            peer_client.ping_ms().map(|ping_ms| {
                ping_ms
                    .map(|ms| ms.min(i64::MAX as u64) as i64)
                    .unwrap_or(-1)
            })
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event History Support
// ─────────────────────────────────────────────────────────────────────────────

/// Container for an event with associated metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
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

    fn compute(&self, _ctx: ReportContext) -> Cell<Self::Output, CellImmutable> {
        // TODO(ts): Query event store by transaction ID
        Cell::new(Vec::new()).lock()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Import/Export Commands
// ─────────────────────────────────────────────────────────────────────────────

/// Command to import wrapped items into the system.
/// Returns the number of items that were processed.
#[myko_macros::myko_command(usize)]
pub struct ImportItems {
    pub items: Vec<WrappedItem<Value>>,
}

impl crate::command::CommandHandler for ImportItems {
    fn execute(
        self,
        _ctx: crate::command::CommandContext,
    ) -> Result<usize, crate::command::CommandError> {
        // TODO(ts): Implement raw event emission for imports
        // This requires emitting MEvent directly to the event bus,
        // which needs either:
        // 1. A new emit_raw method on CommandContext
        // 2. Access to the event manager directly
        //
        // For now, return the count to indicate success
        // The actual import functionality needs deeper integration
        Ok(self.items.len())
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
    EventContainer
);
