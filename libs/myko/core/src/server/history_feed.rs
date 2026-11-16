//! Backend-independent, pull-based replay and live delivery of durable history.

use std::{sync::Arc, time::Duration};

use super::{HistoryEntityKey, HistoryEvent};

/// Union of selected entity types and exact entity identities. Empty selects all.
/// Membership is evaluated against persisted events, including deletions.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistoryFilter {
    pub item_types: Vec<Arc<str>>,
    pub entities: Vec<HistoryEntityKey>,
}

/// One durable row. Decode errors retain their position so a consumer can mark
/// its projection incomplete and continue reading subsequent valid rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryFeedRow {
    pub id: i64,
    pub event: Result<HistoryEvent, String>,
}

/// Why the consumer must discard its accumulated projection before replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryResetReason {
    Initial,
    Reconnected,
    LateCommit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryUpdate {
    /// Start a new replay epoch; all previously delivered rows are invalidated.
    Reset(HistoryResetReason),
    /// Bounded batch in increasing row-ID order within the current epoch.
    Rows(Vec<HistoryFeedRow>),
    /// Replay and queued notifications have drained. This is a read position,
    /// not a durable commit cursor: a lower ID may commit later and cause Reset.
    CaughtUp { last_id: i64 },
    /// No changes arrived during this poll.
    Idle,
}

/// Blocking, bounded history reader. Dropping it closes its backend resources;
/// no background producer or unbounded event queue is required.
pub trait HistorySubscription: Send {
    /// Pull the next update. Timeout bounds notification waiting, not database
    /// queries. After an error, polling again retries the connection and emits
    /// Reset before replaying, recovering commits missed while disconnected.
    /// Consumers must handle every Reset, including those following late commits.
    ///
    /// # Errors
    /// Returns backend connection or read failures; malformed rows are reported
    /// individually in Rows instead.
    fn poll(&mut self, timeout: Duration) -> Result<HistoryUpdate, String>;
}
