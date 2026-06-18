//! Last-writer-wins (LWW) stamps and tombstones for convergent apply.
//!
//! Each entity carries a per-`(type, id)` [`LwwStamp`] kept as side metadata in
//! the [`StoreRegistry`](super::StoreRegistry). On every SET/DEL the registry
//! compares the incoming stamp against the stored one and only mutates the
//! reactive store when the incoming write **strictly wins**. A deleted stamp is
//! a *tombstone*: it stays behind after the entity is removed so a stale SET
//! can't resurrect it, and so two nodes converge regardless of delivery order.
//!
//! The win order is `(timestamp, source_id)`:
//! - `timestamp` — `created_at` parsed to nanoseconds (RFC3339 UTC, wall-clock).
//! - `source_id` — the origin host, as a deterministic tiebreaker for genuinely
//!   concurrent writes (same instant, different origins).
//!
//! Equal stamps do **not** win, so re-delivering the same event (gossip ∪
//! anti-entropy) is a no-op. A content-hash tiebreaker (for the
//! same-origin-same-instant edge and for anti-entropy) is intentionally left for
//! a later phase to keep the write path allocation-free.

use std::sync::Arc;

/// Per-entity LWW metadata. Also serves as a tombstone when `deleted` is set.
#[derive(Clone, Debug)]
pub struct LwwStamp {
    /// The wire `created_at` (RFC3339 UTC). Retained for tombstones / future
    /// anti-entropy; comparison uses the parsed `ts`.
    pub created_at: Arc<str>,
    /// `created_at` parsed to nanoseconds since the epoch (0 if unparseable).
    ts: i64,
    /// Origin host id — deterministic tiebreaker. `None` sorts before `Some`.
    pub source_id: Option<Arc<str>>,
    /// True when this stamp is a tombstone (the entity was deleted).
    pub deleted: bool,
}

impl LwwStamp {
    /// Build a stamp from a wire `created_at` + `source_id`.
    pub fn new(created_at: &str, source_id: Option<&str>, deleted: bool) -> Self {
        Self {
            created_at: created_at.into(),
            ts: parse_ts(created_at),
            source_id: source_id.map(Arc::from),
            deleted,
        }
    }

    /// The deterministic total-order key.
    fn key(&self) -> (i64, Option<&str>) {
        (self.ts, self.source_id.as_deref())
    }

    /// Does `self` **strictly** win over `other`? Equal stamps return `false`
    /// so idempotent re-delivery is a no-op.
    pub fn wins_over(&self, other: &LwwStamp) -> bool {
        self.key() > other.key()
    }
}

fn parse_ts(s: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .and_then(|dt| dt.timestamp_nanos_opt())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(created_at: &str, source: &str) -> LwwStamp {
        LwwStamp::new(created_at, Some(source), false)
    }

    #[test]
    fn newer_timestamp_wins() {
        let older = stamp("2026-06-18T00:00:00Z", "a");
        let newer = stamp("2026-06-18T00:00:01Z", "a");
        assert!(newer.wins_over(&older));
        assert!(!older.wins_over(&newer));
    }

    #[test]
    fn equal_stamp_does_not_win_idempotent() {
        let a = stamp("2026-06-18T00:00:00Z", "a");
        let b = stamp("2026-06-18T00:00:00Z", "a");
        assert!(!a.wins_over(&b));
        assert!(!b.wins_over(&a));
    }

    #[test]
    fn same_instant_breaks_tie_on_source_deterministically() {
        let a = stamp("2026-06-18T00:00:00Z", "a");
        let b = stamp("2026-06-18T00:00:00Z", "b");
        // Exactly one direction wins, and it's stable.
        assert!(b.wins_over(&a));
        assert!(!a.wins_over(&b));
    }

    #[test]
    fn fractional_seconds_compare_correctly() {
        let lo = stamp("2026-06-18T00:00:00.100Z", "a");
        let hi = stamp("2026-06-18T00:00:00.200Z", "a");
        assert!(hi.wins_over(&lo));
        assert!(!lo.wins_over(&hi));
    }

    #[test]
    fn none_source_sorts_before_some() {
        let none = LwwStamp::new("2026-06-18T00:00:00Z", None, false);
        let some = LwwStamp::new("2026-06-18T00:00:00Z", Some("a"), false);
        assert!(some.wins_over(&none));
        assert!(!none.wins_over(&some));
    }
}
