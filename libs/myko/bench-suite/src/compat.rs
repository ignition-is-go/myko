//! The one place that depends on what this myko version offers.
//!
//! `HAS_SEQUENTIAL` records whether `#[myko_sequential]` exists on this
//! branch. It does not: `apply_event_batch` applies the events of one key in
//! order for every entity type, so `ingest` needs no per-key split.

use myko::{event::MEvent, server::MykoServerContext};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const HAS_SEQUENTIAL: bool = false;

/// Applies a wire batch the way `WsHandler` does.
pub fn ingest(ctx: &MykoServerContext, events: Vec<MEvent>) -> usize {
    ctx.apply_event_batch(events).unwrap_or(0)
}

/// Live query and report cache entries, for the lifecycle check.
pub fn cache_live(ctx: &MykoServerContext) -> Option<(usize, usize)> {
    Some((ctx.query_cache_live_count(), ctx.report_cache_live_count()))
}

pub fn query_cache_len(ctx: &MykoServerContext) -> Option<usize> {
    Some(ctx.query_cache_len())
}

pub fn sweep(ctx: &MykoServerContext) {
    ctx.sweep_dead_cache_entries();
}

pub mod pulse {
    use myko::prelude::*;

    use crate::entities::{BEmitter, BEmitterId, Blob};

    #[myko_item]
    pub struct BPulse {
        #[belongs_to(BEmitter)]
        pub emitter_id: BEmitterId,
        pub seq: u64,
        pub writer: u32,
        pub data: Blob,
    }
}
