//! myko level-2 benchmark and stress suite.
//!
//! Scenarios drive myko's real surfaces (`#[myko_item]` stores, generated
//! queries, views, reports, relationships and `ClientSession`) with
//! rship-shaped entities at the scale of the rship load test. Each scenario
//! checks its own correctness and emits one record per data point.
//!
//! The same scenarios serve three uses: `tests/scenarios.rs` runs them at
//! reduced scale as ordinary tests, `benches/level2.rs` runs the timed matrix
//! behind the `bench` feature, and `bench-compare` checks a run against
//! `baseline.json`. See `README.md`.

// The harness measures and reports. It casts between integer and float
// freely, indexes tables it sized itself, and treats a failed setup step as
// fatal, so the workspace's panic and conversion lints do not fit it.
#![allow(
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::significant_drop_tightening,
    clippy::derive_partial_eq_without_eq,
    clippy::module_name_repetitions,
    clippy::missing_const_for_fn,
    clippy::return_self_not_must_use,
    clippy::suboptimal_flops,
    clippy::similar_names,
    clippy::unchecked_time_subtraction,
    clippy::wildcard_imports,
    clippy::too_long_first_doc_paragraph,
    clippy::manual_is_multiple_of,
    clippy::unreadable_literal,
    clippy::duration_suboptimal_units,
    clippy::explicit_counter_loop,
    clippy::collapsible_if,
    clippy::unused_self,
    clippy::semicolon_if_nothing_returned,
    // Client sessions are held in a Vec only to keep their subscriptions alive.
    clippy::collection_is_never_read
)]

pub mod compat;
pub mod entities;
pub mod harness;
pub mod payload;
pub mod reports;
pub mod scenarios;
pub mod tick;
pub mod views;

/// Runs one scenario by name. Returns false for an unknown name.
pub fn run(scenario: &str, args: &scenarios::Args) -> bool {
    match scenario {
        "m1" | "m2" | "m3" | "m5" | "m7" | "payload" | "overload" => {
            scenarios::tick_scenario(scenario, args)
        }
        "idle" => scenarios::idle(args),
        "sweep" => scenarios::sweep(args),
        "churn_cost" => scenarios::churn_cost(args),
        "chain" => scenarios::chain(args),
        "belongs_to" => scenarios::belongs_to(args),
        "bursts" => scenarios::bursts(args),
        "atomic" => scenarios::atomic(args),
        "shared_params" => scenarios::shared_params(args),
        "setup" => scenarios::setup(args),
        "write_cost" => scenarios::write_cost(args),
        "dedup" => scenarios::dedup(args),
        _ => return false,
    }
    true
}
