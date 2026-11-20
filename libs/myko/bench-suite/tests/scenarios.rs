//! Every scenario at reduced scale, as an ordinary test. A test fails when
//! any correctness check inside the scenario fails: a dropped row, a
//! sequence gap, a value out of its writer's order, a wrong value, an
//! inexact cascade. Timings are not asserted here; `benches/level2.rs` and
//! `bench-compare` cover those.

#![allow(clippy::panic, clippy::missing_panics_doc)]

use myko_bench_suite::{
    compat,
    harness::{Record, take_records},
    scenarios::Args,
};

/// hyphae's reactive world is process-global, so scenarios never overlap.
static WORLD: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

fn run(scenario: &str, args: &str) -> Vec<Record> {
    let _world = WORLD.lock();
    assert!(
        myko_bench_suite::run(scenario, &Args::of(args)),
        "unknown scenario {scenario}"
    );
    let records = take_records();
    assert!(!records.is_empty(), "{scenario} emitted no record");
    records
}

fn assert_checks(records: &[Record], known_failing: &[&str]) {
    for record in records {
        let failed: Vec<&str> = record
            .failed_checks()
            .into_iter()
            .filter(|name| !known_failing.contains(name))
            .collect();
        assert!(
            failed.is_empty(),
            "{} {:?}: failed checks {failed:?}",
            record.scenario,
            record.params
        );
    }
}

#[test]
fn setup_delivers_and_stores_every_row() {
    assert_checks(&run("setup", "scale=0.02"), &[]);
    assert_checks(&run("setup", "scale=0.005 batch=1"), &[]);
}

#[test]
fn tick_reaches_every_filtered_query_subscriber() {
    assert_checks(&run("m1", "scale=0.02 ticks=10 warmup=2"), &[]);
}

#[test]
fn tick_reaches_every_view_subscriber() {
    assert_checks(&run("m2", "surface=view scale=0.02 ticks=10 warmup=2"), &[]);
}

#[test]
fn tick_reaches_every_report_subscriber() {
    assert_checks(
        &run("m3", "surface=report scale=0.02 ticks=10 warmup=2"),
        &[],
    );
}

#[test]
fn tick_holds_under_subscription_churn() {
    assert_checks(&run("m5", "scale=0.05 ticks=20 warmup=2 churn=100"), &[]);
}

#[test]
fn tick_delivers_over_cbor_and_with_counting() {
    assert_checks(&run("m7", "scale=0.02 ticks=6 warmup=1 wire=cbor"), &[]);
    assert_checks(
        &run(
            "payload",
            "scale=0.02 ticks=4 warmup=1 count=1 shared=1 probe_hz=0",
        ),
        &[],
    );
}

#[test]
fn idle_probe_reads_its_own_writes() {
    assert_checks(&run("idle", "scale=0.02 seconds=1"), &[]);
}

#[test]
fn a_write_reaches_only_the_query_it_matches() {
    for kind in ["by_ids", "by_id_report", "fk", "report", "scan"] {
        assert_checks(
            &run("sweep", &format!("kind={kind} scale=0.01 writes=20")),
            &[],
        );
    }
}

#[test]
fn connect_and_disconnect_leave_no_live_cache_entry() {
    assert_checks(&run("churn_cost", "scale=0.01 cycles=10"), &[]);
}

#[test]
fn report_chain_delivers_at_every_depth() {
    assert_checks(&run("chain", "scale=0.02 writes=10"), &[]);
}

#[test]
fn belongs_to_index_and_cascades_are_exact() {
    assert_checks(&run("belongs_to", "scale=0.05 batches=4"), &[]);
}

#[test]
fn event_bursts_keep_order_and_the_latest_value() {
    // Without `#[myko_sequential]` a burst on one key settles together and
    // the subscriber sees only its last event.
    let known: &[&str] = if compat::HAS_SEQUENTIAL {
        &[]
    } else {
        &["every_event_delivered"]
    };
    assert_checks(&run("bursts", "scale=0.05 bursts=10"), known);
}

#[test]
#[ignore = "gate L2-5: a three-way join shows part of a multi-type mutation on this engine"]
fn a_multi_type_mutation_is_never_seen_in_part() {
    assert_checks(&run("atomic", "scale=0.05 mutations=40 group=20"), &[]);
    assert_checks(
        &run("atomic", "scale=0.05 mutations=40 group=20 path=events"),
        &[],
    );
}

#[test]
fn a_multi_type_mutation_converges() {
    assert_checks(
        &run("atomic", "scale=0.05 mutations=40 group=20"),
        &["no_partial_mutation_visible"],
    );
}

#[test]
fn identical_subscriptions_each_get_the_full_result() {
    assert_checks(&run("shared_params", "scale=0.02"), &[]);
}

#[test]
fn write_cost_delivers_every_row() {
    assert_checks(&run("write_cost", "scale=0.02 rounds=3 subs=2"), &[]);
}
