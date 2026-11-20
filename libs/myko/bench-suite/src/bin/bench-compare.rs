//! Compares a results file from `benches/level2.rs` with the checked-in
//! baseline, or writes a new baseline.
//!
//! ```sh
//! bench-compare target/bench-suite/results-<commit>.jsonl
//! bench-compare target/bench-suite/results-<commit>.jsonl --write-baseline
//! ```
//!
//! A data point regresses when the median of its runs is worse than the
//! baseline median by more than the metric's tolerance, when a correctness
//! check fails that the baseline does not list as known, or when a run did
//! not finish. The exit status is 1 if anything regressed.
//!
//! The tolerance of a metric is fixed when the baseline is written: twice
//! the relative range of the baseline runs, and never less than 25%. Twice
//! the range keeps a rerun on the same commit from tripping the check. The
//! 25% floor covers what a busy shared machine adds to a metric whose
//! baseline runs happened to agree closely.

#![allow(
    clippy::panic,
    clippy::exit,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::as_conversions,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Metrics the check tracks. All are costs: lower is better.
const TRACKED: &[&str] = &[
    "tick_service_p50_us",
    "per_row_service_us",
    "probe_other_p50_us",
    "probe_same_p50_us",
    "hot_call_p50_us",
    "hot_latency_p50_us",
    "spread_latency_p50_us",
    "connect_cycle_p50_us",
    "latency_p50_us",
    "bulk_us_per_chain",
    "reparent_us_per_row",
    "cascade_us_per_row",
    "ingest_us_per_event",
    "mutation_us",
    "us_per_row",
    "load_total_s",
    "state_ratio",
    "clones_per_changed_row",
    "serializes_per_subscribed_row",
    "call_us_per_row",
    "done_us_per_row",
];
const SUBKEYS: &[&str] = &["queries", "depth", "burst", "population"];
const MIN_TOLERANCE: f64 = 0.25;
/// Differences below this many units are never a regression: a 2 µs change
/// on a 10 µs metric is timer and scheduler noise.
const NOISE_FLOOR: f64 = 2.0;

#[derive(Serialize, Deserialize)]
struct Metric {
    median: f64,
    min: f64,
    max: f64,
    tolerance: f64,
}

#[derive(Serialize, Deserialize, Default)]
struct Point {
    runs: usize,
    metrics: BTreeMap<String, Metric>,
    /// Checks that fail on the baseline commit. They are reported, not gated.
    known_failing_checks: BTreeSet<String>,
    /// True when no baseline run finished. Finishing is then an improvement.
    #[serde(default)]
    did_not_finish: bool,
}

#[derive(Serialize, Deserialize)]
struct Baseline {
    commit: String,
    note: String,
    points: BTreeMap<String, Point>,
}

#[derive(Default)]
struct Observed {
    runs: BTreeSet<u64>,
    failed_runs: usize,
    values: BTreeMap<String, Vec<f64>>,
    failed_checks: BTreeSet<String>,
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        f64::midpoint(sorted[mid - 1], sorted[mid])
    }
}

fn read(path: &PathBuf) -> (String, BTreeMap<String, Observed>) {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let mut points: BTreeMap<String, Observed> = BTreeMap::new();
    let mut commit = String::from("unknown");
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let record: Value =
            serde_json::from_str(line).unwrap_or_else(|error| panic!("bad record: {error}"));
        let label = record["label"].as_str().unwrap_or("unlabelled").to_owned();
        if let Some(found) = record["commit"].as_str() {
            found.clone_into(&mut commit);
        }
        if record["failed_run"].as_bool() == Some(true) {
            // A failed run has no parameters, so it counts against every
            // data point of its label.
            for (key, point) in &mut points {
                if key.split('@').next() == Some(label.as_str()) {
                    point.failed_runs += 1;
                }
            }
            points.entry(label).or_default().failed_runs += 1;
            continue;
        }
        let sub = SUBKEYS
            .iter()
            .find_map(|key| record["params"].get(*key))
            .map_or_else(String::new, |value| format!("@{value}"));
        let point = points.entry(format!("{label}{sub}")).or_default();
        point.runs.insert(record["run"].as_u64().unwrap_or(0));
        if let Some(metrics) = record["metrics"].as_object() {
            for name in TRACKED {
                if let Some(value) = metrics.get(*name).and_then(Value::as_f64) {
                    point
                        .values
                        .entry((*name).to_owned())
                        .or_default()
                        .push(value);
                }
            }
        }
        if let Some(checks) = record["checks"].as_object() {
            for (name, ok) in checks {
                if ok.as_bool() != Some(true) {
                    point.failed_checks.insert(name.clone());
                }
            }
        }
    }
    // A label whose every record carries a sub-key keeps no bare entry.
    points.retain(|key, point| !point.runs.is_empty() || !points_has_sub(key, &text));
    (commit, points)
}

fn points_has_sub(label: &str, text: &str) -> bool {
    let needle = format!("\"label\":\"{label}\"");
    text.lines()
        .any(|line| line.contains(&needle) && !line.contains("\"failed_run\":true"))
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let write = args.iter().any(|arg| arg == "--write-baseline");
    let mut paths = args.iter().filter(|arg| !arg.starts_with("--"));
    let Some(results) = paths.next().map(PathBuf::from) else {
        eprintln!("usage: bench-compare <results.jsonl> [baseline.json] [--write-baseline]");
        std::process::exit(2);
    };
    let baseline_path = paths.next().map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("baseline.json"),
        PathBuf::from,
    );
    let (commit, observed) = read(&results);

    if write {
        let points = observed
            .iter()
            .map(|(key, point)| {
                let metrics = point
                    .values
                    .iter()
                    .map(|(name, values)| {
                        let (min, max) = values
                            .iter()
                            .fold((f64::MAX, f64::MIN), |(lo, hi), v| (lo.min(*v), hi.max(*v)));
                        let mid = median(values);
                        let spread = if mid > 0.0 { (max - min) / mid } else { 0.0 };
                        let tolerance = (2.0 * spread).max(MIN_TOLERANCE);
                        (
                            name.clone(),
                            Metric {
                                median: mid,
                                min,
                                max,
                                tolerance: (tolerance * 1000.0).round() / 1000.0,
                            },
                        )
                    })
                    .collect();
                (
                    key.clone(),
                    Point {
                        runs: point.runs.len(),
                        metrics,
                        known_failing_checks: point.failed_checks.clone(),
                        did_not_finish: point.runs.is_empty(),
                    },
                )
            })
            .collect();
        let baseline = Baseline {
            commit,
            note: "Medians of the runs in one results file. Rewrite with `bench-compare <results> --write-baseline`.".into(),
            points,
        };
        let text =
            serde_json::to_string_pretty(&baseline).unwrap_or_else(|error| panic!("{error}"));
        std::fs::write(&baseline_path, text + "\n")
            .unwrap_or_else(|error| panic!("{}: {error}", baseline_path.display()));
        println!(
            "wrote {} ({} data points)",
            baseline_path.display(),
            baseline.points.len()
        );
        return;
    }

    let text = std::fs::read_to_string(&baseline_path)
        .unwrap_or_else(|error| panic!("{}: {error}", baseline_path.display()));
    let baseline: Baseline =
        serde_json::from_str(&text).unwrap_or_else(|error| panic!("baseline: {error}"));
    println!("baseline {} against {commit}", baseline.commit);
    let (mut regressions, mut improvements, mut compared) = (Vec::new(), Vec::new(), 0usize);
    for (key, point) in &observed {
        let Some(base) = baseline.points.get(key) else {
            println!("new data point, not in the baseline: {key}");
            continue;
        };
        if point.runs.is_empty() || point.failed_runs > 0 {
            if base.did_not_finish {
                continue;
            }
            regressions.push(format!(
                "{key}: {} run(s) did not finish",
                point.failed_runs.max(1)
            ));
            continue;
        }
        if base.did_not_finish {
            improvements.push(format!("{key}: finishes now, did not in the baseline"));
        }
        for check in point.failed_checks.difference(&base.known_failing_checks) {
            regressions.push(format!("{key}: check `{check}` fails"));
        }
        for check in base.known_failing_checks.difference(&point.failed_checks) {
            improvements.push(format!("{key}: check `{check}` passes now"));
        }
        for (name, values) in &point.values {
            let Some(metric) = base.metrics.get(name) else {
                continue;
            };
            compared += 1;
            let now = median(values);
            let limit = metric.median * (1.0 + metric.tolerance);
            if now > limit && now - metric.median > NOISE_FLOOR {
                regressions.push(format!(
                    "{key} {name}: {now:.2} against baseline {:.2} (+{:.0}%, tolerance {:.0}%, {} runs)",
                    metric.median,
                    (now / metric.median - 1.0) * 100.0,
                    metric.tolerance * 100.0,
                    values.len()
                ));
            } else if now < metric.median / (1.0 + metric.tolerance)
                && metric.median - now > NOISE_FLOOR
            {
                improvements.push(format!(
                    "{key} {name}: {now:.2} against baseline {:.2} ({:.0}%)",
                    metric.median,
                    (now / metric.median - 1.0) * 100.0
                ));
            }
        }
    }
    for key in baseline
        .points
        .keys()
        .filter(|key| !observed.contains_key(*key))
    {
        println!("not run: {key}");
    }
    println!("{compared} metrics compared");
    for line in &improvements {
        println!("improved   {line}");
    }
    for line in &regressions {
        println!("REGRESSED  {line}");
    }
    if !improvements.is_empty() && regressions.is_empty() {
        println!("improvements only: re-baseline to lock them in");
    }
    if !regressions.is_empty() {
        std::process::exit(1);
    }
}
