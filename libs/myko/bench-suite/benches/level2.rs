//! The timed full-scale matrix. Built only with `--features bench`.
//!
//! ```sh
//! cargo bench -p myko-bench-suite --features bench --bench level2            # whole matrix
//! cargo bench -p myko-bench-suite --features bench --bench level2 -- sweep   # labels containing "sweep"
//! ```
//!
//! Every data point runs in a fresh process, because hyphae's reactive world
//! is process-global. Each run appends its records, one JSON object per
//! line, to `target/bench-suite/results-<commit>.jsonl`. `MYKO_BENCH_RUNS`
//! sets the runs per data point (default 5) and `MYKO_BENCH_OUT` the file.

#![allow(
    clippy::panic,
    clippy::exit,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::as_conversions
)]

use std::{
    io::Write as _,
    path::PathBuf,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use myko_bench_suite::{harness::commit, scenarios::Args};

const MATRIX: &str = include_str!("../matrix.txt");

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(at) = args.iter().position(|arg| arg == "--one") {
        let Some(scenario) = args.get(at + 1) else {
            panic!("--one needs a scenario");
        };
        let rest = args.get(at + 2..).unwrap_or_default().iter().cloned();
        assert!(
            myko_bench_suite::run(scenario, &Args::parse(rest)),
            "unknown scenario {scenario}"
        );
        return;
    }
    let filters: Vec<&String> = args.iter().filter(|arg| !arg.starts_with("--")).collect();
    let runs: usize = std::env::var("MYKO_BENCH_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let out = std::env::var("MYKO_BENCH_OUT").map_or_else(
        |_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../target/bench-suite")
                .join(format!("results-{}.jsonl", commit()))
        },
        PathBuf::from,
    );
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir).unwrap_or_else(|error| panic!("{}: {error}", dir.display()));
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out)
        .unwrap_or_else(|error| panic!("{}: {error}", out.display()));
    let exe = std::env::current_exe().unwrap_or_else(|error| panic!("{error}"));

    for line in MATRIX
        .lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
    {
        let mut fields = line.split_whitespace();
        let (Some(label), Some(limit)) = (fields.next(), fields.next()) else {
            continue;
        };
        if !filters.is_empty() && !filters.iter().any(|filter| label.contains(filter.as_str())) {
            continue;
        }
        let limit = Duration::from_secs(limit.parse().unwrap_or(300));
        let scenario: Vec<&str> = fields.collect();
        for run in 1..=runs {
            let started = Instant::now();
            let mut child = Command::new(&exe)
                .arg("--one")
                .args(&scenario)
                .env("MYKO_BENCH_COMMIT", commit())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap_or_else(|error| panic!("spawn: {error}"));
            // The child's output is small, so the pipe never fills before it exits.
            let timed_out = loop {
                match child.try_wait() {
                    Ok(Some(_)) => break false,
                    Ok(None) if started.elapsed() > limit => {
                        let _ = child.kill();
                        break true;
                    }
                    Ok(None) => std::thread::sleep(Duration::from_millis(50)),
                    Err(error) => panic!("wait: {error}"),
                }
            };
            let output = child
                .wait_with_output()
                .unwrap_or_else(|error| panic!("{error}"));
            let stdout = String::from_utf8_lossy(&output.stdout);
            let mut records = 0;
            for record in stdout
                .lines()
                .filter_map(|line| line.strip_prefix("RESULT {"))
            {
                writeln!(file, "{{\"label\":\"{label}\",\"run\":{run},{record}")
                    .unwrap_or_else(|error| panic!("{error}"));
                records += 1;
            }
            if timed_out || !output.status.success() || records == 0 {
                writeln!(
                    file,
                    "{{\"label\":\"{label}\",\"run\":{run},\"failed_run\":true,\"commit\":\"{}\",\"timed_out\":{timed_out}}}",
                    commit()
                )
                .unwrap_or_else(|error| panic!("{error}"));
            }
            println!(
                "{label} run {run}: {records} records in {:.1}s{}",
                started.elapsed().as_secs_f64(),
                if timed_out {
                    " (hit the time limit)"
                } else {
                    ""
                }
            );
        }
    }
    println!("results: {}", out.display());
}
