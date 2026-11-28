//! Startup load cost of a server without partitions, from an in-memory log.
//! Run: `cargo test --release -p myko-server --test load_bench -- --ignored --nocapture`
//! `LOAD_BENCH_ROWS` sets the row count (default 1000000).

#![allow(clippy::expect_used, clippy::print_stderr)]

use std::{sync::Arc, time::Instant};

use myko::{
    entities::{
        client::Client,
        server::{Server, ServerId},
    },
    wire::{MEvent, MEventType},
};
use myko_server::MykoServer;

#[test]
#[ignore = "bench; run explicitly with --ignored --nocapture"]
fn load_an_unpartitioned_snapshot() {
    let rows: usize = std::env::var("LOAD_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000_000);
    let server_id = ServerId::from(Arc::<str>::from("bench-server"));
    let parent = MEvent::from_item(
        &Server {
            id: "bench-server".into(),
            version: "1".into(),
            address: "127.0.0.1".into(),
            port: 1,
            started_at: "2026-01-01T00:00:00Z".into(),
        },
        MEventType::SET,
        "log",
    );
    let events: Vec<MEvent> = std::iter::once(parent)
        .chain((0..rows).map(|index| {
            MEvent::from_item(
                &Client {
                    id: format!("client-{index}").into(),
                    server_id: server_id.clone(),
                    address: None,
                    windback: None,
                },
                MEventType::SET,
                "log",
            )
        }))
        .collect();
    let server = MykoServer::builder().build();
    let start = Instant::now();
    let _ = server.hydrate_snapshot(events).expect("hydrate");
    let elapsed = start.elapsed();
    eprintln!(
        "LOAD rows={rows} total={elapsed:?} per_row_ns={}",
        elapsed.as_nanos() / u128::try_from(rows.max(1)).unwrap_or(1)
    );
}
