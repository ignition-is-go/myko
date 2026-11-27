//! Steady-state WebSocket fan-out cost: sampled views, many report
//! subscriptions and inbound event batches driven through the real router and
//! per-connection loop over loopback.
//!
//! Run:
//! `cargo test --release -p myko-server --test ws_fanout_bench -- --ignored --nocapture`
//!
//! Environment (defaults in brackets): `FANOUT_ROWS` [300] rows in the view,
//! `FANOUT_VIEW_CLIENTS` [13] clients each sampling the view at
//! `FANOUT_VIEW_HZ` [60], `FANOUT_REPORT_CLIENTS` [2] clients each holding
//! `FANOUT_REPORTS` [44] report subscriptions, `FANOUT_BATCHES_PER_SEC` [360]
//! inbound batches of `FANOUT_BATCH_EVENTS` [8] events, `FANOUT_SECS` [10]
//! measured seconds, `FANOUT_CBOR` [0] to make every client speak CBOR.
//!
//! The server runs on its own runtime whose threads are named
//! `fanout-server`; clients run on `fanout-client` threads. CPU is read per
//! thread from `/proc/self/task`, so the server figure excludes client work.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::print_stderr,
    clippy::string_slice,
    clippy::unwrap_used,
    reason = "a timing harness reading procfs and printing rates"
)]

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use myko::{
    entities::demo::{DemoStatus, DemoStatusId, DemoTask, DemoTaskId, GetDemoTasksByIds},
    prelude::*,
    report::{ReportContext, ReportHandler},
    wire::{MEvent, MEventType, MykoMessage},
};
use myko_server::{MykoServer, NullPersister, router::route_connection};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

#[myko_report_output]
pub struct FanoutTaskOut {
    pub id: Arc<str>,
    pub title: String,
    pub completed: bool,
}

/// One task's title, recomputed whenever that task changes.
#[myko_report(FanoutTaskOut)]
pub struct FanoutTask {
    pub id: DemoTaskId,
}

impl ReportHandler for FanoutTask {
    type Output = FanoutTaskOut;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let id = self.id.0.clone();
        ctx.query_map(GetDemoTasksByIds {
            ids: vec![self.id.clone()],
        })
        .items()
        .map(move |items| {
            let task = items.first();
            Arc::new(FanoutTaskOut {
                id: id.clone(),
                title: task.map_or_else(String::new, |task| task.title.clone()),
                completed: task.is_some_and(|task| task.completed),
            })
        })
        .materialize()
    }
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn task(index: u64, round: u64) -> DemoTask {
    DemoTask {
        id: DemoTaskId::from(format!("task-{index:05}")),
        title: format!("task {index} round {round}"),
        completed: round % 2 == 1,
        status_id: DemoStatusId::from("todo"),
    }
}

/// User + system CPU per thread-name prefix, in clock ticks.
fn thread_cpu() -> HashMap<String, u64> {
    let mut totals = HashMap::new();
    for entry in std::fs::read_dir("/proc/self/task").expect("procfs") {
        let path = entry.expect("task").path();
        let Ok(stat) = std::fs::read_to_string(path.join("stat")) else {
            continue;
        };
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let name = &stat[open + 1..close];
        let fields: Vec<&str> = stat[close + 2..].split(' ').collect();
        let ticks = fields[11].parse::<u64>().unwrap_or(0) + fields[12].parse::<u64>().unwrap_or(0);
        let group = if name.starts_with("fanout-client") {
            "client"
        } else if name.starts_with("fanout-server") {
            "server-runtime"
        } else {
            "server-other"
        };
        *totals.entry(group.to_owned()).or_insert(0) += ticks;
    }
    totals
}

fn encode(message: &MykoMessage, cbor: bool) -> Message {
    if cbor {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(message, &mut bytes).expect("cbor");
        Message::Binary(bytes.into())
    } else {
        Message::Text(serde_json::to_string(message).expect("json").into())
    }
}

#[derive(Default)]
struct Received {
    frames: AtomicU64,
    bytes: AtomicU64,
}

#[test]
#[ignore = "bench; run explicitly with --ignored --nocapture"]
#[allow(clippy::too_many_lines)]
fn steady_state_fanout() {
    let rows = env("FANOUT_ROWS", 300);
    let view_clients = env("FANOUT_VIEW_CLIENTS", 13);
    let view_hz = env("FANOUT_VIEW_HZ", 60);
    let report_clients = env("FANOUT_REPORT_CLIENTS", 2);
    let reports = env("FANOUT_REPORTS", 44).min(rows);
    let batches_per_sec = env("FANOUT_BATCHES_PER_SEC", 360);
    let batch_events = env("FANOUT_BATCH_EVENTS", 8);
    let secs = env("FANOUT_SECS", 10);
    let cbor = env("FANOUT_CBOR", 0) != 0;
    let server_threads = usize::try_from(env("FANOUT_SERVER_THREADS", 8)).unwrap_or(8);

    let server_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(server_threads)
        .thread_name("fanout-server")
        .enable_all()
        .build()
        .expect("server runtime");
    let client_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("fanout-client")
        .enable_all()
        .build()
        .expect("client runtime");

    let addr = server_rt.block_on(async {
        let server = Arc::new(
            MykoServer::builder()
                .with_default_persister(Arc::new(NullPersister))
                .build(),
        );
        let ctx = server.ctx();
        ctx.set(&DemoStatus {
            id: DemoStatusId::from("todo"),
            name: "Todo".into(),
            color: "#000000".into(),
            emoji: "o".into(),
        })
        .expect("status");
        for index in 0..rows {
            ctx.set(&task(index, 0)).expect("task");
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                let ctx = Arc::new(server.ctx());
                let info = server.server_info();
                tokio::spawn(route_connection(stream, peer, ctx, info));
            }
        });
        addr
    });
    let url = format!("ws://{addr}/myko");
    let stop = Arc::new(AtomicBool::new(false));
    let received = Arc::new(Received::default());

    let mut subscriptions = Vec::new();
    for client in 0..view_clients {
        subscriptions.push(vec![
            serde_json::from_value::<MykoMessage>(json!({
                "event": "ws:m:view",
                "data": {
                    "view": { "tx": format!("view-{client}") },
                    "viewId": "GetDemoTasksWithStatus",
                    "viewItemType": "DemoTaskWithStatus",
                    "sampleRate": view_hz,
                },
            }))
            .expect("view request"),
        ]);
    }
    for client in 0..report_clients {
        subscriptions.push(
            (0..reports)
                .map(|index| {
                    serde_json::from_value::<MykoMessage>(json!({
                        "event": "ws:m:report",
                        "data": {
                            "report": {
                                "tx": format!("report-{client}-{index}"),
                                "id": format!("task-{index:05}"),
                            },
                            "reportId": "FanoutTask",
                        },
                    }))
                    .expect("report request")
                })
                .collect(),
        );
    }

    let mut readers = Vec::new();
    for requests in subscriptions {
        let url = url.clone();
        let stop = stop.clone();
        let received = received.clone();
        readers.push(client_rt.spawn(async move {
            let (socket, _) = tokio_tungstenite::connect_async(&url)
                .await
                .expect("connect");
            let (mut write, mut read) = socket.split();
            for request in &requests {
                write.send(encode(request, cbor)).await.expect("subscribe");
            }
            while !stop.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(200), read.next()).await {
                    Ok(Some(Ok(frame))) => {
                        let len = match &frame {
                            Message::Text(text) => text.len(),
                            Message::Binary(bytes) => bytes.len(),
                            _ => 0,
                        };
                        received.frames.fetch_add(1, Ordering::Relaxed);
                        received
                            .bytes
                            .fetch_add(u64::try_from(len).unwrap_or(0), Ordering::Relaxed);
                    }
                    Ok(_) => panic!("server closed a reader"),
                    Err(_) => {}
                }
            }
        }));
    }

    // Updates walk the rows the reports watch, so every report and every
    // view sees small per-frame deltas.
    let sent_batches = Arc::new(AtomicU64::new(0));
    let writer = {
        let stop = stop.clone();
        let sent_batches = sent_batches.clone();
        client_rt.spawn(async move {
            let (socket, _) = tokio_tungstenite::connect_async(&url)
                .await
                .expect("connect");
            let (mut write, mut read) = socket.split();
            tokio::spawn(async move { while read.next().await.is_some() {} });
            let mut ticker =
                tokio::time::interval(Duration::from_secs_f64(1.0 / batches_per_sec as f64));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
            let mut round = 1u64;
            let mut cursor = 0u64;
            while !stop.load(Ordering::Relaxed) {
                ticker.tick().await;
                let events = (0..batch_events)
                    .map(|_| {
                        let index = cursor % reports.max(1);
                        cursor += 1;
                        if index == 0 {
                            round += 1;
                        }
                        MEvent::from_item(&task(index, round), MEventType::SET, "bench")
                    })
                    .collect();
                write
                    .send(encode(&MykoMessage::EventBatch(events), cbor))
                    .await
                    .expect("send batch");
                sent_batches.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    // Warm up: subscriptions build and initial snapshots go out.
    std::thread::sleep(Duration::from_secs(3));
    let cpu_before = thread_cpu();
    let frames_before = received.frames.load(Ordering::Relaxed);
    let bytes_before = received.bytes.load(Ordering::Relaxed);
    let batches_before = sent_batches.load(Ordering::Relaxed);
    let started = Instant::now();
    std::thread::sleep(Duration::from_secs(secs));
    let elapsed = started.elapsed().as_secs_f64();
    let cpu_after = thread_cpu();
    let frames = received.frames.load(Ordering::Relaxed) - frames_before;
    let bytes = received.bytes.load(Ordering::Relaxed) - bytes_before;
    let batches = sent_batches.load(Ordering::Relaxed) - batches_before;
    stop.store(true, Ordering::Relaxed);
    client_rt.block_on(async {
        let _ = writer.await;
        for reader in readers {
            let _ = reader.await;
        }
    });

    // NOTE(ts): USER_HZ is 100 on Linux; procfs reports CPU in those ticks.
    let ticks_per_sec = 100.0;
    let cores = |group: &str| {
        let delta = cpu_after.get(group).copied().unwrap_or(0)
            - cpu_before.get(group).copied().unwrap_or(0);
        delta as f64 / ticks_per_sec / elapsed
    };
    eprintln!(
        "FANOUT format={} rows={rows} view_clients={view_clients}@{view_hz}Hz report_clients={report_clients}x{reports} batches={:.0}/s x{batch_events}",
        if cbor { "cbor" } else { "json" },
        batches as f64 / elapsed,
    );
    eprintln!(
        "FANOUT delivered frames={:.0}/s bytes={:.2}MB/s",
        frames as f64 / elapsed,
        bytes as f64 / elapsed / 1e6,
    );
    eprintln!(
        "FANOUT cpu cores: server-runtime={:.3} server-other={:.3} client={:.3}",
        cores("server-runtime"),
        cores("server-other"),
        cores("client"),
    );
    eprintln!(
        "FANOUT server us/frame={:.2}",
        (cores("server-runtime") + cores("server-other")) * 1e6 / (frames as f64 / elapsed),
    );
    drop(server_rt);
}
