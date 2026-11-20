//! Allocations per write-path operation, attributed by call site.
//!
//! A counting global allocator records a backtrace for every allocation made
//! while an operation runs, on any thread, until the sink is quiet again. Each
//! allocation is charged to the innermost myko or hyphae frame and, beside it,
//! the innermost frame outside the standard library.
//!
//! `cargo test -p myko-bench-suite --test alloc_breakdown -- --ignored --nocapture`
//! prints the table; `ALLOC_SITES=1` adds the per-site breakdown.

#![allow(
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::as_conversions,
    clippy::cast_precision_loss
)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    backtrace::Backtrace,
    cell::Cell,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use myko::{
    core::command::CommandHandlerRegistration,
    prelude::*,
    request::RequestContext,
    wire::{MEvent, MEventType},
};
use myko_bench_suite::{
    entities::{BPart, BPartId, Blob, GetBPartById, GetBPartsByIds, blob},
    harness::{Bench, Client, Wire},
};

struct Counting;

static ON: AtomicBool = AtomicBool::new(false);
static TRACE: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static TRACES: parking_lot::Mutex<Vec<(Option<String>, Backtrace)>> =
    parking_lot::Mutex::new(Vec::new());

thread_local! {
    static INSIDE: Cell<bool> = const { Cell::new(false) };
}

fn record(counter: &AtomicU64, bytes: usize) {
    if !ON.load(Relaxed) || INSIDE.with(Cell::get) {
        return;
    }
    counter.fetch_add(1, Relaxed);
    BYTES.fetch_add(bytes as u64, Relaxed);
    if TRACE.load(Relaxed) {
        INSIDE.with(|inside| inside.set(true));
        let trace = Backtrace::force_capture();
        let thread = std::thread::current().name().map(str::to_owned);
        TRACES.lock().push((thread, trace));
        INSIDE.with(|inside| inside.set(false));
    }
}

// SAFETY: every call forwards to `System` unchanged; counting only reads and
// writes atomics and a mutex-guarded Vec whose own allocations are excluded by
// the `INSIDE` flag.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(&ALLOCS, layout.size());
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(&ALLOCS, layout.size());
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(&REALLOCS, new_size);
        // SAFETY: forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[myko_command(bool)]
pub struct SetBPartRevision {
    pub id: BPartId,
    pub revision: u64,
}

impl CommandHandler for SetBPartRevision {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        let Some(part) = ctx.exec_report(GetBPartById {
            id: self.id.clone(),
        })?
        else {
            return Ok(false);
        };
        ctx.emit_set(&BPart {
            id: part.id.clone(),
            name: part.name.clone(),
            revision: self.revision,
            data: part.data.clone(),
        })?;
        Ok(true)
    }
}

const ROWS: usize = 100;

/// `Blob(Null)` clones without allocating, so the allocation counts are the
/// framework's own; a deep clone still shows in the `Blob` clone counter.
fn probe(index: usize, seq: u64) -> BPart {
    BPart {
        id: BPartId(Arc::from(format!("probe-{index}"))),
        name: Arc::from("probe"),
        revision: seq,
        data: Blob(myko::serde_json::Value::Null),
    }
}

fn settle(bench: &Bench) {
    bench
        .sink
        .quiesce(Duration::from_millis(30), Duration::from_secs(10));
}

struct Measured {
    allocs: u64,
    reallocs: u64,
    clones: u64,
    bytes: u64,
    instructions: u64,
    cpu_us: f64,
    msgs: u64,
    rows: u64,
    traces: Vec<(Option<String>, Backtrace)>,
}

/// Runs `op` three times untraced so lazy statics and grown buffers settle,
/// then once with counting on, until the sink is quiet.
fn measure(bench: &Bench, mut op: impl FnMut(u64)) -> Measured {
    for warm in 0..3 {
        op(1_000 + warm);
        settle(bench);
    }
    let trace = std::env::var_os("ALLOC_SITES").is_some();
    let counters = Instructions::open();
    let (msgs0, rows0) = (bench.sink.total_msgs(), bench.sink.total_rows_cb());
    BYTES.store(0, Relaxed);
    TRACES.lock().clear();
    ALLOCS.store(0, Relaxed);
    REALLOCS.store(0, Relaxed);
    TRACE.store(trace, Relaxed);
    blob::CLONES.store(0, Relaxed);
    blob::COUNTING.store(true, Relaxed);
    let (instr0, cpu0) = (counters.read(), cpu_ns());
    ON.store(true, Relaxed);
    op(2_000);
    settle(bench);
    ON.store(false, Relaxed);
    let (instr1, cpu1) = (counters.read(), cpu_ns());
    blob::COUNTING.store(false, Relaxed);
    TRACE.store(false, Relaxed);
    Measured {
        allocs: ALLOCS.load(Relaxed),
        reallocs: REALLOCS.load(Relaxed),
        clones: blob::CLONES.load(Relaxed),
        bytes: BYTES.load(Relaxed),
        instructions: if trace { 0 } else { instr1 - instr0 },
        cpu_us: if trace {
            0.0
        } else {
            (cpu1 - cpu0) as f64 / 1_000.0
        },
        msgs: bench.sink.total_msgs() - msgs0,
        rows: bench.sink.total_rows_cb() - rows0,
        traces: std::mem::take(&mut *TRACES.lock()),
    }
}

/// User-space instructions retired by every thread of this process, read
/// through one `perf_event_open` counter per thread. Threads that start after
/// `open` are not counted, so the harness's threads must already exist.
struct Instructions(Vec<i32>);

impl Instructions {
    fn open() -> Self {
        let mut fds = Vec::new();
        for entry in std::fs::read_dir("/proc/self/task")
            .into_iter()
            .flatten()
            .flatten()
        {
            let Ok(tid) = entry.file_name().to_string_lossy().parse::<i32>() else {
                continue;
            };
            // perf_event_attr, zero-filled: type HARDWARE, size 128, config
            // INSTRUCTIONS, exclude_kernel and exclude_hv.
            let mut attr = [0u64; 16];
            attr[0] = 128 << 32;
            attr[1] = 1;
            attr[5] = (1 << 5) | (1 << 6);
            // SAFETY: attr is a valid, zeroed perf_event_attr of the size given.
            let fd =
                unsafe { libc::syscall(libc::SYS_perf_event_open, attr.as_ptr(), tid, -1, -1, 0) };
            if fd >= 0 {
                fds.push(fd as i32);
            }
        }
        Self(fds)
    }

    fn read(&self) -> u64 {
        self.0
            .iter()
            .map(|fd| {
                let mut value = 0u64;
                // SAFETY: reads one u64 counter value from an open perf fd.
                unsafe { libc::read(*fd, std::ptr::addr_of_mut!(value).cast(), 8) };
                value
            })
            .sum()
    }
}

impl Drop for Instructions {
    fn drop(&mut self) {
        for fd in &self.0 {
            // SAFETY: closes a perf fd this value opened.
            unsafe { libc::close(*fd) };
        }
    }
}

/// On-CPU nanoseconds summed over the process's threads, from schedstat:
/// time spent waiting to be scheduled is not included.
fn cpu_ns() -> u64 {
    std::fs::read_dir("/proc/self/task")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| std::fs::read_to_string(entry.path().join("schedstat")).ok())
        .filter_map(|text| text.split_whitespace().next()?.parse::<u64>().ok())
        .sum()
}

struct Frame {
    symbol: String,
    at: String,
}

fn frames(trace: &Backtrace) -> Vec<Frame> {
    let text = trace.to_string();
    let mut out: Vec<Frame> = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if let Some(at) = line.strip_prefix("at ") {
            if let Some(last) = out.last_mut() {
                last.at = at.to_owned();
            }
        } else if let Some((_, symbol)) = line.split_once(": ") {
            let symbol = symbol
                .rsplit_once("::h")
                .filter(|(_, hash)| hash.len() == 16)
                .map_or(symbol, |(head, _)| head);
            out.push(Frame {
                symbol: symbol.to_owned(),
                at: String::new(),
            });
        }
    }
    out
}

fn short(at: &str) -> String {
    for marker in ["/libs/myko/", "/hyphae-", "/registry/src/", "/rustc/"] {
        if let Some(index) = at.find(marker) {
            return at[index + 1..].to_owned();
        }
    }
    at.to_owned()
}

fn is_ours(frame: &Frame) -> bool {
    (frame.at.contains("/libs/myko/") && !frame.at.contains("alloc_breakdown.rs"))
        || frame.at.contains("/hyphae-")
}

fn is_std(frame: &Frame) -> bool {
    frame.at.is_empty()
        || frame.at.contains("/rustc/")
        || frame.at.contains("alloc_breakdown.rs")
        || frame.at.contains("/hashbrown-")
}

fn report(name: &str, measured: &Measured, summary: &mut Vec<String>) {
    summary.push(format!(
        "| {name} | {} | {} | {} | {} | {} | {:.0} | {}/{} |",
        measured.allocs,
        measured.reallocs,
        measured.bytes,
        measured.clones,
        measured.instructions,
        measured.cpu_us,
        measured.msgs,
        measured.rows
    ));
    if measured.traces.is_empty() {
        return;
    }
    let mut by_site: HashMap<String, (u64, HashMap<String, u64>)> = HashMap::new();
    let mut by_thread: HashMap<String, u64> = HashMap::new();
    for (thread, trace) in &measured.traces {
        *by_thread
            .entry(thread.clone().unwrap_or_else(|| "<unnamed>".into()))
            .or_default() += 1;
        let frames = frames(trace);
        let leaf = frames.iter().find(|frame| !is_std(frame));
        let ours = frames.iter().find(|frame| is_ours(frame));
        let site = ours.map_or_else(
            || "<no myko frame>".to_owned(),
            |frame| format!("{} @ {}", frame.symbol, short(&frame.at)),
        );
        let leaf = leaf.map_or_else(
            || "<std only>".to_owned(),
            |frame| format!("{} @ {}", frame.symbol, short(&frame.at)),
        );
        let entry = by_site.entry(site).or_default();
        entry.0 += 1;
        *entry.1.entry(leaf).or_default() += 1;
    }
    println!(
        "\n## {name}: {} allocs + {} reallocs",
        measured.allocs, measured.reallocs
    );
    let mut threads: Vec<_> = by_thread.into_iter().collect();
    threads.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
    println!("threads: {threads:?}");
    let mut sites: Vec<_> = by_site.into_iter().collect();
    sites.sort_by_key(|(_, (count, _))| std::cmp::Reverse(*count));
    for (site, (count, leaves)) in sites {
        println!("{count:>5}  {site}");
        let mut leaves: Vec<_> = leaves.into_iter().collect();
        leaves.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
        for (leaf, count) in leaves.into_iter().take(4) {
            println!("        {count:>4}  {leaf}");
        }
    }
}

fn dispatch_command(bench: &Bench, command_id: &str, value: serde_json::Value) {
    let registration = myko::inventory::iter::<CommandHandlerRegistration>
        .into_iter()
        .find(|registration| registration.command_id == command_id)
        .unwrap_or_else(|| panic!("command {command_id} is not registered"));
    let executor = (registration.factory)();
    let request = Arc::new(RequestContext::from_client(
        Arc::from("tx-command"),
        Arc::from("client-command"),
        bench.ctx.host_id,
    ));
    let ctx = CommandContext::new(Arc::from(command_id), request, bench.ctx.clone());
    executor
        .execute_from_value(value, ctx)
        .unwrap_or_else(|error| panic!("{command_id}: {error:?}"));
}

#[test]
#[ignore = "prints an allocation report; run with --ignored --nocapture"]
fn write_path_allocations_by_call_site() {
    let mut summary = Vec::new();

    let bench = Bench::new(Wire::None, 16, 1);
    for index in 0..ROWS {
        bench.ctx.set(&probe(index, 0)).unwrap();
    }
    settle(&bench);

    let m = measure(&bench, |_| {});
    report("nothing (harness floor: settle only)", &m, &mut summary);

    let m = measure(&bench, |seq| bench.ctx.set(&probe(1, seq)).unwrap());
    report("ctx.set, existing row, no subscribers", &m, &mut summary);

    let m = measure(&bench, |seq| {
        dispatch_command(
            &bench,
            "SetBPartRevision",
            serde_json::json!({ "id": "probe-2", "revision": seq }),
        );
    });
    report(
        "command SetBPartRevision (read + emit_set), no subscribers",
        &m,
        &mut summary,
    );

    let mut events: Vec<Vec<MEvent>> = (0..4)
        .map(|seq| {
            vec![MEvent::from_item(
                &probe(3, 3_000 + seq),
                MEventType::SET,
                "peer",
            )]
        })
        .collect();
    let m = measure(&bench, |_| {
        bench.ctx.apply_event_batch(events.pop().unwrap()).unwrap();
    });
    report(
        "apply_event_batch, one wire SET, no subscribers",
        &m,
        &mut summary,
    );

    let mut client: Client = bench.client("subscriber");
    client.sub_query(&GetBPartsByIds {
        ids: vec![BPartId(Arc::from("probe-4"))],
    });
    settle(&bench);
    let m = measure(&bench, |seq| bench.ctx.set(&probe(4, seq)).unwrap());
    report(
        "ctx.set delivered to one GetBPartsByIds subscriber",
        &m,
        &mut summary,
    );

    let mut report_client: Client = bench.client("reporter");
    report_client.sub_report(&GetBPartById {
        id: BPartId(Arc::from("probe-5")),
    });
    settle(&bench);
    let m = measure(&bench, |seq| bench.ctx.set(&probe(5, seq)).unwrap());
    report(
        "ctx.set delivered to one GetBPartById report subscriber",
        &m,
        &mut summary,
    );

    let mut churn: Client = bench.client("churn");
    let m = measure(&bench, |_| {
        let sub = churn.sub_query(&GetBPartsByIds {
            ids: vec![BPartId(Arc::from("probe-6"))],
        });
        settle(&bench);
        churn.cancel(&sub);
    });
    report(
        "subscribe + unsubscribe GetBPartsByIds (one row)",
        &m,
        &mut summary,
    );

    println!(
        "\n| operation | allocs | reallocs | bytes | deep clones | instructions:u | cpu us | msgs/rows |\n\
         | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );
    for row in &summary {
        println!("{row}");
    }
    drop((client, report_client, churn));
}
