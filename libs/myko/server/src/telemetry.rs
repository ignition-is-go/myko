//! Logging/tracing init: always-on console output, optional OTLP export.
//!
//! Host processes (e.g. rship-control-plane) call [`init_from_env`] once at
//! startup — before `MykoServer::builder()...build()` — replacing the
//! `env_logger::init()` call from before the log→tracing migration. See
//! `README.md`'s Environment table for `MYKO_TRACING_ENDPOINT` /
//! `MYKO_MEM_PROFILE_INTERVAL_SECS`.

use std::{sync::Arc, time::Duration};

use hyphae::Gettable;
use myko::store::StoreRegistry;
use opentelemetry::{KeyValue, global, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use tracing_subscriber::{
    EnvFilter, Layer, layer::SubscriberExt, registry::LookupSpan, util::SubscriberInitExt,
};

const DEFAULT_METRICS_INTERVAL_SECS: u64 = 60;

/// Holds the OTLP provider handles alive for the process lifetime.
///
/// Bind the return value of [`init_from_env`] to a variable in `main()` —
/// dropping it immediately (e.g. `let _ = init_from_env();`) shuts the
/// providers down before anything is exported. `Drop` flushes the last
/// batch of spans/metrics before the process exits.
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("myko telemetry: tracer provider shutdown error: {e}");
        }
        if let Some(provider) = self.meter_provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("myko telemetry: meter provider shutdown error: {e}");
        }
    }
}

/// Initialize logging/tracing from environment — the simple-case wrapper
/// around [`otel_layer_from_env`] for host processes that don't already
/// compose their own `tracing_subscriber` (no existing fmt layer, no Tracy/
/// other tracing consumer to combine with — see [`otel_layer_from_env`] if
/// you do).
///
/// Always installs a `tracing_subscriber` fmt layer filtered by
/// `RUST_LOG`/`EnvFilter::from_default_env()` — identical semantics to the
/// `env_logger::init()` this replaces, so existing runbooks/ops tooling that
/// set `RUST_LOG` keep working unchanged.
///
/// If `MYKO_TRACING_ENDPOINT` is set, additionally builds an OTLP/HTTP trace
/// exporter (bridged into the same `tracing` spans via `tracing-opentelemetry`)
/// and an OTLP/HTTP metrics exporter behind a periodic reader — export
/// interval from `MYKO_MEM_PROFILE_INTERVAL_SECS` (seconds, default 60). If
/// unset, telemetry stays local-only (console logging), matching the prior
/// dev-loop behavior.
pub fn init_from_env() -> TelemetryGuard {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();
    let (otel_layer, guard) = otel_layer_from_env();

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    guard.unwrap_or(TelemetryGuard {
        tracer_provider: None,
        meter_provider: None,
    })
}

/// Build just the OTLP trace layer (+ register the OTLP metrics
/// `MeterProvider` globally, as a side effect) from `MYKO_TRACING_ENDPOINT`/
/// `MYKO_MEM_PROFILE_INTERVAL_SECS` — for host processes that compose their
/// *own* `tracing_subscriber::registry()` (an existing custom fmt layer, a
/// Tracy layer for live profiling sessions, etc.) instead of ceding the
/// whole subscriber to [`init_from_env`]'s monolithic `.init()`.
///
/// Metrics don't compose via `Layer` the way traces do — there's only ever
/// one global `MeterProvider` — so this registers it globally as a side
/// effect regardless of whether the caller uses the returned trace layer.
///
/// Returns `(None, None)` when `MYKO_TRACING_ENDPOINT` is unset: no layer to
/// add, no meter provider registered (metrics recording calls elsewhere in
/// myko fall back to a no-op meter, same as always). Hold the returned
/// [`TelemetryGuard`] for the process lifetime, same as [`init_from_env`].
///
/// ```rust,no_run
/// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
/// let (otel_layer, _guard) = myko_server::telemetry::otel_layer_from_env();
/// tracing_subscriber::registry()
///     .with(tracing_subscriber::fmt::layer()) // your own fmt layer, unchanged
///     .with(otel_layer)                       // adds OTLP export alongside it
///     .init();
/// ```
pub fn otel_layer_from_env<S>() -> (Option<impl Layer<S> + Send + Sync>, Option<TelemetryGuard>)
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
    let Ok(endpoint) = std::env::var("MYKO_TRACING_ENDPOINT") else {
        return (None, None);
    };

    let resource = Resource::builder().with_service_name("myko-server").build();
    let tracer_provider = build_tracer_provider(&endpoint, resource.clone());
    let meter_provider = build_meter_provider(&endpoint, resource);

    global::set_meter_provider(meter_provider.clone());

    let tracer = tracer_provider.tracer("myko-server");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    (
        Some(otel_layer),
        Some(TelemetryGuard {
            tracer_provider: Some(tracer_provider),
            meter_provider: Some(meter_provider),
        }),
    )
}

fn build_tracer_provider(endpoint: &str, resource: Resource) -> SdkTracerProvider {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        // `.with_endpoint` is the exact per-signal URL (opentelemetry-otlp does NOT
        // append the signal path when set programmatically), so append `/v1/traces`
        // to the base gateway endpoint — otherwise it POSTs to `/` and gets 404.
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .build()
        .expect("failed to build OTLP/HTTP trace exporter");

    SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build()
}

/// Registers an OTLP `ObservableGauge` reporting live per-entity-type item
/// counts (`myko.store.item_count`, tagged `entity_type`) — the Rust
/// equivalent of the old TS gateway's `itemCountsGuage`/`repo.getItemCount()`.
///
/// Sampled on each metrics export (interval set by [`init_from_env`] from
/// `MYKO_MEM_PROFILE_INTERVAL_SECS`), not on its own timer — this reuses the
/// OTLP SDK's own periodic reader instead of a bespoke background thread.
/// Cheap/no-op when no real `MeterProvider` is registered (i.e.
/// `MYKO_TRACING_ENDPOINT` unset): `opentelemetry::global::meter` falls back
/// to a no-op meter in that case, so this is safe to call unconditionally.
///
/// The callback is owned by the `Meter`/`MeterProvider` itself (this crate's
/// `ObservableGauge` handle carries no `Drop` — dropping it here does not
/// unregister the callback), so the return value doesn't need to be held.
pub fn register_item_count_gauge(registry: Arc<StoreRegistry>) {
    let meter = global::meter("myko-server");
    let _gauge = meter
        .u64_observable_gauge("myko.store.item_count")
        .with_description("Live entity count per store, sampled on each metrics export")
        .with_callback(move |observer| {
            for entity_type in registry.entity_types() {
                let count = registry.get_or_create(&entity_type).len().get() as u64;
                observer.observe(
                    count,
                    &[KeyValue::new("entity_type", entity_type.to_string())],
                );
            }
        })
        .build();
}

const MALLOC_TRIM_INTERVAL_ENV: &str = "MYKO_MALLOC_TRIM_INTERVAL_SECS";

/// Periodic `malloc_trim(0)` probe: logs RSS before/after asking glibc to
/// return free arena pages to the OS. Opt-in via `MYKO_MALLOC_TRIM_INTERVAL_SECS`
/// (seconds); unset or 0 = disabled, no thread spawned.
///
/// This exists to interpret RSS observations of deployed servers. The M1
/// amplification harness (2026-07) showed a myko process at 5,167 MB RSS while
/// referencing 5.81 MB of live heap — an 889× gap that was pure glibc arena
/// retention, collapsing to 13 MB on trim. If a deployment's "huge RSS"
/// collapses the same way here, the number was allocator behaviour, not
/// retention; if it doesn't, something is genuinely holding the memory.
///
/// Note the probe is not passive: each tick returns free pages to the OS, so
/// enabling it lowers steady-state RSS (that release is the measurement).
/// glibc-only — on other libcs the env var logs a warning and does nothing.
///
/// **This measures glibc's arenas only.** If the host binary installs a
/// different `#[global_allocator]` (rship sets tikv-jemallocator), Rust
/// allocations never touch glibc, `malloc_trim` has ~nothing to release, and
/// `released≈0` says nothing about retention — the probe warns once when a
/// tick looks like that instead of letting it read as a clean result. Under
/// jemalloc, use its own allocated-vs-resident stats (rship's mem_profile
/// ticks) as the discriminator.
pub fn start_malloc_trim_probe() {
    let Some(interval_secs) = std::env::var(MALLOC_TRIM_INTERVAL_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&s| s > 0)
    else {
        return;
    };

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        use std::sync::OnceLock;
        static STARTED: OnceLock<()> = OnceLock::new();
        if STARTED.set(()).is_err() {
            return;
        }
        // Positive detection for the most likely foreign allocator: tikv-
        // jemallocator doesn't replace the C `malloc` symbol, but it does
        // export jemalloc's prefixed control API, so resolving _rjem_mallctl
        // proves jemalloc is linked. Refuse at startup with the reason rather
        // than tick forever measuring arenas that hold nothing. (Other custom
        // allocators aren't detectable this way — the in-loop low-release
        // warning is the backstop for those.)
        if jemalloc_linked() {
            tracing::warn!(
                target: "myko_server::mem_probe",
                "{MALLOC_TRIM_INTERVAL_ENV} is set but this binary links jemalloc \
                 (_rjem_mallctl resolved) — malloc_trim only trims glibc arenas, which \
                 jemalloc bypasses. Probe disabled; use jemalloc's allocated-vs-resident \
                 stats instead."
            );
            return;
        }
        let _ = std::thread::Builder::new()
            .name("myko-malloc-trim".to_string())
            .spawn(move || run_malloc_trim_loop(interval_secs))
            .map_err(|e| {
                tracing::warn!(
                    target: "myko_server::mem_probe",
                    "Failed to spawn malloc_trim probe thread: {e}"
                )
            });
    }

    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        let _ = interval_secs;
        tracing::warn!(
            target: "myko_server::mem_probe",
            "{MALLOC_TRIM_INTERVAL_ENV} is set but malloc_trim is glibc-only; probe disabled"
        );
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn run_malloc_trim_loop(interval_secs: u64) {
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }

    let mb = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
    let mut warned_wrong_allocator = false;

    loop {
        std::thread::sleep(Duration::from_secs(interval_secs));

        let before = rss_bytes();
        // Returns 1 if any memory was actually released back to the system.
        let released = unsafe { malloc_trim(0) } == 1;
        let after = rss_bytes();

        if let (Some(before), Some(after)) = (before, after) {
            let released_bytes = before.saturating_sub(after);
            tracing::info!(
                target: "myko_server::mem_probe",
                "[malloc_trim] rss_before={:.2}MB rss_after={:.2}MB released={:.2}MB ({})",
                mb(before),
                mb(after),
                mb(released_bytes),
                if released { "pages returned" } else { "no-op" },
            );
            // A large RSS that trim barely dents is ambiguous: either the heap
            // is genuinely live, or a non-glibc #[global_allocator] owns the
            // memory and malloc_trim never touched it. Say so once, loudly —
            // "released=0" must not read as "no retention" on its own.
            if !warned_wrong_allocator
                && released_bytes < 16 * 1024 * 1024
                && after > 1024 * 1024 * 1024
            {
                warned_wrong_allocator = true;
                tracing::warn!(
                    target: "myko_server::mem_probe",
                    "[malloc_trim] trim released almost nothing against {:.0}MB RSS. Either \
                     this heap is genuinely live, or this binary sets a non-glibc \
                     #[global_allocator] (e.g. jemalloc) that malloc_trim cannot touch — \
                     check the host's main.rs before drawing conclusions; under jemalloc \
                     use its allocated-vs-resident stats instead of this probe.",
                    mb(after),
                );
            }
        } else {
            tracing::warn!(
                target: "myko_server::mem_probe",
                "[malloc_trim] VmRSS unavailable in /proc/self/status; trim ran unmeasured"
            );
        }
    }
}

/// True when jemalloc is linked into this process, detected by resolving its
/// prefixed control symbol via `dlsym`. `RTLD_DEFAULT` is a null handle on
/// Linux/glibc, and libdl is in Rust's default linux-gnu link set, so this
/// carries no extra link requirement or glibc version floor.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn jemalloc_linked() -> bool {
    use core::ffi::{c_char, c_void};
    unsafe extern "C" {
        fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    }
    unsafe { !dlsym(std::ptr::null_mut(), c"_rjem_mallctl".as_ptr()).is_null() }
}

/// Resident set size in bytes, from `VmRSS` in `/proc/self/status`. Reported
/// in kB by the kernel, so no page-size assumption is needed (statm counts
/// pages, and page size varies across arm64 kernels).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(kb * 1024)
}

fn build_meter_provider(endpoint: &str, resource: Resource) -> SdkMeterProvider {
    let interval_secs = std::env::var("MYKO_MEM_PROFILE_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_METRICS_INTERVAL_SECS);

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        // See build_tracer_provider: append the `/v1/metrics` signal path to the base
        // gateway endpoint, else the exporter POSTs to `/` and gets 404.
        .with_endpoint(format!("{}/v1/metrics", endpoint.trim_end_matches('/')))
        .build()
        .expect("failed to build OTLP/HTTP metrics exporter");

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(interval_secs))
        .build();

    SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build()
}
