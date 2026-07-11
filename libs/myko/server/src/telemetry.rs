//! Logging/tracing init: always-on console output, optional OTLP export.
//!
//! Host processes (e.g. rship-control-plane) call [`init_from_env`] once at
//! startup — before `CellServer::builder()...build()` — replacing the
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
        .with_endpoint(endpoint)
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

fn build_meter_provider(endpoint: &str, resource: Resource) -> SdkMeterProvider {
    let interval_secs = std::env::var("MYKO_MEM_PROFILE_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_METRICS_INTERVAL_SECS);

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
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
