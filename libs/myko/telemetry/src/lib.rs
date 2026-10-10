//! Shared console logging and OTLP/HTTP logs, traces, and metrics.
//! Install once in the host application, before initializing SDK clients.
use opentelemetry::{global, trace::TracerProvider};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource, logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider,
};
use std::time::Duration;
use tracing_subscriber::{
    EnvFilter, Layer, layer::SubscriberExt, registry::LookupSpan, util::SubscriberInitExt,
};

const DEFAULT_METRICS_INTERVAL_SECS: u64 = 60;

/// Holds the OTLP provider handles alive for the process lifetime.
///
/// Bind the return value of [`init_from_env`] to a variable in `main()` —
/// dropping it immediately (e.g. `let _ = init_from_env();`) shuts the
/// providers down before anything is exported. `Drop` flushes the last
/// batch of logs/spans/metrics before the process exits.
pub struct TelemetryGuard {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
    logger: Option<SdkLoggerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.logger.take()
            && let Err(error) = provider.shutdown()
        {
            eprintln!("myko telemetry: logger provider shutdown error: {error}");
        }
        if let Some(provider) = self.tracer.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("myko telemetry: tracer provider shutdown error: {e}");
        }
        if let Some(provider) = self.meter.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("myko telemetry: meter provider shutdown error: {e}");
        }
    }
}

/// Install console logging and optional OTLP logs, traces, and metrics.
///
/// `RUST_LOG` filters console and exported events. Standard OTLP endpoint and
/// resource environment variables are supported; `MYKO_TRACING_ENDPOINT` is
/// a fallback base URL. Without any endpoint, output stays local.
/// Keep the returned guard alive until shutdown. Existing `log` macros are
/// bridged by tracing-subscriber's default tracing-log feature. Do not install
/// another global logger before calling this function.
#[must_use]
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
        tracer: None,
        meter: None,
        logger: None,
    })
}

/// Build a composable OTLP trace and log layer and register the metrics provider.
///
/// Returns `(None, None)` when no endpoint is configured. Use this when the host
/// already composes console/file/other layers, and keep the guard until shutdown.
/// Only OTLP HTTP/protobuf is supported. The standard per-signal endpoint takes
/// precedence over the standard base endpoint, then the legacy Myko base URL.
/// `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` identify the process.
///
/// ```rust,no_run
/// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
/// let (otel_layer, _guard) = myko_telemetry::otel_layer_from_env();
/// tracing_subscriber::registry()
///     .with(tracing_subscriber::fmt::layer())
///     .with(otel_layer)
///     .init();
/// ```
#[must_use]
pub fn otel_layer_from_env<S>() -> (Option<impl Layer<S> + Send + Sync>, Option<TelemetryGuard>)
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
    let legacy_endpoint = std::env::var("MYKO_TRACING_ENDPOINT").ok();
    let configured = legacy_endpoint.is_some()
        || [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        ]
        .iter()
        .any(|key| std::env::var(key).is_ok());
    if !configured {
        return (None, None);
    }
    for key in [
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
    ] {
        if let Ok(protocol) = std::env::var(key)
            && protocol != "http/protobuf"
        {
            eprintln!("myko telemetry: {key}={protocol:?} is unsupported; use http/protobuf");
            return (None, None);
        }
    }
    for signal in ["LOGS", "TRACES", "METRICS"] {
        if let Some(value) = signal_endpoint(legacy_endpoint.as_deref(), signal) {
            let valid = url::Url::parse(&value).is_ok_and(|url| {
                matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
            });
            if !valid {
                // Do not include the value: endpoints can contain credentials.
                eprintln!("myko telemetry: {signal} endpoint must be an absolute HTTP(S) URL");
                return (None, None);
            }
        }
    }
    let detected = Resource::builder().build();
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| {
        detected
            .get(&opentelemetry::Key::new("service.name"))
            .map(|name| name.to_string())
            .filter(|name| !name.starts_with("unknown_service"))
            .unwrap_or_else(|| "myko-server".into())
    });
    let resource = Resource::builder().with_service_name(service_name).build();
    let tracer_provider = build_tracer_provider(legacy_endpoint.as_deref(), resource.clone());
    let meter_provider = build_meter_provider(legacy_endpoint.as_deref(), resource.clone());
    let logger_provider = build_logger_provider(legacy_endpoint.as_deref(), resource);
    let (Some(tracer_provider), Some(meter_provider), Some(logger_provider)) =
        (tracer_provider, meter_provider, logger_provider)
    else {
        return (None, None);
    };

    global::set_meter_provider(meter_provider.clone());

    let tracer = tracer_provider.tracer("myko-server");
    let logs = OpenTelemetryTracingBridge::new(&logger_provider).with_filter(
        tracing_subscriber::filter::filter_fn(|metadata| {
            // Exporter diagnostics must not be fed back into their own exporter.
            !metadata.target().starts_with("opentelemetry")
        }),
    );
    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_context_activation(true)
        .and_then(logs);

    (
        Some(otel_layer),
        Some(TelemetryGuard {
            tracer: Some(tracer_provider),
            meter: Some(meter_provider),
            logger: Some(logger_provider),
        }),
    )
}

fn build_tracer_provider(endpoint: Option<&str>, resource: Resource) -> Option<SdkTracerProvider> {
    let builder = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        // `.with_endpoint` is the exact per-signal URL (opentelemetry-otlp does NOT
        // append the signal path when set programmatically), so append `/v1/traces`
        // to the base gateway endpoint — otherwise it POSTs to `/` and gets 404.
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary);
    let builder = if let Some(endpoint) = signal_endpoint(endpoint, "TRACES") {
        builder.with_endpoint(endpoint)
    } else {
        builder
    };
    let exporter = match builder.build() {
        Ok(exporter) => exporter,
        Err(error) => {
            eprintln!("myko telemetry: failed to build OTLP trace exporter: {error}");
            return None;
        }
    };

    Some(
        SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build(),
    )
}

fn build_meter_provider(endpoint: Option<&str>, resource: Resource) -> Option<SdkMeterProvider> {
    let interval_secs = std::env::var("MYKO_MEM_PROFILE_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_METRICS_INTERVAL_SECS);

    let builder = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        // See build_tracer: append the `/v1/metrics` signal path to the base
        // gateway endpoint, else the exporter POSTs to `/` and gets 404.
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary);
    let builder = if let Some(endpoint) = signal_endpoint(endpoint, "METRICS") {
        builder.with_endpoint(endpoint)
    } else {
        builder
    };
    let exporter = match builder.build() {
        Ok(exporter) => exporter,
        Err(error) => {
            eprintln!("myko telemetry: failed to build OTLP metrics exporter: {error}");
            return None;
        }
    };

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(interval_secs))
        .build();

    Some(
        SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build(),
    )
}

fn signal_endpoint(legacy: Option<&str>, signal: &str) -> Option<String> {
    if let Ok(endpoint) = std::env::var(format!("OTEL_EXPORTER_OTLP_{signal}_ENDPOINT")) {
        return Some(endpoint);
    }
    let base = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .or_else(|| legacy.map(str::to_owned));
    base.map(|base| {
        format!(
            "{}/v1/{}",
            base.trim_end_matches('/'),
            signal.to_ascii_lowercase()
        )
    })
}

fn build_logger_provider(endpoint: Option<&str>, resource: Resource) -> Option<SdkLoggerProvider> {
    let builder = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary);
    let builder = if let Some(endpoint) = signal_endpoint(endpoint, "LOGS") {
        builder.with_endpoint(endpoint)
    } else {
        builder
    };
    match builder.build() {
        Ok(exporter) => Some(
            SdkLoggerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource)
                .build(),
        ),
        Err(error) => {
            eprintln!("myko telemetry: failed to build OTLP log exporter: {error}");
            None
        }
    }
}
