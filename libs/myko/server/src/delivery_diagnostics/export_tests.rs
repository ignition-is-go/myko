#![allow(clippy::panic_in_result_fn)]
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest, metrics::v1::metric,
};
use prost::Message;

use super::DeliveryDiagnostics;

#[derive(Debug)]
struct Sample {
    after_close: bool,
    has_client: bool,
}

fn has_closed_client(request: &ExportMetricsServiceRequest) -> bool {
    request.resource_metrics.iter().flat_map(|r| &r.scope_metrics).flat_map(|s| &s.metrics).any(|m| {
        m.name == "myko.ws.delivery.materialize" && matches!(m.data.as_ref(), Some(metric::Data::Histogram(h)) if !h.data_points.is_empty())
    })
}

fn collect(
    listener: &TcpListener,
    stop: &AtomicBool,
    ready: &std::path::Path,
) -> TestResult<Vec<Sample>> {
    let started = Instant::now();
    let mut samples = Vec::new();
    let closed = ready.with_extension("closed");
    while !stop.load(Ordering::Acquire) && started.elapsed() < Duration::from_secs(30) {
        let (mut stream, _) = match listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let metrics = line.starts_with("POST /v1/metrics ");
        let mut length = None;
        loop {
            line.clear();
            assert!(reader.read_line(&mut line)? > 0, "incomplete HTTP headers");
            if line == "\r\n" {
                break;
            }
            if let Some((key, value)) = line.split_once(':')
                && key.eq_ignore_ascii_case("content-length")
            {
                length = Some(value.trim().parse::<usize>()?);
            }
        }
        let mut body = vec![0; length.ok_or("missing body length")?];
        reader.read_exact(&mut body)?;
        if metrics {
            samples.push(Sample {
                after_close: closed.exists(),
                has_client: has_closed_client(&ExportMetricsServiceRequest::decode(
                    body.as_slice(),
                )?),
            });
        }
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
        if samples.iter().filter(|sample| sample.after_close).count() >= 4 {
            std::fs::write(ready, b"collected")?;
        }
    }
    Ok(samples)
}

fn disconnected_client(ready: &std::path::Path) -> TestResult<()> {
    let guard = myko_telemetry::init_from_env();
    myko_telemetry::websocket_metrics::client_connected("retired-client");
    let diagnostics =
        DeliveryDiagnostics::with_enabled("retired-client", "127.0.0.1:1234".parse()?, true);
    diagnostics.materialized("request", Duration::from_millis(1));
    // DropLogger closes the wire registry before releasing its diagnostics Arc.
    myko_telemetry::websocket_metrics::client_disconnected("retired-client");
    drop(diagnostics);
    std::fs::write(ready.with_extension("closed"), b"closed")?;
    let started = Instant::now();
    while !ready.exists() && started.elapsed() < Duration::from_secs(20) {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        ready.exists(),
        "collector did not observe final and subsequent exports"
    );
    drop(guard);
    Ok(())
}

#[test]
fn closed_clients_stop_exporting_after_success() -> TestResult<()> {
    if let Some(ready) = std::env::var_os("MYKO_DELIVERY_RETIRE_CHILD") {
        return disconnected_client(std::path::Path::new(&ready));
    }
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let ready =
        std::env::temp_dir().join(format!("myko-delivery-retirement-{}", uuid::Uuid::new_v4()));
    let collector_ready = ready.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let collector_stop = stop.clone();
    let collector = thread::spawn(move || collect(&listener, &collector_stop, &collector_ready));
    let output = Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "delivery_diagnostics::export_tests::closed_clients_stop_exporting_after_success",
            "--nocapture",
        ])
        .env("MYKO_DELIVERY_RETIRE_CHILD", &ready)
        .env("MYKO_WS_DIAGNOSTICS_LOGS", "0")
        .env("MYKO_MEM_PROFILE_INTERVAL_SECS", "1")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint)
        .env(
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            format!("{endpoint}/v1/metrics"),
        )
        .env(
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            format!("{endpoint}/v1/logs"),
        )
        .env(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            format!("{endpoint}/v1/traces"),
        )
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        .env("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/protobuf")
        .env("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "http/protobuf")
        .env("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
        .output();
    stop.store(true, Ordering::Release);
    let samples = collector.join().map_err(|_| "collector panicked")??;
    let _ = std::fs::remove_file(&ready);
    let _ = std::fs::remove_file(ready.with_extension("closed"));
    let output = output?;
    assert!(
        output.status.success(),
        "child failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        samples.iter().any(|sample| sample.has_client),
        "final diagnostic histogram was never exported"
    );
    let after_close: Vec<_> = samples.iter().filter(|sample| sample.after_close).collect();
    assert!(after_close.len() >= 4, "insufficient subsequent exports");
    assert!(
        after_close
            .iter()
            .rev()
            .take(2)
            .all(|sample| !sample.has_client),
        "disconnected client histogram remained in successful exports: {samples:?}"
    );
    Ok(())
}
