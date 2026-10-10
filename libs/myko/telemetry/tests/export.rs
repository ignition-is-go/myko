use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use std::{
    io::{Read, Write},
    net::TcpListener,
    process::Command,
    thread,
    time::{Duration, Instant},
};

#[test]
fn child_emits() {
    if std::env::var("MYKO_TELEMETRY_TEST_CHILD").is_err() {
        return;
    }
    if std::env::var("MYKO_TELEMETRY_TEST_DISABLED").is_ok() {
        let (layer, guard) = myko_telemetry::otel_layer_from_env::<tracing_subscriber::Registry>();
        assert!(layer.is_none());
        assert!(guard.is_none());
        return;
    }
    let guard = myko_telemetry::init_from_env();
    let span = tracing::info_span!("test_operation");
    {
        let _entered = span.enter();
        tracing::info!(answer = 42, "tracing delivery");
        log::warn!("sdk log delivery");
        tracing::debug!("filtered delivery");
    }
    drop(span);
    drop(guard);
}

fn exported_logs(
    legacy: bool,
    override_logs: bool,
) -> Result<Vec<ExportLogsServiceRequest>, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let collector = thread::spawn(move || -> std::io::Result<Vec<ExportLogsServiceRequest>> {
        let started = Instant::now();
        let mut logs = Vec::new();
        while started.elapsed() < Duration::from_secs(15) {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if !logs.is_empty() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => return Err(error),
            };
            stream.set_read_timeout(Some(Duration::from_secs(3)))?;
            let mut reader = std::io::BufReader::new(&mut stream);
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                std::io::BufRead::read_line(&mut reader, &mut line)?;
                if line == "\r\n" {
                    break;
                }
                headers.push_str(&line);
            }
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .ok_or_else(|| std::io::Error::other("missing content length"))?;
            let mut body = vec![0; length];
            reader.read_exact(&mut body)?;
            if headers.starts_with("POST /v1/logs ") || headers.starts_with("POST /custom/logs ") {
                let expected_path = if override_logs {
                    "POST /custom/logs "
                } else {
                    "POST /v1/logs "
                };
                if !headers.starts_with(expected_path) {
                    return Err(std::io::Error::other("wrong signal endpoint"));
                }
                logs.push(ExportLogsServiceRequest::decode(body.as_slice())?);
            }
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
        }
        Ok(logs)
    });
    let mut child = Command::new(std::env::current_exe()?);
    // Environment lives in the child, so these tests never mutate global state.
    child
        .env_clear()
        .env("MYKO_TELEMETRY_TEST_CHILD", "1")
        .env("OTEL_SERVICE_NAME", "test-executor")
        .env(
            "OTEL_RESOURCE_ATTRIBUTES",
            "host.name=test-host,deployment.environment.name=hrlv,service.name=overridden-service",
        )
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        .env("RUST_LOG", "info")
        .env(
            if legacy {
                "MYKO_TRACING_ENDPOINT"
            } else {
                "OTEL_EXPORTER_OTLP_ENDPOINT"
            },
            &endpoint,
        )
        .args(["--exact", "child_emits", "--nocapture"]);
    if override_logs {
        child.env(
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            format!("{endpoint}/custom/logs"),
        );
    }
    let output = child.output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    let logs = collector
        .join()
        .map_err(|_| "collector thread panicked")??;
    if logs.is_empty() {
        return Err("no OTLP logs received".into());
    }
    Ok(logs)
}

fn assert_delivery(logs: &[ExportLogsServiceRequest]) {
    let encoded = logs
        .iter()
        .flat_map(Message::encode_to_vec)
        .collect::<Vec<_>>();
    for expected in [
        "test-executor",
        "test-host",
        "hrlv",
        "tracing delivery",
        "sdk log delivery",
        "answer",
    ] {
        assert!(
            encoded
                .windows(expected.len())
                .any(|bytes| bytes == expected.as_bytes()),
            "missing {expected}"
        );
    }
    for record in logs
        .iter()
        .flat_map(|request| &request.resource_logs)
        .flat_map(|resource| &resource.scope_logs)
        .flat_map(|scope| &scope.log_records)
    {
        assert!(!record.trace_id.is_empty(), "missing trace correlation");
        assert!(!record.span_id.is_empty(), "missing span correlation");
    }
    assert!(
        !encoded
            .windows("filtered delivery".len())
            .any(|bytes| bytes == b"filtered delivery")
    );
}

#[test]
fn standard_endpoint_exports_logs_and_sdk_events() -> Result<(), Box<dyn std::error::Error>> {
    assert_delivery(&exported_logs(false, false)?);
    Ok(())
}
#[test]
fn legacy_endpoint_exports_logs() -> Result<(), Box<dyn std::error::Error>> {
    assert_delivery(&exported_logs(true, false)?);
    Ok(())
}
#[test]
fn signal_endpoint_overrides_legacy_base() -> Result<(), Box<dyn std::error::Error>> {
    assert_delivery(&exported_logs(true, true)?);
    Ok(())
}

#[test]
fn no_endpoint_stays_local() -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(std::env::current_exe()?)
        .env_clear()
        .env("MYKO_TELEMETRY_TEST_CHILD", "1")
        .env("MYKO_TELEMETRY_TEST_DISABLED", "1")
        .args(["--exact", "child_emits"])
        .output()?;
    if !output.status.success() {
        return Err("local configuration failed".into());
    }
    Ok(())
}

#[test]
fn invalid_configuration_reports_an_error() -> Result<(), Box<dyn std::error::Error>> {
    for (key, value, diagnostic) in [
        (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "invalid",
            "must be an absolute HTTP(S) URL",
        ),
        ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc", "unsupported"),
    ] {
        let output = Command::new(std::env::current_exe()?)
            .env_clear()
            .env("MYKO_TELEMETRY_TEST_CHILD", "1")
            .env("MYKO_TELEMETRY_TEST_DISABLED", "1")
            .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
            .env(key, value)
            .args(["--exact", "child_emits", "--nocapture"])
            .output()?;
        if !output.status.success() || !String::from_utf8_lossy(&output.stderr).contains(diagnostic)
        {
            return Err(format!("missing diagnostic for {key}").into());
        }
    }
    Ok(())
}
