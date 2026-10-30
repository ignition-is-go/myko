//! Add connection-owned cumulative sums after SDK aggregation.
//!
//! Collector receipts are insufficient for retirement: verify final values
//! against Prometheus.
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use http::{
    Request, Response,
    header::{CONTENT_ENCODING, CONTENT_LENGTH},
};
use num_traits::ToPrimitive;
use opentelemetry_http::{HttpClient, HttpError};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;

use super::{ClientSnapshot, Registry, Snapshot};

#[derive(Debug)]
pub struct Client {
    http: reqwest::blocking::Client,
    registry: Arc<Registry>,
    query_url: Option<url::Url>,
}

impl Client {
    pub fn new(registry: Arc<Registry>) -> Result<Self, HttpError> {
        let query_url = std::env::var("MYKO_PROMETHEUS_QUERY_URL")
            .ok()
            .map(|value| {
                url::Url::parse(&value)
                    .ok()
                    .filter(|url| {
                        matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
                    })
                    .ok_or("MYKO_PROMETHEUS_QUERY_URL must be an absolute HTTP(S) URL")
            })
            .transpose()?;
        if query_url.is_none() {
            eprintln!(
                "myko telemetry: MYKO_PROMETHEUS_QUERY_URL is unset; disconnected client counters will remain pending backend confirmation"
            );
        }
        let timeout = std::env::var("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT")
            .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT"))
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map_or(Duration::from_secs(10), Duration::from_millis);
        Ok(Self {
            http: reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()?,
            registry,
            query_url,
        })
    }

    fn confirm(
        &self,
        snapshot: &Snapshot,
        request: &ExportMetricsServiceRequest,
    ) -> BTreeSet<String> {
        let Some(url) = &self.query_url else {
            return BTreeSet::new();
        };
        let resource_labels = resource_labels(request);
        let mut confirmed = BTreeSet::new();
        let closed = snapshot
            .clients
            .iter()
            .filter(|client| client.closed)
            .collect::<Vec<_>>();
        // Bound request size, never the number of retained clients or series.
        for batch in closed.chunks(128) {
            let mut labels = resource_labels.clone();
            let ids = batch
                .iter()
                .map(|client| regex_literal(&client.id))
                .collect::<Vec<_>>()
                .join("|");
            labels.push(format!("client_id=~{}", quote(&format!("({ids})"))));
            labels.push("__name__=~\"myko_ws_(message_count|operation_message_count|operation_message_bytes)_total\"".into());
            let query = format!("{{{}}}", labels.join(","));
            let response = self
                .http
                .post(url.clone())
                .form(&[("query", query)])
                .send()
                .and_then(reqwest::blocking::Response::error_for_status)
                .and_then(reqwest::blocking::Response::json::<serde_json::Value>);
            let Ok(response) = response else {
                // One failed backend request is enough: do not spend one full
                // timeout per client while the backend is unavailable.
                break;
            };
            for client in batch {
                if contains_final_values(client, &response) {
                    confirmed.insert(client.id.clone());
                }
            }
        }
        confirmed
    }
}

fn regex_literal(value: &str) -> String {
    let mut escaped = String::new();
    for character in value.chars() {
        if "\\.+*?()|[]{}^$".contains(character) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn quote(value: &str) -> String {
    // JSON string escaping is also PromQL string escaping.
    serde_json::Value::String(value.into()).to_string()
}

fn resource_labels(request: &ExportMetricsServiceRequest) -> Vec<String> {
    let attributes = request
        .resource_metrics
        .first()
        .and_then(|resource| resource.resource.as_ref());
    let attribute = |key: &str| -> Option<String> {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        attributes?
            .attributes
            .iter()
            .find(|attribute| attribute.key == key)
            .and_then(|attribute| attribute.value.as_ref())
            .and_then(|value| match &value.value {
                Some(Value::StringValue(value)) => Some(value.clone()),
                _ => None,
            })
    };
    let mut labels = Vec::new();
    if let Some(service) = attribute("service.name") {
        let job = attribute("service.namespace")
            .filter(|namespace| !namespace.is_empty())
            .map_or_else(
                || service.clone(),
                |namespace| format!("{namespace}/{service}"),
            );
        labels.push(format!("job={}", quote(&job)));
    }
    if let Some(instance) = attribute("service.instance.id") {
        labels.push(format!("instance={}", quote(&instance)));
    }
    labels
}

fn contains_final_values(client: &ClientSnapshot, response: &serde_json::Value) -> bool {
    if response.get("status").and_then(serde_json::Value::as_str) != Some("success")
        || response
            .pointer("/data/resultType")
            .and_then(serde_json::Value::as_str)
            != Some("vector")
    {
        return false;
    }
    let Some(results) = response
        .pointer("/data/result")
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    let mut values = BTreeMap::<_, f64>::new();
    for result in results {
        let Some(value) = result
            .pointer("/value/1")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite())
        else {
            continue;
        };
        let labels = ["__name__", "client_id", "direction", "kind", "tag"].map(|key| {
            result
                .get("metric")
                .and_then(|metric| metric.get(key))
                .and_then(serde_json::Value::as_str)
        });
        values
            .entry(labels)
            .and_modify(|previous| *previous = previous.max(value))
            .or_insert(value);
    }
    client.points().iter().all(|point| {
        let name = point.prometheus_name();
        let labels = [
            Some(name.as_str()),
            point.labels.get("client_id").map(String::as_str),
            point.labels.get("direction").map(String::as_str),
            point.labels.get("kind").map(String::as_str),
            point.labels.get("tag").map(String::as_str),
        ];
        values
            .get(&labels)
            .zip(point.value.to_f64())
            .is_some_and(|(actual, expected)| *actual >= expected)
    })
}

fn decode(body: &[u8], gzip: bool) -> Result<Vec<u8>, HttpError> {
    if !gzip {
        return Ok(body.to_vec());
    }
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(body).read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn encode(body: Vec<u8>, gzip: bool) -> Result<Bytes, HttpError> {
    if !gzip {
        return Ok(body.into());
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&body)?;
    Ok(encoder.finish()?.into())
}

#[async_trait]
impl HttpClient for Client {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (mut parts, body) = request.into_parts();
        let gzip = match parts
            .headers
            .get(CONTENT_ENCODING)
            .map(http::HeaderValue::to_str)
            .transpose()?
        {
            None | Some("identity") => false,
            Some("gzip") => true,
            Some(_) => return Err("unsupported OTLP metrics content encoding".into()),
        };
        let mut payload = ExportMetricsServiceRequest::decode(decode(&body, gzip)?.as_slice())?;
        let snapshot = self.registry.snapshot();
        if !snapshot.append(&mut payload) {
            return Err("OTLP metrics request has no resource for client counters".into());
        }
        parts.headers.remove(CONTENT_LENGTH);
        let request = Request::from_parts(parts, encode(payload.encode_to_vec(), gzip)?);
        let response = self.http.send_bytes(request).await?;
        if response.status().is_success()
            && ExportMetricsServiceResponse::decode(response.body().clone()).is_ok_and(|response| {
                response
                    .partial_success
                    .is_none_or(|partial| partial.rejected_data_points == 0)
            })
        {
            let confirmed = self.confirm(&snapshot, &payload);
            self.registry.acknowledge(&snapshot, &confirmed);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic_in_result_fn)]
    use super::*;
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::ExportMetricsPartialSuccess, metrics::v1::ResourceMetrics,
    };
    use std::{
        io::{BufRead, BufReader},
        net::TcpListener,
        thread,
        time::Instant,
    };

    fn backend_values(client: &ClientSnapshot) -> serde_json::Value {
        let results = client
            .points()
            .into_iter()
            .map(|point| {
                let mut labels = serde_json::Map::new();
                labels.insert("__name__".into(), point.prometheus_name().into());
                for (key, value) in point.labels {
                    labels.insert(key.into(), value.into());
                }
                serde_json::json!({"metric": labels, "value": [123, point.value.to_string()]})
            })
            .collect::<Vec<_>>();
        serde_json::json!({"status": "success", "data": {"resultType": "vector", "result": results}})
    }

    fn registry() -> Arc<Registry> {
        let registry = Arc::new(Registry::default());
        registry.open("client");
        registry.record("client", "out", "QueryResponse", Some("query"), Some(123));
        registry.close("client");
        registry
    }

    #[test]
    fn requires_every_operation_and_byte_count() -> Result<(), Box<dyn std::error::Error>> {
        let registry = registry();
        let snapshot = registry.snapshot();
        let client = snapshot.clients.first().ok_or("missing client")?;
        let mut response = backend_values(client);
        assert!(contains_final_values(client, &response));
        response
            .pointer_mut("/data/result")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or("missing results")?
            .pop();
        assert!(!contains_final_values(client, &response));
        assert!(!contains_final_values(
            client,
            &serde_json::json!({"status": "error"})
        ));
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum Case {
        Confirmed,
        Gzip,
        Pending,
        CollectorFailure,
        Partial,
        NoQuery,
        QueryFailure,
    }

    #[allow(clippy::too_many_lines)] // Fake collector and backend exercise the complete delivery boundary.
    fn export_case(case: Case) -> Result<(u64, u64), HttpError> {
        let status = if matches!(case, Case::CollectorFailure) {
            503
        } else {
            200
        };
        let partial = matches!(case, Case::Partial);
        let confirm = matches!(case, Case::Confirmed | Case::Gzip);
        let gzip = matches!(case, Case::Gzip);
        let has_query = !matches!(case, Case::NoQuery);
        let query_status = if matches!(case, Case::QueryFailure) {
            503
        } else {
            200
        };
        let registry = registry();
        let snapshot = registry.snapshot();
        let client_snapshot = snapshot.clients.first().ok_or("missing client")?;
        let response = if confirm {
            backend_values(client_snapshot)
        } else {
            serde_json::json!({"status": "success", "data": {"resultType": "vector", "result": []}})
        };
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let exporter_response = ExportMetricsServiceResponse {
            partial_success: partial.then(|| ExportMetricsPartialSuccess {
                rejected_data_points: 1,
                error_message: "rejected".into(),
            }),
        }
        .encode_to_vec();
        let collector = thread::spawn(move || -> Result<(), HttpError> {
            let requests = if status == 200 && !partial && has_query {
                2
            } else {
                1
            };
            for index in 0..requests {
                let started = Instant::now();
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && started.elapsed() < Duration::from_secs(5) =>
                        {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                stream.set_read_timeout(Some(Duration::from_secs(3)))?;
                let mut reader = BufReader::new(&mut stream);
                let mut headers = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line)? == 0 {
                        return Err("premature request EOF".into());
                    }
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
                    .ok_or("missing content length")?;
                let mut body = vec![0; length];
                reader.read_exact(&mut body)?;
                let (status, content_type, response_body) = if index == 0 {
                    let request =
                        ExportMetricsServiceRequest::decode(decode(&body, gzip)?.as_slice())?;
                    let count = request
                        .resource_metrics
                        .iter()
                        .flat_map(|resource| &resource.scope_metrics)
                        .flat_map(|scope| &scope.metrics)
                        .count();
                    assert_eq!(count, 3);
                    (status, "application/x-protobuf", exporter_response.clone())
                } else {
                    assert!(headers.starts_with("POST /query "));
                    (
                        query_status,
                        "application/json",
                        response.to_string().into_bytes(),
                    )
                };
                write!(
                    stream,
                    "HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response_body.len()
                )?;
                stream.write_all(&response_body)?;
            }
            Ok(())
        });
        let client = Client {
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()?,
            registry: registry.clone(),
            query_url: has_query
                .then(|| url::Url::parse(&format!("http://{address}/query")))
                .transpose()?,
        };
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics::default()],
        };
        let mut builder = Request::post(format!("http://{address}/v1/metrics"));
        if gzip {
            builder = builder.header(CONTENT_ENCODING, "gzip");
        }
        let request = builder.body(encode(request.encode_to_vec(), gzip)?)?;
        // HTTP error is allowed; retirement is never allowed for it.
        let _response = futures::executor::block_on(client.send_bytes(request));
        collector
            .join()
            .map_err(|_| "collector thread panicked")??;
        Ok(registry.retained())
    }

    #[test]
    fn collector_receipt_is_not_backend_confirmation() -> Result<(), HttpError> {
        assert_eq!(export_case(Case::Pending)?, (0, 1));
        assert_eq!(export_case(Case::Confirmed)?, (0, 0));
        assert_eq!(export_case(Case::Gzip)?, (0, 0));
        assert_eq!(export_case(Case::CollectorFailure)?, (0, 1));
        assert_eq!(export_case(Case::Partial)?, (0, 1));
        assert_eq!(export_case(Case::NoQuery)?, (0, 1));
        assert_eq!(export_case(Case::QueryFailure)?, (0, 1));
        Ok(())
    }
}
