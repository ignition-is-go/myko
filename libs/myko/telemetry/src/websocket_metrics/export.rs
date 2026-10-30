//! Add connection-owned cumulative sums after SDK aggregation.
//!
//! A successful collector export retires disconnected clients.
use std::{
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
use opentelemetry_http::{HttpClient, HttpError};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;

use super::Registry;

#[derive(Debug)]
pub struct Client {
    http: reqwest::blocking::Client,
    registry: Arc<Registry>,
}

impl Client {
    pub fn new(registry: Arc<Registry>) -> Result<Self, HttpError> {
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
        })
    }
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
            self.registry.acknowledge(&snapshot);
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

    fn registry() -> Arc<Registry> {
        let registry = Arc::new(Registry::default());
        registry.open("client");
        registry.record("client", "out", "QueryResponse", Some("query"), Some(123));
        registry.close("client");
        registry
    }

    #[derive(Clone, Copy)]
    enum Case {
        Accepted,
        Gzip,
        CollectorFailure,
        Partial,
    }

    fn export_case(case: Case) -> Result<(u64, u64), HttpError> {
        let status = if matches!(case, Case::CollectorFailure) {
            503
        } else {
            200
        };
        let partial = matches!(case, Case::Partial);
        let gzip = matches!(case, Case::Gzip);
        let registry = registry();
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
            let request = ExportMetricsServiceRequest::decode(decode(&body, gzip)?.as_slice())?;
            let count = request
                .resource_metrics
                .iter()
                .flat_map(|resource| &resource.scope_metrics)
                .flat_map(|scope| &scope.metrics)
                .count();
            assert_eq!(count, 3);
            let content_type = "application/x-protobuf";
            let response_body = exporter_response;
            write!(
                stream,
                "HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            )?;
            stream.write_all(&response_body)?;
            Ok(())
        });
        let client = Client {
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()?,
            registry: registry.clone(),
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
    fn successful_export_retires_closed_clients() -> Result<(), HttpError> {
        assert_eq!(export_case(Case::Accepted)?, (0, 0));
        assert_eq!(export_case(Case::Gzip)?, (0, 0));
        assert_eq!(export_case(Case::CollectorFailure)?, (0, 1));
        assert_eq!(export_case(Case::Partial)?, (0, 1));
        Ok(())
    }
}
