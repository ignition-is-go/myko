//! MCP HTTP transport handlers (POST + SSE) for `/myko/mcp`.

use std::{sync::Arc, time::Duration};

use myko::server::CellServerCtx;
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::sleep,
};

use super::{
    dispatch::{self, ServerInfo},
    exec::Executor,
    filter::{
        CALLABLE_ALLOW_HEADER, CALLABLE_DENY_HEADER, ClientFilters, VISIBILITY_ALLOW_HEADER,
        VISIBILITY_DENY_HEADER,
    },
    types::{McpError, McpRequest, McpResponse},
};
use crate::router::{HttpRequestHead, shutdown_cleanly, write_full, write_status};

/// Cap on incoming MCP JSON-RPC body size.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// SSE keepalive comment interval.
const SSE_KEEPALIVE: Duration = Duration::from_secs(15);

/// Handle `POST /myko/mcp`.
pub async fn handle_post(
    mut stream: TcpStream,
    ctx: Arc<CellServerCtx>,
    head: HttpRequestHead,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let content_length: usize = head
        .header("Content-Length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    if content_length > MAX_BODY_BYTES {
        let _ = write_status(&mut stream, 413, "Payload Too Large").await;
        shutdown_cleanly(stream).await;
        return Ok(());
    }

    let body = match read_body(&mut stream, &head, content_length).await {
        Ok(b) => b,
        Err(e) => {
            log::debug!("MCP POST body read error: {}", e);
            let _ = write_status(&mut stream, 400, "Bad Request").await;
            shutdown_cleanly(stream).await;
            return Ok(());
        }
    };

    let filter = filter_from_head(&head);
    let info = ServerInfo::default();
    let executor = Executor::InProcess(ctx);

    let response: McpResponse = match serde_json::from_slice::<McpRequest>(&body) {
        Ok(req) => {
            // Notifications (no id, no response expected) get an empty 204
            // body anyway — MCP HTTP keeps the request/response shape.
            match dispatch::handle_request(req, &filter, &executor, &info).await {
                Some(r) => r,
                None => McpResponse::success(Value::Null, Value::Null),
            }
        }
        Err(e) => McpResponse::error(Value::Null, McpError::parse_error(e.to_string())),
    };

    let body = serde_json::to_vec(&response).unwrap_or_else(|_| b"{}".to_vec());
    let write_result = write_full(
        &mut stream,
        200,
        "OK",
        &[("Content-Type", "application/json")],
        &body,
    )
    .await;
    shutdown_cleanly(stream).await;
    write_result?;
    Ok(())
}

/// Handle `GET /myko/mcp` with `Accept: text/event-stream`.
///
/// v1: writes the SSE response head and emits a keepalive comment every
/// 15 s. No notifications are pushed yet — reactive query → MCP
/// notification wiring is future work.
pub async fn handle_sse(
    mut stream: TcpStream,
    _ctx: Arc<CellServerCtx>,
    _head: HttpRequestHead,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let head = "HTTP/1.1 200 OK\r\n\
                Content-Type: text/event-stream\r\n\
                Cache-Control: no-cache\r\n\
                Connection: keep-alive\r\n\
                X-Accel-Buffering: no\r\n\
                \r\n";
    stream.write_all(head.as_bytes()).await?;
    stream.flush().await?;

    loop {
        sleep(SSE_KEEPALIVE).await;
        if stream.write_all(b": keepalive\n\n").await.is_err() {
            break;
        }
        if stream.flush().await.is_err() {
            break;
        }
    }

    Ok(())
}

/// Build a `ClientFilters` from the request headers.
pub fn filter_from_head(head: &HttpRequestHead) -> ClientFilters {
    ClientFilters::from_strings(
        head.header(VISIBILITY_ALLOW_HEADER),
        head.header(VISIBILITY_DENY_HEADER),
        head.header(CALLABLE_ALLOW_HEADER),
        head.header(CALLABLE_DENY_HEADER),
    )
}

async fn read_body(
    stream: &mut TcpStream,
    head: &HttpRequestHead,
    content_length: usize,
) -> std::io::Result<Vec<u8>> {
    let mut body = head.leftover_body.clone();
    if body.len() >= content_length {
        body.truncate(content_length);
        return Ok(body);
    }

    let remaining = content_length - body.len();
    body.reserve(remaining);
    let mut buf = vec![0u8; 4096.min(remaining)];
    let mut needed = remaining;
    while needed > 0 {
        let take = needed.min(buf.len());
        let n = stream.read(&mut buf[..take]).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short body",
            ));
        }
        body.extend_from_slice(&buf[..n]);
        needed -= n;
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head_with(headers: Vec<(&str, &str)>) -> HttpRequestHead {
        HttpRequestHead {
            method: "POST".to_string(),
            path: "/myko/mcp".to_string(),
            headers: headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            leftover_body: Vec::new(),
        }
    }

    #[test]
    fn filter_from_head_parses_allow_and_deny() {
        let head = head_with(vec![
            (VISIBILITY_ALLOW_HEADER, "query:*"),
            (VISIBILITY_DENY_HEADER, "command:Delete*"),
        ]);
        let filter = filter_from_head(&head);
        assert!(filter.tool_visible("query:GetAllTargets"));
        assert!(!filter.tool_visible("command:DeleteThing"));
        assert!(!filter.tool_visible("report:Health"));
    }

    #[test]
    fn filter_from_head_with_no_headers_allows_all() {
        let head = head_with(vec![]);
        let filter = filter_from_head(&head);
        assert!(filter.tool_visible("anything"));
    }

    #[test]
    fn filter_from_head_parses_callable_allow() {
        let head = head_with(vec![(
            CALLABLE_ALLOW_HEADER,
            r#"{"command:RunPlaybook":{"playbook_id":["site"]}}"#,
        )]);
        let filter = filter_from_head(&head);
        assert!(
            filter
                .tool_callable("command:RunPlaybook", &serde_json::json!({"playbook_id":"site"}))
                .is_ok()
        );
        assert!(
            filter
                .tool_callable(
                    "command:RunPlaybook",
                    &serde_json::json!({"playbook_id":"danger"})
                )
                .is_err()
        );
    }

    #[test]
    fn filter_from_head_parses_callable_deny() {
        let head = head_with(vec![(
            CALLABLE_DENY_HEADER,
            r#"{"command:Tag":{"namespace":["prod"]}}"#,
        )]);
        let filter = filter_from_head(&head);
        assert!(
            filter
                .tool_callable("command:Tag", &serde_json::json!({"namespace": "staging"}))
                .is_ok()
        );
        assert!(
            filter
                .tool_callable("command:Tag", &serde_json::json!({"namespace": "prod"}))
                .is_err()
        );
    }
}
