//! MCP HTTP transport handlers (POST + SSE) for `/myko/mcp`.

use std::{sync::Arc, time::Duration};

use myko::server::CellServerCtx;
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};
use uuid::Uuid;

use super::{
    dispatch::{self, ServerInfo},
    exec::Executor,
    filter::{
        CALLABLE_ALLOW_HEADER, CALLABLE_DENY_HEADER, ClientFilters, VISIBILITY_ALLOW_HEADER,
        VISIBILITY_DENY_HEADER,
    },
    session::{ClientInfo, McpSessionChannel, McpSessionEvent, McpSessionObserver},
    types::{McpError, McpRequest, McpResponse},
};
use crate::router::{HttpRequestHead, shutdown_cleanly, write_full, write_status};

/// Cap on incoming MCP JSON-RPC body size.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// SSE keepalive comment interval.
const SSE_KEEPALIVE: Duration = Duration::from_secs(15);

/// Streamable HTTP transport session correlator header. Server assigns on
/// `initialize` response; client echoes on every subsequent request.
pub const MCP_SESSION_ID_HEADER: &str = "Mcp-Session-Id";

/// Handle `POST /myko/mcp`.
pub async fn handle_post(
    mut stream: TcpStream,
    ctx: Arc<CellServerCtx>,
    server_info: Arc<ServerInfo>,
    head: HttpRequestHead,
    observer: Option<Arc<dyn McpSessionObserver>>,
    custom_registry: super::custom::CustomMcpRegistry,
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

    // Parse the request once so we can both dispatch it and observe the
    // initialize lifecycle event before sending the response.
    let parsed = serde_json::from_slice::<McpRequest>(&body);

    // Identity assignment: on initialize, mint a new Mcp-Session-Id and
    // return it as a response header. On every other request the client
    // echoes a previously-assigned id; pass it through unchanged.
    let is_initialize = matches!(&parsed, Ok(req) if req.method == "initialize");
    let assigned_session_id: Option<String> = if is_initialize {
        Some(Uuid::new_v4().to_string())
    } else {
        head.header(MCP_SESSION_ID_HEADER).map(|s| s.to_string())
    };

    // Carry the Mcp-Session-Id into the executor so command handlers
    // receive it via `RequestContext::mcp_session_id` and can identify
    // the HTTP-MCP caller without a `client_id`.
    let executor = Executor::InProcess {
        ctx,
        caller_session_id: assigned_session_id.as_deref().map(Arc::from),
        custom_registry,
    };

    let response: McpResponse = match parsed {
        Ok(req) => {
            match dispatch::handle_request(req, &filter, &executor, &server_info).await {
                Some(r) => r,
                None => McpResponse::success(Value::Null, Value::Null),
            }
        }
        Err(e) => McpResponse::error(Value::Null, McpError::parse_error(e.to_string())),
    };

    // Fire the observer after the response shape is decided but before the
    // bytes are flushed — gives downstream code (e.g. marshal-daemon) a
    // chance to materialise per-session state synchronously, so the very
    // next request from this client sees it in the registry.
    if is_initialize {
        if let (Some(sid), Some(observer)) = (&assigned_session_id, &observer) {
            let client_info = extract_client_info(&body);
            observer.on_session_event(McpSessionEvent::Started {
                session_id: sid.clone(),
                client_info,
                user_agent: head.header("User-Agent").map(|s| s.to_string()),
            });
        }
    } else if let (Some(sid), Some(observer)) = (&assigned_session_id, &observer) {
        // Activity ping for every non-initialize request — lets downstream
        // reapers keep HTTP-MCP sessions alive when the client doesn't open
        // SSE eagerly. Synchronous + cheap by design (observers should only
        // bump an in-memory timestamp here).
        observer.on_session_event(McpSessionEvent::Activity {
            session_id: sid.clone(),
        });
    }

    let body_out = serde_json::to_vec(&response).unwrap_or_else(|_| b"{}".to_vec());
    let mut extra_headers: Vec<(&str, &str)> = vec![("Content-Type", "application/json")];
    if let Some(sid) = assigned_session_id.as_deref() {
        extra_headers.push((MCP_SESSION_ID_HEADER, sid));
    }
    let write_result = write_full(&mut stream, 200, "OK", &extra_headers, &body_out).await;
    shutdown_cleanly(stream).await;
    write_result?;
    Ok(())
}

/// Best-effort: pull `clientInfo` out of an initialize request body. Returns
/// `None` if the body isn't a parseable initialize.
fn extract_client_info(body: &[u8]) -> Option<ClientInfo> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let ci = v.get("params")?.get("clientInfo")?;
    Some(ClientInfo {
        name: ci.get("name")?.as_str()?.to_string(),
        version: ci.get("version").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        title: ci.get("title").and_then(|x| x.as_str()).map(|s| s.to_string()),
    })
}

/// Handle `GET /myko/mcp` with `Accept: text/event-stream`.
///
/// Keeps the SSE channel open and forwards JSON-RPC notifications
/// addressed to this session into the stream. The session is identified
/// by the `Mcp-Session-Id` header the client echoes from its earlier
/// `initialize` response; clients that GET the SSE without first POSTing
/// `initialize` get an anonymous keepalive-only stream.
pub async fn handle_sse(
    mut stream: TcpStream,
    _ctx: Arc<CellServerCtx>,
    head: HttpRequestHead,
    observer: Option<Arc<dyn McpSessionObserver>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let session_id = head.header(MCP_SESSION_ID_HEADER).map(|s| s.to_string());
    let response_head = "HTTP/1.1 200 OK\r\n\
                         Content-Type: text/event-stream\r\n\
                         Cache-Control: no-cache\r\n\
                         Connection: keep-alive\r\n\
                         X-Accel-Buffering: no\r\n\
                         \r\n";
    stream.write_all(response_head.as_bytes()).await?;
    stream.flush().await?;

    // Push sink: observers hold the channel and `send_notification` into
    // it; this loop drains it onto the wire. Unbounded so observers
    // running synchronous code (the trait is sync) never block — if a
    // misbehaving client stops draining, the receiver buffer will grow
    // and the keepalive write below will eventually fail, breaking the
    // loop and dropping the channel.
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    if let (Some(sid), Some(obs)) = (session_id.as_ref(), observer.as_ref()) {
        obs.on_session_event(McpSessionEvent::SseConnected {
            session_id: sid.clone(),
            channel: Arc::new(McpSessionChannel::new(tx)),
        });
    }
    // Drop our local end so when the observer-held channel goes away the
    // receiver closes cleanly. (No-op if no observer was registered —
    // tx was just moved into the McpSessionChannel above.)

    let mut keepalive = tokio::time::interval(SSE_KEEPALIVE);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                if stream.write_all(b": keepalive\n\n").await.is_err()
                    || stream.flush().await.is_err()
                {
                    break;
                }
            }
            frame = rx.recv() => {
                match frame {
                    Some(f) => {
                        if stream.write_all(f.as_bytes()).await.is_err()
                            || stream.flush().await.is_err()
                        {
                            break;
                        }
                    }
                    None => break, // sender side dropped
                }
            }
        }
    }

    // SSE channel closed — best-effort `Ended` event. Clients that go
    // away without ever opening an SSE leave their session entity in
    // place to be reaped by an external sweeper; this only fires for
    // sessions that *did* open the stream.
    if let (Some(sid), Some(obs)) = (session_id, observer) {
        obs.on_session_event(McpSessionEvent::Ended { session_id: sid });
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
                .tool_callable(
                    "command:RunPlaybook",
                    &serde_json::json!({"playbook_id":"site"})
                )
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
