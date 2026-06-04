//! HTTP/1.1 front-door router for the Myko server.
//!
//! Reads the HTTP request line and headers from a TCP stream, then routes
//! the connection to the appropriate handler:
//!
//! - `GET /myko` with `Upgrade: websocket` → existing WS gateway.
//! - `GET /myko/mcp` with `Upgrade: websocket` → MCP-over-WebSocket.
//! - `GET /myko/mcp` with `Accept: text/event-stream` → MCP SSE.
//! - `POST /myko/mcp` → MCP HTTP JSON-RPC.
//! - Anything else → `404 Not Found`.
//!
//! Header parsing is bounded (8 KB) and lower-cased for lookup.

use std::{net::SocketAddr, sync::Arc};

use myko::server::CellServerCtx;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{mcp, mcp::dispatch::ServerInfo, ws_handler::WsHandler};

/// Cap on the header section (request line + headers).
const MAX_HEADER_BYTES: usize = 8 * 1024;

/// Maximum number of headers to parse.
const MAX_HEADERS: usize = 64;

/// Parsed HTTP request line + headers.
#[derive(Debug, Clone)]
pub struct HttpRequestHead {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    /// Bytes following the header terminator that were already read off the
    /// socket. Will be empty for clean WS handshakes and short POSTs whose
    /// body arrived in a separate TCP segment.
    pub leftover_body: Vec<u8>,
}

impl HttpRequestHead {
    /// Case-insensitive header lookup. Returns the first matching value.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// `true` if the request is a WebSocket upgrade.
    pub fn is_websocket_upgrade(&self) -> bool {
        let upgrade = self
            .header("Upgrade")
            .map(|v| v.eq_ignore_ascii_case("websocket"))
            .unwrap_or(false);
        let connection_has_upgrade = self
            .header("Connection")
            .map(|v| {
                v.split(',')
                    .any(|p| p.trim().eq_ignore_ascii_case("upgrade"))
            })
            .unwrap_or(false);
        upgrade && connection_has_upgrade
    }

    /// `true` if the client wants an SSE stream.
    pub fn wants_event_stream(&self) -> bool {
        self.header("Accept")
            .map(|v| {
                v.split(',').any(|part| {
                    // Strip media-type params like `;q=0.9`.
                    let media = part.split(';').next().unwrap_or("").trim();
                    media.eq_ignore_ascii_case("text/event-stream")
                })
            })
            .unwrap_or(false)
    }
}

/// Read and parse one HTTP request head from the socket.
///
/// Returns `Ok(None)` if the peer closed before sending a complete request.
pub async fn read_request_head(stream: &mut TcpStream) -> std::io::Result<Option<HttpRequestHead>> {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];

    let header_end = loop {
        if buffer.len() > MAX_HEADER_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP header section exceeded 8 KB",
            ));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk[..n]);

        if let Some(idx) = find_header_terminator(&buffer) {
            break idx;
        }
    };

    let mut headers_buf = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut req = httparse::Request::new(&mut headers_buf);
    let status = req
        .parse(&buffer[..header_end])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    if !status.is_complete() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "incomplete HTTP request",
        ));
    }

    let method = req.method.unwrap_or("").to_string();
    let path = req.path.unwrap_or("").to_string();
    let headers = req
        .headers
        .iter()
        .map(|h| {
            (
                h.name.to_string(),
                String::from_utf8_lossy(h.value).into_owned(),
            )
        })
        .collect();

    let leftover_body = buffer[header_end..].to_vec();

    Ok(Some(HttpRequestHead {
        method,
        path,
        headers,
        leftover_body,
    }))
}

fn find_header_terminator(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

/// Route a freshly-accepted TCP connection.
pub async fn route_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    ctx: Arc<CellServerCtx>,
    server_info: Arc<ServerInfo>,
    mcp_session_observer: Option<mcp::SharedObserver>,
    custom_mcp_registry: mcp::CustomMcpRegistry,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let head = match read_request_head(&mut stream).await {
        Ok(Some(h)) => h,
        Ok(None) => return Ok(()),
        Err(e) => {
            log::debug!("HTTP parse error from {}: {}", addr, e);
            let _ = write_status(&mut stream, 400, "Bad Request").await;
            shutdown_cleanly(stream).await;
            return Ok(());
        }
    };

    log::trace!("router accept {} {} from {}", head.method, head.path, addr,);

    let path = head.path.split('?').next().unwrap_or(&head.path);

    match (head.method.as_str(), path) {
        ("GET", p) if p == "/myko" || p.starts_with("/myko?") => {
            if !head.is_websocket_upgrade() {
                let _ = write_status(&mut stream, 426, "Upgrade Required").await;
                shutdown_cleanly(stream).await;
                return Ok(());
            }
            // The existing WS handler does its own handshake via
            // tungstenite, which requires the unmodified raw bytes. We've
            // already consumed the request, so finish the handshake here
            // and pass the upgraded stream to the WS gateway.
            handle_ws_upgrade(stream, addr, ctx, server_info, head, WsTarget::Myko).await
        }
        ("GET", "/myko/mcp") if head.is_websocket_upgrade() => {
            handle_ws_upgrade(stream, addr, ctx, server_info, head, WsTarget::Mcp).await
        }
        ("GET", "/myko/mcp") if head.wants_event_stream() => {
            mcp::http::handle_sse(stream, ctx, head, mcp_session_observer).await
        }
        ("POST", "/myko/mcp") => {
            mcp::http::handle_post(
                stream,
                ctx,
                server_info,
                head,
                mcp_session_observer,
                custom_mcp_registry,
            )
            .await
        }
        ("GET", "/myko/mcp") => {
            // No SSE accept, no WS upgrade — caller probably wants a quick
            // status check or hit the URL in a browser.
            let body = b"{\"status\":\"ok\",\"endpoint\":\"/myko/mcp\",\"transports\":[\"POST\",\"WebSocket\",\"SSE\"]}";
            let result = write_full(
                &mut stream,
                200,
                "OK",
                &[("Content-Type", "application/json")],
                body,
            )
            .await;
            shutdown_cleanly(stream).await;
            result.map_err(Into::into)
        }
        _ => {
            let _ = write_status(&mut stream, 404, "Not Found").await;
            shutdown_cleanly(stream).await;
            Ok(())
        }
    }
}

enum WsTarget {
    Myko,
    Mcp,
}

async fn handle_ws_upgrade(
    stream: TcpStream,
    addr: SocketAddr,
    ctx: Arc<CellServerCtx>,
    server_info: Arc<ServerInfo>,
    head: HttpRequestHead,
    target: WsTarget,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Reject if the client sent body bytes alongside the handshake — a
    // compliant client waits for our 101 before sending WS frames.
    if !head.leftover_body.is_empty() {
        log::warn!(
            "Rejecting WS upgrade from {} with {} leftover body bytes",
            addr,
            head.leftover_body.len()
        );
        let mut stream = stream;
        let _ = write_status(&mut stream, 400, "Bad Request").await;
        shutdown_cleanly(stream).await;
        return Ok(());
    }

    match target {
        WsTarget::Myko => mcp::ws::handle_myko_ws_upgrade(stream, addr, ctx, head).await,
        WsTarget::Mcp => mcp::ws::handle_mcp_ws_upgrade(stream, ctx, server_info, head).await,
    }
}

/// Write a bare `HTTP/1.1 <code> <reason>` response with no body and close.
pub async fn write_status(stream: &mut TcpStream, code: u16, reason: &str) -> std::io::Result<()> {
    write_full(stream, code, reason, &[("Content-Length", "0")], b"").await
}

/// Write a full HTTP/1.1 response with headers and body.
pub async fn write_full(
    stream: &mut TcpStream,
    code: u16,
    reason: &str,
    extra_headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<()> {
    let mut head = format!("HTTP/1.1 {} {}\r\n", code, reason);
    head.push_str("Connection: close\r\n");
    if !extra_headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("Content-Length"))
    {
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    for (k, v) in extra_headers {
        head.push_str(&format!("{}: {}\r\n", k, v));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    if !body.is_empty() {
        stream.write_all(body).await?;
    }
    stream.flush().await?;
    Ok(())
}

/// Close an HTTP response stream cleanly.
///
/// `TcpStream::drop` calls `close(2)`, and Linux sends a RST instead of a
/// FIN if the socket's receive buffer is non-empty (RFC-defined behavior).
/// HTTP/1.1 clients commonly hold the socket open for keep-alive or
/// pipeline a second request, so by the time we finish writing the
/// response, the kernel often has bytes we never read.
///
/// This helper:
/// 1. Shuts down the write half (sends FIN).
/// 2. Drains the read half until EOF or a short timeout, so the kernel can
///    close cleanly without RST.
pub async fn shutdown_cleanly(mut stream: TcpStream) {
    use tokio::io::AsyncReadExt;

    let _ = stream.shutdown().await;
    let mut buf = [0u8; 1024];
    let _ = tokio::time::timeout(std::time::Duration::from_millis(250), async {
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => continue,
            }
        }
    })
    .await;
}

// Wire WsHandler in scope so the path is documented at the module top.
#[allow(dead_code)]
fn _ws_handler_in_scope() -> WsHandler {
    WsHandler
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_head(headers: Vec<(&str, &str)>) -> HttpRequestHead {
        HttpRequestHead {
            method: "GET".to_string(),
            path: "/myko/mcp".to_string(),
            headers: headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            leftover_body: Vec::new(),
        }
    }

    #[test]
    fn header_lookup_is_case_insensitive() {
        let head = make_head(vec![("Content-Type", "application/json")]);
        assert_eq!(head.header("content-type"), Some("application/json"));
        assert_eq!(head.header("CONTENT-TYPE"), Some("application/json"));
    }

    #[test]
    fn websocket_upgrade_requires_both_headers() {
        let head = make_head(vec![("Upgrade", "websocket"), ("Connection", "Upgrade")]);
        assert!(head.is_websocket_upgrade());

        let head_no_conn = make_head(vec![("Upgrade", "websocket")]);
        assert!(!head_no_conn.is_websocket_upgrade());

        let head_no_upgrade = make_head(vec![("Connection", "Upgrade")]);
        assert!(!head_no_upgrade.is_websocket_upgrade());
    }

    #[test]
    fn connection_header_accepts_lists() {
        let head = make_head(vec![
            ("Upgrade", "websocket"),
            ("Connection", "keep-alive, Upgrade"),
        ]);
        assert!(head.is_websocket_upgrade());
    }

    #[test]
    fn accept_header_detects_sse() {
        let head = make_head(vec![("Accept", "text/event-stream")]);
        assert!(head.wants_event_stream());

        let head_html = make_head(vec![("Accept", "text/html")]);
        assert!(!head_html.wants_event_stream());

        let head_mixed = make_head(vec![("Accept", "text/html, text/event-stream;q=0.9")]);
        assert!(head_mixed.wants_event_stream());
    }

    #[test]
    fn header_terminator_is_found() {
        let req = b"GET / HTTP/1.1\r\nHost: x\r\n\r\nbody";
        let idx = find_header_terminator(req).expect("terminator must be found");
        assert_eq!(&req[idx..], b"body");
        assert_eq!(find_header_terminator(b"no terminator here"), None);
    }
}
