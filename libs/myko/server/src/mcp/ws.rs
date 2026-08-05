//! MCP-over-WebSocket handler and shared WS handshake helpers.
//!
//! Used by the front-door router for both `/myko/mcp` (MCP JSON-RPC text
//! frames) and `/myko` (Myko gateway), the latter handing off to the
//! existing [`crate::ws_handler::WsHandler::handle_upgraded`].

use std::{fmt::Write as _, net::SocketAddr, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use myko::server::MykoServerContext;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, handshake::derive_accept_key, protocol::Role},
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
use crate::router::{HttpRequestHead, write_status};

const MCP_SUBPROTOCOL: &str = "mcp";

/// Upgrade `/myko/mcp` to a WebSocket carrying MCP JSON-RPC text frames.
/// # Errors
///
/// Returns an error when the WebSocket upgrade or MCP session fails.
pub async fn handle_mcp_ws_upgrade(
    stream: TcpStream,
    ctx: Arc<MykoServerContext>,
    server_info: Arc<ServerInfo>,
    head: HttpRequestHead,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = ClientFilters::from_strings(
        head.header(VISIBILITY_ALLOW_HEADER),
        head.header(VISIBILITY_DENY_HEADER),
        head.header(CALLABLE_ALLOW_HEADER),
        head.header(CALLABLE_DENY_HEADER),
    );

    let want_mcp_subprotocol = head
        .header("Sec-WebSocket-Protocol")
        .is_some_and(|v| {
            v.split(',')
                .any(|p| p.trim().eq_ignore_ascii_case(MCP_SUBPROTOCOL))
        });

    let ws_stream = match perform_handshake(stream, &head, want_mcp_subprotocol).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("MCP WS handshake failed: {}", e);
            return Ok(());
        }
    };

    run_mcp_loop(ws_stream, ctx, server_info, filter).await
}

/// Upgrade `/myko` to a WebSocket and hand off to the existing Myko gateway.
/// # Errors
///
/// Returns an error when the WebSocket upgrade or Myko session fails.
pub async fn handle_myko_ws_upgrade(
    stream: TcpStream,
    addr: SocketAddr,
    ctx: Arc<MykoServerContext>,
    head: HttpRequestHead,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_stream = match perform_handshake(stream, &head, false).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("Myko WS handshake failed from {}: {}", addr, e);
            return Ok(());
        }
    };
    crate::ws_handler::WsHandler::handle_upgraded(ws_stream, addr, ctx).await
}

async fn perform_handshake(
    mut stream: TcpStream,
    head: &HttpRequestHead,
    echo_mcp_subprotocol: bool,
) -> Result<WebSocketStream<TcpStream>, Box<dyn std::error::Error + Send + Sync>> {
    let version = head.header("Sec-WebSocket-Version").unwrap_or("");
    if version.trim() != "13" {
        let _ = write_status(&mut stream, 400, "Bad Request").await;
        return Err(format!("unsupported Sec-WebSocket-Version: {version}").into());
    }
    let key = head
        .header("Sec-WebSocket-Key")
        .ok_or("missing Sec-WebSocket-Key")?
        .trim()
        .to_string();

    let accept = derive_accept_key(key.as_bytes());

    let mut response = String::with_capacity(256);
    response.push_str("HTTP/1.1 101 Switching Protocols\r\n");
    response.push_str("Upgrade: websocket\r\n");
    response.push_str("Connection: Upgrade\r\n");
    let _ = write!(response, "Sec-WebSocket-Accept: {accept}\r\n");
    if echo_mcp_subprotocol {
        let _ = write!(response, "Sec-WebSocket-Protocol: {MCP_SUBPROTOCOL}\r\n");
    }
    response.push_str("\r\n");
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;

    let mut config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    config.max_message_size = Some(myko::WS_MAX_MESSAGE_SIZE_BYTES);
    config.max_frame_size = Some(myko::WS_MAX_FRAME_SIZE_BYTES);

    Ok(WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await)
}

async fn run_mcp_loop(
    ws_stream: WebSocketStream<TcpStream>,
    ctx: Arc<MykoServerContext>,
    info: Arc<ServerInfo>,
    filter: ClientFilters,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut write, mut read) = ws_stream.split();
    let filter = Arc::new(filter);
    let executor = Arc::new(Executor::InProcess(ctx));

    while let Some(frame) = read.next().await {
        let frame = match frame {
            Ok(f) => f,
            Err(e) => {
                tracing::debug!("MCP WS read error: {}", e);
                break;
            }
        };

        match frame {
            Message::Text(text) => {
                let request: McpRequest = match serde_json::from_str(&text) {
                    Ok(r) => r,
                    Err(e) => {
                        let response = McpResponse::error(
                            serde_json::Value::Null,
                            McpError::parse_error(e.to_string()),
                        );
                        if send_response(&mut write, &response).await.is_err() {
                            break;
                        }
                        continue;
                    }
                };

                if let Some(response) =
                    dispatch::handle_request(request, &filter, &executor, &info).await
                    && send_response(&mut write, &response).await.is_err()
                {
                    break;
                }
            }
            Message::Binary(_) => {
                let response = McpResponse::error(
                    serde_json::Value::Null,
                    McpError {
                        code: McpError::INVALID_REQUEST,
                        message: "MCP-over-WebSocket uses text frames only".to_string(),
                        data: None,
                    },
                );
                if send_response(&mut write, &response).await.is_err() {
                    break;
                }
            }
            Message::Ping(payload) => {
                if write.send(Message::Pong(payload)).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => break,
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    Ok(())
}

async fn send_response<W>(write: &mut W, response: &McpResponse) -> Result<(), ()>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let body = serde_json::to_string(response).map_err(|_| ())?;
    write.send(Message::Text(body.into())).await.map_err(|_| ())
}
