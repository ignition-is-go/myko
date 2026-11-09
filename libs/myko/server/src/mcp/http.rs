//! MCP HTTP transport handlers (POST + SSE) for `/myko/mcp`.

use std::{sync::Arc, time::Duration};

use myko::server::MykoServerContext;
use myko_auth::{AuthError, AuthSession, MykoAuthProvider};
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
/// # Errors
///
/// Returns an error when the HTTP request cannot be read or its response cannot be written.
pub async fn handle_post(
    stream: TcpStream,
    ctx: Arc<MykoServerContext>,
    server_info: Arc<ServerInfo>,
    head: HttpRequestHead,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    handle_post_with_auth(stream, ctx, server_info, head, None).await
}

/// Handle an MCP POST with verified caller authority.
/// # Errors
/// Returns an error when transport handling fails or authority expires.
pub async fn handle_post_with_auth(
    stream: TcpStream,
    ctx: Arc<MykoServerContext>,
    server_info: Arc<ServerInfo>,
    head: HttpRequestHead,
    auth: Option<AuthSession>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::select! {
        result = run_post(stream, ctx, server_info, head, auth.clone(), None) => result,
        () = super::invalidated(auth.as_ref()) => Ok(()),
    }
}

/// Admit MCP bootstrap publicly and authenticate every application request.
/// # Errors
/// Returns errors reading or writing the bounded HTTP exchange.
pub async fn handle_post_with_provider(
    stream: TcpStream,
    ctx: Arc<MykoServerContext>,
    server_info: Arc<ServerInfo>,
    head: HttpRequestHead,
    provider: Arc<MykoAuthProvider>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_post(stream, ctx, server_info, head, None, Some(provider)).await
}

async fn authenticate_post(
    provider: &MykoAuthProvider,
    head: &HttpRequestHead,
    public: bool,
) -> Result<Option<AuthSession>, AuthError> {
    let credentials = crate::router::credentials_from_head(head)?;
    if credentials.bearer.is_none() && public {
        return Ok(None);
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        if let Some(server) = provider.oauth_server() {
            credentials
                .bearer
                .as_deref()
                .ok_or(AuthError::Unauthenticated)
                .and_then(|token| server.authenticate(token))
        } else if provider.mcp_oauth().is_some() {
            provider.authenticate(credentials).await
        } else {
            provider.admit(credentials).await
        }
    })
    .await
    .unwrap_or(Err(AuthError::Unavailable))
    .map(Some)
}

const fn auth_error_status(error: AuthError) -> (u16, &'static str, &'static str) {
    match error {
        AuthError::Forbidden => (403, "Forbidden", "permission_denied"),
        AuthError::Unavailable => (503, "Service Unavailable", "authentication_unavailable"),
        AuthError::Expired => (401, "Unauthorized", "credential_expired"),
        AuthError::Revoked => (401, "Unauthorized", "session_revoked"),
        AuthError::Unauthenticated => (401, "Unauthorized", "authentication_required"),
    }
}

async fn write_auth_error(
    mut stream: TcpStream,
    provider: &MykoAuthProvider,
    error: AuthError,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (status, reason, code) = auth_error_status(error);
    let challenge = provider
        .mcp_oauth()
        .map_or_else(|| "Bearer".to_string(), myko_auth::OAuthResource::challenge);
    let mut headers = vec![("Content-Type", "application/json")];
    if status == 401 {
        headers.push(("WWW-Authenticate", &challenge));
    }
    let body = serde_json::to_vec(&serde_json::json!({
        "error": code, "error_description": error.to_string()
    }))?;
    write_full(&mut stream, status, reason, &headers, &body).await?;
    shutdown_cleanly(stream).await;
    Ok(())
}

async fn run_post(
    mut stream: TcpStream,
    ctx: Arc<MykoServerContext>,
    server_info: Arc<ServerInfo>,
    head: HttpRequestHead,
    mut auth: Option<AuthSession>,
    provider: Option<Arc<MykoAuthProvider>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(session) = &auth {
        session.check()?;
    }
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
            tracing::debug!("MCP POST body read error: {}", e);
            let _ = write_status(&mut stream, 400, "Bad Request").await;
            shutdown_cleanly(stream).await;
            return Ok(());
        }
    };

    let request = serde_json::from_slice::<McpRequest>(&body);
    if let Some(provider) = &provider {
        match authenticate_post(
            provider,
            &head,
            request.as_ref().is_ok_and(McpRequest::is_public_bootstrap),
        )
        .await
        {
            Ok(session) => auth = session,
            Err(error) => return write_auth_error(stream, provider, error).await,
        }
    }
    let filter = filter_from_head(&head);
    let executor = Executor::in_process(ctx, auth.clone());
    let response = match request {
        Ok(req) => tokio::select! {
            response = dispatch::handle_request(req, &filter, &executor, &server_info) => response,
            () = super::invalidated(auth.as_ref()) => {
                if let Some(provider) = &provider {return write_auth_error(stream, provider, AuthError::Unauthenticated).await;}
                return Ok(());
            }
        },
        Err(e) => Some(McpResponse::error(
            Value::Null,
            McpError::parse_error(e.to_string()),
        )),
    };

    if let Some(session) = &auth
        && let Err(error) = session.check()
    {
        if let Some(provider) = &provider {
            return write_auth_error(stream, provider, error).await;
        }
        return Err(error.into());
    }
    let Some(response) = response else {
        write_status(&mut stream, 202, "Accepted").await?;
        shutdown_cleanly(stream).await;
        return Ok(());
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
/// # Errors
///
/// Returns an error when the SSE response cannot be written.
pub async fn handle_sse(
    stream: TcpStream,
    ctx: Arc<MykoServerContext>,
    head: HttpRequestHead,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    handle_sse_with_auth(stream, ctx, head, None).await
}

/// Handle an SSE connection until its authenticated session ends.
/// # Errors
/// Returns an error when transport handling fails or authority expires.
pub async fn handle_sse_with_auth(
    stream: TcpStream,
    _ctx: Arc<MykoServerContext>,
    _head: HttpRequestHead,
    auth: Option<AuthSession>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::select! {
        result = run_sse(stream, auth.clone()) => result,
        () = super::invalidated(auth.as_ref()) => Ok(()),
    }
}

async fn run_sse(
    mut stream: TcpStream,
    auth: Option<AuthSession>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(session) = &auth {
        session.check()?;
    }
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
        if let Some(session) = &auth {
            session.check()?;
        }
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
#[must_use]
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

    let remaining = content_length.saturating_sub(body.len());
    body.reserve(remaining);
    let mut buf = vec![0u8; 4096.min(remaining)];
    let mut needed = remaining;
    while needed > 0 {
        let take = needed.min(buf.len());
        let Some(read_buffer) = buf.get_mut(..take) else {
            break;
        };
        let n = stream.read(read_buffer).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short body",
            ));
        }
        body.extend_from_slice(buf.get(..n).unwrap_or(&buf));
        needed = needed.saturating_sub(n);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_denial_is_not_an_authentication_challenge() {
        assert_eq!(
            auth_error_status(AuthError::Forbidden),
            (403, "Forbidden", "permission_denied")
        );
        for error in [
            AuthError::Unauthenticated,
            AuthError::Expired,
            AuthError::Revoked,
        ] {
            assert_eq!(auth_error_status(error).0, 401);
        }
        assert_eq!(auth_error_status(AuthError::Unavailable).0, 503);
    }

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
