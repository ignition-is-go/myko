//! Standard OAuth endpoints plus the Myko command bridge for custom login UI.
use crate::router::{HttpRequestHead, shutdown_cleanly, write_full};
use myko::{
    oauth_flow::{OAuthFlowHandler, OAuthFlowOperation, OAuthFlowReply},
    server::MykoServerContext,
};
use myko_auth::{MykoAuthProvider, oauth::server::OAuthError};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};
use tokio::{io::AsyncReadExt, net::TcpStream};
type Error = Box<dyn std::error::Error + Send + Sync>;
struct FlowHandler {
    id: String,
    provider: Weak<MykoAuthProvider>,
    runtime: tokio::runtime::Handle,
}
impl OAuthFlowHandler for FlowHandler {
    fn authority_id(&self) -> &str {
        &self.id
    }
    fn handle(&self, operation: OAuthFlowOperation) -> Result<OAuthFlowReply, String> {
        let provider = self
            .provider
            .upgrade()
            .ok_or("OAuth authority unavailable")?;
        self.runtime.block_on(provider.oauth_flow(operation))
    }
}
pub fn install(
    ctx: &MykoServerContext,
    provider: &Arc<MykoAuthProvider>,
) -> Result<Option<Arc<dyn OAuthFlowHandler>>, Error> {
    let Some(server) = provider.oauth_server() else {
        return Ok(None);
    };
    let handler = Arc::new(FlowHandler {
        id: server.id().into(),
        provider: Arc::downgrade(provider),
        runtime: tokio::runtime::Handle::try_current()?,
    });
    Ok(Some(myko::oauth_flow::install(ctx, handler)?))
}
pub fn handles(provider: &MykoAuthProvider, path: &str) -> bool {
    provider.oauth_server().is_some_and(|server| {
        path == server.metadata_path()
            || path == "/.well-known/oauth-authorization-server"
            || ["authorize", "token", "register", "revoke"]
                .iter()
                .any(|name| path == server.endpoint_path(name))
    })
}
fn form(body: &[u8]) -> Result<HashMap<String, String>, OAuthError> {
    let mut args = HashMap::new();
    for (key, value) in url::form_urlencoded::parse(body) {
        if args.len() >= 32 || args.insert(key.into_owned(), value.into_owned()).is_some() {
            return Err(OAuthError {
                code: "invalid_request",
                message: "Duplicate or excessive authorization parameters",
                status: 400,
            });
        }
    }
    Ok(args)
}
async fn body(stream: &mut TcpStream, head: &HttpRequestHead) -> Result<Vec<u8>, Error> {
    if head.header("transfer-encoding").is_some() {
        return Err("Chunked OAuth requests are unsupported".into());
    }
    let length = head
        .header("content-length")
        .unwrap_or("0")
        .parse::<usize>()?;
    if length > 32768 || head.leftover_body.len() > length {
        return Err("OAuth request body exceeds its bound".into());
    }
    let mut bytes = head.leftover_body.clone();
    let mut chunk = [0u8; 4096];
    while bytes.len() < length {
        let remaining = length
            .checked_sub(bytes.len())
            .ok_or("Invalid body length")?;
        let slice = chunk.get_mut(..remaining.min(4096)).ok_or("body length")?;
        let count = stream.read(slice).await?;
        if count == 0 {
            return Err("Incomplete OAuth request".into());
        }
        bytes.extend_from_slice(slice.get(..count).ok_or("body length")?);
    }
    Ok(bytes)
}
async fn send(
    mut stream: TcpStream,
    status: u16,
    location: Option<String>,
    value: Option<Value>,
) -> Result<(), Error> {
    let bytes = value
        .as_ref()
        .map(serde_json::to_vec)
        .transpose()?
        .unwrap_or_default();
    let mut headers = vec![
        ("Content-Type", "application/json"),
        ("Cache-Control", "no-store"),
        ("Pragma", "no-cache"),
        ("Referrer-Policy", "no-referrer"),
        ("Access-Control-Allow-Origin", "*"),
        ("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
        ("Access-Control-Allow-Headers", "content-type"),
    ];
    if let Some(location) = &location {
        headers.push(("Location", location));
    }
    let reason = match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        302 => "Found",
        503 => "Service Unavailable",
        _ => "Bad Request",
    };
    write_full(&mut stream, status, reason, &headers, &bytes).await?;
    shutdown_cleanly(stream).await;
    Ok(())
}
const fn invalid(message: &'static str) -> OAuthError {
    OAuthError {
        code: "invalid_request",
        message,
        status: 400,
    }
}
pub async fn handle(
    mut stream: TcpStream,
    head: HttpRequestHead,
    provider: Arc<MykoAuthProvider>,
) -> Result<(), Error> {
    let server = provider
        .oauth_server()
        .ok_or("OAuth authorization is not configured")?;
    let path = head.path.split('?').next().unwrap_or(&head.path);
    if head.method == "OPTIONS" {
        return send(stream, 204, None, None).await;
    }
    let mut location = None;
    let mut status = 200;
    let result: Result<Option<Value>, OAuthError> = if head.method == "GET"
        && (path == server.metadata_path() || path == "/.well-known/oauth-authorization-server")
    {
        Ok(Some(server.metadata()))
    } else if head.method == "GET" && path == server.endpoint_path("authorize") {
        let query = head.path.split_once('?').map_or("", |(_, query)| query);
        form(query.as_bytes())
            .and_then(|args| server.begin(&args))
            .map(|url| {
                location = Some(url);
                status = 302;
                None
            })
    } else if head.method == "POST" {
        let bytes =
            tokio::time::timeout(std::time::Duration::from_secs(10), body(&mut stream, &head))
                .await;
        match bytes {
            Ok(Ok(bytes)) if path == server.endpoint_path("register") => {
                if head
                    .header("content-type")
                    .is_some_and(|value| value.split(';').next() == Some("application/json"))
                {
                    serde_json::from_slice::<Value>(&bytes)
                        .map_err(|_| invalid("Invalid registration document"))
                        .and_then(|value| server.register(&value))
                        .map(|value| {
                            status = 201;
                            Some(value)
                        })
                } else {
                    Err(invalid("Registration requires JSON"))
                }
            }
            Ok(Ok(bytes))
                if head.header("content-type").is_some_and(|value| {
                    value.split(';').next() == Some("application/x-www-form-urlencoded")
                }) =>
            {
                form(&bytes).and_then(|args| {
                    if path == server.endpoint_path("token") {
                        server.token(&args).map(Some)
                    } else if path == server.endpoint_path("revoke") {
                        server.revoke(&args).map(|()| None)
                    } else {
                        Err(invalid("Unsupported OAuth endpoint"))
                    }
                })
            }
            _ => Err(invalid("Invalid or incomplete OAuth request body")),
        }
    } else {
        Err(invalid("Unsupported OAuth request method"))
    };
    match result {
        Ok(value) => send(stream, status, location, value).await,
        Err(error) => {
            send(
                stream,
                error.status,
                None,
                Some(json!({"error":error.code,"error_description":error.message})),
            )
            .await
        }
    }
}
