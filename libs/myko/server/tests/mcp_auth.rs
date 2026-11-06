#![allow(clippy::panic_in_result_fn)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use futures_util::{SinkExt, StreamExt};
use myko::{
    command::{CommandContext, CommandError, CommandHandler},
    myko_command,
};
use myko_auth::{
    AuthError, AuthSession, AuthenticatedIdentity, AuthorizationRequest, Authorizer, Principal,
    PrincipalKind,
};
use myko_server::{MykoServer, mcp, router::read_request_head};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::tungstenite::Message;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

static MUTATIONS: AtomicUsize = AtomicUsize::new(0);

#[myko_command(String)]
struct CallerIdentity {}
impl CommandHandler for CallerIdentity {
    fn execute(self, ctx: CommandContext) -> std::result::Result<String, CommandError> {
        let identity = ctx
            .req
            .auth
            .as_ref()
            .and_then(AuthSession::identity)
            .map_or("missing", |identity| identity.principal.subject.as_str());
        Ok(format!(
            "{identity}:{}",
            ctx.req.client_id.as_deref().unwrap_or("internal")
        ))
    }
}

#[myko_command(String)]
struct ForbiddenMutation {}
impl CommandHandler for ForbiddenMutation {
    fn execute(self, _ctx: CommandContext) -> std::result::Result<String, CommandError> {
        MUTATIONS.fetch_add(1, Ordering::SeqCst);
        Ok("mutated".into())
    }
}

struct Policy;
impl Authorizer for Policy {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        request: AuthorizationRequest<'_>,
    ) -> std::result::Result<(), AuthError> {
        match request {
            AuthorizationRequest::Discover {
                operation: "CallerIdentity",
                ..
            }
            | AuthorizationRequest::Execute {
                operation: "CallerIdentity",
                ..
            } => Ok(()),
            _ => Err(AuthError::Forbidden),
        }
    }
}

fn session(lifetime: Duration) -> AuthSession {
    AuthSession::new(
        AuthenticatedIdentity {
            principal: Principal {
                issuer: "fixture".into(),
                subject: "service:executor-7".into(),
                kind: PrincipalKind::Service,
            },
            client_id: Some("executor-client".into()),
            scopes: vec![],
            delegation: vec![],
            claims: Value::Null,
            expires_at: SystemTime::now()
                .checked_add(lifetime)
                .unwrap_or_else(SystemTime::now),
        },
        Arc::new(Policy),
    )
}

struct Fixture {
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fixture(auth: AuthSession) -> TestResult<Fixture> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = Arc::new(MykoServer::builder().build());
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let auth = auth.clone();
            let server = server.clone();
            tokio::spawn(async move {
                let Some(head) = read_request_head(&mut stream).await? else {
                    return Ok(());
                };
                let ctx = Arc::new(server.ctx());
                if head.is_websocket_upgrade() {
                    mcp::ws::handle_mcp_ws_upgrade_with_auth(
                        stream,
                        ctx,
                        server.server_info(),
                        head,
                        Some(auth),
                    )
                    .await
                } else if head.wants_event_stream() {
                    mcp::http::handle_sse_with_auth(stream, ctx, head, Some(auth)).await
                } else {
                    mcp::http::handle_post_with_auth(
                        stream,
                        ctx,
                        server.server_info(),
                        head,
                        Some(auth),
                    )
                    .await
                }
            });
        }
    });
    Ok(Fixture { addr, task })
}

async fn post(fixture: &Fixture, method: &str, params: Value) -> TestResult<Value> {
    Ok(reqwest::Client::new()
        .post(format!("http://{}/myko/mcp", fixture.addr))
        .json(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
        .send()
        .await?
        .json()
        .await?)
}

fn content(response: &Value) -> TestResult<Value> {
    Ok(serde_json::from_str(
        response
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .ok_or("missing content")?,
    )?)
}

#[tokio::test]
async fn mcp_preserves_service_identity_and_denies_execution_before_side_effects() -> TestResult {
    let _ = CallerIdentityArgs {};
    let _ = ForbiddenMutationArgs {};
    let auth = session(Duration::from_secs(30));
    let expected = format!("service:executor-7:{}", auth.id());
    let fixture = fixture(auth).await?;
    let response = post(&fixture, "tools/call", json!({"name":"execute", "arguments":{"code":"return await myko.command('CallerIdentity', {});"}})).await?;
    assert_eq!(
        content(&response)?.get("result"),
        Some(&Value::String(expected))
    );
    let before = MUTATIONS.load(Ordering::SeqCst);
    let denied = post(&fixture, "tools/call", json!({"name":"execute", "arguments":{"code":"return await myko.command('ForbiddenMutation', {});"}})).await?;
    assert_eq!(denied.pointer("/result/isError"), Some(&Value::Bool(true)));
    assert!(denied.to_string().contains("permission denied"));
    assert_eq!(MUTATIONS.load(Ordering::SeqCst), before);
    Ok(())
}

#[tokio::test]
async fn mcp_discovery_and_schema_access_use_local_policy() -> TestResult {
    let fixture = fixture(session(Duration::from_secs(30))).await?;
    let search = post(
        &fixture,
        "tools/call",
        json!({"name":"search","arguments":{}}),
    )
    .await?;
    let data = content(&search)?;
    let operations = data
        .get("operations")
        .and_then(Value::as_array)
        .ok_or("missing operations")?;
    assert_eq!(operations.len(), 1);
    assert_eq!(
        operations.first().and_then(|op| op.get("id")),
        Some(&json!("CallerIdentity"))
    );
    let resources = post(&fixture, "resources/list", json!({})).await?;
    let listed = resources
        .pointer("/result/resources")
        .and_then(Value::as_array)
        .ok_or("missing resources")?;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed.first().and_then(|resource| resource.get("uri")),
        Some(&json!("myko://schema/command/CallerIdentity"))
    );
    let denied = post(
        &fixture,
        "resources/read",
        json!({"uri":"myko://schema/command/ForbiddenMutation"}),
    )
    .await?;
    assert_eq!(denied.pointer("/error/code"), Some(&json!(-32602)));
    Ok(())
}

#[tokio::test]
async fn mcp_websocket_uses_same_policy_and_revocation_closes_idle_connection() -> TestResult {
    let auth = session(Duration::from_secs(30));
    let fixture = fixture(auth.clone()).await?;
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/myko/mcp", fixture.addr)).await?;
    socket.send(Message::Text(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"execute","arguments":{"code":"return await myko.command('ForbiddenMutation', {});"}}}).to_string().into())).await?;
    let message = timeout(Duration::from_secs(3), socket.next())
        .await?
        .ok_or("closed")??;
    let response: Value = serde_json::from_str(message.to_text()?)?;
    assert_eq!(
        response.pointer("/result/isError"),
        Some(&Value::Bool(true))
    );
    auth.revoke();
    match timeout(Duration::from_secs(3), socket.next()).await? {
        None | Some(Err(_) | Ok(Message::Close(_))) => {}
        other => return Err(format!("expected revoked socket to close, got {other:?}").into()),
    }
    Ok(())
}

#[tokio::test]
async fn mcp_sse_closes_when_credential_expires() -> TestResult {
    let fixture = fixture(session(Duration::from_secs(1))).await?;
    let mut stream = TcpStream::connect(fixture.addr).await?;
    stream
        .write_all(
            b"GET /myko/mcp HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
        )
        .await?;
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes)).await??;
    assert!(String::from_utf8(bytes)?.starts_with("HTTP/1.1 200 OK\r\n"));
    Ok(())
}
