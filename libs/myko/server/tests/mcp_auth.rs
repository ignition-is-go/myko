#![allow(clippy::panic_in_result_fn)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
            user_id: None,
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
async fn modern_http_discovery_and_routing_use_the_authenticated_transport() -> TestResult {
    let f = fixture(session(Duration::from_secs(30))).await?;
    let client = reqwest::Client::new();
    let post = |method: &str, version: &str, params: Value| {
        let mut params = params.as_object().cloned().unwrap_or_default();
        params.insert(
            "_meta".into(),
            json!({
                "io.modelcontextprotocol/protocolVersion": version,
                "io.modelcontextprotocol/clientInfo": {"name": "myko-test", "version": "1"},
                "io.modelcontextprotocol/clientCapabilities": {}
            }),
        );
        client
            .post(format!("http://{}/myko/mcp", f.addr))
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Method", method)
            .json(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
    };
    let response = post("server/discover", "2026-07-28", json!({}))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let discovery: Value = response.json().await?;
    assert_eq!(
        discovery.pointer("/result/resultType"),
        Some(&json!("complete"))
    );
    assert_eq!(
        discovery.pointer("/result/supportedVersions/0"),
        Some(&json!("2026-07-28"))
    );

    let response = post("server/discover", "1900-01-01", json!({}))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let error: Value = response.json().await?;
    assert_eq!(error.pointer("/error/code"), Some(&json!(-32022)));

    let params = json!({"name":"execute", "arguments":{"code":"return await myko.command('ForbiddenMutation', {});"}});
    let response = post("tools/call", "2026-07-28", params.clone())
        .header("Mcp-Name", "search")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let error: Value = response.json().await?;
    assert_eq!(error.pointer("/error/code"), Some(&json!(-32020)));

    let response = post("tools/call", "2026-07-28", params)
        .header("Mcp-Name", "=?base64?ZXhlY3V0ZQ==?=")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let denied: Value = response.json().await?;
    assert_eq!(denied.pointer("/result/isError"), Some(&json!(true)));
    assert_eq!(
        denied.pointer("/result/resultType"),
        Some(&json!("complete"))
    );
    Ok(())
}

#[tokio::test]
async fn http_notifications_are_accepted_without_json_rpc_responses() -> TestResult {
    let f = fixture(session(Duration::from_secs(30))).await?;
    for method in [
        "notifications/initialized",
        "notifications/cancelled",
        "unknown/notification",
    ] {
        let response = reqwest::Client::new()
            .post(format!("http://{}/myko/mcp", f.addr))
            .json(&json!({"jsonrpc":"2.0", "method":method}))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        assert!(response.bytes().await?.is_empty());
    }
    assert!(
        post(&f, "tools/list", json!({}))
            .await?
            .get("result")
            .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn websocket_notifications_do_not_emit_a_response_or_break_the_connection() -> TestResult {
    let f = fixture(session(Duration::from_secs(30))).await?;
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/myko/mcp", f.addr)).await?;
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0", "method":"notifications/initialized"})
                .to_string()
                .into(),
        ))
        .await?;
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0", "id":7, "method":"tools/list"})
                .to_string()
                .into(),
        ))
        .await?;
    let frame = timeout(Duration::from_secs(5), socket.next())
        .await?
        .ok_or("socket closed")??;
    let response: Value = serde_json::from_str(frame.to_text()?)?;
    assert_eq!(response.get("id"), Some(&json!(7)));
    assert!(response.get("result").is_some());
    Ok(())
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
    assert!(
        denied
            .to_string()
            .contains("permission denied for command ForbiddenMutation")
    );
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

struct DiscoverablePolicy(AtomicBool);
impl Authorizer for DiscoverablePolicy {
    fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: AuthorizationRequest<'_>,
    ) -> std::result::Result<(), AuthError> {
        if matches!(request, AuthorizationRequest::Discover { .. }) {
            return Ok(());
        }
        if !self.0.load(Ordering::SeqCst) {
            return Err(AuthError::Forbidden);
        }
        Policy.authorize(identity, request)
    }
}

#[tokio::test]
async fn search_annotations_are_advisory_and_execution_remains_enforced() -> TestResult {
    let original = session(Duration::from_secs(30));
    let identity = original.identity().ok_or("missing identity")?.clone();
    let policy = Arc::new(DiscoverablePolicy(AtomicBool::new(true)));
    let fixture = fixture(AuthSession::new(identity, policy.clone())).await?;
    for details in [false, true] {
        let response = post(
            &fixture,
            "tools/call",
            json!({"name":"search", "arguments":{"details":details}}),
        )
        .await?;
        let data = content(&response)?;
        assert!(
            data.get("permissionNotice")
                .ok_or("missing notice")?
                .as_str()
                .ok_or("missing notice")?
                .contains("Permissions change live")
        );
        let ops = data
            .get("operations")
            .and_then(Value::as_array)
            .ok_or("missing operations")?;
        let allowed = ops
            .iter()
            .find(|op| {
                op.get("id").or_else(|| op.pointer("/operation/id"))
                    == Some(&json!("CallerIdentity"))
            })
            .ok_or("missing allowed operation")?;
        let denied = ops
            .iter()
            .find(|op| {
                op.get("id").or_else(|| op.pointer("/operation/id"))
                    == Some(&json!("ForbiddenMutation"))
            })
            .ok_or("missing denied operation")?;
        assert_eq!(
            allowed.pointer("/authorization/status"),
            Some(&json!("allowed"))
        );
        assert_eq!(
            denied.pointer("/authorization/status"),
            Some(&json!("denied"))
        );
    }
    let before = MUTATIONS.load(Ordering::SeqCst);
    let response = post(&fixture, "tools/call", json!({"name":"execute", "arguments":{"code":"return await myko.command('ForbiddenMutation', {});"}})).await?;
    assert_eq!(response.pointer("/result/isError"), Some(&json!(true)));
    assert_eq!(MUTATIONS.load(Ordering::SeqCst), before);
    policy.0.store(false, Ordering::SeqCst);
    let response = post(&fixture, "tools/call", json!({"name":"execute", "arguments":{"code":"return await myko.command('CallerIdentity', {});"}})).await?;
    assert_eq!(response.pointer("/result/isError"), Some(&json!(true)));
    let response = post(
        &fixture,
        "tools/call",
        json!({"name":"search", "arguments":{"query":"CallerIdentity"}}),
    )
    .await?;
    assert_eq!(
        content(&response)?.pointer("/operations/0/authorization/status"),
        Some(&json!("denied"))
    );
    Ok(())
}
