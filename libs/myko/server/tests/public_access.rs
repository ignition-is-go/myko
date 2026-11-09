#![allow(clippy::panic_in_result_fn)]

use futures_util::{SinkExt, StreamExt, future::BoxFuture};
use myko::{
    command::{CommandContext, CommandError, CommandHandler},
    hyphae::{Cell, Definite, MapQuery, Materialize},
    prelude::*,
    report::{ReportContext, ReportHandler},
    view::{ViewBuildArgs, ViewHandler},
    wire::{MEvent, MEventType},
};
use myko_auth::{
    AuthError, AuthenticatedIdentity, Authenticator, AuthorizationRequest, Authorizer, Credentials,
    MykoAuthProvider,
};
use myko_server::{GatewayAccess, MykoServer, router::route_connection_with_auth};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[myko_item]
#[myko_public_events(set)]
#[myko_public_queries(by_query)]
pub struct PublicRecord {
    value: i32,
}

#[myko_command(bool)]
#[myko_public]
pub struct PublicCaller {}
impl CommandHandler for PublicCaller {
    fn execute(self, ctx: CommandContext) -> Result<bool, CommandError> {
        Ok(ctx
            .req
            .auth
            .as_ref()
            .is_some_and(|session| session.identity().is_none()))
    }
}
#[myko_query(PublicRecord)]
#[myko_public]
pub struct PublicRecords {}
impl myko::query::QueryHandler for PublicRecords {
    fn test_entity(_: myko::query::QueryTestContext<Self>) -> bool {
        true
    }
}
#[myko_view(PublicRecord)]
#[myko_public]
pub struct PublicRecordsView {}
impl ViewHandler for PublicRecordsView {
    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl MapQuery<Key = Arc<str>, Value = Arc<PublicRecord>> {
        ctx.view_context.query_map_by_str(GetAllPublicRecords {})
    }
}
#[myko_report(bool)]
#[myko_public]
pub struct PublicCallerReport {}
impl ReportHandler for PublicCallerReport {
    type Output = bool;
    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<bool>, Definite> {
        Cell::new(Arc::new(
            ctx.req
                .auth
                .as_ref()
                .is_some_and(|session| session.identity().is_none()),
        ))
        .lock()
    }
}
struct Reject;
impl Authenticator for Reject {
    fn authenticate(
        &self,
        _: Credentials,
    ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async { Err(AuthError::Unauthenticated) })
    }
}
struct Policy;
impl Authorizer for Policy {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        Err(AuthError::Forbidden)
    }
}
struct Fixture {
    server: Arc<MykoServer>,
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn fixture(access: GatewayAccess) -> TestResult<Fixture> {
    fixture_oauth(access, false).await
}
async fn fixture_oauth(access: GatewayAccess, oauth: bool) -> TestResult<Fixture> {
    let server = Arc::new(MykoServer::builder().build());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let runtime = server.clone();
    let mut provider = MykoAuthProvider::new(Arc::new(Reject), Arc::new(Policy));
    if oauth {
        provider = provider.with_mcp_oauth(
            myko_auth::OAuthResource::new(
                format!("http://{addr}/myko/mcp"),
                vec!["https://login.example/".into()],
            )?
            .with_scopes(vec!["mcp:use".into()])?,
        );
    }
    let provider = Arc::new(provider);
    let task = tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            let ctx = Arc::new(runtime.ctx());
            let info = runtime.server_info();
            let provider = provider.clone();
            tokio::spawn(async move {
                route_connection_with_auth(stream, peer, ctx, info, access, provider).await
            });
        }
    });
    Ok(Fixture { server, addr, task })
}

#[tokio::test]
async fn oauth_mcp_discovery_requires_login_without_disabling_public_websocket_operations()
-> TestResult {
    let f = fixture_oauth(GatewayAccess::Full, true).await?;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/myko/mcp", f.addr))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize"}))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers().get("www-authenticate").and_then(|v| v.to_str().ok()),
        Some(format!("Bearer resource_metadata=\"http://{}/.well-known/oauth-protected-resource/myko/mcp\", scope=\"mcp:use\"",f.addr).as_str()));
    for path in [
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/myko/mcp",
    ] {
        let response = client
            .get(format!("http://{}{path}", f.addr))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let metadata: Value = response.json().await?;
        assert_eq!(
            metadata.get("resource"),
            Some(&json!(format!("http://{}/myko/mcp", f.addr)))
        );
        assert_eq!(
            metadata.get("authorization_servers"),
            Some(&json!(["https://login.example/"]))
        );
    }
    let _socket = connect(&f).await?;
    Ok(())
}
async fn connect(f: &Fixture) -> TestResult<Socket> {
    Ok(
        tokio_tungstenite::connect_async(format!("ws://{}/myko", f.addr))
            .await?
            .0,
    )
}
async fn send(socket: &mut Socket, value: Value, binary: bool) -> TestResult {
    let frame = if binary {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&value, &mut bytes)?;
        Message::Binary(bytes.into())
    } else {
        Message::Text(value.to_string().into())
    };
    socket.send(frame).await?;
    Ok(())
}
async fn receive(socket: &mut Socket) -> TestResult<Value> {
    let frame = timeout(Duration::from_secs(3), socket.next())
        .await?
        .ok_or("closed")??;
    match frame {
        Message::Text(text) => Ok(serde_json::from_str(&text)?),
        Message::Binary(bytes) => Ok(ciborium::de::from_reader(bytes.as_ref())?),
        _ => Err("expected data response".into()),
    }
}
#[tokio::test]
async fn decorated_commands_queries_views_reports_and_events_work_anonymously() -> TestResult {
    for binary in [false, true] {
        let f = fixture(GatewayAccess::Full).await?;
        let mut socket = connect(&f).await?;
        let event = MEvent::from_item(
            &PublicRecord {
                id: "public-row".into(),
                value: 7,
            },
            MEventType::SET,
            "untrusted",
        );
        send(
            &mut socket,
            json!({"event":"ws:m:event","data":event}),
            binary,
        )
        .await?;
        for (event, data, response) in [
            (
                "command",
                json!({"commandId":"PublicCaller","command":{"tx":"command"}}),
                "command-response",
            ),
            (
                "query",
                json!({"queryId":"PublicRecords","queryItemType":"PublicRecord","query":{"tx":"query"}}),
                "query-response",
            ),
            (
                "query",
                json!({"queryId":"GetPublicRecordsByQuery","queryItemType":"PublicRecord","query":{"tx":"generated-query"}}),
                "query-response",
            ),
            (
                "view",
                json!({"viewId":"PublicRecordsView","viewItemType":"PublicRecord","view":{"tx":"view"}}),
                "view-response",
            ),
            (
                "report",
                json!({"reportId":"PublicCallerReport","report":{"tx":"report"}}),
                "report-response",
            ),
        ] {
            send(
                &mut socket,
                json!({"event":format!("ws:m:{event}"),"data":data}),
                binary,
            )
            .await?;
            let value = receive(&mut socket).await?;
            assert_eq!(
                value.get("event"),
                Some(&json!(format!("ws:m:{response}"))),
                "{value}"
            );
        }
        assert_eq!(
            f.server
                .registry
                .get_or_create("PublicRecord")
                .snapshot()
                .len(),
            1
        );
    }
    Ok(())
}
#[tokio::test]
async fn public_entity_events_do_not_expose_generated_queries_or_deletes() -> TestResult {
    let f = fixture(GatewayAccess::Full).await?;
    for (event, data) in [
        (
            "query",
            json!({"queryId":"GetAllPublicRecords","queryItemType":"PublicRecord","query":{"tx":"private"}}),
        ),
        (
            "event",
            serde_json::to_value(MEvent::from_item(
                &PublicRecord {
                    id: "public-row".into(),
                    value: 7,
                },
                MEventType::DEL,
                "untrusted",
            ))?,
        ),
    ] {
        let mut socket = connect(&f).await?;
        send(
            &mut socket,
            json!({"event":format!("ws:m:{event}"),"data":data}),
            false,
        )
        .await?;
        if event == "query" {
            let error = receive(&mut socket).await?;
            assert_eq!(
                error.get("event").and_then(Value::as_str),
                Some("ws:m:query-error")
            );
            assert_eq!(
                error.pointer("/data/tx").and_then(Value::as_str),
                Some("private")
            );
        }
        send(
            &mut socket,
            json!({"event":"ws:m:ping","data":{"id":"still-alive","timestamp":0}}),
            false,
        )
        .await?;
        assert_eq!(
            receive(&mut socket)
                .await?
                .get("event")
                .and_then(Value::as_str),
            Some("ws:m:ping")
        );
    }
    Ok(())
}
#[tokio::test]
async fn bad_credentials_are_not_downgraded_to_public_access() -> TestResult {
    let f = fixture(GatewayAccess::Full).await?;
    let mut request = format!("ws://{}/myko", f.addr).into_client_request()?;
    request
        .headers_mut()
        .insert("Authorization", "Bearer invalid".parse()?);
    let result = tokio_tungstenite::connect_async(request).await;
    assert!(
        matches!(result, Err(tokio_tungstenite::tungstenite::Error::Http(response)) if response.status() == 401)
    );
    Ok(())
}
#[tokio::test]
async fn mcp_only_discovers_and_executes_public_operations_for_anonymous_callers() -> TestResult {
    let f = fixture(GatewayAccess::Full).await?;
    let client = reqwest::Client::new();
    let url = format!("http://{}/myko/mcp", f.addr);
    let list: Value = client
        .post(&url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search","arguments":{}}}))
        .send()
        .await?
        .json()
        .await?;
    let text = list.to_string();
    assert!(text.contains("PublicCaller"), "{list}");
    assert!(!text.contains("GetAllPublicRecords"), "{list}");
    let result: Value = client.post(&url).json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"execute","arguments":{"code":"return await myko.command('PublicCaller', {});"}}})).send().await?.json().await?;
    assert_ne!(
        result.pointer("/result/isError"),
        Some(&Value::Bool(true)),
        "{result}"
    );
    assert!(result.get("error").is_none(), "{result}");
    let content: Value = serde_json::from_str(
        result
            .pointer("/result/content/0/text")
            .and_then(Value::as_str)
            .ok_or("missing MCP result")?,
    )?;
    assert_eq!(content.get("result"), Some(&Value::Bool(true)), "{result}");
    let denied: Value = client.post(&url).json(&json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"execute","arguments":{"code":"return await myko.query('GetAllPublicRecords', {});"}}})).send().await?.json().await?;
    assert_eq!(
        denied.pointer("/result/isError"),
        Some(&Value::Bool(true)),
        "{denied}"
    );
    Ok(())
}

#[tokio::test]
async fn public_declarations_do_not_bypass_read_only_gateway() -> TestResult {
    let f = fixture(GatewayAccess::ReadOnly).await?;
    let mut socket = connect(&f).await?;
    send(&mut socket, json!({"event":"ws:m:command","data":{"commandId":"PublicCaller","command":{"tx":"denied"}}}), false).await?;
    let error = receive(&mut socket).await?;
    assert_eq!(
        error.get("event").and_then(Value::as_str),
        Some("ws:m:command-error")
    );
    send(
        &mut socket,
        json!({"event":"ws:m:ping","data":{"id":"read-only-alive","timestamp":0}}),
        false,
    )
    .await?;
    assert_eq!(
        receive(&mut socket)
            .await?
            .get("event")
            .and_then(Value::as_str),
        Some("ws:m:ping")
    );
    Ok(())
}

#[tokio::test]
async fn unauthorized_anonymous_event_batch_has_no_partial_effects() -> TestResult {
    let f = fixture(GatewayAccess::Full).await?;
    for binary in [false, true] {
        let mut socket = connect(&f).await?;
        let item = PublicRecord {
            id: "batch-row".into(),
            value: 1,
        };
        let events = [
            MEvent::from_item(&item, MEventType::SET, "untrusted"),
            MEvent::from_item(&item, MEventType::DEL, "untrusted"),
        ];
        send(
            &mut socket,
            json!({"event":"ws:m:event-batch","data":events}),
            binary,
        )
        .await?;
        send(
            &mut socket,
            json!({"event":"ws:m:ping","data":{"id":"after-rejection","timestamp":0}}),
            binary,
        )
        .await?;
        assert_eq!(
            receive(&mut socket)
                .await?
                .get("event")
                .and_then(Value::as_str),
            Some("ws:m:ping")
        );
        assert!(
            f.server
                .registry
                .get_or_create("PublicRecord")
                .snapshot()
                .is_empty()
        );
    }
    Ok(())
}

#[tokio::test]
async fn mcp_websocket_uses_the_same_anonymous_boundary() -> TestResult {
    let f = fixture(GatewayAccess::Full).await?;
    let (mut socket, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/myko/mcp", f.addr)).await?;
    send(&mut socket, json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"execute","arguments":{"code":"return await myko.command('PublicCaller', {});"}}}), false).await?;
    let allowed = receive(&mut socket).await?;
    assert_ne!(
        allowed.pointer("/result/isError"),
        Some(&Value::Bool(true)),
        "{allowed}"
    );
    send(&mut socket, json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"execute","arguments":{"code":"return await myko.query('GetAllPublicRecords', {});"}}}), false).await?;
    let denied = receive(&mut socket).await?;
    assert_eq!(
        denied.pointer("/result/isError"),
        Some(&Value::Bool(true)),
        "{denied}"
    );
    Ok(())
}

#[tokio::test]
async fn denied_operations_return_correlated_errors_and_preserve_live_subscriptions() -> TestResult
{
    for binary in [false, true] {
        let f = fixture(GatewayAccess::Full).await?;
        let mut socket = connect(&f).await?;
        send(&mut socket, json!({"event":"ws:m:query","data":{"queryId":"PublicRecords","queryItemType":"PublicRecord","query":{"tx":"live"}}}), binary).await?;
        assert_eq!(
            receive(&mut socket)
                .await?
                .pointer("/data/tx")
                .and_then(Value::as_str),
            Some("live")
        );
        for (kind, data) in [
            (
                "query",
                json!({"queryId":"GetAllPublicRecords","queryItemType":"PublicRecord","query":{"tx":"denied-query"}}),
            ),
            (
                "view",
                json!({"viewId":"PrivateView","viewItemType":"PublicRecord","view":{"tx":"denied-view"}}),
            ),
            (
                "report",
                json!({"reportId":"PrivateReport","report":{"tx":"denied-report"}}),
            ),
            (
                "command",
                json!({"commandId":"PrivateCommand","command":{"tx":"denied-command"}}),
            ),
        ] {
            send(
                &mut socket,
                json!({"event":format!("ws:m:{kind}"),"data":data}),
                binary,
            )
            .await?;
            let response = receive(&mut socket).await?;
            assert_eq!(
                response.get("event").and_then(Value::as_str),
                Some(format!("ws:m:{kind}-error").as_str())
            );
            assert_eq!(
                response.pointer("/data/tx").and_then(Value::as_str),
                Some(format!("denied-{kind}").as_str())
            );
            assert_eq!(
                response.pointer("/data/message").and_then(Value::as_str),
                Some("authentication required")
            );
        }
        let event = MEvent::from_item(
            &PublicRecord {
                id: "after-denials".into(),
                value: 42,
            },
            MEventType::SET,
            "untrusted",
        );
        send(
            &mut socket,
            json!({"event":"ws:m:event","data":event}),
            binary,
        )
        .await?;
        let update = receive(&mut socket).await?;
        assert_eq!(
            update.pointer("/data/tx").and_then(Value::as_str),
            Some("live")
        );
        assert_eq!(
            update
                .pointer("/data/upserts/0/item/value")
                .and_then(Value::as_i64),
            Some(42)
        );
    }
    Ok(())
}
