#![allow(clippy::panic_in_result_fn)]
use futures_util::{SinkExt, StreamExt, future::BoxFuture};
use myko::common::to_value::ToValue;
use myko::{
    entities::demo::{DemoStatusId, DemoTask, DemoTaskId},
    server::{PersistError, Persister},
    wire::{MEvent, MEventType, MykoMessage, message::PingData, query::WrappedQuery},
};
use myko_auth::{
    AuthError, AuthenticatedIdentity, Authenticator, AuthorizationRequest, Authorizer, Credentials,
    MykoAuthProvider, Principal, PrincipalKind,
};
use myko_server::{GatewayAccess, MykoServer, router::route_connection_with_auth};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
struct Identity(Arc<AtomicUsize>);
impl Authenticator for Identity {
    fn authenticate(
        &self,
        credentials: Credentials,
    ) -> BoxFuture<'_, std::result::Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            let subject = credentials
                .bearer
                .filter(|token| token == "user" || token == "service")
                .ok_or(AuthError::Unauthenticated)?;
            Ok(AuthenticatedIdentity {
                principal: Principal {
                    issuer: "fixture".into(),
                    kind: if subject == "user" {
                        PrincipalKind::User
                    } else {
                        PrincipalKind::Service
                    },
                    subject,
                },
                client_id: None,
                scopes: vec![],
                delegation: vec![],
                claims: serde_json::Value::Null,
                expires_at: SystemTime::now()
                    .checked_add(Duration::from_mins(1))
                    .ok_or(AuthError::Unavailable)?,
            })
        })
    }
}
struct Policy(AtomicU64);
impl Authorizer for Policy {
    fn revision(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        request: AuthorizationRequest<'_>,
    ) -> std::result::Result<(), AuthError> {
        match request {
            AuthorizationRequest::Execute {
                kind: myko_auth::OperationKind::Query,
                ..
            } => Ok(()),
            AuthorizationRequest::ClientEvent {
                entity_type: "DemoTask",
                change: myko_auth::EventChange::Set,
                proposed,
            } if proposed.get("id").and_then(serde_json::Value::as_str) == Some("allowed") => {
                Ok(())
            }
            _ => Err(AuthError::Forbidden),
        }
    }
}
#[derive(Default)]
struct Journal(Mutex<Vec<MEvent>>);
impl Persister for Journal {
    fn persist(&self, event: MEvent) -> std::result::Result<(), PersistError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
        Ok(())
    }
}
struct Fixture {
    server: Arc<MykoServer>,
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
    policy: Arc<Policy>,
    authenticated: Arc<AtomicUsize>,
    journal: Arc<Journal>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn fixture(access: GatewayAccess) -> TestResult<Fixture> {
    let journal = Arc::new(Journal::default());
    let server = Arc::new(
        MykoServer::builder()
            .with_default_persister(journal.clone())
            .build(),
    );
    let policy = Arc::new(Policy(AtomicU64::new(0)));
    let authenticated = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(MykoAuthProvider::new(
        Arc::new(Identity(authenticated.clone())),
        policy.clone(),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let runtime = server.clone();
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
    Ok(Fixture {
        server,
        addr,
        task,
        policy,
        authenticated,
        journal,
    })
}
async fn connect(fixture: &Fixture) -> TestResult<Socket> {
    let mut request = format!("ws://{}/myko", fixture.addr).into_client_request()?;
    request
        .headers_mut()
        .insert("Authorization", "Bearer service".parse()?);
    Ok(tokio_tungstenite::connect_async(request).await?.0)
}
fn event(id: &str) -> MEvent {
    MEvent::from_item(
        &DemoTask {
            id: DemoTaskId::from(id),
            title: id.into(),
            completed: false,
            status_id: DemoStatusId::from("todo"),
        },
        MEventType::SET,
        "untrusted",
    )
}
async fn send(socket: &mut Socket, message: &MykoMessage, binary: bool) -> TestResult {
    let frame = if binary {
        let mut bytes = vec![];
        ciborium::ser::into_writer(message, &mut bytes)?;
        Message::Binary(bytes.into())
    } else {
        Message::Text(serde_json::to_string(message)?.into())
    };
    socket.send(frame).await?;
    Ok(())
}
async fn closed(socket: &mut Socket) -> TestResult {
    match timeout(Duration::from_secs(3), socket.next()).await? {
        None | Some(Err(_) | Ok(Message::Close(_))) => Ok(()),
        other => Err(format!("expected close, got {other:?}").into()),
    }
}
#[tokio::test]
async fn missing_credentials_rejected_before_upgrade_for_native_and_mcp() -> TestResult {
    let f = fixture(GatewayAccess::Full).await?;
    for path in ["/myko", "/myko/mcp"] {
        match tokio_tungstenite::connect_async(format!("ws://{}{path}", f.addr)).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), 401);
            }
            other => return Err(format!("unexpected handshake {other:?}").into()),
        }
    }
    assert!(
        f.server
            .registry
            .get_or_create("Client")
            .snapshot()
            .is_empty()
    );
    Ok(())
}
#[tokio::test]
async fn rejected_event_batch_has_no_store_or_journal_effect_in_json_and_cbor() -> TestResult {
    let f = fixture(GatewayAccess::Full).await?;
    for binary in [false, true] {
        let mut socket = connect(&f).await?;
        send(
            &mut socket,
            &MykoMessage::EventBatch(vec![event("allowed"), event("denied")]),
            binary,
        )
        .await?;
        closed(&mut socket).await?;
        assert_eq!(
            f.server.registry.get_or_create("DemoTask").snapshot().len(),
            0
        );
        assert_eq!(
            f.journal
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|event| event.item_type.as_ref() == "DemoTask")
                .count(),
            0
        );
    }
    let mut socket = connect(&f).await?;
    send(&mut socket, &MykoMessage::Event(event("allowed")), false).await?;
    timeout(Duration::from_secs(3), async {
        while f.server.registry.get_or_create("DemoTask").snapshot().len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    send(
        &mut socket,
        &MykoMessage::Ping(PingData {
            id: "barrier".into(),
            timestamp: 0,
        }),
        false,
    )
    .await?;
    assert!(
        timeout(Duration::from_secs(3), socket.next())
            .await?
            .is_some()
    );
    assert_eq!(f.authenticated.load(Ordering::SeqCst), 3);
    Ok(())
}
#[tokio::test]
async fn read_only_ceiling_cannot_be_overridden_by_policy() -> TestResult {
    let f = fixture(GatewayAccess::ReadOnly).await?;
    let mut socket = connect(&f).await?;
    send(&mut socket, &MykoMessage::Event(event("allowed")), false).await?;
    closed(&mut socket).await?;
    assert_eq!(
        f.server.registry.get_or_create("DemoTask").snapshot().len(),
        0
    );
    Ok(())
}
#[tokio::test]
async fn policy_change_closes_subscription_without_another_identity_provider_call() -> TestResult {
    let f = fixture(GatewayAccess::Full).await?;
    let mut socket = connect(&f).await?;
    send(
        &mut socket,
        &MykoMessage::Query(WrappedQuery {
            query: serde_json::json!({"tx":"live"}),
            query_id: "GetDemoTasks".into(),
            query_item_type: "DemoTask".into(),
            window: None,
        }),
        false,
    )
    .await?;
    assert!(
        timeout(Duration::from_secs(3), socket.next())
            .await?
            .is_some()
    );
    f.policy.0.fetch_add(1, Ordering::SeqCst);
    closed(&mut socket).await?;
    assert_eq!(f.authenticated.load(Ordering::SeqCst), 1);
    timeout(Duration::from_secs(3), async {
        while !f
            .server
            .registry
            .get_or_create("Client")
            .snapshot()
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[myko::myko_query(DemoTask)]
struct CallerTasks {}
impl myko::query::QueryHandler for CallerTasks {
    fn test_entity(ctx: myko::query::QueryTestContext<Self>) -> bool {
        ctx.query_context
            .req
            .auth
            .as_ref()
            .is_some_and(|session| session.identity().principal.subject == ctx.item.id.to_string())
    }
}

async fn scoped_snapshot(f: &Fixture, token: &str) -> TestResult<Vec<serde_json::Value>> {
    let mut request = format!("ws://{}/myko", f.addr).into_client_request()?;
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {token}").parse()?);
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
    send(
        &mut socket,
        &MykoMessage::Query(WrappedQuery {
            query: serde_json::json!({"tx":"same"}),
            query_id: "CallerTasks".into(),
            query_item_type: "DemoTask".into(),
            window: None,
        }),
        false,
    )
    .await?;
    let frame = timeout(Duration::from_secs(3), socket.next())
        .await?
        .ok_or("closed")??;
    let Message::Text(text) = frame else {
        return Err("expected text".into());
    };
    let MykoMessage::QueryResponse(response) = serde_json::from_str(&text)? else {
        return Err("expected query response".into());
    };
    Ok(response
        .upserts
        .into_iter()
        .map(|wrapped| wrapped.item.to_value())
        .collect())
}

#[tokio::test]
async fn authenticated_query_cache_does_not_share_caller_sensitive_results() -> TestResult {
    let f = fixture(GatewayAccess::Full).await?;
    f.server.ctx().apply_event(event("user"))?;
    f.server.ctx().apply_event(event("service"))?;
    let user = scoped_snapshot(&f, "user").await?;
    let service = scoped_snapshot(&f, "service").await?;
    assert_eq!(user.len(), 1);
    assert_eq!(service.len(), 1);
    assert_eq!(
        user.first()
            .and_then(|item| item.get("id"))
            .and_then(serde_json::Value::as_str),
        Some("user")
    );
    assert_eq!(
        service
            .first()
            .and_then(|item| item.get("id"))
            .and_then(serde_json::Value::as_str),
        Some("service")
    );
    Ok(())
}

#[tokio::test]
async fn browser_handshake_selects_public_protocol_without_echoing_credentials() -> TestResult {
    use base64::Engine as _;
    let f = fixture(GatewayAccess::Full).await?;
    let mut request = format!("ws://{}/myko", f.addr).into_client_request()?;
    let credential = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("service");
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        format!("myko, myko.bearer.{credential}").parse()?,
    );
    let (mut socket, response) = tokio_tungstenite::connect_async(request).await?;
    assert_eq!(
        response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|value| value.to_str().ok()),
        Some("myko")
    );
    socket.close(None).await?;
    Ok(())
}

#[tokio::test]
async fn typed_client_authenticates_before_subscribing() -> TestResult {
    use myko::hyphae::Gettable;
    let f = fixture(GatewayAccess::Full).await?;
    f.server.ctx().apply_event(event("service"))?;
    let client = myko::client::MykoClient::new_with_auto_reconnect(false);
    client.set_bearer_token("service".into())?;
    client.set_address(Some(format!("ws://{}/myko", f.addr)));
    let watch = client.watch_query_state(CallerTasks {});
    timeout(Duration::from_secs(3), async {
        while !watch.ready().get() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(watch.items().get().len(), 1);
    assert_eq!(f.authenticated.load(Ordering::SeqCst), 1);
    client.close();
    Ok(())
}
