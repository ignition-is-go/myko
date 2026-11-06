#![allow(clippy::panic_in_result_fn)]

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use futures_util::{SinkExt, StreamExt, future::BoxFuture};
use myko::{
    command::{CommandContext, CommandError, CommandHandler},
    myko_command,
    wire::{MEvent, MEventType, MykoMessage, command::WrappedCommand, message::PingData},
};
use myko_auth::{
    AuthError, AuthenticatedIdentity, Authenticator, AuthorizationRequest, Credentials,
    EntityAction, EntityScope, MykoAuthProvider, OperationKind, PermissionCatalog, Principal,
    PrincipalKind, ScopeDecision, ScopePolicy, ScopedAuthorizer,
};
use myko_server::{GatewayAccess, MykoServer, router::route_connection_with_auth};
use serde_json::{Value, json};
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

pub mod project {
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct ScopeProject {
        pub name: String,
    }
}

pub mod camera {
    use super::project::{ScopeProject, ScopeProjectId};
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct ScopeCamera {
        #[belongs_to(ScopeProject)]
        pub project_id: ScopeProjectId,
        #[myko_setter]
        pub enabled: bool,
        #[myko_rename("RenameScopedCamera")]
        pub name: String,
    }
}

use camera::{ScopeCamera, ScopeCameraId};
use project::{ScopeProject, ScopeProjectId};

static CUSTOM_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);

#[myko_command]
#[myko_scope(ScopeCamera, write)]
pub struct CalibrateScopeCamera {}
impl CommandHandler for CalibrateScopeCamera {
    fn execute(self, _ctx: CommandContext) -> std::result::Result<(), CommandError> {
        CUSTOM_EXECUTIONS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[myko_command]
pub struct UnmappedScopeCommand {}
impl CommandHandler for UnmappedScopeCommand {
    fn execute(self, _ctx: CommandContext) -> std::result::Result<(), CommandError> {
        CUSTOM_EXECUTIONS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct LocalPolicy(BTreeMap<EntityScope, ScopeDecision>);
impl ScopePolicy for LocalPolicy {
    fn decision(
        &self,
        _: &AuthenticatedIdentity,
        scope: EntityScope,
        _: AuthorizationRequest<'_>,
    ) -> ScopeDecision {
        self.0.get(&scope).copied().unwrap_or(ScopeDecision::Unset)
    }
}

struct TestIdentity;
impl Authenticator for TestIdentity {
    fn authenticate(
        &self,
        credentials: Credentials,
    ) -> BoxFuture<'_, std::result::Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async move {
            if credentials.bearer.as_deref() != Some("fixture-token") {
                return Err(AuthError::Unauthenticated);
            }
            Ok(AuthenticatedIdentity {
                principal: Principal {
                    issuer: "fixture".into(),
                    subject: "operators-group-member".into(),
                    kind: PrincipalKind::User,
                },
                client_id: None,
                scopes: vec![],
                delegation: vec![],
                claims: Value::Null,
                expires_at: SystemTime::now()
                    .checked_add(Duration::from_mins(1))
                    .ok_or(AuthError::Unavailable)?,
            })
        })
    }
}

struct Fixture {
    server: Arc<MykoServer>,
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

const fn scope(entity_type: &'static str, action: EntityAction) -> EntityScope {
    EntityScope {
        entity_type,
        action,
    }
}
fn camera(id: &str, enabled: bool) -> ScopeCamera {
    ScopeCamera {
        id: ScopeCameraId::from(id),
        project_id: ScopeProjectId::from("project-a"),
        enabled,
        name: id.into(),
    }
}
async fn fixture(
    grants: impl IntoIterator<Item = (EntityScope, ScopeDecision)>,
) -> TestResult<Fixture> {
    let server = Arc::new(MykoServer::builder().build());
    server.ctx().set(&ScopeProject {
        id: ScopeProjectId::from("project-a"),
        name: "Project A".into(),
    })?;
    server.ctx().set(&camera("camera-a", false))?;
    let policy = Arc::new(ScopedAuthorizer::new(Arc::new(LocalPolicy(
        grants.into_iter().collect(),
    ))));
    let provider = Arc::new(MykoAuthProvider::new(Arc::new(TestIdentity), policy));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let runtime = server.clone();
    let task = tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            let context = Arc::new(runtime.ctx());
            let info = runtime.server_info();
            let provider = provider.clone();
            tokio::spawn(async move {
                route_connection_with_auth(
                    stream,
                    peer,
                    context,
                    info,
                    GatewayAccess::Full,
                    provider,
                )
                .await
            });
        }
    });
    Ok(Fixture {
        server,
        address,
        task,
    })
}
async fn connect(fixture: &Fixture) -> TestResult<Socket> {
    let mut request = format!("ws://{}/myko", fixture.address).into_client_request()?;
    request
        .headers_mut()
        .insert("Authorization", "Bearer fixture-token".parse()?);
    Ok(tokio_tungstenite::connect_async(request).await?.0)
}
async fn send(socket: &mut Socket, message: &MykoMessage, binary: bool) -> TestResult {
    let frame = if binary {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(message, &mut bytes)?;
        Message::Binary(bytes.into())
    } else {
        Message::Text(serde_json::to_string(message)?.into())
    };
    socket.send(frame).await?;
    Ok(())
}
async fn next(socket: &mut Socket) -> TestResult<MykoMessage> {
    loop {
        match timeout(Duration::from_secs(3), socket.next())
            .await?
            .ok_or("socket closed")??
        {
            Message::Text(text) => return Ok(serde_json::from_str(&text)?),
            Message::Binary(bytes) => return Ok(ciborium::de::from_reader(bytes.as_ref())?),
            _ => {}
        }
    }
}

fn setter() -> MykoMessage {
    MykoMessage::Command(WrappedCommand {
        command_id: "SetScopeCameraEnabled".into(),
        command: json!({"id":"camera-a", "enabled":true, "tx":"setter"}),
    })
}
fn stored_camera(fixture: &Fixture, id: &str) -> TestResult<Value> {
    use myko::common::to_value::ToValue;
    let store = fixture.server.registry.get_or_create("ScopeCamera");
    let item = store
        .snapshot()
        .into_iter()
        .find(|(key, _)| key.as_ref() == id)
        .map(|(_, item)| item)
        .ok_or("camera missing")?;
    Ok(item.to_value())
}

#[test]
fn macros_register_scopes_and_exact_generated_and_custom_operation_bindings() {
    let _ = CalibrateScopeCameraArgs {};
    let _ = UnmappedScopeCommandArgs {};
    let catalog = PermissionCatalog::registered();
    assert_eq!(
        catalog
            .scopes()
            .filter(|scope| scope.entity_type == "ScopeCamera")
            .map(|scope| scope.to_string())
            .collect::<Vec<_>>(),
        vec![
            "ScopeCamera:read",
            "ScopeCamera:write",
            "ScopeCamera:delete"
        ]
    );
    assert_eq!(catalog.parents("ScopeCamera"), &["ScopeProject"]);
    for (kind, operation, action) in [
        (
            OperationKind::Query,
            "GetAllScopeCameras",
            EntityAction::Read,
        ),
        (
            OperationKind::Query,
            "GetScopeCamerasByIds",
            EntityAction::Read,
        ),
        (
            OperationKind::Query,
            "GetScopeCamerasByQuery",
            EntityAction::Read,
        ),
        (
            OperationKind::Report,
            "CountAllScopeCameras",
            EntityAction::Read,
        ),
        (
            OperationKind::Report,
            "CountScopeCameras",
            EntityAction::Read,
        ),
        (
            OperationKind::Report,
            "GetScopeCameraById",
            EntityAction::Read,
        ),
        (
            OperationKind::Command,
            "DeleteScopeCamera",
            EntityAction::Delete,
        ),
        (
            OperationKind::Command,
            "DeleteScopeCameras",
            EntityAction::Delete,
        ),
        (
            OperationKind::Command,
            "SetScopeCameraEnabled",
            EntityAction::Write,
        ),
        (
            OperationKind::Command,
            "RenameScopedCamera",
            EntityAction::Write,
        ),
        (
            OperationKind::Command,
            "CalibrateScopeCamera",
            EntityAction::Write,
        ),
    ] {
        assert_eq!(
            catalog.requirements(kind, operation),
            Some([scope("ScopeCamera", action)].as_slice()),
            "{operation}"
        );
    }
    assert!(
        catalog
            .requirements(OperationKind::Command, "UnmappedScopeCommand")
            .is_none()
    );
    let index = myko::operation_index::build_operation_index();
    assert_eq!(
        index
            .iter()
            .find(|operation| operation.id == "SetScopeCameraEnabled")
            .and_then(|operation| operation.required_scopes.clone()),
        Some(vec!["ScopeCamera:write".into()])
    );
}

#[tokio::test]
async fn project_write_permits_camera_setter_and_events_in_json_and_cbor() -> TestResult {
    let fixture = fixture([(
        scope("ScopeProject", EntityAction::Write),
        ScopeDecision::Allow,
    )])
    .await?;
    for binary in [false, true] {
        let mut socket = connect(&fixture).await?;
        send(&mut socket, &setter(), binary).await?;
        assert!(matches!(
            next(&mut socket).await?,
            MykoMessage::CommandResponse(_)
        ));
        assert_eq!(
            stored_camera(&fixture, "camera-a")?.get("enabled"),
            Some(&Value::Bool(true))
        );
        let id = if binary { "event-cbor" } else { "event-json" };
        send(
            &mut socket,
            &MykoMessage::Event(MEvent::from_item(
                &camera(id, true),
                MEventType::SET,
                "fixture",
            )),
            binary,
        )
        .await?;
        send(
            &mut socket,
            &MykoMessage::Ping(PingData {
                id: "barrier".into(),
                timestamp: 0,
            }),
            binary,
        )
        .await?;
        assert!(matches!(next(&mut socket).await?, MykoMessage::Ping(_)));
        assert_eq!(
            stored_camera(&fixture, id)?.get("enabled"),
            Some(&Value::Bool(true))
        );
        socket.close(None).await?;
    }
    Ok(())
}

#[tokio::test]
async fn child_deny_blocks_inherited_write_and_child_allow_overrides_parent_deny() -> TestResult {
    for (parent, child, permitted) in [
        (ScopeDecision::Allow, ScopeDecision::Deny, false),
        (ScopeDecision::Deny, ScopeDecision::Allow, true),
    ] {
        let fixture = fixture([
            (scope("ScopeProject", EntityAction::Write), parent),
            (scope("ScopeCamera", EntityAction::Write), child),
        ])
        .await?;
        let mut socket = connect(&fixture).await?;
        send(&mut socket, &setter(), false).await?;
        if permitted {
            assert!(matches!(
                next(&mut socket).await?,
                MykoMessage::CommandResponse(_)
            ));
        } else {
            assert!(
                matches!(next(&mut socket).await?, MykoMessage::CommandError(ref error) if error.tx == "setter" && error.message == "permission denied")
            );
            send(
                &mut socket,
                &MykoMessage::Ping(PingData {
                    id: "after-denial".into(),
                    timestamp: 0,
                }),
                false,
            )
            .await?;
            assert!(matches!(next(&mut socket).await?, MykoMessage::Ping(_)));
        }
        assert_eq!(
            stored_camera(&fixture, "camera-a")?.get("enabled"),
            Some(&Value::Bool(permitted))
        );
    }
    Ok(())
}

async fn post(fixture: &Fixture, method: &str, params: Value) -> TestResult<Value> {
    Ok(reqwest::Client::new()
        .post(format!("http://{}/myko/mcp", fixture.address))
        .bearer_auth("fixture-token")
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
async fn mcp_uses_inherited_scope_for_discovery_and_execution_without_granting_read_or_delete()
-> TestResult {
    let fixture = fixture([(
        scope("ScopeProject", EntityAction::Write),
        ScopeDecision::Allow,
    )])
    .await?;
    let response = post(
        &fixture,
        "tools/call",
        json!({"name":"search", "arguments":{}}),
    )
    .await?;
    let data = content(&response)?;
    let operations = data
        .get("operations")
        .and_then(Value::as_array)
        .ok_or("missing operations")?;
    assert!(
        operations
            .iter()
            .any(|operation| operation.get("id") == Some(&json!("SetScopeCameraEnabled")))
    );
    assert!(
        !operations
            .iter()
            .any(|operation| operation.get("id") == Some(&json!("GetAllScopeCameras")))
    );
    assert!(
        !operations
            .iter()
            .any(|operation| operation.get("id") == Some(&json!("DeleteScopeCamera")))
    );
    assert!(
        !operations
            .iter()
            .any(|operation| operation.get("id") == Some(&json!("UnmappedScopeCommand")))
    );
    assert_eq!(
        operations
            .iter()
            .find(|operation| operation.get("id") == Some(&json!("SetScopeCameraEnabled")))
            .and_then(|operation| operation.get("requiredScopes")),
        Some(&json!(["ScopeCamera:write"])),
    );
    let execute = |code: &str| json!({"name":"execute", "arguments":{"code":code}});
    let allowed = post(
        &fixture,
        "tools/call",
        execute(
            "return await myko.command('SetScopeCameraEnabled', {id:'camera-a',enabled:true});",
        ),
    )
    .await?;
    assert_ne!(allowed.pointer("/result/isError"), Some(&Value::Bool(true)));
    assert_eq!(
        stored_camera(&fixture, "camera-a")?.get("enabled"),
        Some(&Value::Bool(true))
    );
    let before = CUSTOM_EXECUTIONS.load(Ordering::SeqCst);
    let custom = post(
        &fixture,
        "tools/call",
        execute("return await myko.command('CalibrateScopeCamera', {});"),
    )
    .await?;
    assert_ne!(custom.pointer("/result/isError"), Some(&Value::Bool(true)));
    assert_eq!(
        CUSTOM_EXECUTIONS.load(Ordering::SeqCst),
        before.saturating_add(1)
    );
    for code in [
        "return await myko.query('GetAllScopeCameras', {});",
        "return await myko.command('DeleteScopeCamera', {id:'camera-a'});",
        "return await myko.command('UnmappedScopeCommand', {});",
    ] {
        let denied = post(&fixture, "tools/call", execute(code)).await?;
        assert_eq!(denied.pointer("/result/isError"), Some(&Value::Bool(true)));
    }
    assert_eq!(
        CUSTOM_EXECUTIONS.load(Ordering::SeqCst),
        before.saturating_add(1)
    );
    assert_eq!(
        stored_camera(&fixture, "camera-a")?.get("id"),
        Some(&json!("camera-a"))
    );
    Ok(())
}
