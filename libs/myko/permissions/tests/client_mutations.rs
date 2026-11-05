#![allow(clippy::panic_in_result_fn)]

use futures_util::{SinkExt, StreamExt, future::BoxFuture};
use myko::{
    item::Eventable,
    wire::{MEvent, MEventType, MykoMessage, command::WrappedCommand},
};
use myko_auth::{
    AuthError, AuthenticatedIdentity, Authenticator, Credentials, MykoAuthProvider, Principal,
    PrincipalKind, ScopedAuthorizer,
};
use myko_permissions::{
    AUTHORITY_ID, CredentialScopeMode, MemoryPermissionStorage, OperatorPrincipal,
    PermissionAuthority, PermissionPrincipal, PermissionPrincipalKind, PermissionRole,
    PermissionStorage, Permissions, bootstrap,
};
use myko_server::{GatewayAccess, MykoServer, router::route_connection_with_auth};
use std::{
    sync::Arc,
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

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Identity;
impl Authenticator for Identity {
    fn authenticate(
        &self,
        credentials: Credentials,
    ) -> BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async move {
            if credentials.bearer.as_deref() != Some("admin-fixture") {
                return Err(AuthError::Unauthenticated);
            }
            Ok(AuthenticatedIdentity {
                principal: Principal {
                    issuer: "fixture".into(),
                    subject: "administrator".into(),
                    kind: PrincipalKind::User,
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

struct ListenerTask(tokio::task::JoinHandle<()>);
impl Drop for ListenerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
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

async fn closed(socket: &mut Socket) -> TestResult {
    match timeout(Duration::from_secs(3), socket.next()).await? {
        None | Some(Err(_) | Ok(Message::Close(_))) => Ok(()),
        frame => Err(format!("expected authorization closure, got {frame:?}").into()),
    }
}

#[tokio::test]
async fn full_administrators_cannot_bypass_commands_with_client_events_or_generated_deletes()
-> TestResult {
    let storage = Arc::new(MemoryPermissionStorage::new());
    bootstrap(
        storage.as_ref(),
        OperatorPrincipal {
            issuer: "fixture".into(),
            subject: "administrator".into(),
            kind: PermissionPrincipalKind::User,
            name: "Administrator".into(),
        },
        "host-operator",
        "bootstrap event test",
    )?;
    let server = Arc::new(MykoServer::builder().build());
    let permissions = Arc::new(Permissions::new(
        storage.clone(),
        CredentialScopeMode::LocalGrants,
    ));
    permissions.attach(Arc::new(server.ctx()))?;
    let provider = Arc::new(MykoAuthProvider::new(
        Arc::new(Identity),
        Arc::new(ScopedAuthorizer::new(permissions.clone())),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let runtime = server.clone();
    let _listener = ListenerTask(tokio::spawn(async move {
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
    }));
    let before = storage.load()?.ok_or("initial policy")?;
    let messages = unsafe_messages(&before)?;
    for binary in [false, true] {
        for message in &messages {
            let mut request = format!("ws://{address}/myko").into_client_request()?;
            request
                .headers_mut()
                .insert("Authorization", "Bearer admin-fixture".parse()?);
            let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
            send(&mut socket, message, binary).await?;
            closed(&mut socket).await?;
            assert_eq!(storage.load()?, Some(before.clone()));
            let projected = server
                .registry
                .get_or_create(PermissionAuthority::ENTITY_NAME_STATIC)
                .snapshot();
            assert_eq!(projected.len(), 1);
            assert_eq!(
                projected
                    .first()
                    .and_then(|(_, item)| item.as_any().downcast_ref::<PermissionAuthority>()),
                Some(&before)
            );
            assert_eq!(
                server
                    .registry
                    .get_or_create(PermissionPrincipal::ENTITY_NAME_STATIC)
                    .snapshot()
                    .len(),
                1
            );
            assert_eq!(
                server
                    .registry
                    .get_or_create(PermissionRole::ENTITY_NAME_STATIC)
                    .snapshot()
                    .len(),
                1
            );
        }
    }
    Ok(())
}

fn unsafe_messages(before: &PermissionAuthority) -> TestResult<Vec<MykoMessage>> {
    let principal = before
        .document
        .principals
        .first()
        .ok_or("principal")?
        .clone();
    let role = before.document.roles.first().ok_or("role")?.clone();
    let mut forged = before.clone();
    forged.document.principals.clear();
    Ok(vec![
        MykoMessage::Event(MEvent::from_item(
            &forged,
            MEventType::SET,
            "untrusted-client",
        )),
        MykoMessage::Event(MEvent::from_item(
            &role,
            MEventType::DEL,
            "untrusted-client",
        )),
        MykoMessage::EventBatch(vec![
            MEvent::from_item(&role, MEventType::DEL, "untrusted-client"),
            MEvent::from_item(&principal, MEventType::DEL, "untrusted-client"),
        ]),
        MykoMessage::Command(WrappedCommand {
            command_id: "DeletePermissionPrincipal".into(),
            command: serde_json::json!({"id":principal.id.to_string(),"tx":"unsafe-delete"}),
        }),
        MykoMessage::Command(WrappedCommand {
            command_id: "DeletePermissionAuthority".into(),
            command: serde_json::json!({"id":AUTHORITY_ID,"tx":"unsafe-authority-delete"}),
        }),
    ])
}
