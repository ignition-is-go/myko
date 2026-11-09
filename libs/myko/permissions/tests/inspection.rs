#![allow(clippy::panic_in_result_fn)]

use hyphae::{Gettable, Materialize, Watchable};
use myko::{
    report::{ReportContext, ReportHandler},
    request::{RequestCacheScope, RequestContext},
};
use myko_auth::{
    AuthError, AuthSession, AuthenticatedIdentity, AuthorizationRequest, Authorizer, EntityAction,
    EventChange, PermissionCatalog, Principal, PrincipalKind, ScopedAuthorizer,
};
use myko_permissions::{
    AUTHORITY_ID, CredentialScopeMode, GetPermissionStructure, InspectPermissionPrincipal,
    MemoryPermissionStorage, OperatorPrincipal, PermissionEffect, PermissionGrantee,
    PermissionInspectionStatus, PermissionOverride, PermissionOverrideId, PermissionPrincipalKind,
    PermissionRole, PermissionRoleBinding, PermissionRoleBindingId, PermissionRoleId,
    PermissionRule, PermissionStorage, PermissionStructure, Permissions, bootstrap,
    replace_as_operator,
};
use myko_server::MykoServer;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

mod application {
    #[allow(clippy::wildcard_imports)]
    use myko::prelude::*;
    #[myko::myko_item]
    pub struct ForeignPermissionProject {
        pub name: String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[myko::myko_view_item]
    pub struct ForeignPermissionComputedRow {
        pub id: std::sync::Arc<str>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[myko::myko_subtype]
    pub struct ForeignPermissionHelper {
        pub name: String,
    }
    pub mod child {
        use super::{ForeignPermissionProject, ForeignPermissionProjectId};
        #[allow(clippy::wildcard_imports)]
        use myko::prelude::*;
        #[myko::myko_item]
        pub struct ForeignPermissionCamera {
            #[belongs_to(ForeignPermissionProject)]
            pub project_id: ForeignPermissionProjectId,
        }
    }
}

struct Fixture {
    server: MykoServer,
    context: Arc<myko::server::MykoServerContext>,
    storage: Arc<MemoryPermissionStorage>,
    permissions: Arc<Permissions>,
}
impl Fixture {
    fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let storage = Arc::new(MemoryPermissionStorage::new());
        bootstrap(
            storage.as_ref(),
            OperatorPrincipal {
                issuer: "fixture".into(),
                subject: "admin".into(),
                kind: PermissionPrincipalKind::User,
                name: "Admin".into(),
            },
            "operator",
            "bootstrap",
        )?;
        let server = MykoServer::builder().build();
        let context = Arc::new(server.ctx());
        let permissions = Arc::new(Permissions::new(
            storage.clone(),
            CredentialScopeMode::LocalGrants,
        ));
        permissions.attach(context.clone())?;
        Ok(Self {
            server,
            context,
            storage,
            permissions,
        })
    }
    fn identity(subject: &str) -> AuthenticatedIdentity {
        AuthenticatedIdentity {
            principal: Principal {
                issuer: "fixture".into(),
                subject: subject.into(),
                kind: PrincipalKind::User,
            },
            client_id: None,
            scopes: Vec::new(),
            delegation: Vec::new(),
            claims: serde_json::Value::Null,
            expires_at: SystemTime::now()
                .checked_add(Duration::from_mins(1))
                .unwrap_or(SystemTime::UNIX_EPOCH),
        }
    }
    fn report(&self, subject: &str) -> ReportContext {
        let auth = AuthSession::new(
            Self::identity(subject),
            Arc::new(ScopedAuthorizer::new(self.permissions.clone())),
        );
        ReportContext::new(
            Arc::new(
                RequestContext::from_client("tx".into(), "client".into(), self.context.host_id)
                    .with_auth(Some(auth)),
            ),
            self.context.clone(),
        )
    }
    fn structure(&self, subject: &str) -> Arc<PermissionStructure> {
        GetPermissionStructure {}
            .compute(self.report(subject))
            .materialize()
            .get()
    }
}

#[test]
fn structure_discovers_server_only_entity_names_relationships_and_operations() -> TestResult {
    let fixture = Fixture::new()?;
    let structure = fixture.structure("admin");
    assert_eq!(structure.status, PermissionInspectionStatus::Ready);
    assert_eq!(structure.version, 1);
    let camera = structure
        .entities
        .iter()
        .find(|entity| entity.entity_type == "ForeignPermissionCamera")
        .ok_or("camera absent")?;
    assert_eq!(camera.parents, ["ForeignPermissionProject"]);
    assert_eq!(
        camera.scopes,
        [
            "ForeignPermissionCamera:read",
            "ForeignPermissionCamera:write",
            "ForeignPermissionCamera:delete"
        ]
    );
    assert!(
        structure
            .operations
            .iter()
            .any(
                |operation| operation.operation == "DeleteForeignPermissionCamera"
                    && operation.required_scopes == ["ForeignPermissionCamera:delete"]
            )
    );
    assert!(
        structure
            .commands
            .iter()
            .any(|command| command.operation == "UpsertPermissionRole" && command.allowed)
    );
    assert!(
        serde_json::to_value(structure.as_ref())?
            .get("document")
            .is_some()
    );
    Ok(())
}

#[test]
fn direct_inspection_denies_missing_unknown_and_child_read_denied_callers() -> TestResult {
    let fixture = Fixture::new()?;
    assert_eq!(
        fixture.structure("outsider").status,
        PermissionInspectionStatus::Denied
    );
    let internal = ReportContext::new(
        Arc::new(RequestContext::internal(
            "internal".into(),
            fixture.context.host_id,
            "test",
        )),
        fixture.context.clone(),
    );
    let result = GetPermissionStructure {}
        .compute(internal)
        .materialize()
        .get();
    assert_eq!(result.status, PermissionInspectionStatus::Denied);
    assert!(result.document.is_none() && result.entities.is_empty());
    let mut accepted = fixture.storage.load()?.ok_or("snapshot")?;
    let principal = accepted.document.principals.first().ok_or("principal")?;
    accepted.document.overrides.push(PermissionOverride {
        id: PermissionOverrideId::from("deny-role-read"),
        authority_id: AUTHORITY_ID.into(),
        grantee: PermissionGrantee::Principal(principal.id.clone()),
        rule: PermissionRule {
            scope: "PermissionRole:read".into(),
            effect: PermissionEffect::Deny,
        },
    });
    replace_as_operator(
        fixture.storage.as_ref(),
        accepted.revision,
        accepted.document,
        "operator",
        "child read denial",
        true,
    )?;
    fixture.permissions.refresh()?;
    let denied = fixture.structure("admin");
    assert_eq!(denied.status, PermissionInspectionStatus::Denied);
    assert!(denied.document.is_none() && denied.commands.is_empty());
    Ok(())
}

#[test]
fn local_inspection_explains_the_same_inherited_decision_as_authorization() -> TestResult {
    let fixture = Fixture::new()?;
    let mut accepted = fixture.storage.load()?.ok_or("snapshot")?;
    let principal_id = accepted
        .document
        .principals
        .first()
        .ok_or("principal")?
        .id
        .clone();
    accepted.document.roles.push(PermissionRole {
        id: PermissionRoleId::from("project-writer"),
        authority_id: AUTHORITY_ID.into(),
        name: "Project writer".into(),
        enabled: true,
        rules: vec![PermissionRule {
            scope: "ForeignPermissionProject:write".into(),
            effect: PermissionEffect::Allow,
        }],
    });
    accepted.document.bindings.push(PermissionRoleBinding {
        id: PermissionRoleBindingId::from("writer-binding"),
        authority_id: AUTHORITY_ID.into(),
        role_id: "project-writer".into(),
        grantee: PermissionGrantee::Principal(principal_id.clone()),
    });
    replace_as_operator(
        fixture.storage.as_ref(),
        accepted.revision,
        accepted.document,
        "operator",
        "grant project writer",
        false,
    )?;
    fixture.permissions.refresh()?;
    let output = InspectPermissionPrincipal { principal_id }
        .compute(fixture.report("admin"))
        .materialize()
        .get();
    assert_eq!(output.status, PermissionInspectionStatus::Ready);
    let camera = output
        .scopes
        .iter()
        .find(|entry| entry.scope == "ForeignPermissionCamera:write")
        .ok_or("camera scope")?;
    assert!(camera.allowed);
    let step = camera
        .steps
        .iter()
        .find(|step| step.scope == "ForeignPermissionProject:write")
        .ok_or("inherited source")?;
    assert_eq!(step.distance, 1);
    assert_eq!(step.decision, "Allow");
    assert_eq!(
        step.sources,
        [myko_permissions::PermissionRuleSource::Role(
            "project-writer".into()
        )]
    );
    let scope = PermissionCatalog::registered()
        .entity_scope("ForeignPermissionCamera", EntityAction::Write)
        .ok_or("scope")?;
    assert!(
        PermissionCatalog::registered()
            .authorize_scope(scope, |scope| myko_auth::ScopePolicy::decision(
                fixture.permissions.as_ref(),
                &Fixture::identity("admin"),
                scope,
                AuthorizationRequest::ClientEvent {
                    entity_type: "ForeignPermissionCamera",
                    change: EventChange::Set,
                    proposed: &serde_json::Value::Null
                }
            ))
            .is_ok()
    );
    Ok(())
}

struct CustomAuthorizer;
impl Authorizer for CustomAuthorizer {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        Ok(())
    }
}

#[test]
fn custom_policy_reports_unsupported_without_dumping_a_fictional_policy() {
    let server = MykoServer::builder().build();
    let auth = AuthSession::new(Fixture::identity("custom"), Arc::new(CustomAuthorizer));
    let context = ReportContext::new(
        Arc::new(
            RequestContext::from_client(
                "custom-tx".into(),
                "custom-client".into(),
                server.ctx().host_id,
            )
            .with_auth(Some(auth)),
        ),
        Arc::new(server.ctx()),
    );
    let structure = GetPermissionStructure {}
        .compute(context)
        .materialize()
        .get();
    assert_eq!(structure.status, PermissionInspectionStatus::Unsupported);
    assert!(structure.document.is_none() && structure.entities.is_empty());
    assert_eq!(
        GetPermissionStructure::request_cache_scope(),
        RequestCacheScope::PerClient
    );
    assert_eq!(
        InspectPermissionPrincipal::request_cache_scope(),
        RequestCacheScope::PerClient
    );
}

struct FixtureAuthenticator;
impl myko_auth::Authenticator for FixtureAuthenticator {
    fn authenticate(
        &self,
        credentials: myko_auth::Credentials,
    ) -> futures_util::future::BoxFuture<'_, Result<AuthenticatedIdentity, AuthError>> {
        Box::pin(async move {
            match credentials.bearer.as_deref() {
                Some("admin" | "outsider") => Ok(Fixture::identity(
                    credentials.bearer.as_deref().unwrap_or("outsider"),
                )),
                _ => Err(AuthError::Unauthenticated),
            }
        })
    }
}
struct ListenerTask(tokio::task::JoinHandle<()>);
impl Drop for ListenerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn typed_client_receives_generated_structure_over_an_authenticated_websocket() -> TestResult {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
    let fixture = Fixture::new()?;
    let provider = Arc::new(myko_auth::MykoAuthProvider::new(
        Arc::new(FixtureAuthenticator),
        Arc::new(ScopedAuthorizer::new(fixture.permissions.clone())),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let context = fixture.context.clone();
    let info = fixture.server.server_info();
    let _listener = ListenerTask(tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            let (context, info, provider) = (context.clone(), info.clone(), provider.clone());
            tokio::spawn(async move {
                let _ = myko_server::router::route_connection_with_auth(
                    stream,
                    peer,
                    context,
                    info,
                    myko_server::GatewayAccess::Full,
                    provider,
                )
                .await;
            });
        }
    }));
    let client = myko::client::MykoClient::new();
    client.set_bearer_token("admin".into())?;
    let result = client.watch_report_result::<_, PermissionStructure>(GetPermissionStructure {});
    client.set_address(Some(format!("ws://{address}/myko")));
    let structure = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(value) = result.get() {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    assert_eq!(structure.status, PermissionInspectionStatus::Ready);
    assert!(
        structure
            .entities
            .iter()
            .any(|entity| entity.entity_type == "ForeignPermissionCamera")
    );
    client.set_address(None);
    let mut request = format!("ws://{address}/myko").into_client_request()?;
    request
        .headers_mut()
        .insert("Authorization", "Bearer outsider".parse()?);
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
    let message = myko::wire::MykoMessage::Report(myko::wire::WrappedReport {
        report_id: "GetPermissionStructure".into(),
        report: serde_json::json!({"tx":"denied-inspection"}),
    });
    socket
        .send(Message::Text(serde_json::to_string(&message)?.into()))
        .await?;
    let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await?
        .ok_or("socket closed on denied report")??;
    let Message::Text(text) = frame else {
        return Err("expected report error frame".into());
    };
    let myko::wire::MykoMessage::ReportError(error) = serde_json::from_str(&text)? else {
        return Err("unauthorized caller did not receive report error".into());
    };
    assert_eq!(error.tx, "denied-inspection");
    assert_eq!(error.report_id, "GetPermissionStructure");
    assert_eq!(error.message, "permission denied");
    let ping = myko::wire::MykoMessage::Ping(myko::wire::PingData {
        id: "after-denied-inspection".into(),
        timestamp: 0,
    });
    socket
        .send(Message::Text(serde_json::to_string(&ping)?.into()))
        .await?;
    let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await?
        .ok_or("socket closed after denial")??;
    let Message::Text(text) = frame else {
        return Err("expected ping response".into());
    };
    assert!(matches!(
        serde_json::from_str::<myko::wire::MykoMessage>(&text)?,
        myko::wire::MykoMessage::Ping(_)
    ));
    Ok(())
}

#[test]
fn edit_capabilities_intersect_custom_authorizer_and_current_local_grants() -> TestResult {
    let fixture = Fixture::new()?;
    let mut accepted = fixture.storage.load()?.ok_or("snapshot")?;
    let mut viewer = accepted.document.principals.first().ok_or("admin")?.clone();
    viewer.id = "viewer".into();
    viewer.subject = "viewer".into();
    accepted.document.principals.push(viewer.clone());
    accepted.document.roles.push(PermissionRole {
        id: "viewer-role".into(),
        authority_id: AUTHORITY_ID.into(),
        name: "Viewer".into(),
        enabled: true,
        rules: vec![PermissionRule {
            scope: "PermissionAuthority:read".into(),
            effect: PermissionEffect::Allow,
        }],
    });
    accepted.document.bindings.push(PermissionRoleBinding {
        id: "viewer-binding".into(),
        authority_id: AUTHORITY_ID.into(),
        role_id: "viewer-role".into(),
        grantee: PermissionGrantee::Principal(viewer.id),
    });
    replace_as_operator(
        fixture.storage.as_ref(),
        accepted.revision,
        accepted.document,
        "operator",
        "add readonly viewer",
        false,
    )?;
    fixture.permissions.refresh()?;
    let auth = AuthSession::new(Fixture::identity("viewer"), Arc::new(CustomAuthorizer));
    let request = RequestContext::from_client(
        "viewer-tx".into(),
        "viewer-client".into(),
        fixture.context.host_id,
    )
    .with_auth(Some(auth));
    let output = GetPermissionStructure {}
        .compute(ReportContext::new(Arc::new(request), fixture.context))
        .materialize()
        .get();
    assert_eq!(output.status, PermissionInspectionStatus::Ready);
    assert!(!output.commands.is_empty());
    assert!(output.commands.iter().all(|command| !command.allowed));
    Ok(())
}

async fn refresh_to(fixture: &Fixture, revision: u64) -> TestResult {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            fixture.permissions.refresh()?;
            if fixture
                .permissions
                .snapshot()?
                .is_some_and(|snapshot| snapshot.revision >= revision)
            {
                return Ok::<_, myko_permissions::PermissionError>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(())
}

fn live_inspection_context(fixture: &Fixture, session: AuthSession) -> ReportContext {
    ReportContext::new(
        Arc::new(
            RequestContext::from_client(
                "live-inspection".into(),
                "client".into(),
                fixture.context.host_id,
            )
            .with_auth(Some(session)),
        ),
        fixture.context.clone(),
    )
}

#[tokio::test]
async fn existing_inspection_cells_follow_role_assignment_and_removal() -> TestResult {
    let fixture = Fixture::new()?;
    let initial = fixture.storage.load()?.ok_or("missing policy")?;
    let mut document = initial.document;
    let mut person = document
        .principals
        .first()
        .ok_or("missing administrator")?
        .clone();
    person.id = "new-user".into();
    person.subject = "new-user".into();
    document.principals.push(person);
    let result = replace_as_operator(
        fixture.storage.as_ref(),
        initial.revision,
        document,
        "operator",
        "Discover user",
        false,
    )?;
    refresh_to(&fixture, result.revision).await?;
    let session = myko_auth::MykoAuthProvider::new(
        Arc::new(FixtureAuthenticator),
        Arc::new(ScopedAuthorizer::new(fixture.permissions.clone())),
    )
    .with_live_permissions()
    .authenticate(myko_auth::Credentials {
        bearer: Some("admin".into()),
    })
    .await?;
    let context = live_inspection_context(&fixture, session.clone());
    let structure = GetPermissionStructure {}
        .compute(context.clone())
        .materialize();
    let inspection = InspectPermissionPrincipal {
        principal_id: "new-user".into(),
    }
    .compute(context)
    .materialize();
    let _structure_subscription = structure.subscribe(|_| {});
    let _inspection_subscription = inspection.subscribe(|_| {});
    assert!(inspection.get().scopes.iter().all(|scope| !scope.allowed));
    let current = fixture.storage.load()?.ok_or("missing policy")?;
    let mut document = current.document;
    let role_id = document.roles.first().ok_or("missing role")?.id.clone();
    document.bindings.push(PermissionRoleBinding {
        id: "live-binding".into(),
        authority_id: AUTHORITY_ID.into(),
        role_id,
        grantee: PermissionGrantee::Principal("new-user".into()),
    });
    let assigned = replace_as_operator(
        fixture.storage.as_ref(),
        current.revision,
        document,
        "operator",
        "Assign role",
        false,
    )?;
    refresh_to(&fixture, assigned.revision).await?;
    assert_eq!(structure.get().revision, Some(assigned.revision));
    assert!(structure.get().document.as_ref().is_some_and(|document| {
        document
            .bindings
            .iter()
            .any(|binding| binding.id.as_ref() == "live-binding")
    }));
    assert!(
        inspection
            .get()
            .scopes
            .iter()
            .any(|scope| scope.scope == "PermissionRole:write" && scope.allowed)
    );
    let current = fixture.storage.load()?.ok_or("missing policy")?;
    let mut document = current.document;
    document
        .bindings
        .retain(|binding| binding.id.as_ref() != "live-binding");
    let removed = replace_as_operator(
        fixture.storage.as_ref(),
        current.revision,
        document,
        "operator",
        "Remove role",
        false,
    )?;
    refresh_to(&fixture, removed.revision).await?;
    assert_eq!(structure.get().revision, Some(removed.revision));
    assert_eq!(inspection.get().revision, Some(removed.revision));
    assert!(inspection.get().scopes.iter().all(|scope| !scope.allowed));
    session.check()?;
    Ok(())
}

#[test]
fn only_entities_advertise_generated_permission_scopes() {
    let _row = application::ForeignPermissionComputedRow { id: "row".into() };
    let _helper = application::ForeignPermissionHelper {
        name: "helper".into(),
    };
    let catalog = PermissionCatalog::registered();
    for action in [
        EntityAction::Read,
        EntityAction::Write,
        EntityAction::Delete,
    ] {
        assert!(
            catalog
                .entity_scope("ForeignPermissionProject", action)
                .is_some()
        );
        assert!(
            catalog
                .entity_scope("ForeignPermissionComputedRow", action)
                .is_none()
        );
        assert!(
            catalog
                .entity_scope("ForeignPermissionHelper", action)
                .is_none()
        );
    }
}
