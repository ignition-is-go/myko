#![allow(clippy::panic_in_result_fn)]
use myko_auth::{
    AuthError, AuthenticatedIdentity, Authenticator, AuthorizationRequest, Authorizer, Credentials,
    MykoAuthProvider, PermissionCatalog, PrincipalKind,
};
use myko_permissions::*;
use std::sync::Arc;
struct Fixture {
    storage: Arc<FilePermissionStorage>,
    policy: Permissions,
    admin_email: Option<AdminEmail>,
}
struct AllowApplication;
impl Authorizer for AllowApplication {
    fn authorize(
        &self,
        _: &AuthenticatedIdentity,
        _: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        Ok(())
    }
}

use myko_auth::{OperationKind, Principal};
use std::time::{Duration, SystemTime};

const INITIAL_OWNER: &str = "admin@example.test";

struct FixedIdentity(AuthenticatedIdentity);
impl Authenticator for FixedIdentity {
    fn authenticate(
        &self,
        _: Credentials,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<AuthenticatedIdentity, AuthError>> + Send + '_>,
    > {
        Box::pin(async { Ok(self.0.clone()) })
    }
}
impl Fixture {
    async fn register_signed_in_user(
        &self,
        identity: &AuthenticatedIdentity,
    ) -> Result<(), AuthError> {
        PermissionAuthenticator::new(
            Arc::new(FixedIdentity(identity.clone())),
            self.policy.clone(),
        )
        .with_user_discovery()
        .authenticate(Credentials::default())
        .await?;
        Ok(())
    }
    async fn initialize_owner(&self, identity: &AuthenticatedIdentity) -> Result<(), AuthError> {
        let mut authenticator = PermissionAuthenticator::new(
            Arc::new(FixedIdentity(identity.clone())),
            self.policy.clone(),
        );
        if let Some(email) = &self.admin_email {
            authenticator = authenticator.with_admin_email(email.clone());
        }
        authenticator.authenticate(Credentials::default()).await?;
        Ok(())
    }
}

#[tokio::test]
async fn configured_verified_admin_is_reconciled_and_policy_survives_restart()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let storage = Arc::new(FilePermissionStorage::new(
        directory.path().join("permissions.jsonl"),
    ));
    let policy = Permissions::new(storage.clone(), CredentialScopeMode::LocalGrants);
    let permissions = Fixture {
        storage: storage.clone(),
        policy: policy.clone(),
        admin_email: Some(AdminEmail::new(INITIAL_OWNER)?),
    };
    let server = myko_server::MykoServer::builder().build();
    permissions.policy.attach(Arc::new(server.ctx()))?;
    let mut identity = AuthenticatedIdentity {
        user_id: None,
        principal: Principal {
            issuer: "https://verified.example/".into(),
            subject: "stable-owner".into(),
            kind: PrincipalKind::User,
        },
        client_id: None,
        scopes: vec![],
        delegation: vec![],
        claims: serde_json::Value::Null,
        expires_at: SystemTime::now() + Duration::from_mins(1),
    };
    permissions.initialize_owner(&identity).await?;
    assert!(storage.load()?.is_none());
    identity.claims = serde_json::json!({"email": INITIAL_OWNER});
    permissions.initialize_owner(&identity).await?;
    assert!(storage.load()?.is_none());
    identity.claims = serde_json::json!({"verified_email": "somebody@lucid.rocks"});
    permissions.initialize_owner(&identity).await?;
    assert!(storage.load()?.is_none());
    identity.claims = serde_json::json!({"verified_email": INITIAL_OWNER});
    permissions.initialize_owner(&identity).await?;
    let first = storage.load()?.ok_or("missing owner")?;
    assert_eq!(
        first
            .document
            .principals
            .first()
            .ok_or("missing owner principal")?
            .subject,
        "stable-owner"
    );
    let role = first.document.roles.first().ok_or("missing Admin role")?;
    assert_eq!(role.name, "Admin");
    for scope in PermissionCatalog::registered().scopes() {
        assert!(role.rules.iter().any(|rule| rule.scope == scope.to_string()
            && rule.effect == myko_permissions::PermissionEffect::Allow));
    }
    let access = policy.administration_authorizer(Arc::new(AllowApplication));
    let arguments = serde_json::Value::Null;
    let request = AuthorizationRequest::Execute {
        kind: OperationKind::Command,
        operation: "unmapped-policy-fixture",
        arguments: &arguments,
    };
    access.authorize(&identity, request)?;
    access
        .authorize(
            &identity,
            AuthorizationRequest::Discover {
                kind: OperationKind::Report,
                operation: stringify!(GetPermissionStructure),
            },
        )
        .map_err(|error| format!("owner permission discovery: {error}"))?;
    assert_eq!(
        access.authorize(
            &identity,
            AuthorizationRequest::ClientEvent {
                entity_type: stringify!(PermissionRole),
                change: myko_auth::EventChange::Set,
                proposed: &arguments
            }
        ),
        Err(AuthError::Forbidden)
    );
    identity.principal.subject = "different-subject".into();
    access.authorize(&identity, request)?;
    let management = AuthorizationRequest::Discover {
        kind: OperationKind::Report,
        operation: stringify!(GetPermissionStructure),
    };
    assert_eq!(
        access.authorize(&identity, management),
        Err(AuthError::Forbidden)
    );
    permissions.initialize_owner(&identity).await?;
    access.authorize(&identity, management)?;
    let reconciled = storage.load()?.ok_or("lost authority")?;
    assert_eq!(reconciled.document.roles, first.document.roles);
    assert_eq!(reconciled.document.principals.len(), 2);
    let reopened = FilePermissionStorage::new(storage.path());
    assert_eq!(
        reopened.load()?.ok_or("lost persisted authority")?,
        reconciled
    );
    Ok(())
}

fn signed_in_identity(subject: &str, email: &str) -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: None,
        principal: Principal {
            issuer: "https://verified.example/".into(),
            subject: subject.into(),
            kind: PrincipalKind::User,
        },
        client_id: None,
        scopes: vec![],
        delegation: vec![],
        claims: serde_json::json!({"verified_email": email}),
        expires_at: SystemTime::now()
            .checked_add(Duration::from_mins(1))
            .unwrap_or(SystemTime::UNIX_EPOCH),
    }
}

#[tokio::test]
async fn signed_in_people_are_discovered_without_grants_and_not_reenabled()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let storage = Arc::new(FilePermissionStorage::new(
        directory.path().join("permissions.jsonl"),
    ));
    let policy = Permissions::new(storage.clone(), CredentialScopeMode::LocalGrants);
    let permissions = Fixture {
        storage: storage.clone(),
        policy,
        admin_email: Some(AdminEmail::new(INITIAL_OWNER)?),
    };
    let server = myko_server::MykoServer::builder().build();
    permissions.policy.attach(Arc::new(server.ctx()))?;
    permissions
        .initialize_owner(&signed_in_identity("owner", INITIAL_OWNER))
        .await?;
    let before = storage.load()?.ok_or("missing authority")?;
    let person = signed_in_identity("person", "person@lucid.rocks");
    let (first, second) = tokio::join!(
        permissions.register_signed_in_user(&person),
        permissions.register_signed_in_user(&person)
    );
    first?;
    second?;
    let after = storage.load()?.ok_or("missing registered person")?;
    assert_eq!(
        after.document.principals.len(),
        before.document.principals.len().saturating_add(1)
    );
    assert_eq!(after.document.bindings, before.document.bindings);
    assert_eq!(after.document.roles, before.document.roles);
    assert_eq!(after.document.overrides, before.document.overrides);
    assert_eq!(after.document.memberships, before.document.memberships);
    permissions.register_signed_in_user(&person).await?;
    assert_eq!(
        storage.load()?.ok_or("lost authority")?.revision,
        after.revision
    );
    let mut document = after.document;
    document
        .principals
        .iter_mut()
        .find(|principal| principal.subject == "person")
        .ok_or("person missing")?
        .enabled = false;
    let disabled = myko_permissions::replace_as_operator(
        storage.as_ref(),
        after.revision,
        document,
        "test operator",
        "Disable discovered identity",
        false,
    )?;
    permissions.register_signed_in_user(&person).await?;
    let final_state = storage.load()?.ok_or("missing authority")?;
    assert_eq!(final_state.revision, disabled.revision);
    assert!(
        !final_state
            .document
            .principals
            .iter()
            .find(|principal| principal.subject == "person")
            .ok_or("person missing")?
            .enabled
    );
    Ok(())
}

async fn wait_for_policy(
    permissions: &Fixture,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let revision = permissions
        .storage
        .load()?
        .ok_or("missing policy")?
        .revision;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            permissions.policy.refresh()?;
            if permissions
                .policy
                .snapshot()?
                .is_some_and(|snapshot| snapshot.revision >= revision)
            {
                return Ok::<_, PermissionError>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(())
}

async fn live_session(
    identity: AuthenticatedIdentity,
    access: Arc<dyn Authorizer>,
) -> Result<myko_auth::AuthSession, AuthError> {
    MykoAuthProvider::new(Arc::new(FixedIdentity(identity)), access)
        .with_live_permissions()
        .authenticate(Credentials::default())
        .await
}

#[tokio::test]
async fn role_changes_update_existing_sessions_without_disconnects()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use myko_permissions::{PermissionGrantee, PermissionRoleBinding};
    let directory = tempfile::tempdir()?;
    let storage = Arc::new(FilePermissionStorage::new(
        directory.path().join("permissions.jsonl"),
    ));
    let policy = Permissions::new(storage.clone(), CredentialScopeMode::LocalGrants);
    let permissions = Fixture {
        storage: storage.clone(),
        policy: policy.clone(),
        admin_email: Some(AdminEmail::new(INITIAL_OWNER)?),
    };
    let server = myko_server::MykoServer::builder().build();
    permissions.policy.attach(Arc::new(server.ctx()))?;
    let editor_identity = signed_in_identity("editor", INITIAL_OWNER);
    permissions.initialize_owner(&editor_identity).await?;
    let affected_identity = signed_in_identity("affected", "person@lucid.rocks");
    permissions
        .register_signed_in_user(&affected_identity)
        .await?;
    let access = policy.administration_authorizer(Arc::new(AllowApplication));
    let editor = live_session(editor_identity, access.clone()).await?;
    let affected = live_session(affected_identity, access.clone()).await?;
    let request = AuthorizationRequest::Discover {
        kind: OperationKind::Report,
        operation: stringify!(GetPermissionStructure),
    };
    assert_eq!(affected.authorize(request), Err(AuthError::Forbidden));
    let before = storage.load()?.ok_or("missing policy")?;
    let mut document = before.document;
    let principal = document
        .principals
        .iter()
        .find(|principal| principal.subject == "affected")
        .ok_or("person missing")?
        .id
        .clone();
    let role_id = document
        .roles
        .first()
        .ok_or("owner role missing")?
        .id
        .clone();
    let binding_id: myko_permissions::PermissionRoleBindingId =
        uuid::Uuid::new_v4().to_string().into();
    document.bindings.push(PermissionRoleBinding {
        id: binding_id.clone(),
        authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
        role_id,
        grantee: PermissionGrantee::Principal(principal),
    });
    myko_permissions::replace_as_operator(
        storage.as_ref(),
        before.revision,
        document,
        "test operator",
        "Assign role to another person",
        false,
    )?;
    wait_for_policy(&permissions).await?;
    editor.check()?;
    affected.check()?;
    editor.authorize(request)?;
    affected.authorize(request)?;
    let assigned = storage.load()?.ok_or("missing assignment")?;
    let mut document = assigned.document;
    document.bindings.retain(|binding| binding.id != binding_id);
    myko_permissions::replace_as_operator(
        storage.as_ref(),
        assigned.revision,
        document,
        "test operator",
        "Remove assigned role",
        false,
    )?;
    wait_for_policy(&permissions).await?;
    editor.check()?;
    affected.check()?;
    assert_eq!(affected.authorize(request), Err(AuthError::Forbidden));
    Ok(())
}

#[tokio::test]
async fn discovery_does_not_bootstrap_or_record_service_and_delegated_identities()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let storage = Arc::new(MemoryPermissionStorage::new());
    let policy = Permissions::new(storage.clone(), CredentialScopeMode::LocalGrants);
    let server = myko_server::MykoServer::builder().build();
    policy.attach(Arc::new(server.ctx()))?;
    let person = signed_in_identity("person", "person@example.test");
    PermissionAuthenticator::new(Arc::new(FixedIdentity(person.clone())), policy.clone())
        .with_user_discovery()
        .authenticate(Credentials::default())
        .await?;
    assert!(storage.load()?.is_none());
    PermissionAuthenticator::new(
        Arc::new(FixedIdentity(signed_in_identity("owner", INITIAL_OWNER))),
        policy.clone(),
    )
    .with_admin_email(AdminEmail::new(INITIAL_OWNER)?)
    .authenticate(Credentials::default())
    .await?;
    let before = storage.load()?;
    let mut service = person.clone();
    service.principal.kind = PrincipalKind::Service;
    let mut delegated = person;
    delegated.delegation.push(delegated.principal.clone());
    for identity in [service, delegated] {
        PermissionAuthenticator::new(Arc::new(FixedIdentity(identity)), policy.clone())
            .with_user_discovery()
            .authenticate(Credentials::default())
            .await?;
    }
    assert_eq!(storage.load()?, before);
    Ok(())
}

#[test]
fn administration_authorizer_preserves_the_application_policy_boundary() {
    struct DenyApplication;
    impl Authorizer for DenyApplication {
        fn authorize(
            &self,
            _: &AuthenticatedIdentity,
            _: AuthorizationRequest<'_>,
        ) -> Result<(), AuthError> {
            Err(AuthError::Forbidden)
        }
        fn authorize_anonymous(&self, _: AuthorizationRequest<'_>) -> Result<(), AuthError> {
            Ok(())
        }
    }
    let policy = Permissions::new(
        Arc::new(MemoryPermissionStorage::new()),
        CredentialScopeMode::LocalGrants,
    );
    let access = policy.administration_authorizer(Arc::new(DenyApplication));
    let identity = signed_in_identity("person", "person@example.test");
    let application = AuthorizationRequest::Discover {
        kind: OperationKind::Query,
        operation: "ApplicationQuery",
    };
    assert_eq!(
        access.authorize(&identity, application),
        Err(AuthError::Forbidden)
    );
    assert_eq!(access.authorize_anonymous(application), Ok(()));
    let administration = AuthorizationRequest::Discover {
        kind: OperationKind::Report,
        operation: stringify!(GetPermissionStructure),
    };
    assert_eq!(
        access.authorize(&identity, administration),
        Err(AuthError::Forbidden)
    );
    assert_eq!(
        access.authorize_anonymous(administration),
        Err(AuthError::Unauthenticated)
    );
}

#[derive(Default)]
struct RecordedEvents(std::sync::Mutex<Vec<myko::wire::MEvent>>);
impl myko::server::Persister for RecordedEvents {
    fn persist(&self, event: myko::wire::MEvent) -> Result<(), myko::server::PersistError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
        Ok(())
    }
}

#[tokio::test]
async fn entity_policy_persists_replays_and_reads_live_entity_queries()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use myko::{item::Eventable, request::RequestContext};
    let events = Arc::new(RecordedEvents::default());
    let server = myko_server::MykoServer::builder()
        .with_default_persister(events.clone())
        .build();
    let context = Arc::new(server.ctx());
    let policy = Permissions::from_entities(CredentialScopeMode::LocalGrants);
    policy.attach(context.clone())?;
    let owner = signed_in_identity("owner", INITIAL_OWNER);
    PermissionAuthenticator::new(Arc::new(FixedIdentity(owner.clone())), policy.clone())
        .with_admin_email(AdminEmail::new(INITIAL_OWNER)?)
        .with_user_discovery()
        .authenticate(Credentials::default())
        .await?;
    let snapshot = policy.snapshot()?.ok_or("missing authority")?;
    let admin_role = snapshot
        .document
        .roles
        .first()
        .ok_or("missing role")?
        .clone();
    let request = Arc::new(RequestContext::internal(
        "test".into(),
        context.host_id,
        "permissions-test",
    ));
    assert_eq!(
        context
            .query_map_by_str(GetAllPermissionRoles {}, request)
            .items_snapshot()
            .len(),
        1
    );
    let events = events
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    for entity in [
        PermissionAuthority::ENTITY_NAME_STATIC,
        PermissionPrincipal::ENTITY_NAME_STATIC,
        PermissionRole::ENTITY_NAME_STATIC,
        PermissionRoleBinding::ENTITY_NAME_STATIC,
    ] {
        assert!(
            events
                .iter()
                .any(|event| event.item_type.as_ref() == entity)
        );
    }
    let restarted = myko_server::MykoServer::builder().build();
    let replay = Arc::new(restarted.ctx());
    for event in events {
        replay.apply_event(event)?;
    }
    let replayed_policy = Permissions::from_entities(CredentialScopeMode::LocalGrants);
    replayed_policy.attach(replay.clone())?;
    assert_eq!(
        replayed_policy
            .snapshot()?
            .map(|snapshot| snapshot.document.clone()),
        Some(snapshot.document.clone())
    );
    let access = policy.administration_authorizer(Arc::new(AllowApplication));
    let session = live_session(owner, access).await?;
    let operation = AuthorizationRequest::Discover {
        kind: OperationKind::Report,
        operation: stringify!(GetPermissionStructure),
    };
    session.authorize(operation)?;
    let mut changed = admin_role;
    changed.enabled = false;
    context.set(&changed)?;
    session.check()?;
    assert_eq!(session.authorize(operation), Err(AuthError::Forbidden));
    assert!(
        !policy
            .snapshot()?
            .ok_or("missing updated policy")?
            .document
            .roles
            .first()
            .ok_or("missing updated role")?
            .enabled
    );
    Ok(())
}

#[tokio::test]
async fn entity_permissions_replay_through_the_normal_postgres_backend()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::atomic::Ordering;
    let Ok(url) = std::env::var("MYKO_PERMISSIONS_TEST_POSTGRES") else {
        return Ok(());
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let config = myko_server::postgres::PostgresConfig {
        url,
        table: format!("permission_events_{suffix}"),
        channel: format!("permission_updates_{suffix}"),
    };
    let mut expected = None;
    for first in [true, false] {
        let policy = Permissions::from_entities(CredentialScopeMode::LocalGrants);
        let attached = policy.clone();
        let (send, ready) = tokio::sync::oneshot::channel();
        let server = myko_server::MykoServer::builder()
            .with_bind_addr("127.0.0.1:0".parse()?)
            .with_postgres(config.clone())
            .try_after_init(move |server| {
                let context = Arc::new(server.ctx());
                attached.attach(context.clone())?;
                send.send(context)
                    .map_err(|_| "fixture startup receiver closed")?;
                Ok(())
            })
            .build();
        let task = tokio::spawn(async move { server.run().await });
        let context = tokio::time::timeout(Duration::from_secs(10), ready).await??;
        if first {
            PermissionAuthenticator::new(
                Arc::new(FixedIdentity(signed_in_identity("owner", INITIAL_OWNER))),
                policy.clone(),
            )
            .with_admin_email(AdminEmail::new(INITIAL_OWNER)?)
            .with_user_discovery()
            .authenticate(Credentials::default())
            .await?;
            expected = policy.snapshot()?.map(|snapshot| snapshot.document.clone());
            let health = context.persist_health();
            tokio::time::timeout(Duration::from_secs(10), async {
                while health.total_persisted.load(Ordering::Acquire) < 4
                    || health.queued.load(Ordering::Acquire) != 0
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
        } else {
            assert!(expected.is_some());
            assert_eq!(
                policy.snapshot()?.map(|snapshot| snapshot.document.clone()),
                expected
            );
        }
        task.abort();
        let _ = task.await;
    }
    Ok(())
}

#[tokio::test]
async fn legacy_import_preserves_ids_and_never_restores_grants_over_entity_state()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let source = MemoryPermissionStorage::new();
    let owner = signed_in_identity("owner", INITIAL_OWNER);
    let server = myko_server::MykoServer::builder().build();
    let context = Arc::new(server.ctx());
    let legacy = Permissions::new(Arc::new(source.clone()), CredentialScopeMode::LocalGrants);
    let legacy_server = myko_server::MykoServer::builder().build();
    legacy.attach(Arc::new(legacy_server.ctx()))?;
    PermissionAuthenticator::new(Arc::new(FixedIdentity(owner)), legacy)
        .with_admin_email(AdminEmail::new(INITIAL_OWNER)?)
        .authenticate(Credentials::default())
        .await?;
    let original = source.load()?.ok_or("missing source")?;
    let policy = Permissions::from_entities(CredentialScopeMode::LocalGrants);
    policy.attach(context.clone())?;
    assert!(policy.import_from(&source)?);
    assert_eq!(
        policy.snapshot()?.ok_or("missing import")?.document,
        original.document
    );
    let mut role = original
        .document
        .roles
        .first()
        .ok_or("missing source role")?
        .clone();
    role.enabled = false;
    context.set(&role)?;
    assert!(!policy.import_from(&source)?);
    assert!(
        !policy
            .snapshot()?
            .ok_or("missing policy")?
            .document
            .roles
            .first()
            .ok_or("missing role")?
            .enabled
    );
    assert_eq!(source.load()?, Some(original));
    Ok(())
}

#[tokio::test]
async fn provider_builder_defaults_to_scoped_access_and_preserves_explicit_fallback()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = myko_server::MykoServer::builder().build();
    let policy = Permissions::from_entities(CredentialScopeMode::LocalGrants);
    policy.attach(Arc::new(server.ctx()))?;
    let strict = policy
        .auth_provider(Arc::new(FixedIdentity(signed_in_identity(
            "owner",
            INITIAL_OWNER,
        ))))
        .with_admin_email(Some(AdminEmail::new(INITIAL_OWNER)?))
        .with_user_discovery()
        .with_live_permissions()
        .build();
    let owner = strict.authenticate(Credentials::default()).await?;
    assert!(owner.uses_live_permissions());
    let custom = AuthorizationRequest::Discover {
        kind: OperationKind::Command,
        operation: "UnmappedApplicationCommand",
    };
    assert_eq!(owner.authorize(custom), Err(AuthError::Forbidden));
    owner.authorize(AuthorizationRequest::Discover {
        kind: OperationKind::Report,
        operation: stringify!(GetPermissionStructure),
    })?;
    let application = policy
        .auth_provider(Arc::new(FixedIdentity(signed_in_identity(
            "person",
            "person@example.test",
        ))))
        .with_admin_email(None)
        .with_user_discovery()
        .with_application_policy(Arc::new(myko_auth::AuthenticatedAccess))
        .with_live_permissions()
        .build();
    let person = application.authenticate(Credentials::default()).await?;
    person.authorize(custom)?;
    assert_eq!(
        person.authorize(AuthorizationRequest::Discover {
            kind: OperationKind::Report,
            operation: stringify!(GetPermissionStructure)
        }),
        Err(AuthError::Forbidden)
    );
    let snapshot = policy.snapshot()?.ok_or("missing authority")?;
    assert!(
        snapshot
            .document
            .principals
            .iter()
            .any(|principal| principal.subject == "person")
    );
    assert_eq!(snapshot.document.bindings.len(), 1);
    Ok(())
}
