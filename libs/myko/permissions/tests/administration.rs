#![allow(clippy::panic_in_result_fn)]

use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use myko::{
    command::{CommandContext, CommandHandler},
    item::Eventable,
    request::RequestContext,
    server::MykoServerContext,
};
use myko_auth::{
    AuthError, AuthSession, AuthenticatedIdentity, AuthorizationRequest, Authorizer, OperationKind,
    Principal, PrincipalKind, ScopedAuthorizer,
};
use myko_permissions::{
    AUTHORITY_ID, ApplyPermissionChanges, CredentialScopeMode, MemoryPermissionStorage,
    OperatorPrincipal, PermissionAuditActor, PermissionAuthority, PermissionAuthorityId,
    PermissionChange, PermissionChangeMode, PermissionChangeResult, PermissionEffect,
    PermissionError, PermissionGrantee, PermissionGroup, PermissionGroupId, PermissionMembership,
    PermissionMembershipId, PermissionOverride, PermissionOverrideId, PermissionPrincipal,
    PermissionPrincipalKind, PermissionRole, PermissionRule, PermissionStorage, Permissions,
    UpsertPermissionRole, bootstrap, recover, replace_as_operator,
};
use myko_server::MykoServer;
use serde_json::Value;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub mod application_entity {
    #[allow(clippy::wildcard_imports)]
    use myko::prelude::*;

    #[myko::myko_item]
    pub struct PermissionTestThing {
        pub name: String,
    }
}

struct Fixture {
    _server: Arc<MykoServer>,
    context: Arc<MykoServerContext>,
    storage: Arc<MemoryPermissionStorage>,
    permissions: Arc<Permissions>,
    authorizer: Arc<ScopedAuthorizer>,
}

fn identity(subject: &str) -> TestResult<AuthenticatedIdentity> {
    Ok(AuthenticatedIdentity {
        user_id: None,
        principal: Principal {
            issuer: "fixture".into(),
            subject: subject.into(),
            kind: PrincipalKind::User,
        },
        client_id: None,
        scopes: vec![],
        delegation: vec![],
        claims: Value::Null,
        expires_at: SystemTime::now()
            .checked_add(Duration::from_mins(1))
            .ok_or("expiry")?,
    })
}

fn operator_principal(subject: &str) -> OperatorPrincipal {
    OperatorPrincipal {
        issuer: "fixture".into(),
        subject: subject.into(),
        kind: PermissionPrincipalKind::User,
        name: subject.into(),
    }
}

fn fixture(bootstrapped: bool) -> TestResult<Fixture> {
    let storage = Arc::new(MemoryPermissionStorage::new());
    if bootstrapped {
        bootstrap(
            storage.as_ref(),
            operator_principal("admin-a"),
            "fixture-operator",
            "first administrator",
        )?;
    }
    let server = Arc::new(MykoServer::builder().build());
    let context = Arc::new(server.ctx());
    let permissions = Arc::new(Permissions::new(
        storage.clone(),
        CredentialScopeMode::LocalGrants,
    ));
    permissions.attach(context.clone())?;
    let authorizer = Arc::new(ScopedAuthorizer::new(permissions.clone()));
    Ok(Fixture {
        _server: server,
        context,
        storage,
        permissions,
        authorizer,
    })
}

fn command_context(
    fixture: &Fixture,
    subject: &str,
    operation: &str,
) -> TestResult<CommandContext> {
    let session = AuthSession::new(identity(subject)?, fixture.authorizer.clone());
    let request = RequestContext::from_client(
        "fixture-tx".into(),
        "fixture-client".into(),
        fixture.context.host_id,
    )
    .with_auth(Some(session));
    Ok(CommandContext::new(
        operation.into(),
        Arc::new(request),
        fixture.context.clone(),
    ))
}

fn apply(
    fixture: &Fixture,
    subject: &str,
    revision: u64,
    changes: Vec<PermissionChange>,
) -> TestResult<PermissionChangeResult> {
    let context = command_context(fixture, subject, "ApplyPermissionChanges")?;
    Ok(ApplyPermissionChanges {
        expected_revision: revision,
        changes,
        reason: "integration edit".into(),
    }
    .execute(context)
    .map_err(|error| std::io::Error::other(error.message))?)
}

fn accepted(fixture: &Fixture) -> TestResult<PermissionAuthority> {
    fixture
        .storage
        .load()?
        .ok_or_else(|| "snapshot missing".into())
}

fn principal(fixture: &Fixture, subject: &str) -> TestResult<PermissionPrincipal> {
    accepted(fixture)?
        .document
        .principals
        .into_iter()
        .find(|principal| principal.subject == subject)
        .ok_or_else(|| "principal missing".into())
}

#[test]
fn empty_store_never_bootstraps_or_opens_administration_implicitly() -> TestResult {
    let fixture = fixture(false)?;
    let request = AuthorizationRequest::Discover {
        kind: OperationKind::Command,
        operation: "ApplyPermissionChanges",
    };
    assert_eq!(
        fixture.authorizer.authorize(&identity("admin-a")?, request),
        Err(AuthError::Forbidden)
    );
    assert!(fixture.storage.load()?.is_none());
    assert!(fixture.permissions.snapshot()?.is_none());
    assert!(
        fixture
            .context
            .registry
            .get_or_create(PermissionPrincipal::ENTITY_NAME_STATIC)
            .snapshot()
            .is_empty()
    );
    assert!(apply(&fixture, "admin-a", 0, vec![]).is_err());
    assert!(fixture.storage.load()?.is_none());
    Ok(())
}

#[test]
fn last_effective_administrator_cannot_be_disabled_unbound_or_denied_child_authority() -> TestResult
{
    let fixture = fixture(true)?;
    let before = accepted(&fixture)?;
    let mut admin = principal(&fixture, "admin-a")?;
    admin.enabled = false;
    let binding = before.document.bindings.first().ok_or("binding")?.clone();
    let changes = [
        PermissionChange::UpsertPrincipal(admin.clone()),
        PermissionChange::DeleteRoleBinding(binding.id),
        PermissionChange::UpsertOverride(PermissionOverride {
            id: PermissionOverrideId::from("deny-role-write"),
            authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
            grantee: PermissionGrantee::Principal(admin.id),
            rule: PermissionRule {
                scope: "PermissionRole:write".into(),
                effect: PermissionEffect::Deny,
            },
        }),
    ];
    for change in changes {
        let error = apply(&fixture, "admin-a", 1, vec![change])
            .err()
            .ok_or("lockout edit unexpectedly succeeded")?;
        assert!(
            error
                .to_string()
                .contains("last effective permissions administrator")
        );
        assert_eq!(accepted(&fixture)?, before);
        let visible = fixture
            .context
            .registry
            .get_or_create(PermissionPrincipal::ENTITY_NAME_STATIC)
            .snapshot();
        assert_eq!(visible.len(), 1);
        assert!(visible.iter().all(|(_, item)| {
            item.as_any()
                .downcast_ref::<PermissionPrincipal>()
                .is_some_and(|principal| principal.enabled)
        }));
    }
    Ok(())
}

#[test]
fn group_membership_and_role_changes_cannot_remove_the_last_effective_administrator() -> TestResult
{
    let fixture = fixture(true)?;
    let before = accepted(&fixture)?;
    let admin = principal(&fixture, "admin-a")?;
    let group = PermissionGroup {
        id: PermissionGroupId::from("administrators"),
        authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
        name: "Administrators".into(),
        enabled: true,
    };
    let membership = PermissionMembership {
        id: PermissionMembershipId::from("admin-membership"),
        authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
        principal_id: admin.id,
        group_id: group.id.clone(),
    };
    let mut binding = before.document.bindings.first().ok_or("binding")?.clone();
    binding.grantee = PermissionGrantee::Group(group.id.clone());
    apply(
        &fixture,
        "admin-a",
        1,
        vec![
            PermissionChange::UpsertGroup(group.clone()),
            PermissionChange::UpsertMembership(membership.clone()),
            PermissionChange::UpsertRoleBinding(binding),
        ],
    )?;
    let accepted = accepted(&fixture)?;
    let mut disabled_group = group;
    disabled_group.enabled = false;
    let mut disabled_role = accepted.document.roles.first().ok_or("role")?.clone();
    disabled_role.enabled = false;
    for change in [
        PermissionChange::UpsertGroup(disabled_group),
        PermissionChange::DeleteMembership(membership.id),
        PermissionChange::UpsertRole(disabled_role),
    ] {
        let error = apply(&fixture, "admin-a", 2, vec![change])
            .err()
            .ok_or("group lockout succeeded")?;
        assert!(
            error
                .to_string()
                .contains("last effective permissions administrator")
        );
        assert_eq!(fixture.storage.load()?, Some(accepted.clone()));
    }
    Ok(())
}

#[test]
fn accepted_role_changes_update_reactive_rows_and_revoke_old_sessions() -> TestResult {
    let fixture = fixture(true)?;
    let old_session = AuthSession::new(identity("admin-a")?, fixture.authorizer.clone());
    let read = AuthorizationRequest::Discover {
        kind: OperationKind::Query,
        operation: "GetAllPermissionTestThings",
    };
    assert_eq!(old_session.authorize(read), Err(AuthError::Forbidden));
    let store = fixture
        .context
        .registry
        .get_or_create(PermissionRole::ENTITY_NAME_STATIC);
    let observed = store.clone();
    let saw_grant = Arc::new(AtomicBool::new(false));
    let observed_flag = saw_grant.clone();
    let _guard = store.subscribe_diffs(move |_| {
        let granted = observed.snapshot().iter().any(|(_, item)| {
            item.as_any()
                .downcast_ref::<PermissionRole>()
                .is_some_and(|role| {
                    role.rules
                        .iter()
                        .any(|rule| rule.scope == "PermissionTestThing:read")
                })
        });
        observed_flag.store(granted, Ordering::SeqCst);
    });
    assert!(!saw_grant.load(Ordering::SeqCst));
    let mut role = accepted(&fixture)?
        .document
        .roles
        .first()
        .ok_or("role")?
        .clone();
    role.rules.push(PermissionRule {
        scope: "PermissionTestThing:read".into(),
        effect: PermissionEffect::Allow,
    });
    let context = command_context(&fixture, "admin-a", "UpsertPermissionRole")?;
    let result = UpsertPermissionRole {
        expected_revision: 1,
        role,
        reason: "permit application reads".into(),
    }
    .execute(context)
    .map_err(|error| std::io::Error::other(error.message))?;
    assert_eq!(result.revision, 2);
    assert!(saw_grant.load(Ordering::SeqCst));
    assert_eq!(old_session.check(), Err(AuthError::Revoked));
    let new_session = AuthSession::new(identity("admin-a")?, fixture.authorizer.clone());
    assert_eq!(new_session.authorize(read), Ok(()));
    assert_eq!(
        accepted(&fixture)?.actor,
        PermissionAuditActor::Principal {
            issuer: "fixture".into(),
            subject: "admin-a".into()
        }
    );
    Ok(())
}

#[test]
fn normal_command_handlers_reauthorize_in_process_and_reject_missing_identity() -> TestResult {
    let fixture = fixture(true)?;
    let before = accepted(&fixture)?;
    let mut role = before.document.roles.first().ok_or("role")?.clone();
    role.name = "renamed".into();
    let outsider = command_context(&fixture, "outsider", "UpsertPermissionRole")?;
    assert!(
        UpsertPermissionRole {
            expected_revision: 1,
            role: role.clone(),
            reason: "unauthorized rename".into()
        }
        .execute(outsider)
        .is_err()
    );
    let request = Arc::new(RequestContext::internal(
        "internal".into(),
        fixture.context.host_id,
        "fixture",
    ));
    let internal = CommandContext::new(
        "UpsertPermissionRole".into(),
        request,
        fixture.context.clone(),
    );
    assert!(
        UpsertPermissionRole {
            expected_revision: 1,
            role,
            reason: "missing caller".into()
        }
        .execute(internal)
        .is_err()
    );
    assert_eq!(accepted(&fixture)?, before);
    Ok(())
}

#[test]
fn concurrent_administrators_cannot_both_disable_themselves() -> TestResult {
    let fixture = fixture(true)?;
    recover(
        fixture.storage.as_ref(),
        operator_principal("admin-b"),
        "fixture-operator",
        "add second administrator",
    )?;
    fixture.permissions.refresh()?;
    let mut admin_a = principal(&fixture, "admin-a")?;
    let mut admin_b = principal(&fixture, "admin-b")?;
    admin_a.enabled = false;
    admin_b.enabled = false;
    let context_a = command_context(&fixture, "admin-a", "ApplyPermissionChanges")?;
    let context_b = command_context(&fixture, "admin-b", "ApplyPermissionChanges")?;
    let barrier = Arc::new(Barrier::new(2));
    let gate_a = barrier.clone();
    let first = std::thread::spawn(move || {
        gate_a.wait();
        ApplyPermissionChanges {
            expected_revision: 2,
            changes: vec![PermissionChange::UpsertPrincipal(admin_a)],
            reason: "retire admin a".into(),
        }
        .execute(context_a)
    });
    let second = std::thread::spawn(move || {
        barrier.wait();
        ApplyPermissionChanges {
            expected_revision: 2,
            changes: vec![PermissionChange::UpsertPrincipal(admin_b)],
            reason: "retire admin b".into(),
        }
        .execute(context_b)
    });
    let first = first.join().map_err(|_| "first command panicked")?;
    let second = second.join().map_err(|_| "second command panicked")?;
    assert_eq!(
        [first.is_ok(), second.is_ok()]
            .into_iter()
            .filter(|succeeded| *succeeded)
            .count(),
        1
    );
    let state = accepted(&fixture)?;
    assert_eq!(state.revision, 3);
    assert_eq!(
        state
            .document
            .principals
            .iter()
            .filter(|principal| principal.enabled)
            .count(),
        1
    );
    Ok(())
}

#[test]
fn operator_recovery_restores_access_after_deliberate_lockout_without_application_credentials()
-> TestResult {
    let fixture = fixture(true)?;
    let mut document = accepted(&fixture)?.document;
    for principal in &mut document.principals {
        principal.enabled = false;
    }
    assert_eq!(
        replace_as_operator(
            fixture.storage.as_ref(),
            1,
            document.clone(),
            "fixture-operator",
            "intentional lockout",
            false
        ),
        Err(PermissionError::LockedOut)
    );
    replace_as_operator(
        fixture.storage.as_ref(),
        1,
        document,
        "fixture-operator",
        "intentional lockout",
        true,
    )?;
    fixture.permissions.refresh()?;
    let read = AuthorizationRequest::Discover {
        kind: OperationKind::Query,
        operation: "GetAllPermissionPrincipals",
    };
    assert_eq!(
        fixture.authorizer.authorize(&identity("admin-a")?, read),
        Err(AuthError::Forbidden)
    );
    recover(
        fixture.storage.as_ref(),
        operator_principal("replacement-admin"),
        "fixture-operator",
        "repair lost access",
    )?;
    fixture.permissions.refresh()?;
    assert_eq!(
        fixture
            .authorizer
            .authorize(&identity("replacement-admin")?, read),
        Ok(())
    );
    assert_eq!(accepted(&fixture)?.revision, 3);
    assert_eq!(accepted(&fixture)?.mode, PermissionChangeMode::Recovery);
    assert_eq!(
        accepted(&fixture)?.actor,
        PermissionAuditActor::Operator {
            identity: "fixture-operator".into()
        }
    );
    assert!(
        bootstrap(
            fixture.storage.as_ref(),
            operator_principal("another"),
            "fixture-operator",
            "duplicate bootstrap"
        )
        .is_err()
    );
    assert_eq!(accepted(&fixture)?.revision, 3);
    Ok(())
}

#[test]
fn operator_changes_are_observed_without_refresh_and_recovery_reuses_its_role() -> TestResult {
    let fixture = fixture(false)?;
    let read = AuthorizationRequest::Discover {
        kind: OperationKind::Query,
        operation: "GetAllPermissionPrincipals",
    };
    let admin = identity("admin-a")?;
    assert_eq!(
        fixture.authorizer.authorize(&admin, read),
        Err(AuthError::Forbidden)
    );
    bootstrap(
        fixture.storage.as_ref(),
        operator_principal("admin-a"),
        "fixture-operator",
        "bootstrap live runtime",
    )?;
    await_revision(&fixture, 1)?;
    assert_eq!(fixture.authorizer.authorize(&admin, read), Ok(()));
    let old_session = AuthSession::new(admin.clone(), fixture.authorizer.clone());
    let mut locked = accepted(&fixture)?.document;
    for principal in &mut locked.principals {
        principal.enabled = false;
    }
    replace_as_operator(
        fixture.storage.as_ref(),
        1,
        locked,
        "fixture-operator",
        "deliberate live lockout",
        true,
    )?;
    await_revision(&fixture, 2)?;
    assert_eq!(old_session.check(), Err(AuthError::Revoked));
    assert_eq!(
        fixture.authorizer.authorize(&admin, read),
        Err(AuthError::Forbidden)
    );
    recover(
        fixture.storage.as_ref(),
        operator_principal("admin-a"),
        "fixture-operator",
        "restore live runtime",
    )?;
    await_revision(&fixture, 3)?;
    assert_eq!(fixture.authorizer.authorize(&admin, read), Ok(()));
    let role_count = accepted(&fixture)?.document.roles.len();
    let role_ids: Vec<_> = accepted(&fixture)?
        .document
        .roles
        .into_iter()
        .map(|role| role.id)
        .collect();
    recover(
        fixture.storage.as_ref(),
        operator_principal("admin-a"),
        "fixture-operator",
        "repeat recovery",
    )?;
    await_revision(&fixture, 4)?;
    let current = accepted(&fixture)?;
    assert_eq!(current.document.roles.len(), role_count);
    assert_eq!(
        current
            .document
            .roles
            .iter()
            .map(|role| role.id.clone())
            .collect::<Vec<_>>(),
        role_ids
    );
    assert_eq!(
        fixture
            .context
            .registry
            .get_or_create(PermissionRole::ENTITY_NAME_STATIC)
            .snapshot()
            .len(),
        role_count
    );
    assert_eq!(fixture.authorizer.authorize(&admin, read), Ok(()));
    Ok(())
}

fn await_revision(fixture: &Fixture, revision: u64) -> TestResult {
    let started = Instant::now();
    loop {
        let projected = fixture
            .context
            .registry
            .get_or_create(PermissionAuthority::ENTITY_NAME_STATIC)
            .snapshot();
        let projected_revision = projected.iter().any(|(_, item)| {
            item.as_any()
                .downcast_ref::<PermissionAuthority>()
                .is_some_and(|authority| authority.revision == revision)
        });
        if fixture
            .permissions
            .snapshot()?
            .is_some_and(|snapshot| snapshot.revision == revision)
            && projected_revision
        {
            return Ok(());
        }
        if started.elapsed() >= Duration::from_secs(3) {
            return Err(format!("observer did not publish revision {revision}").into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
