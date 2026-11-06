use crate::{
    engine,
    error::{PermissionError, PermissionResult},
    model::{
        AUTHORITY_ID, PermissionAuditActor, PermissionAuthority, PermissionAuthorityId,
        PermissionChangeMode, PermissionChangeResult, PermissionDocument, PermissionEffect,
        PermissionGrantee, PermissionPrincipal, PermissionPrincipalId, PermissionPrincipalKind,
        PermissionRole, PermissionRoleBinding, PermissionRoleBindingId, PermissionRoleId,
        PermissionRule,
    },
    storage::PermissionStorage,
};

/// An identity selected by a trusted host operator, independently of application login.
pub struct OperatorPrincipal {
    pub issuer: String,
    pub subject: String,
    pub kind: PermissionPrincipalKind,
    pub name: String,
}

/// Establish the first administrator. This function is not a registered network command.
/// # Errors
/// Returns an error if the authority exists, input is invalid, or persistence fails.
pub fn bootstrap(
    storage: &dyn PermissionStorage,
    principal: OperatorPrincipal,
    operator: &str,
    reason: &str,
) -> PermissionResult<PermissionChangeResult> {
    if let Some(current) = storage.load()? {
        return Err(PermissionError::Conflict {
            expected: None,
            actual: Some(current.revision),
        });
    }
    let document = administrator_document(PermissionDocument::default(), principal);
    commit_operator(
        storage,
        None,
        document,
        operator,
        reason,
        PermissionChangeMode::Bootstrap,
        false,
    )
}

/// Restore an administrator using host authority. No Auth0 request or application grant is needed.
/// # Errors
/// Returns an error if storage is empty, the input is invalid, or a concurrent commit wins.
pub fn recover(
    storage: &dyn PermissionStorage,
    principal: OperatorPrincipal,
    operator: &str,
    reason: &str,
) -> PermissionResult<PermissionChangeResult> {
    let current = storage.load()?.ok_or_else(|| {
        PermissionError::Invalid("use bootstrap for an empty permission store".into())
    })?;
    let document = administrator_document(current.document, principal);
    commit_operator(
        storage,
        Some(current.revision),
        document,
        operator,
        reason,
        PermissionChangeMode::Recovery,
        false,
    )
}

/// Replace policy through explicit host authority. `allow_lockout` deliberately bypasses the last-admin guard.
/// # Errors
/// Returns an error for stale revisions, invalid references, forbidden lockout, or storage failure.
pub fn replace_as_operator(
    storage: &dyn PermissionStorage,
    expected_revision: u64,
    document: PermissionDocument,
    operator: &str,
    reason: &str,
    allow_lockout: bool,
) -> PermissionResult<PermissionChangeResult> {
    commit_operator(
        storage,
        Some(expected_revision),
        document,
        operator,
        reason,
        PermissionChangeMode::Recovery,
        allow_lockout,
    )
}

fn commit_operator(
    storage: &dyn PermissionStorage,
    expected: Option<u64>,
    document: PermissionDocument,
    operator: &str,
    reason: &str,
    mode: PermissionChangeMode,
    allow_lockout: bool,
) -> PermissionResult<PermissionChangeResult> {
    if operator.trim().is_empty()
        || reason.trim().is_empty()
        || operator.len() > 4096
        || reason.len() > 4096
    {
        return Err(PermissionError::Invalid(
            "operator identity and reason must be nonempty and at most 4096 bytes".into(),
        ));
    }
    engine::validate(&document, false)?;
    if !allow_lockout && !engine::has_administrator(&document) {
        return Err(PermissionError::LockedOut);
    }
    let snapshot = PermissionAuthority {
        id: PermissionAuthorityId::from(AUTHORITY_ID),
        revision: expected
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| PermissionError::Invalid("permission revision exhausted".into()))?,
        document,
        change_id: uuid::Uuid::new_v4().to_string(),
        actor: PermissionAuditActor::Operator {
            identity: operator.to_owned(),
        },
        reason: reason.to_owned(),
        mode,
    };
    storage.compare_and_swap(expected, &snapshot)?;
    Ok(PermissionChangeResult {
        revision: snapshot.revision,
        change_id: snapshot.change_id,
    })
}

fn administrator_document(
    mut document: PermissionDocument,
    principal: OperatorPrincipal,
) -> PermissionDocument {
    let id = document
        .principals
        .iter()
        .find(|candidate| {
            candidate.issuer == principal.issuer && candidate.subject == principal.subject
        })
        .map_or_else(
            || PermissionPrincipalId::from(uuid::Uuid::new_v4().to_string()),
            |candidate| candidate.id.clone(),
        );
    document.principals.retain(|candidate| candidate.id != id);
    document
        .memberships
        .retain(|membership| membership.principal_id != id);
    document
        .bindings
        .retain(|binding| binding.grantee != PermissionGrantee::Principal(id.clone()));
    document
        .overrides
        .retain(|entry| entry.grantee != PermissionGrantee::Principal(id.clone()));
    document.principals.push(PermissionPrincipal {
        id: id.clone(),
        authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
        issuer: principal.issuer,
        subject: principal.subject,
        kind: principal.kind,
        enabled: true,
        name: principal.name,
    });
    let role_key = format!("myko-permissions/recovery/role/{id}");
    let role_id = PermissionRoleId::from(
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, role_key.as_bytes()).to_string(),
    );
    document.roles.retain(|role| role.id != role_id);
    let binding_key = format!("myko-permissions/recovery/binding/{id}");
    let binding_id = PermissionRoleBindingId::from(
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, binding_key.as_bytes()).to_string(),
    );
    document.bindings.retain(|binding| binding.id != binding_id);
    document.roles.push(PermissionRole {
        id: role_id.clone(),
        authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
        name: "Operator recovery administrator".into(),
        enabled: true,
        rules: engine::protected_entities()
            .iter()
            .flat_map(|entity_type| {
                ["read", "write", "delete"].map(|action| PermissionRule {
                    scope: format!("{entity_type}:{action}"),
                    effect: PermissionEffect::Allow,
                })
            })
            .collect(),
    });
    document.bindings.push(PermissionRoleBinding {
        id: binding_id,
        authority_id: PermissionAuthorityId::from(AUTHORITY_ID),
        role_id,
        grantee: PermissionGrantee::Principal(id),
    });
    document
}
