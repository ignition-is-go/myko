use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime},
};

use myko::{common::with_id::WithId, item::Eventable};
use myko_auth::{
    AuthenticatedIdentity, EntityAction, EntityScope, PermissionCatalog, Principal, PrincipalKind,
    ScopeDecision,
};

use crate::{
    error::{PermissionError, PermissionResult},
    inspection::PermissionRuleSource,
    model::{
        AUTHORITY_ID, PermissionAuthority, PermissionAuthorityId, PermissionChange,
        PermissionDocument, PermissionEffect, PermissionGrantee, PermissionGroup,
        PermissionMembership, PermissionOverride, PermissionPrincipal, PermissionPrincipalKind,
        PermissionRole, PermissionRoleBinding, PermissionRule,
    },
};

const MAX_RECORDS: usize = 10_000;
const MAX_TEXT_BYTES: usize = 4096;

fn invalid(message: impl Into<String>) -> PermissionError {
    PermissionError::Invalid(message.into())
}

fn text(value: &str, label: &str) -> PermissionResult<()> {
    if value.trim().is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(invalid(format!("invalid {label}")));
    }
    Ok(())
}

fn ids<T: WithId>(rows: &[T], label: &str) -> PermissionResult<BTreeSet<String>> {
    let mut result = BTreeSet::new();
    for row in rows {
        let id = row.id().to_string();
        text(&id, label)?;
        if !result.insert(id) {
            return Err(invalid(format!("duplicate {label}")));
        }
    }
    Ok(result)
}

fn authority(value: &PermissionAuthorityId) -> PermissionResult<()> {
    if value.to_string() != AUTHORITY_ID {
        return Err(invalid("permission record belongs to another authority"));
    }
    Ok(())
}

fn rule(value: &PermissionRule, require_known_scopes: bool) -> PermissionResult<()> {
    text(&value.scope, "permission scope")?;
    let Some((entity, action)) = value.scope.rsplit_once(':') else {
        return Err(invalid("scope must have an entity and action"));
    };
    if entity.is_empty()
        || entity.chars().any(char::is_whitespace)
        || !matches!(action, "read" | "write" | "delete")
    {
        return Err(invalid("scope must use read, write, or delete"));
    }
    if require_known_scopes
        && !PermissionCatalog::registered()
            .scopes()
            .any(|scope| scope.to_string() == value.scope)
    {
        return Err(invalid(format!(
            "unknown permission scope: {}",
            value.scope
        )));
    }
    Ok(())
}

fn grantee_exists(
    value: &PermissionGrantee,
    principals: &BTreeSet<String>,
    groups: &BTreeSet<String>,
) -> bool {
    match value {
        PermissionGrantee::Principal(id) => principals.contains(id.as_ref()),
        PermissionGrantee::Group(id) => groups.contains(id.as_ref()),
    }
}

fn validate_named_records(
    document: &PermissionDocument,
    require_known_scopes: bool,
) -> PermissionResult<()> {
    let mut identities = BTreeSet::new();
    for principal in &document.principals {
        authority(&principal.authority_id)?;
        text(&principal.issuer, "principal issuer")?;
        text(&principal.subject, "principal subject")?;
        text(&principal.name, "principal name")?;
        if !identities.insert((&principal.issuer, &principal.subject)) {
            return Err(invalid("duplicate principal issuer and subject"));
        }
    }
    for group in &document.groups {
        authority(&group.authority_id)?;
        text(&group.name, "group name")?;
    }
    for role in &document.roles {
        authority(&role.authority_id)?;
        text(&role.name, "role name")?;
        let mut scopes = BTreeSet::new();
        for requirement in &role.rules {
            rule(requirement, require_known_scopes)?;
            if !scopes.insert(&requirement.scope) {
                return Err(invalid("duplicate role scope"));
            }
        }
    }
    Ok(())
}

pub fn validate(document: &PermissionDocument, require_known_scopes: bool) -> PermissionResult<()> {
    let records = [
        document.principals.len(),
        document.groups.len(),
        document.memberships.len(),
        document.roles.len(),
        document.bindings.len(),
        document.overrides.len(),
    ]
    .into_iter()
    .chain(document.roles.iter().map(|role| role.rules.len()))
    .fold(0usize, usize::saturating_add);
    if records > MAX_RECORDS {
        return Err(invalid(
            "permission document exceeds 10000 records and rules",
        ));
    }
    let principals = ids(&document.principals, "principal ID")?;
    let groups = ids(&document.groups, "group ID")?;
    ids(&document.memberships, "membership ID")?;
    let roles = ids(&document.roles, "role ID")?;
    ids(&document.bindings, "role binding ID")?;
    ids(&document.overrides, "override ID")?;

    validate_named_records(document, require_known_scopes)?;
    let mut memberships = BTreeSet::new();
    for membership in &document.memberships {
        authority(&membership.authority_id)?;
        if !principals.contains(&membership.principal_id.to_string())
            || !groups.contains(&membership.group_id.to_string())
        {
            return Err(invalid(
                "membership references a missing principal or group",
            ));
        }
        if !memberships.insert((&membership.principal_id, &membership.group_id)) {
            return Err(invalid("duplicate principal and group membership"));
        }
    }
    let mut bindings = BTreeSet::new();
    for binding in &document.bindings {
        authority(&binding.authority_id)?;
        if !roles.contains(&binding.role_id.to_string())
            || !grantee_exists(&binding.grantee, &principals, &groups)
        {
            return Err(invalid("role binding references a missing role or grantee"));
        }
        if !bindings.insert((&binding.role_id, &binding.grantee)) {
            return Err(invalid("duplicate role and grantee binding"));
        }
    }
    let mut overrides = BTreeSet::new();
    for permission_override in &document.overrides {
        authority(&permission_override.authority_id)?;
        rule(&permission_override.rule, require_known_scopes)?;
        if !grantee_exists(&permission_override.grantee, &principals, &groups) {
            return Err(invalid("permission override references a missing grantee"));
        }
        if !overrides.insert((
            &permission_override.grantee,
            &permission_override.rule.scope,
        )) {
            return Err(invalid("duplicate grantee and scope override"));
        }
    }
    Ok(())
}

fn upsert<T: WithId + Clone>(rows: &mut Vec<T>, value: &T) {
    if let Some(existing) = rows.iter_mut().find(|row| row.id() == value.id()) {
        *existing = value.clone();
    } else {
        rows.push(value.clone());
    }
}

fn remove<T: WithId>(rows: &mut Vec<T>, id: &str) {
    rows.retain(|row| row.id().as_ref() != id);
}

pub fn apply_changes(
    document: &mut PermissionDocument,
    changes: &[PermissionChange],
) -> PermissionResult<()> {
    if changes.len() > MAX_RECORDS {
        return Err(invalid("permission batch exceeds 10000 changes"));
    }
    for change in changes {
        match change {
            PermissionChange::UpsertPrincipal(value) => upsert(&mut document.principals, value),
            PermissionChange::DeletePrincipal(id) => {
                remove(&mut document.principals, id.as_ref());
            }
            PermissionChange::UpsertGroup(value) => upsert(&mut document.groups, value),
            PermissionChange::DeleteGroup(id) => remove(&mut document.groups, id.as_ref()),
            PermissionChange::UpsertMembership(value) => upsert(&mut document.memberships, value),
            PermissionChange::DeleteMembership(id) => {
                remove(&mut document.memberships, id.as_ref());
            }
            PermissionChange::UpsertRole(value) => upsert(&mut document.roles, value),
            PermissionChange::DeleteRole(id) => remove(&mut document.roles, id.as_ref()),
            PermissionChange::UpsertRoleBinding(value) => upsert(&mut document.bindings, value),
            PermissionChange::DeleteRoleBinding(id) => {
                remove(&mut document.bindings, id.as_ref());
            }
            PermissionChange::UpsertOverride(value) => upsert(&mut document.overrides, value),
            PermissionChange::DeleteOverride(id) => {
                remove(&mut document.overrides, id.as_ref());
            }
        }
    }
    Ok(())
}

const fn kind_matches(stored: PermissionPrincipalKind, verified: &PrincipalKind) -> bool {
    matches!(
        (stored, verified),
        (PermissionPrincipalKind::User, PrincipalKind::User)
            | (PermissionPrincipalKind::Service, PrincipalKind::Service)
            | (PermissionPrincipalKind::Agent, PrincipalKind::Agent)
    )
}

pub fn explicit_decision(
    document: &PermissionDocument,
    identity: &AuthenticatedIdentity,
    scope: EntityScope,
) -> ScopeDecision {
    resolve_explicit(document, identity, scope, None)
}

pub fn explicit_decision_sources(
    document: &PermissionDocument,
    identity: &AuthenticatedIdentity,
    scope: EntityScope,
) -> (ScopeDecision, Vec<PermissionRuleSource>) {
    let mut sources = Vec::new();
    let decision = resolve_explicit(document, identity, scope, Some(&mut sources));
    (decision, sources)
}

fn resolve_explicit(
    document: &PermissionDocument,
    identity: &AuthenticatedIdentity,
    scope: EntityScope,
    mut sources: Option<&mut Vec<PermissionRuleSource>>,
) -> ScopeDecision {
    let Some(principal) = document.principals.iter().find(|principal| {
        principal.issuer == identity.principal.issuer
            && principal.subject == identity.principal.subject
            && principal.enabled
            && kind_matches(principal.kind, &identity.principal.kind)
    }) else {
        return ScopeDecision::Deny;
    };
    let scope_name = scope.to_string();
    let principal_grantee = PermissionGrantee::Principal(principal.id.clone());
    let direct = combine_with_sources(
        document
            .overrides
            .iter()
            .filter(|entry| entry.grantee == principal_grantee && entry.rule.scope == scope_name)
            .map(|entry| (entry.rule.effect, entry)),
        |entry| PermissionRuleSource::PrincipalOverride(entry.id.to_string()),
        sources.as_deref_mut(),
    );
    if direct != ScopeDecision::Unset {
        return direct;
    }
    let groups: BTreeSet<_> = document
        .groups
        .iter()
        .filter(|group| {
            group.enabled
                && document.memberships.iter().any(|membership| {
                    membership.principal_id == principal.id && membership.group_id == group.id
                })
        })
        .map(|group| group.id.clone())
        .collect();
    let is_group = |grantee: &PermissionGrantee| matches!(grantee, PermissionGrantee::Group(id) if groups.contains(id));
    let overrides = combine_with_sources(
        document
            .overrides
            .iter()
            .filter(|entry| is_group(&entry.grantee) && entry.rule.scope == scope_name)
            .map(|entry| (entry.rule.effect, entry)),
        |entry| PermissionRuleSource::GroupOverride(entry.id.to_string()),
        sources.as_deref_mut(),
    );
    if overrides != ScopeDecision::Unset {
        return overrides;
    }
    combine_with_sources(
        document
            .roles
            .iter()
            .filter(|role| {
                role.enabled
                    && document.bindings.iter().any(|binding| {
                        binding.role_id == role.id
                            && (binding.grantee == principal_grantee || is_group(&binding.grantee))
                    })
            })
            .flat_map(|role| {
                role.rules
                    .iter()
                    .filter(|rule| rule.scope == scope_name)
                    .map(move |rule| (rule.effect, role))
            }),
        |role| PermissionRuleSource::Role(role.id.to_string()),
        sources,
    )
}

fn combine_with_sources<T>(
    entries: impl Iterator<Item = (PermissionEffect, T)>,
    source: impl Fn(T) -> PermissionRuleSource,
    mut sources: Option<&mut Vec<PermissionRuleSource>>,
) -> ScopeDecision {
    let mut allowed = false;
    let mut denied = false;
    for (effect, entry) in entries {
        if let Some(sources) = &mut sources {
            sources.push(source(entry));
        }
        match effect {
            PermissionEffect::Allow => allowed = true,
            PermissionEffect::Deny => denied = true,
        }
    }
    if denied {
        ScopeDecision::Deny
    } else if allowed {
        ScopeDecision::Allow
    } else {
        ScopeDecision::Unset
    }
}

pub const fn protected_entities() -> [&'static str; 7] {
    [
        PermissionAuthority::ENTITY_NAME_STATIC,
        PermissionPrincipal::ENTITY_NAME_STATIC,
        PermissionGroup::ENTITY_NAME_STATIC,
        PermissionMembership::ENTITY_NAME_STATIC,
        PermissionRole::ENTITY_NAME_STATIC,
        PermissionRoleBinding::ENTITY_NAME_STATIC,
        PermissionOverride::ENTITY_NAME_STATIC,
    ]
}

pub fn has_administrator(document: &PermissionDocument) -> bool {
    let catalog = PermissionCatalog::registered();
    document
        .principals
        .iter()
        .filter(|principal| principal.enabled)
        .any(|principal| {
            let identity = AuthenticatedIdentity {
                principal: Principal {
                    issuer: principal.issuer.clone(),
                    subject: principal.subject.clone(),
                    kind: match principal.kind {
                        PermissionPrincipalKind::User => PrincipalKind::User,
                        PermissionPrincipalKind::Service => PrincipalKind::Service,
                        PermissionPrincipalKind::Agent => PrincipalKind::Agent,
                    },
                },
                client_id: None,
                scopes: Vec::new(),
                delegation: Vec::new(),
                claims: serde_json::Value::Null,
                expires_at: SystemTime::UNIX_EPOCH
                    .checked_add(Duration::from_hours(1_139_568))
                    .unwrap_or(SystemTime::UNIX_EPOCH),
            };
            protected_entities().into_iter().all(|entity_type| {
                [
                    EntityAction::Read,
                    EntityAction::Write,
                    EntityAction::Delete,
                ]
                .into_iter()
                .all(|action| {
                    catalog
                        .entity_scope(entity_type, action)
                        .is_some_and(|scope| {
                            catalog
                                .authorize_scope(scope, |candidate| {
                                    explicit_decision(document, &identity, candidate)
                                })
                                .is_ok()
                        })
                })
            })
        })
}

pub const fn required_scope(change: &PermissionChange) -> EntityScope {
    let (entity_type, action) = match change {
        PermissionChange::UpsertPrincipal(_) => {
            (PermissionPrincipal::ENTITY_NAME_STATIC, EntityAction::Write)
        }
        PermissionChange::DeletePrincipal(_) => (
            PermissionPrincipal::ENTITY_NAME_STATIC,
            EntityAction::Delete,
        ),
        PermissionChange::UpsertGroup(_) => {
            (PermissionGroup::ENTITY_NAME_STATIC, EntityAction::Write)
        }
        PermissionChange::DeleteGroup(_) => {
            (PermissionGroup::ENTITY_NAME_STATIC, EntityAction::Delete)
        }
        PermissionChange::UpsertMembership(_) => (
            PermissionMembership::ENTITY_NAME_STATIC,
            EntityAction::Write,
        ),
        PermissionChange::DeleteMembership(_) => (
            PermissionMembership::ENTITY_NAME_STATIC,
            EntityAction::Delete,
        ),
        PermissionChange::UpsertRole(_) => {
            (PermissionRole::ENTITY_NAME_STATIC, EntityAction::Write)
        }
        PermissionChange::DeleteRole(_) => {
            (PermissionRole::ENTITY_NAME_STATIC, EntityAction::Delete)
        }
        PermissionChange::UpsertRoleBinding(_) => (
            PermissionRoleBinding::ENTITY_NAME_STATIC,
            EntityAction::Write,
        ),
        PermissionChange::DeleteRoleBinding(_) => (
            PermissionRoleBinding::ENTITY_NAME_STATIC,
            EntityAction::Delete,
        ),
        PermissionChange::UpsertOverride(_) => {
            (PermissionOverride::ENTITY_NAME_STATIC, EntityAction::Write)
        }
        PermissionChange::DeleteOverride(_) => {
            (PermissionOverride::ENTITY_NAME_STATIC, EntityAction::Delete)
        }
    };
    EntityScope {
        entity_type,
        action,
    }
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;

    fn principal(id: &str, subject: &str) -> PermissionPrincipal {
        PermissionPrincipal {
            id: id.into(),
            authority_id: AUTHORITY_ID.into(),
            issuer: "fixture".into(),
            subject: subject.into(),
            kind: PermissionPrincipalKind::User,
            enabled: true,
            name: subject.into(),
        }
    }
    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity {
            principal: Principal {
                issuer: "fixture".into(),
                subject: "admin".into(),
                kind: PrincipalKind::User,
            },
            client_id: None,
            scopes: Vec::new(),
            delegation: Vec::new(),
            claims: serde_json::Value::Null,
            expires_at: SystemTime::UNIX_EPOCH,
        }
    }
    fn document() -> PermissionDocument {
        PermissionDocument {
            principals: vec![principal("admin", "admin")],
            roles: vec![PermissionRole {
                id: "admin-role".into(),
                authority_id: AUTHORITY_ID.into(),
                name: "Administrators".into(),
                enabled: true,
                rules: [
                    "PermissionAuthority:read",
                    "PermissionAuthority:write",
                    "PermissionAuthority:delete",
                ]
                .into_iter()
                .map(|scope| PermissionRule {
                    scope: scope.into(),
                    effect: PermissionEffect::Allow,
                })
                .collect(),
            }],
            bindings: vec![PermissionRoleBinding {
                id: "admin-binding".into(),
                authority_id: AUTHORITY_ID.into(),
                role_id: "admin-role".into(),
                grantee: PermissionGrantee::Principal("admin".into()),
            }],
            ..PermissionDocument::default()
        }
    }
    fn permission_override(
        id: &str,
        grantee: PermissionGrantee,
        effect: PermissionEffect,
    ) -> PermissionOverride {
        PermissionOverride {
            id: id.into(),
            authority_id: AUTHORITY_ID.into(),
            grantee,
            rule: PermissionRule {
                scope: "PermissionRole:write".into(),
                effect,
            },
        }
    }
    fn role_scope() -> EntityScope {
        EntityScope {
            entity_type: "PermissionRole",
            action: EntityAction::Write,
        }
    }

    #[test]
    fn last_administrator_uses_effective_inherited_permissions_and_child_overrides()
    -> PermissionResult<()> {
        let mut document = document();
        validate(&document, true)?;
        assert!(has_administrator(&document));
        document.overrides.push(permission_override(
            "deny-role-write",
            PermissionGrantee::Principal("admin".into()),
            PermissionEffect::Deny,
        ));
        validate(&document, true)?;
        assert!(!has_administrator(&document));
        document.principals.push(principal("second", "second"));
        document.bindings.push(PermissionRoleBinding {
            id: "second-binding".into(),
            authority_id: AUTHORITY_ID.into(),
            role_id: "admin-role".into(),
            grantee: PermissionGrantee::Principal("second".into()),
        });
        assert!(has_administrator(&document));
        Ok(())
    }

    #[test]
    fn group_rules_disable_cleanly_and_principal_overrides_take_precedence() -> PermissionResult<()>
    {
        let mut document = document();
        document.groups.push(PermissionGroup {
            id: "operators".into(),
            authority_id: AUTHORITY_ID.into(),
            name: "Operators".into(),
            enabled: true,
        });
        document.memberships.push(PermissionMembership {
            id: "membership".into(),
            authority_id: AUTHORITY_ID.into(),
            principal_id: "admin".into(),
            group_id: "operators".into(),
        });
        document
            .bindings
            .first_mut()
            .ok_or_else(|| invalid("fixture binding"))?
            .grantee = PermissionGrantee::Group("operators".into());
        assert!(has_administrator(&document));
        document.overrides.push(permission_override(
            "group-deny",
            PermissionGrantee::Group("operators".into()),
            PermissionEffect::Deny,
        ));
        assert_eq!(
            explicit_decision(&document, &identity(), role_scope()),
            ScopeDecision::Deny
        );
        document.overrides.push(permission_override(
            "principal-allow",
            PermissionGrantee::Principal("admin".into()),
            PermissionEffect::Allow,
        ));
        assert_eq!(
            explicit_decision(&document, &identity(), role_scope()),
            ScopeDecision::Allow
        );
        assert!(has_administrator(&document));
        document
            .groups
            .first_mut()
            .ok_or_else(|| invalid("fixture group"))?
            .enabled = false;
        assert!(!has_administrator(&document));
        Ok(())
    }

    #[test]
    fn denials_win_within_group_and_role_tiers() {
        let mut document = document();
        for (id, effect) in [
            ("allowing", PermissionEffect::Allow),
            ("denying", PermissionEffect::Deny),
        ] {
            document.groups.push(PermissionGroup {
                id: id.into(),
                authority_id: AUTHORITY_ID.into(),
                name: id.into(),
                enabled: true,
            });
            document.memberships.push(PermissionMembership {
                id: id.into(),
                authority_id: AUTHORITY_ID.into(),
                principal_id: "admin".into(),
                group_id: id.into(),
            });
            document.overrides.push(permission_override(
                id,
                PermissionGrantee::Group(id.into()),
                effect,
            ));
        }
        assert_eq!(
            explicit_decision(&document, &identity(), role_scope()),
            ScopeDecision::Deny
        );
        document.overrides.clear();
        document.roles.push(PermissionRole {
            id: "deny-role".into(),
            authority_id: AUTHORITY_ID.into(),
            name: "Deny".into(),
            enabled: true,
            rules: vec![PermissionRule {
                scope: "PermissionRole:write".into(),
                effect: PermissionEffect::Deny,
            }],
        });
        document.bindings.push(PermissionRoleBinding {
            id: "deny-binding".into(),
            authority_id: AUTHORITY_ID.into(),
            role_id: "deny-role".into(),
            grantee: PermissionGrantee::Principal("admin".into()),
        });
        assert_eq!(
            explicit_decision(&document, &identity(), role_scope()),
            ScopeDecision::Deny
        );
    }

    #[test]
    fn duplicate_identities_dangling_references_and_unknown_edit_scopes_are_rejected()
    -> PermissionResult<()> {
        let mut duplicate = document();
        duplicate.principals.push(principal("another-id", "admin"));
        assert!(validate(&duplicate, true).is_err());
        let mut dangling = document();
        dangling
            .bindings
            .first_mut()
            .ok_or_else(|| invalid("fixture binding"))?
            .role_id = "missing".into();
        assert!(validate(&dangling, true).is_err());
        let mut obsolete = document();
        obsolete
            .roles
            .first_mut()
            .ok_or_else(|| invalid("fixture role"))?
            .rules
            .push(PermissionRule {
                scope: "RemovedEntity:read".into(),
                effect: PermissionEffect::Allow,
            });
        assert!(validate(&obsolete, true).is_err());
        validate(&obsolete, false)?;
        Ok(())
    }

    #[test]
    fn principal_management_does_not_grant_role_assignment() -> PermissionResult<()> {
        let mut document = document();
        document
            .roles
            .first_mut()
            .ok_or_else(|| invalid("fixture role"))?
            .rules = vec![PermissionRule {
            scope: "PermissionPrincipal:write".into(),
            effect: PermissionEffect::Allow,
        }];
        let identity = identity();
        let catalog = PermissionCatalog::registered();
        let permitted = |entity_type| {
            catalog.authorize_scope(
                EntityScope {
                    entity_type,
                    action: EntityAction::Write,
                },
                |scope| explicit_decision(&document, &identity, scope),
            )
        };
        assert!(permitted("PermissionPrincipal").is_ok());
        assert!(permitted("PermissionRoleBinding").is_err());
        Ok(())
    }

    #[test]
    fn mutation_batches_have_no_hidden_cascades_and_unknown_deletes_are_idempotent()
    -> PermissionResult<()> {
        let mut document = document();
        apply_changes(
            &mut document,
            &[PermissionChange::DeletePrincipal("admin".into())],
        )?;
        assert!(validate(&document, true).is_err());
        assert_eq!(document.bindings.len(), 1);
        apply_changes(
            &mut document,
            &[PermissionChange::DeleteRoleBinding("admin-binding".into())],
        )?;
        validate(&document, true)?;
        apply_changes(
            &mut document,
            &[PermissionChange::DeletePrincipal("admin".into())],
        )?;
        assert!(document.principals.is_empty());
        Ok(())
    }

    #[test]
    fn disabled_or_wrong_kind_principals_cannot_inherit_permissions() -> PermissionResult<()> {
        let mut document = document();
        let mut wrong_kind = identity();
        wrong_kind.principal.kind = PrincipalKind::Service;
        assert_eq!(
            explicit_decision(&document, &wrong_kind, role_scope()),
            ScopeDecision::Deny
        );
        document
            .principals
            .first_mut()
            .ok_or_else(|| invalid("fixture principal"))?
            .enabled = false;
        assert_eq!(
            explicit_decision(&document, &identity(), role_scope()),
            ScopeDecision::Deny
        );
        assert!(!has_administrator(&document));
        Ok(())
    }
}
