use super::*;
use crate::{Principal, PrincipalKind};
use std::time::{Duration, SystemTime};

static ENTITIES: [EntityScopeRegistration; 4] = [
    EntityScopeRegistration {
        entity_type: "Project",
    },
    EntityScopeRegistration {
        entity_type: "Scene",
    },
    EntityScopeRegistration {
        entity_type: "Camera",
    },
    EntityScopeRegistration {
        entity_type: "User",
    },
];
static RELATIONSHIPS: [ScopeInheritanceRegistration; 3] = [
    ScopeInheritanceRegistration {
        parent_entity: "Project",
        child_entity: "Scene",
    },
    ScopeInheritanceRegistration {
        parent_entity: "Scene",
        child_entity: "Camera",
    },
    ScopeInheritanceRegistration {
        parent_entity: "User",
        child_entity: "Camera",
    },
];
fn catalog() -> PermissionCatalog {
    PermissionCatalog::build(ENTITIES.iter(), std::iter::empty(), RELATIONSHIPS.iter())
}
fn camera(action: EntityAction) -> EntityScope {
    EntityScope {
        entity_type: "Camera",
        action,
    }
}
#[test]
fn project_write_inherits_transitively_but_never_grants_read_or_delete() {
    let catalog = catalog();
    let policy = |scope: EntityScope| {
        if scope.entity_type == "Project" && scope.action == EntityAction::Write {
            ScopeDecision::Allow
        } else {
            ScopeDecision::Unset
        }
    };
    assert_eq!(
        catalog.authorize_scope(camera(EntityAction::Write), policy),
        Ok(())
    );
    assert_eq!(
        catalog.authorize_scope(camera(EntityAction::Read), policy),
        Err(AuthError::Forbidden)
    );
    assert_eq!(
        catalog.authorize_scope(camera(EntityAction::Delete), policy),
        Err(AuthError::Forbidden)
    );
}
#[test]
fn explicit_child_override_wins_and_closest_parent_setting_wins() {
    let catalog = catalog();
    assert_eq!(
        catalog.authorize_scope(camera(EntityAction::Write), |scope| {
            match scope.entity_type {
                "Camera" => ScopeDecision::Deny,
                "Project" => ScopeDecision::Allow,
                _ => ScopeDecision::Unset,
            }
        }),
        Err(AuthError::Forbidden)
    );
    assert_eq!(
        catalog.authorize_scope(camera(EntityAction::Write), |scope| {
            match scope.entity_type {
                "Camera" => ScopeDecision::Allow,
                "Project" => ScopeDecision::Deny,
                _ => ScopeDecision::Unset,
            }
        }),
        Ok(())
    );
    assert_eq!(
        catalog.authorize_scope(camera(EntityAction::Write), |scope| {
            match scope.entity_type {
                "Scene" => ScopeDecision::Deny,
                "Project" => ScopeDecision::Allow,
                _ => ScopeDecision::Unset,
            }
        }),
        Err(AuthError::Forbidden)
    );
}
#[test]
fn equally_close_parent_denials_win_and_cycles_terminate() {
    let catalog = catalog();
    assert_eq!(
        catalog.authorize_scope(camera(EntityAction::Write), |scope| {
            match scope.entity_type {
                "Scene" => ScopeDecision::Allow,
                "User" => ScopeDecision::Deny,
                _ => ScopeDecision::Unset,
            }
        }),
        Err(AuthError::Forbidden)
    );
    let mut relationships = RELATIONSHIPS.to_vec();
    relationships.push(ScopeInheritanceRegistration {
        parent_entity: "Camera",
        child_entity: "Project",
    });
    let cyclic =
        PermissionCatalog::build(ENTITIES.iter(), std::iter::empty(), relationships.iter());
    assert_eq!(
        cyclic.authorize_scope(camera(EntityAction::Write), |_| ScopeDecision::Unset),
        Err(AuthError::Forbidden)
    );
}
#[test]
fn catalog_is_generated_and_conflicting_operation_bindings_fail_closed() {
    let first = OperationScopeRegistration {
        kind: OperationKind::Command,
        operation: "Mutate",
        scopes: &[EntityScope {
            entity_type: "Camera",
            action: EntityAction::Write,
        }],
    };
    let second = OperationScopeRegistration {
        kind: OperationKind::Command,
        operation: "Mutate",
        scopes: &[EntityScope {
            entity_type: "Camera",
            action: EntityAction::Delete,
        }],
    };
    let catalog = PermissionCatalog::build(
        ENTITIES.iter(),
        [&first, &second, &first].into_iter(),
        RELATIONSHIPS.iter(),
    );
    assert!(
        catalog
            .requirements(OperationKind::Command, "Mutate")
            .is_none()
    );
    assert!(
        catalog
            .requirements(OperationKind::Command, "Unknown")
            .is_none()
    );
    assert_eq!(
        catalog
            .scopes()
            .map(|scope| scope.to_string())
            .collect::<Vec<_>>(),
        vec![
            "Camera:read",
            "Camera:write",
            "Camera:delete",
            "Project:read",
            "Project:write",
            "Project:delete",
            "Scene:read",
            "Scene:write",
            "Scene:delete",
            "User:read",
            "User:write",
            "User:delete",
        ]
    );
}

inventory::submit! { EntityScopeRegistration { entity_type: "UnitScopeProject" } }
inventory::submit! { EntityScopeRegistration { entity_type: "UnitScopeCamera" } }
inventory::submit! { ScopeInheritanceRegistration { parent_entity: "UnitScopeProject", child_entity: "UnitScopeCamera" } }
inventory::submit! { OperationScopeRegistration { kind: OperationKind::Command, operation: "WriteUnitScopeCamera", scopes: &[EntityScope { entity_type: "UnitScopeCamera", action: EntityAction::Write }] } }
inventory::submit! { OperationScopeRegistration { kind: OperationKind::Command, operation: "MoveUnitScopeCamera", scopes: &[EntityScope { entity_type: "UnitScopeCamera", action: EntityAction::Write }, EntityScope { entity_type: "UnitScopeProject", action: EntityAction::Delete }] } }
struct ProjectWriter;
impl ScopePolicy for ProjectWriter {
    fn decision(
        &self,
        _: &AuthenticatedIdentity,
        scope: EntityScope,
        _: AuthorizationRequest<'_>,
    ) -> ScopeDecision {
        if scope.entity_type == "UnitScopeProject" && scope.action == EntityAction::Write {
            ScopeDecision::Allow
        } else {
            ScopeDecision::Unset
        }
    }
}
fn identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        principal: Principal {
            issuer: "fixture".into(),
            subject: "user".into(),
            kind: PrincipalKind::User,
        },
        client_id: None,
        scopes: vec![],
        delegation: vec![],
        claims: serde_json::Value::Null,
        expires_at: SystemTime::now()
            .checked_add(Duration::from_mins(1))
            .unwrap_or_else(SystemTime::now),
    }
}
#[test]
fn adapter_maps_events_and_operations_and_requires_every_declared_scope() {
    let policy = ScopedAuthorizer::new(Arc::new(ProjectWriter));
    let identity = identity();
    let args = serde_json::json!({"id":"camera-1"});
    assert_eq!(
        policy.authorize(
            &identity,
            AuthorizationRequest::Execute {
                kind: OperationKind::Command,
                operation: "WriteUnitScopeCamera",
                arguments: &args
            }
        ),
        Ok(())
    );
    assert_eq!(
        policy.authorize(
            &identity,
            AuthorizationRequest::Execute {
                kind: OperationKind::Command,
                operation: "MoveUnitScopeCamera",
                arguments: &args
            }
        ),
        Err(AuthError::Forbidden)
    );
    assert_eq!(
        policy.authorize(
            &identity,
            AuthorizationRequest::Execute {
                kind: OperationKind::Command,
                operation: "Unknown",
                arguments: &args
            }
        ),
        Err(AuthError::Forbidden)
    );
    assert_eq!(
        policy.authorize(
            &identity,
            AuthorizationRequest::ClientEvent {
                entity_type: "UnitScopeCamera",
                change: EventChange::Set,
                proposed: &args
            }
        ),
        Ok(())
    );
    assert_eq!(
        policy.authorize(
            &identity,
            AuthorizationRequest::ClientEvent {
                entity_type: "UnitScopeCamera",
                change: EventChange::Delete,
                proposed: &args
            }
        ),
        Err(AuthError::Forbidden)
    );
    assert_eq!(
        policy.authorize(
            &identity,
            AuthorizationRequest::CommandResult {
                transaction: "unassigned"
            }
        ),
        Err(AuthError::Forbidden)
    );
}
