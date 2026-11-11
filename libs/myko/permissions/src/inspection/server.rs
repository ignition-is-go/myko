use super::{
    GetPermissionStructure, InspectPermissionPrincipal, PERMISSION_STRUCTURE_VERSION,
    PermissionCommandCapability, PermissionDecisionStep, PermissionEntityDescription,
    PermissionInspectionStatus, PermissionOperationDescription, PermissionPrincipalInspection,
    PermissionScopeExplanation, PermissionStructure,
};
use crate::{
    commands::PermissionAdminOperation,
    engine,
    error::PermissionError,
    model::{PermissionAuthority, PermissionPrincipalKind},
    runtime::Permissions,
};
use hyphae::MapExt;
use myko::{
    core::capability::ServerScoped,
    report::{ReportContext, ReportHandler},
    request::RequestCacheScope,
};
use myko_auth::{
    AuthenticatedIdentity, AuthorizationRequest, OperationKind, PermissionCatalog, Principal,
    PrincipalKind, ScopeDecision,
};
use std::{collections::BTreeSet, sync::Arc, time::SystemTime};

fn snapshot(
    ctx: &ReportContext,
    operation: &str,
    arguments: &serde_json::Value,
) -> Result<Arc<PermissionAuthority>, (PermissionInspectionStatus, String)> {
    let session = ctx.req.auth.as_ref().ok_or_else(|| {
        (
            PermissionInspectionStatus::Denied,
            "Permission inspection requires authentication".into(),
        )
    })?;
    session
        .authorize(AuthorizationRequest::Execute {
            kind: OperationKind::Report,
            operation,
            arguments,
        })
        .map_err(|_| {
            (
                PermissionInspectionStatus::Denied,
                "Permission inspection denied".into(),
            )
        })?;
    let runtime = Permissions::for_context(ctx.__server_ctx()).map_err(|_| {
        (
            PermissionInspectionStatus::Unsupported,
            "This server does not expose the standard persisted permission policy".into(),
        )
    })?;
    runtime
        .authorize_inspection(&ctx.req, operation, arguments)
        .map_err(|error| match error {
            PermissionError::Unauthorized => (
                PermissionInspectionStatus::Denied,
                "Permission inspection denied".into(),
            ),
            _ => (
                PermissionInspectionStatus::Unavailable,
                "Permission policy is unavailable".into(),
            ),
        })
}

impl ReportHandler for GetPermissionStructure {
    type Output = PermissionStructure;
    fn request_cache_scope() -> RequestCacheScope {
        RequestCacheScope::PerClient
    }
    fn compute(
        &self,
        ctx: ReportContext,
    ) -> impl hyphae::Materialize<Arc<Self::Output>, hyphae::Definite> {
        ctx.__server_ctx()
            .registry
            .get_or_create("PermissionAuthority")
            .entries()
            .map(move |_| {
                let output = match snapshot(&ctx, "GetPermissionStructure", &serde_json::json!({}))
                {
                    Ok(snapshot) => {
                        let catalog = PermissionCatalog::registered();
                        let entities = catalog
                            .scopes()
                            .map(|scope| scope.entity_type)
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .map(|entity_type| PermissionEntityDescription {
                                entity_type: entity_type.into(),
                                parents: catalog
                                    .parents(entity_type)
                                    .iter()
                                    .map(|parent| (*parent).into())
                                    .collect(),
                                scopes: catalog
                                    .scopes()
                                    .filter(|scope| scope.entity_type == entity_type)
                                    .map(|scope| scope.to_string())
                                    .collect(),
                            })
                            .collect();
                        let operations = catalog
                            .operations()
                            .map(|(kind, operation, scopes)| PermissionOperationDescription {
                                kind: format!("{kind:?}"),
                                operation: operation.into(),
                                required_scopes: scopes
                                    .unwrap_or_default()
                                    .iter()
                                    .map(ToString::to_string)
                                    .collect(),
                                mapped: scopes.is_some(),
                            })
                            .collect();
                        let commands = inventory::iter::<PermissionAdminOperation>
                            .into_iter()
                            .map(|entry| entry.operation)
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .map(|operation| PermissionCommandCapability {
                                operation: operation.into(),
                                allowed: Permissions::for_context(ctx.__server_ctx()).is_ok_and(
                                    |runtime| runtime.permits_administration(&ctx.req, operation),
                                ),
                            })
                            .collect();
                        PermissionStructure {
                    version: PERMISSION_STRUCTURE_VERSION,
                    status: PermissionInspectionStatus::Ready,
                    message:
                        "Entity operation permissions; individual row policies are not described"
                            .into(),
                    revision: Some(snapshot.revision),
                    entities,
                    operations,
                    document: Some(snapshot.document.clone()),
                    commands,
                }
                    }
                    Err((status, message)) => PermissionStructure {
                        version: PERMISSION_STRUCTURE_VERSION,
                        status,
                        message,
                        revision: None,
                        entities: Vec::new(),
                        operations: Vec::new(),
                        document: None,
                        commands: Vec::new(),
                    },
                };
                Arc::new(output)
            })
    }
}

impl ReportHandler for InspectPermissionPrincipal {
    type Output = PermissionPrincipalInspection;
    fn request_cache_scope() -> RequestCacheScope {
        RequestCacheScope::PerClient
    }
    fn compute(
        &self,
        ctx: ReportContext,
    ) -> impl hyphae::Materialize<Arc<Self::Output>, hyphae::Definite> {
        let principal_id = self.principal_id.clone();
        ctx.__server_ctx().registry.get_or_create("PermissionAuthority").entries().map(move |_| {
        let arguments = serde_json::json!({"principalId":principal_id});
        let result =
            snapshot(&ctx, "InspectPermissionPrincipal", &arguments).and_then(|snapshot| {
                let principal = snapshot
                    .document
                    .principals
                    .iter()
                    .find(|entry| entry.id == principal_id)
                    .ok_or_else(|| {
                        (
                            PermissionInspectionStatus::Unavailable,
                            "Principal is not in the current policy".into(),
                        )
                    })?;
                let identity = AuthenticatedIdentity {
                    user_id: None,
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
                    expires_at: SystemTime::UNIX_EPOCH,
                };
                let catalog = PermissionCatalog::registered();
                let scopes = catalog
                    .scopes()
                    .map(|scope| {
                        let mut steps = Vec::new();
                        let resolution = catalog.explain_scope(scope, |candidate| {
                            let (decision, sources) = engine::explicit_decision_sources(
                                &snapshot.document,
                                &identity,
                                candidate,
                            );
                            steps.push((candidate.to_string(), decision, sources));
                            decision
                        });
                        let steps = resolution
                            .decisions
                            .into_iter()
                            .zip(steps)
                            .map(
                                |(step, (scope, decision, sources))| PermissionDecisionStep {
                                    scope,
                                    decision: match decision {
                                        ScopeDecision::Allow => "Allow",
                                        ScopeDecision::Deny => "Deny",
                                        ScopeDecision::Unset => "Inherit",
                                    }
                                    .into(),
                                    distance: step.distance,
                                    sources,
                                },
                            )
                            .collect();
                        PermissionScopeExplanation {
                            scope: scope.to_string(),
                            allowed: resolution.allowed,
                            steps,
                        }
                    })
                    .collect();
                Ok((snapshot.revision, scopes))
            });
        let (status, message, revision, scopes) = match result {
            Ok((revision, scopes)) => (PermissionInspectionStatus::Ready,
                "Local grants only. Token scope ceilings, delegation, external account availability, and row policies are not evaluated".into(), Some(revision), scopes),
            Err((status, message)) => (status, message, None, Vec::new()),
        };
        Arc::new(PermissionPrincipalInspection {
            version: PERMISSION_STRUCTURE_VERSION,
            status,
            message,
            revision,
            principal_id: principal_id.clone(),
            scopes,
        })
        })
    }
}
