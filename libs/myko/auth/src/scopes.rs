//! Generated entity permissions and operation-level inheritance.
use crate::{
    AuthError, AuthenticatedIdentity, AuthorizationRequest, Authorizer, EventChange, OperationKind,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, OnceLock},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntityAction {
    Read,
    Write,
    Delete,
}
impl fmt::Display for EntityAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Delete => "delete",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityScope {
    pub entity_type: &'static str,
    pub action: EntityAction,
}
impl fmt::Display for EntityScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.entity_type, self.action)
    }
}

pub struct EntityScopeRegistration {
    pub entity_type: &'static str,
}
pub struct OperationScopeRegistration {
    pub kind: OperationKind,
    pub operation: &'static str,
    pub scopes: &'static [EntityScope],
}
#[derive(Clone, Copy)]
pub struct ScopeInheritanceRegistration {
    pub parent_entity: &'static str,
    pub child_entity: &'static str,
}
inventory::collect!(EntityScopeRegistration);
inventory::collect!(OperationScopeRegistration);
inventory::collect!(ScopeInheritanceRegistration);

/// Unset inherits. An explicit child decision overrides its ancestors for the same action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeDecision {
    Allow,
    Deny,
    Unset,
}

/// App-owned explicit grants. Return Unset when no grant or override exists.
///
/// Group and role resolution stays local. Implementations must also honor verified
/// credential and delegation limits. OAuth scope strings have no built-in mapping.
/// The request retains original arguments for additional app constraints.
pub trait ScopePolicy: Send + Sync + 'static {
    fn decision(
        &self,
        identity: &AuthenticatedIdentity,
        scope: EntityScope,
        request: AuthorizationRequest<'_>,
    ) -> ScopeDecision;
    fn revision(&self) -> u64 {
        0
    }
    /// # Errors
    /// Returns a denial unless the app explicitly permits this executor response.
    fn authorize_command_result(
        &self,
        _identity: &AuthenticatedIdentity,
        _transaction: &str,
    ) -> Result<(), AuthError> {
        Err(AuthError::Forbidden)
    }
}

#[derive(Clone, Debug)]
pub struct ScopeResolutionStep {
    pub scope: EntityScope,
    pub decision: ScopeDecision,
    pub distance: usize,
}
#[derive(Clone, Debug)]
pub struct ScopeResolution {
    pub allowed: bool,
    pub decisions: Vec<ScopeResolutionStep>,
}

/// The catalog is generated from linked Rust registrations, never client-supplied names.
pub struct PermissionCatalog {
    entities: BTreeSet<&'static str>,
    operations: BTreeMap<OperationKind, BTreeMap<&'static str, Option<Vec<EntityScope>>>>,
    parents: BTreeMap<&'static str, Vec<&'static str>>,
}
impl PermissionCatalog {
    #[must_use]
    pub fn registered() -> &'static Self {
        static CATALOG: OnceLock<PermissionCatalog> = OnceLock::new();
        CATALOG.get_or_init(|| {
            Self::build(
                inventory::iter::<EntityScopeRegistration>.into_iter(),
                inventory::iter::<OperationScopeRegistration>.into_iter(),
                inventory::iter::<ScopeInheritanceRegistration>.into_iter(),
            )
        })
    }

    fn build<'a>(
        entities: impl Iterator<Item = &'a EntityScopeRegistration>,
        operations: impl Iterator<Item = &'a OperationScopeRegistration>,
        inheritance: impl Iterator<Item = &'a ScopeInheritanceRegistration>,
    ) -> Self {
        let entities: BTreeSet<_> = entities.map(|entity| entity.entity_type).collect();
        let mut bindings =
            BTreeMap::<OperationKind, BTreeMap<&'static str, Option<Vec<EntityScope>>>>::new();
        for operation in operations {
            let mut requirements = operation.scopes.to_vec();
            requirements.sort_unstable();
            requirements.dedup();
            let valid = !requirements.is_empty()
                && requirements
                    .iter()
                    .all(|scope| entities.contains(scope.entity_type));
            let candidate = valid.then_some(requirements);
            let entry = bindings
                .entry(operation.kind)
                .or_default()
                .entry(operation.operation)
                .or_insert_with(|| candidate.clone());
            if *entry != candidate {
                *entry = None;
            }
        }
        let mut parents = BTreeMap::<_, Vec<_>>::new();
        for relationship in inheritance {
            if entities.contains(relationship.parent_entity)
                && entities.contains(relationship.child_entity)
            {
                parents
                    .entry(relationship.child_entity)
                    .or_default()
                    .push(relationship.parent_entity);
            }
        }
        for ancestors in parents.values_mut() {
            ancestors.sort_unstable();
            ancestors.dedup();
        }
        Self {
            entities,
            operations: bindings,
            parents,
        }
    }

    pub fn scopes(&self) -> impl Iterator<Item = EntityScope> + '_ {
        self.entities.iter().flat_map(|entity_type| {
            [
                EntityAction::Read,
                EntityAction::Write,
                EntityAction::Delete,
            ]
            .map(|action| EntityScope {
                entity_type,
                action,
            })
        })
    }

    #[must_use]
    pub fn requirements(&self, kind: OperationKind, operation: &str) -> Option<&[EntityScope]> {
        self.operations.get(&kind)?.get(operation)?.as_deref()
    }

    #[must_use]
    pub fn entity_scope(&self, entity_type: &str, action: EntityAction) -> Option<EntityScope> {
        self.entities
            .get(entity_type)
            .map(|entity_type| EntityScope {
                entity_type,
                action,
            })
    }

    /// Ancestors come from declared belongs-to and owns-many relationships.
    #[must_use]
    pub fn parents(&self, entity_type: &str) -> &[&'static str] {
        self.parents.get(entity_type).map_or(&[], Vec::as_slice)
    }

    pub fn operations(
        &self,
    ) -> impl Iterator<Item = (OperationKind, &'static str, Option<&[EntityScope]>)> {
        self.operations.iter().flat_map(|(kind, operations)| {
            operations
                .iter()
                .map(move |(name, scopes)| (*kind, *name, scopes.as_deref()))
        })
    }

    /// Evaluate an entity scope using the same inheritance rules as operation dispatch.
    /// # Errors
    /// Returns a denial for unknown scopes or when explicit grants do not permit access.
    pub fn authorize_scope(
        &self,
        scope: EntityScope,
        decision: impl FnMut(EntityScope) -> ScopeDecision,
    ) -> Result<(), AuthError> {
        if self.resolve_scope(scope, decision, |_| {}) {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }

    /// Trace the exact decisions used by operation authorization.
    pub fn explain_scope(
        &self,
        scope: EntityScope,
        decision: impl FnMut(EntityScope) -> ScopeDecision,
    ) -> ScopeResolution {
        let mut decisions = Vec::new();
        let allowed = self.resolve_scope(scope, decision, |step| decisions.push(step));
        ScopeResolution { allowed, decisions }
    }

    fn resolve_scope(
        &self,
        scope: EntityScope,
        mut decision: impl FnMut(EntityScope) -> ScopeDecision,
        mut visit: impl FnMut(ScopeResolutionStep),
    ) -> bool {
        if !self.entities.contains(scope.entity_type) {
            return false;
        }
        let mut level = BTreeSet::from([scope.entity_type]);
        let mut visited = BTreeSet::new();
        let mut distance: usize = 0;
        while !level.is_empty() {
            let mut next = BTreeSet::new();
            let mut allowed = false;
            let mut denied = false;
            for entity_type in level {
                if !visited.insert(entity_type) {
                    continue;
                }
                let candidate = EntityScope {
                    entity_type,
                    action: scope.action,
                };
                let value = decision(candidate);
                visit(ScopeResolutionStep {
                    scope: candidate,
                    decision: value,
                    distance,
                });
                match value {
                    ScopeDecision::Allow => allowed = true,
                    ScopeDecision::Deny => denied = true,
                    ScopeDecision::Unset => next.extend(
                        self.parents(entity_type)
                            .iter()
                            .copied()
                            .filter(|parent| !visited.contains(parent)),
                    ),
                }
            }
            if denied {
                return false;
            }
            if allowed {
                return true;
            }
            level = next;
            distance = distance.saturating_add(1);
        }
        false
    }
}

/// Resolves generated operation requirements and inherited grants before dispatch.
/// This adapter grants operations on an entity type. It does not filter individual rows.
pub struct ScopedAuthorizer {
    policy: Arc<dyn ScopePolicy>,
}
impl ScopedAuthorizer {
    pub fn new(policy: Arc<dyn ScopePolicy>) -> Self {
        Self { policy }
    }
}
impl Authorizer for ScopedAuthorizer {
    fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: AuthorizationRequest<'_>,
    ) -> Result<(), AuthError> {
        let catalog = PermissionCatalog::registered();
        let event_scope;
        let requirements = match request {
            AuthorizationRequest::Discover { kind, operation }
            | AuthorizationRequest::Execute {
                kind, operation, ..
            } => catalog
                .requirements(kind, operation)
                .ok_or(AuthError::Forbidden)?,
            AuthorizationRequest::ClientEvent {
                entity_type,
                change,
                ..
            } => {
                event_scope = [catalog
                    .entity_scope(
                        entity_type,
                        match change {
                            EventChange::Set => EntityAction::Write,
                            EventChange::Delete => EntityAction::Delete,
                        },
                    )
                    .ok_or(AuthError::Forbidden)?];
                &event_scope
            }
            AuthorizationRequest::CommandResult { transaction } => {
                return self.policy.authorize_command_result(identity, transaction);
            }
        };
        for scope in requirements {
            catalog.authorize_scope(*scope, |candidate| {
                self.policy.decision(identity, candidate, request)
            })?;
        }
        Ok(())
    }
    fn revision(&self) -> u64 {
        self.policy.revision()
    }
}

#[cfg(test)]
mod tests;
