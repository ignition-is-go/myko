//! Transport-neutral identity, claim, decision, and visibility contracts.
//!
//! Durable authority state is ordinary Myko entity history implemented by the
//! `myko-authority` service. This module intentionally contains no second
//! journal, cache-owned truth, or snapshot authorization surface.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AccessOperation, AccessRequest, FederationPermission, NodeId, PrincipalId, ScopeId,
    ScopeSelection, ScopeTopology, ServiceId,
};

macro_rules! authority_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[must_use]
            pub fn random() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

authority_id!(AuthorityGrantId);
authority_id!(DelegationId);
authority_id!(CapabilityId);
authority_id!(ObligationId);
authority_id!(ChallengeId);
authority_id!(ApprovalId);
authority_id!(LeaseId);
authority_id!(AuthorityRealmId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Person,
    Node,
    Agent,
    Command,
    Task,
    Tool,
    Service,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Principal {
    pub id: PrincipalId,
    pub kind: PrincipalKind,
}

impl Principal {
    #[must_use]
    pub const fn new(id: PrincipalId, kind: PrincipalKind) -> Self {
        Self { id, kind }
    }

    #[must_use]
    pub const fn node(id: PrincipalId) -> Self {
        Self::new(id, PrincipalKind::Node)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProvenanceOperation {
    AgentInvocation {
        agent_id: String,
    },
    CommandInvocation {
        command_id: String,
    },
    TaskInvocation {
        task_id: String,
    },
    ToolResourceOperation {
        tool_id: String,
        resource: String,
        operation: String,
    },
    NodeForward {
        node_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProvenanceHop {
    pub delegation_id: DelegationId,
    pub delegator: Principal,
    pub delegate: Principal,
    /// Must equal the operation stored in the authoritative delegation entity.
    pub operation: ProvenanceOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityPresentation {
    pub principal: Principal,
    pub executor: Principal,
    #[serde(default)]
    pub provenance: Vec<ProvenanceHop>,
    #[serde(default)]
    pub approvals: Vec<ApprovalId>,
    /// Optional lease request made at admission.
    #[serde(default)]
    pub requested_lease: Option<AuthorityLeaseRequest>,
    /// Durable lease presented when a long-lived request is rechecked.
    #[serde(default)]
    pub active_lease: Option<LeaseId>,
}

impl AuthorityPresentation {
    #[must_use]
    pub fn direct(principal: Principal) -> Self {
        Self {
            executor: principal.clone(),
            principal,
            provenance: Vec::new(),
            approvals: Vec::new(),
            requested_lease: None,
            active_lease: None,
        }
    }

    #[must_use]
    pub fn direct_node(id: PrincipalId) -> Self {
        Self::direct(Principal::node(id))
    }

    #[must_use]
    pub fn forward(mut self, hop: ProvenanceHop) -> Self {
        self.executor = hop.delegate.clone();
        self.provenance.push(hop);
        self
    }

    #[must_use]
    pub fn with_approval(mut self, approval: ApprovalId) -> Self {
        self.approvals.push(approval);
        self
    }

    #[must_use]
    pub const fn requesting_lease(mut self, request: AuthorityLeaseRequest) -> Self {
        self.requested_lease = Some(request);
        self
    }

    #[must_use]
    pub fn with_lease(mut self, lease: LeaseId) -> Self {
        self.active_lease = Some(lease);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceClaimKind {
    Primary,
    Referenced,
    Affected,
}

/// A conjunctive claim. Requirements are local to this resource, while
/// `AccessRequest::application_capabilities` remains truly request-global.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceClaim {
    pub selection: ScopeSelection,
    pub kind: ResourceClaimKind,
    /// Optional immutable authoritative source for reactive/read dependencies.
    /// A missing source is a wildcard only in a declaration; concrete reads
    /// populate it so a handler cannot swap peers after admission.
    #[serde(default)]
    pub source_node: Option<NodeId>,
    pub service_id: Option<ServiceId>,
    pub item_type: Option<String>,
    pub item_id: Option<String>,
    #[serde(default)]
    pub required_permissions: Vec<FederationPermission>,
    #[serde(default)]
    pub required_operations: Vec<AccessOperation>,
    #[serde(default)]
    pub required_capabilities: Vec<CapabilityId>,
}

impl ResourceClaim {
    #[must_use]
    pub const fn scope(scope_id: ScopeId, kind: ResourceClaimKind) -> Self {
        Self {
            selection: ScopeSelection::Exact(scope_id),
            kind,
            source_node: None,
            service_id: None,
            item_type: None,
            item_id: None,
            required_permissions: Vec::new(),
            required_operations: Vec::new(),
            required_capabilities: Vec::new(),
        }
    }

    #[must_use]
    pub fn requiring_capability(mut self, capability: CapabilityId) -> Self {
        self.required_capabilities.push(capability);
        self
    }

    /// Returns whether this preflight claim safely contains one actual
    /// handler read/effect claim under locally authoritative topology.
    #[must_use]
    pub fn covers_actual(&self, actual: &Self, topology: &ScopeTopology) -> bool {
        selection_covers(&self.selection, &actual.selection, topology)
            && self.covers_resource(actual)
            && self.covers_requirements(actual)
            && (self.kind == ResourceClaimKind::Primary || self.kind == actual.kind)
    }

    /// Returns whether an outer command declaration contains a nested
    /// handler's potential declaration. Kind is intentionally ignored here;
    /// concrete reads/mutations still enforce it in [`Self::covers_actual`].
    #[must_use]
    pub fn covers_declared(&self, nested: &Self, topology: &ScopeTopology) -> bool {
        selection_covers(&self.selection, &nested.selection, topology)
            && self.covers_resource(nested)
            && self.covers_requirements(nested)
    }

    fn covers_resource(&self, actual: &Self) -> bool {
        self.source_node
            .is_none_or(|source| actual.source_node == Some(source))
            && self
                .service_id
                .as_ref()
                .is_none_or(|service| actual.service_id.as_ref() == Some(service))
            && self
                .item_type
                .as_ref()
                .is_none_or(|item| actual.item_type.as_ref() == Some(item))
            && self
                .item_id
                .as_ref()
                .is_none_or(|item| actual.item_id.as_ref() == Some(item))
    }

    fn covers_requirements(&self, actual: &Self) -> bool {
        actual
            .required_permissions
            .iter()
            .all(|permission| self.required_permissions.contains(permission))
            && actual
                .required_operations
                .iter()
                .all(|operation| self.required_operations.contains(operation))
            && actual
                .required_capabilities
                .iter()
                .all(|capability| self.required_capabilities.contains(capability))
    }
}

fn selection_covers(
    declared: &ScopeSelection,
    actual: &ScopeSelection,
    topology: &ScopeTopology,
) -> bool {
    match (declared, actual) {
        (ScopeSelection::Exact(declared), ScopeSelection::Exact(actual)) => declared == actual,
        (
            ScopeSelection::Subtree(declared),
            ScopeSelection::Exact(actual) | ScopeSelection::Subtree(actual),
        ) => declared == actual || topology.is_descendant_of(actual, declared),
        (ScopeSelection::Exact(_), ScopeSelection::Subtree(_)) => false,
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityConstraints {
    #[serde(default)]
    pub services: Vec<ServiceId>,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub item_types: Vec<String>,
    pub max_lease_seconds: Option<u64>,
    #[serde(default)]
    pub allow_offline: bool,
}

impl AuthorityConstraints {
    #[must_use]
    pub fn permits(&self, request: &AccessRequest) -> bool {
        (self.services.is_empty()
            || request
                .service_id
                .as_ref()
                .is_some_and(|service| self.services.contains(service)))
            && (self.commands.is_empty()
                || request
                    .command_type
                    .as_ref()
                    .is_some_and(|command| self.commands.contains(command)))
            && (self.item_types.is_empty()
                || (!request.resource_claims.is_empty()
                    && request.resource_claims.iter().all(|claim| {
                        claim
                            .item_type
                            .as_ref()
                            .is_some_and(|item| self.item_types.contains(item))
                    })))
            && request.lease.as_ref().is_none_or(|lease| {
                (!lease.offline || self.allow_offline)
                    && self
                        .max_lease_seconds
                        .is_none_or(|maximum| lease.duration_seconds <= maximum)
            })
    }

    #[must_use]
    pub fn attenuates(&self, parent: &Self) -> bool {
        subset_or_parent_unbounded(&self.services, &parent.services)
            && subset_or_parent_unbounded(&self.commands, &parent.commands)
            && subset_or_parent_unbounded(&self.item_types, &parent.item_types)
            && match (self.max_lease_seconds, parent.max_lease_seconds) {
                (_, None) => true,
                (Some(child), Some(parent)) => child <= parent,
                (None, Some(_)) => false,
            }
            && (!self.allow_offline || parent.allow_offline)
    }
}

fn subset_or_parent_unbounded<T: Eq>(child: &[T], parent: &[T]) -> bool {
    parent.is_empty() || (!child.is_empty() && child.iter().all(|value| parent.contains(value)))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationCapability {
    pub id: CapabilityId,
    pub description: String,
    #[serde(default)]
    pub constraints: AuthorityConstraints,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityLeaseRequest {
    pub duration_seconds: u64,
    pub offline: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityLease {
    pub id: LeaseId,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub offline: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationPhase {
    #[default]
    Admission,
    Effect,
    Continuation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityGrant {
    pub id: AuthorityGrantId,
    pub realm_id: AuthorityRealmId,
    pub grantor: Principal,
    pub grantee: Principal,
    pub selection: ScopeSelection,
    #[serde(default)]
    pub permissions: Vec<FederationPermission>,
    #[serde(default)]
    pub operations: Vec<AccessOperation>,
    #[serde(default)]
    pub capabilities: Vec<CapabilityId>,
    #[serde(default)]
    pub constraints: AuthorityConstraints,
    #[serde(default)]
    pub obligations: Vec<ObligationId>,
    pub valid_from: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub max_uses: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum DelegationParent {
    Grant(AuthorityGrantId),
    Delegation(DelegationId),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityDelegation {
    pub id: DelegationId,
    pub realm_id: AuthorityRealmId,
    pub parent: DelegationParent,
    pub delegator: Principal,
    pub delegate: Principal,
    pub provenance_operation: ProvenanceOperation,
    #[serde(default)]
    pub selections: Vec<ScopeSelection>,
    #[serde(default)]
    pub permissions: Vec<FederationPermission>,
    #[serde(default)]
    pub operations: Vec<AccessOperation>,
    #[serde(default)]
    pub capabilities: Vec<CapabilityId>,
    #[serde(default)]
    pub constraints: AuthorityConstraints,
    pub expires_at: Option<DateTime<Utc>>,
    pub max_uses: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Obligation {
    pub id: ObligationId,
    pub realm_id: AuthorityRealmId,
    pub challenge_kind: String,
    pub prompt: String,
    #[serde(default)]
    pub approvers: Vec<Principal>,
    pub approval_lifetime_seconds: u64,
    pub approval_use_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationBinding {
    pub principal: Principal,
    pub executor: Principal,
    pub provenance: Vec<ProvenanceHop>,
    pub operation: AccessOperation,
    pub service_id: Option<ServiceId>,
    pub command_id: Option<crate::CommandId>,
    pub command_type: Option<String>,
    pub resources: Vec<ResourceClaim>,
    pub capabilities: Vec<CapabilityId>,
    pub arguments_digest: Option<String>,
    pub effect_digest: Option<String>,
}

impl AuthorizationBinding {
    #[must_use]
    pub fn from_request(request: &AccessRequest) -> Self {
        Self {
            principal: request.presentation.principal.clone(),
            executor: request.presentation.executor.clone(),
            provenance: request.presentation.provenance.clone(),
            operation: request.operation,
            service_id: request.service_id.clone(),
            command_id: request.command_id,
            command_type: request.command_type.clone(),
            resources: request.resource_claims.clone(),
            capabilities: request.application_capabilities.clone(),
            arguments_digest: request.arguments_digest.clone(),
            effect_digest: request.effect_digest.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityChallenge {
    pub id: ChallengeId,
    pub realm_id: AuthorityRealmId,
    pub obligation_id: ObligationId,
    pub kind: String,
    pub prompt: String,
    pub binding: AuthorizationBinding,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub id: ApprovalId,
    pub realm_id: AuthorityRealmId,
    pub challenge_id: ChallengeId,
    pub obligation_id: ObligationId,
    pub approver: Principal,
    pub binding: AuthorizationBinding,
    pub approved: bool,
    pub decided_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub max_uses: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceVisibility {
    Present,
    AuthoritativelyAbsent,
    Unbound,
    Unauthorized,
    Undiscoverable,
    Unreachable,
    NotReplicated,
    TopologyIncomplete,
}

/// Proof supplied with a selected projection. Callers cannot convert an empty
/// replicated cache into authoritative absence without a completeness proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionCoverage {
    LocalAuthoritative,
    ReplicatedComplete,
    ReplicatedIncomplete,
    Unreachable,
    Undiscoverable,
}

/// Authorization-filtered selected query result.
///
/// `value == None` means the resource was not visible; an empty value inside
/// `Some` is an actual query result and its visibility states whether absence
/// is authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedQueryResult<T> {
    pub value: Option<T>,
    pub visibility: ResourceVisibility,
    /// Node-owned proof describing why `complete` is trustworthy.
    pub coverage: ProjectionCoverage,
    /// Source watermark through which this projection is complete.
    pub through: Option<crate::LogPosition>,
    pub complete: bool,
    /// False when authorization deliberately narrowed the requested subtree.
    /// `complete` then applies only to `included_scopes` and never asserts
    /// absence in the hidden remainder.
    pub requested_fully_authorized: bool,
    pub authorization: Option<AuthorizationDecision>,
    #[serde(default)]
    pub included_scopes: Vec<ScopeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationExplanation {
    pub code: String,
    pub message: String,
    pub grant_id: Option<AuthorityGrantId>,
    pub delegation_id: Option<DelegationId>,
    pub obligation_id: Option<ObligationId>,
    pub constraint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationReport {
    pub evaluated_at: DateTime<Utc>,
    pub principal: Principal,
    pub executor: Principal,
    pub operation: AccessOperation,
    pub explanations: Vec<AuthorizationExplanation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermitDecision {
    pub report: AuthorizationReport,
    pub lease: Option<AuthorityLease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenyDecision {
    pub report: AuthorizationReport,
    pub visibility: ResourceVisibility,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // Decisions stay directly inspectable across the public wire API.
pub enum AuthorizationDecision {
    Permit(PermitDecision),
    Deny(DenyDecision),
    Challenge {
        challenge: AuthorityChallenge,
        report: AuthorizationReport,
    },
}

impl AuthorizationDecision {
    #[must_use]
    pub const fn is_permit(&self) -> bool {
        matches!(self, Self::Permit(_))
    }

    #[must_use]
    pub fn public_message(&self) -> String {
        match self {
            Self::Permit(_) => "authorized".to_owned(),
            Self::Deny(decision) => decision
                .report
                .explanations
                .last()
                .map_or_else(|| "access denied".to_owned(), |value| value.message.clone()),
            Self::Challenge { challenge, .. } => format!(
                "authorization challenge {} ({})",
                challenge.id, challenge.kind
            ),
        }
    }
}
